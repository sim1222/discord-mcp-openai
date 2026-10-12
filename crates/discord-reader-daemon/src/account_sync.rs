//! Account discovery and fair, resumable, bounded message retrieval.

use crate::{
    message_lookup::parse_message,
    thread_listing::{DiscordThreadListing, ThreadListing, ThreadQuery},
};
use chrono::{Duration, Utc};
use discord_api::{
    types::{Channel, Guild, Message},
    DiscordRequest, SharedDiscordClient,
};
use discord_store::{
    sqlite::{
        account::{AccountInventory, AccountJob, AccountTarget, TargetCheckpoint},
        now_iso,
    },
    Store, StoreError,
};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct AccountSyncOptions {
    pub(crate) max_targets: u32,
    pub(crate) page_size: u8,
    pub(crate) refresh_inventory: bool,
    pub(crate) scope: AccountScope,
    pub(crate) max_discovery_http_requests: u32,
    pub(crate) max_body_http_requests: u32,
    pub(crate) max_messages: u32,
    pub(crate) max_duration_seconds: u32,
    pub(crate) identity_http_reservation: u32,
    pub(crate) execution_timeout: Option<std::time::Duration>,
    pub(crate) execution_deadline: Option<tokio::time::Instant>,
}

impl Default for AccountSyncOptions {
    fn default() -> Self {
        Self {
            max_targets: 50,
            page_size: 100,
            refresh_inventory: true,
            scope: AccountScope::default(),
            max_discovery_http_requests: 3000,
            max_body_http_requests: 3000,
            max_messages: 100000,
            max_duration_seconds: 60,
            identity_http_reservation: 0,
            execution_timeout: None,
            execution_deadline: None,
        }
    }
}

#[derive(Clone, serde::Serialize)]
pub(crate) struct AccountScope {
    pub(crate) guild_ids: Option<Vec<String>>,
    pub(crate) channel_ids: Option<Vec<String>>,
    pub(crate) exclude_guild_ids: Vec<String>,
    pub(crate) exclude_channel_ids: Vec<String>,
    pub(crate) include_dms: bool,
    pub(crate) include_threads: bool,
}
impl Default for AccountScope {
    fn default() -> Self {
        Self {
            guild_ids: None,
            channel_ids: None,
            exclude_guild_ids: vec![],
            exclude_channel_ids: vec![],
            include_dms: true,
            include_threads: true,
        }
    }
}
impl AccountScope {
    fn allows(&self, channel: &str, guild: Option<&str>, parent: Option<&str>, kind: i32) -> bool {
        if self
            .exclude_channel_ids
            .iter()
            .any(|id| id == channel || Some(id.as_str()) == parent)
            || guild.is_some_and(|guild| self.exclude_guild_ids.iter().any(|id| id == guild))
        {
            return false;
        }
        if !self.include_threads && matches!(kind, 10..=12) {
            return false;
        }
        if guild.is_none() && (!self.include_dms || self.guild_ids.is_some()) {
            return false;
        }
        if self
            .guild_ids
            .as_ref()
            .is_some_and(|ids| !guild.is_some_and(|guild| ids.iter().any(|id| id == guild)))
        {
            return false;
        }
        !self.channel_ids.as_ref().is_some_and(|ids| {
            !ids.iter()
                .any(|id| id == channel || (self.include_threads && Some(id.as_str()) == parent))
        })
    }
    fn target(&self, target: &AccountTarget) -> bool {
        self.allows(
            &target.channel_id,
            target.guild_id.as_deref(),
            target.parent_id.as_deref(),
            target.kind,
        )
    }
    fn channel(&self, channel: &Channel) -> bool {
        self.allows(
            &channel.id,
            channel.guild_id.as_deref(),
            channel.parent_id.as_deref(),
            channel.kind,
        )
    }
}

struct HttpAttemptBudget {
    limit: u32,
    reserved: u32,
}
impl HttpAttemptBudget {
    fn new(limit: u32) -> Self {
        Self { limit, reserved: 0 }
    }
    fn reserve(&mut self, logical_requests: u32) -> bool {
        let cost =
            logical_requests.saturating_mul(discord_api::DiscordClient::max_attempts_per_request());
        if cost > self.limit.saturating_sub(self.reserved) {
            return false;
        }
        self.reserved += cost;
        true
    }
}

/// Apply durable user exclusions without clearing them through a new job scope.
pub(crate) fn merge_persistent_exclusions(
    store: &Store,
    user: &str,
    options: &mut AccountSyncOptions,
) -> Result<(), StoreError> {
    let (guilds, channels) = store.account_exclusions(user)?;
    for guild in guilds {
        if !options.scope.exclude_guild_ids.contains(&guild) {
            options.scope.exclude_guild_ids.push(guild);
        }
    }
    for channel in channels {
        if !options.scope.exclude_channel_ids.contains(&channel) {
            options.scope.exclude_channel_ids.push(channel);
        }
    }
    Ok(())
}

/// Cache-only preview. Unknown discovery cost and permissions remain unknown.
pub(crate) fn preview_account_sync(
    store: &Store,
    user: &str,
    options: &AccountSyncOptions,
) -> Result<Value, StoreError> {
    let mut options = options.clone();
    merge_persistent_exclusions(store, user, &mut options)?;
    let all = store.account_targets(user)?;
    let selected: Vec<_> = all
        .iter()
        .filter(|target| options.scope.target(target))
        .collect();
    Ok(
        json!({"dry_run":true,"job_id":null,"network_requests":0,"state_changes":0,"metadata_reading":"cache_only","scope":options.scope,"known_targets":all.len(),"selected_targets":selected.len(),"excluded_or_out_of_scope":all.len()-selected.len(),"unknown_discovery_targets":null,"permission_status":"unknown","complete":false,"budgets":{"max_targets":options.max_targets,"max_discovery_http_requests":options.max_discovery_http_requests,"max_body_http_requests":options.max_body_http_requests,"max_messages":options.max_messages,"max_duration_seconds":options.max_duration_seconds,"concurrency":1,"reservation":"maximum_client_retry_attempts"}}),
    )
}

#[derive(Debug, thiserror::Error)]
#[error("account synchronization failed: {data}")]
pub(crate) struct AccountSyncError {
    data: Value,
}
impl AccountSyncError {
    pub(crate) fn payload(&self) -> Value {
        self.data.clone()
    }
}
impl From<StoreError> for AccountSyncError {
    fn from(error: StoreError) -> Self {
        Self {
            data: json!({"error_source":"cache","code":error.code(),"retryable":error.retryable(),"message":"account synchronization cache operation failed"}),
        }
    }
}

#[async_trait::async_trait]
trait AccountSource: Send + Sync {
    async fn page(&self, target: &AccountTarget, limit: u8) -> Result<Vec<Message>, Value>;
}

trait AccountSyncState: Send + Sync {
    fn targets(&self, user: &str, at: &str, limit: u32) -> Result<Vec<AccountTarget>, StoreError>;
    fn withheld(&self, _user: &str, _channel: &str) -> Result<bool, StoreError> {
        Ok(false)
    }
    fn attempt(&self, user: &str, channel: &str, at: &str) -> Result<(), StoreError>;
    fn save_page(
        &self,
        user: &str,
        channel: &str,
        messages: &[Message],
        checkpoint: TargetCheckpoint,
    ) -> Result<(), StoreError>;
    fn fail(&self, user: &str, channel: &str, at: &str, error: &Value) -> Result<(), StoreError>;
    fn cancelled(&self, job: &str) -> Result<bool, StoreError>;
    fn progress(&self, job: &AccountJob) -> Result<(), StoreError>;
}

struct SqliteAccountState {
    store: Arc<Store>,
}
impl AccountSyncState for SqliteAccountState {
    fn targets(&self, user: &str, at: &str, limit: u32) -> Result<Vec<AccountTarget>, StoreError> {
        self.store.due_account_targets(user, at, limit)
    }
    fn withheld(&self, user: &str, channel: &str) -> Result<bool, StoreError> {
        Ok(self.store.target_sync_withheld(user, channel)?
            || (self.store.has_execution_restrictions()?
                && !self.store.target_execution_scope_known(user, channel)?))
    }
    fn attempt(&self, user: &str, channel: &str, at: &str) -> Result<(), StoreError> {
        self.store.record_account_attempt(user, channel, at)
    }
    fn save_page(
        &self,
        user: &str,
        channel: &str,
        messages: &[Message],
        checkpoint: TargetCheckpoint,
    ) -> Result<(), StoreError> {
        self.store
            .save_account_page(user, channel, messages, &checkpoint)
    }
    fn fail(&self, user: &str, channel: &str, at: &str, error: &Value) -> Result<(), StoreError> {
        self.store.fail_account_target(user, channel, at, error)
    }
    fn cancelled(&self, job: &str) -> Result<bool, StoreError> {
        self.store.account_cancel_requested(job)
    }
    fn progress(&self, job: &AccountJob) -> Result<(), StoreError> {
        self.store.save_account_job(job)
    }
}

struct DiscordAccountSource {
    client: SharedDiscordClient,
}

#[async_trait::async_trait]
trait GuildInventorySource: Send + Sync {
    async fn guild_page(&self, after: Option<String>) -> Result<Vec<Guild>, Value>;
}

#[async_trait::async_trait]
impl GuildInventorySource for DiscordAccountSource {
    async fn guild_page(&self, after: Option<String>) -> Result<Vec<Guild>, Value> {
        let request = DiscordRequest::ListGuilds {
            limit: 200,
            before: None,
            after,
        };
        let raw = self
            .client
            .execute_for(&request)
            .await
            .map_err(|error| json!(error))?;
        serde_json::from_value(raw).map_err(|_|json!({"code":"invalid_response","retryable":false,"operation":request.bucket_key()}))
    }
}

#[cfg(test)]
struct GuildInventory {
    guilds: Vec<Guild>,
    exhausted: bool,
    resume_after: Option<String>,
    error: Option<Value>,
}

#[cfg(test)]
async fn collect_guild_inventory(
    source: &dyn GuildInventorySource,
    store: &Store,
    job: &str,
    budget: &mut HttpAttemptBudget,
) -> Result<GuildInventory, AccountSyncError> {
    let mut result = GuildInventory {
        guilds: vec![],
        exhausted: false,
        resume_after: None,
        error: None,
    };
    let mut seen = std::collections::HashSet::new();
    for _ in 0..1000 {
        if store.account_cancel_requested(job)? {
            return Ok(result);
        }
        if !budget.reserve(1) {
            result.error = Some(json!({"code":"discovery_http_budget_exhausted","retryable":true}));
            return Ok(result);
        }
        let page = source.guild_page(result.resume_after.clone()).await;
        if store.account_cancel_requested(job)? {
            return Ok(result);
        }
        let page = match page {
            Ok(page) => page,
            Err(error) => {
                result.error = Some(error);
                return Ok(result);
            }
        };
        let invalid = page.len() > 200
            || page.iter().any(|guild| {
                let id = snowflake(&guild.id);
                id.is_none()
                    || !seen.insert(guild.id.clone())
                    || result
                        .resume_after
                        .as_deref()
                        .is_some_and(|after| id <= snowflake(after))
            });
        if invalid {
            result.error = Some(
                json!({"code":"invalid_response","retryable":false,"message":"guild inventory page has invalid identifiers, cursor progression, or duplicates"}),
            );
            return Ok(result);
        }
        let exhausted = page.len() < 200;
        if let Some(high) = page.iter().max_by_key(|guild| snowflake(&guild.id)) {
            result.resume_after = Some(high.id.clone());
        }
        result.guilds.extend(page);
        if exhausted {
            result.exhausted = true;
            result.resume_after = None;
            return Ok(result);
        }
    }
    result.error = Some(json!({"code":"inventory_page_limit","retryable":true}));
    Ok(result)
}
#[async_trait::async_trait]
impl AccountSource for DiscordAccountSource {
    async fn page(&self, target: &AccountTarget, limit: u8) -> Result<Vec<Message>, Value> {
        let request = message_page_request(target, limit);
        let raw = self
            .client
            .execute_for(&request)
            .await
            .map_err(|error| json!(error))?;
        let array=raw.as_array().ok_or_else(||json!({"code":"invalid_response","retryable":false,"message":"message page is not an array"}))?;
        let mut messages = Vec::new();
        for value in array {
            messages.push(parse_message(value.clone()).map_err(|error| error.error_payload())?);
        }
        Ok(messages)
    }
}

fn message_page_request(target: &AccountTarget, limit: u8) -> DiscordRequest {
    let backfill = target.next_direction == "backfill";
    DiscordRequest::GetMessages {
        channel_id: target.channel_id.clone(),
        limit,
        before: if backfill {
            target.backfill_before.clone()
        } else {
            None
        },
        after: if backfill {
            None
        } else {
            Some(target.increment_after.clone().unwrap_or_else(|| "0".into()))
        },
        around: None,
    }
}

/// Persist a pending job before the application spawns its worker.
pub(crate) fn create_account_job(store: &Store, id: &str, user: &str) -> Result<Value, StoreError> {
    let at = now_iso();
    let job = AccountJob {
        job_id: id.into(),
        account_user_id: user.into(),
        generation: String::new(),
        status: "pending".into(),
        created_at: at.clone(),
        updated_at: at,
        cancel_requested: false,
        progress: json!({"job_id":id,"scope":"account","account_user_id":user,"status":"pending","complete":false}),
    };
    store.save_account_job(&job)?;
    Ok(job.progress)
}

/// Observe inventory and perform one fair bounded retrieval round.
pub(crate) async fn run_account_sync(
    client: SharedDiscordClient,
    store: Arc<Store>,
    id: String,
    user: String,
    mut options: AccountSyncOptions,
) -> Result<Value, AccountSyncError> {
    let budget = options
        .execution_timeout
        .unwrap_or_else(|| std::time::Duration::from_secs(options.max_duration_seconds.into()));
    let deadline = tokio::time::Instant::now() + budget;
    options.execution_deadline = Some(deadline);
    let result = match tokio::time::timeout_at(
        deadline,
        run_account_sync_inner(client, Arc::clone(&store), id.clone(), user, options),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(AccountSyncError {
            data: json!({"error_source":"transport","code":"budget_timeout","retryable":true,"complete":false}),
        }),
    };
    if let Err(error) = &result {
        if let Some(mut job) = store.account_job(&id)? {
            if error.data["code"] == "budget_timeout" {
                if let Some(channel) = job.progress["active_channel_id"].as_str() {
                    store.fail_account_target(
                        &job.account_user_id,
                        channel,
                        &now_iso(),
                        &error.data,
                    )?;
                }
            }
            job.status = if error.data["code"] == "budget_timeout" {
                "paused"
            } else {
                "failed"
            }
            .into();
            job.updated_at = now_iso();
            job.progress["status"] = json!(job.status);
            job.progress["error"] = error.payload();
            job.progress["as_of"] = json!(job.updated_at);
            store.save_account_job(&job)?;
        }
    }
    result
}

async fn run_account_sync_inner(
    client: SharedDiscordClient,
    store: Arc<Store>,
    id: String,
    user: String,
    mut options: AccountSyncOptions,
) -> Result<Value, AccountSyncError> {
    let mut job = store.account_job(&id)?.ok_or_else(|| AccountSyncError {
        data: json!({"code":"unknown_job","retryable":false}),
    })?;
    if job.account_user_id != user {
        return Err(AccountSyncError {
            data: json!({"code":"account_mismatch","retryable":false}),
        });
    }
    merge_persistent_exclusions(&store, &user, &mut options)?;
    job.progress["target_scope"] = json!(options.scope);
    job.progress["identity_http_attempt_upper_bound"] = json!(options.identity_http_reservation);
    job.progress["budgets"] = json!({"max_discovery_http_requests":options.max_discovery_http_requests,"max_body_http_requests":options.max_body_http_requests,"max_messages":options.max_messages,"max_duration_seconds":options.max_duration_seconds,"concurrency":1});
    store.save_account_job(&job)?;
    if options.refresh_inventory && !store.account_cancel_requested(&id)? {
        job.status = "running".into();
        job.updated_at = now_iso();
        store.save_account_job(&job)?;
        job.generation = refresh_inventory(&client, &store, &id, &user, &options).await?;
    } else if let Some(inventory) = store.account_inventory(&user)? {
        job.generation = inventory.generation;
    }
    let source = DiscordAccountSource { client };
    let state = SqliteAccountState {
        store: Arc::clone(&store),
    };
    let mut report = run_round(&state, &source, job, &options).await?;
    report["coverage"] = account_coverage(&store, &user)?;
    if let Some(mut saved) = store.account_job(&id)? {
        saved.progress = report.clone();
        store.save_account_job(&saved)?;
    }
    Ok(report)
}

async fn run_round(
    state: &dyn AccountSyncState,
    source: &dyn AccountSource,
    mut job: AccountJob,
    options: &AccountSyncOptions,
) -> Result<Value, AccountSyncError> {
    let deadline = options.execution_deadline.unwrap_or_else(|| {
        tokio::time::Instant::now()
            + options.execution_timeout.unwrap_or_else(|| {
                std::time::Duration::from_secs(options.max_duration_seconds.into())
            })
    });
    let at = now_iso();
    let targets: Vec<_> = state
        .targets(&job.account_user_id, &at, u32::MAX)?
        .into_iter()
        .filter(|target| options.scope.target(target))
        .collect();
    let mut body_budget = HttpAttemptBudget::new(options.max_body_http_requests);
    let mut selected = 0;
    let mut failures = Vec::new();
    let mut withheld_targets = Vec::new();
    let mut attempted = 0;
    let mut saved = 0;
    let mut messages_saved = 0;
    job.status = "running".into();
    for target in &targets {
        if attempted >= options.max_targets {
            break;
        }
        if state.cancelled(&job.job_id)? {
            job.status = "cancelled".into();
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            job.status = "paused".into();
            break;
        }
        if state.withheld(&job.account_user_id, &target.channel_id)? {
            withheld_targets.push(target.channel_id.clone());
            continue;
        }
        selected += 1;
        let remaining_messages = options.max_messages.saturating_sub(messages_saved as u32);
        if tokio::time::Instant::now() >= deadline
            || remaining_messages == 0
            || !body_budget.reserve(1)
        {
            job.status = "paused".into();
            break;
        }
        let limit = options.page_size.min(remaining_messages.min(100) as u8);
        let attempt_at = now_iso();
        state.attempt(&job.account_user_id, &target.channel_id, &attempt_at)?;
        attempted += 1;
        job.progress["active_channel_id"] = json!(target.channel_id);
        job.updated_at = attempt_at.clone();
        state.progress(&job)?;
        if tokio::time::Instant::now() >= deadline {
            job.status = "paused".into();
            break;
        }
        let result = match tokio::time::timeout_at(deadline, source.page(target, limit)).await {
            Ok(result) => result,
            Err(_) => {
                job.status = "paused".into();
                Err(json!({"error_source":"transport","code":"budget_timeout","retryable":true}))
            }
        };
        if state.cancelled(&job.job_id)? {
            job.status = "cancelled".into();
            break;
        }
        match result.and_then(|messages| validate_page(target, messages, limit)) {
            Ok(messages) => {
                let oldest = messages.last().map(|m| m.id.clone());
                let newest = messages.first().map(|m| m.id.clone());
                let backfill = target.next_direction == "backfill";
                let exhausted = messages.len() < limit as usize;
                let history_pending =
                    matches!(target.history_status.as_str(), "unfetched" | "partial");
                let page_interval = oldest
                    .as_ref()
                    .zip(newest.as_ref())
                    .map(|(oldest, newest)| {
                        if backfill {
                            (
                                oldest.clone(),
                                target
                                    .backfill_before
                                    .as_deref()
                                    .and_then(snowflake)
                                    .and_then(|before| before.checked_sub(1))
                                    .map(|id| id.to_string())
                                    .unwrap_or_else(|| newest.clone()),
                            )
                        } else {
                            (
                                target
                                    .increment_after
                                    .as_deref()
                                    .and_then(snowflake)
                                    .and_then(|after| after.checked_add(1))
                                    .map(|id| id.to_string())
                                    .unwrap_or_else(|| oldest.clone()),
                                newest.clone(),
                            )
                        }
                    });
                let initial_latest = target.history_status == "unfetched"
                    && target.backfill_before.is_none()
                    && target.increment_after.is_none()
                    && target.last_checked_at.is_none();
                let high = if backfill && !initial_latest {
                    target.increment_after.clone()
                } else {
                    match (&target.increment_after, newest) {
                        (Some(previous), Some(next)) => {
                            Some(if snowflake(previous) > snowflake(&next) {
                                previous.clone()
                            } else {
                                next
                            })
                        }
                        (previous, next) => next.or_else(|| previous.clone()),
                    }
                };
                let checkpoint = TargetCheckpoint {
                    page_interval,
                    backfill_before: if backfill {
                        oldest.or_else(|| target.backfill_before.clone())
                    } else {
                        target.backfill_before.clone()
                    },
                    increment_after: high,
                    checked_at: now_iso(),
                    next_check_at: if !exhausted || (history_pending && !backfill) {
                        Some(now_iso())
                    } else {
                        Some(
                            (Utc::now() + Duration::minutes(5))
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        )
                    },
                    history_status: if backfill && exhausted {
                        "history_access_unknown".into()
                    } else if history_pending {
                        "partial".into()
                    } else {
                        target.history_status.clone()
                    },
                    next_direction: if !backfill && history_pending {
                        "backfill"
                    } else {
                        "incremental"
                    }
                    .into(),
                };
                if tokio::time::Instant::now() >= deadline {
                    job.status = "paused".into();
                    break;
                }
                state.save_page(
                    &job.account_user_id,
                    &target.channel_id,
                    &messages,
                    checkpoint,
                )?;
                saved += 1;
                messages_saved += messages.len();
            }
            Err(error) => {
                state.fail(
                    &job.account_user_id,
                    &target.channel_id,
                    &attempt_at,
                    &error,
                )?;
                failures.push(json!({"channel_id":target.channel_id,"error":error}));
            }
        }
        job.updated_at = now_iso();
        job.progress = json!({"job_id":job.job_id,"scope":"account","account_user_id":job.account_user_id,"generation":job.generation,"as_of":job.updated_at,"status":"running","selected_targets":selected,"candidate_targets":targets.len(),"attempted":attempted,"synced":saved,"messages_saved":messages_saved,"failures":failures.iter().take(20).map(failure_summary).collect::<Vec<_>>(),"failures_total":failures.len(),"failures_omitted":failures.len().saturating_sub(20),"complete":false});
        state.progress(&job)?;
        if job.status == "paused" {
            break;
        }
    }
    if state.cancelled(&job.job_id)? {
        job.status = "cancelled".into();
    }
    if job.status != "cancelled" && tokio::time::Instant::now() >= deadline {
        job.status = "paused".into();
    }
    if job.status != "cancelled" && job.status != "paused" {
        job.status = if failures.is_empty() && withheld_targets.is_empty() {
            "finished"
        } else {
            "partial"
        }
        .into();
    }
    job.updated_at = now_iso();
    job.progress = json!({"job_id":job.job_id,"scope":"account","account_user_id":job.account_user_id,"generation":job.generation,"as_of":job.updated_at,"status":job.status,"selected_targets":selected,"candidate_targets":targets.len(),"attempted":attempted,"synced":saved,"messages_saved":messages_saved,"failures":failures.iter().take(20).map(failure_summary).collect::<Vec<_>>(),"failures_total":failures.len(),"failures_omitted":failures.len().saturating_sub(20),"cancel_reason":if job.status=="cancelled"{Some("cancellation_requested")}else{None},"resume_cursor":{"account_user_id":job.account_user_id,"generation":job.generation,"source":"persisted_target_checkpoints"},"complete":false});
    job.progress["withheld_count"] = json!(withheld_targets.len());
    job.progress["withheld_targets"] = json!(withheld_targets.iter().take(20).collect::<Vec<_>>());
    job.progress["withheld_omitted"] = json!(withheld_targets.len().saturating_sub(20));
    job.progress["page_complete"] =
        json!(job.status == "finished" && saved == selected && withheld_targets.is_empty());
    job.progress["scope_complete"] = json!(false);
    job.progress["inventory_complete"] = json!(false);
    job.progress["history_complete"] = json!(false);
    job.progress["continuous_coverage"] = json!(false);
    job.progress["target_scope"] = json!(options.scope);
    job.progress["body_http_attempt_upper_bound"] = json!(body_budget.reserved);
    job.progress["identity_http_attempt_upper_bound"] = json!(options.identity_http_reservation);
    job.progress["budget_accounting"] = json!("conservative_maximum_client_retry_reservation");
    job.progress["cancel_actor"] = json!("unknown");
    state.progress(&job)?;
    Ok(job.progress)
}

fn snowflake(id: &str) -> Option<u64> {
    id.parse().ok()
}
fn validate_page(
    target: &AccountTarget,
    mut messages: Vec<Message>,
    limit: u8,
) -> Result<Vec<Message>, Value> {
    let mut seen = std::collections::HashSet::new();
    let backfill = target.next_direction == "backfill";
    let invalid = messages.len() > limit as usize
        || messages.iter().any(|message| {
            let id = snowflake(&message.id);
            message.channel_id != target.channel_id
                || id.is_none_or(|id| id == 0)
                || !seen.insert(message.id.clone())
                || (backfill
                    && target
                        .backfill_before
                        .as_deref()
                        .is_some_and(|before| id >= snowflake(before)))
                || (!backfill
                    && target
                        .increment_after
                        .as_deref()
                        .is_some_and(|after| id <= snowflake(after)))
        });
    if invalid {
        return Err(
            json!({"code":"invalid_response","retryable":false,"message":"message page has invalid identity, boundaries, or duplicates"}),
        );
    }
    messages.sort_by_key(|message| std::cmp::Reverse(snowflake(&message.id)));
    Ok(messages)
}

async fn fetch_channels(
    client: &SharedDiscordClient,
    request: DiscordRequest,
) -> Result<Vec<Channel>, Value> {
    let raw = client
        .execute_for(&request)
        .await
        .map_err(|error| json!(error))?;
    serde_json::from_value(raw).map_err(
        |_| json!({"code":"invalid_response","retryable":false,"operation":request.bucket_key()}),
    )
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
enum DiscoveryTask {
    GuildPage {
        after: Option<String>,
    },
    Dms,
    GuildChannels {
        guild_id: String,
    },
    Channel {
        channel_id: String,
    },
    Threads {
        parent: Box<Channel>,
        cursor: Option<String>,
    },
}
impl DiscoveryTask {
    fn resource_key(&self) -> String {
        match self {
            Self::GuildPage { .. } => "guild_inventory".into(),
            Self::Dms => "dm_inventory".into(),
            Self::GuildChannels { guild_id } => format!("guild:{guild_id}"),
            Self::Channel { channel_id } => format!("channel:{channel_id}"),
            Self::Threads { parent, .. } => format!("thread_parent:{}", parent.id),
        }
    }
    fn requests(&self) -> u32 {
        if matches!(self, Self::Threads { .. }) {
            2
        } else {
            1
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
struct DiscoveryCheckpoint {
    pending: std::collections::VecDeque<DiscoveryTask>,
    completed_at: Option<String>,
    failures: Vec<Value>,
    #[serde(default)]
    blocked: Vec<String>,
}
impl DiscoveryCheckpoint {
    fn initial(scope: &AccountScope) -> Self {
        let mut pending = std::collections::VecDeque::new();
        if let Some(ids) = &scope.channel_ids {
            pending.extend(
                ids.iter()
                    .filter(|id| !scope.exclude_channel_ids.contains(id))
                    .map(|id| DiscoveryTask::Channel {
                        channel_id: id.clone(),
                    }),
            );
        } else if let Some(ids) = &scope.guild_ids {
            pending.extend(
                ids.iter()
                    .filter(|id| !scope.exclude_guild_ids.contains(id))
                    .map(|id| DiscoveryTask::GuildChannels {
                        guild_id: id.clone(),
                    }),
            );
        } else {
            pending.push_back(DiscoveryTask::GuildPage { after: None });
            if scope.include_dms {
                pending.push_back(DiscoveryTask::Dms);
            }
        }
        Self {
            pending,
            completed_at: None,
            failures: vec![],
            blocked: vec![],
        }
    }
}

#[async_trait::async_trait]
trait InventorySource: Send + Sync {
    async fn guilds(&self, after: Option<String>) -> Result<Vec<Guild>, Value>;
    async fn channels(&self, request: DiscordRequest) -> Result<Vec<Channel>, Value>;
    async fn threads(
        &self,
        parent: &Channel,
        cursor: Option<&str>,
    ) -> Result<crate::thread_listing::ThreadPage, Value>;
}
#[async_trait::async_trait]
impl InventorySource for DiscordAccountSource {
    async fn guilds(&self, after: Option<String>) -> Result<Vec<Guild>, Value> {
        self.guild_page(after).await
    }
    async fn channels(&self, request: DiscordRequest) -> Result<Vec<Channel>, Value> {
        if matches!(request, DiscordRequest::GetChannel { .. }) {
            let raw = self
                .client
                .execute_for(&request)
                .await
                .map_err(|error| json!(error))?;
            serde_json::from_value(raw)
                .map(|channel| vec![channel])
                .map_err(|_| json!({"code":"invalid_response","retryable":false}))
        } else {
            fetch_channels(&self.client, request).await
        }
    }
    async fn threads(
        &self,
        parent: &Channel,
        cursor: Option<&str>,
    ) -> Result<crate::thread_listing::ThreadPage, Value> {
        let query = ThreadQuery::new(
            Some(&parent.id),
            parent.guild_id.as_deref(),
            Some("all"),
            None,
            25,
            cursor,
        )
        .map_err(|error| error.payload())?;
        DiscordThreadListing::new(Arc::clone(&self.client))
            .list(&query)
            .await
            .map_err(|error| error.payload())
    }
}
fn discovery_scope_key(scope: &AccountScope) -> String {
    let mut value = json!(scope);
    for field in [
        "guild_ids",
        "channel_ids",
        "exclude_guild_ids",
        "exclude_channel_ids",
    ] {
        if let Some(ids) = value[field].as_array_mut() {
            ids.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
            ids.dedup();
        }
    }
    format!("inventory-v1:{value}")
}
async fn refresh_inventory(
    client: &SharedDiscordClient,
    store: &Store,
    job: &str,
    user: &str,
    options: &AccountSyncOptions,
) -> Result<String, AccountSyncError> {
    discover_inventory(
        &DiscordAccountSource {
            client: Arc::clone(client),
        },
        store,
        job,
        user,
        options,
    )
    .await
}
fn discovery_requires_explicit_resume(error: &Value) -> bool {
    matches!(error["http_status"].as_u64(), Some(401 | 403))
        || matches!(
            error["code"].as_str(),
            Some(
                "execution_withheld"
                    | "permission_denied"
                    | "access_denied"
                    | "unsupported_scope"
                    | "unsupported_filter"
                    | "unsupported_channel_kind"
            )
        )
        || (error["error_source"] == "discord"
            && matches!(error["discord_code"].as_u64(), Some(50001 | 50013 | 20002)))
}

async fn discover_inventory(
    source: &dyn InventorySource,
    store: &Store,
    job: &str,
    user: &str,
    options: &AccountSyncOptions,
) -> Result<String, AccountSyncError> {
    let deadline = options.execution_deadline.unwrap_or_else(|| {
        tokio::time::Instant::now()
            + options
                .execution_timeout
                .unwrap_or(std::time::Duration::from_secs(u64::from(
                    options.max_duration_seconds,
                )))
    });
    let key = discovery_scope_key(&options.scope);
    let mut checkpoint = match store.account_discovery_checkpoint(user, &key)? {
        Some(value) => serde_json::from_value::<DiscoveryCheckpoint>(value)
            .map_err(|_| StoreError::Data("invalid persisted discovery checkpoint".into()))?,
        None => DiscoveryCheckpoint::initial(&options.scope),
    };
    if checkpoint
        .completed_at
        .as_deref()
        .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
        .is_some_and(|at| Utc::now().signed_duration_since(at) > Duration::minutes(15))
    {
        let failures = std::mem::take(&mut checkpoint.failures);
        let blocked = std::mem::take(&mut checkpoint.blocked);
        checkpoint = DiscoveryCheckpoint::initial(&options.scope);
        checkpoint.failures = failures;
        checkpoint.blocked = blocked;
    }
    let generation = format!("inventory:{job}");
    store.begin_account_inventory(user, &generation, &now_iso())?;
    let mut budget = HttpAttemptBudget::new(options.max_discovery_http_requests);
    budget.reserved = options.identity_http_reservation;
    let mut visited = std::collections::HashSet::new();
    let mut budget_blocked = Vec::new();
    while let Some(task) = checkpoint.pending.pop_front() {
        let task_key = serde_json::to_string(&task)
            .map_err(|_| StoreError::Data("invalid discovery task".into()))?;
        let resource_key = task.resource_key();
        if store
            .account_discovery_blocked_operations(user)?
            .contains(&resource_key)
            || checkpoint.blocked.contains(&resource_key)
        {
            continue;
        }
        let (excluded_guilds, excluded_channels) = store.account_exclusions(user)?;
        let withheld = match &task {
            DiscoveryTask::GuildChannels { guild_id } => excluded_guilds.contains(guild_id),
            DiscoveryTask::Channel { channel_id } => {
                excluded_channels.contains(channel_id)
                    || store.target_sync_withheld(user, channel_id)?
            }
            DiscoveryTask::Threads { parent, .. } => {
                excluded_channels.contains(&parent.id)
                    || parent
                        .guild_id
                        .as_ref()
                        .is_some_and(|id| excluded_guilds.contains(id))
                    || store.target_sync_withheld(user, &parent.id)?
            }
            _ => false,
        };
        if withheld {
            checkpoint.failures.push(json!({"source":"discovery","code":"execution_withheld","retryable":false,"resource":resource_key}));
            checkpoint.blocked.push(resource_key);
            continue;
        }

        if !visited.insert(task_key.clone()) || store.account_cancel_requested(job)? {
            checkpoint.pending.push_front(task);
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            checkpoint.pending.push_front(task);
            budget_blocked.push(json!({"code":"execution_time_budget_exhausted","retryable":true}));
            break;
        }
        if !budget.reserve(task.requests()) {
            budget_blocked.push(json!({"code":"insufficient_discovery_budget","required_http_attempt_upper_bound":task.requests()*discord_api::DiscordClient::max_attempts_per_request(),"configured_http_attempt_cap":options.max_discovery_http_requests,"operation":match &task{DiscoveryTask::Threads{..}=>"threads",_=>"inventory"}}));
            checkpoint.pending.push_back(task);
            // Rotation is persisted even when the next operation cannot fit this job.
            if budget.reserved >= budget.limit {
                break;
            } else {
                continue;
            }
        }
        let result: Result<Vec<Channel>, Value> = match &task {
            DiscoveryTask::GuildPage { after } => match source.guilds(after.clone()).await {
                Ok(guilds) => {
                    let last = guilds.last().map(|guild| guild.id.clone());
                    if guilds.iter().any(|guild| {
                        guild
                            .id
                            .parse::<u64>()
                            .ok()
                            .filter(|id| {
                                *id > 0
                                    && after
                                        .as_deref()
                                        .and_then(snowflake)
                                        .is_none_or(|after| *id > after)
                            })
                            .is_none()
                    }) || (guilds.len() == 200
                        && last.as_deref().is_none_or(|last| {
                            snowflake(last).unwrap_or(0)
                                <= after.as_deref().and_then(snowflake).unwrap_or(0)
                        }))
                    {
                        Err(json!({"code":"invalid_guild_pagination","retryable":false}))
                    } else {
                        for guild in &guilds {
                            if !options.scope.exclude_guild_ids.contains(&guild.id) {
                                checkpoint.pending.push_back(DiscoveryTask::GuildChannels {
                                    guild_id: guild.id.clone(),
                                });
                            }
                        }
                        if guilds.len() == 200 {
                            checkpoint
                                .pending
                                .push_back(DiscoveryTask::GuildPage { after: last });
                        }
                        Ok(vec![])
                    }
                }
                Err(error) => Err(error),
            },
            DiscoveryTask::Dms => source.channels(DiscordRequest::ListUserDmChannels).await,
            DiscoveryTask::GuildChannels { guild_id } => source
                .channels(DiscordRequest::GetGuildChannels {
                    guild_id: guild_id.clone(),
                })
                .await
                .map(|mut channels| {
                    for channel in &mut channels {
                        channel.guild_id = Some(guild_id.clone());
                    }
                    channels
                }),
            DiscoveryTask::Channel { channel_id } => {
                source
                    .channels(DiscordRequest::GetChannel {
                        channel_id: channel_id.clone(),
                    })
                    .await
            }
            DiscoveryTask::Threads { parent, cursor } => {
                if store.target_sync_withheld(user, &parent.id)? {
                    Err(json!({"code":"execution_withheld","retryable":false}))
                } else {
                    match source.threads(parent, cursor.as_deref()).await {
                        Ok(page) => {
                            if let Some(cursor) = page.next_cursor {
                                checkpoint.pending.push_back(DiscoveryTask::Threads {
                                    parent: parent.clone(),
                                    cursor: Some(cursor),
                                });
                            }
                            if page.search_window_exhausted {
                                checkpoint.failures.push(json!({"source":"threads","parent_id":parent.id,"code":"search_window_exhausted","retryable":false}));
                            }
                            Ok(page.threads)
                        }
                        Err(error) => Err(error),
                    }
                }
            }
        };
        match result {
            Ok(mut channels) => {
                channels.retain(|channel| options.scope.channel(channel));
                for channel in &channels {
                    store.upsert_channel(channel)?;
                    if options.scope.include_threads
                        && !matches!(task, DiscoveryTask::Threads { .. })
                        && matches!(channel.kind, 0 | 5 | 15 | 16)
                    {
                        checkpoint.pending.push_back(DiscoveryTask::Threads {
                            parent: Box::new(channel.clone()),
                            cursor: None,
                        });
                    }
                }
                store.observe_account_targets(user, &generation, &channels, &now_iso())?;
            }
            Err(error) => {
                let (guild_id, channel_id, parent_id) = match &task {
                    DiscoveryTask::GuildChannels { guild_id } => {
                        (Some(guild_id.as_str()), None, None)
                    }
                    DiscoveryTask::Channel { channel_id } => {
                        (None, Some(channel_id.as_str()), None)
                    }
                    DiscoveryTask::Threads { parent, .. } => {
                        (parent.guild_id.as_deref(), None, Some(parent.id.as_str()))
                    }
                    _ => (None, None, None),
                };
                checkpoint.failures.push(json!({"source":"discovery","guild_id":guild_id,"channel_id":channel_id,"parent_id":parent_id,"resource":resource_key,"task":task,"error":error}));
                if discovery_requires_explicit_resume(&error) {
                    checkpoint.blocked.push(resource_key);
                } else if error["retryable"].as_bool() == Some(true) {
                    checkpoint.pending.push_back(task);
                }
            }
        }
        checkpoint.failures.truncate(100);
        store.save_account_discovery_checkpoint(user, &key, &json!(checkpoint), &now_iso())?;
    }
    if checkpoint.pending.is_empty() && checkpoint.completed_at.is_none() {
        checkpoint.completed_at = Some(now_iso());
    }
    store.save_account_discovery_checkpoint(user, &key, &json!(checkpoint), &now_iso())?;
    store.finish_account_inventory(&generation,&now_iso(),&json!({"scope":"account","complete":false,"cancelled":store.account_cancel_requested(job)?,"target_scope":options.scope,"discovery_http_attempt_upper_bound":budget.reserved,"budget_accounting":"conservative_maximum_client_retry_reservation","discovery_cycle_completed_at":checkpoint.completed_at,"pending_discovery_operations":checkpoint.pending.len(),"blocked_discovery_resources":store.account_discovery_blocked_operations(user)?,"discovery_checkpoint_scope":key,"failures":checkpoint.failures,"budget_blocked":budget_blocked,"reasons":["inventory_discovery_not_proven_exhaustive","thread_search_index_not_exhaustive","history_permissions_not_verified"]}))?;
    Ok(generation)
}

fn failure_summary(failure: &Value) -> Value {
    let error = failure.get("error").unwrap_or(failure);
    json!({"source":failure["source"].as_str().map(|s|s.chars().take(64).collect::<String>()),"channel_id":failure["channel_id"].as_str(),"guild_id":failure["guild_id"].as_str(),"parent_id":failure["parent_id"].as_str(),"error":{"code":error["code"].as_str().map(|s|s.chars().take(128).collect::<String>()),"error_source":error["error_source"].as_str(),"http_status":error["http_status"].as_u64(),"retryable":error["retryable"].as_bool(),"retry_after_ms":error["retry_after_ms"].as_u64(),"message":error["message"].as_str().map(|s|s.chars().take(256).collect::<String>())}})
}

fn inventory_summary(inventory: &AccountInventory) -> Value {
    let discovery = &inventory.discovery;
    let inventory_errors = discovery["inventory_errors"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let displayed_inventory_errors: Vec<_> = inventory_errors
        .iter()
        .take(20)
        .filter_map(Value::as_str)
        .map(|s| s.chars().take(256).collect::<String>())
        .collect();
    let failures = discovery["failures"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let displayed: Vec<_> = failures.iter().take(20).map(failure_summary).collect();
    let reasons: Vec<_> = discovery["reasons"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[])
        .iter()
        .take(16)
        .filter_map(Value::as_str)
        .map(|s| s.chars().take(256).collect::<String>())
        .collect();
    json!({"generation":inventory.generation,"account_user_id":inventory.account_user_id,"started_at":inventory.started_at,"refreshed_at":inventory.refreshed_at,"status":inventory.status,"discovery":{"source":discovery["source"].as_str(),"partial":discovery["partial"].as_bool(),"inventory_errors":displayed_inventory_errors,"inventory_errors_total":inventory_errors.len(),"inventory_errors_omitted":inventory_errors.len().saturating_sub(20),"complete":false,"cancelled":discovery["cancelled"].as_bool(),"guilds_observed":discovery["guilds_observed"].as_u64(),"guild_inventory_exhausted":discovery["guild_inventory_exhausted"].as_bool(),"failures_total":failures.len(),"failures":displayed,"failures_omitted":failures.len().saturating_sub(20),"pending_discovery_operations":discovery["pending_discovery_operations"].as_u64(),"discovery_cycle_completed_at":discovery["discovery_cycle_completed_at"],"blocked_discovery_resource_count":discovery["blocked_discovery_resources"].as_array().map(Vec::len),"budget_blocked":discovery["budget_blocked"].as_array().map(|entries|entries.iter().take(20).cloned().collect::<Vec<_>>()),"thread_resume_cursor_count":discovery["thread_resume_cursors"].as_object().map(|map|map.len()),"reasons":reasons}})
}

fn coverage_summary(
    store: &Store,
    user: &str,
    guild: Option<&str>,
    channel: Option<&str>,
    as_of: &str,
) -> Result<Value, StoreError> {
    let counts = store.account_target_counts(user, guild, channel, as_of)?;
    let (gap_observations, unresolved_gaps) = store.account_gap_counts(user, guild, channel)?;
    let excluded_count = store.account_excluded_target_count(user, guild, channel)?;
    let inventory = store.account_inventory(user)?;
    let inventory_at = inventory
        .as_ref()
        .and_then(|entry| entry.refreshed_at.clone());
    let stale = inventory_at
        .as_deref()
        .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
        .is_none_or(|at| Utc::now().signed_duration_since(at) > Duration::minutes(15));
    let scope = if channel.is_some() {
        "channel"
    } else if guild.is_some() {
        "guild"
    } else {
        "account"
    };
    Ok(
        json!({"observation_consistency":"sequential_cache_reads","as_of_basis":"inspection_started_at","scope":scope,"target_scope":{"guild_id":guild,"channel_id":channel},"account_user_id":user,"as_of":as_of,"inventory_updated_at":inventory_at,"inventory_stale":stale,"inventory_initialized":inventory.is_some(),"inventory":inventory.as_ref().map(inventory_summary),"targets_total":counts.total,"target_count_basis":"retained_discovered_targets","undiscovered_target_count":null,"enumerated":counts.total,"synced":counts.synced,"unfetched":counts.unfetched,"failed":counts.failed,"blocked":counts.blocked,"due":counts.due,"targets":[],"details_included":false,"has_more":null,"next_cursor":null,"page_complete":true,"scope_complete":false,"inventory_complete":false,"history_complete":false,"continuous_coverage":false,"gap_observations":gap_observations,"unresolved_gaps":unresolved_gaps,"excluded_count":excluded_count,"excluded_count_basis":"retained_discovered_targets","complete":false,"reasons":["account_inventory_not_proven_exhaustive","history_permissions_not_verified","edits_and_deletions_not_continuously_observed"]}),
    )
}

/// Return bounded account summary counts without expanding retained targets.
pub(crate) fn account_coverage(store: &Store, user: &str) -> Result<Value, StoreError> {
    coverage_summary(store, user, None, None, &now_iso())
}

/// Return summary counts for the requested guild and channel intersection.
pub(crate) fn account_coverage_scoped(
    store: &Store,
    user: &str,
    guild: Option<&str>,
    channel: Option<&str>,
) -> Result<Value, StoreError> {
    coverage_summary(store, user, guild, channel, &now_iso())
}

/// Return scoped retrieval details with a bounded keyset continuation.
pub(crate) fn account_coverage_page(
    store: &Store,
    user: &str,
    guild: Option<&str>,
    channel: Option<&str>,
    after: Option<&str>,
    limit: u32,
) -> Result<Value, StoreError> {
    let limit = limit.clamp(1, 100);
    let mut report = coverage_summary(store, user, guild, channel, &now_iso())?;
    let generation = report["inventory"]["generation"].as_str();
    let mut page = store.account_target_page(user, guild, channel, after, limit)?;
    let has_more = page.len() > limit as usize;
    page.truncate(limit as usize);
    let next_cursor = if has_more {
        page.last().map(|target| target.channel_id.clone())
    } else {
        None
    };
    let mut entries = Vec::new();
    for target in &page {
        let mut ranges = store.account_target_ranges(&target.channel_id, 20)?;
        let ranges_truncated = ranges.len() > 20;
        ranges.truncate(20);
        let gaps:Vec<_>=ranges.windows(2).map(|pair|json!({"after_message_id":pair[0].to_id,"before_message_id":pair[1].from_id})).collect();
        let body = store.channel_sync(&target.channel_id)?;
        let body_success = body
            .as_ref()
            .and_then(|row| row.body_last_success_at.as_deref());
        let stale = body_success
            .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
            .is_none_or(|at| Utc::now().signed_duration_since(at) > Duration::minutes(5));
        entries.push(json!({"channel_id":target.channel_id,"guild_id":target.guild_id,"present_in_current_inventory":generation==Some(target.generation.as_str()),"first_discovered_at":target.first_discovered_at,"last_checked_at":target.last_checked_at,"account_checkpoint_checked_at":target.last_checked_at,"body_synced":body_success.is_some(),"last_body_success_at":body_success,"metadata_fetched_at":body.as_ref().and_then(|row|row.metadata_fetched_at.as_deref()),"unfetched_since":if body_success.is_none(){Some(&target.first_discovered_at)}else{None},"last_attempt_at":target.last_attempt_at,"next_check_at":target.next_check_at,"stale":stale,"history_status":target.history_status,"next_direction":target.next_direction,"covered_ranges":ranges,"ranges_truncated":ranges_truncated,"gaps":gaps,"covered_from":target.backfill_before,"covered_to":target.increment_after,"remaining_history":{"before_message_id":target.backfill_before,"status":"unknown"},"error":target.last_error.as_ref().map(|error|failure_summary(&json!({"error":error}))["error"].clone())}));
    }
    report["targets"] = json!(entries);
    report["details_included"] = json!(true);
    report["has_more"] = json!(has_more);
    report["next_cursor"] = json!(next_cursor);
    report["limit"] = json!(limit);
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };

    struct Source {
        messages: Vec<Message>,
    }
    #[async_trait::async_trait]
    impl AccountSource for Source {
        async fn page(&self, _: &AccountTarget, _: u8) -> Result<Vec<Message>, Value> {
            Ok(self.messages.clone())
        }
    }
    struct State {
        target: AccountTarget,
        saved: Mutex<Vec<TargetCheckpoint>>,
        cancelled: AtomicBool,
        save_failure: bool,
    }
    impl AccountSyncState for State {
        fn targets(&self, _: &str, _: &str, _: u32) -> Result<Vec<AccountTarget>, StoreError> {
            Ok(vec![self.target.clone()])
        }
        fn attempt(&self, _: &str, _: &str, _: &str) -> Result<(), StoreError> {
            Ok(())
        }
        fn save_page(
            &self,
            _: &str,
            _: &str,
            _: &[Message],
            checkpoint: TargetCheckpoint,
        ) -> Result<(), StoreError> {
            if self.save_failure {
                return Err(StoreError::Data("save failed".into()));
            }
            self.saved.lock().unwrap().push(checkpoint);
            Ok(())
        }
        fn fail(&self, _: &str, _: &str, _: &str, _: &Value) -> Result<(), StoreError> {
            Ok(())
        }
        fn cancelled(&self, _: &str) -> Result<bool, StoreError> {
            Ok(self.cancelled.load(Ordering::SeqCst))
        }
        fn progress(&self, _: &AccountJob) -> Result<(), StoreError> {
            Ok(())
        }
    }
    fn state(cancelled: bool) -> State {
        State {
            target: AccountTarget {
                account_user_id: "me".into(),
                channel_id: "1".into(),
                guild_id: None,
                parent_id: None,
                kind: 1,
                generation: "g".into(),
                first_discovered_at: "2026-01-01".into(),
                last_seen_at: "2026-01-01".into(),
                last_attempt_at: None,
                last_checked_at: None,
                next_check_at: None,
                backfill_before: None,
                increment_after: None,
                history_status: "unfetched".into(),
                next_direction: "backfill".into(),
                last_error: None,
            },
            saved: Mutex::new(vec![]),
            cancelled: AtomicBool::new(cancelled),
            save_failure: false,
        }
    }
    fn job() -> AccountJob {
        AccountJob {
            job_id: "j".into(),
            account_user_id: "me".into(),
            generation: "g".into(),
            status: "pending".into(),
            created_at: "2026-01-01".into(),
            updated_at: "2026-01-01".into(),
            cancel_requested: false,
            progress: json!({}),
        }
    }
    struct FairFixtureSource {
        calls: Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl AccountSource for FairFixtureSource {
        async fn page(&self, target: &AccountTarget, _: u8) -> Result<Vec<Message>, Value> {
            self.calls.lock().unwrap().push(target.channel_id.clone());
            if target.channel_id == "3" && target.last_checked_at.is_none() {
                return Ok(vec![]);
            }
            let id = match target.channel_id.as_str() {
                "1" => {
                    target
                        .increment_after
                        .as_deref()
                        .and_then(snowflake)
                        .unwrap_or(100)
                        + 1
                }
                "2" => 20,
                _ => 30,
            };
            Ok(vec![Message {
                id: id.to_string(),
                channel_id: target.channel_id.clone(),
                guild_id: Some("10".into()),
                mentions: if target.channel_id == "2" {
                    vec![discord_api::types::User {
                        id: "9".into(),
                        ..Default::default()
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            }])
        }
    }
    #[tokio::test]
    async fn fair_round_finds_quiet_old_mention_and_rechecks_initial_empty_target() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store
            .begin_account_inventory("9", "g", "2026-01-01")
            .unwrap();
        let channels: Vec<_> = (1..=3)
            .map(|id| Channel {
                id: id.to_string(),
                guild_id: Some("10".into()),
                kind: 0,
                ..Default::default()
            })
            .collect();
        store
            .observe_account_targets("9", "g", &channels, "2026-01-01")
            .unwrap();
        let state = SqliteAccountState {
            store: Arc::clone(&store),
        };
        let source = FairFixtureSource {
            calls: Mutex::new(vec![]),
        };
        let options = AccountSyncOptions {
            max_targets: 1,
            page_size: 1,
            refresh_inventory: false,
            ..Default::default()
        };
        for _ in 0..3 {
            let mut job = job();
            job.account_user_id = "9".into();
            run_round(&state, &source, job, &options).await.unwrap();
        }
        assert_eq!(*source.calls.lock().unwrap(), vec!["1", "2", "3"]);
        let quiet = store.get_message("20").unwrap().unwrap();
        let message = quiet.known_view().unwrap();
        assert_eq!(message.mentions[0].id, "9");
        let empty = store
            .account_targets("9")
            .unwrap()
            .into_iter()
            .find(|target| target.channel_id == "3")
            .unwrap();
        assert!(empty.last_checked_at.is_some());
        assert!(empty.increment_after.is_none());
        assert!(empty.backfill_before.is_none());
        store
            .save_account_page(
                "9",
                "3",
                &[],
                &TargetCheckpoint {
                    page_interval: None,
                    backfill_before: None,
                    increment_after: None,
                    checked_at: empty.last_checked_at.unwrap(),
                    next_check_at: Some("2026-01-01".into()),
                    history_status: empty.history_status,
                    next_direction: empty.next_direction,
                },
            )
            .unwrap();
        // Make only the previously empty channel eligible after its periodic deadline.
        let options = AccountSyncOptions {
            scope: AccountScope {
                channel_ids: Some(vec!["3".into()]),
                ..Default::default()
            },
            ..options
        };
        let mut job = job();
        job.account_user_id = "9".into();
        run_round(&state, &source, job, &options).await.unwrap();
        assert!(store.get_message("30").unwrap().is_some());
    }

    struct RestrictionDuringRoundSource {
        store: Arc<Store>,
        calls: Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl AccountSource for RestrictionDuringRoundSource {
        async fn page(&self, target: &AccountTarget, _: u8) -> Result<Vec<Message>, Value> {
            self.calls.lock().unwrap().push(target.channel_id.clone());
            if target.channel_id == "1" {
                self.store
                    .record_account_gap(&discord_store::sqlite::account::AccountGapOutcome {
                        account_user_id: "9",
                        channel_id: "2",
                        layer: "orchestration_approval",
                        status: "cancelled",
                        reason: "caller_reported_cancellation",
                        observed_at: &now_iso(),
                        evidence: &json!({"cancel_actor":"unknown"}),
                    })
                    .unwrap();
            }
            Ok(vec![])
        }
    }
    #[tokio::test]
    async fn unknown_scope_does_not_starve_next_allowed_candidate() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store
            .begin_account_inventory("9", "g", "2026-01-01")
            .unwrap();
        store
            .persist_account_exclusions("9", &[], &["99".into()], "2026-01-01")
            .unwrap();
        let channels = vec![
            Channel {
                id: "1".into(),
                kind: 0,
                ..Default::default()
            },
            Channel {
                id: "2".into(),
                kind: 1,
                ..Default::default()
            },
        ];
        store
            .observe_account_targets("9", "g", &channels, "2026-01-01")
            .unwrap();
        let state = SqliteAccountState {
            store: Arc::clone(&store),
        };
        let source = RestrictionDuringRoundSource {
            store: Arc::clone(&store),
            calls: Mutex::new(vec![]),
        };
        let mut job = job();
        job.account_user_id = "9".into();
        let report = run_round(
            &state,
            &source,
            job,
            &AccountSyncOptions {
                max_targets: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(*source.calls.lock().unwrap(), vec!["2"]);
        assert_eq!(report["attempted"], 1);
        assert_eq!(report["withheld_count"], 1);
        assert!(store
            .account_targets("9")
            .unwrap()
            .iter()
            .find(|t| t.channel_id == "1")
            .unwrap()
            .last_attempt_at
            .is_none());
    }

    struct DeadlineState {
        state: State,
        deadline: tokio::time::Instant,
        delay_before_request: bool,
        fetched: Arc<AtomicBool>,
    }
    impl DeadlineState {
        fn wait_for_deadline(&self) {
            while tokio::time::Instant::now() < self.deadline {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
    impl AccountSyncState for DeadlineState {
        fn targets(
            &self,
            user: &str,
            at: &str,
            limit: u32,
        ) -> Result<Vec<AccountTarget>, StoreError> {
            self.state.targets(user, at, limit)
        }
        fn attempt(&self, user: &str, channel: &str, at: &str) -> Result<(), StoreError> {
            self.state.attempt(user, channel, at)
        }
        fn save_page(
            &self,
            user: &str,
            channel: &str,
            messages: &[Message],
            checkpoint: TargetCheckpoint,
        ) -> Result<(), StoreError> {
            self.state.save_page(user, channel, messages, checkpoint)
        }
        fn fail(
            &self,
            user: &str,
            channel: &str,
            at: &str,
            error: &Value,
        ) -> Result<(), StoreError> {
            self.state.fail(user, channel, at, error)
        }
        fn cancelled(&self, job: &str) -> Result<bool, StoreError> {
            if !self.delay_before_request && self.fetched.load(Ordering::SeqCst) {
                self.wait_for_deadline();
            }
            self.state.cancelled(job)
        }
        fn progress(&self, job: &AccountJob) -> Result<(), StoreError> {
            if self.delay_before_request {
                self.wait_for_deadline();
            }
            self.state.progress(job)
        }
    }
    struct ObservedSource(Arc<AtomicBool>);
    #[async_trait::async_trait]
    impl AccountSource for ObservedSource {
        async fn page(&self, _: &AccountTarget, _: u8) -> Result<Vec<Message>, Value> {
            self.0.store(true, Ordering::SeqCst);
            Ok(vec![])
        }
    }
    #[tokio::test]
    async fn synchronous_state_work_cannot_start_http_or_save_after_deadline() {
        for delay_before_request in [true, false] {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(100);
            let fetched = Arc::new(AtomicBool::new(false));
            let state = DeadlineState {
                state: state(false),
                deadline,
                delay_before_request,
                fetched: Arc::clone(&fetched),
            };
            let report = run_round(
                &state,
                &ObservedSource(Arc::clone(&fetched)),
                job(),
                &AccountSyncOptions {
                    execution_deadline: Some(deadline),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert_eq!(report["status"], "paused");
            assert_eq!(fetched.load(Ordering::SeqCst), !delay_before_request);
            assert!(state.state.saved.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn expired_round_budget_pauses_before_attempt_or_http_reservation() {
        let state = state(false);
        let source = Source { messages: vec![] };
        let report = run_round(
            &state,
            &source,
            job(),
            &AccountSyncOptions {
                execution_timeout: Some(std::time::Duration::ZERO),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report["status"], "paused");
        assert_eq!(report["attempted"], 0);
        assert_eq!(report["body_http_attempt_upper_bound"], 0);
        assert_eq!(report["page_complete"], false);
        assert!(state.saved.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn candidate_listing_defers_individual_scope_checks_until_selected_round() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store
            .begin_account_inventory("9", "g", "2026-01-01")
            .unwrap();
        store
            .persist_account_exclusions("9", &[], &["99".into()], "2026-01-01")
            .unwrap();
        let channels = vec![
            Channel {
                id: "1".into(),
                kind: 1,
                ..Default::default()
            },
            Channel {
                id: "2".into(),
                kind: 0,
                ..Default::default()
            },
        ];
        store
            .observe_account_targets("9", "g", &channels, "2026-01-01")
            .unwrap();
        let state = SqliteAccountState {
            store: Arc::clone(&store),
        };
        let candidates = state.targets("9", &now_iso(), 100).unwrap();
        assert_eq!(candidates.len(), 2);
        assert!(state.withheld("9", "2").unwrap());
        let source = RestrictionDuringRoundSource {
            store,
            calls: Mutex::new(vec![]),
        };
        let mut job = job();
        job.account_user_id = "9".into();
        let report = run_round(
            &state,
            &source,
            job,
            &AccountSyncOptions {
                scope: AccountScope {
                    channel_ids: Some(vec!["2".into()]),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(source.calls.lock().unwrap().is_empty());
        assert_eq!(report["attempted"], 0);
        assert_eq!(report["withheld_count"], 1);
    }

    #[tokio::test]
    async fn restrictions_inserted_during_round_skip_preselected_targets_before_attempt() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store
            .begin_account_inventory("9", "g", "2026-01-01")
            .unwrap();
        let channels: Vec<_> = (1..=2)
            .map(|id| Channel {
                id: id.to_string(),
                kind: 1,
                ..Default::default()
            })
            .collect();
        store
            .observe_account_targets("9", "g", &channels, "2026-01-01")
            .unwrap();
        let state = SqliteAccountState {
            store: Arc::clone(&store),
        };
        let source = RestrictionDuringRoundSource {
            store: Arc::clone(&store),
            calls: Mutex::new(vec![]),
        };
        let mut job = job();
        job.account_user_id = "9".into();
        let report = run_round(
            &state,
            &source,
            job,
            &AccountSyncOptions {
                max_targets: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(*source.calls.lock().unwrap(), vec!["1"]);
        assert_eq!(report["attempted"], 1);
        assert_eq!(report["withheld_count"], 1);
        let blocked = store
            .account_targets("9")
            .unwrap()
            .into_iter()
            .find(|target| target.channel_id == "2")
            .unwrap();
        assert!(blocked.last_attempt_at.is_none());
        assert!(blocked.last_checked_at.is_none());
        assert!(state
            .targets("9", &now_iso(), 100)
            .unwrap()
            .iter()
            .all(|target| target.channel_id != "2"));
    }

    #[tokio::test]
    async fn persisted_exclusion_cannot_be_bypassed_by_new_default_or_allowlisted_round() {
        let store = Store::open_in_memory().unwrap();
        store
            .persist_account_exclusions("me", &[], &["1".into()], "2026-01-01")
            .unwrap();
        for mut options in [
            AccountSyncOptions::default(),
            AccountSyncOptions {
                scope: AccountScope {
                    channel_ids: Some(vec!["1".into()]),
                    ..Default::default()
                },
                ..Default::default()
            },
        ] {
            merge_persistent_exclusions(&store, "me", &mut options).unwrap();
            let state = state(false);
            assert_eq!(
                run_round(&state, &Source { messages: vec![] }, job(), &options)
                    .await
                    .unwrap()["attempted"],
                0
            );
            assert!(state.saved.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn coverage_detail_and_summary_share_body_success_evidence() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("me", "g", "2026-01-01")
            .unwrap();
        store
            .observe_account_targets(
                "me",
                "g",
                &[Channel {
                    id: "1".into(),
                    kind: 1,
                    ..Default::default()
                }],
                "2026-01-01",
            )
            .unwrap();
        store
            .record_channel_sync("1", None, None, &now_iso())
            .unwrap();
        let report = account_coverage_page(&store, "me", None, None, None, 10).unwrap();
        assert_eq!(report["synced"], 0);
        assert_eq!(report["targets"][0]["body_synced"], false);
        assert!(report["targets"][0]["last_body_success_at"].is_null());
        let at = now_iso();
        store
            .save_account_page(
                "me",
                "1",
                &[],
                &TargetCheckpoint {
                    page_interval: None,
                    backfill_before: None,
                    increment_after: None,
                    checked_at: at.clone(),
                    next_check_at: None,
                    history_status: "history_access_unknown".into(),
                    next_direction: "incremental".into(),
                },
            )
            .unwrap();
        let report = account_coverage_page(&store, "me", None, None, None, 10).unwrap();
        assert_eq!(report["synced"], 1);
        assert_eq!(report["targets"][0]["body_synced"], true);
        assert_eq!(report["targets"][0]["last_body_success_at"], at);
        assert!(report["targets"][0]["unfetched_since"].is_null());
        assert_eq!(report["scope_complete"], false);
        assert_eq!(report["page_complete"], true);
    }

    #[test]
    fn cache_preview_preserves_unfetched_state_and_creates_no_job() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("me", "g", "2026-01-01")
            .unwrap();
        store
            .observe_account_targets(
                "me",
                "g",
                &[Channel {
                    id: "1".into(),
                    kind: 1,
                    ..Default::default()
                }],
                "2026-01-01",
            )
            .unwrap();
        let preview = preview_account_sync(&store, "me", &AccountSyncOptions::default()).unwrap();
        assert_eq!(preview["selected_targets"], 1);
        assert_eq!(preview["network_requests"], 0);
        assert_eq!(preview["state_changes"], 0);
        assert_eq!(
            store.account_targets("me").unwrap()[0].history_status,
            "unfetched"
        );
        assert!(store.account_job("preview").unwrap().is_none());
    }
    #[tokio::test]
    async fn discovery_zero_budget_leaves_guild_page_unrequested() {
        let store = Store::open_in_memory().unwrap();
        create_account_job(&store, "guilds", "me").unwrap();
        let source = GuildPages {
            pages: Mutex::new([vec![]].into()),
            after: Mutex::new(vec![]),
        };
        let result =
            collect_guild_inventory(&source, &store, "guilds", &mut HttpAttemptBudget::new(0))
                .await
                .unwrap();
        assert!(!result.exhausted);
        assert!(source.after.lock().unwrap().is_empty());
        assert_eq!(
            result.error.unwrap()["code"],
            "discovery_http_budget_exhausted"
        );
    }

    #[test]
    fn scope_and_retry_budget_fail_closed() {
        let scope = AccountScope {
            guild_ids: Some(vec!["10".into()]),
            exclude_channel_ids: vec!["2".into()],
            ..Default::default()
        };
        assert!(scope.allows("1", Some("10"), None, 0));
        assert!(!scope.allows("2", Some("10"), None, 0));
        assert!(!scope.allows("3", Some("20"), None, 0));
        assert!(!scope.allows("4", None, None, 1));
        let mut budget = HttpAttemptBudget::new(5);
        assert!(budget.reserve(1));
        assert!(!budget.reserve(1));
        assert_eq!(budget.reserved, 3);
    }
    #[tokio::test]
    async fn excluded_target_and_zero_budget_do_not_fetch_or_checkpoint() {
        for options in [
            AccountSyncOptions {
                scope: AccountScope {
                    exclude_channel_ids: vec!["1".into()],
                    ..Default::default()
                },
                ..Default::default()
            },
            AccountSyncOptions {
                max_body_http_requests: 0,
                ..Default::default()
            },
        ] {
            let state = state(false);
            let report = run_round(&state, &Source { messages: vec![] }, job(), &options)
                .await
                .unwrap();
            assert_eq!(report["attempted"], 0);
            assert!(state.saved.lock().unwrap().is_empty());
        }
    }
    #[tokio::test]
    async fn empty_page_keeps_history_unknown() {
        let state = state(false);
        let source = Source { messages: vec![] };
        let report = run_round(
            &state,
            &source,
            job(),
            &AccountSyncOptions {
                max_targets: 10,
                page_size: 50,
                refresh_inventory: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            state.saved.lock().unwrap()[0].history_status,
            "history_access_unknown"
        );
        assert_eq!(report["complete"], false);
    }
    #[tokio::test]
    async fn cancelled_job_saves_no_checkpoint() {
        let state = state(true);
        let source = Source { messages: vec![] };
        let report = run_round(
            &state,
            &source,
            job(),
            &AccountSyncOptions {
                max_targets: 10,
                page_size: 50,
                refresh_inventory: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report["status"], "cancelled");
        assert!(state.saved.lock().unwrap().is_empty());
    }

    struct CancelDuringRead<'a> {
        state: &'a State,
    }
    #[async_trait::async_trait]
    impl AccountSource for CancelDuringRead<'_> {
        async fn page(&self, _: &AccountTarget, _: u8) -> Result<Vec<Message>, Value> {
            self.state.cancelled.store(true, Ordering::SeqCst);
            Ok(vec![])
        }
    }
    #[tokio::test]
    async fn cancellation_during_request_prevents_checkpoint_commit() {
        let state = state(false);
        let source = CancelDuringRead { state: &state };
        let report = run_round(
            &state,
            &source,
            job(),
            &AccountSyncOptions {
                max_targets: 10,
                page_size: 50,
                refresh_inventory: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report["status"], "cancelled");
        assert!(state.saved.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn failed_save_cannot_advance_checkpoint() {
        let mut state = state(false);
        state.save_failure = true;
        assert!(run_round(
            &state,
            &Source { messages: vec![] },
            job(),
            &AccountSyncOptions {
                max_targets: 10,
                page_size: 50,
                refresh_inventory: false,
                ..Default::default()
            }
        )
        .await
        .is_err());
        assert!(state.saved.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn mis_scoped_and_duplicate_pages_cannot_advance_checkpoint() {
        let state = state(false);
        let message=parse_message(json!({"id":"10","channel_id":"2","content":"x","timestamp":"2026-01-01T00:00:00Z","author":{"id":"4","username":"u"}})).unwrap();
        let source = Source {
            messages: vec![message.clone(), message],
        };
        let report = run_round(
            &state,
            &source,
            job(),
            &AccountSyncOptions {
                max_targets: 10,
                page_size: 50,
                refresh_inventory: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report["failures"][0]["error"]["code"], "invalid_response");
        assert!(state.saved.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn continuing_new_posts_alternate_with_historical_backfill() {
        let mut state = state(false);
        state.target.next_direction = "backfill".into();
        let options = AccountSyncOptions {
            max_targets: 1,
            page_size: 1,
            refresh_inventory: false,
            ..Default::default()
        };
        for (id, direction, oldest) in [
            ("100", "incremental", "100"),
            ("101", "backfill", "100"),
            ("90", "incremental", "90"),
            ("102", "backfill", "90"),
        ] {
            let message=parse_message(json!({"id":id,"channel_id":"1","content":"<@me>","timestamp":"2026-01-01T00:00:00Z","author":{"id":"4","username":"u"}})).unwrap();
            run_round(
                &state,
                &Source {
                    messages: vec![message],
                },
                job(),
                &options,
            )
            .await
            .unwrap();
            let checkpoints = state.saved.lock().unwrap();
            let checkpoint = checkpoints.last().unwrap();
            assert_eq!(checkpoint.next_direction, direction);
            assert_eq!(checkpoint.backfill_before.as_deref(), Some(oldest));
            assert_eq!(checkpoint.history_status, "partial");
            state.target.backfill_before = checkpoint.backfill_before.clone();
            state.target.increment_after = checkpoint.increment_after.clone();
            state.target.history_status = checkpoint.history_status.clone();
            state.target.next_direction = checkpoint.next_direction.clone();
        }
        assert_eq!(state.target.increment_after.as_deref(), Some("102"));
    }

    #[tokio::test]
    async fn empty_incremental_page_keeps_backfill_pending() {
        let mut state = state(false);
        state.target.history_status = "partial".into();
        state.target.next_direction = "incremental".into();
        state.target.backfill_before = Some("90".into());
        state.target.increment_after = Some("100".into());
        run_round(
            &state,
            &Source { messages: vec![] },
            job(),
            &AccountSyncOptions {
                max_targets: 1,
                page_size: 50,
                refresh_inventory: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let checkpoints = state.saved.lock().unwrap();
        let checkpoint = checkpoints.last().unwrap();
        assert_eq!(checkpoint.history_status, "partial");
        assert_eq!(checkpoint.backfill_before.as_deref(), Some("90"));
        assert_eq!(checkpoint.next_direction, "backfill");
        assert!(checkpoint.next_check_at.is_some());
    }

    #[test]
    fn incremental_without_observed_id_requests_oldest_new_messages() {
        let mut state = state(false);
        state.target.next_direction = "incremental".into();
        let request = message_page_request(&state.target, 100);
        let DiscordRequest::GetMessages { after, .. } = request else {
            panic!("message request")
        };
        assert_eq!(after.as_deref(), Some("0"));
        assert!(state.target.increment_after.is_none());
    }

    #[tokio::test]
    async fn cancelled_job_with_no_due_targets_stays_cancelled() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        create_account_job(&store, "cancelled", "me").unwrap();
        store
            .request_account_cancel("cancelled", &now_iso())
            .unwrap();
        let state = SqliteAccountState {
            store: Arc::clone(&store),
        };
        let report = run_round(
            &state,
            &Source { messages: vec![] },
            store.account_job("cancelled").unwrap().unwrap(),
            &AccountSyncOptions {
                max_targets: 1,
                page_size: 50,
                refresh_inventory: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report["status"], "cancelled");
    }

    struct DiscoveryFixture {
        calls: Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl InventorySource for DiscoveryFixture {
        async fn guilds(&self, after: Option<String>) -> Result<Vec<Guild>, Value> {
            self.calls.lock().unwrap().push(format!("guilds:{after:?}"));
            Ok(if after.is_none() {
                (1..=200)
                    .map(|id| Guild {
                        id: id.to_string(),
                        ..Default::default()
                    })
                    .collect()
            } else {
                vec![]
            })
        }
        async fn channels(&self, request: DiscordRequest) -> Result<Vec<Channel>, Value> {
            self.calls.lock().unwrap().push(format!("{request:?}"));
            Ok(vec![])
        }
        async fn threads(
            &self,
            _: &Channel,
            _: Option<&str>,
        ) -> Result<crate::thread_listing::ThreadPage, Value> {
            panic!("no parents")
        }
    }
    #[tokio::test]
    async fn small_discovery_budget_resumes_guild_pages_and_rotates_guild_channels() {
        let store = Store::open_in_memory().unwrap();
        let source = DiscoveryFixture {
            calls: Mutex::new(vec![]),
        };
        let mut options = AccountSyncOptions {
            max_discovery_http_requests: 3,
            ..Default::default()
        };
        options.scope.include_dms = false;
        options.scope.exclude_guild_ids = (1..200).map(|id| id.to_string()).collect();
        for job in ["one", "two", "three"] {
            create_account_job(&store, job, "9").unwrap();
            discover_inventory(&source, &store, job, "9", &options)
                .await
                .unwrap();
        }
        let calls = source.calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0], "guilds:None");
        assert!(calls.iter().any(|call| call == "guilds:Some(\"200\")"));
        assert!(calls
            .iter()
            .any(|call| call.contains("200") && call != "guilds:Some(\"200\")"));
    }
    #[tokio::test]
    async fn explicit_guild_discovery_does_not_restart_first_guild_with_small_budget() {
        let store = Store::open_in_memory().unwrap();
        let source = DiscoveryFixture {
            calls: Mutex::new(vec![]),
        };
        let mut options = AccountSyncOptions {
            max_discovery_http_requests: 3,
            ..Default::default()
        };
        options.scope.guild_ids = Some(vec!["10".into(), "20".into()]);
        for job in ["one", "two", "three"] {
            create_account_job(&store, job, "9").unwrap();
            discover_inventory(&source, &store, job, "9", &options)
                .await
                .unwrap();
        }
        let calls = source.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls[0].contains("10"));
        assert!(calls[1].contains("20"));
    }

    #[test]
    fn invalid_discovery_payload_is_not_a_permanent_permission_or_capability_denial() {
        for code in [
            "invalid_response",
            "invalid_guild_pagination",
            "UNKNOWN_ERROR",
        ] {
            assert!(!discovery_requires_explicit_resume(
                &json!({"code":code,"retryable":false})
            ));
        }
        assert!(discovery_requires_explicit_resume(
            &json!({"http_status":403,"retryable":false})
        ));
        assert!(discovery_requires_explicit_resume(
            &json!({"code":"execution_withheld","retryable":false})
        ));
    }

    #[tokio::test]
    async fn invalid_discovery_response_recovers_after_cycle_refresh_and_scope_change() {
        struct RecoveringInventory {
            fail: AtomicBool,
            calls: Mutex<Vec<String>>,
        }
        #[async_trait::async_trait]
        impl InventorySource for RecoveringInventory {
            async fn guilds(&self, _: Option<String>) -> Result<Vec<Guild>, Value> {
                panic!("scoped")
            }
            async fn channels(&self, request: DiscordRequest) -> Result<Vec<Channel>, Value> {
                self.calls.lock().unwrap().push(format!("{request:?}"));
                if self.fail.load(Ordering::SeqCst) {
                    Err(json!({"code":"invalid_response","retryable":false}))
                } else {
                    Ok(vec![])
                }
            }
            async fn threads(
                &self,
                _: &Channel,
                _: Option<&str>,
            ) -> Result<crate::thread_listing::ThreadPage, Value> {
                panic!("no parent")
            }
        }
        let store = Store::open_in_memory().unwrap();
        let source = RecoveringInventory {
            fail: AtomicBool::new(true),
            calls: Mutex::new(vec![]),
        };
        let mut options = AccountSyncOptions::default();
        options.scope.guild_ids = Some(vec!["10".into()]);
        create_account_job(&store, "bad", "9").unwrap();
        discover_inventory(&source, &store, "bad", "9", &options)
            .await
            .unwrap();
        let key = discovery_scope_key(&options.scope);
        let mut saved = store
            .account_discovery_checkpoint("9", &key)
            .unwrap()
            .unwrap();
        saved["completed_at"] = json!("2000-01-01T00:00:00Z");
        store
            .save_account_discovery_checkpoint("9", &key, &saved, &now_iso())
            .unwrap();
        source.fail.store(false, Ordering::SeqCst);
        create_account_job(&store, "recover", "9").unwrap();
        discover_inventory(&source, &store, "recover", "9", &options)
            .await
            .unwrap();
        assert_eq!(source.calls.lock().unwrap().len(), 2);
        assert!(store
            .account_discovery_blocked_operations("9")
            .unwrap()
            .is_empty());
        options.scope.guild_ids = Some(vec!["10".into(), "20".into()]);
        create_account_job(&store, "newscope", "9").unwrap();
        discover_inventory(&source, &store, "newscope", "9", &options)
            .await
            .unwrap();
        assert_eq!(source.calls.lock().unwrap().len(), 4);
    }

    struct ThreadDiscoveryFixture {
        calls: Mutex<Vec<String>>,
        deny: bool,
    }
    #[async_trait::async_trait]
    impl InventorySource for ThreadDiscoveryFixture {
        async fn guilds(&self, _: Option<String>) -> Result<Vec<Guild>, Value> {
            panic!("scoped")
        }
        async fn channels(&self, request: DiscordRequest) -> Result<Vec<Channel>, Value> {
            let DiscordRequest::GetGuildChannels { guild_id } = request else {
                panic!("guild")
            };
            Ok(vec![Channel {
                id: format!("{guild_id}0"),
                guild_id: Some(guild_id),
                kind: 0,
                ..Default::default()
            }])
        }
        async fn threads(
            &self,
            parent: &Channel,
            _: Option<&str>,
        ) -> Result<crate::thread_listing::ThreadPage, Value> {
            self.calls.lock().unwrap().push(parent.id.clone());
            if self.deny {
                return Err(json!({"code":50001,"http_status":403,"retryable":false}));
            }
            Ok(crate::thread_listing::ThreadPage {
                threads: vec![Channel {
                    id: format!("{}1", parent.id),
                    guild_id: parent.guild_id.clone(),
                    parent_id: Some(parent.id.clone()),
                    kind: 11,
                    ..Default::default()
                }],
                next_cursor: None,
                has_more: false,
                effective_limit: 25,
                search_window_exhausted: false,
            })
        }
    }
    #[tokio::test]
    async fn discovery_resumes_later_thread_parent_and_reports_too_small_budget() {
        let store = Store::open_in_memory().unwrap();
        let source = ThreadDiscoveryFixture {
            calls: Mutex::new(vec![]),
            deny: false,
        };
        let mut options = AccountSyncOptions {
            max_discovery_http_requests: 6,
            ..Default::default()
        };
        options.scope.guild_ids = Some(vec!["10".into(), "20".into()]);
        create_account_job(&store, "one", "9").unwrap();
        discover_inventory(&source, &store, "one", "9", &options)
            .await
            .unwrap();
        options.max_discovery_http_requests = 3;
        create_account_job(&store, "small", "9").unwrap();
        discover_inventory(&source, &store, "small", "9", &options)
            .await
            .unwrap();
        assert!(source.calls.lock().unwrap().is_empty());
        assert_eq!(
            store.account_inventory("9").unwrap().unwrap().discovery["budget_blocked"][0]
                ["required_http_attempt_upper_bound"],
            6
        );
        options.max_discovery_http_requests = 6;
        for job in ["two", "three"] {
            create_account_job(&store, job, "9").unwrap();
            discover_inventory(&source, &store, job, "9", &options)
                .await
                .unwrap();
        }
        let mut calls = source.calls.lock().unwrap().clone();
        calls.sort();
        assert_eq!(calls, vec!["100", "200"]);
    }
    #[tokio::test]
    async fn discovery_denial_is_retained_across_new_scope_and_parent_metadata() {
        let store = Store::open_in_memory().unwrap();
        let source = ThreadDiscoveryFixture {
            calls: Mutex::new(vec![]),
            deny: true,
        };
        let mut options = AccountSyncOptions {
            max_discovery_http_requests: 30,
            ..Default::default()
        };
        options.scope.guild_ids = Some(vec!["10".into()]);
        for job in ["one", "two"] {
            if job == "two" {
                options.scope.guild_ids = Some(vec!["10".into(), "20".into()]);
            }
            create_account_job(&store, job, "9").unwrap();
            discover_inventory(&source, &store, job, "9", &options)
                .await
                .unwrap();
        }
        assert_eq!(*source.calls.lock().unwrap(), vec!["100", "200"]);
    }

    struct GuildPages {
        pages: Mutex<std::collections::VecDeque<Vec<Guild>>>,
        after: Mutex<Vec<Option<String>>>,
    }
    #[async_trait::async_trait]
    impl GuildInventorySource for GuildPages {
        async fn guild_page(&self, after: Option<String>) -> Result<Vec<Guild>, Value> {
            self.after.lock().unwrap().push(after);
            Ok(self.pages.lock().unwrap().pop_front().unwrap())
        }
    }
    #[tokio::test]
    async fn guild_inventory_continues_full_page_until_confirmed_exhaustion() {
        let store = Store::open_in_memory().unwrap();
        create_account_job(&store, "guilds", "me").unwrap();
        let first = (1..=200)
            .map(|id| serde_json::from_value(json!({"id":id.to_string(),"name":"g"})).unwrap())
            .collect();
        let source = GuildPages {
            pages: Mutex::new([first, vec![]].into()),
            after: Mutex::new(vec![]),
        };
        let result = collect_guild_inventory(
            &source,
            &store,
            "guilds",
            &mut HttpAttemptBudget::new(10000),
        )
        .await
        .unwrap();
        assert_eq!(result.guilds.len(), 200);
        assert!(result.exhausted);
        assert_eq!(
            *source.after.lock().unwrap(),
            vec![None, Some("200".into())]
        );
    }

    #[tokio::test]
    async fn uninitialized_incremental_empty_page_keeps_cursor_unknown() {
        let mut state = state(false);
        state.target.next_direction = "incremental".into();
        state.target.history_status = "history_access_unknown".into();
        run_round(
            &state,
            &Source { messages: vec![] },
            job(),
            &AccountSyncOptions {
                max_targets: 1,
                page_size: 100,
                refresh_inventory: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(state.saved.lock().unwrap()[0].increment_after.is_none());
    }

    struct CancelGuildRead<'a> {
        store: &'a Store,
    }
    #[async_trait::async_trait]
    impl GuildInventorySource for CancelGuildRead<'_> {
        async fn guild_page(&self, _: Option<String>) -> Result<Vec<Guild>, Value> {
            self.store
                .request_account_cancel("guilds", &now_iso())
                .unwrap();
            Ok(vec![])
        }
    }
    #[tokio::test]
    async fn cancellation_during_discovery_does_not_claim_exhaustion() {
        let store = Store::open_in_memory().unwrap();
        create_account_job(&store, "guilds", "me").unwrap();
        let result = collect_guild_inventory(
            &CancelGuildRead { store: &store },
            &store,
            "guilds",
            &mut HttpAttemptBudget::new(10000),
        )
        .await
        .unwrap();
        assert!(!result.exhausted);
        assert!(store.account_cancel_requested("guilds").unwrap());
    }

    #[tokio::test]
    async fn later_latest_backfill_cannot_jump_incremental_over_pending_gap() {
        let mut state = state(false);
        state.target.history_status = "partial".into();
        state.target.increment_after = Some("100".into());
        let message=parse_message(json!({"id":"1000","channel_id":"1","content":"test","timestamp":"2026-01-01T00:00:00Z"})).unwrap();
        run_round(
            &state,
            &Source {
                messages: vec![message],
            },
            job(),
            &AccountSyncOptions {
                max_targets: 1,
                page_size: 1,
                refresh_inventory: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let checkpoints = state.saved.lock().unwrap();
        assert_eq!(checkpoints[0].increment_after.as_deref(), Some("100"));
        assert_eq!(checkpoints[0].backfill_before.as_deref(), Some("1000"));
    }

    #[tokio::test]
    async fn short_backfill_records_proven_upper_boundary_without_claiming_channel_start() {
        let mut state = state(false);
        state.target.history_status = "partial".into();
        state.target.backfill_before = Some("1000".into());
        state.target.increment_after = Some("2000".into());
        let message=parse_message(json!({"id":"900","channel_id":"1","content":"test","timestamp":"2026-01-01T00:00:00Z"})).unwrap();
        run_round(
            &state,
            &Source {
                messages: vec![message],
            },
            job(),
            &AccountSyncOptions {
                max_targets: 1,
                page_size: 50,
                refresh_inventory: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let checkpoints = state.saved.lock().unwrap();
        assert_eq!(
            checkpoints[0].page_interval,
            Some(("900".into(), "999".into()))
        );
        assert_eq!(checkpoints[0].history_status, "history_access_unknown");
    }

    #[test]
    fn large_account_summary_and_scoped_details_are_bounded() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("me", "g", "2026-01-01T00:00:00Z")
            .unwrap();
        let channels: Vec<Channel> = (1..=13000)
            .map(|id| {
                serde_json::from_value(json!({"id":id.to_string(),"type":0,"guild_id":"42"}))
                    .unwrap()
            })
            .collect();
        store
            .observe_account_targets("me", "g", &channels, "2026-01-01T00:00:00Z")
            .unwrap();
        let failures:Vec<_>=(1..=13000).map(|id|json!({"channel_id":id.to_string(),"error":{"code":"failed","message":"x".repeat(1000)}})).collect();
        store
            .finish_account_inventory("g", "2026-01-01T00:00:00Z", &json!({"failures":failures}))
            .unwrap();
        let summary = account_coverage(&store, "me").unwrap();
        assert_eq!(summary["targets_total"], 13000);
        assert!(summary.to_string().len() < 65536);
        assert!(summary["targets"].as_array().unwrap().is_empty());
        assert_eq!(summary["inventory"]["discovery"]["failures_total"], 13000);
        let page = account_coverage_page(&store, "me", Some("42"), None, None, 2).unwrap();
        assert_eq!(page["targets_total"], 13000);
        assert_eq!(page["targets"].as_array().unwrap().len(), 2);
        assert_eq!(page["has_more"], true);
        assert!(page.to_string().len() < 65536);
        let next = account_coverage_page(
            &store,
            "me",
            Some("42"),
            None,
            page["next_cursor"].as_str(),
            2,
        )
        .unwrap();
        assert_ne!(
            page["targets"][1]["channel_id"],
            next["targets"][0]["channel_id"]
        );
        let empty = account_coverage(&store, "unknown").unwrap();
        assert_eq!(empty["inventory_initialized"], false);
        assert_eq!(empty["scope"], "account");
        assert_eq!(empty["targets_total"], 0);
        assert_eq!(empty["complete"], false);
    }
}
