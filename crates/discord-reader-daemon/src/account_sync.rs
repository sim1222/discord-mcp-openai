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

pub(crate) struct AccountSyncOptions {
    pub(crate) max_targets: u32,
    pub(crate) page_size: u8,
    pub(crate) refresh_inventory: bool,
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

struct GuildInventory {
    guilds: Vec<Guild>,
    exhausted: bool,
    resume_after: Option<String>,
    error: Option<Value>,
}

async fn collect_guild_inventory(
    source: &dyn GuildInventorySource,
    store: &Store,
    job: &str,
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
    options: AccountSyncOptions,
) -> Result<Value, AccountSyncError> {
    let result =
        run_account_sync_inner(client, Arc::clone(&store), id.clone(), user, options).await;
    if let Err(error) = &result {
        if let Some(mut job) = store.account_job(&id)? {
            job.status = "failed".into();
            job.updated_at = now_iso();
            job.progress["status"] = json!("failed");
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
    options: AccountSyncOptions,
) -> Result<Value, AccountSyncError> {
    let mut job = store.account_job(&id)?.ok_or_else(|| AccountSyncError {
        data: json!({"code":"unknown_job","retryable":false}),
    })?;
    if job.account_user_id != user {
        return Err(AccountSyncError {
            data: json!({"code":"account_mismatch","retryable":false}),
        });
    }
    if options.refresh_inventory && !store.account_cancel_requested(&id)? {
        job.status = "running".into();
        job.updated_at = now_iso();
        store.save_account_job(&job)?;
        job.generation = refresh_inventory(&client, &store, &id, &user).await?;
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
    let at = now_iso();
    let limit = options.page_size.clamp(1, 100);
    let targets = state.targets(
        &job.account_user_id,
        &at,
        options.max_targets.clamp(1, 1000),
    )?;
    let mut failures = Vec::new();
    let mut attempted = 0;
    let mut saved = 0;
    let mut messages_saved = 0;
    job.status = "running".into();
    for target in &targets {
        if state.cancelled(&job.job_id)? {
            job.status = "cancelled".into();
            break;
        }
        let attempt_at = now_iso();
        state.attempt(&job.account_user_id, &target.channel_id, &attempt_at)?;
        attempted += 1;
        let result = source.page(target, limit).await;
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
        job.progress = json!({"job_id":job.job_id,"scope":"account","account_user_id":job.account_user_id,"generation":job.generation,"as_of":job.updated_at,"status":"running","selected_targets":targets.len(),"attempted":attempted,"synced":saved,"messages_saved":messages_saved,"failures":failures.iter().take(20).map(failure_summary).collect::<Vec<_>>(),"failures_total":failures.len(),"failures_omitted":failures.len().saturating_sub(20),"complete":false});
        state.progress(&job)?;
    }
    if state.cancelled(&job.job_id)? {
        job.status = "cancelled".into();
    }
    if job.status != "cancelled" {
        job.status = if failures.is_empty() {
            "finished"
        } else {
            "partial"
        }
        .into();
    }
    job.updated_at = now_iso();
    job.progress = json!({"job_id":job.job_id,"scope":"account","account_user_id":job.account_user_id,"generation":job.generation,"as_of":job.updated_at,"status":job.status,"selected_targets":targets.len(),"attempted":attempted,"synced":saved,"messages_saved":messages_saved,"failures":failures.iter().take(20).map(failure_summary).collect::<Vec<_>>(),"failures_total":failures.len(),"failures_omitted":failures.len().saturating_sub(20),"cancel_reason":if job.status=="cancelled"{Some("user_requested")}else{None},"resume_cursor":{"account_user_id":job.account_user_id,"generation":job.generation,"source":"persisted_target_checkpoints"},"complete":false});
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

async fn refresh_inventory(
    client: &SharedDiscordClient,
    store: &Store,
    job: &str,
    user: &str,
) -> Result<String, AccountSyncError> {
    let previous = store.account_inventory(user)?;
    let mut thread_cursors = previous
        .and_then(|inventory| {
            inventory
                .discovery
                .get("thread_resume_cursors")
                .and_then(Value::as_object)
                .cloned()
        })
        .unwrap_or_default();
    let at = now_iso();
    let generation = format!("inventory:{job}");
    store.begin_account_inventory(user, &generation, &at)?;
    let mut failures = Vec::new();
    let guild_source = DiscordAccountSource {
        client: Arc::clone(client),
    };
    let guild_inventory = collect_guild_inventory(&guild_source, store, job).await?;
    let guilds = guild_inventory.guilds;
    if let Some(error) = guild_inventory.error {
        failures.push(json!({"source":"guilds","error":error}));
    }
    let mut parents = Vec::new();
    if !store.account_cancel_requested(job)? {
        match fetch_channels(client, DiscordRequest::ListUserDmChannels).await {
            Ok(channels) => {
                store.observe_account_targets(user, &generation, &channels, &now_iso())?
            }
            Err(error) => failures.push(json!({"source":"dms","error":error})),
        }
    }
    for guild in &guilds {
        if store.account_cancel_requested(job)? {
            break;
        }
        match fetch_channels(
            client,
            DiscordRequest::GetGuildChannels {
                guild_id: guild.id.clone(),
            },
        )
        .await
        {
            Ok(mut channels) => {
                for channel in &mut channels {
                    channel.guild_id = Some(guild.id.clone());
                    store.upsert_channel(channel)?;
                }
                store.observe_account_targets(user, &generation, &channels, &now_iso())?;
                parents.extend(
                    channels
                        .into_iter()
                        .filter(|channel| matches!(channel.kind, 0 | 5 | 15 | 16)),
                );
            }
            Err(error) => {
                failures.push(json!({"source":"guild_channels","guild_id":guild.id,"error":error}))
            }
        }
    }
    let listing = DiscordThreadListing::new(Arc::clone(client));
    for parent in parents {
        if store.account_cancel_requested(job)? {
            break;
        }
        let cursor = thread_cursors.get(&parent.id).and_then(Value::as_str);
        let query = ThreadQuery::new(
            Some(&parent.id),
            parent.guild_id.as_deref(),
            Some("all"),
            None,
            25,
            cursor,
        )
        .map_err(|error| AccountSyncError {
            data: error.payload(),
        })?;
        match listing.list(&query).await {
            Ok(page) => {
                store.observe_account_targets(user, &generation, &page.threads, &now_iso())?;
                for thread in &page.threads {
                    store.upsert_channel(thread)?;
                }
                if page.search_window_exhausted {
                    failures.push(json!({"source":"threads","parent_id":parent.id,"code":"search_window_exhausted","retryable":false,"remaining_scope":"threads_beyond_search_offset_limit"}));
                }
                if let Some(cursor) = page.next_cursor {
                    thread_cursors.insert(parent.id.clone(), json!(cursor));
                } else {
                    thread_cursors.remove(&parent.id);
                }
            }
            Err(error) => failures
                .push(json!({"source":"threads","parent_id":parent.id,"error":error.payload()})),
        }
    }
    let cancelled = store.account_cancel_requested(job)?;
    store.finish_account_inventory(&generation,&now_iso(),&json!({"scope":"account","complete":false,"cancelled":cancelled,"guilds_observed":guilds.len(),"guild_inventory_exhausted":guild_inventory.exhausted,"guild_resume_after":guild_inventory.resume_after,"failures":failures,"thread_resume_cursors":thread_cursors,"reasons":["dm_inventory_completeness_unverified","thread_search_index_not_exhaustive","thread_discovery_one_page_per_parent","history_permissions_not_verified"]}))?;
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
    json!({"generation":inventory.generation,"account_user_id":inventory.account_user_id,"started_at":inventory.started_at,"refreshed_at":inventory.refreshed_at,"status":inventory.status,"discovery":{"source":discovery["source"].as_str(),"partial":discovery["partial"].as_bool(),"inventory_errors":displayed_inventory_errors,"inventory_errors_total":inventory_errors.len(),"inventory_errors_omitted":inventory_errors.len().saturating_sub(20),"complete":false,"cancelled":discovery["cancelled"].as_bool(),"guilds_observed":discovery["guilds_observed"].as_u64(),"guild_inventory_exhausted":discovery["guild_inventory_exhausted"].as_bool(),"failures_total":failures.len(),"failures":displayed,"failures_omitted":failures.len().saturating_sub(20),"thread_resume_cursor_count":discovery["thread_resume_cursors"].as_object().map_or(0,|map|map.len()),"reasons":reasons}})
}

fn coverage_summary(
    store: &Store,
    user: &str,
    guild: Option<&str>,
    channel: Option<&str>,
    as_of: &str,
) -> Result<Value, StoreError> {
    let counts = store.account_target_counts(user, guild, channel, as_of)?;
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
        json!({"observation_consistency":"sequential_cache_reads","as_of_basis":"inspection_started_at","scope":scope,"target_scope":{"guild_id":guild,"channel_id":channel},"account_user_id":user,"as_of":as_of,"inventory_updated_at":inventory_at,"inventory_stale":stale,"inventory_initialized":inventory.is_some(),"inventory":inventory.as_ref().map(inventory_summary),"targets_total":counts.total,"target_count_basis":"retained_discovered_targets","undiscovered_target_count":null,"enumerated":counts.total,"synced":counts.synced,"unfetched":counts.unfetched,"failed":counts.failed,"blocked":counts.blocked,"due":counts.due,"targets":[],"details_included":false,"has_more":null,"next_cursor":null,"complete":false,"reasons":["account_inventory_not_proven_exhaustive","history_permissions_not_verified","edits_and_deletions_not_continuously_observed"]}),
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
        let stale = target
            .last_checked_at
            .as_deref()
            .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
            .is_none_or(|at| Utc::now().signed_duration_since(at) > Duration::minutes(5));
        entries.push(json!({"channel_id":target.channel_id,"guild_id":target.guild_id,"present_in_current_inventory":generation==Some(target.generation.as_str()),"first_discovered_at":target.first_discovered_at,"last_checked_at":target.last_checked_at,"unfetched_since":if target.last_checked_at.is_none(){Some(&target.first_discovered_at)}else{None},"last_attempt_at":target.last_attempt_at,"next_check_at":target.next_check_at,"stale":stale,"history_status":target.history_status,"next_direction":target.next_direction,"covered_ranges":ranges,"ranges_truncated":ranges_truncated,"gaps":gaps,"covered_from":target.backfill_before,"covered_to":target.increment_after,"remaining_history":{"before_message_id":target.backfill_before,"status":"unknown"},"error":target.last_error.as_ref().map(|error|failure_summary(&json!({"error":error}))["error"].clone())}));
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
                refresh_inventory: false
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
            },
        )
        .await
        .unwrap();
        assert_eq!(report["status"], "cancelled");
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
        let result = collect_guild_inventory(&source, &store, "guilds")
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
        let result = collect_guild_inventory(&CancelGuildRead { store: &store }, &store, "guilds")
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
