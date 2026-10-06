//! Discord-backed implementation of [`ReaderApi`].
//!
//! Everything here is read-only: the only network operations are the
//! allowlisted `GET` requests of [`DiscordRequest`]. Fetched data is written
//! to the SQLite cache; search reads the cache first.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use discord_api::types::{
    AttachmentView, Channel, ChannelView, DmView, Guild, GuildView, Message, MessageSnapshotView,
    MessageView, Role, User,
};
use discord_api::{ApiError, DiscordRequest, ErrorSource, SharedDiscordClient};
use discord_store::{SearchQuery, Store};
use serde_json::{json, Value};

use crate::rpc::{
    AfterParams, BeforeParams, ChangedChannelsParams, ChannelParams, ContextParams, GuildIdParams,
    InboxFilter, MemberParams, MessageEventsParams, MessageParams, ReaderApi, RpcError,
    SearchParams, SyncProgressParams, SyncStartParams, ThreadParams, ThreadsParams,
};

/// Reads Discord on demand and caches the results.
pub struct DaemonApi {
    client: SharedDiscordClient,
    store: Arc<Store>,
    /// Memoized current user id (`GetCurrentUser`), fetched once per process.
    me_id: Mutex<Option<String>>,
    /// In-memory job registry for `start_sync` / `get_sync_progress`.
    jobs: Arc<Mutex<HashMap<String, Value>>>,
}

impl std::fmt::Debug for DaemonApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonApi")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

fn api_error(error: discord_api::DiscordError) -> RpcError {
    // DiscordError display strings never contain the credential.
    RpcError::internal(error.to_string())
}

fn store_error(error: discord_store::StoreError) -> RpcError {
    RpcError::internal(format!("cache error: {error}"))
}

/// Map a structured [`ApiError`] to a machine-readable [`RpcError`].
///
/// The `error_source` selects the JSON-RPC code so callers can branch on
/// retry behaviour without parsing prose:
/// `-32000` Discord, `-32001` transport, `-32002` rate limit, `-32003`
/// client/invalid. The full structured error is embedded in the message as
/// JSON so nothing is flattened into an empty result.
fn rpc_error_from_api(api: ApiError, context: Option<Value>) -> RpcError {
    let code = match api.error_source {
        ErrorSource::Discord => -32000,
        ErrorSource::Transport => -32001,
        ErrorSource::RateLimit => -32002,
        ErrorSource::Client => -32003,
    };
    let payload = json!({"error": api, "context": context});
    let message = serde_json::to_string(&payload).unwrap_or_default();
    RpcError::new(code, message)
}

/// One classified inbox hit (mention / reply).
#[derive(Debug, Clone)]
struct InboxHit {
    view: MessageView,
    matched_by: &'static str,
    matched_role_ids: Vec<String>,
    guild_id: Option<String>,
    channel_id: String,
    /// Numeric snowflake key for newest-first ordering.
    key: u128,
    /// ISO timestamp for `after`/`before` window filtering.
    timestamp: String,
}

impl DaemonApi {
    pub fn new(client: SharedDiscordClient, store: Arc<Store>) -> Self {
        Self {
            client,
            store,
            me_id: Mutex::new(None),
            jobs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn fetch_messages(
        &self,
        channel_id: &str,
        limit: u32,
        before: Option<&str>,
        after: Option<&str>,
        around: Option<&str>,
    ) -> Result<Vec<Message>, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::GetMessages {
                channel_id: channel_id.to_string(),
                limit: limit.min(100) as u8,
                before: before.map(str::to_string),
                after: after.map(str::to_string),
                around: around.map(str::to_string),
            })
            .await
            .map_err(api_error)?;
        let messages: Vec<Message> = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected message payload: {e}")))?;
        self.store.insert_messages(&messages).map_err(store_error)?;

        // Record confirmed coverage + the resume cursor for this page.
        if !messages.is_empty() {
            let min_id = messages
                .iter()
                .map(|m| m.id.clone())
                .min_by_key(|id| snowflake_num(id));
            let max_id = messages
                .iter()
                .map(|m| m.id.clone())
                .max_by_key(|id| snowflake_num(id));
            if let (Some(min_id), Some(max_id)) = (min_id, max_id) {
                let now = discord_store::sqlite::now_iso();
                // A `before` page that comes back short reached the channel's
                // beginning: the backfill is complete.
                let backfill_complete =
                    before.is_some() && (messages.len() as u32) < limit.min(100);
                if let Err(error) = self.store.record_coverage(
                    channel_id,
                    &min_id,
                    &max_id,
                    &now,
                    backfill_complete,
                ) {
                    tracing::warn!(%error, "failed to record coverage");
                }
                if let Err(error) =
                    self.store
                        .record_channel_sync(channel_id, Some(&max_id), None, &now)
                {
                    tracing::warn!(%error, "failed to record channel sync");
                }
                if backfill_complete {
                    if let Err(error) = self.store.mark_backfill_complete(channel_id, &now) {
                        tracing::warn!(%error, "failed to mark backfill complete");
                    }
                }
            }
        }
        Ok(messages)
    }

    fn cache_message(&self, message: &Message) {
        if let Err(error) = self.store.insert_message(message) {
            tracing::warn!(%error, "failed to cache message");
        }
    }

    /// Current user id, fetched once and memoized.
    async fn me_user_id(&self) -> Result<String, RpcError> {
        if let Some(id) = self.me_id.lock().expect("me_id lock").clone() {
            return Ok(id);
        }
        let value = self
            .client
            .execute_for(&DiscordRequest::GetCurrentUser)
            .await
            .map_err(|api| rpc_error_from_api(api, None))?;
        let user: User = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected user payload: {e}")))?;
        if let Err(error) = self.store.upsert_user(&user) {
            tracing::warn!(%error, "failed to cache current user");
        }
        let id = user.id.clone();
        *self.me_id.lock().expect("me_id lock") = Some(id.clone());
        Ok(id)
    }

    /// Fetch the current user object, returning the normalized `me` view.
    async fn fetch_me(&self) -> Result<(User, Value), RpcError> {
        let value = self
            .client
            .execute_for(&DiscordRequest::GetCurrentUser)
            .await
            .map_err(|api| rpc_error_from_api(api, None))?;
        let user: User = serde_json::from_value(value.clone())
            .map_err(|e| RpcError::internal(format!("unexpected user payload: {e}")))?;
        if let Err(error) = self.store.upsert_user(&user) {
            tracing::warn!(%error, "failed to cache current user");
        }
        *self.me_id.lock().expect("me_id lock") = Some(user.id.clone());
        let me = json!({
            "id": user.id,
            "username": user.username,
            "global_name": user.global_name,
            "bot": user.bot,
            "discriminator": user.discriminator,
            "avatar": user.avatar,
        });
        Ok((user, me))
    }

    /// The current user's role ids in one guild (cached in the roles table).
    /// Returns an empty set when the membership cannot be read.
    async fn my_role_ids(&self, guild_id: &str) -> Vec<String> {
        let req = DiscordRequest::GetOwnGuildMember {
            guild_id: guild_id.to_string(),
        };
        let Ok(value) = self.client.execute_for(&req).await else {
            return Vec::new();
        };
        let roles = value
            .get("roles")
            .and_then(|r| r.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        // Cache role ids (names resolved later via `store.role_name`).
        for role_id in &roles {
            let role = Role {
                id: role_id.clone(),
                ..Role::default()
            };
            if let Err(error) = self.store.upsert_role(Some(guild_id), &role) {
                tracing::warn!(%error, "failed to cache role");
            }
        }
        roles
    }

    /// Resolve one reply reference. On 404/403 the reference is annotated;
    /// on success the fetched target is attached.
    async fn resolve_reply(&self, message: &mut Message) {
        let (ref_id, ref_channel) = match (
            message
                .message_reference
                .as_ref()
                .and_then(|r| r.message_id.clone()),
            message
                .message_reference
                .as_ref()
                .and_then(|r| r.channel_id.clone())
                .unwrap_or_else(|| message.channel_id.clone()),
        ) {
            (Some(id), channel) => (id, channel),
            (None, _) => return,
        };
        if message.referenced_message.is_some() {
            return;
        }
        let req = DiscordRequest::GetMessage {
            channel_id: ref_channel,
            message_id: ref_id,
        };
        match self.client.execute_for(&req).await {
            Ok(value) => match serde_json::from_value::<Message>(value) {
                Ok(target) => {
                    self.cache_message(&target);
                    message.referenced_message = Some(Box::new(target));
                }
                Err(error) => {
                    tracing::warn!(%error, "malformed referenced message");
                }
            },
            Err(api) => {
                let status = match api.http_status {
                    Some(404) => Some("deleted"),
                    Some(403) => Some("forbidden"),
                    _ => None,
                };
                if let Some(status) = status {
                    message.referenced_message_status = Some(status.to_string());
                }
            }
        }
    }

    /// Resolve reply references for a batch, capped at `budget` network calls.
    async fn resolve_replies_capped(&self, messages: &mut [Message], budget: usize) {
        let mut attempts = 0;
        for message in messages.iter_mut() {
            if attempts >= budget {
                break;
            }
            let unresolved = message.message_reference.is_some()
                && message.referenced_message.is_none()
                && message.referenced_message_status.is_none();
            if unresolved {
                attempts += 1;
                self.resolve_reply(message).await;
            }
        }
    }

    /// Enumerate the candidate channel ids for an inbox scan.
    async fn inbox_candidate_channels(
        &self,
        guild_id: Option<&str>,
        channel_id: Option<&str>,
    ) -> Vec<String> {
        if let Some(channel_id) = channel_id {
            return vec![channel_id.to_string()];
        }
        if let Some(guild_id) = guild_id {
            if let Ok(channels) = self.store.channels(guild_id) {
                let ids: Vec<String> = channels.into_iter().map(|c| c.id).collect();
                if !ids.is_empty() {
                    return ids;
                }
            }
        }
        // Fall back to every channel the sync table knows about.
        self.store
            .channel_sync_all(500)
            .map(|rows| rows.into_iter().map(|r| r.channel_id).collect())
            .unwrap_or_default()
    }

    /// Classify one message against the current user: `direct`, `reply`,
    /// `role` or `everyone`. Returns `None` when the message is not an inbox
    /// hit at all.
    async fn classify_inbox(
        &self,
        message: &Message,
        me_id: &str,
        my_roles: &[String],
        replies_only: bool,
    ) -> Option<(&'static str, Vec<String>)> {
        if !replies_only && message.mentions.iter().any(|u| u.id == me_id) {
            return Some(("direct", Vec::new()));
        }
        // Reply: the referenced message was authored by me.
        if let Some(reference) = &message.message_reference {
            if let Some(ref_id) = reference.message_id.as_deref() {
                let authored_by_me = match message.referenced_message.as_deref() {
                    Some(target) => target.author.as_ref().map(|a| a.id.as_str()) == Some(me_id),
                    None => self
                        .store
                        .get_message(ref_id)
                        .ok()
                        .flatten()
                        .and_then(|row| row.author_id)
                        .map(|id| id == me_id)
                        .unwrap_or(false),
                };
                if authored_by_me {
                    return Some(("reply", Vec::new()));
                }
            }
        }
        if replies_only {
            return None;
        }
        // Role mention: intersection with my roles in the message's guild.
        if !message.mention_roles.is_empty() && !my_roles.is_empty() {
            let overlap: Vec<String> = message
                .mention_roles
                .iter()
                .filter(|r| my_roles.iter().any(|m| m == *r))
                .cloned()
                .collect();
            if !overlap.is_empty() {
                return Some(("role", overlap));
            }
        }
        if message.mention_everyone == Some(true) {
            return Some(("everyone", Vec::new()));
        }
        None
    }

    /// Shared implementation for `list_mentions` / `list_replies`.
    async fn inbox_scan(
        &self,
        params: &InboxFilter,
        replies_only: bool,
    ) -> Result<Value, RpcError> {
        let me_id = self.me_user_id().await?;
        let channel_ids = self
            .inbox_candidate_channels(params.guild_id.as_deref(), params.channel_id.as_deref())
            .await;

        let mut hits: Vec<InboxHit> = Vec::new();
        let mut channels_scanned = 0u64;
        let mut channels_skipped = 0u64;
        let source = if params.refresh {
            "discord+cache"
        } else {
            "cache"
        };

        for channel_id in &channel_ids {
            // Role cache is per guild; reuse whatever we have for this channel.
            let guild_id = params.guild_id.clone();
            let my_roles = match &guild_id {
                Some(guild_id) => self.my_role_ids(guild_id).await,
                None => Vec::new(),
            };
            let rows = self
                .store
                .channel_messages(channel_id, 200, None, None)
                .map_err(store_error)?;
            let mut messages: Vec<Message> = Vec::new();
            for row in &rows {
                if let Some(json) = &row.message_json {
                    if let Ok(view) = serde_json::from_str::<MessageView>(json) {
                        // Rebuild a minimal `Message` from the normalized view.
                        messages.push(message_from_view(view));
                    }
                }
            }
            if params.refresh {
                match self.fetch_messages(channel_id, 100, None, None, None).await {
                    Ok(fresh) => {
                        messages = fresh;
                    }
                    Err(error) => {
                        // Structured, machine-readable failure; never folded
                        // into an empty result.
                        return Err(error);
                    }
                }
            }
            if messages.is_empty() {
                channels_skipped += 1;
                continue;
            }
            channels_scanned += 1;
            for message in &messages {
                let Some((matched_by, matched_role_ids)) = self
                    .classify_inbox(message, &me_id, &my_roles, replies_only)
                    .await
                else {
                    continue;
                };
                hits.push(InboxHit {
                    view: MessageView::from(message),
                    matched_by,
                    matched_role_ids,
                    guild_id: message.guild_id.clone().or_else(|| guild_id.clone()),
                    channel_id: channel_id.clone(),
                    key: snowflake_num(&message.id),
                    timestamp: message.timestamp.clone(),
                });
            }
        }

        // Time-window filtering.
        if let Some(after) = params.after.as_deref() {
            hits.retain(|h| h.timestamp.as_str() >= after);
        }
        if let Some(before) = params.before.as_deref() {
            hits.retain(|h| h.timestamp.as_str() < before);
        }
        // Newest first, capped at `limit`.
        hits.sort_by_key(|hit| std::cmp::Reverse(hit.key));
        let total = hits.len();
        let limit = params.limit.clamp(1, 100) as usize;
        hits.truncate(limit);

        let entries: Vec<Value> = hits
            .iter()
            .map(|h| {
                json!({
                    "message": h.view,
                    "matched_by": h.matched_by,
                    "matched_role_ids": h.matched_role_ids,
                    "guild_id": h.guild_id,
                    "channel_id": h.channel_id,
                })
            })
            .collect();

        // Coverage summary over the scanned channels.
        let mut covered_from: Option<String> = None;
        let mut covered_to: Option<String> = None;
        let mut channels_checked = 0u64;
        for channel_id in &channel_ids {
            if let Ok(Some((from, to, _))) = self.store.coverage_envelope(channel_id) {
                channels_checked += 1;
                covered_from = Some(match covered_from {
                    Some(existing) if snowflake_num(&existing) <= snowflake_num(&from) => existing,
                    _ => from,
                });
                covered_to = Some(match covered_to {
                    Some(existing) if snowflake_num(&existing) >= snowflake_num(&to) => existing,
                    _ => to,
                });
            }
        }

        Ok(json!({
            "mentions": entries,
            "checked": {
                "channels_scanned": channels_scanned,
                "channels_skipped": channels_skipped,
                "source": source,
            },
            "coverage": {
                "channels_checked": channels_checked,
                "channels_total_known": channel_ids.len(),
                "from": covered_from,
                "to": covered_to,
                "complete": false,
            },
            "next_cursor": Value::Null,
            "has_more": total > limit,
        }))
    }
}

/// Rebuild a minimal `Message` from a normalized `MessageView` (used to run
/// inbox classification over cached rows without re-fetching).
fn message_from_view(view: MessageView) -> Message {
    let mut message = Message {
        id: view.id.clone(),
        channel_id: view.channel_id.clone(),
        guild_id: view.guild_id.clone(),
        author: Some(User {
            id: view.author.id.clone(),
            username: Some(view.author.name.clone()),
            ..User::default()
        }),
        content: view.content.clone(),
        timestamp: view.timestamp.clone(),
        edited_timestamp: view.edited_timestamp.clone(),
        mentions: view
            .mentions
            .iter()
            .map(|a| User {
                id: a.id.clone(),
                username: Some(a.name.clone()),
                ..User::default()
            })
            .collect(),
        mention_roles: view.mention_roles.clone(),
        mention_everyone: view.mention_everyone,
        kind: view.message_type.unwrap_or(0),
        pinned: view.pinned,
        flags: view.flags,
        webhook_id: view.webhook_id.clone(),
        ..Message::default()
    };
    if let Some(reference) = &view.reply_to {
        message.message_reference = Some(discord_api::types::MessageReference {
            message_id: Some(reference.message_id.clone()),
            channel_id: reference.channel_id.clone(),
            guild_id: reference.guild_id.clone(),
            fail_if_not_exists: None,
        });
    }
    message
}

/// Numeric snowflake value, saturating at 0 for malformed ids.
fn snowflake_num(id: &str) -> u128 {
    id.parse::<u128>().unwrap_or(0)
}

#[async_trait::async_trait]
impl ReaderApi for DaemonApi {
    async fn ping(&self) -> Result<Value, RpcError> {
        // Deliberately touches neither Discord nor the cache: this is the
        // health check for the RPC socket.
        Ok(json!({"ok": true}))
    }

    async fn list_guilds(&self) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::ListGuilds {
                limit: 200,
                before: None,
                after: None,
            })
            .await
            .map_err(api_error)?;
        let guilds: Vec<Guild> = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected guild payload: {e}")))?;
        for guild in &guilds {
            if let Err(error) = self.store.upsert_guild(guild) {
                tracing::warn!(%error, "failed to cache guild");
            }
        }
        let views: Vec<GuildView> = guilds.iter().map(GuildView::from).collect();
        Ok(json!({"guilds": views}))
    }

    async fn list_channels(&self, params: GuildIdParams) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::GetGuildChannels {
                guild_id: params.guild_id.clone(),
            })
            .await
            .map_err(api_error)?;
        let mut channels: Vec<Channel> = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected channel payload: {e}")))?;
        for channel in &mut channels {
            if channel.guild_id.is_none() {
                channel.guild_id = Some(params.guild_id.clone());
            }
            if let Err(error) = self.store.upsert_channel(channel) {
                tracing::warn!(%error, "failed to cache channel");
            }
        }
        let mut views: Vec<ChannelView> = channels.iter().map(ChannelView::from).collect();
        views.sort_by(|a, b| {
            a.parent_id
                .is_none()
                .cmp(&b.parent_id.is_none())
                .then(a.name.cmp(&b.name))
        });
        Ok(json!({"channels": views}))
    }

    async fn recent_messages(&self, params: ChannelParams) -> Result<Value, RpcError> {
        let mut messages = self
            .fetch_messages(&params.channel_id, params.limit, None, None, None)
            .await?;
        self.resolve_replies_capped(&mut messages, 5).await;
        let views: Vec<MessageView> = messages.iter().map(MessageView::from).collect();
        Ok(json!({"channel_id": params.channel_id, "messages": views}))
    }

    async fn messages_before(&self, params: BeforeParams) -> Result<Value, RpcError> {
        let mut messages = self
            .fetch_messages(
                &params.channel_id,
                params.limit,
                Some(&params.before_message_id),
                None,
                None,
            )
            .await?;
        self.resolve_replies_capped(&mut messages, 5).await;
        let views: Vec<MessageView> = messages.iter().map(MessageView::from).collect();
        Ok(json!({"channel_id": params.channel_id, "messages": views}))
    }

    async fn get_message(&self, params: MessageParams) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::GetMessage {
                channel_id: params.channel_id.clone(),
                message_id: params.message_id.clone(),
            })
            .await
            .map_err(api_error)?;
        let mut message: Message = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected message payload: {e}")))?;
        self.resolve_reply(&mut message).await;
        self.cache_message(&message);
        let view = MessageView::from(&message);
        Ok(json!({"message": view}))
    }

    async fn message_context(&self, params: ContextParams) -> Result<Value, RpcError> {
        // The message itself, then the surrounding conversation.
        let value = self
            .client
            .execute(DiscordRequest::GetMessage {
                channel_id: params.channel_id.clone(),
                message_id: params.message_id.clone(),
            })
            .await
            .map_err(api_error)?;
        let mut message: Message = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected message payload: {e}")))?;
        self.resolve_reply(&mut message).await;
        self.cache_message(&message);

        let before = if params.before == 0 {
            Vec::new()
        } else {
            self.fetch_messages(
                &params.channel_id,
                params.before,
                Some(&params.message_id),
                None,
                None,
            )
            .await?
        };
        let after = if params.after == 0 {
            Vec::new()
        } else {
            self.fetch_messages(
                &params.channel_id,
                params.after,
                None,
                Some(&params.message_id),
                None,
            )
            .await?
        };

        // Normalize to chronological order so that
        // `before ++ [message] ++ after` reads naturally.
        let mut before_views: Vec<MessageView> = before.iter().map(MessageView::from).collect();
        before_views.sort_by_key(|m| snowflake_key(&m.id));
        let mut after_views: Vec<MessageView> = after.iter().map(MessageView::from).collect();
        after_views.sort_by_key(|m| snowflake_key(&m.id));

        Ok(json!({
            "channel_id": params.channel_id,
            "message_id": params.message_id,
            "before": before_views,
            "message": MessageView::from(&message),
            "after": after_views,
        }))
    }

    async fn search_messages(&self, params: SearchParams) -> Result<Value, RpcError> {
        // Optional targeted refresh: only the explicitly named channel is
        // fetched, and only its recent messages. Never a crawl.
        if params.refresh {
            if let Some(channel_id) = params.channel_id.as_deref() {
                self.fetch_messages(channel_id, 50, None, None, None)
                    .await?;
            }
        }

        let query = SearchQuery::new(params.query.clone())
            .limit(params.limit.clamp(1, 200).saturating_add(1))
            .guild_id(params.guild_id.clone())
            .channel_id(params.channel_id.clone())
            .author_id(params.author_id.clone())
            .after(params.after.clone())
            .before(params.before.clone());

        let offset = params
            .next_cursor
            .as_deref()
            .and_then(|c| c.parse::<u32>().ok())
            .unwrap_or(params.offset) as usize;
        let hits = self.store.search(&query).map_err(store_error)?;
        let limit = params.limit.clamp(1, 200) as usize;
        let total = hits.len();
        let page = hits
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
        let results: Vec<_> = page
            .iter()
            .map(|hit| {
                json!({
                    "message": hit.message.to_view(),
                    "relevance": hit.relevance,
                })
            })
            .collect();
        let has_more = (offset + page.len()) < total;
        let next_cursor = if has_more {
            Some((offset + page.len()).to_string())
        } else {
            None
        };

        // Coverage summary for the searched channel (when scoped to one).
        let coverage = params
            .channel_id
            .as_deref()
            .and_then(|cid| self.store.coverage_envelope(cid).ok().flatten())
            .map(|(from, to, _)| json!({"from": from, "to": to}))
            .unwrap_or_else(|| json!({"from": Value::Null, "to": Value::Null}));

        Ok(json!({
            "results": results,
            "source": "local-fts",
            "cached_messages": self.store.message_count().map_err(store_error)?,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "coverage": coverage,
        }))
    }

    async fn read_thread(&self, params: ThreadParams) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::GetChannel {
                channel_id: params.thread_id.clone(),
            })
            .await
            .map_err(api_error)?;
        let thread: Channel = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected channel payload: {e}")))?;
        if let Err(error) = self.store.upsert_channel(&thread) {
            tracing::warn!(%error, "failed to cache thread");
        }

        let messages = self
            .fetch_messages(&params.thread_id, params.limit, None, None, None)
            .await?;
        let views: Vec<MessageView> = messages.iter().map(MessageView::from).collect();
        Ok(json!({
            "thread": ChannelView::from(&thread),
            "messages": views,
        }))
    }

    async fn list_threads(&self, params: ThreadsParams) -> Result<Value, RpcError> {
        // Map the friendly `filter` to the archived/joined query flags.
        // `GetGuildActiveThreads` is bot-only (20002) for user accounts, so we
        // route through the user-account friendly thread listings instead.
        let filter = params.filter.as_deref().unwrap_or("all");
        let archived = match filter {
            "archived" => Some(true),
            "active" => Some(false),
            _ => params.include_archived,
        };
        let joined = match filter {
            "joined" => Some(true),
            _ => None,
        };
        let cursor = params.cursor.clone();

        let value = if let Some(channel_id) = params.channel_id.clone() {
            self.client
                .execute_for(&DiscordRequest::ListChannelThreads {
                    channel_id,
                    archived,
                    joined,
                    sort_by: None,
                    sort_order: None,
                    limit: params.limit.clamp(1, 100),
                    before: cursor,
                })
                .await
                .map_err(|api| rpc_error_from_api(api, None))?
        } else if let Some(guild_id) = params.guild_id.clone() {
            self.client
                .execute_for(&DiscordRequest::SearchGuildThreads {
                    guild_id,
                    archived,
                    joined,
                    sort_by: None,
                    sort_order: None,
                    limit: params.limit.clamp(1, 100),
                    before: cursor,
                })
                .await
                .map_err(|api| rpc_error_from_api(api, None))?
        } else {
            return Err(RpcError::invalid_params("guild_id or channel_id required"));
        };

        let empty = Vec::new();
        let threads = value
            .get("threads")
            .and_then(|t| t.as_array())
            .unwrap_or(&empty);
        let has_more = value
            .get("has_more")
            .and_then(|h| h.as_bool())
            .unwrap_or(false);

        let mut entries = Vec::new();
        let mut next_cursor: Option<String> = None;
        for thread in threads {
            match serde_json::from_value::<Channel>(thread.clone()) {
                Ok(channel) => {
                    if let Err(error) = self.store.upsert_channel(&channel) {
                        tracing::warn!(%error, "failed to cache thread");
                    }
                    let meta = channel.thread_metadata.as_ref();
                    entries.push(json!({
                        "id": channel.id,
                        "guild_id": channel.guild_id,
                        "name": channel.name,
                        "kind": channel.kind,
                        "parent_id": channel.parent_id,
                        "topic": channel.topic,
                        "archived": meta.and_then(|m| m.archived),
                        "locked": meta.and_then(|m| m.locked),
                        "last_message_id": channel.last_message_id,
                        "message_count": channel.message_count,
                        "member_count": channel.member_count,
                    }));
                    next_cursor = Some(channel.id.clone());
                }
                Err(error) => {
                    tracing::warn!(%error, "skipping malformed thread entry");
                }
            }
        }
        Ok(json!({
            "threads": entries,
            "next_cursor": if has_more { next_cursor } else { None },
            "has_more": has_more,
        }))
    }

    async fn list_dms(&self) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::ListUserDmChannels)
            .await
            .map_err(api_error)?;
        let channels: Vec<Channel> = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected dm payload: {e}")))?;
        let dms: Vec<_> = channels
            .iter()
            .filter(|c| c.kind == 1 || c.kind == 3)
            .map(DmView::from)
            .collect();
        Ok(json!({"dms": dms}))
    }

    async fn messages_after(&self, params: AfterParams) -> Result<Value, RpcError> {
        let mut messages = self
            .fetch_messages(
                &params.channel_id,
                params.limit,
                None,
                Some(&params.after_message_id),
                None,
            )
            .await?;
        // Discord returns `after` pages oldest-first; normalize to newest-first.
        self.resolve_replies_capped(&mut messages, 5).await;
        messages.sort_by_key(|m| std::cmp::Reverse(snowflake_num(&m.id)));
        let views: Vec<MessageView> = messages.iter().map(MessageView::from).collect();
        Ok(json!({"channel_id": params.channel_id, "messages": views}))
    }

    async fn get_me(&self) -> Result<Value, RpcError> {
        let (_user, me) = self.fetch_me().await?;
        Ok(json!({"me": me}))
    }

    async fn get_capabilities(&self) -> Result<Value, RpcError> {
        Ok(json!({
            "auth": "user",
            "methods": {
                "ping": {"supported": true},
                "get_me": {"supported": true},
                "get_capabilities": {"supported": true},
                "list_guilds": {"supported": true},
                "list_channels": {"supported": true},
                "list_dms": {"supported": true},
                "recent_messages": {"supported": true},
                "messages_before": {"supported": true},
                "messages_after": {"supported": true},
                "get_message": {"supported": true, "notes": "reply targets resolved via GET message"},
                "get_message_raw": {"supported": true, "notes": "returns verbatim wire payload"},
                "message_context": {"supported": true},
                "search_messages": {"supported": true, "notes": "local FTS over cached messages"},
                "search_server_side": {"supported": "partial", "notes": "channel-scoped for user accounts"},
                "list_mentions": {"supported": true},
                "list_replies": {"supported": true},
                "read_thread": {"supported": true},
                "list_threads": {"supported": "partial", "notes": "per-channel/user-account endpoints; GetGuildActiveThreads is bot-only (20002)"},
                "list_changed_channels": {"supported": true},
                "get_sync_status": {"supported": true},
                "start_sync": {"supported": true},
                "get_sync_progress": {"supported": true},
                "get_member": {"supported": "partial", "notes": "only self member is fetchable with a user account + GET-only"},
                "get_message_events": {"supported": true},
                "get_attachment": {"supported": true}
            },
            "limitations": [
                "read-only: only GET requests are issued; no write operations exist",
                "GetGuildActiveThreads is bot-only (20002) for user accounts; list_threads uses per-channel/thread-search endpoints",
                "search_server_side is channel-scoped for user accounts (no global search)",
                "get_member only returns the current user's membership",
                "no background crawling; data is fetched on demand and cached in SQLite"
            ]
        }))
    }

    async fn get_message_raw(&self, params: MessageParams) -> Result<Value, RpcError> {
        let raw = self
            .client
            .execute_for(&DiscordRequest::GetMessage {
                channel_id: params.channel_id.clone(),
                message_id: params.message_id.clone(),
            })
            .await
            .map_err(|api| rpc_error_from_api(api, None))?;
        let message: Message = serde_json::from_value(raw.clone())
            .map_err(|e| RpcError::internal(format!("unexpected message payload: {e}")))?;
        self.cache_message(&message);
        let normalized = MessageView::from(&message);
        let fields_present: Vec<String> = raw
            .as_object()
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();
        Ok(json!({
            "raw": raw,
            "normalized": normalized,
            "fields_present": fields_present,
            "operation": format!("GET /channels/{}/messages/{}", params.channel_id, params.message_id),
            "api_base": discord_api::endpoints::API_BASE,
            "fetched_at": discord_store::sqlite::now_iso(),
        }))
    }

    async fn search_server_side(&self, params: SearchParams) -> Result<Value, RpcError> {
        let Some(channel_id) = params.channel_id.clone() else {
            return Err(RpcError::invalid_params(
                "channel_id is required for search_server_side",
            ));
        };
        let offset = params
            .next_cursor
            .as_deref()
            .and_then(|c| c.parse::<u32>().ok())
            .unwrap_or(params.offset);
        let value = self
            .client
            .execute_for(&DiscordRequest::SearchChannelMessages {
                channel_id: channel_id.clone(),
                content: Some(params.query.clone()),
                offset,
                limit: params.limit.clamp(1, 25),
                sort_by: params.sort.clone(),
                sort_order: params.sort_order.clone(),
            })
            .await
            .map_err(|api| {
                rpc_error_from_api(
                    api,
                    Some(json!({"channel_id": channel_id, "query": params.query})),
                )
            })?;

        let empty = Vec::new();
        let groups = value
            .get("messages")
            .and_then(|m| m.as_array())
            .unwrap_or(&empty);
        let total_results = value
            .get("total_results")
            .and_then(|t| t.as_i64())
            .unwrap_or(0);

        let mut results = Vec::new();
        let mut parsed: Vec<Message> = Vec::new();
        for group in groups {
            let Some(first) = group.as_array().and_then(|a| a.first()) else {
                continue;
            };
            match serde_json::from_value::<Message>(first.clone()) {
                Ok(message) => {
                    parsed.push(message.clone());
                    let context: Vec<Value> = group
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .skip(1)
                                .filter_map(|m| serde_json::from_value::<Message>(m.clone()).ok())
                                .map(|m| MessageView::from(&m))
                                .map(|v| json!(v))
                                .collect()
                        })
                        .unwrap_or_default();
                    results.push(json!({
                        "message": MessageView::from(&message),
                        "context": context,
                    }));
                }
                Err(error) => {
                    tracing::warn!(%error, "skipping malformed search hit");
                }
            }
        }
        if let Err(error) = self.store.insert_messages(&parsed) {
            tracing::warn!(%error, "failed to cache search hits");
        }

        let has_more = ((offset as u64) + results.len() as u64) < total_results as u64;
        Ok(json!({
            "results": results,
            "total_results": total_results,
            "next_cursor": if has_more { Some((offset + results.len() as u32).to_string()) } else { Option::<String>::None },
            "has_more": has_more,
            "source": "discord-search",
            "uncovered_ranges": [],
        }))
    }

    async fn list_mentions(&self, params: InboxFilter) -> Result<Value, RpcError> {
        self.inbox_scan(&params, false).await
    }

    async fn list_replies(&self, params: InboxFilter) -> Result<Value, RpcError> {
        self.inbox_scan(&params, true).await
    }

    async fn list_changed_channels(
        &self,
        params: ChangedChannelsParams,
    ) -> Result<Value, RpcError> {
        let offset = params
            .cursor
            .as_deref()
            .and_then(|c| c.parse::<u32>().ok())
            .unwrap_or(0);
        let changed = self
            .store
            .changed_channels(params.limit.saturating_add(offset))
            .map_err(store_error)?;
        // Optional guild filter: keep channels that belong to the guild.
        let mut entries = Vec::new();
        for (channel_id, last_message_id) in changed {
            if let Some(guild_id) = params.guild_id.as_deref() {
                let in_guild = self
                    .store
                    .channels(guild_id)
                    .map(|cs| cs.iter().any(|c| c.id == channel_id))
                    .unwrap_or(false);
                if !in_guild {
                    continue;
                }
            }
            let sync = self.store.channel_sync(&channel_id).map_err(store_error)?;
            let guild = params.guild_id.clone();
            entries.push(json!({
                "channel_id": channel_id,
                "guild_id": guild,
                "last_message_id": last_message_id,
                "last_synced_message_id": sync.as_ref().and_then(|s| s.last_synced_message_id.clone()),
                "last_activity_at": sync.as_ref().and_then(|s| s.last_activity_at.clone()),
                "last_fetched_at": sync.as_ref().and_then(|s| s.last_fetched_at.clone()),
                "backfill": sync.as_ref().map(|s| s.backfill.clone()).unwrap_or_else(|| "partial".into()),
            }));
        }
        let total = entries.len();
        let limit = params.limit.clamp(1, 100) as usize;
        let start = (offset as usize).min(total);
        let end = (start + limit).min(total);
        let page: Vec<Value> = entries.into_iter().skip(start).take(end - start).collect();
        let has_more = end < total;
        Ok(json!({
            "changed_channels": page,
            "next_cursor": if has_more { Some(end.to_string()) } else { Option::<String>::None },
            "has_more": has_more,
        }))
    }

    async fn get_sync_status(&self) -> Result<Value, RpcError> {
        let rows = self.store.channel_sync_all(500).map_err(store_error)?;
        let coverage: Vec<Value> = rows
            .iter()
            .map(|row| {
                let envelope = self.store.coverage_envelope(&row.channel_id).ok().flatten();
                let (covered_from, covered_to, has_gaps) = match envelope {
                    Some((from, to, gaps)) => (Some(from), Some(to), gaps),
                    None => (None, None, false),
                };
                json!({
                    "channel_id": row.channel_id,
                    "covered_from": covered_from,
                    "covered_to": covered_to,
                    "has_gaps": has_gaps,
                    "backfill": row.backfill,
                    "last_synced_message_id": row.last_synced_message_id,
                    "oldest_synced_message_id": row.oldest_synced_message_id,
                    "last_message_id": row.last_message_id,
                    "last_activity_at": row.last_activity_at,
                    "last_fetched_at": row.last_fetched_at,
                })
            })
            .collect();
        let deletions: u64 = rows
            .iter()
            .map(|r| {
                self.store
                    .deletions_after(&r.channel_id, None, u32::MAX)
                    .map(|d| d.len() as u64)
                    .unwrap_or(0)
            })
            .sum();
        Ok(json!({
            "db_id": format!("discord-cache-{}", std::process::id()),
            "generated_at": discord_store::sqlite::now_iso(),
            "cached_messages": self.store.message_count().map_err(store_error)?,
            "channels_tracked": rows.len(),
            "coverage": coverage,
            "deletions_observed": deletions,
            "metrics": self.client.metrics().snapshot(),
        }))
    }

    async fn start_sync(&self, params: SyncStartParams) -> Result<Value, RpcError> {
        let mut registry = self.jobs.lock().expect("jobs lock");
        let job_id = format!("sync-{}-{}", std::process::id(), registry.len() + 1);
        let started_at = discord_store::sqlite::now_iso();

        // Resolve target channels.
        let targets: Vec<String> = if let Some(ids) = params.channel_ids.clone() {
            ids
        } else {
            self.store
                .changed_channels(200)
                .map(|rows| rows.into_iter().map(|(id, _)| id).collect())
                .unwrap_or_default()
        };
        let channels_total = targets.len();
        let state = json!({
            "job_id": job_id,
            "status": "running",
            "channels_total": channels_total,
            "channels_done": 0,
            "channels_failed": [],
            "started_at": started_at,
            "finished_at": Value::Null,
            "next_cursor": Value::Null,
        });
        registry.insert(job_id.clone(), state);
        drop(registry);

        // Spawn the cache-warming loop; it shares the job registry via Arc.
        let client = Arc::clone(&self.client);
        let store = Arc::clone(&self.store);
        let jobs = Arc::clone(&self.jobs);
        let job_id_task = job_id.clone();
        tokio::spawn(async move {
            let mut done = 0u64;
            let mut failed: Vec<Value> = Vec::new();
            for channel_id in &targets {
                let req = DiscordRequest::GetMessages {
                    channel_id: channel_id.clone(),
                    limit: 50,
                    before: None,
                    after: None,
                    around: None,
                };
                match client.execute_for(&req).await {
                    Ok(value) => {
                        if let Ok(messages) = serde_json::from_value::<Vec<Message>>(value) {
                            let _ = store.insert_messages(&messages);
                            if let (Some(min), Some(max)) = (
                                messages
                                    .iter()
                                    .map(|m| m.id.clone())
                                    .min_by_key(|i| snowflake_num(i)),
                                messages
                                    .iter()
                                    .map(|m| m.id.clone())
                                    .max_by_key(|i| snowflake_num(i)),
                            ) {
                                let now = discord_store::sqlite::now_iso();
                                let _ = store.record_coverage(channel_id, &min, &max, &now, false);
                                let _ =
                                    store.record_channel_sync(channel_id, Some(&max), None, &now);
                            }
                        }
                        done += 1;
                    }
                    Err(api) => {
                        failed.push(json!({"channel_id": channel_id, "error": api}));
                    }
                }
                let mut reg = jobs.lock().expect("jobs lock");
                if let Some(entry) = reg.get_mut(&job_id_task) {
                    entry["channels_done"] = json!(done);
                    entry["channels_failed"] = json!(failed);
                }
            }
            let mut reg = jobs.lock().expect("jobs lock");
            if let Some(entry) = reg.get_mut(&job_id_task) {
                entry["status"] = json!("done");
                entry["finished_at"] = json!(discord_store::sqlite::now_iso());
            }
        });
        Ok(json!({"job_id": job_id}))
    }

    async fn get_sync_progress(&self, params: SyncProgressParams) -> Result<Value, RpcError> {
        let jobs = self.jobs.lock().expect("jobs lock");
        let progress = jobs.get(&params.job_id).cloned();
        Ok(json!({"progress": progress}))
    }

    async fn get_member(&self, params: MemberParams) -> Result<Value, RpcError> {
        let me_id = self.me_user_id().await?;
        if let Some(user_id) = params.user_id.as_deref() {
            if user_id != me_id {
                return Ok(json!({
                    "member": Value::Null,
                    "reason": "only self member is fetchable with user account + GET-only",
                }));
            }
        }
        let value = self
            .client
            .execute_for(&DiscordRequest::GetOwnGuildMember {
                guild_id: params.guild_id.clone(),
            })
            .await
            .map_err(|api| rpc_error_from_api(api, Some(json!({"guild_id": params.guild_id}))))?;
        let user_id = value
            .get("user")
            .and_then(|u| u.get("id"))
            .and_then(|i| i.as_str())
            .unwrap_or(&me_id)
            .to_string();
        let nick = value
            .get("nick")
            .and_then(|n| n.as_str())
            .map(str::to_string);
        let joined_at = value
            .get("joined_at")
            .and_then(|j| j.as_str())
            .map(str::to_string);
        let role_ids: Vec<String> = value
            .get("roles")
            .and_then(|r| r.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let mut roles = Vec::new();
        for role_id in &role_ids {
            let name = self.store.role_name(role_id).map_err(store_error)?;
            roles.push(json!({"id": role_id, "name": name}));
        }
        Ok(json!({
            "member": {
                "user_id": user_id,
                "guild_id": params.guild_id,
                "nick": nick,
                "roles": roles,
                "joined_at": joined_at,
            }
        }))
    }

    async fn get_message_events(&self, params: MessageEventsParams) -> Result<Value, RpcError> {
        let req = DiscordRequest::GetMessageEvents {
            channel_id: params.channel_id.clone(),
            message_id: params.message_id.clone(),
        };
        match self.client.execute_for(&req).await {
            Ok(value) => {
                let events = value.as_array().cloned().unwrap_or_else(|| {
                    value
                        .get("events")
                        .and_then(|e| e.as_array())
                        .cloned()
                        .unwrap_or_default()
                });
                Ok(json!({"events": events, "fetched_at": discord_store::sqlite::now_iso()}))
            }
            Err(api) => {
                if api.http_status == Some(404) {
                    Ok(json!({"events": [], "note": "no scheduled events linked"}))
                } else {
                    Err(rpc_error_from_api(api, None))
                }
            }
        }
    }

    async fn get_attachment(&self, params: MessageParams) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute_for(&DiscordRequest::GetMessage {
                channel_id: params.channel_id.clone(),
                message_id: params.message_id.clone(),
            })
            .await
            .map_err(|api| rpc_error_from_api(api, None))?;
        let message: Message = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected message payload: {e}")))?;
        self.cache_message(&message);
        let attachments: Vec<AttachmentView> = message
            .attachments
            .iter()
            .map(AttachmentView::from)
            .collect();
        let snapshot_attachments: Vec<AttachmentView> = message
            .message_snapshots
            .iter()
            .map(MessageSnapshotView::from)
            .flat_map(|s| s.attachments)
            .collect();
        Ok(json!({
            "attachments": attachments,
            "snapshot_attachments": snapshot_attachments,
        }))
    }
}

/// Numeric ordering key for a Discord snowflake (falling back to 0).
fn snowflake_key(id: &str) -> u64 {
    id.parse::<u64>().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{BeforeParams, ChannelParams, ContextParams, MessageParams, SearchParams};
    use discord_api::{DiscordClient, Token, TokenKind};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    const SECRET: &str = "SECRET-USER-TOKEN-DO-NOT-LEAK";

    /// Minimal HTTP/1.1 test server: records request lines and replays canned
    /// responses. Used to prove that only GET requests ever leave the client.
    struct MockServer {
        base: String,
        requests: Arc<Mutex<Vec<String>>>,
    }

    async fn spawn_mock(responses: VecDeque<(u16, String)>) -> MockServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let requests_clone = Arc::clone(&requests);
        let responses = Arc::new(Mutex::new(responses));

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let requests = Arc::clone(&requests_clone);
                let responses = Arc::clone(&responses);
                tokio::spawn(async move {
                    let (read_half, mut write_half) = stream.into_split();
                    let mut lines = BufReader::new(read_half).lines();
                    let mut request_line = String::new();
                    while let Ok(Some(line)) = lines.next_line().await {
                        if request_line.is_empty() {
                            request_line = line;
                        } else if line.is_empty() {
                            break;
                        }
                    }
                    requests.lock().unwrap().push(request_line);
                    let (status, body) = responses
                        .lock()
                        .unwrap()
                        .pop_front()
                        .unwrap_or((404, "{\"message\":\"not mocked\"}".to_string()));
                    let reason = if status == 200 { "OK" } else { "Mock" };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = write_half.write_all(response.as_bytes()).await;
                    let _ = write_half.flush().await;
                });
            }
        });

        MockServer { base, requests }
    }

    fn api_for(base: &str) -> (DaemonApi, Arc<discord_store::Store>) {
        let client = DiscordClient::with_options(
            Token::new(SECRET).unwrap(),
            TokenKind::User,
            base.to_string(),
            2,
        )
        .unwrap();
        let store = Arc::new(discord_store::Store::open_in_memory().unwrap());
        let api = DaemonApi::new(Arc::new(client), Arc::clone(&store));
        (api, store)
    }

    #[tokio::test]
    async fn list_guilds_is_get_only_and_cached() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"[{"id":"42","name":"ZENVR"}]"#.to_string(),
        )]))
        .await;
        let (api, store) = api_for(&mock.base);

        let result = api.list_guilds().await.unwrap();
        let text = serde_json::to_string(&result).unwrap();
        assert!(text.contains("ZENVR"));
        assert!(
            !text.contains(SECRET),
            "response must not contain the token"
        );

        let requests = mock.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0].starts_with("GET "),
            "only GET may reach Discord, got: {}",
            requests[0]
        );
        assert!(requests[0].contains("/users/@me/guilds"));

        let cached = store.guilds().unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].name, "ZENVR");
    }

    #[tokio::test]
    async fn recent_messages_are_cached_and_searchable() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"[{"id":"10","channel_id":"200","guild_id":"42","author":{"id":"7","username":"alice"},"content":"展軸祭の準備をしています","timestamp":"2026-10-05T00:00:00.000000+00:00"}]"#
                .to_string(),
        )]))
        .await;
        let (api, _store) = api_for(&mock.base);

        let result = api
            .recent_messages(ChannelParams {
                channel_id: "200".into(),
                limit: 50,
            })
            .await
            .unwrap();
        assert!(serde_json::to_string(&result).unwrap().contains("展軸祭"));

        let search = api
            .search_messages(SearchParams {
                query: "展軸祭".into(),
                guild_id: Some("42".into()),
                channel_id: None,
                author_id: None,
                after: None,
                before: None,
                limit: 50,
                refresh: false,
                offset: 0,
                next_cursor: None,
                sort: None,
                sort_order: None,
            })
            .await
            .unwrap();
        let text = serde_json::to_string(&search).unwrap();
        assert!(text.contains("展軸祭の準備"), "search must hit the cache");
        assert!(text.contains("local-fts"));
        assert!(!text.contains(SECRET));
    }

    #[tokio::test]
    async fn message_context_fetches_before_and_after() {
        let message = r#"{"id":"100","channel_id":"200","author":{"id":"7","username":"alice"},"content":"target","timestamp":"2026-10-05T00:00:00.000000+00:00"}"#;
        let before = r#"[{"id":"98","channel_id":"200","author":{"id":"7","username":"alice"},"content":"older two","timestamp":"2026-10-04T00:00:00.000000+00:00"},{"id":"99","channel_id":"200","author":{"id":"7","username":"alice"},"content":"older one","timestamp":"2026-10-04T01:00:00.000000+00:00"}]"#;
        let after = r#"[{"id":"101","channel_id":"200","author":{"id":"7","username":"alice"},"content":"newer one","timestamp":"2026-10-06T00:00:00.000000+00:00"}]"#;
        let mock = spawn_mock(VecDeque::from([
            (200, message.to_string()),
            (200, before.to_string()),
            (200, after.to_string()),
        ]))
        .await;
        let (api, _store) = api_for(&mock.base);

        let context = api
            .message_context(ContextParams {
                channel_id: "200".into(),
                message_id: "100".into(),
                before: 20,
                after: 20,
            })
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::to_value(&context).unwrap();
        assert_eq!(value["message"]["content"], "target");
        assert_eq!(value["before"][0]["content"], "older two");
        assert_eq!(value["before"][1]["content"], "older one");
        assert_eq!(value["after"][0]["content"], "newer one");

        let requests = mock.requests.lock().unwrap().clone();
        assert!(requests.iter().all(|r| r.starts_with("GET ")));
    }

    #[tokio::test]
    async fn messages_before_paging_uses_before_param() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"[{"id":"9","channel_id":"200","author":{"id":"7","username":"alice"},"content":"old","timestamp":"2026-10-04T00:00:00.000000+00:00"}]"#.to_string(),
        )]))
        .await;
        let (api, _store) = api_for(&mock.base);

        let result = api
            .messages_before(BeforeParams {
                channel_id: "200".into(),
                before_message_id: "10".into(),
                limit: 50,
            })
            .await
            .unwrap();
        assert!(serde_json::to_string(&result).unwrap().contains("old"));

        let requests = mock.requests.lock().unwrap().clone();
        assert!(requests[0].contains("before=10") || requests[0].contains("before=10&"));
    }

    #[tokio::test]
    async fn get_message_returns_normalized_view() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"{"id":"10","channel_id":"200","guild_id":"42","author":{"id":"7","username":"alice"},"content":"hello","timestamp":"2026-10-05T00:00:00.000000+00:00","attachments":[{"id":"a","filename":"x.png","url":"https://cdn/x.png","size":10,"content_type":"image/png"}]}"#.to_string(),
        )]))
        .await;
        let (api, _store) = api_for(&mock.base);

        let result = api
            .get_message(MessageParams {
                channel_id: "200".into(),
                message_id: "10".into(),
            })
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::to_value(&result).unwrap();
        assert_eq!(value["message"]["author"]["name"], "alice");
        assert_eq!(value["message"]["attachments"][0]["filename"], "x.png");
        assert!(!serde_json::to_string(&result).unwrap().contains(SECRET));
    }

    #[tokio::test]
    async fn debug_output_redacts_the_credential() {
        let mock = spawn_mock(VecDeque::new()).await;
        let (api, _store) = api_for(&mock.base);
        let dump = format!("{api:?}");
        assert!(!dump.contains(SECRET));
        assert!(dump.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn messages_after_uses_after_query_param() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"[{"id":"11","channel_id":"200","author":{"id":"7","username":"alice"},"content":"newer","timestamp":"2026-10-05T00:00:00.000000+00:00"}]"#.to_string(),
        )]))
        .await;
        let (api, _store) = api_for(&mock.base);

        let result = api
            .messages_after(AfterParams {
                channel_id: "200".into(),
                after_message_id: "10".into(),
                limit: 50,
            })
            .await
            .unwrap();
        assert!(serde_json::to_string(&result).unwrap().contains("newer"));

        let requests = mock.requests.lock().unwrap().clone();
        assert!(
            requests[0].contains("after=10"),
            "must page with after=, got: {}",
            requests[0]
        );
        assert!(!requests[0].contains("before="));
    }

    #[tokio::test]
    async fn recent_messages_records_coverage() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"[{"id":"10","channel_id":"200","author":{"id":"7","username":"alice"},"content":"one","timestamp":"2026-10-05T00:00:00.000000+00:00"},{"id":"20","channel_id":"200","author":{"id":"7","username":"alice"},"content":"two","timestamp":"2026-10-05T01:00:00.000000+00:00"}]"#.to_string(),
        )]))
        .await;
        let (api, store) = api_for(&mock.base);

        api.recent_messages(ChannelParams {
            channel_id: "200".into(),
            limit: 50,
        })
        .await
        .unwrap();

        let coverage = store.coverage("200").unwrap();
        assert!(
            !coverage.is_empty(),
            "record_coverage must leave a coverage range"
        );
        assert_eq!(coverage[0].from_id, "10");
        assert_eq!(coverage[0].to_id, "20");
        let sync = store.channel_sync("200").unwrap().unwrap();
        assert_eq!(sync.last_synced_message_id.as_deref(), Some("20"));
    }

    #[tokio::test]
    async fn get_message_raw_returns_raw_and_normalized() {
        let raw = r#"{"id":"10","channel_id":"200","guild_id":"42","author":{"id":"7","username":"alice"},"content":"hello","timestamp":"2026-10-05T00:00:00.000000+00:00","custom_field":"kept"}"#;
        let mock = spawn_mock(VecDeque::from([(200, raw.to_string())])).await;
        let (api, _store) = api_for(&mock.base);

        let result = api
            .get_message_raw(MessageParams {
                channel_id: "200".into(),
                message_id: "10".into(),
            })
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::to_value(&result).unwrap();
        // Raw is verbatim: unknown fields survive.
        assert_eq!(value["raw"]["custom_field"], "kept");
        assert_eq!(value["raw"]["content"], "hello");
        // Normalized view is present and typed.
        assert_eq!(value["normalized"]["author"]["name"], "alice");
        assert_eq!(value["normalized"]["content_kind"], "text");
        assert!(value["fields_present"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f == "custom_field"));
        assert_eq!(value["operation"], "GET /channels/200/messages/10");
        assert!(!serde_json::to_string(&result).unwrap().contains(SECRET));
    }

    #[tokio::test]
    async fn list_threads_with_channel_id_hits_channel_threads_endpoint() {
        let thread = r#"{"threads":[{"id":"300","guild_id":"42","name":"topic-a","type":11,"parent_id":"200","message_count":3,"member_count":2,"thread_metadata":{"archived":false,"locked":false}}]}"#;
        let mock = spawn_mock(VecDeque::from([(200, thread.to_string())])).await;
        let (api, store) = api_for(&mock.base);

        let result = api
            .list_threads(ThreadsParams {
                guild_id: None,
                channel_id: Some("200".into()),
                filter: Some("active".into()),
                include_archived: None,
                limit: 50,
                cursor: None,
            })
            .await
            .unwrap();
        let text = serde_json::to_string(&result).unwrap();
        assert!(text.contains("topic-a"));

        let requests = mock.requests.lock().unwrap().clone();
        assert!(
            requests[0].contains("/channels/200/threads"),
            "must hit the per-channel thread listing, got: {}",
            requests[0]
        );
        assert!(
            !requests[0].contains("/threads/active"),
            "must not use the bot-only active-threads endpoint, got: {}",
            requests[0]
        );
        assert!(requests[0].contains("archived=false"));
        // Threads are cached.
        let cached = store.channel_sync("300");
        assert!(cached.is_ok());
    }

    #[tokio::test]
    async fn list_mentions_classifies_direct_role_everyone_reply() {
        // me_id will be "7" (from GetCurrentUser).
        let me = r#"{"id":"7","username":"alice","global_name":"Alice"}"#;
        // My membership in guild 42 carries role 555.
        let member = r#"{"user":{"id":"7"},"roles":["555"],"nick":null,"joined_at":"2020-01-01T00:00:00.000000+00:00"}"#;
        // Channel 200 messages from cache: one direct mention, one role
        // mention, one @everyone, one reply to me.
        let mock = spawn_mock(VecDeque::from([
            (200, me.to_string()),
            (200, member.to_string()),
        ]))
        .await;
        let (api, store) = api_for(&mock.base);

        // Seed the cache directly (list_mentions reads cache when not refresh).
        let direct: Message = serde_json::from_value(json!({
            "id":"10","channel_id":"200","guild_id":"42",
            "author":{"id":"99","username":"bob"},
            "content":"hey @alice","timestamp":"2026-10-05T00:00:00.000000+00:00",
            "mentions":[{"id":"7","username":"alice"}]
        }))
        .unwrap();
        let role: Message = serde_json::from_value(json!({
            "id":"11","channel_id":"200","guild_id":"42",
            "author":{"id":"99","username":"bob"},
            "content":"@mods","timestamp":"2026-10-05T01:00:00.000000+00:00",
            "mention_roles":["555"]
        }))
        .unwrap();
        let everyone: Message = serde_json::from_value(json!({
            "id":"12","channel_id":"200","guild_id":"42",
            "author":{"id":"99","username":"bob"},
            "content":"@everyone","timestamp":"2026-10-05T02:00:00.000000+00:00",
            "mention_everyone":true
        }))
        .unwrap();
        let reply: Message = serde_json::from_value(json!({
            "id":"13","channel_id":"200","guild_id":"42",
            "author":{"id":"99","username":"bob"},
            "content":"agree","timestamp":"2026-10-05T03:00:00.000000+00:00",
            "message_reference":{"message_id":"100","channel_id":"200"}
        }))
        .unwrap();
        // The reply target (id 100) is authored by me.
        let target: Message = serde_json::from_value(json!({
            "id":"100","channel_id":"200","guild_id":"42",
            "author":{"id":"7","username":"alice"},
            "content":"original","timestamp":"2026-10-04T00:00:00.000000+00:00"
        }))
        .unwrap();
        for m in [&direct, &role, &everyone, &reply, &target] {
            store.insert_message(m).unwrap();
        }
        // Seed channel 200 so the guild's channel list is non-empty.
        store
            .upsert_channel(&Channel {
                id: "200".into(),
                guild_id: Some("42".into()),
                kind: 0,
                ..Channel::default()
            })
            .unwrap();
        // Cache my role 555 in guild 42.
        store
            .upsert_role(
                Some("42"),
                &Role {
                    id: "555".into(),
                    name: Some("mods".into()),
                    ..Role::default()
                },
            )
            .unwrap();

        let result = api
            .list_mentions(InboxFilter {
                guild_id: Some("42".into()),
                channel_id: None,
                after: None,
                before: None,
                limit: 50,
                refresh: false,
                next_cursor: None,
            })
            .await
            .unwrap();
        let text = serde_json::to_string(&result).unwrap();

        // Collect matched_by per message id.
        let value: serde_json::Value = serde_json::to_value(&result).unwrap();
        let mentions = value["mentions"].as_array().unwrap();
        let by_id = |id: &str| -> String {
            mentions
                .iter()
                .find(|m| m["message"]["id"] == id)
                .map(|m| m["matched_by"].as_str().unwrap().to_string())
                .unwrap_or_default()
        };
        assert_eq!(by_id("10"), "direct");
        assert_eq!(by_id("11"), "role");
        assert_eq!(by_id("12"), "everyone");
        assert_eq!(by_id("13"), "reply");
        // Role match carries the overlapping role id.
        let role_hit = mentions
            .iter()
            .find(|m| m["message"]["id"] == "11")
            .unwrap();
        assert_eq!(role_hit["matched_role_ids"][0], "555");
        assert!(!text.contains(SECRET));
    }
}
