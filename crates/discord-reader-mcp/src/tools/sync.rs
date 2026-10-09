//! Sync and coverage tools: what has been checked, what changed, and
//! resumable background differential sync.

use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{limit_or, DiscordReaderTools};

/// Aggregate by default; inspect a bounded page with include_details or channel_id.
#[derive(Debug, Clone, Default, Deserialize, serde::Serialize, JsonSchema)]
pub struct StatusArgs {
    pub guild_id: Option<String>,
    pub channel_id: Option<String>,
    #[serde(default)]
    pub include_details: bool,
    /// Detail rows per page, 1..100, default 50.
    pub limit: Option<u32>,
    /// Scope-bound opaque cursor returned with details.
    pub cursor: Option<String>,
}

/// Arguments for `list_changed_channels`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct ChangedChannelsArgs {
    /// Restrict to one guild (server) ID.
    pub guild_id: Option<String>,
    /// How many channels to return, 1-100. Defaults to 50.
    pub limit: Option<u32>,
    /// Resume cursor from a previous call's `next_cursor`.
    pub cursor: Option<String>,
}

/// Arguments for `start_sync`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct StartSyncArgs {
    /// What to sync: `mentions`, `replies`, `changed_channels`, `all` or `refetch`.
    /// Defaults to `changed_channels`.
    pub scope: Option<String>,
    /// Restrict to one guild (server) ID.
    pub guild_id: Option<String>,
    /// Restrict to an explicit list of channel IDs.
    pub channel_ids: Option<Vec<String>>,
    /// Maximum retained metadata lookups for `refetch`, 1-1000, defaults to 100.
    pub max_messages: Option<u32>,
    /// Exclusive per-channel boundaries from `progress.next_refetch_before`.
    /// Only valid with `scope: "refetch"`; omit to retry every pending row.
    pub refetch_before: Option<std::collections::HashMap<String, String>>,
}

/// Arguments for `get_sync_progress`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct SyncProgressArgs {
    /// Job ID returned by `start_sync`.
    pub job_id: String,
}

#[tool_router(vis = "pub(crate)", router = sync_router)]
impl DiscordReaderTools {
    /// List cached channels whose last observed message has not been synced.
    ///
    /// Returns JSON: `{"changed_channels": [{"channel_id", "guild_id",
    /// "last_message_id", "last_synced_message_id", "last_activity_at",
    /// "last_fetched_at", "backfill", "reason"}], "next_cursor": ..., "has_more":
    /// bool}`. This cache-only probe makes no Discord requests. First refresh
    /// `list_channels` to update the observed `last_message_id`; the result
    /// is relative to the local sync cursor, not a live new-message feed.
    /// `reason: "never_synced"` includes old channels without a sync cursor;
    /// `reason: "newer_activity"` means observed activity exceeds that cursor.
    /// Listing freshness does not establish complete history or inbox coverage.
    #[tool(name = "list_changed_channels")]
    pub async fn list_changed_channels(
        &self,
        Parameters(args): Parameters<ChangedChannelsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 50, 100)?;
        self.call_tool(
            "list_changed_channels",
            json!({
                "guild_id": args.guild_id,
                "limit": limit,
                "cursor": args.cursor,
            }),
        )
        .await
    }

    /// Show aggregate sync counts by default, or scoped, paginated channel details.
    ///
    /// Returns JSON: `{"db_id": ..., "generated_at": ..., "cached_messages":
    /// N, "channels_tracked": N, "coverage": [{"channel_id", "covered_from",
    /// "covered_to", "has_gaps", "backfill", "last_synced_message_id",
    /// "oldest_synced_message_id", "last_message_id", "last_activity_at",
    /// "last_fetched_at"}], "deletions_observed": N, "metrics":
    /// {"http_requests", "rate_limit_waits", "rate_limit_wait_ms", ...}}`.
    /// `db_id` identifies the cache database and its generation, so you can
    /// tell whether a restart or a different connection changed what is
    /// searchable. `metrics` exposes internal HTTP request counts and 429
    /// waits so bulk operations can be costed.
    #[tool(name = "get_sync_status")]
    pub async fn get_sync_status(
        &self,
        Parameters(args): Parameters<StatusArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "get_sync_status",
            serde_json::to_value(args)
                .map_err(|_| ErrorData::internal_error("invalid status arguments", None))?,
        )
        .await
    }

    /// Start a bounded metadata refresh or recent-message sync job.
    ///
    /// `scope: "refetch"` refreshes retained messages with unknown metadata,
    /// up to `max_messages` attempts. Pass `progress.next_refetch_before` as
    /// `refetch_before` to continue past attempted rows, including failures;
    /// omit it to retry all pending rows. Updated rows persist across restarts.
    /// Lookup failures remain pending
    /// and appear as `done_with_gaps`; historical coverage is not inferred.
    /// Other scopes fetch latest messages from known channels, with
    /// `changed_channels` restricted to cached unsynced activity. Refresh
    /// channel listings first. This is a recent snapshot, not a history crawl.
    /// Returns `{"job_id": ...}`.
    /// Job IDs expire when the daemon restarts.
    #[tool(name = "start_sync")]
    pub async fn start_sync(
        &self,
        Parameters(args): Parameters<StartSyncArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "start_sync",
            json!({
                "scope": args.scope,
                "guild_id": args.guild_id,
                "channel_ids": args.channel_ids,
                "max_messages": args.max_messages,
                "refetch_before": args.refetch_before,
            }),
        )
        .await
    }

    /// Read the progress of a sync job started with `start_sync`.
    ///
    /// Returns JSON: `{"progress": {"job_id", "status": "running"|"done"|
    /// "paused"|"done_with_gaps"|"failed", "channels_total", "channels_done", "channels_failed":
    /// [{"channel_id", "error": {...}}], "started_at", "finished_at",
    /// "next_cursor", "attempted", "refetched", "remaining", "remaining_channels",
    /// "next_refetch_before"}}`.
    /// Refetch counters are metadata attempts and pending retained rows,
    /// not claims that historical ranges are complete. `remaining: null`
    /// means the count is not known. Returns `{"progress": null}` when unknown
    /// (for example after a daemon restart). Failed channels carry a
    /// structured error (`error_source`, `code`, `discord_code`, `retryable`)
    /// and are never counted as "no new messages".
    #[tool(name = "get_sync_progress")]
    pub async fn get_sync_progress(
        &self,
        Parameters(args): Parameters<SyncProgressArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool("get_sync_progress", json!({"job_id": args.job_id}))
            .await
    }
}
