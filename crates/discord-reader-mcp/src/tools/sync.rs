//! Sync and coverage tools: what has been checked, what changed, and
//! resumable background differential sync.

use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{limit_or, DiscordReaderTools};

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
    /// What to sync: `mentions`, `replies`, `changed_channels` or `all`.
    /// Defaults to `changed_channels`.
    pub scope: Option<String>,
    /// Restrict to one guild (server) ID.
    pub guild_id: Option<String>,
    /// Restrict to an explicit list of channel IDs.
    pub channel_ids: Option<Vec<String>>,
}

/// Arguments for `get_sync_progress`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct SyncProgressArgs {
    /// Job ID returned by `start_sync`.
    pub job_id: String,
}

#[tool_router(vis = "pub(crate)", router = sync_router)]
impl DiscordReaderTools {
    /// List channels with new activity since the last sync.
    ///
    /// Returns JSON: `{"changed_channels": [{"channel_id", "guild_id",
    /// "last_message_id", "last_synced_message_id", "last_activity_at",
    /// "last_fetched_at", "backfill"}], "next_cursor": ..., "has_more":
    /// bool}`. This is a cheap probe (no message bodies fetched): it compares
    /// each channel's Discord-reported `last_message_id` against the sync
    /// cursor. Use it to decide which channels are worth reading instead of
    /// re-reading every channel.
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

    /// Show sync state and confirmed coverage per channel.
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
    pub async fn get_sync_status(&self) -> Result<CallToolResult, ErrorData> {
        self.call_tool("get_sync_status", json!({})).await
    }

    /// Start a resumable differential sync job.
    ///
    /// Fetches recent messages for channels with new activity (not a full
    /// history crawl), following rate limits. Returns JSON: `{"job_id": ...}`.
    /// Poll `get_sync_progress` for per-channel success/failure, the next
    /// cursor and remaining wait. Jobs live for this daemon process; sync
    /// positions themselves are persisted and survive restarts.
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
            }),
        )
        .await
    }

    /// Read the progress of a sync job started with `start_sync`.
    ///
    /// Returns JSON: `{"progress": {"job_id", "status": "running"|"done"|
    /// "failed", "channels_total", "channels_done", "channels_failed":
    /// [{"channel_id", "error": {...}}], "started_at", "finished_at",
    /// "next_cursor"}}` or `{"progress": null}` when the job id is unknown
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
