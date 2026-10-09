//! Account observations and bounded retrieval, with explicit completeness limits.

use super::{limit_or, DiscordReaderTools};
use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct ReadStateArgs {
    /// Restrict to a guild; with channel_id this is an intersection.
    pub guild_id: Option<String>,
    /// Restrict to one channel or thread ID.
    pub channel_id: Option<String>,
    /// Obtain a fresh Gateway READY observation; defaults to true. False reads local observations.
    pub refresh: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct AccountSyncArgs {
    /// At most this many targets per fair round, 1..1000, default 50.
    pub max_targets: Option<u32>,
    /// Messages per target page, 1..100, default 100.
    pub page_size: Option<u32>,
    /// Refresh account inventory first, default true. False resumes known targets.
    pub refresh_inventory: Option<bool>,
}

/// Local confirmations only; this operation does not send a notification or modify Discord.
#[derive(Debug, Clone, Deserialize, serde::Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InboxStateArgs {
    pub channel_id: String,
    pub message_id: String,
    pub notified_at: Option<String>,
    pub snoozed_until: Option<String>,
    pub action_required: Option<bool>,
    pub action_evidence: Option<String>,
    pub completed_at: Option<String>,
    pub completion_evidence: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct CancelSyncArgs {
    /// Persist cancellation for this local account synchronization job.
    pub job_id: String,
}

#[tool_router(vis = "pub(crate)", router = account_router)]
impl DiscordReaderTools {
    /// Observe Discord-origin channel mention counts and last-read positions.
    /// Missing counts/positions are null with reasons; verified zero remains 0.
    /// READY source, account ID, observation time/version and scope are included.
    /// Guild UI badge aggregation is unavailable; it is never a cache search count.
    /// This fresh observation authenticates and heartbeats only; no message ACK is sent.
    #[tool(name = "get_read_state")]
    pub async fn get_read_state(
        &self,
        Parameters(args): Parameters<ReadStateArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool("get_read_state",json!({"guild_id":args.guild_id,"channel_id":args.channel_id,"refresh":args.refresh.unwrap_or(true)})).await
    }

    /// Report the account target ledger, listing freshness, missing history and failures.
    /// complete=false includes unknown inventory and history permissions, even if all known targets were checked.
    #[tool(name = "get_account_coverage")]
    pub async fn get_account_coverage(&self) -> Result<CallToolResult, ErrorData> {
        self.call_tool("get_account_coverage", json!({})).await
    }

    /// Run one bounded fair account synchronization round with persisted checkpoints.
    /// Untouched quiet channels precede attempted targets. Repeat until known initial pages are obtained;
    /// further rounds backfill older pages and poll new messages. Empty pages do not prove history permission.
    /// Refresh discovers current guilds, DMs, channels and bounded thread-search pages; discovery limits remain explicit.
    /// Use get_sync_progress(job_id); after restart start a new round from persisted checkpoints.
    #[tool(name = "start_account_sync")]
    pub async fn start_account_sync(
        &self,
        Parameters(args): Parameters<AccountSyncArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let max_targets = limit_or(args.max_targets, 50, 1000)?;
        let page_size = limit_or(args.page_size, 100, 100)?;
        self.call_tool("start_account_sync",json!({"max_targets":max_targets,"page_size":page_size,"refresh_inventory":args.refresh_inventory.unwrap_or(true)})).await
    }

    /// Record explicit local notification, snooze, required action and completion confirmations.
    /// Replaces the entire previous local state for this message; omitted fields become unknown.
    /// Required action and completion need evidence; completion also needs an RFC3339 timestamp.
    /// Acknowledging Discord or posting another message never completes the action. This does not send notifications.
    #[tool(name = "record_inbox_state")]
    pub async fn record_inbox_state(
        &self,
        Parameters(args): Parameters<InboxStateArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "record_inbox_state",
            serde_json::to_value(args)
                .map_err(|_| ErrorData::internal_error("invalid local state", None))?,
        )
        .await
    }

    /// Cancel a local account sync job at the next page boundary without changing Discord.
    /// Cancellation persists across restart and never triggers an alternate acquisition route.
    #[tool(name = "cancel_sync")]
    pub async fn cancel_sync(
        &self,
        Parameters(args): Parameters<CancelSyncArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool("cancel_sync", json!({"job_id":args.job_id}))
            .await
    }
}
