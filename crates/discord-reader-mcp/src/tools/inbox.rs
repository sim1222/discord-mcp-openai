//! Inbox tools: cross-server mentions and replies directed at the current
//! user. Both make the checked range explicit so an empty result can be
//! distinguished from an un-checked scope.

use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{limit_or, DiscordReaderTools};

/// Shared filters for `list_mentions` / `list_replies`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct InboxFilterArgs {
    /// Restrict to one guild (server) ID. Omit to check every visible guild.
    pub guild_id: Option<String>,
    /// Restrict to one channel ID.
    pub channel_id: Option<String>,
    /// Only messages at or after this ISO-8601 timestamp.
    pub after: Option<String>,
    /// Only messages at or before this ISO-8601 timestamp.
    pub before: Option<String>,
    /// How many results to return, 1-100. Defaults to 50.
    pub limit: Option<u32>,
    /// When true, also fetch recent messages for channels with new activity
    /// before classifying. Off by default: the local cache is scanned only.
    pub refresh: Option<bool>,
    /// Resume cursor from a previous call's `next_cursor`.
    pub next_cursor: Option<String>,
}

#[tool_router(vis = "pub(crate)", router = inbox_router)]
impl DiscordReaderTools {
    /// List mentions directed at this account, across servers.
    ///
    /// Returns JSON: `{"mentions": [{"message": {...}, "matched_by":
    /// "direct"|"reply"|"role"|"everyone", "matched_role_ids": [...],
    /// "guild_id": ..., "channel_id": ...}], "checked": {...}, "coverage":
    /// {...}, "next_cursor": ..., "has_more": bool}`. `matched_by` says why
    /// the message is directed at you; role matches list the roles that
    /// applied. The `coverage` object states which channels and time span were
    /// actually checked, so `"mentions": []` means "checked and none found"
    /// only when `coverage.complete` is true. Use `refresh: true` to also
    /// fetch recent messages for channels reporting new activity first.
    #[tool(name = "list_mentions")]
    pub async fn list_mentions(
        &self,
        Parameters(args): Parameters<InboxFilterArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 50, 100)?;
        self.call_tool(
            "list_mentions",
            json!({
                "guild_id": args.guild_id,
                "channel_id": args.channel_id,
                "after": args.after,
                "before": args.before,
                "limit": limit,
                "refresh": args.refresh.unwrap_or(false),
                "next_cursor": args.next_cursor,
            }),
        )
        .await
    }

    /// List replies to this account's messages, across servers.
    ///
    /// Same shape as `list_mentions` but every entry has `matched_by:
    /// "reply"`; the referenced message (your original) is resolved inline in
    /// `message.reply_to.referenced` when available. The `coverage` object
    /// states the checked scope exactly as in `list_mentions`.
    #[tool(name = "list_replies")]
    pub async fn list_replies(
        &self,
        Parameters(args): Parameters<InboxFilterArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 50, 100)?;
        self.call_tool(
            "list_replies",
            json!({
                "guild_id": args.guild_id,
                "channel_id": args.channel_id,
                "after": args.after,
                "before": args.before,
                "limit": limit,
                "refresh": args.refresh.unwrap_or(false),
                "next_cursor": args.next_cursor,
            }),
        )
        .await
    }
}
