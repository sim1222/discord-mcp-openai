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
    /// Restrict to one guild (server) ID. Combined with channel_id as an
    /// intersection. Omit to scan retained cache channels, not every live guild.
    pub guild_id: Option<String>,
    /// Restrict to one channel ID. Known conflicting guild/channel scopes
    /// fail; unknown guild associations are excluded, never filled from a filter.
    pub channel_id: Option<String>,
    /// Only messages at or after this RFC3339 timestamp (inclusive).
    pub after: Option<String>,
    /// Only messages at or before this RFC3339 timestamp (inclusive).
    pub before: Option<String>,
    /// How many results to return, 1-100. Defaults to 50.
    pub limit: Option<u32>,
    /// When true, fetch the newest 100 messages of each selected channel
    /// before classifying. This is not a requested-period backfill.
    pub refresh: Option<bool>,
    /// Resume cursor from a previous call's `next_cursor`.
    pub next_cursor: Option<String>,
}

#[tool_router(vis = "pub(crate)", router = inbox_router)]
impl DiscordReaderTools {
    /// List mentions directed at this account, across servers.
    ///
    /// Returns JSON: `{"mentions": [{"message": {...}, "matched_by":
    /// "dm"|"group_dm"|"direct"|"reply"|"role"|"everyone", "matched_role_ids": [...],
    /// "guild_id": ..., "channel_id": ...}], "checked": {...}, "coverage":
    /// {...}, "next_cursor": ..., "has_more": bool}`. `matched_by` says why
    /// the message is directed at you; role matches list the roles that
    /// applied. Self-authored mentions/replies/everyone posts are included.
    /// `coverage.requested_window` has inclusive time bounds; `checked_ranges`
    /// and `uncovered_ranges` use message IDs, while `cached_history` describes
    /// all cached retrieval evidence. `reasons` explains incompleteness.
    /// A bounded explicit channel window may be complete only when every
    /// conservative boundary-millisecond ID is covered and classification is
    /// certain. This means cached observations, not current edit/deletion
    /// monitoring or complete history. Open bounds and unproven guild-wide
    /// channel inventories stay incomplete. Use `refresh: true` for one newest
    /// page per selected channel, not period backfill. Empty incomplete results
    /// do not establish that no contact occurred. Paging uses an immutable persisted snapshot cursor; keep scope/window unchanged and do not refresh on resume.
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
