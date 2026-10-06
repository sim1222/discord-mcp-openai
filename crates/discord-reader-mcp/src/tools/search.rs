//! Full-text search over cached Discord messages.

use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{limit_or, DiscordReaderTools};

/// Arguments for `search_messages`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct SearchMessagesArgs {
    /// Keywords to search for. All terms must appear (case-insensitive,
    /// whitespace-separated; CJK text works as substring-ish token matching).
    pub query: String,
    /// Restrict to one guild (server) ID.
    pub guild_id: Option<String>,
    /// Restrict to one channel ID.
    pub channel_id: Option<String>,
    /// Restrict to one author user ID.
    pub author_id: Option<String>,
    /// Only messages at or after this ISO-8601 timestamp
    /// (for example `2026-10-01T00:00:00Z`).
    pub after: Option<String>,
    /// Only messages at or before this ISO-8601 timestamp.
    pub before: Option<String>,
    /// How many results to return, 1-200. Defaults to 50.
    pub limit: Option<u32>,
    /// When true, re-fetch the targeted channel's recent messages from Discord
    /// before searching. Off by default: search reads the local cache only.
    pub refresh: Option<bool>,
    /// Resume cursor from a previous call's `next_cursor`.
    pub next_cursor: Option<String>,
}

/// Arguments for `search_server_side`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct SearchServerSideArgs {
    /// Query string (Discord search syntax: phrases in quotes, `OR`, `from:`,
    /// `mentions:`, `has:`, `before:`/`after:` and friends).
    pub query: String,
    /// Channel ID to search within. Required: Discord's message search is
    /// channel-scoped for user accounts.
    pub channel_id: String,
    /// How many results to return, 1-25. Defaults to 25.
    pub limit: Option<u32>,
    /// Sort: `timestamp` (default) or `relevance`.
    pub sort: Option<String>,
    /// `desc` (newest first, default) or `asc`.
    pub sort_order: Option<String>,
    /// Resume cursor from a previous call's `next_cursor`.
    pub next_cursor: Option<String>,
}

#[tool_router(vis = "pub(crate)", router = search_router)]
impl DiscordReaderTools {
    /// Full-text search over Discord messages already fetched into the local
    /// cache.
    ///
    /// Returns JSON: `{"results": [{"message": {...}, "relevance": 1.23}],
    /// "source": "local-fts", "cached_messages": 1234, "next_cursor": ...,
    /// "has_more": bool, "coverage": {"from": ..., "to": ...}}`. Higher
    /// `relevance` means a better match. Only messages that were fetched
    /// before (by `recent_messages`, `messages_before`, `message_context`,
    /// ...) are searchable; set `refresh: true` with a `channel_id` to fetch
    /// that channel's recent messages first. Narrow with `guild_id`,
    /// `channel_id`, `author_id`, `after`, `before` before asking for wide
    /// searches. Page with `next_cursor`/`has_more`; `coverage` states which
    /// span of messages the search actually covered, so zero results are
    /// meaningful only within that span.
    #[tool(name = "search_messages")]
    pub async fn search_messages(
        &self,
        Parameters(args): Parameters<SearchMessagesArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 50, 200)?;
        self.call_tool(
            "search_messages",
            json!({
                "query": args.query,
                "guild_id": args.guild_id,
                "channel_id": args.channel_id,
                "author_id": args.author_id,
                "after": args.after,
                "before": args.before,
                "limit": limit,
                "refresh": args.refresh.unwrap_or(false),
                "next_cursor": args.next_cursor,
            }),
        )
        .await
    }

    /// Search one channel using Discord's own search endpoint.
    ///
    /// Returns JSON: `{"results": [{"message": {...}, "context": [...]}],
    /// "total_results": N, "next_cursor": ..., "has_more": bool, "source":
    /// "discord-search", "uncovered_ranges": []}`. Unlike `search_messages`
    /// this queries Discord, so it finds messages never fetched locally — but
    /// Discord's message search is channel-scoped for user accounts, so
    /// `channel_id` is required and results cannot span servers. Supports
    /// Discord search syntax (quoted phrases, `OR`, `from:`, `mentions:`,
    /// `has:`, `before:`/`after:`). `next_cursor`/`has_more` page through
    /// `total_results`; failures (bot-only, forbidden) are reported as
    /// structured errors rather than an empty result.
    #[tool(name = "search_server_side")]
    pub async fn search_server_side(
        &self,
        Parameters(args): Parameters<SearchServerSideArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 25, 25)?;
        self.call_tool(
            "search_server_side",
            json!({
                "query": args.query,
                "channel_id": args.channel_id,
                "limit": limit,
                "sort": args.sort,
                "sort_order": args.sort_order,
                "next_cursor": args.next_cursor,
            }),
        )
        .await
    }
}
