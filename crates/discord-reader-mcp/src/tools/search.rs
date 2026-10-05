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
}

#[tool_router(vis = "pub(crate)", router = search_router)]
impl DiscordReaderTools {
    /// Full-text search over Discord messages already fetched into the local
    /// cache.
    ///
    /// Returns JSON: `{"results": [{"message": {...}, "relevance": 1.23}],
    /// "source": "local-fts", "cached_messages": 1234}`. Higher `relevance`
    /// means a better match. Only messages that were fetched before (by
    /// `recent_messages`, `messages_before`, `message_context`, ...) are
    /// searchable; set `refresh: true` with a `channel_id` to fetch that
    /// channel's recent messages first. Narrow with `guild_id`, `channel_id`,
    /// `author_id`, `after`, `before` before asking for wide searches.
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
            }),
        )
        .await
    }
}
