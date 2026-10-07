//! Thread reading tools.

use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{limit_or, DiscordReaderTools};

/// Arguments for `read_thread`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ReadThreadArgs {
    /// Thread ID (threads are Discord channels; IDs look like channel IDs).
    pub thread_id: String,
    /// How many messages to return, 1-100. Defaults to 100.
    pub limit: Option<u32>,
}

/// Arguments for `list_threads`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct ListThreadsArgs {
    /// Optional guild constraint for the parent channel. Guild-only scope is
    /// unsupported; provide channel_id.
    pub guild_id: Option<String>,
    /// Parent channel ID to list threads for (channel scope; covers forum
    /// posts and archived threads of that channel). Required. Supported parent
    /// kinds are text (0), announcement (5), forum (15), and media (16).
    pub channel_id: Option<String>,
    /// Which threads: `active`, `archived`, or `all`. Defaults to `all`.
    /// `joined` produces an explicit unsupported_filter error.
    pub filter: Option<String>,
    /// Requested page size, 1-100. Defaults to 50; Discord search caps each
    /// page at 25, reported as effective_limit.
    pub limit: Option<u32>,
    /// Opaque next_cursor from the same channel/filter. This is an offset
    /// cursor, not a thread ID; search results can change between requests.
    pub cursor: Option<String>,
}

#[tool_router(vis = "pub(crate)", router = threads_router)]
impl DiscordReaderTools {
    /// Read the messages of a thread.
    ///
    /// Returns JSON: `{"thread": {...}, "messages": [...]}`. The thread entry
    /// has `id`, `name`, `parent_id` and `kind`; messages use the normalized
    /// message shape.
    #[tool(name = "read_thread")]
    pub async fn read_thread(
        &self,
        Parameters(args): Parameters<ReadThreadArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 100, 100)?;
        self.call_tool(
            "read_thread",
            json!({"thread_id": args.thread_id, "limit": limit}),
        )
        .await
    }

    /// Search threads of one parent channel, paged.
    ///
    /// Returns JSON: `{"threads": [{"id", "guild_id", "name", "kind",
    /// "parent_id", "topic", "archived", "locked", "last_message_id",
    /// "message_count", "member_count"}], "next_cursor": ..., "has_more":
    /// bool, "effective_limit": ..., "search_window_exhausted": bool,
    /// "coverage": {"complete": false, "reasons": [...]}}`.
    /// Uses GET /channels/{id}/threads/search after checking parent metadata.
    /// Unsupported parent kinds, guild-only scope, joined filter, unavailable
    /// indexes and malformed responses return structured errors, not empty
    /// success. Bot-only and permission failures remain distinct. Continue
    /// with next_cursor unchanged. Results are search-index observations, not
    /// proof of all accessible threads; offset paging is not a frozen snapshot.
    /// At the search offset bound, the final valid page is retained with
    /// has_more=true, next_cursor=null, search_window_exhausted=true, and a
    /// coverage reason. This means continuation is unavailable, not completion.
    /// Thread IDs come from Discord; never derive them from message IDs.
    #[tool(name = "list_threads")]
    pub async fn list_threads(
        &self,
        Parameters(args): Parameters<ListThreadsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 50, 100)?;
        self.call_tool(
            "list_threads",
            json!({
                "guild_id": args.guild_id,
                "channel_id": args.channel_id,
                "filter": args.filter,
                "limit": limit,
                "cursor": args.cursor,
            }),
        )
        .await
    }
}
