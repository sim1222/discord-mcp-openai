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
    /// Guild (server) ID to list threads for (whole-guild scope).
    pub guild_id: Option<String>,
    /// Parent channel ID to list threads for (channel scope; covers forum
    /// posts and archived threads of that channel). Required when `guild_id`
    /// is omitted.
    pub channel_id: Option<String>,
    /// Which threads: `active`, `archived`, `joined`, or `all`. Defaults to
    /// `all` (active plus archived).
    pub filter: Option<String>,
    /// How many threads to return, 1-100. Defaults to 50.
    pub limit: Option<u32>,
    /// Resume cursor from a previous call's `next_cursor`.
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

    /// List threads of a guild or of one parent channel, paged.
    ///
    /// Returns JSON: `{"threads": [{"id", "guild_id", "name", "kind",
    /// "parent_id", "topic", "archived", "locked", "last_message_id",
    /// "message_count", "member_count"}], "next_cursor": ..., "has_more":
    /// bool}`. Covers active, archived and (with `filter: "joined"`) joined
    /// threads — including forum posts, which are threads and are not visible
    /// from the parent channel's message list alone. Give `channel_id` to
    /// scope to one parent channel (this is the reliable path under a user
    /// account), or `guild_id` for a whole-guild listing. Thread IDs are taken
    /// from Discord's response; never guess them from message IDs.
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
