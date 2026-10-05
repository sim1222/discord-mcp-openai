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
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ListThreadsArgs {
    /// Guild (server) ID to list active threads for.
    pub guild_id: String,
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

    /// List the active threads of a guild.
    ///
    /// Returns JSON: `{"threads": [{"id", "guild_id", "name", "type",
    /// "parent_id", "topic"}]}`. Only threads currently active are listed;
    /// read a specific one with `read_thread`.
    #[tool(name = "list_threads")]
    pub async fn list_threads(
        &self,
        Parameters(args): Parameters<ListThreadsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool("list_threads", json!({"guild_id": args.guild_id}))
            .await
    }
}
