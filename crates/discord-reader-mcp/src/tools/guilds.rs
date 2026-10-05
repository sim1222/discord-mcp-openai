//! Guild-level tools: server listing.

use rmcp::{model::CallToolResult, tool, tool_router, ErrorData};
use serde_json::json;

use super::DiscordReaderTools;

#[tool_router(vis = "pub(crate)", router = guilds_router)]
impl DiscordReaderTools {
    /// List the Discord servers (guilds) this account can see.
    ///
    /// Returns JSON: `{"guilds": [{"id": "...", "name": "..."}]}`. Use the
    /// returned IDs with `list_channels`.
    #[tool(name = "list_guilds")]
    pub async fn list_guilds(&self) -> Result<CallToolResult, ErrorData> {
        self.call_tool("list_guilds", json!({})).await
    }
}
