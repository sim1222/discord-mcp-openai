//! Meta tools: identity and capability discovery.

use rmcp::{model::CallToolResult, tool, tool_router, ErrorData};
use serde_json::json;

use super::DiscordReaderTools;

#[tool_router(vis = "pub(crate)", router = meta_router)]
impl DiscordReaderTools {
    /// Show the current account's identity.
    ///
    /// Returns JSON: `{"me": {"id", "username", "global_name", "bot",
    /// "discriminator", "avatar"}}`. Use the `id` to interpret `mentions`,
    /// `matched_by` and `author.id` fields instead of inferring identity from
    /// DM authors.
    #[tool(name = "get_me")]
    pub async fn get_me(&self) -> Result<CallToolResult, ErrorData> {
        self.call_tool("get_me", json!({})).await
    }

    /// Show what this server supports under the current credential kind.
    ///
    /// Returns JSON: `{"auth": "user"|"bot", "methods": {"<tool>":
    /// {"supported": bool|"partial", "notes": ...}}, "limitations": [...]}`.
    /// Discord restricts some endpoints to bot tokens (for example the
    /// guild-wide active-thread listing and guild-wide message search, error
    /// code 20002); this table says which tools work, which are partial, and
    /// what to use instead. Check this before assuming a tool is callable.
    #[tool(name = "get_capabilities")]
    pub async fn get_capabilities(&self) -> Result<CallToolResult, ErrorData> {
        self.call_tool("get_capabilities", json!({})).await
    }
}
