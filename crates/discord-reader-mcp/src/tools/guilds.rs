//! Guild-level tools: server listing and member/role lookup.

use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::DiscordReaderTools;

/// Arguments for `get_member`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct GetMemberArgs {
    /// Guild (server) ID.
    pub guild_id: String,
    /// User ID to look up. Omit (or pass your own ID) to get your own member
    /// entry. Other users are not fetchable with a user account and read-only
    /// GET access, and are reported as such rather than failing silently.
    pub user_id: Option<String>,
}

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

    /// Show this account's member entry and roles in one guild.
    ///
    /// Returns JSON: `{"member": {"user_id", "guild_id", "nick", "roles":
    /// [{"id", "name"}], "joined_at"}}`. Use `roles` to decide whether a role
    /// mention (`matched_by: "role"` from `list_mentions`) actually applies to
    /// you, and to resolve role IDs to names.
    #[tool(name = "get_member")]
    pub async fn get_member(
        &self,
        Parameters(args): Parameters<GetMemberArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "get_member",
            json!({"guild_id": args.guild_id, "user_id": args.user_id}),
        )
        .await
    }
}
