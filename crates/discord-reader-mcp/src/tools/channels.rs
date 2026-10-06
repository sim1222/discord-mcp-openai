//! Channel listing and DM / group DM exploration tools.

use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::DiscordReaderTools;

/// Arguments for `list_channels`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ListChannelsArgs {
    /// Discord guild (server) ID, as returned by `list_guilds`.
    pub guild_id: String,
}

#[tool_router(vis = "pub(crate)", router = channels_router)]
impl DiscordReaderTools {
    /// List the channels of one Discord server.
    ///
    /// Returns JSON: `{"channels": [{"id", "guild_id", "name", "kind",
    /// "parent_id", "topic", "last_message_id", "last_activity_at"}]}` sorted
    /// with categories first. `kind` is the Discord channel type integer (0
    /// text, 2 voice, 4 category, 5 announcement, 15 forum, ...).
    /// `last_message_id` is Discord's latest message snowflake and
    /// `last_activity_at` our timestamp for it when known: together with
    /// `list_changed_channels` they let you skip quiet channels instead of
    /// re-reading every one.
    #[tool(name = "list_channels")]
    pub async fn list_channels(
        &self,
        Parameters(args): Parameters<ListChannelsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool("list_channels", json!({"guild_id": args.guild_id}))
            .await
    }

    /// List direct message and group DM conversations visible to this account.
    ///
    /// Returns JSON: `{"dms": [{"channel_id", "kind", "name", "participants":
    /// [{"id", "name"}], "last_message_id"}]}`. `kind` is 1 for a 1:1 DM and 3
    /// for a group DM. Use `channel_id` with `recent_messages` to read one.
    #[tool(name = "list_dms")]
    pub async fn list_dms(&self) -> Result<CallToolResult, ErrorData> {
        self.call_tool("list_dms", json!({})).await
    }
}
