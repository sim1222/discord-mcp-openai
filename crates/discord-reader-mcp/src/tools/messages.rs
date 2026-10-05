//! Message reading tools: recent history, backwards paging, single message
//! and surrounding context.

use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{limit_or, window_or, DiscordReaderTools};

/// Arguments for `recent_messages`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct RecentMessagesArgs {
    /// Channel ID (text channel, thread, or DM channel).
    pub channel_id: String,
    /// How many messages to return, 1-100. Defaults to 50.
    pub limit: Option<u32>,
}

/// Arguments for `messages_before`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct MessagesBeforeArgs {
    /// Channel ID.
    pub channel_id: String,
    /// Return messages older than this message ID.
    pub before_message_id: String,
    /// How many messages to return, 1-100. Defaults to 50.
    pub limit: Option<u32>,
}

/// Arguments for `get_message`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct GetMessageArgs {
    /// Channel ID containing the message.
    pub channel_id: String,
    /// Message ID.
    pub message_id: String,
}

/// Arguments for `message_context`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct MessageContextArgs {
    /// Channel ID containing the message.
    pub channel_id: String,
    /// Message ID to centre on.
    pub message_id: String,
    /// How many messages before it to include, 0-50. Defaults to 20.
    pub before: Option<u32>,
    /// How many messages after it to include, 0-50. Defaults to 20.
    pub after: Option<u32>,
}

#[tool_router(vis = "pub(crate)", router = messages_router)]
impl DiscordReaderTools {
    /// Read the most recent messages of a channel.
    ///
    /// Returns JSON: `{"channel_id": "...", "messages": [...]}` where each
    /// message has `id`, `channel_id`, `guild_id`, `author` (`id`, `name`),
    /// `timestamp`, `content`, optional `reply_to`, and minimal
    /// `attachments` / `embeds` metadata. Fetched messages are cached
    /// locally so `search_messages` can find them later.
    #[tool(name = "recent_messages")]
    pub async fn recent_messages(
        &self,
        Parameters(args): Parameters<RecentMessagesArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 50, 100)?;
        self.call_tool(
            "recent_messages",
            json!({"channel_id": args.channel_id, "limit": limit}),
        )
        .await
    }

    /// Page backwards through a channel's history.
    ///
    /// Returns messages older than `before_message_id`, newest first. Pass
    /// the oldest message ID you have seen to continue deeper into history.
    #[tool(name = "messages_before")]
    pub async fn messages_before(
        &self,
        Parameters(args): Parameters<MessagesBeforeArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 50, 100)?;
        self.call_tool(
            "messages_before",
            json!({
                "channel_id": args.channel_id,
                "before_message_id": args.before_message_id,
                "limit": limit,
            }),
        )
        .await
    }

    /// Fetch one message by ID.
    ///
    /// Returns JSON: `{"message": {...}}` in the normalized message shape.
    #[tool(name = "get_message")]
    pub async fn get_message(
        &self,
        Parameters(args): Parameters<GetMessageArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "get_message",
            json!({"channel_id": args.channel_id, "message_id": args.message_id}),
        )
        .await
    }

    /// Read a message together with the conversation around it.
    ///
    /// Returns JSON: `{"before": [...], "message": {...}, "after": [...]}`.
    /// `before` and `after` are chronological (oldest first) so that
    /// concatenating them with `message` reads like the conversation.
    #[tool(name = "message_context")]
    pub async fn message_context(
        &self,
        Parameters(args): Parameters<MessageContextArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let before = window_or(args.before, 20)?;
        let after = window_or(args.after, 20)?;
        self.call_tool(
            "message_context",
            json!({
                "channel_id": args.channel_id,
                "message_id": args.message_id,
                "before": before,
                "after": after,
            }),
        )
        .await
    }
}
