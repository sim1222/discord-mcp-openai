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

/// Arguments for `messages_after`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct MessagesAfterArgs {
    /// Channel ID.
    pub channel_id: String,
    /// Return messages newer than this message ID (exclusive).
    pub after_message_id: String,
    /// How many messages to return, 1-100. Defaults to 50.
    pub limit: Option<u32>,
}

/// Arguments for `get_message_raw`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct GetMessageRawArgs {
    /// Channel ID containing the message.
    pub channel_id: String,
    /// Message ID.
    pub message_id: String,
}

/// Arguments for `get_attachment`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct GetAttachmentArgs {
    /// Channel ID containing the message.
    pub channel_id: String,
    /// Message ID whose attachments to describe.
    pub message_id: String,
}

/// Arguments for `get_message_events`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct GetMessageEventsArgs {
    /// Channel ID containing the message.
    pub channel_id: String,
    /// Message ID linked to the scheduled events.
    pub message_id: String,
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
    /// concatenating them with `message` reads like the conversation. When the
    /// target message replies to another, the referenced message is resolved
    /// inline in `message.reply_to.referenced` where possible, with
    /// `reply_to.status` telling whether it was `resolved`, `deleted`,
    /// `forbidden` or `unknown`.
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

    /// Fetch messages posted after a given message ID (differential sync).
    ///
    /// Returns JSON: `{"channel_id": "...", "messages": [...], "next_cursor":
    /// ..., "has_more": bool, "covered_from": ..., "covered_to": ...}`.
    /// Messages newer than `after_message_id`, newest first. Pass the highest
    /// message ID you have already seen to resume where a previous call left
    /// off; this is the cheap way to catch up without re-reading a channel.
    #[tool(name = "messages_after")]
    pub async fn messages_after(
        &self,
        Parameters(args): Parameters<MessagesAfterArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let limit = limit_or(args.limit, 50, 100)?;
        self.call_tool(
            "messages_after",
            json!({
                "channel_id": args.channel_id,
                "after_message_id": args.after_message_id,
                "limit": limit,
            }),
        )
        .await
    }

    /// Fetch one message's raw Discord API payload alongside the normalized
    /// view, for diagnosing empty-content or missing-field reports.
    ///
    /// Returns JSON: `{"raw": {...}, "normalized": {...}, "fields_present":
    /// [...], "operation": ..., "api_base": ..., "fetched_at": ...}`. Compare
    /// `raw` (what Discord actually returned) with `normalized` (what tools
    /// return) and the Discord UI for the same message ID to tell whether an
    /// empty body is genuine (system/forward/attachment-only), a fetch
    /// problem, or a normalization loss.
    #[tool(name = "get_message_raw")]
    pub async fn get_message_raw(
        &self,
        Parameters(args): Parameters<GetMessageRawArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "get_message_raw",
            json!({"channel_id": args.channel_id, "message_id": args.message_id}),
        )
        .await
    }

    /// Describe a message's attachments with download URLs (metadata only).
    ///
    /// Returns JSON: `{"attachments": [{"id", "filename", "url", "proxy_url",
    /// "size", "content_type", "description", "height", "width", "ephemeral"}],
    /// "snapshot_attachments": [...]}`. File bytes are not fetched here (the
    /// client can download from `url` itself); forwarded-message snapshots are
    /// listed separately so nothing is silently dropped.
    #[tool(name = "get_attachment")]
    pub async fn get_attachment(
        &self,
        Parameters(args): Parameters<GetAttachmentArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "get_attachment",
            json!({"channel_id": args.channel_id, "message_id": args.message_id}),
        )
        .await
    }

    /// List scheduled events linked to a message.
    ///
    /// Returns JSON: `{"events": [...], "fetched_at": ...}` with the event
    /// objects as Discord returned them. A message with no linked events
    /// returns an empty list (not an error).
    #[tool(name = "get_message_events")]
    pub async fn get_message_events(
        &self,
        Parameters(args): Parameters<GetMessageEventsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "get_message_events",
            json!({"channel_id": args.channel_id, "message_id": args.message_id}),
        )
        .await
    }
}
