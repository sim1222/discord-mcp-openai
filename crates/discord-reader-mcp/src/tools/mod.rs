//! MCP tools. Every tool is read-only: they proxy the daemon's read-only RPC
//! surface and never send Discord credentials anywhere.

pub mod channels;
pub mod guilds;
pub mod inbox;
pub mod messages;
pub mod meta;
pub mod search;
pub mod sync;
pub mod threads;

use std::sync::Arc;

use rmcp::{
    handler::server::router::tool::ToolRouter,
    model::{
        CallToolResult, ContentBlock, Implementation, InitializeResult, ProtocolVersion,
        ServerCapabilities,
    },
    tool_handler, ErrorData, ServerHandler,
};
use serde_json::{json, Value};

use crate::protocol::{RpcClient, RpcClientError};

/// Shared state for all tools.
#[derive(Clone)]
pub struct DiscordReaderTools {
    client: Arc<RpcClient>,
}

impl DiscordReaderTools {
    pub fn new(client: Arc<RpcClient>) -> Self {
        Self { client }
    }

    /// Combined router: each tool group lives in its own module.
    pub fn tool_router() -> ToolRouter<Self> {
        let mut router = Self::meta_router();
        router.merge(Self::guilds_router());
        router.merge(Self::channels_router());
        router.merge(Self::messages_router());
        router.merge(Self::search_router());
        router.merge(Self::inbox_router());
        router.merge(Self::threads_router());
        router.merge(Self::sync_router());
        router
    }

    /// Invoke a read-only daemon RPC method and render the result.
    ///
    /// Daemon-side failures become visible tool errors (`isError: true`) so
    /// the model can report them, instead of opaque protocol errors.
    pub(crate) async fn call_tool(
        &self,
        method: &str,
        params: Value,
    ) -> Result<CallToolResult, ErrorData> {
        match self.client.call(method, params).await {
            Ok(value) => json_result(&value),
            Err(error) => Ok(remote_error(error)),
        }
    }
}

#[tool_handler]
impl ServerHandler for DiscordReaderTools {
    fn get_info(&self) -> InitializeResult {
        let mut info = InitializeResult::new(ServerCapabilities::builder().enable_tools().build());
        info.server_info = Implementation::new("discord-reader-mcp", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(
            "Read-only view of Discord servers, channels, messages, threads and DMs. \
             Data is fetched on demand and cached locally; full-text search covers \
             messages already fetched. Cross-server inbox tools (list_mentions, \
             list_replies) report the checked range so empty results are meaningful, \
             and every listing carries next_cursor/has_more and coverage info. \
             get_capabilities reports what works under the current credential kind. \
             This server cannot write to Discord and never marks anything read."
                .to_string(),
        );
        info
    }

    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Borrowed(&[
            ProtocolVersion::V_2026_07_28,
            ProtocolVersion::V_2025_11_25,
            ProtocolVersion::V_2025_06_18,
            ProtocolVersion::V_2025_03_26,
        ])
    }
}

/// Render a JSON payload as a tool result.
pub(crate) fn json_result(value: &Value) -> Result<CallToolResult, ErrorData> {
    let text = serde_json::to_string_pretty(value)
        .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
    Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
}

/// Render a daemon-side failure as a visible tool error.
///
/// When the daemon returned a structured error (JSON-encoded `message` with
/// `error`/`context` fields: `error_source`, `code`, `http_status`,
/// `discord_code`, `retryable`, `retry_after_ms`, `operation`), it is passed
/// through verbatim so callers can decide mechanically whether to retry and
/// what failed, instead of parsing prose. Failures are never rendered as empty
/// results: an error is an error, not "nothing there".
pub(crate) fn remote_error(error: RpcClientError) -> CallToolResult {
    if let RpcClientError::Remote { code, message } = &error {
        if let Ok(parsed) = serde_json::from_str::<Value>(message) {
            if parsed.get("error").is_some() {
                let text = serde_json::to_string_pretty(&json!({
                    "error": parsed["error"],
                    "context": parsed.get("context").cloned().unwrap_or(Value::Null),
                    "rpc_code": code,
                }))
                .unwrap_or_else(|_| message.clone());
                return CallToolResult::error(vec![ContentBlock::text(text)]);
            }
        }
    }
    CallToolResult::error(vec![ContentBlock::text(format!(
        "discord-reader-daemon error: {error}"
    ))])
}

/// Validate an optional limit argument.
pub(crate) fn limit_or(limit: Option<u32>, default: u32, max: u32) -> Result<u32, ErrorData> {
    let limit = limit.unwrap_or(default);
    if (1..=max).contains(&limit) {
        Ok(limit)
    } else {
        Err(ErrorData::invalid_params(
            format!("limit must be between 1 and {max}"),
            None,
        ))
    }
}

/// Validate an optional context window argument.
pub(crate) fn window_or(value: Option<u32>, default: u32) -> Result<u32, ErrorData> {
    let value = value.unwrap_or(default);
    if value <= 50 {
        Ok(value)
    } else {
        Err(ErrorData::invalid_params(
            "before/after must be between 0 and 50",
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The tool surface must stay strictly read-only: no write verbs may ever
    /// appear as tool names.
    #[test]
    fn tool_surface_is_read_only() {
        let router = DiscordReaderTools::tool_router();
        let mut names: Vec<String> = router
            .list_all()
            .iter()
            .map(|tool| tool.name.to_string())
            .collect();
        names.sort();
        let mut expected = [
            "get_me",
            "get_capabilities",
            "list_guilds",
            "list_channels",
            "list_dms",
            "list_changed_channels",
            "recent_messages",
            "messages_before",
            "messages_after",
            "get_message",
            "get_message_raw",
            "message_context",
            "get_attachment",
            "get_message_events",
            "list_mentions",
            "list_replies",
            "search_messages",
            "search_server_side",
            "read_thread",
            "list_threads",
            "get_member",
            "get_sync_status",
            "start_sync",
            "get_sync_progress",
        ]
        .map(str::to_string)
        .to_vec();
        expected.sort();
        assert_eq!(names, expected);

        let banned = [
            "send", "edit", "delete", "react", "join", "leave", "typing", "webhook", "create",
            "update", "post", "put", "patch",
        ];
        for name in &names {
            for verb in banned {
                assert!(
                    !name.contains(verb),
                    "tool {name} looks like a write operation"
                );
            }
        }
    }

    #[test]
    fn tool_schemas_declare_required_arguments() {
        let router = DiscordReaderTools::tool_router();
        let by_name = |name: &str| {
            router
                .list_all()
                .into_iter()
                .find(|tool| tool.name == name)
                .unwrap_or_else(|| panic!("tool {name} must exist"))
        };

        let channels = by_name("list_channels");
        let required = channels
            .input_schema
            .get("required")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(required.contains(&json!("guild_id")));

        let recent = by_name("recent_messages");
        let required = recent
            .input_schema
            .get("required")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(required.contains(&json!("channel_id")));
        // limit has a default, so it must not be required.
        assert!(!required.contains(&json!("limit")));

        let search = by_name("search_messages");
        let required = search
            .input_schema
            .get("required")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(required.contains(&json!("query")));
    }

    #[test]
    fn limits_are_validated() {
        assert!(limit_or(None, 50, 100).is_ok());
        assert!(limit_or(Some(1), 50, 100).is_ok());
        assert!(limit_or(Some(100), 50, 100).is_ok());
        assert!(limit_or(Some(0), 50, 100).is_err());
        assert!(limit_or(Some(101), 50, 100).is_err());
        assert!(window_or(Some(51), 20).is_err());
    }
}
