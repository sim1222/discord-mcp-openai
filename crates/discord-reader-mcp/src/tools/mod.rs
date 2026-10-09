//! MCP tools. Every tool is read-only: they proxy the daemon's read-only RPC
//! surface and never send Discord credentials anywhere.

pub mod account;
pub mod channels;
pub mod guilds;
pub mod inbox;
pub mod messages;
pub mod meta;
pub mod search;
pub mod sync;
pub mod threads;

#[cfg(test)]
mod transport_regressions;

use std::sync::Arc;

use rmcp::{
    handler::server::router::tool::ToolRouter,
    model::{
        CallToolResponse, CallToolResult, ContentBlock, Implementation, InitializeResult,
        ProtocolVersion, ServerCapabilities,
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
        router.merge(Self::account_router());
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
            Err(error) => Ok(remote_error(method, error)),
        }
    }
}

#[tool_handler]
impl ServerHandler for DiscordReaderTools {
    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let operation = request.name.to_string();
        let call = rmcp::handler::server::tool::ToolCallContext::new(self, request, context);
        match Self::tool_router().call(call).await {
            Ok(CallToolResponse::Complete(result)) if result.is_error == Some(true) => {
                let text = result
                    .content
                    .first()
                    .and_then(|content| content.as_text())
                    .map(|content| content.text.as_str())
                    .unwrap_or("tool failed");
                if serde_json::from_str::<Value>(text)
                    .ok()
                    .and_then(|value| value.get("error").cloned())
                    .is_some()
                {
                    Ok(result.into())
                } else {
                    let error = if text.starts_with("failed to deserialize parameters:") {
                        ErrorData::invalid_params("invalid tool arguments", None)
                    } else {
                        ErrorData::internal_error("tool failed", None)
                    };
                    Ok(tool_error(&operation, error).into())
                }
            }
            Ok(result) => Ok(result),
            Err(error) => Ok(tool_error(&operation, error).into()),
        }
    }

    fn get_info(&self) -> InitializeResult {
        let mut info = InitializeResult::new(ServerCapabilities::builder().enable_tools().build());
        info.server_info = Implementation::new("discord-reader-mcp", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(
            "Read-only view of Discord servers, channels, messages, threads and DMs. \
             Data is fetched on demand and cached locally; full-text search covers \
             messages already fetched. Cross-server inbox tools (list_mentions, \
             list_replies) report metadata and coverage limits; an incomplete empty \
             result cannot prove there are no messages. Consult each tool's paging \
             contract. list_changed_channels compares cached activity with local \
             sync cursors; refresh list_channels first for a current observation. \
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
pub(crate) fn remote_error(operation: &str, error: RpcClientError) -> CallToolResult {
    let (mut payload, rpc_code) = match error {
        RpcClientError::Remote {
            code,
            message,
            data,
        } => {
            let payload = data.filter(|value| value.get("error").is_some_and(Value::is_object)).or_else(|| serde_json::from_str::<Value>(&message).ok().filter(|value| value.get("error").is_some_and(Value::is_object))).unwrap_or_else(|| {
                let (name, source) = if code == -32602 { ("INVALID_PARAMS", "client") } else if code == -32601 { ("METHOD_NOT_FOUND", "client") } else { ("INTERNAL_ERROR", "daemon") };
                json!({"error":{"error_source":source,"code":name,"retryable":false,"message":message}})
            });
            (payload, Some(code))
        }
        RpcClientError::Connect { .. } => (
            json!({"error":{"error_source":"transport","code":"DAEMON_UNAVAILABLE","retryable":true,"message":"cannot connect to Discord reader daemon"}}),
            None,
        ),
        RpcClientError::Timeout => (
            json!({"error":{"error_source":"transport","code":"DAEMON_TIMEOUT","retryable":true,"message":"Discord reader daemon call timed out"}}),
            None,
        ),
        RpcClientError::Protocol(_) => (
            json!({"error":{"error_source":"protocol","code":"RPC_PROTOCOL_ERROR","retryable":false,"message":"invalid Discord reader daemon response"}}),
            None,
        ),
    };
    if let Some(request) = payload["error"]["operation"]
        .as_str()
        .filter(|value| value.starts_with("GET "))
    {
        payload["error"]["request_operation"] = json!(request.to_string());
    }
    payload["error"]["operation"] = json!(operation);
    if payload["error"].get("error_source").is_none() {
        payload["error"]["error_source"] = json!("daemon");
    }
    if payload["error"].get("code").is_none() {
        payload["error"]["code"] = json!("INTERNAL_ERROR");
    }
    if payload["error"].get("retryable").is_none() {
        payload["error"]["retryable"] = json!(false);
    }
    if payload["error"].get("message").is_none() {
        payload["error"]["message"] = json!("request failed");
    }
    if let Some(code) = rpc_code {
        payload["rpc_code"] = json!(code);
    }
    CallToolResult::error(vec![ContentBlock::text(
        serde_json::to_string_pretty(&payload).expect("JSON error payload"),
    )])
}

fn tool_error(operation: &str, error: ErrorData) -> CallToolResult {
    let invalid = error.code == rmcp::model::ErrorCode::INVALID_PARAMS;
    let payload = json!({"error":{
        "error_source":if invalid { "client" } else { "mcp" }, "code":if invalid { "INVALID_PARAMS" } else { "INTERNAL_ERROR" },
        "operation":operation, "retryable":false, "message":error.message
    }, "rpc_code":error.code.0});
    CallToolResult::error(vec![ContentBlock::text(
        serde_json::to_string_pretty(&payload).expect("JSON error payload"),
    )])
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

    fn result_json(result: &CallToolResult) -> Value {
        let rendered = serde_json::to_value(result).unwrap();
        serde_json::from_str(rendered["content"][0]["text"].as_str().unwrap()).unwrap()
    }

    #[test]
    fn remote_errors_prefer_data_and_retain_legacy_message_compatibility() {
        let result = remote_error(
            "get_message",
            RpcClientError::Remote {
                code: -32005,
                message: "human detail".into(),
                data: Some(
                    json!({"error":{"error_source":"discord","code":"bot_only","operation":"get_message","retryable":false,"message":"bot endpoint","discord_code":20002},"context":{"channel_id":"200"}}),
                ),
            },
        );
        let value = result_json(&result);
        assert_eq!(value["error"]["discord_code"], 20002);
        assert_eq!(value["context"]["channel_id"], "200");
        let legacy = remote_error(
            "list_changed_channels",
            RpcClientError::Remote {
                code: -32602,
                message: "invalid cursor".into(),
                data: None,
            },
        );
        assert_eq!(result_json(&legacy)["error"]["code"], "INVALID_PARAMS");
        assert_eq!(
            result_json(&legacy)["error"]["operation"],
            "list_changed_channels"
        );
    }

    #[test]
    fn local_validation_and_transport_errors_are_structured() {
        let result = tool_error("recent_messages", limit_or(Some(0), 50, 100).unwrap_err());
        let value = result_json(&result);
        assert_eq!(value["error"]["code"], "INVALID_PARAMS");
        assert_eq!(value["error"]["operation"], "recent_messages");
        assert_eq!(value["error"]["retryable"], false);
        let timeout = result_json(&remote_error("start_sync", RpcClientError::Timeout));
        assert_eq!(timeout["error"]["code"], "DAEMON_TIMEOUT");
        assert_eq!(timeout["error"]["retryable"], true);
    }

    #[test]
    fn current_start_sync_discovery_advertises_refetch_arguments() {
        let tool = DiscordReaderTools::tool_router()
            .list_all()
            .into_iter()
            .find(|tool| tool.name == "start_sync")
            .unwrap();
        let properties = tool.input_schema["properties"].as_object().unwrap();
        assert!(properties.contains_key("max_messages"));
        assert!(properties.contains_key("refetch_before"));
        assert!(tool
            .description
            .as_deref()
            .unwrap()
            .contains("refetch_before"));
    }

    #[tokio::test]
    async fn mcp_calls_render_limit_and_argument_type_failures_as_structured_tool_errors() {
        use futures::{channel::mpsc, StreamExt};
        let (sender, incoming) = mpsc::unbounded::<rmcp::model::ClientJsonRpcMessage>();
        let (outgoing, mut receiver) = mpsc::unbounded::<rmcp::model::ServerJsonRpcMessage>();
        let tools =
            DiscordReaderTools::new(Arc::new(RpcClient::new("/nonexistent/discord-reader.sock")));
        let server = tokio::spawn(async move {
            rmcp::serve_server(tools, (outgoing, incoming))
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap()
        });
        sender.unbounded_send(serde_json::from_value(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"regression","version":"1"}}})).unwrap()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next())
            .await
            .unwrap()
            .unwrap();
        sender
            .unbounded_send(
                serde_json::from_value(
                    json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                )
                .unwrap(),
            )
            .unwrap();
        for (id, arguments) in [
            (2, json!({"channel_id":"200","limit":0})),
            (3, json!({"channel_id":"200","limit":"not a number"})),
        ] {
            sender.unbounded_send(serde_json::from_value(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"recent_messages","arguments":arguments}})).unwrap()).unwrap();
            let response = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next())
                .await
                .unwrap()
                .unwrap();
            let response = serde_json::to_value(response).unwrap();
            assert_eq!(response["result"]["isError"], true);
            let payload: Value =
                serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())
                    .unwrap();
            assert_eq!(payload["error"]["code"], "INVALID_PARAMS");
            assert_eq!(payload["error"]["operation"], "recent_messages");
            assert_eq!(payload["error"]["retryable"], false);
        }
        drop(sender);
        server.abort();
    }

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
            "get_read_state",
            "get_account_coverage",
            "start_account_sync",
            "cancel_sync",
            "record_inbox_state",
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
