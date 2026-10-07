//! JSON-Lines RPC over a Unix domain socket.
//!
//! Wire format (one JSON object per line):
//!
//! ```json
//! {"id":1,"method":"recent_messages","params":{"channel_id":"123","limit":50}}
//! {"id":1,"result":{"messages":[]}}
//! {"id":2,"error":{"code":-32601,"message":"unknown method"}}
//! ```
//!
//! The protocol is read-only by construction: it only contains the methods of
//! [`ReaderApi`], and there is no method that can write to Discord.

use std::{path::Path, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// Maximum accepted RPC request line (1 MiB).
const MAX_LINE_BYTES: usize = 1024 * 1024;

pub(crate) const CACHE_ERROR: i64 = -32004;

/// Socket file mode: owner + group read/write only.
#[cfg(unix)]
pub const SOCKET_MODE: u32 = 0o660;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcRequest {
    pub id: Option<u64>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("rpc error {code}: {message}")]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(-32601, format!("unknown method: {method}"))
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(-32602, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(-32603, message)
    }

    /// Structured error data, mirrored in `message` for existing daemon clients.
    pub fn structured(code: i64, data: Value) -> Self {
        Self {
            code,
            message: data.to_string(),
            data: Some(data),
        }
    }

    fn with_operation(mut self, operation: &str) -> Self {
        if !self
            .data
            .as_ref()
            .and_then(|data| data.get("error"))
            .is_some_and(Value::is_object)
        {
            let (code, source) = match self.code {
                -32602 => ("INVALID_PARAMS", "client"),
                -32601 => ("METHOD_NOT_FOUND", "client"),
                -32700 => ("PARSE_ERROR", "client"),
                -32600 => ("INVALID_REQUEST", "client"),
                _ => ("INTERNAL_ERROR", "daemon"),
            };
            self.data = Some(serde_json::json!({"error": {
                "error_source": source, "code": code, "operation": operation,
                "retryable": false, "message": self.message
            }}));
        }
        if let Some(data) = self.data.as_mut() {
            if data["error"].get("error_source").is_none() {
                data["error"]["error_source"] = Value::String("daemon".into());
            }
            if data["error"].get("code").is_none() {
                data["error"]["code"] = Value::String("INTERNAL_ERROR".into());
            }
            if data["error"].get("retryable").is_none() {
                data["error"]["retryable"] = Value::Bool(false);
            }
            if let Some(request) = data["error"]["operation"]
                .as_str()
                .filter(|value| value.starts_with("GET "))
            {
                data["error"]["request_operation"] = Value::String(request.to_string());
            }
            data["error"]["operation"] = Value::String(operation.to_string());
            if data["error"].get("message").is_none() {
                data["error"]["message"] = Value::String("request failed".into());
            }
            self.message = data.to_string();
        }
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl RpcResponse {
    pub fn ok(id: Option<u64>, result: Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: Option<u64>, error: RpcError) -> Self {
        Self {
            id,
            result: None,
            error: Some(error),
        }
    }
}

/* ----------------------------- request params ----------------------------- */

#[derive(Debug, Clone, Deserialize)]
pub struct GuildIdParams {
    pub guild_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelParams {
    pub channel_id: String,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BeforeParams {
    pub channel_id: String,
    pub before_message_id: String,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MessageParams {
    pub channel_id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContextParams {
    pub channel_id: String,
    pub message_id: String,
    #[serde(default = "default_context_window")]
    pub before: u32,
    #[serde(default = "default_context_window")]
    pub after: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchParams {
    pub query: String,
    #[serde(default)]
    pub guild_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub author_id: Option<String>,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub before: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// When true, re-fetch the targeted channel's recent messages from Discord
    /// before searching. Opt-in on purpose: search never triggers surprise
    /// network traffic.
    #[serde(default)]
    pub refresh: bool,
    /// Offset into server-side search results (`search_server_side` only).
    #[serde(default)]
    pub offset: u32,
    /// Resume cursor returned by a previous `search_server_side` call.
    #[serde(default)]
    pub next_cursor: Option<String>,
    /// Server-side sort: `timestamp` or `relevance`.
    #[serde(default)]
    pub sort: Option<String>,
    /// `desc` (newest first) or `asc`.
    #[serde(default)]
    pub sort_order: Option<String>,
}

/// Filters for `list_mentions` / `list_replies`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct InboxFilter {
    #[serde(default)]
    pub guild_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    /// ISO-8601 lower bound on message timestamp.
    #[serde(default)]
    pub after: Option<String>,
    /// ISO-8601 upper bound on message timestamp.
    #[serde(default)]
    pub before: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// When true, refresh targeted channels from Discord first.
    #[serde(default)]
    pub refresh: bool,
    /// Server-side search cursor (`search_server_side` integration).
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// Parameters for `messages_after`.
#[derive(Debug, Clone, Deserialize)]
pub struct AfterParams {
    pub channel_id: String,
    /// Return messages newer than this message id (exclusive).
    pub after_message_id: String,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

/// Parameters for `list_changed_channels`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ChangedChannelsParams {
    #[serde(default)]
    pub guild_id: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// Resume cursor from a previous call.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Parameters for `get_message_raw`.
#[derive(Debug, Clone, Deserialize)]
pub struct MessageParams2 {
    pub channel_id: String,
    pub message_id: String,
}

/// Parameters for `get_message_events`.
#[derive(Debug, Clone, Deserialize)]
pub struct MessageEventsParams {
    pub channel_id: String,
    pub message_id: String,
}

/// Parameters for `get_member`.
#[derive(Debug, Clone, Deserialize)]
pub struct MemberParams {
    pub guild_id: String,
    #[serde(default)]
    pub user_id: Option<String>,
}

/// Parameters for `list_threads`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ThreadsParams {
    pub guild_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    /// `active`, `archived` or `all`; `joined` returns an unsupported-filter error.
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub include_archived: Option<bool>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Parameters for `start_sync`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SyncStartParams {
    /// `mentions`, `replies`, `changed_channels`, `all` or `refetch`.
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub guild_id: Option<String>,
    #[serde(default)]
    pub channel_ids: Option<Vec<String>>,
    /// Bound on metadata lookup attempts for `refetch`, defaults to 100.
    #[serde(default)]
    pub max_messages: Option<u32>,
    /// Exclusive per-channel boundaries from a previous refetch job.
    #[serde(default)]
    pub refetch_before: Option<std::collections::HashMap<String, String>>,
}

/// Parameters for `get_sync_progress`.
#[derive(Debug, Clone, Deserialize)]
pub struct SyncProgressParams {
    pub job_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThreadParams {
    pub thread_id: String,
    #[serde(default = "default_thread_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    50
}

fn default_thread_limit() -> u32 {
    100
}

fn default_context_window() -> u32 {
    20
}

/* -------------------------------- API trait ------------------------------- */

/// The complete read-only surface of the daemon.
///
/// Implemented against Discord + SQLite by `api::DaemonApi`, and by fakes in
/// tests. Every method is read-only.
#[async_trait::async_trait]
pub trait ReaderApi: Send + Sync {
    async fn ping(&self) -> Result<Value, RpcError>;
    async fn get_me(&self) -> Result<Value, RpcError>;
    async fn get_capabilities(&self) -> Result<Value, RpcError>;
    async fn list_guilds(&self) -> Result<Value, RpcError>;
    async fn list_channels(&self, params: GuildIdParams) -> Result<Value, RpcError>;
    async fn list_dms(&self) -> Result<Value, RpcError>;
    async fn recent_messages(&self, params: ChannelParams) -> Result<Value, RpcError>;
    async fn messages_before(&self, params: BeforeParams) -> Result<Value, RpcError>;
    async fn messages_after(&self, params: AfterParams) -> Result<Value, RpcError>;
    async fn get_message(&self, params: MessageParams) -> Result<Value, RpcError>;
    async fn get_message_raw(&self, params: MessageParams) -> Result<Value, RpcError>;
    async fn message_context(&self, params: ContextParams) -> Result<Value, RpcError>;
    async fn search_messages(&self, params: SearchParams) -> Result<Value, RpcError>;
    async fn search_server_side(&self, params: SearchParams) -> Result<Value, RpcError>;
    async fn list_mentions(&self, params: InboxFilter) -> Result<Value, RpcError>;
    async fn list_replies(&self, params: InboxFilter) -> Result<Value, RpcError>;
    async fn read_thread(&self, params: ThreadParams) -> Result<Value, RpcError>;
    async fn list_threads(&self, params: ThreadsParams) -> Result<Value, RpcError>;
    async fn list_changed_channels(&self, params: ChangedChannelsParams)
        -> Result<Value, RpcError>;
    async fn get_sync_status(&self) -> Result<Value, RpcError>;
    async fn start_sync(&self, params: SyncStartParams) -> Result<Value, RpcError>;
    async fn get_sync_progress(&self, params: SyncProgressParams) -> Result<Value, RpcError>;
    async fn get_member(&self, params: MemberParams) -> Result<Value, RpcError>;
    async fn get_message_events(&self, params: MessageEventsParams) -> Result<Value, RpcError>;
    async fn get_attachment(&self, params: MessageParams) -> Result<Value, RpcError>;
}

/* -------------------------------- dispatch -------------------------------- */

fn check_limit(limit: u32, max: u32) -> Result<(), RpcError> {
    if (1..=max).contains(&limit) {
        Ok(())
    } else {
        Err(RpcError::invalid_params(format!(
            "limit must be between 1 and {max}"
        )))
    }
}

fn check_window(value: u32) -> Result<(), RpcError> {
    if value <= 50 {
        Ok(())
    } else {
        Err(RpcError::invalid_params(
            "before/after context must be between 0 and 50",
        ))
    }
}

fn parse<T: for<'de> Deserialize<'de>>(params: &Value) -> Result<T, RpcError> {
    serde_json::from_value(params.clone())
        .map_err(|e| RpcError::invalid_params(format!("invalid params: {e}")))
}

/// Dispatch one request. Returns `None` for notification-style requests (no
/// `id`), which get no response line.
pub async fn dispatch(api: &dyn ReaderApi, request: RpcRequest) -> Option<RpcResponse> {
    let RpcRequest { id, method, params } = request;
    let outcome: Result<Value, RpcError> = async {
        match method.as_str() {
            "ping" => api.ping().await,
            "get_me" => api.get_me().await,
            "get_capabilities" => api.get_capabilities().await,
            "list_guilds" => api.list_guilds().await,
            "list_channels" => {
                let p: GuildIdParams = parse(&params)?;
                api.list_channels(p).await
            }
            "list_dms" => api.list_dms().await,
            "recent_messages" => {
                let p: ChannelParams = parse(&params)?;
                check_limit(p.limit, 100)?;
                api.recent_messages(p).await
            }
            "messages_before" => {
                let p: BeforeParams = parse(&params)?;
                check_limit(p.limit, 100)?;
                api.messages_before(p).await
            }
            "messages_after" => {
                let p: AfterParams = parse(&params)?;
                check_limit(p.limit, 100)?;
                api.messages_after(p).await
            }
            "get_message" => {
                let p: MessageParams = parse(&params)?;
                api.get_message(p).await
            }
            "get_message_raw" => {
                let p: MessageParams = parse(&params)?;
                api.get_message_raw(p).await
            }
            "message_context" => {
                let p: ContextParams = parse(&params)?;
                check_window(p.before)?;
                check_window(p.after)?;
                api.message_context(p).await
            }
            "search_messages" => {
                let p: SearchParams = parse(&params)?;
                check_limit(p.limit, 200)?;
                api.search_messages(p).await
            }
            "search_server_side" => {
                let p: SearchParams = parse(&params)?;
                check_limit(p.limit, 25)?;
                api.search_server_side(p).await
            }
            "list_mentions" => {
                let p: InboxFilter = parse(&params)?;
                check_limit(p.limit, 100)?;
                api.list_mentions(p).await
            }
            "list_replies" => {
                let p: InboxFilter = parse(&params)?;
                check_limit(p.limit, 100)?;
                api.list_replies(p).await
            }
            "read_thread" => {
                let p: ThreadParams = parse(&params)?;
                check_limit(p.limit, 100)?;
                api.read_thread(p).await
            }
            "list_threads" => {
                let p: ThreadsParams = parse(&params)?;
                check_limit(p.limit, 100)?;
                api.list_threads(p).await
            }
            "list_changed_channels" => {
                let p: ChangedChannelsParams = parse(&params)?;
                check_limit(p.limit, 100)?;
                api.list_changed_channels(p).await
            }
            "get_sync_status" => api.get_sync_status().await,
            "start_sync" => {
                let p: SyncStartParams = parse(&params)?;
                api.start_sync(p).await
            }
            "get_sync_progress" => {
                let p: SyncProgressParams = parse(&params)?;
                api.get_sync_progress(p).await
            }
            "get_member" => {
                let p: MemberParams = parse(&params)?;
                api.get_member(p).await
            }
            "get_message_events" => {
                let p: MessageEventsParams = parse(&params)?;
                api.get_message_events(p).await
            }
            "get_attachment" => {
                let p: MessageParams = parse(&params)?;
                api.get_attachment(p).await
            }
            other => Err(RpcError::method_not_found(other)),
        }
    }
    .await;

    id.map(|id| match outcome {
        Ok(result) => RpcResponse::ok(Some(id), result),
        Err(error) => RpcResponse::err(Some(id), error.with_operation(&method)),
    })
}

/* --------------------------------- server --------------------------------- */

/// Create the listening socket at `path`, with `0660` permissions.
pub async fn bind_socket(path: impl AsRef<Path>) -> std::io::Result<UnixListener> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    // Remove a stale socket from a previous run.
    if tokio::fs::symlink_metadata(path).await.is_ok() {
        tokio::fs::remove_file(path).await?;
    }
    let listener = UnixListener::bind(path)?;
    set_socket_mode(path)?;
    Ok(listener)
}

#[cfg(unix)]
fn set_socket_mode(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE))
}

#[cfg(not(unix))]
fn set_socket_mode(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Serve RPC requests until the listener is closed.
pub async fn serve(listener: UnixListener, api: Arc<dyn ReaderApi>) -> anyhow::Result<()> {
    loop {
        let (stream, _addr) = listener.accept().await?;
        let api = Arc::clone(&api);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, api).await {
                tracing::debug!(%error, "rpc connection ended");
            }
        });
    }
}

/// Serve a single connection. Reads JSON-lines, writes JSON-lines.
pub async fn handle_connection(stream: UnixStream, api: Arc<dyn ReaderApi>) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        if line.len() > MAX_LINE_BYTES {
            let response = RpcResponse::err(
                None,
                RpcError::new(-32600, "request line too large").with_operation("rpc_request"),
            );
            write_response(&mut write_half, &response).await?;
            continue;
        }
        let response = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(request) => dispatch(api.as_ref(), request).await,
            Err(error) => Some(RpcResponse::err(
                None,
                RpcError::new(-32700, format!("invalid request: {error}"))
                    .with_operation("rpc_request"),
            )),
        };
        if let Some(response) = response {
            write_response(&mut write_half, &response).await?;
        }
    }
    Ok(())
}

async fn write_response(
    write_half: &mut tokio::net::unix::OwnedWriteHalf,
    response: &RpcResponse,
) -> anyhow::Result<()> {
    let mut payload = serde_json::to_string(response)?;
    payload.push('\n');
    write_half.write_all(payload.as_bytes()).await?;
    write_half.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn plain_rpc_errors_have_uniform_structured_fields() {
        for (error, code, source) in [
            (
                super::RpcError::invalid_params("invalid cursor"),
                "INVALID_PARAMS",
                "client",
            ),
            (
                super::RpcError::internal("unexpected payload"),
                "INTERNAL_ERROR",
                "daemon",
            ),
            (
                super::RpcError::method_not_found("unknown"),
                "METHOD_NOT_FOUND",
                "client",
            ),
        ] {
            let error = error.with_operation("list_changed_channels");
            let data = error.data.unwrap();
            assert_eq!(data["error"]["code"], code);
            assert_eq!(data["error"]["error_source"], source);
            assert_eq!(data["error"]["operation"], "list_changed_channels");
            assert_eq!(data["error"]["retryable"], false);
            assert!(data["error"]["message"].is_string());
        }
    }
    #[test]
    fn rpc_operation_retains_actual_message_request() {
        let error = super::RpcError::structured(
            -32005,
            serde_json::json!({"error": {
                "code":"bot_only", "operation":"GET /channels/200/messages?limit=1&around=10"
            }}),
        )
        .with_operation("get_message");
        let data = error.data.unwrap();
        assert_eq!(data["error"]["operation"], "get_message");
        assert_eq!(
            data["error"]["request_operation"],
            "GET /channels/200/messages?limit=1&around=10"
        );
    }
    use super::*;
    use serde_json::json;

    #[derive(Default)]
    struct FakeApi;

    #[async_trait::async_trait]
    impl ReaderApi for FakeApi {
        async fn ping(&self) -> Result<Value, RpcError> {
            Ok(json!({"ok": true}))
        }
        async fn get_me(&self) -> Result<Value, RpcError> {
            Ok(json!({"me": null}))
        }
        async fn get_capabilities(&self) -> Result<Value, RpcError> {
            Ok(json!({}))
        }
        async fn list_guilds(&self) -> Result<Value, RpcError> {
            Ok(json!({"guilds": [{"id": "1", "name": "ZENVR"}]}))
        }
        async fn list_channels(&self, _p: GuildIdParams) -> Result<Value, RpcError> {
            Ok(json!({"channels": []}))
        }
        async fn list_dms(&self) -> Result<Value, RpcError> {
            Ok(json!({"dms": []}))
        }
        async fn recent_messages(&self, p: ChannelParams) -> Result<Value, RpcError> {
            Ok(json!({"messages": [], "channel_id": p.channel_id, "limit": p.limit}))
        }
        async fn messages_before(&self, _p: BeforeParams) -> Result<Value, RpcError> {
            Ok(json!({"messages": []}))
        }
        async fn messages_after(&self, _p: AfterParams) -> Result<Value, RpcError> {
            Ok(json!({"messages": []}))
        }
        async fn get_message(&self, _p: MessageParams) -> Result<Value, RpcError> {
            Ok(json!({"message": null}))
        }
        async fn get_message_raw(&self, _p: MessageParams) -> Result<Value, RpcError> {
            Ok(json!({"message": null}))
        }
        async fn message_context(&self, _p: ContextParams) -> Result<Value, RpcError> {
            Ok(json!({"before": [], "message": null, "after": []}))
        }
        async fn search_messages(&self, _p: SearchParams) -> Result<Value, RpcError> {
            Ok(json!({"results": [], "source": "local"}))
        }
        async fn search_server_side(&self, _p: SearchParams) -> Result<Value, RpcError> {
            Ok(json!({"results": []}))
        }
        async fn list_mentions(&self, _p: InboxFilter) -> Result<Value, RpcError> {
            Ok(json!({"mentions": []}))
        }
        async fn list_replies(&self, _p: InboxFilter) -> Result<Value, RpcError> {
            Ok(json!({"replies": []}))
        }
        async fn read_thread(&self, _p: ThreadParams) -> Result<Value, RpcError> {
            Ok(json!({"thread": null, "messages": []}))
        }
        async fn list_threads(&self, _p: ThreadsParams) -> Result<Value, RpcError> {
            Ok(json!({"threads": []}))
        }
        async fn list_changed_channels(
            &self,
            _p: ChangedChannelsParams,
        ) -> Result<Value, RpcError> {
            Ok(json!({"changed_channels": []}))
        }
        async fn get_sync_status(&self) -> Result<Value, RpcError> {
            Ok(json!({}))
        }
        async fn start_sync(&self, _p: SyncStartParams) -> Result<Value, RpcError> {
            Ok(json!({"job_id": "1"}))
        }
        async fn get_sync_progress(&self, _p: SyncProgressParams) -> Result<Value, RpcError> {
            Ok(json!({"progress": null}))
        }
        async fn get_member(&self, _p: MemberParams) -> Result<Value, RpcError> {
            Ok(json!({"member": null}))
        }
        async fn get_message_events(&self, _p: MessageEventsParams) -> Result<Value, RpcError> {
            Ok(json!({"events": []}))
        }
        async fn get_attachment(&self, _p: MessageParams) -> Result<Value, RpcError> {
            Ok(json!({"attachments": []}))
        }
    }

    fn api() -> Arc<dyn ReaderApi> {
        Arc::new(FakeApi)
    }

    #[tokio::test]
    async fn ping_round_trip() {
        let response = dispatch(
            api().as_ref(),
            RpcRequest {
                id: Some(7),
                method: "ping".into(),
                params: json!({}),
            },
        )
        .await
        .unwrap();
        assert_eq!(response.id, Some(7));
        assert_eq!(response.result.unwrap()["ok"], json!(true));
    }

    #[tokio::test]
    async fn unknown_method_is_rejected() {
        let response = dispatch(
            api().as_ref(),
            RpcRequest {
                id: Some(1),
                method: "send_message".into(),
                params: json!({}),
            },
        )
        .await
        .unwrap();
        let error = response.error.unwrap();
        assert_eq!(error.code, -32601);
    }

    #[tokio::test]
    async fn limits_are_validated() {
        for limit in [0, 101, 1000] {
            let response = dispatch(
                api().as_ref(),
                RpcRequest {
                    id: Some(1),
                    method: "recent_messages".into(),
                    params: json!({"channel_id": "1", "limit": limit}),
                },
            )
            .await
            .unwrap();
            let error = response.error.unwrap();
            assert_eq!(error.code, -32602, "limit {limit} must be rejected");
        }
    }

    #[tokio::test]
    async fn invalid_params_are_rejected() {
        let response = dispatch(
            api().as_ref(),
            RpcRequest {
                id: Some(1),
                method: "message_context".into(),
                params: json!({"channel_id": "1"}),
            },
        )
        .await
        .unwrap();
        assert_eq!(response.error.unwrap().code, -32602);
    }

    #[tokio::test]
    async fn socket_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("reader.sock");
        let listener = bind_socket(&socket_path).await.unwrap();
        let api = api();
        let server = tokio::spawn(serve(listener, api));

        let stream = UnixStream::connect(&socket_path).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();

        let request = json!({"id": 1, "method": "list_guilds", "params": {}});
        let mut payload = serde_json::to_string(&request).unwrap();
        payload.push('\n');
        write_half.write_all(payload.as_bytes()).await.unwrap();
        write_half.flush().await.unwrap();

        let line = lines.next_line().await.unwrap().unwrap();
        let response: RpcResponse = serde_json::from_str(&line).unwrap();
        assert_eq!(response.id, Some(1));
        assert_eq!(response.result.unwrap()["guilds"][0]["name"], "ZENVR");

        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn socket_permissions_are_restricted() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("reader.sock");
        let _listener = bind_socket(&socket_path).await.unwrap();
        let mode = std::fs::metadata(&socket_path)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, SOCKET_MODE);
    }
}
