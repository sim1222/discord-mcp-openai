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
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
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
    async fn list_guilds(&self) -> Result<Value, RpcError>;
    async fn list_channels(&self, params: GuildIdParams) -> Result<Value, RpcError>;
    async fn recent_messages(&self, params: ChannelParams) -> Result<Value, RpcError>;
    async fn messages_before(&self, params: BeforeParams) -> Result<Value, RpcError>;
    async fn get_message(&self, params: MessageParams) -> Result<Value, RpcError>;
    async fn message_context(&self, params: ContextParams) -> Result<Value, RpcError>;
    async fn search_messages(&self, params: SearchParams) -> Result<Value, RpcError>;
    async fn read_thread(&self, params: ThreadParams) -> Result<Value, RpcError>;
    async fn list_threads(&self, params: GuildIdParams) -> Result<Value, RpcError>;
    async fn list_dms(&self) -> Result<Value, RpcError>;
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
            "list_guilds" => api.list_guilds().await,
            "list_channels" => {
                let p: GuildIdParams = parse(&params)?;
                api.list_channels(p).await
            }
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
            "get_message" => {
                let p: MessageParams = parse(&params)?;
                api.get_message(p).await
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
            "read_thread" => {
                let p: ThreadParams = parse(&params)?;
                check_limit(p.limit, 100)?;
                api.read_thread(p).await
            }
            "list_threads" => {
                let p: GuildIdParams = parse(&params)?;
                api.list_threads(p).await
            }
            "list_dms" => api.list_dms().await,
            other => Err(RpcError::method_not_found(other)),
        }
    }
    .await;

    id.map(|id| match outcome {
        Ok(result) => RpcResponse::ok(Some(id), result),
        Err(error) => RpcResponse::err(Some(id), error),
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
            let response = RpcResponse::err(None, RpcError::new(-32600, "request line too large"));
            write_response(&mut write_half, &response).await?;
            continue;
        }
        let response = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(request) => dispatch(api.as_ref(), request).await,
            Err(error) => Some(RpcResponse::err(
                None,
                RpcError::new(-32700, format!("invalid request: {error}")),
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
    use super::*;
    use serde_json::json;

    #[derive(Default)]
    struct FakeApi;

    #[async_trait::async_trait]
    impl ReaderApi for FakeApi {
        async fn ping(&self) -> Result<Value, RpcError> {
            Ok(json!({"ok": true}))
        }
        async fn list_guilds(&self) -> Result<Value, RpcError> {
            Ok(json!({"guilds": [{"id": "1", "name": "ZENVR"}]}))
        }
        async fn list_channels(&self, _p: GuildIdParams) -> Result<Value, RpcError> {
            Ok(json!({"channels": []}))
        }
        async fn recent_messages(&self, p: ChannelParams) -> Result<Value, RpcError> {
            Ok(json!({"messages": [], "channel_id": p.channel_id, "limit": p.limit}))
        }
        async fn messages_before(&self, _p: BeforeParams) -> Result<Value, RpcError> {
            Ok(json!({"messages": []}))
        }
        async fn get_message(&self, _p: MessageParams) -> Result<Value, RpcError> {
            Ok(json!({"message": null}))
        }
        async fn message_context(&self, _p: ContextParams) -> Result<Value, RpcError> {
            Ok(json!({"before": [], "message": null, "after": []}))
        }
        async fn search_messages(&self, _p: SearchParams) -> Result<Value, RpcError> {
            Ok(json!({"results": [], "source": "local"}))
        }
        async fn read_thread(&self, _p: ThreadParams) -> Result<Value, RpcError> {
            Ok(json!({"thread": null, "messages": []}))
        }
        async fn list_threads(&self, _p: GuildIdParams) -> Result<Value, RpcError> {
            Ok(json!({"threads": []}))
        }
        async fn list_dms(&self) -> Result<Value, RpcError> {
            Ok(json!({"dms": []}))
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
