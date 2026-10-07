//! JSON-Lines RPC client for `discord-reader-daemon`.
//!
//! This is the MCP server's only path to Discord data. It speaks the
//! read-only protocol over a Unix domain socket and holds no Discord
//! credential itself.

use std::{path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// How long one RPC call may take (Discord fetches can wait on rate limits).
const CALL_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, thiserror::Error)]
pub enum RpcClientError {
    #[error("cannot reach discord-reader-daemon at {path}: {source}")]
    Connect {
        path: String,
        source: std::io::Error,
    },
    #[error("rpc protocol error: {0}")]
    Protocol(String),
    #[error("rpc call timed out")]
    Timeout,
    #[error("daemon returned error {code}: {message}")]
    Remote {
        code: i64,
        message: String,
        data: Option<Value>,
    },
}

#[derive(Debug, Serialize)]
struct RpcRequest {
    id: u64,
    method: String,
    params: Value,
}

#[derive(Debug, Deserialize)]
struct RpcResponse {
    #[allow(dead_code)]
    id: Option<u64>,
    result: Option<Value>,
    error: Option<RpcErrorPayload>,
}

#[derive(Debug, Deserialize)]
struct RpcErrorPayload {
    code: i64,
    message: String,
    #[serde(default)]
    data: Option<Value>,
}

/// Client for the daemon's Unix socket RPC.
#[derive(Debug, Clone)]
pub struct RpcClient {
    socket: PathBuf,
}

impl RpcClient {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    /// Call a read-only RPC method.
    ///
    /// Each call opens its own connection (and retries once on connect
    /// failure), so the client reconnects automatically whenever the daemon
    /// restarts. Startup ordering is never load-bearing.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, RpcClientError> {
        match self.call_once(method, params.clone()).await {
            Err(RpcClientError::Connect { .. }) => self.call_once(method, params).await,
            other => other,
        }
    }

    async fn call_once(&self, method: &str, params: Value) -> Result<Value, RpcClientError> {
        let request = RpcRequest {
            id: 1,
            method: method.to_string(),
            params,
        };

        let work = async {
            let stream = UnixStream::connect(&self.socket).await.map_err(|source| {
                RpcClientError::Connect {
                    path: self.socket.display().to_string(),
                    source,
                }
            })?;
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();

            let mut payload = serde_json::to_string(&request)
                .map_err(|e| RpcClientError::Protocol(e.to_string()))?;
            payload.push('\n');
            write_half
                .write_all(payload.as_bytes())
                .await
                .map_err(|e| RpcClientError::Protocol(e.to_string()))?;
            write_half
                .flush()
                .await
                .map_err(|e| RpcClientError::Protocol(e.to_string()))?;

            let line = lines
                .next_line()
                .await
                .map_err(|e| RpcClientError::Protocol(e.to_string()))?
                .ok_or_else(|| RpcClientError::Protocol("daemon closed the connection".into()))?;

            let response: RpcResponse = serde_json::from_str(&line)
                .map_err(|e| RpcClientError::Protocol(format!("invalid response: {e}")))?;

            if let Some(error) = response.error {
                return Err(RpcClientError::Remote {
                    code: error.code,
                    message: error.message,
                    data: error.data,
                });
            }
            response.result.ok_or_else(|| {
                RpcClientError::Protocol("response has neither result nor error".into())
            })
        };

        tokio::time::timeout(CALL_TIMEOUT, work)
            .await
            .map_err(|_| RpcClientError::Timeout)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::net::UnixListener;

    async fn spawn_stub<F>(handler: F) -> (PathBuf, tokio::task::JoinHandle<()>)
    where
        F: Fn(Value) -> Option<String> + Send + Sync + 'static,
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stub.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let handler = std::sync::Arc::new(handler);
        let handle = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let (read_half, mut write_half) = stream.into_split();
                let mut lines = BufReader::new(read_half).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let request: Value = serde_json::from_str(&line).unwrap_or(json!({}));
                    if let Some(response) = handler(request.clone()) {
                        let _ = write_half
                            .write_all(format!("{response}\n").as_bytes())
                            .await;
                        let _ = write_half.flush().await;
                    }
                }
            }
        });
        // Keep the tempdir alive for the duration of the test.
        std::mem::forget(dir);
        (path, handle)
    }

    #[tokio::test]
    async fn call_round_trip() {
        let (path, handle) = spawn_stub(|request| {
            let id = request["id"].as_u64().unwrap_or(0);
            Some(json!({"id": id, "result": {"ok": true}}).to_string())
        })
        .await;

        let client = RpcClient::new(path);
        let result = client.call("ping", json!({})).await.unwrap();
        assert_eq!(result["ok"], json!(true));
        handle.abort();
    }

    #[tokio::test]
    async fn remote_errors_are_surfaced() {
        let (path, handle) = spawn_stub(|request| {
            let id = request["id"].as_u64().unwrap_or(0);
            Some(
                json!({"id": id, "error": {"code": -32601, "message": "unknown method: send_message"}})
                    .to_string(),
            )
        })
        .await;

        let client = RpcClient::new(path);
        let error = client.call("send_message", json!({})).await.unwrap_err();
        match error {
            RpcClientError::Remote { code, message, .. } => {
                assert_eq!(code, -32601);
                assert!(message.contains("unknown method"));
            }
            other => panic!("unexpected error: {other}"),
        }
        handle.abort();
    }

    #[tokio::test]
    async fn missing_socket_is_a_connection_error() {
        let client = RpcClient::new("/nonexistent/discord-reader.sock");
        let error = client.call("ping", json!({})).await.unwrap_err();
        assert!(matches!(error, RpcClientError::Connect { .. }));
    }

    #[tokio::test]
    async fn structured_error_data_survives_the_rpc_client() {
        let (path, handle) = spawn_stub(|request| Some(serde_json::json!({"id":request["id"],"error":{"code":-32602,"message":"invalid cursor","data":{"error":{"error_source":"client","code":"INVALID_PARAMS","operation":"list_changed_channels","retryable":false,"message":"invalid cursor"}}}}).to_string())).await;
        let error = RpcClient::new(path)
            .call("list_changed_channels", json!({"cursor":"invalid"}))
            .await
            .unwrap_err();
        match error {
            RpcClientError::Remote {
                data: Some(data), ..
            } => assert_eq!(data["error"]["operation"], "list_changed_channels"),
            other => panic!("unexpected error: {other}"),
        }
        handle.abort();
    }
}
