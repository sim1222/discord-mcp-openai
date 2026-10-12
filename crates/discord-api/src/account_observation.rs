//! Account read-state observations from Discord's user Gateway.

use crate::{types::Channel, Token};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{future::Future, time::Duration};
use tokio_tungstenite::tungstenite::Message as Frame;

/// An observed Discord channel read position and notification count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadStateObservation {
    pub channel_id: String,
    pub last_read_message_id: Option<String>,
    pub discord_mention_count: Option<u64>,
    pub observed_at: String,
    pub version: Option<String>,
    pub availability: String,
    pub reason: Option<String>,
}

/// Account state received at one Gateway READY boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountObservation {
    pub account_user_id: String,
    pub observed_at: String,
    pub version: Option<String>,
    pub partial: bool,
    pub read_states: Vec<ReadStateObservation>,
    pub channels: Vec<Channel>,
    pub inventory_complete: bool,
    #[serde(default)]
    pub inventory_errors: Vec<String>,
}

/// Observe account state without acknowledging messages or changing settings.
pub trait AccountObserver {
    fn observe_account(
        &self,
    ) -> impl Future<Output = Result<AccountObservation, ObservationError>> + Send;
}

/// Fixed failure categories never contain credentials or remote payloads.
#[derive(Debug, thiserror::Error)]
pub enum ObservationError {
    #[error("account read state requires user authentication")]
    Unsupported,
    #[error("gateway connection or read failed")]
    Transport,
    #[error("gateway observation timed out")]
    Timeout,
    #[error("gateway session rejected or closed")]
    SessionRejected,
    #[error("gateway returned an invalid account observation")]
    InvalidPayload,
}

fn snowflake(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|id| id.parse::<u64>().is_ok_and(|n| n > 0))
        .map(str::to_owned)
}

impl AccountObservation {
    pub(crate) fn from_ready(value: &Value, at: &str) -> Result<Self, ObservationError> {
        let account_user_id =
            snowflake(&value["user"]["id"]).ok_or(ObservationError::InvalidPayload)?;
        let state = value
            .get("read_state")
            .ok_or(ObservationError::InvalidPayload)?;
        let entries = state["entries"]
            .as_array()
            .ok_or(ObservationError::InvalidPayload)?;
        let version = state.get("version").and_then(|v| {
            v.as_str()
                .map(str::to_owned)
                .or_else(|| v.as_u64().map(|n| n.to_string()))
        });
        let partial = state["partial"].as_bool().unwrap_or(true);
        let mut read_states = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for entry in entries {
            if entry
                .get("read_state_type")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                != 0
            {
                continue;
            }
            let channel_id = snowflake(&entry["id"]).ok_or(ObservationError::InvalidPayload)?;
            if !seen.insert(channel_id.clone()) {
                return Err(ObservationError::InvalidPayload);
            }
            let count = entry.get("mention_count").and_then(Value::as_u64);
            let last_read_message_id = snowflake(&entry["last_message_id"]);
            read_states.push(ReadStateObservation {
                channel_id,
                last_read_message_id,
                discord_mention_count: count,
                observed_at: at.into(),
                version: version.clone(),
                availability: if count.is_some() {
                    "available"
                } else {
                    "partial"
                }
                .into(),
                reason: count.is_none().then(|| "mention_count_not_observed".into()),
            });
        }
        let mut channels = Vec::new();
        let mut inventory_errors = Vec::new();
        if let Some(private) = value["private_channels"].as_array() {
            for c in private {
                if let Ok(channel) = serde_json::from_value::<Channel>(c.clone()) {
                    channels.push(channel);
                } else {
                    inventory_errors.push("channel_parse_failed".into());
                }
            }
        } else {
            inventory_errors.push("private_channel_list_unavailable".into());
        }
        if let Some(guilds) = value["guilds"].as_array() {
            for guild in guilds {
                if guild["unavailable"] == true {
                    inventory_errors.push("guild_unavailable".into());
                    continue;
                }
                let Some(id) = snowflake(&guild["id"]) else {
                    inventory_errors.push("guild_id_missing".into());
                    continue;
                };
                let channel_list = if guild["data_mode"] == "partial" {
                    &guild["partial_updates"]["channels"]
                } else {
                    &guild["channels"]
                };
                for (field, required) in [(channel_list, true), (&guild["threads"], false)] {
                    if let Some(list) = field.as_array() {
                        for c in list {
                            if let Ok(mut channel) = serde_json::from_value::<Channel>(c.clone()) {
                                channel.guild_id = Some(id.clone());
                                channels.push(channel);
                            } else {
                                inventory_errors.push("channel_parse_failed".into());
                            }
                        }
                    } else if required || !field.is_null() {
                        inventory_errors.push("channel_list_unavailable".into());
                    }
                }
            }
        } else {
            inventory_errors.push("guild_list_unavailable".into());
        }
        Ok(Self {
            account_user_id,
            observed_at: at.into(),
            version,
            partial,
            read_states,
            channels,
            inventory_complete: false,
            inventory_errors,
        })
    }
}

// No generic opcode or arbitrary data operation exists at the send boundary.
enum GatewayReadOperation<'a> {
    Identify(&'a Token),
    Heartbeat(Option<u64>),
}

impl GatewayReadOperation<'_> {
    fn payload(&self) -> Value {
        match self {
            Self::Heartbeat(seq) => json!({"op":1,"d":seq}),
            Self::Identify(token) => json!({"op":2,"d":{
                "token":token.expose(),"capabilities":1734653,
                "properties":{"os":"linux","browser":"Discord Reader","device":"Discord Reader"},
                "compress":false,"client_state":{"guild_versions":{}}
            }}),
        }
    }
}

const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(30);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) async fn observe(token: &Token) -> Result<AccountObservation, ObservationError> {
    let token = token.clone();
    let (response, receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + OBSERVATION_TIMEOUT;
        let mut response = response;
        let connection = tokio::select! {
            _ = response.closed() => return,
            result = tokio::time::timeout_at(deadline, tokio_tungstenite::connect_async("wss://gateway.discord.gg/?v=9&encoding=json")) => result,
        };
        match connection {
            Ok(Ok((socket, _))) => {
                observe_socket(socket, token, response, deadline, CLEANUP_TIMEOUT).await;
            }
            Ok(Err(_)) => {
                let _ = response.send(Err(ObservationError::Transport));
            }
            Err(_) => {
                let _ = response.send(Err(ObservationError::Timeout));
            }
        }
    });
    receiver.await.unwrap_or(Err(ObservationError::Transport))
}

async fn observe_socket<S>(
    mut socket: tokio_tungstenite::WebSocketStream<S>,
    token: Token,
    mut response: tokio::sync::oneshot::Sender<Result<AccountObservation, ObservationError>>,
    deadline: tokio::time::Instant,
    cleanup_timeout: Duration,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let result = tokio::select! {
        _ = response.closed() => None,
        result = tokio::time::timeout_at(deadline, receive_ready_events(&mut socket, &token)) => {
            Some(result.unwrap_or(Err(ObservationError::Timeout)))
        }
    };
    let cleanup = tokio::time::timeout(cleanup_timeout, close_observation(&mut socket)).await;
    let outcome = match cleanup {
        Ok(Ok(())) => "completed",
        Ok(Err(())) => "transport_failed",
        Err(_) => "timed_out",
    };
    tracing::debug!(operation = "gateway_observation_cleanup", outcome);
    if let Some(result) = result {
        let _ = response.send(result);
    }
}

async fn close_observation<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Result<(), ()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
    let close = socket
        .close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        }))
        .await;
    // Receiving a peer close queues its reply; flush it even if a new close is rejected.
    socket.flush().await.map_err(|_| ())?;
    close.map_err(|_| ())?;
    while let Some(frame) = socket.next().await {
        match frame {
            Ok(Frame::Close(_)) => {
                socket.flush().await.map_err(|_| ())?;
                return Ok(());
            }
            Ok(_) => {}
            Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed) => return Ok(()),
            Err(_) => return Err(()),
        }
    }
    Ok(())
}

#[cfg(test)]
async fn receive_ready<S>(
    socket: tokio_tungstenite::WebSocketStream<S>,
    token: &Token,
) -> Result<AccountObservation, ObservationError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    receive_ready_bounded(socket, token, OBSERVATION_TIMEOUT, CLEANUP_TIMEOUT).await
}

#[cfg(test)]
async fn receive_ready_bounded<S>(
    socket: tokio_tungstenite::WebSocketStream<S>,
    token: &Token,
    observation_timeout: Duration,
    cleanup_timeout: Duration,
) -> Result<AccountObservation, ObservationError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (response, receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(observe_socket(
        socket,
        token.clone(),
        response,
        tokio::time::Instant::now() + observation_timeout,
        cleanup_timeout,
    ));
    receiver.await.unwrap_or(Err(ObservationError::Transport))
}

async fn receive_ready_events<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    token: &Token,
) -> Result<AccountObservation, ObservationError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut seq = None;
    let mut heartbeat = None;
    let mut heartbeat_interval = Duration::from_secs(30);
    let mut heartbeat_pending = false;
    let mut identified = false;
    loop {
        let frame = if let Some(at) = heartbeat {
            tokio::select! {
                frame = socket.next() => frame,
                _ = tokio::time::sleep_until(at) => {
                    if heartbeat_pending { return Err(ObservationError::SessionRejected); }
                    socket.send(Frame::Text(GatewayReadOperation::Heartbeat(seq).payload().to_string().into())).await.map_err(|_| ObservationError::Transport)?;
                    heartbeat_pending=true;
                    heartbeat = Some(tokio::time::Instant::now()+heartbeat_interval);
                    continue;
                }
            }
        } else {
            socket.next().await
        };
        let frame = frame
            .ok_or(ObservationError::SessionRejected)?
            .map_err(|_| ObservationError::Transport)?;
        let Frame::Text(text) = frame else {
            if matches!(frame, Frame::Close(_)) {
                return Err(ObservationError::SessionRejected);
            }
            continue;
        };
        let event: Value =
            serde_json::from_str(&text).map_err(|_| ObservationError::InvalidPayload)?;
        if let Some(s) = event["s"].as_u64() {
            seq = Some(s);
        }
        match event["op"].as_u64() {
            Some(10) if !identified => {
                let interval = event["d"]["heartbeat_interval"]
                    .as_u64()
                    .filter(|n| *n > 0)
                    .ok_or(ObservationError::InvalidPayload)?;
                heartbeat_interval = Duration::from_millis(interval);
                heartbeat = Some(tokio::time::Instant::now() + heartbeat_interval);
                socket
                    .send(Frame::Text(
                        GatewayReadOperation::Identify(token)
                            .payload()
                            .to_string()
                            .into(),
                    ))
                    .await
                    .map_err(|_| ObservationError::Transport)?;
                identified = true;
            }
            Some(1) => {
                socket
                    .send(Frame::Text(
                        GatewayReadOperation::Heartbeat(seq)
                            .payload()
                            .to_string()
                            .into(),
                    ))
                    .await
                    .map_err(|_| ObservationError::Transport)?;
                heartbeat_pending = true;
                heartbeat = Some(tokio::time::Instant::now() + heartbeat_interval);
            }
            Some(11) => heartbeat_pending = false,
            Some(7 | 9) => return Err(ObservationError::SessionRejected),
            Some(0) if event["t"] == "READY" => {
                let at = chrono::Utc::now().to_rfc3339();
                return AccountObservation::from_ready(&event["d"], &at);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ready_preserves_discord_counts_and_never_invents_missing_state() {
        let snapshot = AccountObservation::from_ready(
            &json!({
                "user":{"id":"7"},
                "read_state":{"version":42,"partial":false,"entries":[
                    {"id":"200","mention_count":7,"last_message_id":"1500000000000000100"},
                    {"id":"300","last_message_id":0},
                    {"id":"7","read_state_type":1,"badge_count":9,"last_acked_id":"6"}
                ]},"guilds":[],"private_channels":[]
            }),
            "now",
        )
        .unwrap();
        assert_eq!(snapshot.read_states.len(), 2);
        assert_eq!(snapshot.read_states[0].discord_mention_count, Some(7));
        assert_eq!(
            snapshot.read_states[0].last_read_message_id.as_deref(),
            Some("1500000000000000100")
        );
        assert_eq!(snapshot.read_states[1].discord_mention_count, None);
        assert_eq!(snapshot.read_states[1].last_read_message_id, None);
        assert_eq!(snapshot.version.as_deref(), Some("42"));
    }

    #[test]
    fn partial_ready_and_malformed_ids_are_not_complete_observations() {
        assert!(AccountObservation::from_ready(
            &json!({"user":{"id":"x"},"read_state":{"entries":[]}}),
            "now"
        )
        .is_err());
        let s=AccountObservation::from_ready(&json!({"user":{"id":"7"},"read_state":{"partial":true,"entries":[]},"guilds":[],"private_channels":[]}),"now").unwrap();
        assert!(s.partial);
        assert!(!s.inventory_complete);
    }

    #[test]
    fn outbound_operations_cannot_express_ack_or_presence_updates() {
        assert_eq!(GatewayReadOperation::Heartbeat(None).payload()["op"], 1);
        let token = crate::Token::new("secret-value").unwrap();
        let payload = GatewayReadOperation::Identify(&token).payload();
        assert_eq!(payload["op"], 2);
        assert!(payload["d"].get("presence").is_none());
        assert!(payload["d"].get("intents").is_none());
    }

    #[test]
    fn malformed_channels_and_unavailable_guilds_leave_inventory_evidence() {
        let observation = AccountObservation::from_ready(
            &json!({"user":{"id":"7"},"read_state":{"partial":false,"entries":[]},
                "private_channels":[{"type":1}],"guilds":[{"id":"9","unavailable":true}]}),
            "now",
        )
        .unwrap();
        let result = serde_json::to_value(observation).unwrap();
        assert_eq!(
            result["inventory_errors"],
            json!(["channel_parse_failed", "guild_unavailable"])
        );
        assert_eq!(result["inventory_complete"], false);
    }

    #[test]
    fn malformed_inventory_lists_and_missing_guild_ids_are_recorded() {
        let observation = AccountObservation::from_ready(
            &json!({"user":{"id":"7"},"read_state":{"partial":false,"entries":[]},
                "private_channels":{},"guilds":[{"channels":[{"id":"11","type":0}]},{"id":"9","channels":{}}]}),
            "now",
        ).unwrap();
        let result = serde_json::to_value(observation).unwrap();
        assert_eq!(
            result["inventory_errors"],
            json!([
                "private_channel_list_unavailable",
                "guild_id_missing",
                "channel_list_unavailable"
            ])
        );
        assert!(result["channels"].as_array().unwrap().is_empty());
    }

    async fn assert_normal_close(
        server: &mut tokio_tungstenite::WebSocketStream<tokio::io::DuplexStream>,
    ) {
        let frame = tokio::time::timeout(Duration::from_secs(1), server.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(frame, Frame::Close(Some(ref close)) if close.code == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal)
        );
        server.flush().await.unwrap();
    }

    #[tokio::test]
    async fn timeout_and_caller_abort_attempt_normal_close() {
        use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};
        for abort in [false, true] {
            let (client_io, server_io) = tokio::io::duplex(65536);
            let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
            let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
            let token = Token::new("test-token").unwrap();
            let task = tokio::spawn(async move {
                receive_ready_bounded(
                    client,
                    &token,
                    Duration::from_millis(50),
                    Duration::from_millis(100),
                )
                .await
            });
            server
                .send(Frame::Text(
                    json!({"op":10,"d":{"heartbeat_interval":10000}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let identify = server.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(identify.to_text().unwrap()).unwrap()["op"],
                2
            );
            if abort {
                task.abort();
            }
            assert_normal_close(&mut server).await;
            let result = task.await;
            if abort {
                assert!(result.unwrap_err().is_cancelled());
            } else {
                assert!(matches!(result.unwrap(), Err(ObservationError::Timeout)));
            }
            assert!(tokio::time::timeout(Duration::from_secs(1), server.next())
                .await
                .unwrap()
                .is_none());
        }
    }

    #[tokio::test]
    async fn ready_and_protocol_rejection_close_with_code_1000() {
        use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};
        for ready in [false, true] {
            let (client_io, server_io) = tokio::io::duplex(65536);
            let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
            let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
            let token = Token::new("test-token").unwrap();
            let task = tokio::spawn(async move { receive_ready(client, &token).await });
            let event = if ready {
                json!({"op":0,"t":"READY","d":{"user":{"id":"7"},"read_state":{"partial":false,"entries":[]},"guilds":[],"private_channels":[]}})
            } else {
                json!({"op":9})
            };
            server
                .send(Frame::Text(event.to_string().into()))
                .await
                .unwrap();
            assert_normal_close(&mut server).await;
            let result = task.await.unwrap();
            if ready {
                assert!(result.is_ok());
            } else {
                assert!(matches!(result, Err(ObservationError::SessionRejected)));
            }
        }
    }

    #[tokio::test]
    async fn unresponsive_peer_cannot_extend_cleanup_indefinitely() {
        use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};
        let (client_io, server_io) = tokio::io::duplex(65536);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        server
            .send(Frame::Text("invalid json".into()))
            .await
            .unwrap();
        let token = Token::new("test-token").unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(500),
            receive_ready_bounded(
                client,
                &token,
                Duration::from_millis(100),
                Duration::from_millis(25),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(ObservationError::InvalidPayload)));
        // The peer deliberately does not read or acknowledge the close until cleanup ends.
        assert!(
            matches!(server.next().await.unwrap().unwrap(), Frame::Close(Some(ref close)) if close.code == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal)
        );
    }

    #[tokio::test]
    async fn peer_close_is_flushed_and_worker_ends() {
        use tokio_tungstenite::{
            tungstenite::protocol::{frame::coding::CloseCode, CloseFrame, Role},
            WebSocketStream,
        };
        let (client_io, server_io) = tokio::io::duplex(65536);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let token = Token::new("test-token").unwrap();
        let task = tokio::spawn(async move { receive_ready(client, &token).await });
        server
            .close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "".into(),
            }))
            .await
            .unwrap();
        assert_normal_close(&mut server).await;
        assert!(matches!(
            task.await.unwrap(),
            Err(ObservationError::SessionRejected)
        ));
    }

    #[tokio::test]
    async fn observation_errors_attempt_normal_close() {
        use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};
        let (client_io, server_io) = tokio::io::duplex(65536);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let task = tokio::spawn(async move {
            server
                .send(Frame::Text("invalid json".into()))
                .await
                .unwrap();
            let frame = tokio::time::timeout(Duration::from_secs(1), server.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(
                matches!(frame, Frame::Close(Some(ref close)) if close.code == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal)
            );
            server.flush().await.unwrap();
        });
        let token = Token::new("test-token").unwrap();
        assert!(matches!(
            receive_ready(client, &token).await,
            Err(ObservationError::InvalidPayload)
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn requested_heartbeat_resets_ack_deadline() {
        use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};
        let (client_io, server_io) = tokio::io::duplex(65536);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let task = tokio::spawn(async move {
            server
                .send(Frame::Text(
                    json!({"op":10,"d":{"heartbeat_interval":100}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            server.next().await.unwrap().unwrap();
            tokio::time::sleep(Duration::from_millis(70)).await;
            server
                .send(Frame::Text(json!({"op":1,"d":null}).to_string().into()))
                .await
                .unwrap();
            let response = server.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(response.to_text().unwrap()).unwrap()["op"],
                1
            );
            tokio::time::sleep(Duration::from_millis(60)).await;
            server
                .send(Frame::Text(json!({"op":11,"d":null}).to_string().into()))
                .await
                .unwrap();
            server.send(Frame::Text(json!({"op":0,"t":"READY","d":{"user":{"id":"7"},"read_state":{"partial":false,"entries":[]}}}).to_string().into())).await.unwrap();
        });
        let token = Token::new("test-token").unwrap();
        assert!(receive_ready(client, &token).await.is_ok());
        task.await.unwrap();
    }
    #[tokio::test]
    async fn waiting_for_ready_keeps_heartbeating_without_other_operations() {
        use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};
        let (client_io, server_io) = tokio::io::duplex(65536);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let task = tokio::spawn(async move {
            server
                .send(Frame::Text(
                    json!({"op":10,"d":{"heartbeat_interval":10}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let identify = server.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(identify.to_text().unwrap()).unwrap()["op"],
                2
            );
            for _ in 0..2 {
                let heartbeat = server.next().await.unwrap().unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(heartbeat.to_text().unwrap()).unwrap()["op"],
                    1
                );
                server
                    .send(Frame::Text(json!({"op":11,"d":null}).to_string().into()))
                    .await
                    .unwrap();
            }
            server.send(Frame::Text(json!({"op":0,"t":"READY","s":3,"d":{"user":{"id":"7"},"read_state":{"version":1,"partial":false,"entries":[]},"guilds":[],"private_channels":[]}}).to_string().into())).await.unwrap();
        });
        let token = Token::new("test-token").unwrap();
        let observation =
            tokio::time::timeout(Duration::from_secs(1), receive_ready(client, &token))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(observation.account_user_id, "7");
        task.await.unwrap();
    }
}
