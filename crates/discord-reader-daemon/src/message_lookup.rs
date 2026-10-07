//! Exact message observations independent of cache and synchronization state.

use discord_api::{types::Message, ApiError, DiscordRequest, SharedDiscordClient, TokenKind};
use serde_json::{json, Value};

#[derive(Debug)]
pub(crate) struct LocatedMessage {
    pub(crate) raw: Value,
    pub(crate) message: Message,
    pub(crate) operation: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LookupError {
    #[error("Discord message lookup failed: {0:?}")]
    Api(Box<ApiError>),
    #[error("message {message_id} was not observed in channel {channel_id}")]
    NotObserved {
        channel_id: String,
        message_id: String,
        operation: String,
    },
    #[error("invalid message payload: {message}")]
    InvalidPayload { operation: String, message: String },
}

impl LookupError {
    pub(crate) fn error_payload(&self) -> Value {
        match self {
            Self::Api(api) => json!(api),
            Self::NotObserved {
                channel_id,
                message_id,
                operation,
            } => json!({
                "error_source": "message_lookup", "code": "message_not_observed",
                "retryable": false, "operation": operation,
                "channel_id": channel_id, "message_id": message_id,
                "message": "target was not observed; deletion is not established"
            }),
            Self::InvalidPayload { operation, message } => json!({
                "error_source": "client", "code": "invalid_response", "retryable": false,
                "operation": operation, "message": message
            }),
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait MessageLookup: Send + Sync {
    async fn lookup(
        &self,
        channel_id: &str,
        message_id: &str,
    ) -> Result<LocatedMessage, LookupError>;
}

pub(crate) struct DiscordMessageLookup {
    client: SharedDiscordClient,
}

impl DiscordMessageLookup {
    pub(crate) fn new(client: SharedDiscordClient) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl MessageLookup for DiscordMessageLookup {
    async fn lookup(
        &self,
        channel_id: &str,
        message_id: &str,
    ) -> Result<LocatedMessage, LookupError> {
        DiscordRequest::GetMessage {
            channel_id: channel_id.into(),
            message_id: message_id.into(),
        }
        .path()
        .map_err(|error| {
            LookupError::Api(Box::new(ApiError::from_discord_error(
                &error,
                Some("message lookup".into()),
            )))
        })?;
        let req = match self.client.token_kind() {
            TokenKind::User => DiscordRequest::GetMessages {
                channel_id: channel_id.into(),
                limit: 1,
                before: None,
                after: None,
                around: Some(message_id.into()),
            },
            TokenKind::Bot => DiscordRequest::GetMessage {
                channel_id: channel_id.into(),
                message_id: message_id.into(),
            },
        };
        // Build operation from validated identifiers, before any network request.
        let path = req.path().map_err(|error| {
            LookupError::Api(Box::new(ApiError::from_discord_error(
                &error,
                Some(req.bucket_key()),
            )))
        })?;
        let operation = if self.client.token_kind() == TokenKind::User {
            format!("GET {path}?limit=1&around={message_id}")
        } else {
            format!("GET {path}")
        };
        let payload = self.client.execute_for(&req).await.map_err(|mut api| {
            api.operation = Some(operation.clone());
            LookupError::Api(Box::new(api))
        })?;
        let raw = if self.client.token_kind() == TokenKind::User {
            let messages = payload
                .as_array()
                .ok_or_else(|| LookupError::InvalidPayload {
                    operation: operation.clone(),
                    message: "expected message array".into(),
                })?;
            messages
                .iter()
                .find(|message| {
                    message["id"].as_str() == Some(message_id)
                        && message["channel_id"].as_str() == Some(channel_id)
                })
                .cloned()
        } else {
            (payload["id"].as_str() == Some(message_id)
                && payload["channel_id"].as_str() == Some(channel_id))
            .then_some(payload)
        }
        .ok_or_else(|| LookupError::NotObserved {
            channel_id: channel_id.into(),
            message_id: message_id.into(),
            operation: operation.clone(),
        })?;
        let message = parse_message(raw.clone()).map_err(|error| match error {
            LookupError::InvalidPayload { message, .. } => LookupError::InvalidPayload {
                operation: operation.clone(),
                message,
            },
            other => other,
        })?;
        Ok(LocatedMessage {
            raw,
            message,
            operation,
        })
    }
}

pub(crate) fn parse_message(raw: Value) -> Result<Message, LookupError> {
    let deleted_reply = raw.get("referenced_message").is_some_and(Value::is_null)
        && raw.get("message_reference").is_some_and(|reference| {
            reference["type"].as_u64() != Some(1)
                && (raw["type"].as_u64() == Some(19)
                    || reference["type"].as_u64().unwrap_or(0) == 0)
        });
    let mut message: Message =
        serde_json::from_value(raw).map_err(|error| LookupError::InvalidPayload {
            operation: "decode message".into(),
            message: error.to_string(),
        })?;
    if deleted_reply {
        message.referenced_message_status = Some("deleted".into());
    }
    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use discord_api::{DiscordClient, Token};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    async fn lookup_response(
        kind: TokenKind,
        status: u16,
        body: &str,
    ) -> (DiscordMessageLookup, Arc<Mutex<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let request = Arc::new(Mutex::new(String::new()));
        let recorded = Arc::clone(&request);
        let body = body.to_string();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            let mut headers = String::new();
            while let Some(line) = lines.next_line().await.unwrap() {
                if line.is_empty() {
                    break;
                }
                headers.push_str(&line);
                headers.push('\n');
            }
            *recorded.lock().unwrap() = headers;
            let response = format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}", body.len());
            write.write_all(response.as_bytes()).await.unwrap();
        });
        let client =
            DiscordClient::with_options(Token::new("test-token").unwrap(), kind, base, 1).unwrap();
        (DiscordMessageLookup::new(Arc::new(client)), request)
    }

    #[tokio::test]
    async fn user_lookup_selects_exact_message_and_preserves_raw_fields() {
        let (lookup, request) = lookup_response(TokenKind::User, 200,
            r#"[{"id":"9","channel_id":"200"},{"id":"10","channel_id":"200","future_field":{"x":true}}]"#).await;
        let found = lookup.lookup("200", "10").await.unwrap();
        assert_eq!(found.message.id, "10");
        assert_eq!(found.raw["future_field"]["x"], true);
        assert_eq!(
            found.operation,
            "GET /channels/200/messages?limit=1&around=10"
        );
        let recorded = request.lock().unwrap();
        assert!(recorded.starts_with("GET /channels/200/messages?limit=1&around=10 HTTP/1.1"));
        assert!(recorded.contains("authorization: test-token"));
        assert!(!recorded.contains("Bot "));
    }

    #[tokio::test]
    async fn missing_or_wrong_channel_target_is_not_observed() {
        for body in [
            "[]",
            r#"[{"id":"9","channel_id":"200"}]"#,
            r#"[{"id":"10","channel_id":"201"}]"#,
        ] {
            let (lookup, _) = lookup_response(TokenKind::User, 200, body).await;
            let error = lookup.lookup("200", "10").await.unwrap_err();
            assert_eq!(error.error_payload()["code"], "message_not_observed");
            assert_eq!(error.error_payload()["retryable"], false);
        }
    }

    #[tokio::test]
    async fn bot_lookup_uses_direct_route() {
        let (lookup, request) =
            lookup_response(TokenKind::Bot, 200, r#"{"id":"10","channel_id":"200"}"#).await;
        assert_eq!(
            lookup.lookup("200", "10").await.unwrap().operation,
            "GET /channels/200/messages/10"
        );
        assert!(request
            .lock()
            .unwrap()
            .contains("authorization: Bot test-token"));
    }

    #[tokio::test]
    async fn discord_failures_preserve_bot_only_and_forbidden() {
        for (discord_code, expected) in [(20002, "bot_only"), (50013, "forbidden")] {
            let (lookup, _) = lookup_response(
                TokenKind::User,
                403,
                &format!(r#"{{"code":{discord_code},"message":"denied"}}"#),
            )
            .await;
            let payload = lookup
                .lookup("200", "10")
                .await
                .unwrap_err()
                .error_payload();
            assert_eq!(payload["code"], expected);
            assert_eq!(payload["discord_code"], discord_code);
            assert_eq!(payload["retryable"], false);
            assert_eq!(
                payload["operation"],
                "GET /channels/200/messages?limit=1&around=10"
            );
        }
    }

    #[test]
    fn reply_null_proves_deletion_but_missing_and_forwarding_do_not() {
        for (raw, expected) in [
            (
                r#"{"id":"10","type":19,"message_reference":{"message_id":"9"},"referenced_message":null}"#,
                Some("deleted"),
            ),
            (
                r#"{"id":"10","type":19,"message_reference":{"message_id":"9"}}"#,
                None,
            ),
            (
                r#"{"id":"10","message_reference":{"type":0,"message_id":"9"},"referenced_message":null}"#,
                Some("deleted"),
            ),
            (
                r#"{"id":"10","message_reference":{"type":1,"message_id":"9"},"referenced_message":null}"#,
                None,
            ),
        ] {
            let message = parse_message(serde_json::from_str(raw).unwrap()).unwrap();
            assert_eq!(message.referenced_message_status.as_deref(), expected);
        }
    }

    #[tokio::test]
    async fn malformed_lookup_payload_is_nonretryable() {
        let (lookup, _) = lookup_response(TokenKind::User, 200, "{}").await;
        assert_eq!(
            lookup
                .lookup("200", "10")
                .await
                .unwrap_err()
                .error_payload()["code"],
            "invalid_response"
        );
    }
}
