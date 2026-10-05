//! Discord-backed implementation of [`ReaderApi`].
//!
//! Everything here is read-only: the only network operations are the
//! allowlisted `GET` requests of [`DiscordRequest`]. Fetched data is written
//! to the SQLite cache; search reads the cache first.

use std::sync::Arc;

use discord_api::{
    types::{Channel, ChannelView, DmView, Guild, GuildView, Message, MessageView},
    DiscordRequest, SharedDiscordClient,
};
use discord_store::{SearchQuery, Store};
use serde_json::{json, Value};

use crate::rpc::{
    BeforeParams, ChannelParams, ContextParams, GuildIdParams, MessageParams, ReaderApi, RpcError,
    SearchParams, ThreadParams,
};

/// Reads Discord on demand and caches the results.
pub struct DaemonApi {
    client: SharedDiscordClient,
    store: Arc<Store>,
}

impl std::fmt::Debug for DaemonApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonApi")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

fn api_error(error: discord_api::DiscordError) -> RpcError {
    // DiscordError display strings never contain the credential.
    RpcError::internal(error.to_string())
}

fn store_error(error: discord_store::StoreError) -> RpcError {
    RpcError::internal(format!("cache error: {error}"))
}

impl DaemonApi {
    pub fn new(client: SharedDiscordClient, store: Arc<Store>) -> Self {
        Self { client, store }
    }

    async fn fetch_messages(
        &self,
        channel_id: &str,
        limit: u32,
        before: Option<&str>,
        after: Option<&str>,
        around: Option<&str>,
    ) -> Result<Vec<Message>, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::GetMessages {
                channel_id: channel_id.to_string(),
                limit: limit.min(100) as u8,
                before: before.map(str::to_string),
                after: after.map(str::to_string),
                around: around.map(str::to_string),
            })
            .await
            .map_err(api_error)?;
        let messages: Vec<Message> = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected message payload: {e}")))?;
        self.store.insert_messages(&messages).map_err(store_error)?;
        Ok(messages)
    }

    fn cache_message(&self, message: &Message) {
        if let Err(error) = self.store.insert_message(message) {
            tracing::warn!(%error, "failed to cache message");
        }
    }
}

#[async_trait::async_trait]
impl ReaderApi for DaemonApi {
    async fn ping(&self) -> Result<Value, RpcError> {
        // Deliberately touches neither Discord nor the cache: this is the
        // health check for the RPC socket.
        Ok(json!({"ok": true}))
    }

    async fn list_guilds(&self) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::ListGuilds {
                limit: 200,
                before: None,
                after: None,
            })
            .await
            .map_err(api_error)?;
        let guilds: Vec<Guild> = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected guild payload: {e}")))?;
        for guild in &guilds {
            if let Err(error) = self.store.upsert_guild(guild) {
                tracing::warn!(%error, "failed to cache guild");
            }
        }
        let views: Vec<GuildView> = guilds.iter().map(GuildView::from).collect();
        Ok(json!({"guilds": views}))
    }

    async fn list_channels(&self, params: GuildIdParams) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::GetGuildChannels {
                guild_id: params.guild_id.clone(),
            })
            .await
            .map_err(api_error)?;
        let mut channels: Vec<Channel> = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected channel payload: {e}")))?;
        for channel in &mut channels {
            if channel.guild_id.is_none() {
                channel.guild_id = Some(params.guild_id.clone());
            }
            if let Err(error) = self.store.upsert_channel(channel) {
                tracing::warn!(%error, "failed to cache channel");
            }
        }
        let mut views: Vec<ChannelView> = channels.iter().map(ChannelView::from).collect();
        views.sort_by(|a, b| {
            a.parent_id
                .is_none()
                .cmp(&b.parent_id.is_none())
                .then(a.name.cmp(&b.name))
        });
        Ok(json!({"channels": views}))
    }

    async fn recent_messages(&self, params: ChannelParams) -> Result<Value, RpcError> {
        let messages = self
            .fetch_messages(&params.channel_id, params.limit, None, None, None)
            .await?;
        let views: Vec<MessageView> = messages.iter().map(MessageView::from).collect();
        Ok(json!({"channel_id": params.channel_id, "messages": views}))
    }

    async fn messages_before(&self, params: BeforeParams) -> Result<Value, RpcError> {
        let messages = self
            .fetch_messages(
                &params.channel_id,
                params.limit,
                Some(&params.before_message_id),
                None,
                None,
            )
            .await?;
        let views: Vec<MessageView> = messages.iter().map(MessageView::from).collect();
        Ok(json!({"channel_id": params.channel_id, "messages": views}))
    }

    async fn get_message(&self, params: MessageParams) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::GetMessage {
                channel_id: params.channel_id.clone(),
                message_id: params.message_id.clone(),
            })
            .await
            .map_err(api_error)?;
        let message: Message = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected message payload: {e}")))?;
        self.cache_message(&message);
        let view = MessageView::from(&message);
        Ok(json!({"message": view}))
    }

    async fn message_context(&self, params: ContextParams) -> Result<Value, RpcError> {
        // The message itself, then the surrounding conversation.
        let value = self
            .client
            .execute(DiscordRequest::GetMessage {
                channel_id: params.channel_id.clone(),
                message_id: params.message_id.clone(),
            })
            .await
            .map_err(api_error)?;
        let message: Message = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected message payload: {e}")))?;
        self.cache_message(&message);

        let before = if params.before == 0 {
            Vec::new()
        } else {
            self.fetch_messages(
                &params.channel_id,
                params.before,
                Some(&params.message_id),
                None,
                None,
            )
            .await?
        };
        let after = if params.after == 0 {
            Vec::new()
        } else {
            self.fetch_messages(
                &params.channel_id,
                params.after,
                None,
                Some(&params.message_id),
                None,
            )
            .await?
        };

        // Normalize to chronological order so that
        // `before ++ [message] ++ after` reads naturally.
        let mut before_views: Vec<MessageView> = before.iter().map(MessageView::from).collect();
        before_views.sort_by_key(|m| snowflake_key(&m.id));
        let mut after_views: Vec<MessageView> = after.iter().map(MessageView::from).collect();
        after_views.sort_by_key(|m| snowflake_key(&m.id));

        Ok(json!({
            "channel_id": params.channel_id,
            "message_id": params.message_id,
            "before": before_views,
            "message": MessageView::from(&message),
            "after": after_views,
        }))
    }

    async fn search_messages(&self, params: SearchParams) -> Result<Value, RpcError> {
        // Optional targeted refresh: only the explicitly named channel is
        // fetched, and only its recent messages. Never a crawl.
        if params.refresh {
            if let Some(channel_id) = params.channel_id.as_deref() {
                self.fetch_messages(channel_id, 50, None, None, None)
                    .await?;
            }
        }

        let query = SearchQuery::new(params.query.clone())
            .limit(params.limit.clamp(1, 200))
            .guild_id(params.guild_id.clone())
            .channel_id(params.channel_id.clone())
            .author_id(params.author_id.clone())
            .after(params.after.clone())
            .before(params.before.clone());

        let hits = self.store.search(&query).map_err(store_error)?;
        let results: Vec<_> = hits
            .iter()
            .map(|hit| {
                json!({
                    "message": hit.message.to_view(),
                    "relevance": hit.relevance,
                })
            })
            .collect();
        Ok(json!({
            "results": results,
            "source": "local-fts",
            "cached_messages": self.store.message_count().map_err(store_error)?,
        }))
    }

    async fn read_thread(&self, params: ThreadParams) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::GetChannel {
                channel_id: params.thread_id.clone(),
            })
            .await
            .map_err(api_error)?;
        let thread: Channel = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected channel payload: {e}")))?;
        if let Err(error) = self.store.upsert_channel(&thread) {
            tracing::warn!(%error, "failed to cache thread");
        }

        let messages = self
            .fetch_messages(&params.thread_id, params.limit, None, None, None)
            .await?;
        let views: Vec<MessageView> = messages.iter().map(MessageView::from).collect();
        Ok(json!({
            "thread": ChannelView::from(&thread),
            "messages": views,
        }))
    }

    async fn list_threads(&self, params: GuildIdParams) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::GetGuildActiveThreads {
                guild_id: params.guild_id.clone(),
            })
            .await
            .map_err(api_error)?;
        let parsed: serde_json::Value = value;
        let empty = Vec::new();
        let threads = parsed
            .get("threads")
            .and_then(|t| t.as_array())
            .unwrap_or(&empty);
        let mut views = Vec::new();
        for thread in threads {
            match serde_json::from_value::<Channel>(thread.clone()) {
                Ok(channel) => {
                    if let Err(error) = self.store.upsert_channel(&channel) {
                        tracing::warn!(%error, "failed to cache thread");
                    }
                    views.push(ChannelView::from(&channel));
                }
                Err(error) => {
                    tracing::warn!(%error, "skipping malformed thread entry");
                }
            }
        }
        Ok(json!({"threads": views}))
    }

    async fn list_dms(&self) -> Result<Value, RpcError> {
        let value = self
            .client
            .execute(DiscordRequest::ListUserDmChannels)
            .await
            .map_err(api_error)?;
        let channels: Vec<Channel> = serde_json::from_value(value)
            .map_err(|e| RpcError::internal(format!("unexpected dm payload: {e}")))?;
        let dms: Vec<_> = channels
            .iter()
            .filter(|c| c.kind == 1 || c.kind == 3)
            .map(DmView::from)
            .collect();
        Ok(json!({"dms": dms}))
    }
}

/// Numeric ordering key for a Discord snowflake (falling back to 0).
fn snowflake_key(id: &str) -> u64 {
    id.parse::<u64>().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{BeforeParams, ChannelParams, ContextParams, MessageParams, SearchParams};
    use discord_api::{DiscordClient, Token, TokenKind};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    const SECRET: &str = "SECRET-USER-TOKEN-DO-NOT-LEAK";

    /// Minimal HTTP/1.1 test server: records request lines and replays canned
    /// responses. Used to prove that only GET requests ever leave the client.
    struct MockServer {
        base: String,
        requests: Arc<Mutex<Vec<String>>>,
    }

    async fn spawn_mock(responses: VecDeque<(u16, String)>) -> MockServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let requests_clone = Arc::clone(&requests);
        let responses = Arc::new(Mutex::new(responses));

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let requests = Arc::clone(&requests_clone);
                let responses = Arc::clone(&responses);
                tokio::spawn(async move {
                    let (read_half, mut write_half) = stream.into_split();
                    let mut lines = BufReader::new(read_half).lines();
                    let mut request_line = String::new();
                    while let Ok(Some(line)) = lines.next_line().await {
                        if request_line.is_empty() {
                            request_line = line;
                        } else if line.is_empty() {
                            break;
                        }
                    }
                    requests.lock().unwrap().push(request_line);
                    let (status, body) = responses
                        .lock()
                        .unwrap()
                        .pop_front()
                        .unwrap_or((404, "{\"message\":\"not mocked\"}".to_string()));
                    let reason = if status == 200 { "OK" } else { "Mock" };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = write_half.write_all(response.as_bytes()).await;
                    let _ = write_half.flush().await;
                });
            }
        });

        MockServer { base, requests }
    }

    fn api_for(base: &str) -> (DaemonApi, Arc<discord_store::Store>) {
        let client = DiscordClient::with_options(
            Token::new(SECRET).unwrap(),
            TokenKind::User,
            base.to_string(),
            2,
        )
        .unwrap();
        let store = Arc::new(discord_store::Store::open_in_memory().unwrap());
        let api = DaemonApi::new(Arc::new(client), Arc::clone(&store));
        (api, store)
    }

    #[tokio::test]
    async fn list_guilds_is_get_only_and_cached() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"[{"id":"42","name":"ZENVR"}]"#.to_string(),
        )]))
        .await;
        let (api, store) = api_for(&mock.base);

        let result = api.list_guilds().await.unwrap();
        let text = serde_json::to_string(&result).unwrap();
        assert!(text.contains("ZENVR"));
        assert!(
            !text.contains(SECRET),
            "response must not contain the token"
        );

        let requests = mock.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0].starts_with("GET "),
            "only GET may reach Discord, got: {}",
            requests[0]
        );
        assert!(requests[0].contains("/users/@me/guilds"));

        let cached = store.guilds().unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].name, "ZENVR");
    }

    #[tokio::test]
    async fn recent_messages_are_cached_and_searchable() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"[{"id":"10","channel_id":"200","guild_id":"42","author":{"id":"7","username":"alice"},"content":"展軸祭の準備をしています","timestamp":"2026-10-05T00:00:00.000000+00:00"}]"#
                .to_string(),
        )]))
        .await;
        let (api, _store) = api_for(&mock.base);

        let result = api
            .recent_messages(ChannelParams {
                channel_id: "200".into(),
                limit: 50,
            })
            .await
            .unwrap();
        assert!(serde_json::to_string(&result).unwrap().contains("展軸祭"));

        let search = api
            .search_messages(SearchParams {
                query: "展軸祭".into(),
                guild_id: Some("42".into()),
                channel_id: None,
                author_id: None,
                after: None,
                before: None,
                limit: 50,
                refresh: false,
            })
            .await
            .unwrap();
        let text = serde_json::to_string(&search).unwrap();
        assert!(text.contains("展軸祭の準備"), "search must hit the cache");
        assert!(text.contains("local-fts"));
        assert!(!text.contains(SECRET));
    }

    #[tokio::test]
    async fn message_context_fetches_before_and_after() {
        let message = r#"{"id":"100","channel_id":"200","author":{"id":"7","username":"alice"},"content":"target","timestamp":"2026-10-05T00:00:00.000000+00:00"}"#;
        let before = r#"[{"id":"98","channel_id":"200","author":{"id":"7","username":"alice"},"content":"older two","timestamp":"2026-10-04T00:00:00.000000+00:00"},{"id":"99","channel_id":"200","author":{"id":"7","username":"alice"},"content":"older one","timestamp":"2026-10-04T01:00:00.000000+00:00"}]"#;
        let after = r#"[{"id":"101","channel_id":"200","author":{"id":"7","username":"alice"},"content":"newer one","timestamp":"2026-10-06T00:00:00.000000+00:00"}]"#;
        let mock = spawn_mock(VecDeque::from([
            (200, message.to_string()),
            (200, before.to_string()),
            (200, after.to_string()),
        ]))
        .await;
        let (api, _store) = api_for(&mock.base);

        let context = api
            .message_context(ContextParams {
                channel_id: "200".into(),
                message_id: "100".into(),
                before: 20,
                after: 20,
            })
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::to_value(&context).unwrap();
        assert_eq!(value["message"]["content"], "target");
        assert_eq!(value["before"][0]["content"], "older two");
        assert_eq!(value["before"][1]["content"], "older one");
        assert_eq!(value["after"][0]["content"], "newer one");

        let requests = mock.requests.lock().unwrap().clone();
        assert!(requests.iter().all(|r| r.starts_with("GET ")));
    }

    #[tokio::test]
    async fn messages_before_paging_uses_before_param() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"[{"id":"9","channel_id":"200","author":{"id":"7","username":"alice"},"content":"old","timestamp":"2026-10-04T00:00:00.000000+00:00"}]"#.to_string(),
        )]))
        .await;
        let (api, _store) = api_for(&mock.base);

        let result = api
            .messages_before(BeforeParams {
                channel_id: "200".into(),
                before_message_id: "10".into(),
                limit: 50,
            })
            .await
            .unwrap();
        assert!(serde_json::to_string(&result).unwrap().contains("old"));

        let requests = mock.requests.lock().unwrap().clone();
        assert!(requests[0].contains("before=10") || requests[0].contains("before=10&"));
    }

    #[tokio::test]
    async fn get_message_returns_normalized_view() {
        let mock = spawn_mock(VecDeque::from([(
            200,
            r#"{"id":"10","channel_id":"200","guild_id":"42","author":{"id":"7","username":"alice"},"content":"hello","timestamp":"2026-10-05T00:00:00.000000+00:00","attachments":[{"id":"a","filename":"x.png","url":"https://cdn/x.png","size":10,"content_type":"image/png"}]}"#.to_string(),
        )]))
        .await;
        let (api, _store) = api_for(&mock.base);

        let result = api
            .get_message(MessageParams {
                channel_id: "200".into(),
                message_id: "10".into(),
            })
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::to_value(&result).unwrap();
        assert_eq!(value["message"]["author"]["name"], "alice");
        assert_eq!(value["message"]["attachments"][0]["filename"], "x.png");
        assert!(!serde_json::to_string(&result).unwrap().contains(SECRET));
    }

    #[tokio::test]
    async fn debug_output_redacts_the_credential() {
        let mock = spawn_mock(VecDeque::new()).await;
        let (api, _store) = api_for(&mock.base);
        let dump = format!("{api:?}");
        assert!(!dump.contains(SECRET));
        assert!(dump.contains("[REDACTED]"));
    }
}
