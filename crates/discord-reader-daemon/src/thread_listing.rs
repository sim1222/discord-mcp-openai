//! Parent-scoped thread search with explicit endpoint limitations.

use discord_api::{types::Channel, ApiError, DiscordRequest, SharedDiscordClient};
use serde_json::{json, Value};

#[derive(Debug, thiserror::Error)]
#[error("thread listing failed: {data}")]
pub(crate) struct ThreadListingError {
    data: Value,
}

impl ThreadListingError {
    pub(crate) fn payload(&self) -> Value {
        self.data.clone()
    }

    fn local(code: &str, message: &str, context: Value) -> Self {
        Self {
            data: json!({"error_source":"thread_listing", "code":code,
            "operation":"list_threads", "retryable":false, "message":message, "context":context}),
        }
    }

    fn api(mut error: ApiError, operation: String, context: Value) -> Self {
        error.operation = Some(operation);
        let mut data = json!(error);
        data["context"] = context;
        Self { data }
    }
}

#[derive(Debug)]
pub(crate) struct ThreadQuery {
    channel_id: String,
    guild_id: Option<String>,
    filter: String,
    archived: Option<bool>,
    pub(crate) limit: u32,
    offset: u32,
}

impl ThreadQuery {
    pub(crate) fn new(
        channel_id: Option<&str>,
        guild_id: Option<&str>,
        filter: Option<&str>,
        include_archived: Option<bool>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Self, ThreadListingError> {
        let channel_id = channel_id.ok_or_else(|| ThreadListingError::local("unsupported_scope",
            "user-compatible thread search requires a parent channel_id; guild-wide search is not established",
            json!({"guild_id":guild_id,"required_argument":"channel_id"})))?;
        let filter = match (filter.unwrap_or("all"), include_archived) {
            ("all", Some(true)) => "archived",
            ("all", Some(false)) => "active",
            (filter, _) => filter,
        };
        let archived =
            match filter {
                "active" => Some(false),
                "archived" => Some(true),
                "all" => include_archived,
                "joined" => return Err(ThreadListingError::local(
                    "unsupported_filter",
                    "joined search is not supported by this endpoint; use active, archived or all",
                    json!({"filter":filter,"channel_id":channel_id}),
                )),
                _ => {
                    return Err(ThreadListingError::local(
                        "invalid_filter",
                        "filter must be active, archived or all",
                        json!({"filter":filter}),
                    ))
                }
            };
        if !(1..=100).contains(&limit) {
            return Err(ThreadListingError::local(
                "invalid_limit",
                "limit must be between 1 and 100",
                json!({"limit":limit}),
            ));
        }
        let mut query = Self {
            channel_id: channel_id.into(),
            guild_id: guild_id.map(str::to_owned),
            filter: filter.into(),
            archived,
            limit: limit.min(25),
            offset: 0,
        };
        if let Some(cursor) = cursor {
            let prefix = format!("threads:{channel_id}:{filter}:");
            query.offset = cursor
                .strip_prefix(&prefix)
                .and_then(|value| value.parse::<u32>().ok())
                .filter(|offset| *offset <= 9975)
                .ok_or_else(|| {
                    ThreadListingError::local(
                        "invalid_cursor",
                        "cursor must come from the same channel and filter",
                        json!({"channel_id":channel_id,"filter":filter}),
                    )
                })?;
        }
        Ok(query)
    }

    fn context(&self) -> Value {
        json!({"channel_id":self.channel_id,"filter":self.filter,"offset":self.offset})
    }

    fn search_request(&self) -> DiscordRequest {
        DiscordRequest::ListChannelThreads {
            channel_id: self.channel_id.clone(),
            archived: self.archived,
            sort_by: Some("creation_time".into()),
            sort_order: Some("desc".into()),
            limit: self.limit,
            offset: self.offset,
        }
    }

    fn search_operation(&self) -> String {
        let request = self.search_request();
        let path = request
            .path()
            .unwrap_or_else(|_| "/channels/{invalid}/threads/search".into());
        let query = request
            .query()
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        format!("GET {path}?{query}")
    }

    fn validate_parent(&self, channel: &Channel) -> Result<(), ThreadListingError> {
        if channel.id != self.channel_id {
            return Err(ThreadListingError::local(
                "invalid_response",
                "channel metadata ID does not match target",
                self.context(),
            ));
        }
        if !matches!(channel.kind, 0 | 5 | 15 | 16) {
            let mut error = ThreadListingError::local(
                "unsupported_channel_kind",
                "thread search requires a text, announcement, forum or media parent channel",
                self.context(),
            );
            error.data["channel_kind"] = json!(channel.kind);
            return Err(error);
        }
        if self
            .guild_id
            .as_ref()
            .is_some_and(|guild| channel.guild_id.as_ref() != Some(guild))
        {
            return Err(ThreadListingError::local(
                "invalid_scope",
                "channel does not belong to requested guild",
                self.context(),
            ));
        }
        Ok(())
    }

    fn parse_page(&self, value: Value) -> Result<ThreadPage, ThreadListingError> {
        if value.get("code").and_then(Value::as_i64) == Some(110000) {
            return Err(ThreadListingError {
                data: json!({"error_source":"discord","code":"index_not_ready","discord_code":110000,
                "retryable":true,"retry_after_ms":value["retry_after"].as_f64().map(|seconds| (seconds*1000.0) as u64),
                "operation":self.search_operation(),"context":self.context(),"message":"Discord thread search index is not ready"}),
            });
        }
        let rows = value
            .get("threads")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ThreadListingError::local(
                    "invalid_response",
                    "expected threads array",
                    self.context(),
                )
            })?;
        let has_more = value
            .get("has_more")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                ThreadListingError::local(
                    "invalid_response",
                    "expected has_more boolean",
                    self.context(),
                )
            })?;
        if rows.len() > self.limit as usize || (rows.is_empty() && has_more) {
            return Err(ThreadListingError::local(
                "invalid_response",
                "thread page cannot be consumed safely",
                self.context(),
            ));
        }
        let mut threads = Vec::new();
        for row in rows {
            let channel: Channel = serde_json::from_value(row.clone()).map_err(|_| {
                ThreadListingError::local(
                    "invalid_response",
                    "malformed thread entry",
                    self.context(),
                )
            })?;
            if !matches!(channel.kind, 10..=12)
                || channel.parent_id.is_none()
                || channel.thread_metadata.is_none()
            {
                return Err(ThreadListingError::local(
                    "invalid_response",
                    "thread entry lacks type, parent or metadata",
                    self.context(),
                ));
            }
            if channel.parent_id.as_deref() == Some(&self.channel_id) {
                if self.archived.is_some_and(|archived| {
                    channel
                        .thread_metadata
                        .as_ref()
                        .and_then(|meta| meta.archived)
                        != Some(archived)
                }) {
                    return Err(ThreadListingError::local(
                        "invalid_response",
                        "thread entry does not match archived filter",
                        self.context(),
                    ));
                }
                threads.push(channel);
            }
        }
        let offset = self.offset + rows.len() as u32;
        let next_cursor = if has_more && offset <= 9975 {
            Some(format!(
                "threads:{}:{}:{offset}",
                self.channel_id, self.filter
            ))
        } else {
            None
        };
        let search_window_exhausted = has_more && next_cursor.is_none();
        Ok(ThreadPage {
            threads,
            next_cursor,
            has_more,
            effective_limit: self.limit,
            search_window_exhausted,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ThreadPage {
    pub(crate) threads: Vec<Channel>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) has_more: bool,
    pub(crate) effective_limit: u32,
    pub(crate) search_window_exhausted: bool,
}

#[async_trait::async_trait]
pub(crate) trait ThreadListing {
    async fn list(&self, query: &ThreadQuery) -> Result<ThreadPage, ThreadListingError>;
}

pub(crate) struct DiscordThreadListing {
    client: SharedDiscordClient,
}

impl DiscordThreadListing {
    pub(crate) fn new(client: SharedDiscordClient) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl ThreadListing for DiscordThreadListing {
    async fn list(&self, query: &ThreadQuery) -> Result<ThreadPage, ThreadListingError> {
        let metadata = self
            .client
            .execute_for(&DiscordRequest::GetChannel {
                channel_id: query.channel_id.clone(),
            })
            .await
            .map_err(|error| {
                ThreadListingError::api(
                    error,
                    DiscordRequest::GetChannel {
                        channel_id: query.channel_id.clone(),
                    }
                    .path()
                    .map(|path| format!("GET {path}"))
                    .unwrap_or_else(|_| "GET channel metadata".into()),
                    query.context(),
                )
            })?;
        if metadata.get("type").and_then(Value::as_i64).is_none() {
            return Err(ThreadListingError::local(
                "invalid_response",
                "parent channel metadata lacks a channel type",
                query.context(),
            ));
        }
        let channel = serde_json::from_value(metadata).map_err(|_| {
            ThreadListingError::local(
                "invalid_response",
                "malformed parent channel",
                query.context(),
            )
        })?;
        query.validate_parent(&channel)?;
        let value = self
            .client
            .execute_for(&query.search_request())
            .await
            .map_err(|error| {
                ThreadListingError::api(error, query.search_operation(), query.context())
            })?;
        query.parse_page(value).map_err(|mut error| {
            error.data["operation"] = json!(query.search_operation());
            error
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn query_accepts_active_limit_one_and_rejects_unsupported_inputs() {
        let query = ThreadQuery::new(Some("200"), None, Some("active"), None, 1, None).unwrap();
        assert_eq!(query.offset, 0);
        assert_eq!(query.limit, 1);
        assert_eq!(query.archived, Some(false));
        for (channel, filter, code) in [
            (None, "active", "unsupported_scope"),
            (Some("200"), "joined", "unsupported_filter"),
            (Some("200"), "nonsense", "invalid_filter"),
        ] {
            let error =
                ThreadQuery::new(channel, Some("42"), Some(filter), None, 1, None).unwrap_err();
            assert_eq!(error.payload()["code"], code);
            assert_eq!(error.payload()["retryable"], false);
        }
        assert!(ThreadQuery::new(Some("200"), None, Some("active"), None, 1, Some("300")).is_err());
        assert!(ThreadQuery::new(
            Some("201"),
            None,
            Some("active"),
            None,
            1,
            Some("threads:200:active:1")
        )
        .is_err());
    }

    #[test]
    fn page_validates_shape_and_index_pending_instead_of_empty_success() {
        let query = ThreadQuery::new(Some("200"), None, Some("active"), None, 1, None).unwrap();
        assert_eq!(
            query.parse_page(json!({})).unwrap_err().payload()["code"],
            "invalid_response"
        );
        let pending = query
            .parse_page(json!({"code":110000,"message":"Index not available","retry_after":2}))
            .unwrap_err()
            .payload();
        assert_eq!(pending["code"], "index_not_ready");
        assert_eq!(pending["retryable"], true);
        assert_eq!(pending["discord_code"], 110000);
        assert_eq!(pending["operation"], "GET /channels/200/threads/search?archived=false&sort_by=creation_time&sort_order=desc&limit=1&offset=0");
    }

    #[test]
    fn paging_advances_by_consumed_records_and_rejects_malformed_rows() {
        let query = ThreadQuery::new(Some("200"), None, Some("active"), None, 100, None).unwrap();
        assert_eq!(query.limit, 25);
        let page = query
            .parse_page(json!({"threads":[
            {"id":"300","type":11,"parent_id":"201","thread_metadata":{"archived":false}},
            {"id":"301","type":11,"parent_id":"200","thread_metadata":{"archived":false}}
        ],"has_more":true}))
            .unwrap();
        assert_eq!(page.threads.len(), 1);
        assert_eq!(page.next_cursor.as_deref(), Some("threads:200:active:2"));
        assert!(query
            .parse_page(json!({"threads":[{"bad":"row"}],"has_more":false}))
            .is_err());
        assert!(query
            .parse_page(json!({"threads":[],"has_more":true}))
            .is_err());
    }

    #[test]
    fn final_search_window_page_preserves_observations_and_marks_exhaustion() {
        let query = ThreadQuery::new(
            Some("200"),
            None,
            Some("active"),
            None,
            1,
            Some("threads:200:active:9975"),
        )
        .unwrap();
        let page=query.parse_page(json!({"threads":[{"id":"300","type":11,"parent_id":"200","thread_metadata":{"archived":false}}],"has_more":true})).unwrap();
        assert_eq!(page.threads.len(), 1);
        assert_eq!(page.next_cursor, None);
        assert!(page.has_more);
        assert!(page.search_window_exhausted);
        assert!(ThreadQuery::new(
            Some("200"),
            None,
            Some("active"),
            None,
            1,
            Some("threads:200:active:9976")
        )
        .is_err());
        let all = ThreadQuery::new(Some("200"), None, Some("all"), None, 1, None).unwrap();
        assert!(!all
            .search_request()
            .query()
            .iter()
            .any(|(key, _)| key == "archived"));
    }

    #[test]
    fn parent_kind_guard_explains_unsupported_channels() {
        let query = ThreadQuery::new(Some("200"), None, Some("active"), None, 1, None).unwrap();
        for kind in [1, 2, 3, 4, 10, 11, 12, 13] {
            let error = query
                .validate_parent(&Channel {
                    id: "200".into(),
                    kind,
                    ..Channel::default()
                })
                .unwrap_err();
            assert_eq!(error.payload()["code"], "unsupported_channel_kind");
            assert_eq!(error.payload()["channel_kind"], kind);
        }
        for kind in [0, 5, 15, 16] {
            query
                .validate_parent(&Channel {
                    id: "200".into(),
                    kind,
                    ..Channel::default()
                })
                .unwrap();
        }
    }
}
