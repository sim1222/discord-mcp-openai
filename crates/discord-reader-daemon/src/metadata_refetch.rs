//! Refresh retained metadata without inferring historical coverage.

use std::{collections::HashMap, sync::Arc};

use discord_api::types::Message;
use discord_store::{Store, StoreError};
use serde::Serialize;
use serde_json::{json, Value};

use crate::message_lookup::MessageLookup;

/// Retained messages awaiting authentic metadata, ordered newest first.
pub(crate) trait RefetchCache: Send + Sync {
    fn pending(
        &self,
        channel: &str,
        before: Option<&str>,
        limit: u32,
    ) -> Result<Vec<String>, StoreError>;
    fn save(&self, message: &Message) -> Result<(), StoreError>;
    fn remaining(&self, channel: &str) -> Result<u64, StoreError>;
}

/// Receives snapshots of a bounded metadata refresh.
pub(crate) trait RefetchProgress: Send + Sync {
    fn record(&self, report: &RefetchReport);
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RefetchReport {
    pub(crate) scope: &'static str,
    pub(crate) status: &'static str,
    pub(crate) channels_total: usize,
    pub(crate) channels_done: usize,
    pub(crate) attempted: u64,
    pub(crate) refetched: u64,
    pub(crate) remaining: Option<u64>,
    pub(crate) remaining_channels: Vec<Value>,
    pub(crate) next_refetch_before: HashMap<String, String>,
    pub(crate) failures: Vec<Value>,
}

pub(crate) struct SqliteRefetchCache {
    store: Arc<Store>,
}

impl SqliteRefetchCache {
    pub(crate) fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

impl RefetchCache for SqliteRefetchCache {
    fn pending(
        &self,
        channel: &str,
        before: Option<&str>,
        limit: u32,
    ) -> Result<Vec<String>, StoreError> {
        let mut cursor = before.map(str::to_owned);
        let mut pending = Vec::new();
        while pending.len() < limit as usize {
            let rows = self
                .store
                .channel_messages(channel, 200, cursor.as_deref(), None)?;
            let finished = rows.len() < 200;
            cursor = rows.last().map(|row| row.id.clone());
            for row in rows {
                if row.known_view().is_none() {
                    pending.push(row.id);
                    if pending.len() == limit as usize {
                        break;
                    }
                }
            }
            if finished {
                break;
            }
        }
        Ok(pending)
    }

    fn save(&self, message: &Message) -> Result<(), StoreError> {
        self.store.insert_message(message)
    }

    fn remaining(&self, channel: &str) -> Result<u64, StoreError> {
        let mut cursor = None;
        let mut count = 0;
        loop {
            let rows = self
                .store
                .channel_messages(channel, 200, cursor.as_deref(), None)?;
            let finished = rows.len() < 200;
            cursor = rows.last().map(|row| row.id.clone());
            count += rows.iter().filter(|row| row.known_view().is_none()).count() as u64;
            if finished {
                return Ok(count);
            }
        }
    }
}

fn cache_failure(channel: &str, error: StoreError) -> Value {
    json!({"channel_id": channel, "error": {
        "error_source": "cache", "code": error.code(), "message": error.to_string(),
        "operation": "metadata_refetch", "retryable": error.retryable()
    }})
}

/// Attempts each selected pending row once; refreshed rows persist as the resume state.
pub(crate) async fn run_refetch(
    cache: &dyn RefetchCache,
    lookup: &dyn MessageLookup,
    progress: &dyn RefetchProgress,
    channels: &[String],
    max_messages: u32,
    refetch_before: &HashMap<String, String>,
) -> RefetchReport {
    let mut report = RefetchReport {
        scope: "refetch",
        status: "running",
        channels_total: channels.len(),
        channels_done: 0,
        attempted: 0,
        refetched: 0,
        remaining: None,
        remaining_channels: Vec::new(),
        next_refetch_before: refetch_before.clone(),
        failures: Vec::new(),
    };
    progress.record(&report);
    let mut cache_failed = false;
    for channel in channels {
        let failures_before = report.failures.len();
        let mut channel_exhausted = false;
        let mut before = refetch_before.get(channel).cloned();
        while report.attempted < u64::from(max_messages) {
            let pending = match cache.pending(channel, before.as_deref(), 1) {
                Ok(pending) => pending,
                Err(error) => {
                    report.failures.push(cache_failure(channel, error));
                    cache_failed = true;
                    break;
                }
            };
            let Some(id) = pending.into_iter().next() else {
                channel_exhausted = true;
                break;
            };
            before = Some(id.clone());
            report
                .next_refetch_before
                .insert(channel.clone(), id.clone());
            report.attempted += 1;
            match lookup.lookup(channel, &id).await {
                Ok(located) => match cache.save(&located.message) {
                    Ok(()) => report.refetched += 1,
                    Err(error) => {
                        report.failures.push(cache_failure(channel, error));
                        cache_failed = true;
                        break;
                    }
                },
                Err(error) => report.failures.push(json!({"channel_id": channel, "message_id": id, "error": error.error_payload()})),
            }
            progress.record(&report);
        }
        if channel_exhausted && report.failures.len() == failures_before {
            report.channels_done += 1;
        }
        if cache_failed {
            break;
        }
    }
    let mut remaining = 0;
    for channel in channels {
        match cache.remaining(channel) {
            Ok(count) => {
                remaining += count;
                report
                    .remaining_channels
                    .push(json!({"channel_id": channel, "messages_requiring_refetch": count}));
            }
            Err(error) => {
                report.failures.push(cache_failure(channel, error));
                cache_failed = true;
                break;
            }
        }
    }
    if !cache_failed {
        report.remaining = Some(remaining);
        report.channels_done = report
            .remaining_channels
            .iter()
            .filter(|entry| {
                entry["messages_requiring_refetch"] == 0
                    && !report
                        .failures
                        .iter()
                        .any(|failure| failure["channel_id"] == entry["channel_id"])
            })
            .count();
    }
    report.status = if cache_failed {
        "failed"
    } else if remaining > 0 && report.attempted >= u64::from(max_messages) {
        "paused"
    } else if !report.failures.is_empty() || remaining > 0 {
        "done_with_gaps"
    } else {
        "done"
    };
    progress.record(&report);
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_lookup::{LocatedMessage, LookupError};
    use async_trait::async_trait;
    use std::sync::Mutex;

    fn message(id: &str) -> Message {
        serde_json::from_value(serde_json::json!({
            "id": id, "channel_id": "200", "content": "retained",
            "timestamp": "2026-10-07T08:30:00Z", "author": {"id": "7"},
            "mentions": [{"id": "8"}]
        }))
        .unwrap()
    }

    struct Lookup;

    #[async_trait]
    impl MessageLookup for Lookup {
        async fn lookup(
            &self,
            channel_id: &str,
            message_id: &str,
        ) -> Result<LocatedMessage, LookupError> {
            assert_eq!(channel_id, "200");
            Ok(LocatedMessage {
                message: message(message_id),
                raw: serde_json::json!({}),
                operation: "refetch_test".into(),
            })
        }
    }

    #[derive(Default)]
    struct Progress(Mutex<Vec<RefetchReport>>);

    impl RefetchProgress for Progress {
        fn record(&self, report: &RefetchReport) {
            self.0.lock().unwrap().push(report.clone());
        }
    }

    fn legacy_store(path: &std::path::Path) -> Arc<Store> {
        let connection = rusqlite::Connection::open(path).unwrap();
        connection.execute_batch("CREATE TABLE channels (id TEXT PRIMARY KEY, guild_id TEXT, name TEXT, kind INTEGER NOT NULL, parent_id TEXT, topic TEXT);").unwrap();
        connection.execute_batch("CREATE TABLE messages (id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, guild_id TEXT, author_id TEXT, timestamp TEXT NOT NULL, edited_timestamp TEXT, content TEXT NOT NULL); INSERT INTO messages VALUES ('10','200',NULL,'7','2026-10-07T08:30:00Z',NULL,'retained'),('20','200',NULL,'7','2026-10-07T08:30:00Z',NULL,'retained');").unwrap();
        drop(connection);
        Arc::new(Store::open(path).unwrap())
    }

    #[tokio::test]
    async fn legacy_refetch_is_bounded_and_resumes_after_reopening_without_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.sqlite3");
        let store = legacy_store(&path);
        let cache = SqliteRefetchCache::new(Arc::clone(&store));
        let report = run_refetch(
            &cache,
            &Lookup,
            &Progress::default(),
            &["200".into()],
            1,
            &HashMap::new(),
        )
        .await;
        assert_eq!(report.status, "paused");
        assert_eq!(report.attempted, 1);
        assert_eq!(report.refetched, 1);
        assert_eq!(report.remaining, Some(1));
        assert!(store
            .get_message("20")
            .unwrap()
            .unwrap()
            .known_view()
            .is_some());
        assert!(store.coverage("200").unwrap().is_empty());
        assert!(store.channel_sync("200").unwrap().is_none());
        drop(cache);
        drop(store);
        let store = Arc::new(Store::open(&path).unwrap());
        let cache = SqliteRefetchCache::new(Arc::clone(&store));
        let report = run_refetch(
            &cache,
            &Lookup,
            &Progress::default(),
            &["200".into()],
            10,
            &HashMap::new(),
        )
        .await;
        assert_eq!(report.status, "done");
        assert_eq!(report.attempted, 1);
        assert_eq!(report.remaining, Some(0));
        assert_eq!(store.message_count().unwrap(), 2);
        assert!(store.coverage("200").unwrap().is_empty());
    }

    struct FailingCache;

    impl RefetchCache for FailingCache {
        fn pending(&self, _: &str, _: Option<&str>, _: u32) -> Result<Vec<String>, StoreError> {
            Err(StoreError::Schema("missing metadata".into()))
        }
        fn save(&self, _: &Message) -> Result<(), StoreError> {
            unreachable!()
        }
        fn remaining(&self, _: &str) -> Result<u64, StoreError> {
            Err(StoreError::Schema("missing metadata".into()))
        }
    }

    #[tokio::test]
    async fn cache_failure_is_reported_instead_of_success() {
        let progress = Progress::default();
        let report = run_refetch(
            &FailingCache,
            &Lookup,
            &progress,
            &["200".into()],
            10,
            &HashMap::new(),
        )
        .await;
        assert_eq!(report.status, "failed");
        assert_eq!(report.failures[0]["error"]["code"], "CACHE_SCHEMA_MISMATCH");
        assert_eq!(report.failures[0]["error"]["retryable"], false);
    }

    struct MissingLookup;

    #[async_trait]
    impl MessageLookup for MissingLookup {
        async fn lookup(
            &self,
            channel_id: &str,
            message_id: &str,
        ) -> Result<LocatedMessage, LookupError> {
            Err(LookupError::NotObserved {
                channel_id: channel_id.into(),
                message_id: message_id.into(),
                operation: "metadata_refetch".into(),
            })
        }
    }

    #[tokio::test]
    async fn missing_messages_stay_pending_and_do_not_block_other_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.sqlite3");
        let store = legacy_store(&path);
        let cache = SqliteRefetchCache::new(Arc::clone(&store));
        let report = run_refetch(
            &cache,
            &MissingLookup,
            &Progress::default(),
            &["200".into()],
            10,
            &HashMap::new(),
        )
        .await;
        assert_eq!(report.status, "done_with_gaps");
        assert_eq!(report.attempted, 2);
        assert_eq!(report.refetched, 0);
        assert_eq!(report.remaining, Some(2));
        assert_eq!(report.failures.len(), 2);
        assert_eq!(report.channels_done, 0);
        assert!(store
            .get_message("10")
            .unwrap()
            .unwrap()
            .known_view()
            .is_none());
        assert!(store.coverage("200").unwrap().is_empty());
        assert_eq!(store.message_count().unwrap(), 2);
    }

    #[test]
    fn pending_scans_past_known_pages_and_respects_exclusive_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.sqlite3");
        let store = legacy_store(&path);
        let known: Vec<Message> = (1000..1205).map(|id| message(&id.to_string())).collect();
        store.insert_messages(&known).unwrap();
        let cache = SqliteRefetchCache::new(store);
        assert_eq!(cache.pending("200", None, 2).unwrap(), vec!["20", "10"]);
        assert_eq!(cache.pending("200", Some("20"), 2).unwrap(), vec!["10"]);
        assert_eq!(cache.remaining("200").unwrap(), 2);
    }

    struct NewestUnavailable;

    #[async_trait]
    impl MessageLookup for NewestUnavailable {
        async fn lookup(
            &self,
            channel_id: &str,
            message_id: &str,
        ) -> Result<LocatedMessage, LookupError> {
            if message_id == "20" {
                MissingLookup.lookup(channel_id, message_id).await
            } else {
                Lookup.lookup(channel_id, message_id).await
            }
        }
    }

    #[tokio::test]
    async fn continuation_cursor_advances_past_failed_newer_rows_without_marking_them_known() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.sqlite3");
        let store = legacy_store(&path);
        let cache = SqliteRefetchCache::new(Arc::clone(&store));
        let first = run_refetch(
            &cache,
            &NewestUnavailable,
            &Progress::default(),
            &["200".into()],
            1,
            &HashMap::new(),
        )
        .await;
        assert_eq!(first.status, "paused");
        assert_eq!(first.next_refetch_before["200"], "20");
        let retry = run_refetch(
            &cache,
            &NewestUnavailable,
            &Progress::default(),
            &["200".into()],
            1,
            &HashMap::new(),
        )
        .await;
        assert_eq!(retry.refetched, 0);
        let continued = run_refetch(
            &cache,
            &NewestUnavailable,
            &Progress::default(),
            &["200".into()],
            1,
            &first.next_refetch_before,
        )
        .await;
        assert_eq!(continued.refetched, 1);
        assert_eq!(continued.remaining, Some(1));
        assert_eq!(continued.next_refetch_before["200"], "10");
        assert!(store
            .get_message("10")
            .unwrap()
            .unwrap()
            .known_view()
            .is_some());
        assert!(store
            .get_message("20")
            .unwrap()
            .unwrap()
            .known_view()
            .is_none());
        assert!(store.coverage("200").unwrap().is_empty());
    }
}
