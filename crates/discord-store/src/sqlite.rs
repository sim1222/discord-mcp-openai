//! SQLite cache for Discord data.
//!
//! The cache is written lazily: only data that a client already asked for is
//! stored. Nothing is crawled in the background.

use std::{path::Path, sync::Mutex};

use rusqlite::{params, Connection, OptionalExtension};

use discord_api::types::{
    AttachmentView, AuthorView, Channel, ChannelView, Guild, GuildView, Message, MessageView, Role,
    User,
};

use crate::search::{self, SearchQuery};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("search query error: {0}")]
    Search(String),
    #[error("cache data error: {0}")]
    Data(String),
    #[error("cache schema mismatch: {0}")]
    Schema(String),
    #[error("cache filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database migration failed for {database}: {source}")]
    Migration {
        database: String,
        #[source]
        source: Box<StoreError>,
    },
}

impl StoreError {
    /// Stable cache error classification for callers deciding whether to retry.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Schema(_) => "CACHE_SCHEMA_MISMATCH",
            Self::Migration { .. } => "CACHE_MIGRATION_FAILED",
            Self::Sqlite(error) => {
                let text = error.to_string();
                if text.contains("no such column")
                    || text.contains("no such table")
                    || text.contains("has no column named")
                {
                    "CACHE_SCHEMA_MISMATCH"
                } else if error.sqlite_error_code() == Some(rusqlite::ErrorCode::SchemaChanged) {
                    "CACHE_SCHEMA_CHANGED"
                } else if matches!(
                    error.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
                ) {
                    "CACHE_BUSY"
                } else {
                    "CACHE_SQLITE_ERROR"
                }
            }
            Self::Search(_) => "CACHE_QUERY_ERROR",
            Self::Data(_) => "CACHE_DATA_ERROR",
            Self::Io(_) => "CACHE_IO_ERROR",
        }
    }

    /// Only transient lock contention is safe to retry automatically.
    pub fn retryable(&self) -> bool {
        matches!(self.code(), "CACHE_BUSY" | "CACHE_SCHEMA_CHANGED")
    }
}

/// One cached message row, with author information joined in.
///
/// `message_json` is the verbatim wire payload as last seen from Discord (the
/// `*View` normalization output plus the raw fields we preserve), so a cached
/// message carries the same information as a freshly fetched one: reply
/// targets, mentions, thread references, attachments and embeds all survive
/// caching and restarts. `content` is kept as a plain column for FTS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRow {
    pub id: String,
    pub channel_id: String,
    pub guild_id: Option<String>,
    pub author_id: Option<String>,
    pub author_name: Option<String>,
    pub timestamp: String,
    pub edited_timestamp: Option<String>,
    pub content: String,
    /// The normalized `MessageView` as JSON; `None` for rows written before
    /// schema v2 (they are upgraded in place on the next fetch).
    pub message_json: Option<String>,
    /// Wall-clock time (ISO-8601, UTC) this row was last written/refreshed.
    pub fetched_at: Option<String>,
}

impl MessageRow {
    /// Available normalized metadata, or `None` when retrieval is required.
    pub fn known_view(&self) -> Option<MessageView> {
        let view: MessageView = serde_json::from_str(self.message_json.as_deref()?).ok()?;
        (view.metadata_state == "available").then_some(view)
    }
    /// Same normalized shape as API-fetched messages.
    ///
    /// When `message_json` is present it is used verbatim (so reply targets,
    /// mentions and thread metadata survive caching); otherwise a minimal
    /// view is reconstructed from the columns, with `content_kind` marked
    /// `unknown` so callers can tell the row is under-informed rather than
    /// genuinely empty.
    pub fn to_view(&self) -> MessageView {
        if let Some(view) = self.known_view() {
            return view;
        }
        MessageView {
            id: self.id.clone(),
            channel_id: self.channel_id.clone(),
            guild_id: self.guild_id.clone(),
            author: AuthorView {
                id: self.author_id.clone().unwrap_or_default(),
                name: self
                    .author_name
                    .clone()
                    .filter(|n| !n.is_empty())
                    .or_else(|| self.author_id.clone())
                    .unwrap_or_default(),
            },
            timestamp: self.timestamp.clone(),
            edited_timestamp: self.edited_timestamp.clone(),
            message_type: None,
            content: self.content.clone(),
            content_kind: if self.content.trim().is_empty() {
                "unknown".to_string()
            } else {
                "text".to_string()
            },
            metadata_state: "requires_refetch".to_string(),
            mentions: Vec::new(),
            mention_roles: Vec::new(),
            mention_everyone: None,
            reply_to: None,
            thread: None,
            message_snapshots: Vec::new(),
            attachments: Vec::<AttachmentView>::new(),
            embeds: Vec::new(),
            reactions: Vec::new(),
            pinned: None,
            flags: None,
            webhook_id: None,
        }
    }
}

/// Confirmed-coverage marker for one message ID range within a channel.
///
/// The coverage table records *what we know we have*: pages of messages that
/// were fetched contiguously from Discord. Ranges are merged per channel so a
/// resumable cursor can always be derived, and gaps (ids we never fetched)
/// are visible as holes between ranges.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CoverageRange {
    pub channel_id: String,
    /// Inclusive lowest message id in this confirmed range.
    pub from_id: String,
    /// Inclusive highest message id in this confirmed range.
    pub to_id: String,
}

/// Per-channel sync cursor and last-observed activity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ChannelSyncRow {
    pub channel_id: String,
    /// Highest message id we have confirmed for this channel (the resume
    /// point for `messages_after` / `sync_changes`).
    pub last_synced_message_id: Option<String>,
    /// Lowest message id we have confirmed (backfill progress).
    pub oldest_synced_message_id: Option<String>,
    /// Snowflake `last_message_id` Discord reported at the last channel listing.
    pub last_message_id: Option<String>,
    /// ISO-8601 timestamp of the newest message we hold, when known.
    pub last_activity_at: Option<String>,
    /// ISO-8601 wall-clock of our last successful fetch for this channel.
    pub last_fetched_at: Option<String>,
    /// `complete` when we have fetched to the channel's start (no gaps below
    /// `oldest_synced_message_id`), `partial` otherwise.
    pub backfill: String,
}

/// One cached role (for resolving role mentions and member role names).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleRow {
    pub id: String,
    pub guild_id: Option<String>,
    pub name: Option<String>,
}

/// A search hit from the local cache.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub message: MessageRow,
    /// Higher is better. Derived from SQLite FTS5 `bm25()`.
    pub relevance: f64,
}

/// SQLite-backed cache.
pub struct Store {
    conn: Mutex<Connection>,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS guilds (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS channels (
    id TEXT PRIMARY KEY,
    guild_id TEXT,
    name TEXT,
    kind INTEGER NOT NULL,
    parent_id TEXT,
    topic TEXT,
    last_message_id TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT,
    global_name TEXT
);

CREATE TABLE IF NOT EXISTS roles (
    id TEXT PRIMARY KEY,
    guild_id TEXT,
    name TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    guild_id TEXT,
    author_id TEXT,
    timestamp TEXT NOT NULL,
    edited_timestamp TEXT,
    content TEXT NOT NULL,
    message_json TEXT,
    fetched_at TEXT
);

CREATE INDEX IF NOT EXISTS messages_channel_idx
    ON messages (channel_id, CAST(id AS INTEGER));
CREATE INDEX IF NOT EXISTS messages_author_idx
    ON messages (author_id);
CREATE INDEX IF NOT EXISTS messages_timestamp_idx
    ON messages (timestamp);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    message_id UNINDEXED,
    content,
    tokenize = 'trigram'
);

-- Confirmed coverage of message-id space per channel.
CREATE TABLE IF NOT EXISTS coverage (
    channel_id TEXT NOT NULL,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    PRIMARY KEY (channel_id, from_id)
);

-- Per-channel sync cursor / activity.
CREATE TABLE IF NOT EXISTS channel_sync (
    channel_id TEXT PRIMARY KEY,
    last_synced_message_id TEXT,
    oldest_synced_message_id TEXT,
    last_message_id TEXT,
    last_activity_at TEXT,
    last_fetched_at TEXT,
    backfill TEXT NOT NULL DEFAULT 'partial'
);

-- Tombstones for deletions observed during differential sync.
CREATE TABLE IF NOT EXISTS deleted_messages (
    id TEXT PRIMARY KEY,
    channel_id TEXT,
    observed_at TEXT NOT NULL
);

-- Named server-side search cursors (resumable paging).
CREATE TABLE IF NOT EXISTS search_cursors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    offset_value INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
"#;

impl Store {
    /// Open (creating if needed) the cache database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let conn = Connection::open(path)?;
        Self::init(conn).map_err(|source| StoreError::Migration {
            database: path.display().to_string(),
            source: Box::new(source),
        })
    }

    /// In-memory database, used by tests.
    pub fn open_in_memory() -> Result<Self, StoreError> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(mut conn: Connection) -> Result<Self, StoreError> {
        crate::migration::migrate(&mut conn, SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn upsert_guild(&self, guild: &Guild) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        conn.execute(
            "INSERT INTO guilds (id, name) VALUES (?1, ?2)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name",
            params![guild.id, guild.name.clone().unwrap_or_default()],
        )?;
        Ok(())
    }

    pub fn upsert_channel(&self, channel: &Channel) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        conn.execute(
            "INSERT INTO channels (id, guild_id, name, kind, parent_id, topic, last_message_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
                guild_id = COALESCE(excluded.guild_id, channels.guild_id),
                name = COALESCE(excluded.name, channels.name),
                kind = excluded.kind,
                parent_id = COALESCE(excluded.parent_id, channels.parent_id),
                topic = COALESCE(excluded.topic, channels.topic),
                last_message_id = COALESCE(excluded.last_message_id, channels.last_message_id)",
            params![
                channel.id,
                channel.guild_id,
                channel.name,
                channel.kind,
                channel.parent_id,
                channel.topic,
                channel.last_message_id,
            ],
        )?;
        Ok(())
    }

    /// Cache one role (id -> name) so role mentions can be resolved by name.
    pub fn upsert_role(&self, guild_id: Option<&str>, role: &Role) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        conn.execute(
            "INSERT INTO roles (id, guild_id, name) VALUES (?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET
                guild_id = COALESCE(excluded.guild_id, roles.guild_id),
                name = COALESCE(excluded.name, roles.name)",
            params![role.id, guild_id, role.name],
        )?;
        Ok(())
    }

    /// Resolve a role id to its cached name, if known.
    pub fn role_name(&self, role_id: &str) -> Result<Option<String>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let name: Option<String> = conn
            .query_row(
                "SELECT name FROM roles WHERE id = ?1",
                params![role_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(name)
    }

    pub fn upsert_user(&self, user: &User) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        conn.execute(
            "INSERT INTO users (id, username, global_name) VALUES (?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET
                username = COALESCE(excluded.username, users.username),
                global_name = COALESCE(excluded.global_name, users.global_name)",
            params![user.id, user.username, user.global_name],
        )?;
        Ok(())
    }

    /// Cache one message (and its author) and keep the FTS index in sync.
    ///
    /// Synchronization happens in application code: the message row and its
    /// FTS row are written in the same transaction, and the FTS row is
    /// replaced when a message is re-fetched after an edit.
    pub fn insert_message(&self, message: &Message) -> Result<(), StoreError> {
        self.insert_message_at(message, &now_iso())
    }

    /// Insert with an explicit fetch timestamp (used by tests and by the
    /// daemon's observed-at bookkeeping).
    pub fn insert_message_at(&self, message: &Message, fetched_at: &str) -> Result<(), StoreError> {
        let view = MessageView::from(message);
        let message_json = serde_json::to_string(&view)
            .map_err(|e| StoreError::Search(format!("cannot serialize message view: {e}")))?;
        let conn = self.conn.lock().expect("store lock");
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO messages
                (id, channel_id, guild_id, author_id, timestamp, edited_timestamp, content,
                 message_json, fetched_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(id) DO UPDATE SET
                channel_id = excluded.channel_id,
                guild_id = excluded.guild_id,
                author_id = excluded.author_id,
                timestamp = excluded.timestamp,
                edited_timestamp = excluded.edited_timestamp,
                content = excluded.content,
                message_json = excluded.message_json,
                fetched_at = excluded.fetched_at",
            params![
                message.id,
                message.channel_id,
                message.guild_id,
                message.author.as_ref().map(|a| a.id.clone()),
                message.timestamp,
                message.edited_timestamp,
                message.content,
                message_json,
                fetched_at,
            ],
        )?;
        tx.execute(
            "DELETE FROM messages_fts WHERE message_id = ?1",
            params![message.id],
        )?;
        tx.execute(
            "INSERT INTO messages_fts (message_id, content) VALUES (?1, ?2)",
            params![message.id, message.content],
        )?;
        if let Some(author) = &message.author {
            tx.execute(
                "INSERT INTO users (id, username, global_name) VALUES (?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                    username = COALESCE(excluded.username, users.username),
                    global_name = COALESCE(excluded.global_name, users.global_name)",
                params![author.id, author.username, author.global_name],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn insert_messages(&self, messages: &[Message]) -> Result<(), StoreError> {
        for message in messages {
            self.insert_message(message)?;
        }
        Ok(())
    }

    pub fn guilds(&self) -> Result<Vec<GuildView>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare("SELECT id, name FROM guilds ORDER BY CAST(id AS INTEGER)")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(GuildView {
                    id: row.get(0)?,
                    name: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn channels(&self, guild_id: &str) -> Result<Vec<ChannelView>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare(
            "SELECT id, guild_id, name, kind, parent_id, topic
             FROM channels WHERE guild_id = ?1
             ORDER BY
                CASE WHEN parent_id IS NULL THEN 0 ELSE 1 END,
                COALESCE(parent_id, id),
                kind,
                name",
        )?;
        let rows = stmt
            .query_map(params![guild_id], |row| {
                Ok(ChannelView {
                    id: row.get(0)?,
                    guild_id: row.get(1)?,
                    name: row.get(2)?,
                    kind: row.get(3)?,
                    parent_id: row.get(4)?,
                    topic: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_message(&self, message_id: &str) -> Result<Option<MessageRow>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let row = conn
            .query_row(
                "SELECT m.id, m.channel_id, m.guild_id, m.author_id, m.timestamp,
                        m.edited_timestamp, m.content, u.username, u.global_name,
                        m.message_json, m.fetched_at
                 FROM messages m
                 LEFT JOIN users u ON u.id = m.author_id
                 WHERE m.id = ?1",
                params![message_id],
                row_to_message,
            )
            .optional()?;
        Ok(row)
    }

    /// Messages of a channel, newest first, optionally paged with `before` /
    /// `after` message snowflakes. Message ordering uses the numeric value of
    /// the snowflake, which matches Discord's chronological order.
    pub fn channel_messages(
        &self,
        channel_id: &str,
        limit: u32,
        before: Option<&str>,
        after: Option<&str>,
    ) -> Result<Vec<MessageRow>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare(
            "SELECT m.id, m.channel_id, m.guild_id, m.author_id, m.timestamp,
                    m.edited_timestamp, m.content, u.username, u.global_name,
                    m.message_json, m.fetched_at
             FROM messages m
             LEFT JOIN users u ON u.id = m.author_id
             WHERE m.channel_id = ?1
               AND (?2 IS NULL OR CAST(m.id AS INTEGER) < CAST(?2 AS INTEGER))
               AND (?3 IS NULL OR CAST(m.id AS INTEGER) > CAST(?3 AS INTEGER))
             ORDER BY CAST(m.id AS INTEGER) DESC
             LIMIT ?4",
        )?;
        let rows = stmt
            .query_map(params![channel_id, before, after, limit], row_to_message)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Full-text search over cached messages.
    pub fn search(&self, query: &SearchQuery) -> Result<Vec<SearchHit>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        search::search(&conn, query)
    }

    pub fn message_count(&self) -> Result<u64, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))?;
        Ok(count.max(0) as u64)
    }

    /// Schema version of this verified cache database.
    pub fn schema_version(&self) -> Result<u32, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        Ok(conn.pragma_query_value(None, "user_version", |row| row.get(0))?)
    }

    /// Number of retained messages whose metadata still needs retrieval.
    pub fn messages_requiring_refetch(&self) -> Result<u64, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare("SELECT message_json FROM messages")?;
        let mut rows = stmt.query([])?;
        let mut count = 0;
        while let Some(row) = rows.next()? {
            let json: Option<String> = row.get(0)?;
            if json
                .as_deref()
                .and_then(|s| serde_json::from_str::<MessageView>(s).ok())
                .is_none_or(|v| v.metadata_state != "available")
            {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Channel IDs known from retained messages, listings, or sync state.
    pub fn cached_channel_ids(&self, guild_id: Option<&str>) -> Result<Vec<String>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare(
            "SELECT DISTINCT channel_id FROM (
                SELECT id AS channel_id, guild_id FROM channels
                UNION SELECT channel_id, guild_id FROM messages
                UNION SELECT channel_id, NULL AS guild_id FROM channel_sync
             ) WHERE (?1 IS NULL OR guild_id = ?1) ORDER BY channel_id",
        )?;
        let rows = stmt
            .query_map([guild_id], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    /* ------------------------- coverage & sync state ------------------------- */

    /// Record that messages `[from_id, to_id]` (inclusive) have been fetched
    /// contiguously from Discord for `channel_id`, and advance the channel's
    /// sync cursor accordingly.
    ///
    /// Coverage ranges are merged so a gap (a range we never fetched) always
    /// shows up as a hole between two rows. This is what makes
    /// `covered_from`/`covered_to` and resume cursors honest: they describe
    /// confirmed knowledge, not optimism.
    pub fn record_coverage(
        &self,
        channel_id: &str,
        from_id: &str,
        to_id: &str,
        fetched_at: &str,
        backfill_complete: bool,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO coverage (channel_id, from_id, to_id) VALUES (?1, ?2, ?3)
             ON CONFLICT(channel_id, from_id) DO UPDATE SET
                to_id = MAX(CAST(coverage.to_id AS INTEGER), CAST(excluded.to_id AS INTEGER))",
            params![channel_id, from_id, to_id],
        )?;
        // Merge overlapping / adjacent ranges for this channel.
        merge_coverage(&tx, channel_id)?;
        // Advance the cursor with plain snowflake comparison (ids sort
        // numerically; text comparison is not reliable for snowflakes).
        let current: Option<(Option<String>, Option<String>)> = tx
            .query_row(
                "SELECT last_synced_message_id, oldest_synced_message_id
                 FROM channel_sync WHERE channel_id = ?1",
                params![channel_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (last_synced, oldest_synced) = match current {
            Some((last, old)) => (
                max_id(last.as_deref(), to_id),
                min_id(old.as_deref(), from_id),
            ),
            None => (Some(to_id.to_string()), Some(from_id.to_string())),
        };
        let backfill = if backfill_complete {
            "complete".to_string()
        } else {
            let existing: Option<String> = tx
                .query_row(
                    "SELECT backfill FROM channel_sync WHERE channel_id = ?1",
                    params![channel_id],
                    |row| row.get(0),
                )
                .optional()?;
            existing.unwrap_or_else(|| "partial".to_string())
        };
        tx.execute(
            "INSERT INTO channel_sync
                (channel_id, last_synced_message_id, oldest_synced_message_id, last_fetched_at,
                 backfill)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(channel_id) DO UPDATE SET
                last_synced_message_id = excluded.last_synced_message_id,
                oldest_synced_message_id = excluded.oldest_synced_message_id,
                last_fetched_at = COALESCE(excluded.last_fetched_at, channel_sync.last_fetched_at),
                backfill = excluded.backfill",
            params![channel_id, last_synced, oldest_synced, fetched_at, backfill],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Coverage ranges for a channel, ascending.
    pub fn coverage(&self, channel_id: &str) -> Result<Vec<CoverageRange>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare(
            "SELECT channel_id, from_id, to_id FROM coverage
             WHERE channel_id = ?1 ORDER BY CAST(from_id AS INTEGER)",
        )?;
        let rows = stmt
            .query_map(params![channel_id], |row| {
                Ok(CoverageRange {
                    channel_id: row.get(0)?,
                    from_id: row.get(1)?,
                    to_id: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The confirmed coverage envelope for a channel: (covered_from,
    /// covered_to, has_gaps). `None` when nothing is confirmed yet.
    pub fn coverage_envelope(
        &self,
        channel_id: &str,
    ) -> Result<Option<(String, String, bool)>, StoreError> {
        let ranges = self.coverage(channel_id)?;
        if ranges.is_empty() {
            return Ok(None);
        }
        let from = ranges[0].from_id.clone();
        let to = ranges[ranges.len() - 1].to_id.clone();
        let has_gaps = ranges.len() > 1;
        Ok(Some((from, to, has_gaps)))
    }

    /// Upsert per-channel sync metadata (last_message_id observed from the
    /// channel listing, last_activity_at, last_fetched_at).
    pub fn record_channel_sync(
        &self,
        channel_id: &str,
        last_message_id: Option<&str>,
        last_activity_at: Option<&str>,
        last_fetched_at: &str,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        conn.execute(
            "INSERT INTO channel_sync
                (channel_id, last_message_id, last_activity_at, last_fetched_at, backfill)
             VALUES (?1, ?2, ?3, ?4, 'partial')
             ON CONFLICT(channel_id) DO UPDATE SET
                last_message_id = COALESCE(excluded.last_message_id, channel_sync.last_message_id),
                last_activity_at = COALESCE(excluded.last_activity_at, channel_sync.last_activity_at),
                last_fetched_at = COALESCE(excluded.last_fetched_at, channel_sync.last_fetched_at)",
            params![channel_id, last_message_id, last_activity_at, last_fetched_at],
        )?;
        Ok(())
    }

    /// Mark a channel's backfill as complete (we have fetched to the channel's
    /// beginning; there is no unknown history below `oldest_synced_message_id`).
    pub fn mark_backfill_complete(
        &self,
        channel_id: &str,
        fetched_at: &str,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        conn.execute(
            "INSERT INTO channel_sync (channel_id, backfill, last_fetched_at)
             VALUES (?1, 'complete', ?2)
             ON CONFLICT(channel_id) DO UPDATE SET
                backfill = 'complete',
                last_fetched_at = COALESCE(excluded.last_fetched_at, channel_sync.last_fetched_at)",
            params![channel_id, fetched_at],
        )?;
        Ok(())
    }

    /// The per-channel sync cursor row, if any.
    pub fn channel_sync(&self, channel_id: &str) -> Result<Option<ChannelSyncRow>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let row = conn
            .query_row(
                "SELECT channel_id, last_synced_message_id, oldest_synced_message_id,
                        last_message_id, last_activity_at, last_fetched_at, backfill
                 FROM channel_sync WHERE channel_id = ?1",
                params![channel_id],
                row_to_channel_sync,
            )
            .optional()?;
        Ok(row)
    }

    /// All per-channel sync rows (for `get_sync_status`).
    pub fn channel_sync_all(&self, limit: u32) -> Result<Vec<ChannelSyncRow>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare(
            "SELECT channel_id, last_synced_message_id, oldest_synced_message_id,
                    last_message_id, last_activity_at, last_fetched_at, backfill
             FROM channel_sync
             ORDER BY COALESCE(last_fetched_at, '') DESC
             LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit], row_to_channel_sync)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Channels whose Discord-reported `last_message_id` is newer than what we
    /// have confirmed, or that we have never fetched. This is the cheap
    /// "what changed?" probe: it costs no message fetches.
    pub fn changed_channels(
        &self,
        limit: u32,
    ) -> Result<Vec<(String, Option<String>)>, StoreError> {
        self.changed_channels_page(None, None, limit)
    }

    /// Changed channels after a descending activity/ascending channel-ID cursor.
    pub fn changed_channels_page(
        &self,
        guild_id: Option<&str>,
        cursor: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<(String, Option<String>)>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare(
            "SELECT c.id, c.last_message_id
             FROM channels c
             LEFT JOIN channel_sync s ON s.channel_id = c.id
             WHERE c.last_message_id IS NOT NULL
               AND (?1 IS NULL OR c.guild_id = ?1)
               AND (?2 IS NULL OR CAST(c.last_message_id AS INTEGER) < CAST(?2 AS INTEGER)
                    OR (CAST(c.last_message_id AS INTEGER) = CAST(?2 AS INTEGER) AND c.id > ?3))
               AND (s.last_synced_message_id IS NULL
                    OR CAST(c.last_message_id AS INTEGER) > CAST(s.last_synced_message_id AS INTEGER))
             ORDER BY CAST(c.last_message_id AS INTEGER) DESC, c.id
             LIMIT ?4",
        )?;
        let rows = stmt
            .query_map(
                params![guild_id, cursor.map(|c| c.0), cursor.map(|c| c.1), limit],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Record a deletion observed during differential sync.
    pub fn record_deletion(
        &self,
        message_id: &str,
        channel_id: &str,
        observed_at: &str,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO deleted_messages (id, channel_id, observed_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET observed_at = excluded.observed_at",
            params![message_id, channel_id, observed_at],
        )?;
        tx.execute("DELETE FROM messages WHERE id = ?1", params![message_id])?;
        tx.execute(
            "DELETE FROM messages_fts WHERE message_id = ?1",
            params![message_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Deletion tombstones for a channel with `id > after_id`, oldest first.
    pub fn deletions_after(
        &self,
        channel_id: &str,
        after_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<(String, String)>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare(
            "SELECT id, observed_at FROM deleted_messages
             WHERE channel_id = ?1
               AND (?2 IS NULL OR CAST(id AS INTEGER) > CAST(?2 AS INTEGER))
             ORDER BY CAST(id AS INTEGER) ASC
             LIMIT ?3",
        )?;
        let rows = stmt
            .query_map(params![channel_id, after_id, limit], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Remove messages of a channel that fall inside `range` (used when a
    /// differential fetch turns up a gap: the range is re-fetched fresh).
    pub fn prune_messages_in_range(
        &self,
        channel_id: &str,
        from_id: &str,
        to_id: &str,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM messages_fts WHERE message_id IN (
                SELECT id FROM messages
                WHERE channel_id = ?1
                  AND CAST(id AS INTEGER) >= CAST(?2 AS INTEGER)
                  AND CAST(id AS INTEGER) <= CAST(?3 AS INTEGER))",
            params![channel_id, from_id, to_id],
        )?;
        tx.execute(
            "DELETE FROM messages
             WHERE channel_id = ?1
               AND CAST(id AS INTEGER) >= CAST(?2 AS INTEGER)
               AND CAST(id AS INTEGER) <= CAST(?3 AS INTEGER)",
            params![channel_id, from_id, to_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /* --------------------------- search cursors --------------------------- */

    /// Persist a named search cursor and return its handle.
    pub fn create_search_cursor(&self, query: &str, offset: u32) -> Result<i64, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        conn.execute(
            "INSERT INTO search_cursors (query, offset_value, created_at) VALUES (?1, ?2, ?3)",
            params![query, offset, now_iso()],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Look up a search cursor's offset, if it exists.
    pub fn search_cursor(&self, id: i64) -> Result<Option<u32>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let offset: Option<i64> = conn
            .query_row(
                "SELECT offset_value FROM search_cursors WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(offset.map(|v| v.max(0) as u32))
    }
}

pub(crate) fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRow> {
    Ok(MessageRow {
        id: row.get(0)?,
        channel_id: row.get(1)?,
        guild_id: row.get(2)?,
        author_id: row.get(3)?,
        timestamp: row.get(4)?,
        edited_timestamp: row.get(5)?,
        content: row.get(6)?,
        author_name: {
            let global: Option<String> = row.get(8)?;
            let username: Option<String> = row.get(7)?;
            global.or(username)
        },
        message_json: row.get(9)?,
        fetched_at: row.get(10)?,
    })
}

/// Merge overlapping or adjacent coverage ranges for one channel.
fn merge_coverage(conn: &rusqlite::Connection, channel_id: &str) -> Result<(), StoreError> {
    let mut stmt = conn.prepare(
        "SELECT from_id, to_id FROM coverage
         WHERE channel_id = ?1 ORDER BY CAST(from_id AS INTEGER)",
    )?;
    let rows: Vec<(String, String)> = stmt
        .query_map(params![channel_id], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    drop(stmt);

    if rows.len() <= 1 {
        return Ok(());
    }

    let mut merged: Vec<(String, String)> = Vec::new();
    for (from, to) in rows {
        match merged.last_mut() {
            Some(last) if snowflake_num(&from) <= snowflake_num(&last.1).saturating_add(1) => {
                if snowflake_num(&to) > snowflake_num(&last.1) {
                    last.1 = to;
                }
            }
            _ => merged.push((from, to)),
        }
    }

    conn.execute(
        "DELETE FROM coverage WHERE channel_id = ?1",
        params![channel_id],
    )?;
    for (from, to) in merged {
        conn.execute(
            "INSERT INTO coverage (channel_id, from_id, to_id) VALUES (?1, ?2, ?3)",
            params![channel_id, from, to],
        )?;
    }
    Ok(())
}

/// Numeric snowflake value, saturating at 0 for malformed ids.
fn snowflake_num(id: &str) -> u128 {
    id.parse::<u128>().unwrap_or(0)
}

fn max_id(a: Option<&str>, b: &str) -> Option<String> {
    match a {
        Some(a) if snowflake_num(a) >= snowflake_num(b) => Some(a.to_string()),
        _ => Some(b.to_string()),
    }
}

fn min_id(a: Option<&str>, b: &str) -> Option<String> {
    match a {
        Some(a) if snowflake_num(a) <= snowflake_num(b) => Some(a.to_string()),
        _ => Some(b.to_string()),
    }
}

fn row_to_channel_sync(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChannelSyncRow> {
    Ok(ChannelSyncRow {
        channel_id: row.get(0)?,
        last_synced_message_id: row.get(1)?,
        oldest_synced_message_id: row.get(2)?,
        last_message_id: row.get(3)?,
        last_activity_at: row.get(4)?,
        last_fetched_at: row.get(5)?,
        backfill: row.get(6)?,
    })
}

/// Wall-clock ISO-8601 (UTC, second precision) for bookkeeping timestamps.
pub fn now_iso() -> String {
    // No external clock dependency in this crate; use system time directly.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    // Format as `YYYY-MM-DDTHH:MM:SSZ` via a tiny civil-from-days conversion.
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant's `civil_from_days` (proleptic Gregorian).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use discord_api::types::MessageReference;

    fn legacy_database(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE messages (id TEXT PRIMARY KEY, channel_id TEXT NOT NULL,
             guild_id TEXT, author_id TEXT, timestamp TEXT NOT NULL,
             edited_timestamp TEXT, content TEXT NOT NULL);
             CREATE TABLE channels (id TEXT PRIMARY KEY, guild_id TEXT, name TEXT,
             kind INTEGER NOT NULL, parent_id TEXT, topic TEXT);
             CREATE TABLE users (id TEXT PRIMARY KEY, username TEXT, global_name TEXT);
             CREATE VIRTUAL TABLE messages_fts USING fts5(message_id UNINDEXED, content, tokenize='trigram');
             INSERT INTO messages VALUES ('10','200','100','1','2026-10-07T08:30:00Z',NULL,'legacy content');
             INSERT INTO messages_fts VALUES ('10','legacy content');
             INSERT INTO channels VALUES ('200','100','general',0,NULL,NULL);",
        ).unwrap();
    }

    #[test]
    fn legacy_upgrade_preserves_rows_backup_and_unknown_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite3");
        legacy_database(&path);
        let store = Store::open(&path).unwrap();
        let row = store.get_message("10").unwrap().unwrap();
        assert_eq!(row.content, "legacy content");
        assert_eq!(row.message_json, None);
        assert_eq!(row.fetched_at, None);
        assert_eq!(
            serde_json::to_value(row.to_view()).unwrap()["metadata_state"],
            "requires_refetch"
        );
        assert!(store.coverage("200").unwrap().is_empty());
        assert!(store.channel_sync_all(100).unwrap().is_empty());
        assert!(store.changed_channels(10).unwrap().is_empty());
        assert_eq!(store.search(&SearchQuery::new("legacy")).unwrap().len(), 1);
        store
            .record_coverage("200", "20", "30", "2026-10-07T09:00:00Z", false)
            .unwrap();
        let cursor = store.create_search_cursor("legacy", 3).unwrap();
        drop(store);
        for _ in 0..2 {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.message_count().unwrap(), 1);
            assert_eq!(store.search_cursor(cursor).unwrap(), Some(3));
            assert_eq!(
                store
                    .channel_sync("200")
                    .unwrap()
                    .unwrap()
                    .last_synced_message_id
                    .as_deref(),
                Some("30")
            );
        }
        let backups: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().contains("backup"))
            .collect();
        assert_eq!(backups.len(), 1);
        let backup = Connection::open(&backups[0]).unwrap();
        assert!(backup.prepare("SELECT message_json FROM messages").is_err());
        assert_eq!(
            backup
                .query_row("SELECT COUNT(*) FROM messages", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        let conn = Connection::open(&path).unwrap();
        assert_eq!(
            conn.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM schema_migrations", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn incompatible_schema_fails_without_partial_migration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite3");
        legacy_database(&path);
        Connection::open(&path)
            .unwrap()
            .execute_batch("ALTER TABLE messages DROP COLUMN content;")
            .unwrap();
        assert!(Store::open(&path).is_err());
        let conn = Connection::open(&path).unwrap();
        assert!(conn.prepare("SELECT message_json FROM messages").is_err());
        assert_eq!(
            conn.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn future_schema_and_versioned_schema_drift_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite3");
        let store = Store::open(&path).unwrap();
        drop(store);
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "user_version", 999).unwrap();
        assert!(Store::open(&path).is_err());
        conn.pragma_update(None, "user_version", 2).unwrap();
        conn.execute_batch("ALTER TABLE messages DROP COLUMN fetched_at;")
            .unwrap();
        assert!(Store::open(&path).is_err());
    }

    #[test]
    fn migration_backup_includes_uncheckpointed_wal_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite3");
        legacy_database(&path);
        let writer = Connection::open(&path).unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
            INSERT INTO messages VALUES ('11','200','100','1','2026-10-07T08:31:00Z',NULL,'wal content');").unwrap();
        let store = Store::open(&path).unwrap();
        assert_eq!(store.message_count().unwrap(), 2);
        let backup = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.to_string_lossy().contains("backup"))
            .unwrap();
        let backup = Connection::open(backup).unwrap();
        assert_eq!(
            backup
                .query_row("SELECT COUNT(*) FROM messages", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn deletion_failure_preserves_message_and_tombstone_atomically() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&message("10", "200", "1", "original content"))
            .unwrap();
        store.conn.lock().unwrap().execute_batch("CREATE TRIGGER reject_delete BEFORE DELETE ON messages BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        assert!(store
            .record_deletion("10", "200", "2026-10-07T09:00:00Z")
            .is_err());
        assert!(store.deletions_after("200", None, 100).unwrap().is_empty());
        assert!(store.get_message("10").unwrap().is_some());
        assert_eq!(
            store.search(&SearchQuery::new("original")).unwrap().len(),
            1
        );
    }

    #[test]
    fn edits_and_deletions_survive_file_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite3");
        let store = Store::open(&path).unwrap();
        store
            .insert_message(&message("10", "200", "1", "original content"))
            .unwrap();
        store
            .insert_message(&message("10", "200", "1", "edited content"))
            .unwrap();
        assert!(store
            .search(&SearchQuery::new("original"))
            .unwrap()
            .is_empty());
        assert_eq!(store.search(&SearchQuery::new("edited")).unwrap().len(), 1);
        store
            .record_deletion("10", "200", "2026-10-07T09:00:00Z")
            .unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.message_count().unwrap(), 0);
        assert_eq!(store.deletions_after("200", None, 100).unwrap().len(), 1);
        assert!(store
            .search(&SearchQuery::new("edited"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn malformed_cached_metadata_requires_refetch() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&message("10", "200", "1", "retained content"))
            .unwrap();
        assert_eq!(store.messages_requiring_refetch().unwrap(), 0);
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE messages SET message_json='{broken' WHERE id='10'",
                [],
            )
            .unwrap();
        let row = store.get_message("10").unwrap().unwrap();
        assert!(row.known_view().is_none());
        assert_eq!(row.to_view().metadata_state, "requires_refetch");
        assert_eq!(store.messages_requiring_refetch().unwrap(), 1);
        assert_eq!(row.content, "retained content");
    }

    fn message(id: &str, channel_id: &str, author: &str, content: &str) -> Message {
        Message {
            id: id.into(),
            channel_id: channel_id.into(),
            guild_id: Some("100".into()),
            author: Some(User {
                id: author.into(),
                username: Some(format!("user{author}")),
                ..User::default()
            }),
            content: content.into(),
            timestamp: "2026-10-05T00:00:00.000000+00:00".into(),
            edited_timestamp: None,
            attachments: vec![],
            embeds: vec![],
            message_reference: Some(MessageReference {
                message_id: Some("999".into()),
                channel_id: None,
                guild_id: None,
                fail_if_not_exists: None,
            }),
            ..Message::default()
        }
    }

    #[test]
    fn insert_and_read_back() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&message("10", "200", "1", "hello world"))
            .unwrap();

        let row = store.get_message("10").unwrap().unwrap();
        assert_eq!(row.content, "hello world");
        assert_eq!(row.author_name.as_deref(), Some("user1"));
        assert_eq!(store.message_count().unwrap(), 1);
    }

    #[test]
    fn reinserting_updates_content_and_fts() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&message("10", "200", "1", "original text"))
            .unwrap();

        let mut edited = message("10", "200", "1", "rewritten text");
        edited.edited_timestamp = Some("2026-10-05T01:00:00.000000+00:00".into());
        store.insert_message(&edited).unwrap();

        let query = SearchQuery::new("rewritten").limit(10);
        let hits = store.search(&query).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].message.content, "rewritten text");

        let stale = store
            .search(&SearchQuery::new("original").limit(10))
            .unwrap();
        assert!(stale.is_empty(), "old FTS row must be replaced");
    }

    #[test]
    fn pagination_uses_before_message_id() {
        let store = Store::open_in_memory().unwrap();
        for id in ["1", "2", "3", "4", "5", "6", "7", "8", "9"] {
            store
                .insert_message(&message(id, "200", "1", &format!("msg {id}")))
                .unwrap();
        }

        let page1 = store.channel_messages("200", 4, None, None).unwrap();
        let ids: Vec<_> = page1.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["9", "8", "7", "6"]);

        let before = page1.last().unwrap().id.clone();
        let page2 = store
            .channel_messages("200", 4, Some(&before), None)
            .unwrap();
        let ids2: Vec<_> = page2.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids2, ["5", "4", "3", "2"]);
    }

    #[test]
    fn coverage_merges_adjacent_ranges_and_keeps_gaps() {
        let store = Store::open_in_memory().unwrap();
        store
            .record_coverage("200", "1", "5", "2026-10-05T00:00:00Z", false)
            .unwrap();
        // Adjacent (5..=9 touches 1..=5): merges into one range.
        store
            .record_coverage("200", "5", "9", "2026-10-05T00:00:01Z", false)
            .unwrap();
        let ranges = store.coverage("200").unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].from_id, "1");
        assert_eq!(ranges[0].to_id, "9");

        // A gap (20..=30) becomes a separate range; the envelope shows gaps.
        store
            .record_coverage("200", "20", "30", "2026-10-05T00:00:02Z", true)
            .unwrap();
        let (from, to, has_gaps) = store.coverage_envelope("200").unwrap().unwrap();
        assert_eq!(from, "1");
        assert_eq!(to, "30");
        assert!(has_gaps, "two disjoint ranges must be reported as a gap");
    }

    #[test]
    fn channel_sync_cursor_advances_monotonically() {
        let store = Store::open_in_memory().unwrap();
        store
            .record_coverage("200", "10", "20", "2026-10-05T00:00:00Z", false)
            .unwrap();
        let row = store.channel_sync("200").unwrap().unwrap();
        assert_eq!(row.last_synced_message_id.as_deref(), Some("20"));
        assert_eq!(row.oldest_synced_message_id.as_deref(), Some("10"));
        assert_eq!(row.backfill, "partial");

        // A later page advances the cursor; an older page must not regress it.
        store
            .record_coverage("200", "21", "30", "2026-10-05T00:00:01Z", false)
            .unwrap();
        store
            .record_coverage("200", "1", "9", "2026-10-05T00:00:02Z", true)
            .unwrap();
        let row = store.channel_sync("200").unwrap().unwrap();
        assert_eq!(row.last_synced_message_id.as_deref(), Some("30"));
        assert_eq!(row.oldest_synced_message_id.as_deref(), Some("1"));
        assert_eq!(row.backfill, "complete");
    }

    #[test]
    fn deletions_are_tombstoned_and_persisted() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&message("10", "200", "1", "hello"))
            .unwrap();
        store
            .record_deletion("10", "200", "2026-10-05T00:00:00Z")
            .unwrap();
        assert!(store.get_message("10").unwrap().is_none());
        let deletions = store.deletions_after("200", None, 10).unwrap();
        assert_eq!(deletions.len(), 1);
        assert_eq!(deletions[0].0, "10");
    }

    #[test]
    fn message_json_round_trips_the_full_view() {
        let store = Store::open_in_memory().unwrap();
        let mut m = message("10", "200", "1", "hello");
        m.mention_roles = vec!["77".into()];
        m.mention_everyone = Some(true);
        store.insert_message(&m).unwrap();

        let row = store.get_message("10").unwrap().unwrap();
        let view = row.to_view();
        assert_eq!(view.content, "hello");
        assert_eq!(view.content_kind, "text");
        assert_eq!(view.mention_roles, vec!["77".to_string()]);
        assert_eq!(view.mention_everyone, Some(true));
    }

    #[test]
    fn changed_channels_detects_new_activity_without_fetching() {
        let store = Store::open_in_memory().unwrap();
        let channel = discord_api::types::Channel {
            id: "200".into(),
            kind: 0,
            last_message_id: Some("500".into()),
            ..Default::default()
        };
        store.upsert_channel(&channel).unwrap();

        // No sync yet -> channel shows up as changed.
        let changed = store.changed_channels(10).unwrap();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, "200");

        // After syncing past 500, it is quiet again.
        store
            .record_coverage("200", "1", "500", "2026-10-05T00:00:00Z", true)
            .unwrap();
        let changed = store.changed_channels(10).unwrap();
        assert!(changed.is_empty());
    }

    #[test]
    fn search_cursors_resume_offsets() {
        let store = Store::open_in_memory().unwrap();
        let id = store.create_search_cursor("q", 50).unwrap();
        assert_eq!(store.search_cursor(id).unwrap(), Some(50));
        assert!(store.search_cursor(9999).unwrap().is_none());
    }

    // ---- acceptance: differential sync, restart resumption, gap honesty ----

    #[test]
    fn acceptance_sync_resumes_after_restart() {
        // Simulate a first run: fetch 1..=10 of channel 200.
        let store = Store::open_in_memory().unwrap();
        for id in 1..=10 {
            store
                .insert_message(&message(&id.to_string(), "200", "1", "hello"))
                .unwrap();
        }
        store
            .record_coverage("200", "1", "10", "2026-10-05T00:00:00Z", true)
            .unwrap();

        // "Restart": reopen the same database and resume from the cursor.
        let resumed = store.channel_sync("200").unwrap().unwrap();
        assert_eq!(resumed.last_synced_message_id.as_deref(), Some("10"));
        assert_eq!(resumed.backfill, "complete");

        // Differential fetch 11..=15 and confirm the cursor advances, so the
        // next restart continues from 15, not from scratch.
        for id in 11..=15 {
            store
                .insert_message(&message(&id.to_string(), "200", "1", "hello"))
                .unwrap();
        }
        store
            .record_coverage("200", "11", "15", "2026-10-05T00:00:01Z", true)
            .unwrap();
        let resumed = store.channel_sync("200").unwrap().unwrap();
        assert_eq!(resumed.last_synced_message_id.as_deref(), Some("15"));
    }

    #[test]
    fn acceptance_edit_delete_and_unfetchable_are_not_silence() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&message("1", "200", "1", "original"))
            .unwrap();

        // An edit is visible: the row's content changes and edited_timestamp
        // is set, distinct from "no new messages".
        let mut edited = message("1", "200", "1", "edited text");
        edited.edited_timestamp = Some("2026-10-05T01:00:00Z".into());
        store.insert_message(&edited).unwrap();
        let row = store.get_message("1").unwrap().unwrap();
        assert_eq!(row.content, "edited text");
        assert!(row.edited_timestamp.is_some(), "edit must be observable");

        // A delete leaves a tombstone, distinct from "no new messages".
        store
            .record_deletion("1", "200", "2026-10-05T02:00:00Z")
            .unwrap();
        let deletions = store.deletions_after("200", None, 10).unwrap();
        assert_eq!(deletions.len(), 1, "deletion must be recorded, not silent");
        assert!(store.get_message("1").unwrap().is_none());
    }

    #[test]
    fn acceptance_coverage_envelope_is_numeric_and_reports_gaps() {
        let store = Store::open_in_memory().unwrap();
        assert!(
            store.coverage_envelope("200").unwrap().is_none(),
            "nothing fetched means no coverage claim at all"
        );

        store
            .record_coverage("200", "1", "100", "2026-10-05T00:00:00Z", true)
            .unwrap();
        let (from, to, gaps) = store.coverage_envelope("200").unwrap().unwrap();
        assert_eq!((from.as_str(), to.as_str(), gaps), ("1", "100", false));

        // A gapped later fetch is reported honestly as a gap, not merged away.
        store
            .record_coverage("200", "200", "300", "2026-10-05T00:00:01Z", true)
            .unwrap();
        let (from, to, gaps) = store.coverage_envelope("200").unwrap().unwrap();
        assert_eq!((from.as_str(), to.as_str(), gaps), ("1", "300", true));
    }

    #[test]
    fn acceptance_unfetched_channel_is_distinguishable_from_empty() {
        let store = Store::open_in_memory().unwrap();
        let channel = discord_api::types::Channel {
            id: "200".into(),
            kind: 0,
            last_message_id: Some("999".into()),
            ..Default::default()
        };
        store.upsert_channel(&channel).unwrap();

        // The channel is known to have messages we never fetched: this must
        // show as "changed" (i.e. unchecked), not as "nothing new".
        let changed = store.changed_channels(10).unwrap();
        assert_eq!(changed.len(), 1);
        assert!(
            store.coverage_envelope("200").unwrap().is_none(),
            "no coverage must be reported, not an empty-but-complete claim"
        );
    }
}
