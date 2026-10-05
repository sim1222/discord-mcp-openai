//! SQLite cache for Discord data.
//!
//! The cache is written lazily: only data that a client already asked for is
//! stored. Nothing is crawled in the background.

use std::{path::Path, sync::Mutex};

use rusqlite::{params, Connection, OptionalExtension};

use discord_api::types::{
    AttachmentView, AuthorView, Channel, ChannelView, EmbedView, Guild, GuildView, Message,
    MessageView, User,
};

use crate::search::{self, SearchQuery};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("search query error: {0}")]
    Search(String),
}

/// One cached message row, with author information joined in.
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
}

impl MessageRow {
    /// Same normalized shape as API-fetched messages.
    pub fn to_view(&self) -> MessageView {
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
            content: self.content.clone(),
            reply_to: None,
            attachments: Vec::<AttachmentView>::new(),
            embeds: Vec::<EmbedView>::new(),
        }
    }
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
    topic TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT,
    global_name TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    guild_id TEXT,
    author_id TEXT,
    timestamp TEXT NOT NULL,
    edited_timestamp TEXT,
    content TEXT NOT NULL
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
"#;

impl Store {
    /// Open (creating if needed) the cache database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let conn = Connection::open(path)?;
        Self::init(conn)
    }

    /// In-memory database, used by tests.
    pub fn open_in_memory() -> Result<Self, StoreError> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self, StoreError> {
        conn.execute_batch(SCHEMA)?;
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
            "INSERT INTO channels (id, guild_id, name, kind, parent_id, topic)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(id) DO UPDATE SET
                guild_id = COALESCE(excluded.guild_id, channels.guild_id),
                name = COALESCE(excluded.name, channels.name),
                kind = excluded.kind,
                parent_id = COALESCE(excluded.parent_id, channels.parent_id),
                topic = COALESCE(excluded.topic, channels.topic)",
            params![
                channel.id,
                channel.guild_id,
                channel.name,
                channel.kind,
                channel.parent_id,
                channel.topic,
            ],
        )?;
        Ok(())
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
        let conn = self.conn.lock().expect("store lock");
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO messages
                (id, channel_id, guild_id, author_id, timestamp, edited_timestamp, content)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
                channel_id = excluded.channel_id,
                guild_id = excluded.guild_id,
                author_id = excluded.author_id,
                timestamp = excluded.timestamp,
                edited_timestamp = excluded.edited_timestamp,
                content = excluded.content",
            params![
                message.id,
                message.channel_id,
                message.guild_id,
                message.author.as_ref().map(|a| a.id.clone()),
                message.timestamp,
                message.edited_timestamp,
                message.content,
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
                        m.edited_timestamp, m.content, u.username, u.global_name
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
                    m.edited_timestamp, m.content, u.username, u.global_name
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use discord_api::types::MessageReference;

    fn message(id: &str, channel_id: &str, author: &str, content: &str) -> Message {
        Message {
            id: id.into(),
            channel_id: channel_id.into(),
            guild_id: Some("100".into()),
            author: Some(User {
                id: author.into(),
                username: Some(format!("user{author}")),
                global_name: None,
                bot: None,
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
            }),
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
}
