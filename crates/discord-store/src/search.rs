//! FTS5 full-text search over the message cache.

use rusqlite::{params, Connection};

use crate::{
    sqlite::{row_to_message, SearchHit},
    StoreError,
};

/// Search request against the local cache.
#[derive(Debug, Clone, Default)]
pub struct SearchQuery {
    pub query: String,
    pub guild_id: Option<String>,
    pub channel_id: Option<String>,
    pub author_id: Option<String>,
    /// Inclusive lower bound on the message timestamp (ISO-8601 string compare).
    pub after: Option<String>,
    /// Inclusive upper bound on the message timestamp (ISO-8601 string compare).
    pub before: Option<String>,
    pub limit: u32,
}

impl SearchQuery {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            limit: 50,
            ..Default::default()
        }
    }

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit.clamp(1, 500);
        self
    }

    pub fn guild_id(mut self, guild_id: Option<String>) -> Self {
        self.guild_id = guild_id;
        self
    }

    pub fn channel_id(mut self, channel_id: Option<String>) -> Self {
        self.channel_id = channel_id;
        self
    }

    pub fn author_id(mut self, author_id: Option<String>) -> Self {
        self.author_id = author_id;
        self
    }

    pub fn after(mut self, after: Option<String>) -> Self {
        self.after = after;
        self
    }

    pub fn before(mut self, before: Option<String>) -> Self {
        self.before = before;
        self
    }
}

/// Escape a user query into a safe FTS5 MATCH expression.
///
/// Every whitespace-separated token is quoted, so FTS5 operators in user input
/// (`"`, `*`, `NEAR`, `AND`, `OR`, `(`, `)`, `-`, `:`) are treated as literal
/// text instead of query syntax. Tokens are combined with implicit AND.
pub fn escape_fts_query(raw: &str) -> String {
    raw.split_whitespace()
        .map(|token| {
            let escaped = token.replace('"', "\"\"");
            format!("\"{escaped}\"")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

const SELECT_COLUMNS: &str = "m.id, m.channel_id, m.guild_id, m.author_id, m.timestamp,
        m.edited_timestamp, m.content, u.username, u.global_name,
        m.message_json, m.fetched_at";

// column order must match `row_to_message`: 0..7 base, 7 username, 8 global_name,
// 9 message_json, 10 fetched_at.

fn row_to_hit(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchHit> {
    // `SELECT_COLUMNS` yields 11 columns (indices 0-10); the search queries
    // append the rank as the 12th column (index 11).
    // SQLite FTS5 `bm25()` is "lower is better" (usually negative); flip the
    // sign so callers see "higher is better".
    let bm25: f64 = row.get(11)?;
    Ok(SearchHit {
        message: row_to_message(row)?,
        relevance: -bm25,
    })
}

/// Search the cache.
///
/// Matching strategy:
/// - queries with fewer than 3 characters (or malformed FTS expressions) fall
///   back to a substring scan, because the FTS5 `trigram` tokenizer needs at
///   least 3 characters,
/// - longer queries use the FTS5 trigram index, which matches substrings and
///   works for CJK text (where whitespace tokenization does not apply),
/// - if FTS returns nothing, a substring scan runs as a recall safety net.
pub fn search(conn: &Connection, query: &SearchQuery) -> Result<Vec<SearchHit>, StoreError> {
    let limit = query.limit.clamp(1, 500) as i64;
    let trimmed = query.query.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let match_expr = escape_fts_query(trimmed);
    if match_expr.is_empty() || trimmed.chars().count() < 3 {
        return like_search(conn, query, limit);
    }

    let fts_hits = fts_search(conn, query, &match_expr, limit);
    match fts_hits {
        Ok(hits) if hits.is_empty() => like_search(conn, query, limit),
        Ok(hits) => Ok(hits),
        Err(StoreError::Sqlite(e)) => {
            tracing::debug!(error = %e, "fts query failed, falling back to LIKE");
            like_search(conn, query, limit)
        }
        Err(other) => Err(other),
    }
}

fn fts_search(
    conn: &Connection,
    query: &SearchQuery,
    match_expr: &str,
    limit: i64,
) -> Result<Vec<SearchHit>, StoreError> {
    let sql = format!(
        "SELECT {SELECT_COLUMNS}, bm25(messages_fts) AS rank
         FROM messages_fts
         JOIN messages m ON m.id = messages_fts.message_id
         LEFT JOIN users u ON u.id = m.author_id
         WHERE messages_fts MATCH ?1
           AND (?2 IS NULL OR m.guild_id = ?2)
           AND (?3 IS NULL OR m.channel_id = ?3)
           AND (?4 IS NULL OR m.author_id = ?4)
           AND (?5 IS NULL OR m.timestamp >= ?5)
           AND (?6 IS NULL OR m.timestamp <= ?6)
         ORDER BY rank
         LIMIT ?7"
    );

    let mut stmt = conn.prepare_cached(&sql)?;
    let hits = stmt
        .query_map(
            params![
                match_expr,
                query.guild_id,
                query.channel_id,
                query.author_id,
                query.after,
                query.before,
                limit
            ],
            row_to_hit,
        )
        .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())?;
    Ok(hits)
}

fn like_search(
    conn: &Connection,
    query: &SearchQuery,
    limit: i64,
) -> Result<Vec<SearchHit>, StoreError> {
    let sql = format!(
        "SELECT {SELECT_COLUMNS}, 0.0 AS rank
         FROM messages m
         LEFT JOIN users u ON u.id = m.author_id
         WHERE m.content LIKE '%' || ?1 || '%'
           AND (?2 IS NULL OR m.guild_id = ?2)
           AND (?3 IS NULL OR m.channel_id = ?3)
           AND (?4 IS NULL OR m.author_id = ?4)
           AND (?5 IS NULL OR m.timestamp >= ?5)
           AND (?6 IS NULL OR m.timestamp <= ?6)
         ORDER BY m.timestamp DESC
         LIMIT ?7"
    );
    let mut stmt = conn.prepare_cached(&sql)?;
    let hits = stmt
        .query_map(
            params![
                query.query,
                query.guild_id,
                query.channel_id,
                query.author_id,
                query.after,
                query.before,
                limit
            ],
            row_to_hit,
        )?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::Store;
    use discord_api::types::{Message, User};

    fn msg(id: &str, channel: &str, content: &str) -> Message {
        Message {
            id: id.into(),
            channel_id: channel.into(),
            guild_id: Some("100".into()),
            author: Some(User {
                id: "7".into(),
                username: Some("alice".into()),
                ..User::default()
            }),
            content: content.into(),
            timestamp: "2026-10-05T00:00:00.000000+00:00".into(),
            ..Message::default()
        }
    }

    #[test]
    fn fts_finds_keyword() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&msg("1", "200", "会議予定の打ち合わせは明日です"))
            .unwrap();
        store
            .insert_message(&msg("2", "200", "ランチはカレーにしましょう"))
            .unwrap();

        let hits = store
            .search(&SearchQuery::new("会議予定").limit(10))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].message.id, "1");
        assert!(hits[0].relevance > 0.0);
    }

    #[test]
    fn fts_matches_all_terms() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&msg("1", "200", "project launch planning"))
            .unwrap();
        store
            .insert_message(&msg("2", "200", "project retro notes"))
            .unwrap();

        let hits = store
            .search(&SearchQuery::new("project launch").limit(10))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].message.id, "1");
    }

    #[test]
    fn fts_operators_in_query_are_treated_as_text() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&msg("1", "200", "weird \"quoted\" text"))
            .unwrap();
        store
            .insert_message(&msg("2", "200", "other text"))
            .unwrap();

        // Without escaping this would be a syntax error or an OR query.
        for hostile in ["\" OR 1:1", "NEAR(a b)", "text*", "\"\"", "*"] {
            let result = store.search(&SearchQuery::new(hostile).limit(10));
            assert!(result.is_ok(), "query {hostile:?} must not error");
        }
    }

    #[test]
    fn filters_restrict_results() {
        let store = Store::open_in_memory().unwrap();
        store
            .insert_message(&msg("1", "200", "alpha in channel one"))
            .unwrap();
        let mut other = msg("2", "300", "alpha in channel two");
        other.guild_id = Some("999".into());
        store.insert_message(&other).unwrap();

        let hits = store
            .search(
                &SearchQuery::new("alpha")
                    .channel_id(Some("200".into()))
                    .limit(10),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].message.channel_id, "200");

        let by_guild = store
            .search(
                &SearchQuery::new("alpha")
                    .guild_id(Some("999".into()))
                    .limit(10),
            )
            .unwrap();
        assert_eq!(by_guild.len(), 1);
        assert_eq!(by_guild[0].message.id, "2");
    }
}
