use std::{
    fs::OpenOptions,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use crate::sqlite::StoreError;

const VERSION: i64 = 3;
const V2_ADDED_COLUMNS: [(&str, &str); 3] = [
    ("messages", "message_json"),
    ("messages", "fetched_at"),
    ("channels", "last_message_id"),
];
const HISTORY: &str = "CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL, backup_path TEXT)";

pub(crate) fn migrate(conn: &mut Connection, schema: &str) -> Result<(), StoreError> {
    let canonical = Connection::open_in_memory()?;
    canonical.execute_batch(schema)?;
    canonical.execute_batch(HISTORY)?;
    let database: String = conn.query_row(
        "SELECT file FROM pragma_database_list WHERE name='main'",
        [],
        |row| row.get(0),
    )?;
    let version = schema_version(conn)?;
    if !(0..=VERSION).contains(&version) {
        return Err(StoreError::Schema(format!(
            "unsupported user_version {version}; expected at most {VERSION}"
        )));
    }
    if version == VERSION {
        validate(conn, &canonical)?;
        let recorded: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [VERSION],
            |row| row.get(0),
        )?;
        if !recorded {
            return Err(StoreError::Schema(
                "current schema has no migration history entry".into(),
            ));
        }
        log_schema(conn, &database)?;
        return Ok(());
    }
    if version == 2 {
        validate_versioned(conn, &canonical, 2)?;
        let recorded: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=2)",
            [],
            |row| row.get(0),
        )?;
        if !recorded {
            return Err(StoreError::Schema(
                "version 2 has no migration history entry".into(),
            ));
        }
    }
    let has_tables: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%')", [], |row| row.get(0))?;
    if has_tables {
        validate_legacy(conn, &canonical)?;
    }
    let backup = if has_tables && !database.is_empty() {
        Some(backup(conn, &database)?)
    } else {
        None
    };
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = schema_version(&tx)?;
    if current != version {
        return Err(StoreError::Schema(format!(
            "user_version changed during migration: {version} to {current}"
        )));
    }
    let had_fts = exists(&tx, "messages_fts")?;
    if has_tables {
        for (table, column) in V2_ADDED_COLUMNS {
            if !columns(&tx, table)?.iter().any(|c| c.name == column) {
                tx.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT"))?;
            }
        }
    }
    tx.execute_batch(schema)?;
    tx.execute_batch(HISTORY)?;
    if !had_fts {
        tx.execute(
            "INSERT INTO messages_fts(message_id, content) SELECT id, content FROM messages",
            [],
        )?;
    }
    validate(&tx, &canonical)?;
    tx.execute("INSERT INTO schema_migrations(version, applied_at, backup_path) VALUES (?1, strftime('%Y-%m-%dT%H:%M:%fZ','now'), ?2)", rusqlite::params![VERSION, backup.as_ref().map(|p| p.to_string_lossy().into_owned())])?;
    tx.pragma_update(None, "user_version", VERSION)?;
    tx.commit()?;
    log_schema(conn, &database)
}

fn schema_version(conn: &Connection) -> Result<i64, StoreError> {
    Ok(conn.pragma_query_value(None, "user_version", |row| row.get(0))?)
}

fn exists(conn: &Connection, name: &str) -> Result<bool, StoreError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?1 AND type='table')",
        [name],
        |row| row.get(0),
    )?)
}

#[derive(Debug, PartialEq, Eq)]
struct Column {
    name: String,
    kind: String,
    not_null: bool,
    default: Option<String>,
    primary_key: i64,
}

fn columns(conn: &Connection, table: &str) -> Result<Vec<Column>, StoreError> {
    let mut stmt =
        conn.prepare("SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?1)")?;
    let rows = stmt.query_map([table], |row| {
        Ok(Column {
            name: row.get(0)?,
            kind: row.get::<_, String>(1)?.to_ascii_uppercase(),
            not_null: row.get(2)?,
            default: row.get(3)?,
            primary_key: row.get(4)?,
        })
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn objects(conn: &Connection) -> Result<Vec<(String, String, String)>, StoreError> {
    let mut stmt = conn.prepare("SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'")?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn validate_legacy(conn: &Connection, canonical: &Connection) -> Result<(), StoreError> {
    for table in ["messages", "channels"] {
        if !exists(conn, table)? {
            return Err(StoreError::Schema(format!(
                "legacy cache is missing required table {table}"
            )));
        }
    }
    for (kind, table, _) in objects(canonical)? {
        if kind != "table" || !exists(conn, &table)? {
            continue;
        }
        let actual = columns(conn, &table)?;
        for expected in columns(canonical, &table)? {
            let optional = V2_ADDED_COLUMNS.contains(&(table.as_str(), expected.name.as_str()));
            match actual.iter().find(|column| column.name == expected.name) {
                Some(column) if column == &expected => {}
                None if optional => {}
                _ => {
                    return Err(StoreError::Schema(format!(
                        "incompatible column {table}.{}",
                        expected.name
                    )))
                }
            }
        }
    }
    Ok(())
}

fn normalize(sql: &str) -> String {
    sql.to_ascii_lowercase()
        .split_whitespace()
        .collect::<String>()
        .replace("ifnotexists", "")
}

fn validate(conn: &Connection, canonical: &Connection) -> Result<(), StoreError> {
    validate_versioned(conn, canonical, VERSION)
}

fn validate_versioned(
    conn: &Connection,
    canonical: &Connection,
    version: i64,
) -> Result<(), StoreError> {
    const V3_OBJECTS: &[&str] = &[
        "account_inventory",
        "account_inventory_user",
        "account_targets",
        "account_targets_fair",
        "account_sync_jobs",
        "inbox_snapshots",
        "account_observations",
        "inbox_workflow",
    ];
    for (kind, name, sql) in objects(canonical)? {
        if version == 2 && V3_OBJECTS.contains(&name.as_str()) {
            continue;
        }
        if kind == "table" {
            if !exists(conn, &name)? {
                return Err(StoreError::Schema(format!(
                    "missing or incompatible table {name}"
                )));
            }
            let actual = columns(conn, &name)?;
            for expected in columns(canonical, &name)? {
                if !actual.contains(&expected) {
                    return Err(StoreError::Schema(format!(
                        "missing or incompatible column {name}.{}",
                        expected.name
                    )));
                }
            }
        }
        if kind == "index" || name == "messages_fts" {
            let actual: Option<String> = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE name=?1 AND type=?2",
                    rusqlite::params![name, kind],
                    |row| row.get(0),
                )
                .optional()?;
            if actual.as_deref().map(normalize) != Some(normalize(&sql)) {
                return Err(StoreError::Schema(format!(
                    "missing or incompatible {kind} {name}"
                )));
            }
        }
    }
    Ok(())
}

fn backup(conn: &Connection, database: &str) -> Result<PathBuf, StoreError> {
    let source = std::fs::canonicalize(database)?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| StoreError::Schema(error.to_string()))?
        .as_nanos();
    let mut name = source.as_os_str().to_os_string();
    name.push(format!(".backup-v3-{}-{stamp}.sqlite3", std::process::id()));
    let path = PathBuf::from(name);
    let mut partial_name = path.as_os_str().to_os_string();
    partial_name.push(".partial");
    let partial = PathBuf::from(partial_name);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&partial)?;
    conn.execute("VACUUM main INTO ?1", [partial.to_string_lossy().as_ref()])?;
    file.sync_all()?;
    let snapshot =
        Connection::open_with_flags(&partial, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let integrity: String = snapshot.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(StoreError::Schema(format!(
            "backup integrity check failed: {integrity}"
        )));
    }
    drop(snapshot);
    std::fs::rename(&partial, &path)?;
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(path)
}

fn log_schema(conn: &Connection, database: &str) -> Result<(), StoreError> {
    let path = if database.is_empty() {
        ":memory:".to_string()
    } else {
        std::fs::canonicalize(database)?.display().to_string()
    };
    let mut stmt = conn.prepare(
        "SELECT version, applied_at, backup_path FROM schema_migrations ORDER BY version",
    )?;
    let history = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    tracing::info!(database = %path, schema_version = VERSION, migrations = ?history, "cache schema verified");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS messages(id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, content TEXT NOT NULL, message_json TEXT, fetched_at TEXT); CREATE TABLE IF NOT EXISTS channels(id TEXT PRIMARY KEY, last_message_id TEXT); CREATE INDEX IF NOT EXISTS messages_channel_idx ON messages(channel_id, CAST(id AS INTEGER)); CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(message_id UNINDEXED, content, tokenize='trigram');";

    #[test]
    fn legacy_rows_keep_unknown_metadata_and_migration_is_repeatable() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE messages(id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, content TEXT NOT NULL); CREATE TABLE channels(id TEXT PRIMARY KEY); INSERT INTO messages VALUES('1','2','legacy content');").unwrap();
        migrate(&mut conn, SCHEMA).unwrap();
        migrate(&mut conn, SCHEMA).unwrap();
        let missing: bool = conn
            .query_row(
                "SELECT message_json IS NULL AND fetched_at IS NULL FROM messages",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(missing);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'legacy'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn malformed_existing_columns_roll_back_all_changes() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE messages(id TEXT PRIMARY KEY, channel_id INTEGER NOT NULL, content TEXT NOT NULL); CREATE TABLE channels(id TEXT PRIMARY KEY);").unwrap();
        assert!(migrate(&mut conn, SCHEMA).is_err());
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name='message_json'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn versioned_schema_is_validated_instead_of_silently_repaired() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA user_version=2; CREATE TABLE messages(id TEXT);")
            .unwrap();
        assert!(migrate(&mut conn, SCHEMA).is_err());
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='channels'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn wrong_index_definition_rolls_back_added_columns() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE messages(id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, content TEXT NOT NULL); CREATE TABLE channels(id TEXT PRIMARY KEY); CREATE INDEX messages_channel_idx ON messages(content);").unwrap();
        assert!(matches!(
            migrate(&mut conn, SCHEMA),
            Err(StoreError::Schema(_))
        ));
        assert!(!columns(&conn, "messages")
            .unwrap()
            .iter()
            .any(|c| c.name == "message_json"));
        assert_eq!(schema_version(&conn).unwrap(), 0);
    }

    #[test]
    fn version_one_and_unversioned_v2_are_adopted_without_resetting_data() {
        for version in [0, 1] {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(SCHEMA).unwrap();
            conn.execute(
                "INSERT INTO messages VALUES ('1','2','cached',NULL,NULL)",
                [],
            )
            .unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            migrate(&mut conn, SCHEMA).unwrap();
            assert_eq!(schema_version(&conn).unwrap(), VERSION);
            assert_eq!(
                conn.query_row("SELECT content FROM messages", [], |row| row
                    .get::<_, String>(0))
                    .unwrap(),
                "cached"
            );
        }
    }

    #[test]
    fn unrelated_database_is_rejected_without_changes() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE unrelated(id INTEGER)")
            .unwrap();
        assert!(matches!(
            migrate(&mut conn, SCHEMA),
            Err(StoreError::Schema(_))
        ));
        assert!(!exists(&conn, "messages").unwrap());
    }

    #[test]
    fn version_two_requires_recorded_migration_history() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrate(&mut conn, SCHEMA).unwrap();
        conn.execute("DELETE FROM schema_migrations", []).unwrap();
        assert!(matches!(
            migrate(&mut conn, SCHEMA),
            Err(StoreError::Schema(_))
        ));
    }

    #[test]
    fn adopting_unversioned_v2_preserves_existing_sync_and_cursor_rows() {
        let schema = format!("{SCHEMA} CREATE TABLE IF NOT EXISTS coverage(channel_id TEXT, from_id TEXT, to_id TEXT); CREATE TABLE IF NOT EXISTS channel_sync(channel_id TEXT PRIMARY KEY, last_synced_message_id TEXT); CREATE TABLE IF NOT EXISTS search_cursors(id INTEGER PRIMARY KEY, offset_value INTEGER NOT NULL);");
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&schema).unwrap();
        conn.execute_batch("INSERT INTO coverage VALUES ('2','10','20'); INSERT INTO channel_sync VALUES ('2','20'); INSERT INTO search_cursors VALUES (1,5);").unwrap();
        for _ in 0..2 {
            migrate(&mut conn, &schema).unwrap();
        }
        assert_eq!(
            conn.query_row("SELECT to_id FROM coverage", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "20"
        );
        assert_eq!(
            conn.query_row(
                "SELECT last_synced_message_id FROM channel_sync",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "20"
        );
        assert_eq!(
            conn.query_row("SELECT offset_value FROM search_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            5
        );
    }
}
