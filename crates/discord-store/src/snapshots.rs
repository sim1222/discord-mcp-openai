//! Immutable inbox observations retained across page requests and restarts.

use rusqlite::{params, OptionalExtension};

use super::{Store, StoreError};

/// Persisted account-scoped inbox observation.
pub struct InboxSnapshot {
    pub account_user_id: String,
    pub query_json: String,
    pub created_at: String,
    pub response_json: String,
}

impl Store {
    /// Capture a complete inbox response without replacing earlier observations.
    pub fn save_inbox_snapshot(
        &self,
        account_user_id: &str,
        query_json: &str,
        response_json: &str,
    ) -> Result<String, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let snapshot_id: String =
            conn.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?;
        conn.execute(
            "INSERT INTO inbox_snapshots(snapshot_id,account_user_id,query_json,created_at,response_json) VALUES(?1,?2,?3,?4,?5)",
            params![snapshot_id, account_user_id, query_json, super::now_iso(), response_json],
        )?;
        Ok(snapshot_id)
    }

    /// Load an immutable observation, or `None` when its cursor is no longer available.
    pub fn load_inbox_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<InboxSnapshot>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        Ok(conn.query_row(
            "SELECT account_user_id,query_json,created_at,response_json FROM inbox_snapshots WHERE snapshot_id=?1",
            params![snapshot_id],
            |row| Ok(InboxSnapshot {account_user_id:row.get(0)?,query_json:row.get(1)?,created_at:row.get(2)?,response_json:row.get(3)?}),
        ).optional()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_payload_survives_restart_without_being_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.sqlite3");
        let id = {
            let store = Store::open(&path).unwrap();
            let id = store
                .save_inbox_snapshot("7", "{\"channel_id\":\"200\"}", "{\"mentions\":[1]}")
                .unwrap();
            let other = store
                .save_inbox_snapshot("7", "{\"channel_id\":\"200\"}", "{\"mentions\":[2]}")
                .unwrap();
            assert_ne!(id, other);
            id
        };
        let store = Store::open(&path).unwrap();
        let snapshot = store.load_inbox_snapshot(&id).unwrap().unwrap();
        assert_eq!(snapshot.account_user_id, "7");
        assert_eq!(snapshot.response_json, "{\"mentions\":[1]}");
        assert!(!snapshot.created_at.is_empty());
        assert!(store.load_inbox_snapshot("missing").unwrap().is_none());
    }
}
