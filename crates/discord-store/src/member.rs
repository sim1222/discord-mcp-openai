//! Last successfully observed role memberships, separate from current access.

use super::{Store, StoreError};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;

/// Historical evidence; it does not establish current membership.
#[derive(Debug, Serialize)]
pub struct MemberObservation {
    pub role_ids: Vec<String>,
    pub observed_at: String,
}

impl Store {
    /// Retain validated membership evidence without copying it into a current access decision.
    pub fn save_member_observation(
        &self,
        user: &str,
        guild: &str,
        roles: &[String],
        at: &str,
    ) -> Result<(), StoreError> {
        for id in [user, guild]
            .into_iter()
            .chain(roles.iter().map(String::as_str))
        {
            if !id.parse::<u64>().is_ok_and(|id| id > 0) {
                return Err(StoreError::Data("invalid member observation ID".into()));
            }
        }
        chrono::DateTime::parse_from_rfc3339(at)
            .map_err(|_| StoreError::Data("invalid member observation timestamp".into()))?;
        let json = serde_json::to_string(roles)
            .map_err(|_| StoreError::Data("invalid role observation".into()))?;
        self.conn.lock().expect("store lock").execute("INSERT INTO member_observations(account_user_id,guild_id,observed_at,roles_json) VALUES(?1,?2,?3,?4) ON CONFLICT(account_user_id,guild_id) DO UPDATE SET observed_at=excluded.observed_at,roles_json=excluded.roles_json WHERE julianday(excluded.observed_at)>=julianday(member_observations.observed_at)",params![user,guild,at,json])?;
        Ok(())
    }
    /// Load historical membership after a current fetch fails.
    pub fn member_observation(
        &self,
        user: &str,
        guild: &str,
    ) -> Result<Option<MemberObservation>, StoreError> {
        let value:Option<(String,String)>=self.conn.lock().expect("store lock").query_row("SELECT roles_json,observed_at FROM member_observations WHERE account_user_id=?1 AND guild_id=?2",params![user,guild],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
        value
            .map(|(json, observed_at)| {
                Ok(MemberObservation {
                    role_ids: serde_json::from_str(&json).map_err(|_| {
                        StoreError::Data("invalid retained member observation".into())
                    })?,
                    observed_at,
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn role_history_survives_restart_and_remains_account_scoped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.sqlite3");
        {
            let store = Store::open(&path).unwrap();
            store
                .save_member_observation("7", "42", &["555".into()], "2026-01-01T00:00:00Z")
                .unwrap();
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(
            store
                .member_observation("7", "42")
                .unwrap()
                .unwrap()
                .role_ids,
            vec!["555"]
        );
        assert!(store.member_observation("8", "42").unwrap().is_none());
    }
}
