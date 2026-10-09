//! Persist the latest account read-state observation separately from message history.

use super::{Store, StoreError};
use discord_api::account_observation::AccountObservation;
use rusqlite::{params, OptionalExtension};

impl Store {
    /// Persist a source observation without allowing older requests to replace newer state.
    pub fn save_account_observation(
        &self,
        observation: &AccountObservation,
    ) -> Result<(), StoreError> {
        let json = serde_json::to_string(observation)
            .map_err(|error| StoreError::Data(error.to_string()))?;
        let conn = self.conn.lock().expect("store lock");
        let valid: Option<f64> =
            conn.query_row("SELECT julianday(?1)", [&observation.observed_at], |row| {
                row.get(0)
            })?;
        if valid.is_none() {
            return Err(StoreError::Data("invalid observation timestamp".into()));
        }
        conn.execute(
            "INSERT INTO account_observations(account_user_id,observed_at,observation_json) VALUES(?1,?2,?3) ON CONFLICT(account_user_id) DO UPDATE SET observed_at=excluded.observed_at,observation_json=excluded.observation_json WHERE julianday(excluded.observed_at)>=julianday(account_observations.observed_at)",
            params![observation.account_user_id, observation.observed_at, json],
        )?;
        Ok(())
    }

    /// Return the last saved account observation, preserving missing wire fields.
    pub fn load_account_observation(
        &self,
        user: &str,
    ) -> Result<Option<AccountObservation>, StoreError> {
        let json: Option<String> = self
            .conn
            .lock()
            .expect("store lock")
            .query_row(
                "SELECT observation_json FROM account_observations WHERE account_user_id=?1",
                [user],
                |row| row.get(0),
            )
            .optional()?;
        json.map(|value| {
            serde_json::from_str(&value).map_err(|error| StoreError::Data(error.to_string()))
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn observation(user: &str, at: &str) -> AccountObservation {
        serde_json::from_value(json!({
            "account_user_id":user,"observed_at":at,"version":"42","partial":false,
            "channels":[],"inventory_complete":false,
            "read_states":[{"channel_id":"200","last_read_message_id":"100",
                "discord_mention_count":0,"observed_at":at,"version":"42",
                "availability":"available","reason":null}]
        }))
        .unwrap()
    }

    #[test]
    fn observations_survive_reopen_and_are_isolated_by_account() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");
        {
            let store = Store::open(&path).unwrap();
            store
                .save_account_observation(&observation("7", "2026-01-01T00:00:00Z"))
                .unwrap();
            store
                .save_account_observation(&observation("8", "2026-01-02T00:00:00Z"))
                .unwrap();
        }
        let store = Store::open(&path).unwrap();
        let loaded = store.load_account_observation("7").unwrap().unwrap();
        assert_eq!(loaded.read_states[0].discord_mention_count, Some(0));
        assert_eq!(loaded.version.as_deref(), Some("42"));
        assert_eq!(loaded.observed_at, "2026-01-01T00:00:00Z");
        assert!(store.load_account_observation("9").unwrap().is_none());
    }

    #[test]
    fn older_observations_cannot_replace_newer_state() {
        let store = Store::open_in_memory().unwrap();
        store
            .save_account_observation(&observation("7", "2026-01-02T00:00:00Z"))
            .unwrap();
        store
            .save_account_observation(&observation("7", "2026-01-01T00:00:00Z"))
            .unwrap();
        assert_eq!(
            store
                .load_account_observation("7")
                .unwrap()
                .unwrap()
                .observed_at,
            "2026-01-02T00:00:00Z"
        );
    }
}
