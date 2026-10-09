//! Explicit local notification and action confirmations, independent of Discord read state.

use chrono::DateTime;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

use super::{Store, StoreError};

/// Local user-confirmed state for one account/message pair.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct InboxWorkflow {
    pub account_user_id: String,
    pub channel_id: String,
    pub message_id: String,
    pub notified_at: Option<String>,
    pub snoozed_until: Option<String>,
    pub action_required: Option<bool>,
    pub action_evidence: Option<String>,
    pub completed_at: Option<String>,
    pub completion_evidence: Option<String>,
}

impl InboxWorkflow {
    fn validate(&self) -> Result<(), StoreError> {
        validate_ids(&self.account_user_id, &self.channel_id, &self.message_id)?;
        for timestamp in [&self.notified_at, &self.snoozed_until, &self.completed_at]
            .into_iter()
            .flatten()
        {
            DateTime::parse_from_rfc3339(timestamp)
                .map_err(|_| StoreError::Data("workflow timestamps must be RFC3339".into()))?;
        }
        for evidence in [&self.action_evidence, &self.completion_evidence]
            .into_iter()
            .flatten()
        {
            if evidence.trim().is_empty() {
                return Err(StoreError::Data(
                    "workflow evidence must not be empty".into(),
                ));
            }
        }
        if self.action_required == Some(true) && self.action_evidence.is_none() {
            return Err(StoreError::Data(
                "required action needs explicit evidence".into(),
            ));
        }
        if self.completed_at.is_some() != self.completion_evidence.is_some() {
            return Err(StoreError::Data(
                "completion requires both an explicit timestamp and evidence".into(),
            ));
        }
        Ok(())
    }
}

fn validate_ids(user: &str, channel: &str, message: &str) -> Result<(), StoreError> {
    for id in [user, channel, message] {
        if !id.bytes().all(|byte| byte.is_ascii_digit())
            || id.parse::<u64>().ok().is_none_or(|id| id == 0)
        {
            return Err(StoreError::Data(
                "workflow IDs must be positive Discord snowflake strings".into(),
            ));
        }
    }
    Ok(())
}

impl Store {
    /// Replace explicit local confirmations without updating Discord read state.
    pub fn record_inbox_workflow(&self, workflow: &InboxWorkflow) -> Result<(), StoreError> {
        workflow.validate()?;
        let workflow_json = serde_json::to_string(workflow)
            .map_err(|_| StoreError::Data("invalid workflow state".into()))?;
        self.conn.lock().expect("store lock").execute(
            "INSERT INTO inbox_workflow(account_user_id,channel_id,message_id,workflow_json) VALUES(?1,?2,?3,?4) ON CONFLICT(account_user_id,channel_id,message_id) DO UPDATE SET workflow_json=excluded.workflow_json",
            params![workflow.account_user_id,workflow.channel_id,workflow.message_id,workflow_json],
        )?;
        Ok(())
    }

    /// Load the independent local workflow state for one retained message.
    pub fn inbox_workflow(
        &self,
        user: &str,
        channel: &str,
        message: &str,
    ) -> Result<Option<InboxWorkflow>, StoreError> {
        validate_ids(user, channel, message)?;
        let json: Option<String> = self.conn.lock().expect("store lock").query_row(
            "SELECT workflow_json FROM inbox_workflow WHERE account_user_id=?1 AND channel_id=?2 AND message_id=?3",
            params![user,channel,message], |row| row.get(0),
        ).optional()?;
        json.map(|json| {
            let workflow: InboxWorkflow = serde_json::from_str(&json)
                .map_err(|_| StoreError::Data("invalid retained workflow state".into()))?;
            workflow.validate()?;
            if workflow.account_user_id != user
                || workflow.channel_id != channel
                || workflow.message_id != message
            {
                return Err(StoreError::Data(
                    "retained workflow account or message mismatch".into(),
                ));
            }
            Ok(workflow)
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> InboxWorkflow {
        InboxWorkflow {
            account_user_id: "7".into(),
            channel_id: "200".into(),
            message_id: "300".into(),
            ..InboxWorkflow::default()
        }
    }

    #[test]
    fn invalid_confirmations_are_rejected_before_saving() {
        let store = Store::open_in_memory().unwrap();
        let cases = [
            InboxWorkflow {
                completed_at: Some("2026-10-10T10:00:00Z".into()),
                ..state()
            },
            InboxWorkflow {
                completion_evidence: Some("done".into()),
                ..state()
            },
            InboxWorkflow {
                action_required: Some(true),
                ..state()
            },
            InboxWorkflow {
                notified_at: Some("yesterday".into()),
                ..state()
            },
            InboxWorkflow {
                snoozed_until: Some("2026-02-30T00:00:00Z".into()),
                ..state()
            },
            InboxWorkflow {
                message_id: "0".into(),
                ..state()
            },
            InboxWorkflow {
                channel_id: "-1".into(),
                ..state()
            },
            InboxWorkflow {
                account_user_id: "user".into(),
                ..state()
            },
        ];
        for workflow in cases {
            assert!(
                matches!(
                    store.record_inbox_workflow(&workflow),
                    Err(StoreError::Data(_))
                ),
                "accepted {workflow:?}"
            );
        }
        assert!(store.inbox_workflow("7", "200", "300").unwrap().is_none());
    }

    #[test]
    fn confirmations_are_deduplicated_account_scoped_and_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("workflow.sqlite3");
        let initial = InboxWorkflow {
            notified_at: Some("2026-10-10T10:00:00Z".into()),
            action_required: Some(true),
            action_evidence: Some("explicit deadline request".into()),
            ..state()
        };
        {
            let store = Store::open(&path).unwrap();
            store.record_inbox_workflow(&initial).unwrap();
            store.record_inbox_workflow(&initial).unwrap();
            assert_eq!(
                store.inbox_workflow("7", "200", "300").unwrap(),
                Some(initial.clone())
            );
            assert!(store.inbox_workflow("8", "200", "300").unwrap().is_none());
            let count: i64 = store
                .conn
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM inbox_workflow", [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, 1);
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(
            store.inbox_workflow("7", "200", "300").unwrap(),
            Some(initial.clone())
        );
        let completed = InboxWorkflow {
            completed_at: Some("2026-10-10T12:00:00+09:00".into()),
            completion_evidence: Some("user confirmed task completion".into()),
            ..initial
        };
        store.record_inbox_workflow(&completed).unwrap();
        assert_eq!(
            store.inbox_workflow("7", "200", "300").unwrap(),
            Some(completed)
        );
    }
}
