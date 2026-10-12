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

/// A partial update distinguishes an omitted field from explicit null.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum FieldPatch<T> {
    #[default]
    Keep,
    Clear,
    Set(T),
}

impl<T: serde::Serialize> Serialize for FieldPatch<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Set(value) => value.serialize(serializer),
            _ => serializer.serialize_none(),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for FieldPatch<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<T>::deserialize(deserializer).map(|value| value.map_or(Self::Clear, Self::Set))
    }
}

impl<T> FieldPatch<T> {
    fn is_keep(&self) -> bool {
        matches!(self, Self::Keep)
    }
    fn apply(&self, value: &mut Option<T>)
    where
        T: Clone,
    {
        match self {
            Self::Keep => {}
            Self::Clear => *value = None,
            Self::Set(new) => *value = Some(new.clone()),
        }
    }
}

/// Atomic local workflow update; version zero represents an absent row.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboxWorkflowPatch {
    pub expected_version: u64,
    #[serde(default, skip_serializing_if = "FieldPatch::is_keep")]
    pub notified_at: FieldPatch<String>,
    #[serde(default, skip_serializing_if = "FieldPatch::is_keep")]
    pub snoozed_until: FieldPatch<String>,
    #[serde(default, skip_serializing_if = "FieldPatch::is_keep")]
    pub action_required: FieldPatch<bool>,
    #[serde(default, skip_serializing_if = "FieldPatch::is_keep")]
    pub action_evidence: FieldPatch<String>,
    #[serde(default, skip_serializing_if = "FieldPatch::is_keep")]
    pub completed_at: FieldPatch<String>,
    #[serde(default, skip_serializing_if = "FieldPatch::is_keep")]
    pub completion_evidence: FieldPatch<String>,
}

/// Retained workflow together with its concurrency revision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRevision {
    pub workflow: InboxWorkflow,
    pub version: u64,
}

/// A stale writer cannot overwrite newer user confirmations.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowUpdateError {
    #[error("workflow version conflict (expected {expected}, current {current})")]
    Conflict { expected: u64, current: u64 },
    #[error(transparent)]
    Store(#[from] StoreError),
}

pub(super) fn load_revision(
    conn: &rusqlite::Connection,
    user: &str,
    channel: &str,
    message: &str,
) -> Result<Option<WorkflowRevision>, StoreError> {
    let row: Option<(String,i64)> = conn.query_row("SELECT workflow_json,revision FROM inbox_workflow WHERE account_user_id=?1 AND channel_id=?2 AND message_id=?3", params![user,channel,message], |row| Ok((row.get(0)?,row.get(1)?))).optional()?;
    row.map(|(json, version)| {
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
        Ok(WorkflowRevision {
            workflow,
            version: u64::try_from(version)
                .map_err(|_| StoreError::Data("invalid workflow revision".into()))?,
        })
    })
    .transpose()
}

impl InboxWorkflow {
    pub(super) fn validate(&self) -> Result<(), StoreError> {
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

pub(super) fn validate_ids(user: &str, channel: &str, message: &str) -> Result<(), StoreError> {
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
            "INSERT INTO inbox_workflow(account_user_id,channel_id,message_id,workflow_json) VALUES(?1,?2,?3,?4) ON CONFLICT(account_user_id,channel_id,message_id) DO UPDATE SET workflow_json=excluded.workflow_json,revision=inbox_workflow.revision+1",
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
        Ok(self
            .inbox_workflow_revision(user, channel, message)?
            .map(|revision| revision.workflow))
    }

    /// Load current local confirmations with their revision.
    pub fn inbox_workflow_revision(
        &self,
        user: &str,
        channel: &str,
        message: &str,
    ) -> Result<Option<WorkflowRevision>, StoreError> {
        validate_ids(user, channel, message)?;
        load_revision(
            &self.conn.lock().expect("store lock"),
            user,
            channel,
            message,
        )
    }

    /// Apply only explicit fields if the retained revision still matches.
    pub fn patch_inbox_workflow(
        &self,
        user: &str,
        channel: &str,
        message: &str,
        patch: &InboxWorkflowPatch,
    ) -> Result<WorkflowRevision, WorkflowUpdateError> {
        validate_ids(user, channel, message)?;
        let mut conn = self.conn.lock().expect("store lock");
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::from)?;
        let retained = load_revision(&tx, user, channel, message)?;
        let current = retained.as_ref().map_or(0, |revision| revision.version);
        if current != patch.expected_version {
            return Err(WorkflowUpdateError::Conflict {
                expected: patch.expected_version,
                current,
            });
        }
        let mut workflow = retained.map_or_else(
            || InboxWorkflow {
                account_user_id: user.into(),
                channel_id: channel.into(),
                message_id: message.into(),
                ..InboxWorkflow::default()
            },
            |revision| revision.workflow,
        );
        patch.notified_at.apply(&mut workflow.notified_at);
        patch.snoozed_until.apply(&mut workflow.snoozed_until);
        patch.action_required.apply(&mut workflow.action_required);
        patch.action_evidence.apply(&mut workflow.action_evidence);
        patch.completed_at.apply(&mut workflow.completed_at);
        patch
            .completion_evidence
            .apply(&mut workflow.completion_evidence);
        workflow.validate()?;
        let version = current
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| StoreError::Data("workflow revision exhausted".into()))?;
        let json = serde_json::to_string(&workflow)
            .map_err(|_| StoreError::Data("invalid workflow state".into()))?;
        tx.execute("INSERT INTO inbox_workflow(account_user_id,channel_id,message_id,workflow_json,revision) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(account_user_id,channel_id,message_id) DO UPDATE SET workflow_json=excluded.workflow_json,revision=excluded.revision",params![user,channel,message,json,version as i64]).map_err(StoreError::from)?;
        tx.commit().map_err(StoreError::from)?;
        Ok(WorkflowRevision { workflow, version })
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
    fn patch_preserves_omissions_distinguishes_null_and_rejects_old_revision() {
        let store = Store::open_in_memory().unwrap();
        let initial = InboxWorkflow {
            notified_at: Some("2026-10-10T10:00:00Z".into()),
            action_evidence: Some("request".into()),
            ..state()
        };
        store.record_inbox_workflow(&initial).unwrap();
        let patch: InboxWorkflowPatch = serde_json::from_value(
            serde_json::json!({"expected_version":1,"action_evidence":null}),
        )
        .unwrap();
        let updated = store
            .patch_inbox_workflow("7", "200", "300", &patch)
            .unwrap();
        assert_eq!(updated.version, 2);
        assert_eq!(updated.workflow.notified_at, initial.notified_at);
        assert_eq!(updated.workflow.action_evidence, None);
        assert!(matches!(
            store.patch_inbox_workflow("7", "200", "300", &patch),
            Err(WorkflowUpdateError::Conflict { .. })
        ));
        store.record_inbox_workflow(&state()).unwrap();
        assert_eq!(
            store
                .inbox_workflow_revision("7", "200", "300")
                .unwrap()
                .unwrap()
                .version,
            3
        );
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
