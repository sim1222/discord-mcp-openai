//! Durable local notification claims; this module never sends external messages.

use super::{
    workflow::{load_revision, validate_ids},
    Store, StoreError,
};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

/// Identity of a material notification, independent of Discord read state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NotificationKey {
    pub account_user_id: String,
    pub channel_id: String,
    pub message_id: String,
    pub material_version: String,
    pub notification_kind: String,
}

/// Durable delivery stages distinguish a claim from a potentially sent message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NotificationStatus {
    Queued,
    Claimed,
    Sending,
    Delivered,
    AmbiguousDelivery,
}

/// Retained outbox state, including scheduling, ownership, and receipt evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationEntry {
    #[serde(default = "initial_revision")]
    pub revision: u64,
    pub key: NotificationKey,
    pub status: NotificationStatus,
    pub available_at: String,
    pub expected_workflow_version: u64,
    pub renotify_material_change: bool,
    pub claim_token: Option<String>,
    pub claimed_by: Option<String>,
    pub claim_expires_at: Option<String>,
    pub receipt: Option<String>,
    pub delivered_at: Option<String>,
    pub reason: Option<String>,
}

fn initial_revision() -> u64 {
    1
}

/// A live account-scoped queue page; membership is not a frozen snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct NotificationOutboxPage {
    pub entries: Vec<NotificationEntry>,
    pub next_after_rowid: Option<i64>,
    pub has_more: bool,
}

/// Historical local delivery evidence across a message's material versions and kinds.
#[derive(Debug, Clone, Serialize)]
pub struct NotificationDeliverySummary {
    pub account_user_id: String,
    pub channel_id: String,
    pub message_id: String,
    pub observed_at: String,
    pub observed_at_basis: &'static str,
    pub delivered_count: u64,
    pub ambiguous_delivery_count: u64,
    pub pending_count: u64,
    pub unknown_status_count: u64,
    pub latest_delivery_at: Option<String>,
    pub latest_delivery_order_basis: &'static str,
    pub notification_status: &'static str,
    pub current_material_delivery_status: &'static str,
    pub source: &'static str,
    pub observation_scope: &'static str,
}

/// Externally observed results; ambiguity never schedules a blind retry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeliveryOutcome {
    Delivered { receipt: String },
    AmbiguousDelivery { reason: String },
    NotSent { reason: String },
}

/// Claims fail closed on competing ownership, stale workflow, or unknown delivery.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("notification entry does not exist")]
    NotFound,
    #[error("notification ownership or delivery state conflict")]
    Conflict,
    #[error("notification workflow changed or is not eligible")]
    Ineligible,
    #[error(transparent)]
    Store(#[from] StoreError),
}

fn text(value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() || value.len() > 1024 {
        return Err(StoreError::Data(
            "outbox text must contain 1..1024 bytes".into(),
        ));
    }
    Ok(())
}
fn instant(value: &str) -> Result<DateTime<Utc>, StoreError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| StoreError::Data("outbox timestamps must be RFC3339".into()))
}
impl NotificationKey {
    fn validate(&self) -> Result<(), StoreError> {
        validate_ids(&self.account_user_id, &self.channel_id, &self.message_id)?;
        text(&self.material_version)?;
        text(&self.notification_kind)
    }
}

fn load(
    conn: &rusqlite::Connection,
    key: &NotificationKey,
) -> Result<Option<NotificationEntry>, StoreError> {
    let json: Option<String> = conn.query_row("SELECT entry_json FROM notification_outbox WHERE account_user_id=?1 AND channel_id=?2 AND message_id=?3 AND material_version=?4 AND notification_kind=?5",params![key.account_user_id,key.channel_id,key.message_id,key.material_version,key.notification_kind],|row| row.get(0)).optional()?;
    json.map(|json| {
        let entry: NotificationEntry = serde_json::from_str(&json)
            .map_err(|_| StoreError::Data("invalid retained outbox state".into()))?;
        if entry.key != *key {
            return Err(StoreError::Data("retained outbox identity mismatch".into()));
        }
        Ok(entry)
    })
    .transpose()
}
fn save(conn: &rusqlite::Connection, entry: &mut NotificationEntry) -> Result<(), StoreError> {
    entry.revision = entry
        .revision
        .checked_add(1)
        .ok_or_else(|| StoreError::Data("outbox revision exhausted".into()))?;
    let json = serde_json::to_string(entry)
        .map_err(|_| StoreError::Data("invalid notification state".into()))?;
    let key = &entry.key;
    conn.execute("INSERT INTO notification_outbox(account_user_id,channel_id,message_id,material_version,notification_kind,entry_json) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(account_user_id,channel_id,message_id,material_version,notification_kind) DO UPDATE SET entry_json=excluded.entry_json",params![key.account_user_id,key.channel_id,key.message_id,key.material_version,key.notification_kind,json])?;
    Ok(())
}
fn eligible(
    conn: &rusqlite::Connection,
    entry: &NotificationEntry,
    now: DateTime<Utc>,
) -> Result<(), OutboxError> {
    let current = load_revision(
        conn,
        &entry.key.account_user_id,
        &entry.key.channel_id,
        &entry.key.message_id,
    )?;
    if current.as_ref().map_or(0, |state| state.version) != entry.expected_workflow_version {
        return Err(OutboxError::Ineligible);
    }
    if let Some(state) = current {
        let workflow = state.workflow;
        if workflow.completed_at.is_some()
            || (workflow.notified_at.is_some() && !entry.renotify_material_change)
            || workflow
                .snoozed_until
                .as_deref()
                .map(instant)
                .transpose()?
                .is_some_and(|until| until > now)
        {
            return Err(OutboxError::Ineligible);
        }
    }
    if instant(&entry.available_at)? > now {
        return Err(OutboxError::Ineligible);
    }
    Ok(())
}
fn owned(entry: &NotificationEntry, token: &str, now: DateTime<Utc>) -> Result<(), OutboxError> {
    if entry.claim_token.as_deref() != Some(token)
        || !entry
            .claim_expires_at
            .as_deref()
            .map(instant)
            .transpose()?
            .is_some_and(|expiry| expiry > now)
    {
        return Err(OutboxError::Conflict);
    }
    Ok(())
}

impl Store {
    /// Enqueue once per material key; available_at persists quiet-hours deferral.
    pub fn enqueue_notification(
        &self,
        key: &NotificationKey,
        available_at: &str,
        expected_workflow_version: u64,
        renotify_material_change: bool,
    ) -> Result<NotificationEntry, OutboxError> {
        key.validate()?;
        instant(available_at)?;
        let mut conn = self.conn.lock().expect("store lock");
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::from)?;
        if let Some(existing) = load(&tx, key)? {
            return Ok(existing);
        }
        let mut entry = NotificationEntry {
            revision: 0,
            key: key.clone(),
            status: NotificationStatus::Queued,
            available_at: available_at.into(),
            expected_workflow_version,
            renotify_material_change,
            claim_token: None,
            claimed_by: None,
            claim_expires_at: None,
            receipt: None,
            delivered_at: None,
            reason: None,
        };
        save(&tx, &mut entry)?;
        tx.commit().map_err(StoreError::from)?;
        Ok(entry)
    }

    /// Explicitly refresh a queued candidate using both outbox and current workflow CAS.
    pub fn revalidate_notification(
        &self,
        key: &NotificationKey,
        expected_entry_version: u64,
        expected_workflow_version: u64,
        available_at: &str,
    ) -> Result<NotificationEntry, OutboxError> {
        key.validate()?;
        instant(available_at)?;
        if expected_entry_version == 0 {
            return Err(StoreError::Data("outbox version must be positive".into()).into());
        }
        let mut conn = self.conn.lock().expect("store lock");
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::from)?;
        let mut entry = load(&tx, key)?.ok_or(OutboxError::NotFound)?;
        if entry.status != NotificationStatus::Queued || entry.revision != expected_entry_version {
            return Err(OutboxError::Conflict);
        }
        let state = load_revision(&tx, &key.account_user_id, &key.channel_id, &key.message_id)?;
        if state.as_ref().map_or(0, |state| state.version) != expected_workflow_version {
            return Err(OutboxError::Ineligible);
        }
        if state.is_some_and(|state| {
            state.workflow.completed_at.is_some()
                || (state.workflow.notified_at.is_some() && !entry.renotify_material_change)
        }) {
            return Err(OutboxError::Ineligible);
        }
        entry.available_at = available_at.into();
        entry.expected_workflow_version = expected_workflow_version;
        entry.reason = Some("explicit_queued_revalidation".into());
        save(&tx, &mut entry)?;
        tx.commit().map_err(StoreError::from)?;
        Ok(entry)
    }

    /// Claim a due item for a worker, returning an opaque token for delivery operations.
    /// Expired sending remains ambiguous and cannot be reclaimed.
    pub fn claim_notification(
        &self,
        key: &NotificationKey,
        worker_id: &str,
        now: &str,
        lease_seconds: u32,
    ) -> Result<NotificationEntry, OutboxError> {
        key.validate()?;
        text(worker_id)?;
        let now = instant(now)?;
        if !(1..=3600).contains(&lease_seconds) {
            return Err(StoreError::Data("claim lease must be 1..3600 seconds".into()).into());
        }
        let mut conn = self.conn.lock().expect("store lock");
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::from)?;
        let mut entry = load(&tx, key)?.ok_or(OutboxError::NotFound)?;
        let expired = entry
            .claim_expires_at
            .as_deref()
            .map(instant)
            .transpose()?
            .is_some_and(|at| at <= now);
        if entry.status == NotificationStatus::Sending && expired {
            entry.status = NotificationStatus::AmbiguousDelivery;
            entry.reason = Some("claim_expired_after_delivery_started".into());
            save(&tx, &mut entry)?;
            tx.commit().map_err(StoreError::from)?;
            return Ok(entry);
        }
        if entry.status == NotificationStatus::Claimed && expired {
            entry.status = NotificationStatus::Queued;
        }
        if entry.status != NotificationStatus::Queued {
            return Err(OutboxError::Conflict);
        }
        eligible(&tx, &entry, now)?;
        entry.status = NotificationStatus::Claimed;
        entry.claimed_by = Some(worker_id.into());
        entry.claim_token = Some(
            tx.query_row("SELECT lower(hex(randomblob(16)))", [], |row| {
                row.get::<_, String>(0)
            })
            .map_err(StoreError::from)?,
        );
        entry.claim_expires_at = Some(
            now.checked_add_signed(Duration::seconds(lease_seconds.into()))
                .ok_or_else(|| StoreError::Data("claim expiry exceeds timestamp range".into()))?
                .to_rfc3339(),
        );
        entry.reason = None;
        save(&tx, &mut entry)?;
        tx.commit().map_err(StoreError::from)?;
        Ok(entry)
    }

    /// Recheck the newest workflow immediately before an external send may start.
    pub fn begin_notification_delivery(
        &self,
        key: &NotificationKey,
        token: &str,
        now: &str,
    ) -> Result<NotificationEntry, OutboxError> {
        key.validate()?;
        text(token)?;
        let now = instant(now)?;
        let mut conn = self.conn.lock().expect("store lock");
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::from)?;
        let mut entry = load(&tx, key)?.ok_or(OutboxError::NotFound)?;
        owned(&entry, token, now)?;
        if entry.status != NotificationStatus::Claimed {
            return Err(OutboxError::Conflict);
        }
        eligible(&tx, &entry, now)?;
        entry.status = NotificationStatus::Sending;
        save(&tx, &mut entry)?;
        tx.commit().map_err(StoreError::from)?;
        Ok(entry)
    }

    /// Record a worker's result using its live ownership token.
    pub fn finish_notification_delivery(
        &self,
        key: &NotificationKey,
        token: &str,
        now: &str,
        outcome: &DeliveryOutcome,
    ) -> Result<NotificationEntry, OutboxError> {
        key.validate()?;
        text(token)?;
        let at = instant(now)?;
        let mut conn = self.conn.lock().expect("store lock");
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::from)?;
        let mut entry = load(&tx, key)?.ok_or(OutboxError::NotFound)?;
        owned(&entry, token, at)?;
        if entry.status != NotificationStatus::Sending
            && entry.status != NotificationStatus::Claimed
        {
            return Err(OutboxError::Conflict);
        }
        match outcome {
            DeliveryOutcome::Delivered { receipt } => {
                text(receipt)?;
                if entry.status != NotificationStatus::Sending {
                    return Err(OutboxError::Conflict);
                }
                entry.status = NotificationStatus::Delivered;
                entry.receipt = Some(receipt.clone());
                entry.delivered_at = Some(now.into());
                entry.reason = None;
            }
            DeliveryOutcome::AmbiguousDelivery { reason } => {
                text(reason)?;
                entry.status = NotificationStatus::AmbiguousDelivery;
                entry.reason = Some(reason.clone());
            }
            DeliveryOutcome::NotSent { reason } => {
                text(reason)?;
                entry.status = NotificationStatus::Queued;
                entry.reason = Some(reason.clone());
            }
        }
        entry.claim_token = None;
        entry.claimed_by = None;
        entry.claim_expires_at = None;
        save(&tx, &mut entry)?;
        tx.commit().map_err(StoreError::from)?;
        Ok(entry)
    }

    /// Import externally confirmed delivery or reconcile ambiguity with a receipt.
    pub fn import_notification_delivery(
        &self,
        key: &NotificationKey,
        receipt: &str,
        at: &str,
    ) -> Result<NotificationEntry, OutboxError> {
        key.validate()?;
        text(receipt)?;
        instant(at)?;
        let mut conn = self.conn.lock().expect("store lock");
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::from)?;
        let mut entry = load(&tx, key)?.unwrap_or(NotificationEntry {
            revision: 0,
            key: key.clone(),
            status: NotificationStatus::Queued,
            available_at: at.into(),
            expected_workflow_version: 0,
            renotify_material_change: false,
            claim_token: None,
            claimed_by: None,
            claim_expires_at: None,
            receipt: None,
            delivered_at: None,
            reason: None,
        });
        if entry.status == NotificationStatus::Delivered {
            if entry.receipt.as_deref() != Some(receipt) {
                return Err(OutboxError::Conflict);
            }
            return Ok(entry);
        }
        entry.status = NotificationStatus::Delivered;
        entry.receipt = Some(receipt.into());
        entry.delivered_at = Some(at.into());
        entry.claim_token = None;
        entry.claimed_by = None;
        entry.claim_expires_at = None;
        entry.reason = Some("externally_confirmed_delivery".into());
        save(&tx, &mut entry)?;
        tx.commit().map_err(StoreError::from)?;
        Ok(entry)
    }

    /// Summarize historical delivery without exposing claim tokens or private receipts.
    pub fn notification_delivery_summary(
        &self,
        user: &str,
        channel: &str,
        message: &str,
    ) -> Result<NotificationDeliverySummary, StoreError> {
        validate_ids(user, channel, message)?;
        let conn = self.conn.lock().expect("store lock");
        let (delivered, ambiguous, pending, total, latest): (i64,i64,i64,i64,Option<String>) = conn.query_row(
            "SELECT COALESCE(SUM(json_extract(entry_json,'$.status')='delivered'),0), COALESCE(SUM(json_extract(entry_json,'$.status')='ambiguous_delivery'),0), COALESCE(SUM(json_extract(entry_json,'$.status') IN ('queued','claimed','sending')),0), COUNT(*), (SELECT json_extract(delivery.entry_json,'$.delivered_at') FROM notification_outbox delivery WHERE delivery.account_user_id=?1 AND delivery.channel_id=?2 AND delivery.message_id=?3 AND json_extract(delivery.entry_json,'$.status')='delivered' ORDER BY julianday(json_extract(delivery.entry_json,'$.delivered_at')) DESC,delivery.rowid DESC LIMIT 1) FROM notification_outbox WHERE account_user_id=?1 AND channel_id=?2 AND message_id=?3",
            params![user,channel,message], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)))?;
        let notification_status = if delivered > 0 {
            "prior_delivery_recorded"
        } else if ambiguous > 0 {
            "ambiguous_delivery"
        } else if pending > 0 {
            "pending_local_delivery"
        } else {
            "not_recorded"
        };
        Ok(NotificationDeliverySummary {
            account_user_id: user.into(),
            channel_id: channel.into(),
            message_id: message.into(),
            observed_at: Utc::now().to_rfc3339(),
            observed_at_basis: "local_inspection_completed_at",
            delivered_count: delivered as u64,
            ambiguous_delivery_count: ambiguous as u64,
            pending_count: pending as u64,
            unknown_status_count: (total - delivered - ambiguous - pending) as u64,
            latest_delivery_at: latest,
            latest_delivery_order_basis: "sqlite_datetime_millisecond_precision",
            notification_status,
            current_material_delivery_status: "unknown",
            source: "local_notification_outbox",
            observation_scope: "all_retained_material_versions_and_kinds",
        })
    }

    /// Page unresolved entries by stable row identity, or one explicit status.
    /// New or changed entries behind a cursor are found by starting another round.
    pub fn notification_outbox_page(
        &self,
        account_user_id: &str,
        after_rowid: Option<i64>,
        limit: u32,
        status: Option<NotificationStatus>,
    ) -> Result<NotificationOutboxPage, StoreError> {
        validate_ids(account_user_id, "1", "1")?;
        if !(1..=100).contains(&limit) || after_rowid.is_some_and(|id| id <= 0) {
            return Err(StoreError::Data(
                "outbox page requires limit 1..100 and positive cursor".into(),
            ));
        }
        let status = status.map(|status| match status {
            NotificationStatus::Queued => "queued",
            NotificationStatus::Claimed => "claimed",
            NotificationStatus::Sending => "sending",
            NotificationStatus::Delivered => "delivered",
            NotificationStatus::AmbiguousDelivery => "ambiguous_delivery",
        });
        let conn = self.conn.lock().expect("store lock");
        let mut stmt = conn.prepare("SELECT rowid,entry_json FROM notification_outbox WHERE account_user_id=?1 AND rowid>?2 AND ((?3 IS NULL AND json_extract(entry_json,'$.status')!='delivered') OR json_extract(entry_json,'$.status')=?3) ORDER BY rowid LIMIT ?4")?;
        let rows = stmt.query_map(
            params![account_user_id, after_rowid.unwrap_or(0), status, limit + 1],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )?;
        let mut retained = rows.collect::<Result<Vec<_>, _>>()?;
        let has_more = retained.len() > limit as usize;
        if has_more {
            retained.pop();
        }
        let next_after_rowid = has_more.then(|| retained.last().expect("nonempty bounded page").0);
        let entries = retained
            .into_iter()
            .map(|(_, json)| {
                let entry: NotificationEntry = serde_json::from_str(&json)
                    .map_err(|_| StoreError::Data("invalid retained outbox state".into()))?;
                if entry.key.account_user_id != account_user_id {
                    return Err(StoreError::Data("retained outbox account mismatch".into()));
                }
                Ok(entry)
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        Ok(NotificationOutboxPage {
            entries,
            next_after_rowid,
            has_more,
        })
    }

    /// Read bounded account-scoped entries without initiating delivery.
    pub fn notification_outbox(
        &self,
        account_user_id: &str,
        limit: u32,
    ) -> Result<Vec<NotificationEntry>, StoreError> {
        validate_ids(account_user_id, "1", "1")?;
        if !(1..=100).contains(&limit) {
            return Err(StoreError::Data("outbox limit must be 1..100".into()));
        }
        let conn = self.conn.lock().expect("store lock");
        let mut stmt=conn.prepare("SELECT entry_json FROM notification_outbox WHERE account_user_id=?1 ORDER BY channel_id,message_id,material_version,notification_kind LIMIT ?2")?;
        let rows = stmt.query_map(params![account_user_id, limit], |row| {
            row.get::<_, String>(0)
        })?;
        rows.map(|row| {
            serde_json::from_str(&row?)
                .map_err(|_| StoreError::Data("invalid retained outbox state".into()))
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(version: &str) -> NotificationKey {
        NotificationKey {
            account_user_id: "7".into(),
            channel_id: "200".into(),
            message_id: "300".into(),
            material_version: version.into(),
            notification_kind: "schedule".into(),
        }
    }
    const NOW: &str = "2026-10-13T10:00:00Z";
    #[test]
    fn queued_revalidation_reschedules_after_snooze_clear_and_rejects_stale_writers() {
        let store = Store::open_in_memory().unwrap();
        let state = super::super::workflow::InboxWorkflow {
            account_user_id: "7".into(),
            channel_id: "200".into(),
            message_id: "300".into(),
            snoozed_until: Some("2026-10-14T10:00:00Z".into()),
            ..Default::default()
        };
        store.record_inbox_workflow(&state).unwrap();
        let entry = store
            .enqueue_notification(&key("v1"), NOW, 1, false)
            .unwrap();
        store
            .patch_inbox_workflow(
                "7",
                "200",
                "300",
                &super::super::workflow::InboxWorkflowPatch {
                    expected_version: 1,
                    snoozed_until: super::super::workflow::FieldPatch::Clear,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(matches!(
            store.claim_notification(&key("v1"), "worker", NOW, 10),
            Err(OutboxError::Ineligible)
        ));
        let updated = store
            .revalidate_notification(&key("v1"), entry.revision, 2, NOW)
            .unwrap();
        assert!(updated.revision > entry.revision);
        assert!(matches!(
            store.revalidate_notification(&key("v1"), entry.revision, 2, NOW),
            Err(OutboxError::Conflict)
        ));
        let claim = store
            .claim_notification(&key("v1"), "worker", NOW, 10)
            .unwrap();
        assert!(matches!(
            store.revalidate_notification(&key("v1"), claim.revision, 2, NOW),
            Err(OutboxError::Conflict)
        ));
        let token = claim.claim_token.as_deref().unwrap();
        let sending = store
            .begin_notification_delivery(&key("v1"), token, NOW)
            .unwrap();
        assert!(matches!(
            store.revalidate_notification(&key("v1"), sending.revision, 2, NOW),
            Err(OutboxError::Conflict)
        ));
        let ambiguous = store
            .finish_notification_delivery(
                &key("v1"),
                token,
                NOW,
                &DeliveryOutcome::AmbiguousDelivery {
                    reason: "delivery result unknown".into(),
                },
            )
            .unwrap();
        assert!(matches!(
            store.revalidate_notification(&key("v1"), ambiguous.revision, 2, NOW),
            Err(OutboxError::Conflict)
        ));
        let delivered = store
            .import_notification_delivery(&key("v1"), "receipt", NOW)
            .unwrap();
        assert!(matches!(
            store.revalidate_notification(&key("v1"), delivered.revision, 2, NOW),
            Err(OutboxError::Conflict)
        ));
    }

    #[test]
    fn delivery_summary_exposes_prior_delivery_and_ambiguity_without_receipts() {
        let store = Store::open_in_memory().unwrap();
        store
            .import_notification_delivery(&key("v1"), "private-receipt", NOW)
            .unwrap();
        store
            .enqueue_notification(&key("v2"), NOW, 0, true)
            .unwrap();
        let claim = store
            .claim_notification(&key("v2"), "worker", NOW, 10)
            .unwrap();
        store
            .begin_notification_delivery(&key("v2"), claim.claim_token.as_deref().unwrap(), NOW)
            .unwrap();
        store
            .finish_notification_delivery(
                &key("v2"),
                claim.claim_token.as_deref().unwrap(),
                NOW,
                &DeliveryOutcome::AmbiguousDelivery {
                    reason: "external_result_unknown".into(),
                },
            )
            .unwrap();
        let summary = store
            .notification_delivery_summary("7", "200", "300")
            .unwrap();
        assert_eq!(summary.delivered_count, 1);
        assert_eq!(summary.ambiguous_delivery_count, 1);
        assert_eq!(summary.notification_status, "prior_delivery_recorded");
        let json = serde_json::to_string(&summary).unwrap();
        assert!(!json.contains("private-receipt"));
        assert_eq!(
            store
                .notification_delivery_summary("8", "200", "300")
                .unwrap()
                .delivered_count,
            0
        );
    }

    #[test]
    fn live_queue_pages_skip_delivered_history_and_do_not_repeat_rows() {
        let store = Store::open_in_memory().unwrap();
        for number in 1..=5 {
            store
                .import_notification_delivery(&key(&format!("delivered-{number}")), "receipt", NOW)
                .unwrap();
        }
        for number in 1..=3 {
            store
                .enqueue_notification(&key(&format!("pending-{number}")), NOW, 0, false)
                .unwrap();
        }
        let page = store.notification_outbox_page("7", None, 2, None).unwrap();
        assert_eq!(page.entries.len(), 2);
        assert!(page
            .entries
            .iter()
            .all(|entry| entry.status == NotificationStatus::Queued));
        assert!(page.has_more);
        let final_page = store
            .notification_outbox_page("7", page.next_after_rowid, 2, None)
            .unwrap();
        assert_eq!(final_page.entries.len(), 1);
        assert!(!final_page.has_more);
        assert!(page
            .entries
            .iter()
            .all(|entry| entry.key != final_page.entries[0].key));
        assert_eq!(
            store
                .notification_outbox_page("7", None, 2, Some(NotificationStatus::Delivered))
                .unwrap()
                .entries
                .len(),
            2
        );
        assert!(store
            .notification_outbox_page("8", None, 2, None)
            .unwrap()
            .entries
            .is_empty());
    }

    #[test]
    fn deferred_queue_and_material_change_policy_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deferred.sqlite3");
        {
            let store = Store::open(&path).unwrap();
            let workflow = super::super::workflow::InboxWorkflow {
                account_user_id: "7".into(),
                channel_id: "200".into(),
                message_id: "300".into(),
                notified_at: Some(NOW.into()),
                ..Default::default()
            };
            store.record_inbox_workflow(&workflow).unwrap();
            store
                .enqueue_notification(&key("v1"), "2026-10-13T11:00:00Z", 1, false)
                .unwrap();
            store
                .enqueue_notification(&key("v2"), "2026-10-13T11:00:00Z", 1, true)
                .unwrap();
        }
        let store = Store::open(&path).unwrap();
        assert!(matches!(
            store.claim_notification(&key("v2"), "worker", NOW, 10),
            Err(OutboxError::Ineligible)
        ));
        let later = "2026-10-13T11:00:00Z";
        assert!(matches!(
            store.claim_notification(&key("v1"), "worker", later, 10),
            Err(OutboxError::Ineligible)
        ));
        let old = store
            .claim_notification(&key("v2"), "worker", later, 10)
            .unwrap();
        let new = store
            .claim_notification(&key("v2"), "worker", "2026-10-13T11:00:11Z", 10)
            .unwrap();
        assert_ne!(old.claim_token, new.claim_token);
        assert!(matches!(
            store.begin_notification_delivery(
                &key("v2"),
                old.claim_token.as_deref().unwrap(),
                "2026-10-13T11:00:12Z"
            ),
            Err(OutboxError::Conflict)
        ));
        store
            .begin_notification_delivery(
                &key("v2"),
                new.claim_token.as_deref().unwrap(),
                "2026-10-13T11:00:12Z",
            )
            .unwrap();
    }

    #[test]
    fn repeated_receipt_import_preserves_original_delivery_evidence() {
        let store = Store::open_in_memory().unwrap();
        store
            .import_notification_delivery(&key("v1"), "receipt", NOW)
            .unwrap();
        let repeat = store
            .import_notification_delivery(&key("v1"), "receipt", "2026-10-13T11:00:00Z")
            .unwrap();
        assert_eq!(repeat.delivered_at.as_deref(), Some(NOW));
    }

    #[test]
    fn independent_workers_cannot_both_claim_the_same_material_notification() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("workers.sqlite3");
        Store::open(&path)
            .unwrap()
            .enqueue_notification(&key("v1"), NOW, 0, false)
            .unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let workers: Vec<_> = ["worker-a", "worker-b"]
            .into_iter()
            .map(|token| {
                let store = Store::open(&path).unwrap();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.claim_notification(&key("v1"), token, NOW, 10).is_ok()
                })
            })
            .collect();
        assert_eq!(
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .filter(|claimed| *claimed)
                .count(),
            1
        );
    }

    #[test]
    fn claims_survive_restart_and_expired_sending_never_blindly_retries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("outbox.sqlite3");
        {
            let store = Store::open(&path).unwrap();
            store
                .enqueue_notification(&key("v1"), NOW, 0, false)
                .unwrap();
            let claim = store
                .claim_notification(&key("v1"), "worker-a", NOW, 10)
                .unwrap();
            assert!(matches!(
                store.claim_notification(&key("v1"), "worker-b", NOW, 10),
                Err(OutboxError::Conflict)
            ));
            store
                .begin_notification_delivery(&key("v1"), claim.claim_token.as_deref().unwrap(), NOW)
                .unwrap();
        }
        let store = Store::open(&path).unwrap();
        let result = store
            .claim_notification(&key("v1"), "worker-b", "2026-10-13T10:00:11Z", 10)
            .unwrap();
        assert_eq!(result.status, NotificationStatus::AmbiguousDelivery);
        assert!(matches!(
            store.claim_notification(&key("v1"), "worker-b", "2026-10-13T10:00:12Z", 10),
            Err(OutboxError::Conflict)
        ));
        store
            .import_notification_delivery(&key("v1"), "receipt-1", NOW)
            .unwrap();
        assert_eq!(
            store.notification_outbox("7", 10).unwrap()[0].status,
            NotificationStatus::Delivered
        );
    }
    #[test]
    fn crash_before_send_can_reclaim_and_quiet_hours_and_newest_workflow_are_checked() {
        let store = Store::open_in_memory().unwrap();
        store
            .enqueue_notification(&key("v1"), "2026-10-13T11:00:00Z", 0, false)
            .unwrap();
        assert!(matches!(
            store.claim_notification(&key("v1"), "a", NOW, 10),
            Err(OutboxError::Ineligible)
        ));
        let later = "2026-10-13T11:00:00Z";
        store
            .claim_notification(&key("v1"), "a", later, 10)
            .unwrap();
        let claim = store
            .claim_notification(&key("v1"), "b", "2026-10-13T11:00:11Z", 10)
            .unwrap();
        let state = super::super::workflow::InboxWorkflow {
            account_user_id: "7".into(),
            channel_id: "200".into(),
            message_id: "300".into(),
            completed_at: Some(later.into()),
            completion_evidence: Some("user confirmed completion".into()),
            ..Default::default()
        };
        store.record_inbox_workflow(&state).unwrap();
        assert!(matches!(
            store.begin_notification_delivery(
                &key("v1"),
                claim.claim_token.as_deref().unwrap(),
                "2026-10-13T11:00:12Z"
            ),
            Err(OutboxError::Ineligible)
        ));
    }
    #[test]
    fn imported_dedup_and_material_edit_are_independent() {
        let store = Store::open_in_memory().unwrap();
        store
            .import_notification_delivery(&key("v1"), "external-receipt", NOW)
            .unwrap();
        assert_eq!(
            store
                .enqueue_notification(&key("v1"), NOW, 0, false)
                .unwrap()
                .status,
            NotificationStatus::Delivered
        );
        store
            .enqueue_notification(&key("v2"), NOW, 0, true)
            .unwrap();
        let claim = store.claim_notification(&key("v2"), "a", NOW, 10).unwrap();
        let token = claim.claim_token.as_deref().unwrap();
        store
            .begin_notification_delivery(&key("v2"), token, NOW)
            .unwrap();
        store
            .finish_notification_delivery(
                &key("v2"),
                token,
                NOW,
                &DeliveryOutcome::Delivered {
                    receipt: "new-receipt".into(),
                },
            )
            .unwrap();
        assert_eq!(store.notification_outbox("7", 10).unwrap().len(), 2);
        assert!(store.notification_outbox("8", 10).unwrap().is_empty());
    }
}
