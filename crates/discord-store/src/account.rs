//! Persistent account inventory and resumable retrieval scheduling.

use super::{merge_coverage, persist_message, CoverageRange, Store, StoreError};
use discord_api::types::{Channel, Message};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;
use serde_json::Value;

/// One observed account-wide discovery generation, including incomplete sources.
#[derive(Debug, Clone, Serialize)]
pub struct AccountInventory {
    pub generation: String,
    pub account_user_id: String,
    pub started_at: String,
    pub refreshed_at: Option<String>,
    pub status: String,
    pub discovery: Value,
}

/// A discovered channel and its independently persisted retrieval checkpoint.
#[derive(Debug, Clone, Serialize)]
pub struct AccountTarget {
    pub account_user_id: String,
    pub channel_id: String,
    pub guild_id: Option<String>,
    pub parent_id: Option<String>,
    pub kind: i32,
    pub generation: String,
    pub first_discovered_at: String,
    pub last_seen_at: String,
    pub last_attempt_at: Option<String>,
    pub last_checked_at: Option<String>,
    pub next_check_at: Option<String>,
    pub backfill_before: Option<String>,
    pub increment_after: Option<String>,
    pub history_status: String,
    pub next_direction: String,
    pub last_error: Option<Value>,
}

/// Retrieval progress committed only after saving every message in a page.
pub struct TargetCheckpoint {
    pub backfill_before: Option<String>,
    pub increment_after: Option<String>,
    pub checked_at: String,
    pub next_check_at: Option<String>,
    pub history_status: String,
    pub next_direction: String,
    /// Inclusive interval established by a nonempty successful request and its cursor.
    pub page_interval: Option<(String, String)>,
}

/// Durable progress and cancellation for a bounded synchronization job.
#[derive(Debug, Clone, Serialize)]
pub struct AccountJob {
    pub job_id: String,
    pub account_user_id: String,
    pub generation: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub cancel_requested: bool,
    pub progress: Value,
}

/// Counts of retained targets within an explicit account, guild, and channel scope.
#[derive(Debug, Clone, Serialize)]
pub struct AccountTargetCounts {
    pub total: u64,
    pub synced: u64,
    pub unfetched: u64,
    pub failed: u64,
    pub blocked: u64,
    pub due: u64,
}

fn decode(value: String) -> rusqlite::Result<Value> {
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn target(row: &rusqlite::Row<'_>) -> rusqlite::Result<AccountTarget> {
    Ok(AccountTarget {
        account_user_id: row.get(0)?,
        channel_id: row.get(1)?,
        guild_id: row.get(2)?,
        parent_id: row.get(3)?,
        kind: row.get(4)?,
        generation: row.get(5)?,
        first_discovered_at: row.get(6)?,
        last_seen_at: row.get(7)?,
        last_attempt_at: row.get(8)?,
        last_checked_at: row.get(9)?,
        next_check_at: row.get(10)?,
        backfill_before: row.get(11)?,
        increment_after: row.get(12)?,
        history_status: row.get(13)?,
        last_error: row.get::<_, Option<String>>(14)?.map(decode).transpose()?,
        next_direction: row.get(15)?,
    })
}

const TARGET_COLUMNS: &str = "account_user_id,channel_id,guild_id,parent_id,kind,generation,first_discovered_at,last_seen_at,last_attempt_at,last_checked_at,next_check_at,backfill_before,increment_after,history_status,last_error_json,next_direction";

impl Store {
    /// Begin discovery without discarding previously observed targets.
    pub fn begin_account_inventory(
        &self,
        user: &str,
        generation: &str,
        at: &str,
    ) -> Result<(), StoreError> {
        self.conn.lock().expect("store lock").execute("INSERT INTO account_inventory(generation,account_user_id,started_at,status) VALUES(?1,?2,?3,'running')", params![generation,user,at])?;
        Ok(())
    }

    /// Persist discovered message-bearing channels while retaining checkpoints.
    pub fn observe_account_targets(
        &self,
        user: &str,
        generation: &str,
        channels: &[Channel],
        at: &str,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let tx = conn.unchecked_transaction()?;
        for channel in channels
            .iter()
            .filter(|c| matches!(c.kind, 0 | 1 | 2 | 3 | 5 | 10 | 11 | 12 | 13))
        {
            tx.execute("INSERT INTO account_targets(account_user_id,channel_id,guild_id,parent_id,kind,generation,first_discovered_at,last_seen_at,next_check_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?7,?7) ON CONFLICT(account_user_id,channel_id) DO UPDATE SET guild_id=excluded.guild_id,parent_id=excluded.parent_id,kind=excluded.kind,generation=excluded.generation,last_seen_at=excluded.last_seen_at",params![user,channel.id,channel.guild_id,channel.parent_id,channel.kind,generation,at])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Finish discovery with explicit evidence; incomplete sources remain visible.
    pub fn finish_account_inventory(
        &self,
        generation: &str,
        at: &str,
        report: &Value,
    ) -> Result<(), StoreError> {
        self.conn.lock().expect("store lock").execute("UPDATE account_inventory SET refreshed_at=?2,status='partial',discovery_json=?3 WHERE generation=?1",params![generation,at,report.to_string()])?;
        Ok(())
    }

    /// Return the latest generation observed for this account.
    pub fn account_inventory(&self, user: &str) -> Result<Option<AccountInventory>, StoreError> {
        Ok(self.conn.lock().expect("store lock").query_row("SELECT generation,account_user_id,started_at,refreshed_at,status,discovery_json FROM account_inventory WHERE account_user_id=?1 ORDER BY started_at DESC,rowid DESC LIMIT 1",[user],|r|Ok(AccountInventory{generation:r.get(0)?,account_user_id:r.get(1)?,started_at:r.get(2)?,refreshed_at:r.get(3)?,status:r.get(4)?,discovery:decode(r.get(5)?)?})).optional()?)
    }

    /// Return all retained targets, including blocked and previously seen targets.
    pub fn account_targets(&self, user: &str) -> Result<Vec<AccountTarget>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt=conn.prepare(&format!("SELECT {TARGET_COLUMNS} FROM account_targets WHERE account_user_id=?1 ORDER BY channel_id"))?;
        let targets = stmt
            .query_map([user], target)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(targets)
    }

    /// Aggregate retained retrieval evidence without loading target rows.
    pub fn account_target_counts(
        &self,
        user: &str,
        guild: Option<&str>,
        channel: Option<&str>,
        at: &str,
    ) -> Result<AccountTargetCounts, StoreError> {
        Ok(self.conn.lock().expect("store lock").query_row("SELECT COUNT(*),COALESCE(SUM(last_checked_at IS NOT NULL),0),COALESCE(SUM(last_checked_at IS NULL),0),COALESCE(SUM(last_error_json IS NOT NULL),0),COALESCE(SUM(history_status='blocked'),0),COALESCE(SUM(history_status!='blocked' AND (next_check_at IS NULL OR next_check_at<=?4)),0) FROM account_targets WHERE account_user_id=?1 AND (?2 IS NULL OR guild_id=?2) AND (?3 IS NULL OR channel_id=?3)",params![user,guild,channel,at],|row|Ok(AccountTargetCounts{total:row.get::<_,i64>(0)? as u64,synced:row.get::<_,i64>(1)? as u64,unfetched:row.get::<_,i64>(2)? as u64,failed:row.get::<_,i64>(3)? as u64,blocked:row.get::<_,i64>(4)? as u64,due:row.get::<_,i64>(5)? as u64}))?)
    }

    /// Read at most one bounded keyset page plus one continuation witness.
    pub fn account_target_page(
        &self,
        user: &str,
        guild: Option<&str>,
        channel: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<AccountTarget>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt=conn.prepare(&format!("SELECT {TARGET_COLUMNS} FROM account_targets WHERE account_user_id=?1 AND (?2 IS NULL OR guild_id=?2) AND (?3 IS NULL OR channel_id=?3) AND (?4 IS NULL OR channel_id>?4) ORDER BY channel_id LIMIT ?5"))?;
        let page = stmt
            .query_map(
                params![user, guild, channel, after, limit.clamp(1, 100) + 1],
                target,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(page)
    }

    /// Read a bounded coverage-range sample plus one truncation witness.
    pub fn account_target_ranges(
        &self,
        channel: &str,
        limit: u32,
    ) -> Result<Vec<CoverageRange>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt=conn.prepare("SELECT channel_id,from_id,to_id FROM coverage WHERE channel_id=?1 ORDER BY CAST(from_id AS INTEGER) LIMIT ?2")?;
        let ranges = stmt
            .query_map(params![channel, limit.clamp(1, 100) + 1], |row| {
                Ok(CoverageRange {
                    channel_id: row.get(0)?,
                    from_id: row.get(1)?,
                    to_id: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ranges)
    }

    /// Schedule untouched targets first, then oldest attempts; blocked targets need explicit intervention.
    pub fn due_account_targets(
        &self,
        user: &str,
        at: &str,
        limit: u32,
    ) -> Result<Vec<AccountTarget>, StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let mut stmt=conn.prepare(&format!("SELECT {TARGET_COLUMNS} FROM account_targets WHERE account_user_id=?1 AND history_status!='blocked' AND (next_check_at IS NULL OR next_check_at<=?2) ORDER BY last_attempt_at IS NOT NULL,attempt_sequence,channel_id LIMIT ?3"))?;
        let targets = stmt
            .query_map(params![user, at, limit], target)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(targets)
    }

    /// Rotate a target in the fair schedule before attempting network retrieval.
    pub fn record_account_attempt(
        &self,
        user: &str,
        channel: &str,
        at: &str,
    ) -> Result<(), StoreError> {
        self.conn.lock().expect("store lock").execute("UPDATE account_targets SET last_attempt_at=?3,attempt_sequence=(SELECT COALESCE(MAX(attempt_sequence),0)+1 FROM account_targets WHERE account_user_id=?1) WHERE account_user_id=?1 AND channel_id=?2",params![user,channel,at])?;
        Ok(())
    }

    /// Commit a saved page's resume state without declaring unknown history complete.
    pub fn advance_account_target(
        &self,
        user: &str,
        channel: &str,
        checkpoint: &TargetCheckpoint,
    ) -> Result<(), StoreError> {
        self.conn.lock().expect("store lock").execute("UPDATE account_targets SET backfill_before=?3,increment_after=?4,last_checked_at=?5,next_check_at=?6,history_status=?7,next_direction=?8,last_error_json=NULL WHERE account_user_id=?1 AND channel_id=?2",params![user,channel,checkpoint.backfill_before,checkpoint.increment_after,checkpoint.checked_at,checkpoint.next_check_at,checkpoint.history_status,checkpoint.next_direction])?;
        Ok(())
    }

    /// Atomically save messages, retained coverage, and the account resume checkpoint.
    pub fn save_account_page(
        &self,
        user: &str,
        channel: &str,
        messages: &[Message],
        checkpoint: &TargetCheckpoint,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store lock");
        let tx = conn.unchecked_transaction()?;
        for message in messages {
            if message.channel_id != channel {
                return Err(StoreError::Data("account page channel mismatch".into()));
            }
            persist_message(&tx, message, &checkpoint.checked_at)?;
        }
        if let (Some(oldest), Some(newest)) = (messages.last(), messages.first()) {
            let (lower, upper) = checkpoint
                .page_interval
                .as_ref()
                .map(|(lower, upper)| (lower.as_str(), upper.as_str()))
                .unwrap_or((&oldest.id, &newest.id));
            let low = lower
                .parse::<u64>()
                .map_err(|_| StoreError::Data("invalid account page lower boundary".into()))?;
            let high = upper
                .parse::<u64>()
                .map_err(|_| StoreError::Data("invalid account page upper boundary".into()))?;
            if u128::from(low) > super::snowflake_num(&oldest.id)
                || u128::from(high) < super::snowflake_num(&newest.id)
                || low > high
            {
                return Err(StoreError::Data(
                    "account page interval does not contain observed messages".into(),
                ));
            }
            tx.execute("INSERT INTO coverage(channel_id,from_id,to_id) VALUES(?1,?2,?3) ON CONFLICT(channel_id,from_id) DO UPDATE SET to_id=MAX(CAST(coverage.to_id AS INTEGER),CAST(excluded.to_id AS INTEGER))",params![channel,lower,upper])?;
            merge_coverage(&tx, channel)?;
            tx.execute("INSERT INTO channel_sync(channel_id,last_synced_message_id,oldest_synced_message_id,last_fetched_at,backfill) VALUES(?1,?2,?3,?4,'partial') ON CONFLICT(channel_id) DO UPDATE SET last_synced_message_id=CASE WHEN channel_sync.last_synced_message_id IS NULL OR CAST(excluded.last_synced_message_id AS INTEGER)>CAST(channel_sync.last_synced_message_id AS INTEGER) THEN excluded.last_synced_message_id ELSE channel_sync.last_synced_message_id END, oldest_synced_message_id=CASE WHEN channel_sync.oldest_synced_message_id IS NULL OR CAST(excluded.oldest_synced_message_id AS INTEGER)<CAST(channel_sync.oldest_synced_message_id AS INTEGER) THEN excluded.oldest_synced_message_id ELSE channel_sync.oldest_synced_message_id END,last_fetched_at=excluded.last_fetched_at",params![channel,newest.id,oldest.id,checkpoint.checked_at])?;
        }
        let changed=tx.execute("UPDATE account_targets SET backfill_before=?3,increment_after=?4,last_checked_at=?5,next_check_at=?6,history_status=?7,next_direction=?8,last_error_json=NULL WHERE account_user_id=?1 AND channel_id=?2",params![user,channel,checkpoint.backfill_before,checkpoint.increment_after,checkpoint.checked_at,checkpoint.next_check_at,checkpoint.history_status,checkpoint.next_direction])?;
        if changed != 1 {
            return Err(StoreError::Data("account page target missing".into()));
        }
        tx.commit()?;
        Ok(())
    }

    /// Suspend denied targets; schedule other failures without changing their retrieval lane.
    pub fn fail_account_target(
        &self,
        user: &str,
        channel: &str,
        at: &str,
        error: &Value,
    ) -> Result<(), StoreError> {
        let blocked = matches!(error["http_status"].as_u64(), Some(403 | 404))
            || matches!(
                error["code"].as_str(),
                Some("forbidden" | "not_found" | "bot_only")
            );
        let delay_seconds = error["retry_after_ms"]
            .as_u64()
            .unwrap_or(0)
            .saturating_add(999)
            .saturating_div(1000)
            .max(300);
        let conn = self.conn.lock().expect("store lock");
        let next_check: Option<String> = conn.query_row(
            "SELECT strftime('%Y-%m-%dT%H:%M:%SZ',?1,?2)",
            params![at, format!("+{delay_seconds} seconds")],
            |row| row.get(0),
        )?;
        let next_check =
            next_check.ok_or_else(|| StoreError::Data("invalid account retry deadline".into()))?;
        conn.execute("UPDATE account_targets SET last_attempt_at=?3,last_error_json=?4,history_status=CASE WHEN ?5 THEN 'blocked' ELSE history_status END,next_check_at=CASE WHEN ?5 THEN next_check_at ELSE ?6 END WHERE account_user_id=?1 AND channel_id=?2",params![user,channel,at,error.to_string(),blocked,next_check])?;
        Ok(())
    }

    /// Save progress while preserving cancellation requested by another task.
    pub fn save_account_job(&self, job: &AccountJob) -> Result<(), StoreError> {
        self.conn.lock().expect("store lock").execute("INSERT INTO account_sync_jobs(job_id,account_user_id,generation,status,created_at,updated_at,cancel_requested,progress_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(job_id) DO UPDATE SET generation=excluded.generation,status=excluded.status,updated_at=excluded.updated_at,cancel_requested=MAX(account_sync_jobs.cancel_requested,excluded.cancel_requested),progress_json=excluded.progress_json",params![job.job_id,job.account_user_id,job.generation,job.status,job.created_at,job.updated_at,job.cancel_requested,job.progress.to_string()])?;
        Ok(())
    }

    /// Read durable progress, including cancellation and interruption state.
    pub fn account_job(&self, id: &str) -> Result<Option<AccountJob>, StoreError> {
        Ok(self.conn.lock().expect("store lock").query_row("SELECT job_id,account_user_id,generation,status,created_at,updated_at,cancel_requested,progress_json FROM account_sync_jobs WHERE job_id=?1",[id],|r|Ok(AccountJob{job_id:r.get(0)?,account_user_id:r.get(1)?,generation:r.get(2)?,status:r.get(3)?,created_at:r.get(4)?,updated_at:r.get(5)?,cancel_requested:r.get(6)?,progress:decode(r.get(7)?)?})).optional()?)
    }

    /// Request cancellation without discarding successful page checkpoints.
    pub fn request_account_cancel(&self, id: &str, at: &str) -> Result<bool, StoreError> {
        Ok(self.conn.lock().expect("store lock").execute("UPDATE account_sync_jobs SET cancel_requested=1,updated_at=?2 WHERE job_id=?1 AND status IN ('pending','running')",params![id,at])?>0)
    }

    /// Observe the persistent cancellation flag.
    pub fn account_cancel_requested(&self, id: &str) -> Result<bool, StoreError> {
        Ok(self.account_job(id)?.is_some_and(|j| j.cancel_requested))
    }

    /// Mark jobs interrupted at process restart; their target cursors remain resumable.
    pub fn interrupt_account_jobs(&self, at: &str) -> Result<usize, StoreError> {
        Ok(self.conn.lock().expect("store lock").execute("UPDATE account_sync_jobs SET status='interrupted',updated_at=?1 WHERE status IN ('pending','running')",[at])?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::Store;
    use discord_api::types::Channel;

    fn channel(id: &str) -> Channel {
        serde_json::from_value(serde_json::json!({"id":id,"type":0,"guild_id":"42"})).unwrap()
    }

    #[test]
    fn untouched_targets_precede_attempted_targets_and_blocked_targets_stay_blocked() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("me", "g", "2026-01-01")
            .unwrap();
        store
            .observe_account_targets("me", "g", &[channel("1"), channel("2")], "2026-01-01")
            .unwrap();
        assert_eq!(
            store.account_targets("me").unwrap()[0]
                .next_check_at
                .as_deref(),
            Some("2026-01-01")
        );
        store
            .record_account_attempt("me", "1", "2026-01-02")
            .unwrap();
        assert_eq!(
            store.due_account_targets("me", "2026-01-03", 1).unwrap()[0].channel_id,
            "2"
        );
        store
            .fail_account_target(
                "me",
                "2",
                "2026-01-03",
                &serde_json::json!({"retryable":false,"code":"forbidden"}),
            )
            .unwrap();
        store
            .observe_account_targets("me", "g", &[channel("2")], "2026-01-04")
            .unwrap();
        assert_eq!(
            store
                .due_account_targets("me", "2026-01-05", 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn repeated_attempts_with_identical_clock_values_still_rotate() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("me", "g", "2026-01-01")
            .unwrap();
        store
            .observe_account_targets("me", "g", &[channel("1"), channel("2")], "2026-01-01")
            .unwrap();
        for id in ["1", "2", "1"] {
            store
                .record_account_attempt("me", id, "2026-01-02")
                .unwrap();
        }
        assert_eq!(
            store.due_account_targets("me", "2026-01-03", 1).unwrap()[0].channel_id,
            "2"
        );
    }

    #[test]
    fn non_denial_failures_preserve_lane_and_respect_retry_delay() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("me", "g", "2026-01-01T00:00:00Z")
            .unwrap();
        store
            .observe_account_targets("me", "g", &[channel("1")], "2026-01-01T00:00:00Z")
            .unwrap();
        store.fail_account_target("me","1","2026-01-01T00:00:00Z",&serde_json::json!({"code":"invalid_response","retryable":false,"retry_after_ms":1200000})).unwrap();
        let target = &store.account_targets("me").unwrap()[0];
        assert_eq!(target.history_status, "unfetched");
        assert_eq!(target.next_direction, "backfill");
        assert_eq!(
            target.next_check_at.as_deref(),
            Some("2026-01-01T00:20:00Z")
        );
        assert!(store
            .due_account_targets("me", "2026-01-01T00:19:00Z", 10)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn proven_request_boundaries_bridge_snowflake_pages() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("me", "g", "2026-01-01")
            .unwrap();
        store
            .observe_account_targets("me", "g", &[channel("1")], "2026-01-01")
            .unwrap();
        let mut checkpoint = TargetCheckpoint {
            backfill_before: Some("100000".into()),
            increment_after: Some("100000".into()),
            checked_at: "2026-01-02".into(),
            next_check_at: None,
            history_status: "partial".into(),
            next_direction: "backfill".into(),
            page_interval: None,
        };
        let message = |id: &str| {
            serde_json::from_value(serde_json::json!({"id":id,"channel_id":"1","timestamp":"2026-01-01T00:00:00Z","content":"test"})).unwrap()
        };
        store
            .save_account_page("me", "1", &[message("100000")], &checkpoint)
            .unwrap();
        checkpoint.page_interval = Some(("50000".into(), "99999".into()));
        checkpoint.backfill_before = Some("50000".into());
        store
            .save_account_page("me", "1", &[message("50000")], &checkpoint)
            .unwrap();
        checkpoint.page_interval = Some(("100001".into(), "150000".into()));
        checkpoint.increment_after = Some("150000".into());
        checkpoint.next_direction = "backfill".into();
        store
            .save_account_page("me", "1", &[message("150000")], &checkpoint)
            .unwrap();
        let ranges = store.coverage("1").unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].from_id, "50000");
        assert_eq!(ranges[0].to_id, "150000");
        checkpoint.page_interval = None;
        store
            .save_account_page("me", "1", &[], &checkpoint)
            .unwrap();
        assert_eq!(store.coverage("1").unwrap(), ranges);
    }

    #[test]
    fn aggregate_and_keyset_pages_handle_large_inventory_without_scope_leaks() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("me", "g", "2026-01-01")
            .unwrap();
        let channels: Vec<_> = (1..=13000).map(|id| channel(&id.to_string())).collect();
        store
            .observe_account_targets("me", "g", &channels, "2026-01-01")
            .unwrap();
        store
            .observe_account_targets("other", "g", &[channel("99999")], "2026-01-01")
            .unwrap();
        let counts = store
            .account_target_counts("me", Some("42"), None, "2026-01-02")
            .unwrap();
        assert_eq!(counts.total, 13000);
        assert_eq!(counts.unfetched, 13000);
        assert_eq!(
            store
                .account_target_counts("me", Some("43"), None, "2026-01-02")
                .unwrap()
                .total,
            0
        );
        let mut cursor = None;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = store
                .account_target_page("me", Some("42"), None, cursor.as_deref(), 100)
                .unwrap();
            assert!(page.len() <= 101);
            let has_more = page.len() > 100;
            for target in page.iter().take(100) {
                assert!(seen.insert(target.channel_id.clone()));
                cursor = Some(target.channel_id.clone());
            }
            if !has_more {
                break;
            }
        }
        assert_eq!(seen.len(), 13000);
        assert!(!seen.contains("99999"));
    }

    #[test]
    fn page_storage_failure_rolls_back_messages_and_checkpoint_together() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("me", "g", "2026-01-01")
            .unwrap();
        store
            .observe_account_targets("me", "g", &[channel("1")], "2026-01-01")
            .unwrap();
        store.conn.lock().unwrap().execute_batch("CREATE TRIGGER fail_coverage BEFORE INSERT ON coverage BEGIN SELECT RAISE(ABORT,'coverage failure'); END;").unwrap();
        let message:Message=serde_json::from_value(serde_json::json!({"id":"10","channel_id":"1","timestamp":"2026-01-01T00:00:00Z","content":"test"})).unwrap();
        let checkpoint = TargetCheckpoint {
            backfill_before: Some("10".into()),
            increment_after: Some("10".into()),
            checked_at: "2026-01-02".into(),
            next_check_at: None,
            history_status: "partial".into(),
            next_direction: "backfill".into(),
            page_interval: None,
        };
        assert!(store
            .save_account_page("me", "1", &[message], &checkpoint)
            .is_err());
        assert!(store.get_message("10").unwrap().is_none());
        assert!(store.account_targets("me").unwrap()[0]
            .backfill_before
            .is_none());
    }

    #[test]
    fn cancellation_and_checkpoints_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");
        {
            let store = Store::open(&path).unwrap();
            store
                .begin_account_inventory("me", "g", "2026-01-01")
                .unwrap();
            store
                .observe_account_targets("me", "g", &[channel("1")], "2026-01-01")
                .unwrap();
            store
                .advance_account_target(
                    "me",
                    "1",
                    &TargetCheckpoint {
                        backfill_before: Some("10".into()),
                        increment_after: Some("20".into()),
                        checked_at: "2026-01-02".into(),
                        next_check_at: None,
                        history_status: "partial".into(),
                        next_direction: "incremental".into(),
                        page_interval: None,
                    },
                )
                .unwrap();
            store
                .save_account_job(&AccountJob {
                    job_id: "j".into(),
                    account_user_id: "me".into(),
                    generation: "g".into(),
                    status: "running".into(),
                    created_at: "2026-01-01".into(),
                    updated_at: "2026-01-01".into(),
                    cancel_requested: false,
                    progress: serde_json::json!({}),
                })
                .unwrap();
            assert!(store.request_account_cancel("j", "2026-01-02").unwrap());
        }
        let store = Store::open(&path).unwrap();
        assert!(store.account_cancel_requested("j").unwrap());
        assert_eq!(
            store.account_targets("me").unwrap()[0].next_direction,
            "incremental"
        );
        assert_eq!(
            store.account_targets("me").unwrap()[0]
                .backfill_before
                .as_deref(),
            Some("10")
        );
    }
}
