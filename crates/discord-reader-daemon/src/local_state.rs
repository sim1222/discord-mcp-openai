//! Local workflow, notification claims, and caller-reported coverage gaps.

use crate::rpc::RpcError;
use discord_store::{
    sqlite::{
        account::AccountGapOutcome,
        outbox::{DeliveryOutcome, NotificationKey, NotificationStatus, OutboxError},
        workflow::{FieldPatch, InboxWorkflowPatch, WorkflowUpdateError},
    },
    Store, StoreError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

fn present<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InboxItemParams {
    pub channel_id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InboxPatchParams {
    pub channel_id: String,
    pub message_id: String,
    pub expected_version: u64,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub notified_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub snoozed_until: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub action_required: Option<Option<bool>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub action_evidence: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub completed_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub completion_evidence: Option<Option<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GapListParams {
    pub guild_id: Option<String>,
    pub channel_id: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WithheldStatus {
    Denied,
    Cancelled,
    Excluded,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GapRecordParams {
    pub channel_id: String,
    pub status: WithheldStatus,
    pub reason: String,
    pub observed_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterialKey {
    pub channel_id: String,
    pub message_id: String,
    pub material_version: String,
    pub notification_kind: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum NotificationParams {
    List {
        limit: Option<u32>,
        status: Option<NotificationStatus>,
        cursor: Option<String>,
    },
    Enqueue {
        key: MaterialKey,
        available_at: String,
        expected_workflow_version: u64,
        #[serde(default)]
        renotify_material_change: bool,
    },
    Revalidate {
        key: MaterialKey,
        expected_entry_version: u64,
        expected_workflow_version: u64,
        available_at: String,
    },
    Claim {
        key: MaterialKey,
        worker_id: String,
        lease_seconds: u32,
    },
    BeginDelivery {
        key: MaterialKey,
        claim_token: String,
    },
    Finish {
        key: MaterialKey,
        claim_token: String,
        outcome: DeliveryOutcome,
    },
    Import {
        key: MaterialKey,
        receipt: String,
        observed_at: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn patch_json_preserves_absent_null_and_value_through_serialization() {
        let raw = json!({"channel_id":"2","message_id":"3","expected_version":0,"action_required":true,"snoozed_until":null});
        let params: InboxPatchParams = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(params.action_required, Some(Some(true)));
        assert_eq!(params.snoozed_until, Some(None));
        assert_eq!(params.notified_at, None);
        assert_eq!(serde_json::to_value(params).unwrap(), raw);
        assert!(serde_json::from_value::<InboxPatchParams>(
            json!({"channel_id":"2","message_id":"3","expected_version":0,"typo":true})
        )
        .is_err());
    }
    #[test]
    fn workflow_handlers_preserve_fields_and_reject_stale_cas() {
        let store = Store::open_in_memory().unwrap();
        let params=serde_json::from_value(json!({"channel_id":"2","message_id":"3","expected_version":0,"action_required":true,"action_evidence":"explicit request"})).unwrap();
        assert_eq!(
            patch_inbox_state(&store, "1", params).unwrap()["state"]["version"],
            1
        );
        let params = serde_json::from_value(
            json!({"channel_id":"2","message_id":"3","expected_version":1,"snoozed_until":null}),
        )
        .unwrap();
        patch_inbox_state(&store, "1", params).unwrap();
        let state = get_inbox_state(
            &store,
            "1",
            InboxItemParams {
                channel_id: "2".into(),
                message_id: "3".into(),
            },
        )
        .unwrap();
        assert_eq!(state["state"]["workflow"]["action_required"], true);
        let params =
            serde_json::from_value(json!({"channel_id":"2","message_id":"3","expected_version":1}))
                .unwrap();
        assert_eq!(
            patch_inbox_state(&store, "1", params)
                .unwrap_err()
                .data
                .unwrap()["error"]["code"],
            "CONFLICT"
        );
    }
    #[test]
    fn unknown_notification_control_and_nested_keys_are_rejected() {
        assert!(serde_json::from_value::<NotificationParams>(
            json!({"operation":"list","dry_run":true})
        )
        .is_err());
        assert!(serde_json::from_value::<NotificationParams>(json!({"operation":"enqueue","key":{"channel_id":"2","message_id":"3","material_version":"v1","notification_kind":"notice","account_user_id":"9"},"available_at":"2026-01-01T00:00:00Z","expected_workflow_version":0})).is_err());
    }
}

fn validate_id(id: &str) -> Result<(), RpcError> {
    if id.parse::<u64>().ok().filter(|id| *id > 0).is_none() {
        return Err(RpcError::invalid_params(
            "IDs must be positive numeric strings",
        ));
    }
    Ok(())
}
fn validate_text(value: &str) -> Result<(), RpcError> {
    if value.trim().is_empty() || value.len() > 1024 {
        Err(RpcError::invalid_params("text must contain 1..1024 bytes"))
    } else {
        Ok(())
    }
}
fn timestamp(value: &str) -> Result<(), RpcError> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|_| ())
        .map_err(|_| RpcError::invalid_params("timestamp must be RFC3339"))
}
fn cache_error(error: StoreError, operation: &str) -> RpcError {
    tracing::error!(
        code = error.code(),
        operation,
        "local state cache operation failed"
    );
    RpcError::structured(
        crate::rpc::CACHE_ERROR,
        json!({"error":{"code":error.code(),"error_source":"cache","layer":"storage","operation":operation,"retryable":error.retryable(),"message":"local state cache operation failed; inspect daemon database diagnostics"}}),
    )
}
fn conflict(operation: &str, code: &str) -> RpcError {
    RpcError::structured(
        -32006,
        json!({"error":{"code":code,"error_source":"local_state","layer":"storage","operation":operation,"retryable":false,"message":"local state conflict; inspect latest workflow or claim"}}),
    )
}
fn patch<T>(value: Option<Option<T>>) -> FieldPatch<T> {
    match value {
        None => FieldPatch::Keep,
        Some(None) => FieldPatch::Clear,
        Some(Some(value)) => FieldPatch::Set(value),
    }
}

pub(crate) fn get_inbox_state(
    store: &Store,
    user: &str,
    params: InboxItemParams,
) -> Result<Value, RpcError> {
    validate_id(&params.channel_id)?;
    validate_id(&params.message_id)?;
    let state = store
        .inbox_workflow_revision(user, &params.channel_id, &params.message_id)
        .map_err(|e| cache_error(e, "get_inbox_state"))?;
    Ok(
        json!({"account_user_id":user,"state":state,"version":state.as_ref().map_or(0,|state|state.version),"source":"local_workflow","observed_at":discord_store::sqlite::now_iso()}),
    )
}
pub(crate) fn patch_inbox_state(
    store: &Store,
    user: &str,
    params: InboxPatchParams,
) -> Result<Value, RpcError> {
    validate_id(&params.channel_id)?;
    validate_id(&params.message_id)?;
    for value in [
        &params.notified_at,
        &params.snoozed_until,
        &params.completed_at,
    ]
    .into_iter()
    .flatten()
    .flatten()
    {
        timestamp(value)?;
    }
    for value in [&params.action_evidence, &params.completion_evidence]
        .into_iter()
        .flatten()
        .flatten()
    {
        validate_text(value)?;
    }
    let patch = InboxWorkflowPatch {
        expected_version: params.expected_version,
        notified_at: patch(params.notified_at),
        snoozed_until: patch(params.snoozed_until),
        action_required: patch(params.action_required),
        action_evidence: patch(params.action_evidence),
        completed_at: patch(params.completed_at),
        completion_evidence: patch(params.completion_evidence),
    };
    let state=store.patch_inbox_workflow(user,&params.channel_id,&params.message_id,&patch).map_err(|e|match e{WorkflowUpdateError::Conflict{expected,current}=>RpcError::structured(-32006,json!({"error":{"code":"CONFLICT","error_source":"local_state","operation":"patch_inbox_state","retryable":false,"expected_version":expected,"current_version":current}})),WorkflowUpdateError::Store(e)=>cache_error(e,"patch_inbox_state")})?;
    Ok(json!({"state":state,"source":"local_workflow","discord_modified":false}))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GapCursor {
    account: String,
    guild: Option<String>,
    channel: Option<String>,
    after: u64,
}
pub(crate) fn get_account_gaps(
    store: &Store,
    user: &str,
    params: GapListParams,
) -> Result<Value, RpcError> {
    for id in [&params.guild_id, &params.channel_id].into_iter().flatten() {
        validate_id(id)?;
    }
    let limit = params.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(RpcError::invalid_params("limit must be 1..100"));
    }
    let after = if let Some(raw) = params.cursor.as_deref() {
        let parsed: GapCursor = raw
            .strip_prefix("gaps-v1:")
            .filter(|_| raw.len() <= 1024)
            .and_then(|raw| serde_json::from_str(raw).ok())
            .ok_or_else(|| RpcError::invalid_params("invalid gap cursor"))?;
        if parsed.account != user
            || parsed.guild != params.guild_id
            || parsed.channel != params.channel_id
        {
            return Err(RpcError::invalid_params("gap cursor scope mismatch"));
        }
        if parsed.after == 0 || i64::try_from(parsed.after).is_err() {
            return Err(RpcError::invalid_params("invalid gap cursor position"));
        }
        Some(parsed.after)
    } else {
        None
    };
    let (mut rows, (total, unresolved)) = (
        store
            .account_gap_page(
                user,
                params.guild_id.as_deref(),
                params.channel_id.as_deref(),
                after,
                limit,
            )
            .map_err(|e| cache_error(e, "get_account_gaps"))?,
        store
            .account_gap_counts(
                user,
                params.guild_id.as_deref(),
                params.channel_id.as_deref(),
            )
            .map_err(|e| cache_error(e, "get_account_gaps"))?,
    );
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if has_more {
        rows.last().map(|row|format!("gaps-v1:{}",json!({"account":user,"guild":params.guild_id,"channel":params.channel_id,"after":row.gap_id})))
    } else {
        None
    };
    Ok(
        json!({"account_user_id":user,"target_scope":{"guild_id":params.guild_id,"channel_id":params.channel_id},"gaps":rows,"total":total,"unresolved":unresolved,"has_more":has_more,"next_cursor":next_cursor,"limit":limit,"complete":false,"source":"persisted_observation_evidence","pagination_mode":"live_keyset","observed_at":discord_store::sqlite::now_iso(),"implicit_unfetched_and_history_gaps":"see get_account_coverage"}),
    )
}
pub(crate) fn record_account_gap(
    store: &Store,
    user: &str,
    params: GapRecordParams,
) -> Result<Value, RpcError> {
    validate_id(&params.channel_id)?;
    validate_text(&params.reason)?;
    timestamp(&params.observed_at)?;
    let status = match params.status {
        WithheldStatus::Denied => "denied",
        WithheldStatus::Cancelled => "cancelled",
        WithheldStatus::Excluded => "excluded",
    };
    store.record_account_gap(&AccountGapOutcome{account_user_id:user,channel_id:&params.channel_id,layer:"orchestration_approval",status,reason:&params.reason,observed_at:&params.observed_at,evidence:&json!({"source":"caller_reported","http_status":null,"discord_code":null,"cancel_actor":"unknown","retry_class":"user_confirmation_required","requires_user_action":true})}).map_err(|e|cache_error(e,"record_account_gap"))?;
    Ok(
        json!({"recorded":true,"source":"caller_reported","layer":"orchestration_approval","status":status,"http_status":null,"discord_code":null,"discord_modified":false,"resumption":"not_automatically_authorized"}),
    )
}
fn notification_key(user: &str, key: MaterialKey) -> Result<NotificationKey, RpcError> {
    validate_id(&key.channel_id)?;
    validate_id(&key.message_id)?;
    validate_text(&key.material_version)?;
    validate_text(&key.notification_kind)?;
    Ok(NotificationKey {
        account_user_id: user.into(),
        channel_id: key.channel_id,
        message_id: key.message_id,
        material_version: key.material_version,
        notification_kind: key.notification_kind,
    })
}
fn outbox_error(error: OutboxError) -> RpcError {
    match error {
        OutboxError::Store(e) => cache_error(e, "notification_outbox"),
        OutboxError::NotFound => conflict("notification_outbox", "NOT_FOUND"),
        OutboxError::Conflict => conflict("notification_outbox", "CONFLICT"),
        OutboxError::Ineligible => conflict("notification_outbox", "WORKFLOW_INELIGIBLE"),
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboxCursor {
    account: String,
    status: Option<NotificationStatus>,
    after: i64,
}
pub(crate) fn notification_outbox(
    store: &Store,
    user: &str,
    params: NotificationParams,
) -> Result<Value, RpcError> {
    let now = discord_store::sqlite::now_iso();
    let entry=match params {
        NotificationParams::List{limit,status,cursor}=>{
            let limit=limit.unwrap_or(50);if !(1..=100).contains(&limit){return Err(RpcError::invalid_params("limit must be 1..100"));}
            let after=if let Some(raw)=cursor {let parsed:OutboxCursor=raw.strip_prefix("outbox-v1:").filter(|_|raw.len()<=1024).and_then(|raw|serde_json::from_str(raw).ok()).ok_or_else(||RpcError::invalid_params("invalid outbox cursor"))?;if parsed.account!=user||parsed.status!=status{return Err(RpcError::invalid_params("outbox cursor scope mismatch"));}
            if parsed.after<=0{return Err(RpcError::invalid_params("invalid outbox cursor position"));}Some(parsed.after)}else{None};
            let page=store.notification_outbox_page(user,after,limit,status.clone()).map_err(|e|cache_error(e,"notification_outbox"))?;
            let next_cursor=page.next_after_rowid.map(|after|format!("outbox-v1:{}",json!({"account":user,"status":status,"after":after})));
            return Ok(json!({"entries":page.entries,"has_more":page.has_more,"next_cursor":next_cursor,"filter":{"status":status,"default":"unresolved"},"limit":limit,"source":"local_outbox","pagination_mode":"live_keyset","complete":false,"discord_modified":false}));
        }
        NotificationParams::Enqueue{key,available_at,expected_workflow_version,renotify_material_change}=>{timestamp(&available_at)?;store.enqueue_notification(&notification_key(user,key)?,&available_at,expected_workflow_version,renotify_material_change)}
        NotificationParams::Revalidate{key,expected_entry_version,expected_workflow_version,available_at}=>{timestamp(&available_at)?;if expected_entry_version==0{return Err(RpcError::invalid_params("expected_entry_version must be positive"));}store.revalidate_notification(&notification_key(user,key)?,expected_entry_version,expected_workflow_version,&available_at)}
        NotificationParams::Claim{key,worker_id,lease_seconds}=>{validate_text(&worker_id)?;if !(1..=3600).contains(&lease_seconds){return Err(RpcError::invalid_params("lease_seconds must be 1..3600"));}store.claim_notification(&notification_key(user,key)?,&worker_id,&now,lease_seconds)}
        NotificationParams::BeginDelivery{key,claim_token}=>{validate_text(&claim_token)?;store.begin_notification_delivery(&notification_key(user,key)?,&claim_token,&now)}
        NotificationParams::Finish{key,claim_token,outcome}=>{validate_text(&claim_token)?;match &outcome{DeliveryOutcome::Delivered{receipt}=>validate_text(receipt)?,DeliveryOutcome::AmbiguousDelivery{reason}|DeliveryOutcome::NotSent{reason}=>validate_text(reason)?};store.finish_notification_delivery(&notification_key(user,key)?,&claim_token,&now,&outcome)}
        NotificationParams::Import{key,receipt,observed_at}=>{validate_text(&receipt)?;timestamp(&observed_at)?;store.import_notification_delivery(&notification_key(user,key)?,&receipt,&observed_at)}
    }.map_err(outbox_error)?;
    Ok(
        json!({"entry":entry,"source":"local_outbox","discord_modified":false,"external_send_performed":false}),
    )
}

#[cfg(test)]
mod handler_regressions {
    use super::*;
    #[test]
    fn queued_revalidation_handler_keeps_material_key_after_workflow_update() {
        let store = Store::open_in_memory().unwrap();
        patch_inbox_state(&store,"1",serde_json::from_value(json!({"channel_id":"2","message_id":"3","expected_version":0,"snoozed_until":"2026-10-14T10:00:00Z"})).unwrap()).unwrap();
        let key = json!({"channel_id":"2","message_id":"3","material_version":"v1","notification_kind":"notice"});
        let queued=notification_outbox(&store,"1",serde_json::from_value(json!({"operation":"enqueue","key":key,"available_at":"2026-01-01T00:00:00Z","expected_workflow_version":1})).unwrap()).unwrap();
        patch_inbox_state(&store,"1",serde_json::from_value(json!({"channel_id":"2","message_id":"3","expected_version":1,"snoozed_until":null})).unwrap()).unwrap();
        let request = json!({"operation":"revalidate","key":key,"expected_entry_version":queued["entry"]["revision"],"expected_workflow_version":2,"available_at":"2026-01-01T00:00:00Z"});
        let updated = notification_outbox(
            &store,
            "1",
            serde_json::from_value(request.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(updated["entry"]["expected_workflow_version"], 2);
        assert_eq!(
            notification_outbox(&store, "1", serde_json::from_value(request).unwrap())
                .unwrap_err()
                .data
                .unwrap()["error"]["code"],
            "CONFLICT"
        );
    }

    #[test]
    fn outbox_continuation_is_bound_to_account_and_status_filter() {
        let store = Store::open_in_memory().unwrap();
        for id in ["3", "4"] {
            notification_outbox(&store,"1",serde_json::from_value(json!({"operation":"enqueue","key":{"channel_id":"2","message_id":id,"material_version":"v1","notification_kind":"notice"},"available_at":"2026-01-01T00:00:00Z","expected_workflow_version":0})).unwrap()).unwrap();
        }
        let first = notification_outbox(
            &store,
            "1",
            NotificationParams::List {
                limit: Some(1),
                status: None,
                cursor: None,
            },
        )
        .unwrap();
        let cursor = first["next_cursor"].as_str().unwrap().to_string();
        assert!(notification_outbox(
            &store,
            "1",
            NotificationParams::List {
                limit: Some(1),
                status: Some(NotificationStatus::Delivered),
                cursor: Some(cursor.clone())
            }
        )
        .is_err());
        assert!(notification_outbox(
            &store,
            "9",
            NotificationParams::List {
                limit: Some(1),
                status: None,
                cursor: Some(cursor.clone())
            }
        )
        .is_err());
        let next = notification_outbox(
            &store,
            "1",
            NotificationParams::List {
                limit: Some(1),
                status: None,
                cursor: Some(cursor),
            },
        )
        .unwrap();
        assert_eq!(next["has_more"], false);
        assert_ne!(
            first["entries"][0]["key"]["message_id"],
            next["entries"][0]["key"]["message_id"]
        );
    }

    #[test]
    fn outbox_handler_preserves_dedup_and_claim_conflict_without_sending() {
        let store = Store::open_in_memory().unwrap();
        let key = json!({"channel_id":"2","message_id":"3","material_version":"v1","notification_kind":"deadline"});
        let enqueue = json!({"operation":"enqueue","key":key,"available_at":"2026-01-01T00:00:00Z","expected_workflow_version":0});
        notification_outbox(
            &store,
            "1",
            serde_json::from_value(enqueue.clone()).unwrap(),
        )
        .unwrap();
        notification_outbox(&store, "1", serde_json::from_value(enqueue).unwrap()).unwrap();
        let claim =
            json!({"operation":"claim","key":key,"worker_id":"worker-a","lease_seconds":60});
        let response =
            notification_outbox(&store, "1", serde_json::from_value(claim.clone()).unwrap())
                .unwrap();
        let token = response["entry"]["claim_token"].as_str().unwrap();
        assert_ne!(token, "worker-a");
        assert_eq!(
            notification_outbox(&store, "1", serde_json::from_value(claim).unwrap())
                .unwrap_err()
                .data
                .unwrap()["error"]["code"],
            "CONFLICT"
        );
        let result = notification_outbox(
            &store,
            "1",
            serde_json::from_value(
                json!({"operation":"begin_delivery","key":key,"claim_token":token}),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(result["external_send_performed"], false);
        let result=notification_outbox(&store,"1",serde_json::from_value(json!({"operation":"finish","key":key,"claim_token":token,"outcome":{"status":"ambiguous_delivery","reason":"worker cannot verify send"}})).unwrap()).unwrap();
        assert_eq!(result["entry"]["status"], "ambiguous_delivery");
        let result = notification_outbox(
            &store,
            "1",
            NotificationParams::List {
                limit: Some(50),
                status: None,
                cursor: None,
            },
        )
        .unwrap();
        assert_eq!(result["entries"].as_array().unwrap().len(), 1);
    }
    #[test]
    fn caller_reported_gap_has_no_discord_error_and_cursor_is_scope_bound() {
        let store = Store::open_in_memory().unwrap();
        store
            .begin_account_inventory("1", "generation", "2026-01-01")
            .unwrap();
        let channel = serde_json::from_value(json!({"id":"2","type":0,"guild_id":"4"})).unwrap();
        store
            .observe_account_targets("1", "generation", &[channel], "2026-01-01")
            .unwrap();
        for _ in 0..2 {
            record_account_gap(
                &store,
                "1",
                GapRecordParams {
                    channel_id: "2".into(),
                    status: WithheldStatus::Denied,
                    reason: "caller tool approval denied".into(),
                    observed_at: "2026-01-01T00:00:00Z".into(),
                },
            )
            .unwrap();
        }
        let result = get_account_gaps(
            &store,
            "1",
            GapListParams {
                guild_id: Some("4".into()),
                limit: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(result["gaps"][0]["evidence"]["http_status"].is_null());
        assert_eq!(result["gaps"][0]["evidence"]["source"], "caller_reported");
        let cursor = result["next_cursor"].as_str().unwrap().to_string();
        assert!(get_account_gaps(
            &store,
            "1",
            GapListParams {
                guild_id: Some("5".into()),
                cursor: Some(cursor.clone()),
                ..Default::default()
            }
        )
        .is_err());
        let next = get_account_gaps(
            &store,
            "1",
            GapListParams {
                guild_id: Some("4".into()),
                cursor: Some(cursor),
                limit: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(next["has_more"], false);
        assert!(store
            .due_account_targets("1", "2026-01-03", 10)
            .unwrap()
            .is_empty());
    }
}
