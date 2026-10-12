//! Local-only workflow updates, notification claims, and caller-reported gaps.
use super::DiscordReaderTools;
use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, tool, tool_router, ErrorData,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeliveryOutcomeArgs {
    Delivered { receipt: String },
    AmbiguousDelivery { reason: String },
    NotSent { reason: String },
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NotificationStatus {
    Queued,
    Claimed,
    Sending,
    Delivered,
    AmbiguousDelivery,
}

fn present<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InboxItemParams {
    pub channel_id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
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

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GapListParams {
    pub guild_id: Option<String>,
    pub channel_id: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WithheldStatus {
    Denied,
    Cancelled,
    Excluded,
}
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GapRecordParams {
    pub channel_id: String,
    pub status: WithheldStatus,
    pub reason: String,
    pub observed_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterialKey {
    pub channel_id: String,
    pub message_id: String,
    pub material_version: String,
    pub notification_kind: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(extend("type" = "object"))]
pub enum NotificationParams {
    List {
        limit: Option<u32>,
        /// Omit to inspect unresolved work; delivered selects completed receipts.
        status: Option<NotificationStatus>,
        /// Continue the same account and status filter.
        cursor: Option<String>,
    },
    Enqueue {
        key: MaterialKey,
        available_at: String,
        expected_workflow_version: u64,
        #[serde(default)]
        renotify_material_change: bool,
    },
    /// Explicit queued-only reschedule; requires both entry and current workflow versions.
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
        outcome: DeliveryOutcomeArgs,
    },
    Import {
        key: MaterialKey,
        receipt: String,
        observed_at: String,
    },
}

#[tool_router(vis="pub(crate)",router=local_state_router)]
impl DiscordReaderTools {
    /// Read the latest local workflow and CAS version independently of snapshots or Discord read state.
    #[tool(name = "get_inbox_state")]
    pub async fn get_inbox_state(
        &self,
        Parameters(args): Parameters<InboxItemParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "get_inbox_state",
            serde_json::to_value(args)
                .map_err(|_| ErrorData::internal_error("invalid local state arguments", None))?,
        )
        .await
    }
    /// Patch only supplied workflow fields. Null clears a field; omitted fields remain unchanged.
    /// expected_version rejects stale writers. This modifies local state and never Discord.
    #[tool(name = "patch_inbox_state")]
    pub async fn patch_inbox_state(
        &self,
        Parameters(args): Parameters<InboxPatchParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "patch_inbox_state",
            serde_json::to_value(args)
                .map_err(|_| ErrorData::internal_error("invalid local state arguments", None))?,
        )
        .await
    }
    /// Manage durable local notification claims and receipts. No external message is sent.
    /// Enqueue defers until available_at; claim and begin_delivery verify current workflow.
    /// Revalidate explicitly refreshes a queued candidate after workflow changes; terminal deliveries stay immutable.
    /// Unknown delivery remains ambiguous until explicit receipt reconciliation via import.
    #[tool(name = "notification_outbox")]
    pub async fn notification_outbox(
        &self,
        Parameters(args): Parameters<NotificationParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "notification_outbox",
            serde_json::to_value(args)
                .map_err(|_| ErrorData::internal_error("invalid outbox arguments", None))?,
        )
        .await
    }
    /// Read bounded durable gap evidence. Caller-reported approval denials remain separate from Discord errors.
    /// Implicit unfetched and incomplete-history ranges remain available through get_account_coverage.
    #[tool(name = "get_account_gaps")]
    pub async fn get_account_gaps(
        &self,
        Parameters(args): Parameters<GapListParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "get_account_gaps",
            serde_json::to_value(args)
                .map_err(|_| ErrorData::internal_error("invalid gap arguments", None))?,
        )
        .await
    }
    /// Record a caller-reported denial, cancellation, or exclusion for a target.
    /// This blocks automatic retries; it does not establish Discord 403 or change caller authorization.
    #[tool(name = "record_account_gap")]
    pub async fn record_account_gap(
        &self,
        Parameters(args): Parameters<GapRecordParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call_tool(
            "record_account_gap",
            serde_json::to_value(args)
                .map_err(|_| ErrorData::internal_error("invalid gap arguments", None))?,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn notification_tool_schema_root_is_an_object_with_operation_variants() {
        let schema = serde_json::to_value(schemars::schema_for!(NotificationParams)).unwrap();
        assert_eq!(schema["type"], "object");
        assert!(schema["oneOf"].is_array());
    }
    #[test]
    fn patch_tool_keeps_explicit_null_and_omits_missing_fields() {
        let raw =
            json!({"channel_id":"2","message_id":"3","expected_version":1,"snoozed_until":null});
        let args: InboxPatchParams = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(serde_json::to_value(args).unwrap(), raw);
    }
    #[test]
    fn notification_tool_rejects_unknown_nested_identity() {
        assert!(serde_json::from_value::<NotificationParams>(json!({"operation":"claim","key":{"channel_id":"2","message_id":"3","material_version":"v1","notification_kind":"notice","account_user_id":"4"},"worker_id":"worker","lease_seconds":60})).is_err());
    }
}
