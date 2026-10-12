//! Render source-backed read states without turning missing or stale data into zero.

use chrono::{DateTime, Utc};
use discord_api::account_observation::AccountObservation;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

const MAX_AGE_SECONDS: i64 = 300;

pub(crate) fn render_read_states(snapshot: &AccountObservation, channel_ids: &[String]) -> Value {
    render_read_states_at(snapshot, channel_ids, Utc::now())
}

fn fresh(at: &str, as_of: DateTime<Utc>) -> bool {
    DateTime::parse_from_rfc3339(at).ok().is_some_and(|at| {
        let age = as_of.signed_duration_since(at);
        age >= chrono::Duration::zero() && age <= chrono::Duration::seconds(MAX_AGE_SECONDS)
    })
}

fn render_read_states_at(
    snapshot: &AccountObservation,
    channel_ids: &[String],
    as_of: DateTime<Utc>,
) -> Value {
    let states: HashMap<_, _> = snapshot
        .read_states
        .iter()
        .map(|state| (state.channel_id.as_str(), state))
        .collect();
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    let snapshot_fresh = fresh(&snapshot.observed_at, as_of);
    for channel in channel_ids.iter().filter(|id| seen.insert(id.as_str())) {
        let state = states.get(channel.as_str()).copied();
        let is_fresh =
            snapshot_fresh && state.is_some_and(|state| fresh(&state.observed_at, as_of));
        let availability = match state {
            None => "unobserved",
            Some(_) if !is_fresh => "stale",
            Some(state) => state.availability.as_str(),
        };
        let usable = is_fresh && matches!(availability, "available" | "partial");
        let reason = match state {
            None => Some("channel_read_state_not_observed"),
            Some(_) if !is_fresh => Some("read_state_observation_not_fresh"),
            Some(state) => state.reason.as_deref().or_else(|| {
                state
                    .discord_mention_count
                    .is_none()
                    .then_some("mention_count_not_observed")
            }),
        };
        rows.push(json!({
            "channel_id":channel,
            "last_read_message_id":state.filter(|_| usable).and_then(|state| state.last_read_message_id.as_deref()),
            "last_read_message_id_reason":if !usable {reason}else if state.is_none_or(|state|state.last_read_message_id.is_none()) {Some("last_read_position_not_observed_or_unacked")}else{None},
            "discord_mention_count":state.filter(|_| usable).and_then(|state| state.discord_mention_count),
            "computed_unread_candidate_count":null,
            "source":"discord_gateway_ready",
            "fetched_at":state.map(|state| state.observed_at.as_str()),
            "version":state.and_then(|state| state.version.as_deref()),
            "availability":availability,"reason":reason,
            "current_reason":reason,
            "last_observed":state.map(|state|json!({"discord_mention_count":state.discord_mention_count,"last_read_message_id":state.last_read_message_id,"observed_at":state.observed_at,"version":state.version,"availability":state.availability})),
        }));
    }
    let complete = !snapshot.partial
        && snapshot_fresh
        && !rows.is_empty()
        && rows.iter().all(|row| {
            row["availability"] == "available" && !row["discord_mention_count"].is_null()
        });
    let observed = rows
        .iter()
        .filter(|row| row["discord_mention_count"].is_u64())
        .count();
    let unknown = rows.len() - observed;
    let sum = rows
        .iter()
        .filter_map(|row| row["discord_mention_count"].as_u64())
        .fold(0u64, u64::saturating_add);
    json!({
        "account_user_id":snapshot.account_user_id,"source":"discord_gateway_ready",
        "as_of":as_of.to_rfc3339(),"fetched_at":if snapshot.observed_at.is_empty(){None}else{Some(snapshot.observed_at.as_str())},"version":snapshot.version,
        "freshness":if snapshot_fresh {"fresh"} else {"stale"},
        "scope":{"kind":"channels","channel_ids":channel_ids},
        "partial":snapshot.partial,"complete":complete,
        "read_state":rows,
        "counts":{"observed_rows":observed,"unknown_rows":unknown,"observed_mention_sum":sum,"discord_scope_mention_total":if unknown==0 && observed>0 {Some(sum)}else{None},"source":"discord_gateway_ready","basis":"requested_channel_rows_at_as_of","is_guild_ui_badge":false},
        "inventory_errors":snapshot.inventory_errors.iter().take(20).collect::<Vec<_>>(),
        "inventory_errors_total":snapshot.inventory_errors.len(),
        "inventory_errors_omitted":snapshot.inventory_errors.len().saturating_sub(20),
        "coverage":{"complete":false,"reason":"read_state_does_not_establish_message_history_coverage"},
    })
}

/// Compare a cached message to a fresh observed read position independently of notification or task state.
pub(crate) fn classify_message_read(
    snapshot: &AccountObservation,
    channel_id: &str,
    message_id: &str,
    as_of: DateTime<Utc>,
) -> Value {
    let state = snapshot
        .read_states
        .iter()
        .find(|state| state.channel_id == channel_id);
    let evidence = state.filter(|state| {
        matches!(state.availability.as_str(), "available" | "partial")
            && fresh(&snapshot.observed_at, as_of)
            && fresh(&state.observed_at, as_of)
    });
    let comparison = evidence.and_then(|state| {
        let message = message_id.parse::<u64>().ok().filter(|id| *id > 0)?;
        let read = state
            .last_read_message_id
            .as_deref()?
            .parse::<u64>()
            .ok()
            .filter(|id| *id > 0)?;
        Some(message <= read)
    });
    let status = match comparison {
        Some(true) => "read",
        Some(false) => "unread",
        None => "unknown",
    };
    json!({
        "status":status,"source":"discord_gateway_ready",
        "basis":if comparison.is_some() {"message_id_compared_with_observed_last_read_message_id"} else {"fresh_read_position_unavailable"},
        "last_read_message_id":evidence.and_then(|state| state.last_read_message_id.as_deref()),
        "fetched_at":state.map(|state| state.observed_at.as_str()),
        "version":state.and_then(|state| state.version.as_deref()),"as_of":as_of.to_rfc3339(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_observed_counts_cannot_become_a_scope_total() {
        let mut observation = snapshot("2026-01-01T00:00:00Z");
        let example = observation.read_states[0].clone();
        observation.read_states = (0..31)
            .map(|index| {
                let mut state = example.clone();
                state.channel_id = (200 + index).to_string();
                state.discord_mention_count = Some(if index < 4 { 2 } else { 0 });
                state
            })
            .collect();
        let ids = (200..308).map(|id| id.to_string()).collect::<Vec<_>>();
        let result = render_read_states_at(&observation, &ids, now());
        assert_eq!(result["counts"]["observed_rows"], 31);
        assert_eq!(result["counts"]["unknown_rows"], 77);
        assert_eq!(result["counts"]["observed_mention_sum"], 8);
        assert!(result["counts"]["discord_scope_mention_total"].is_null());
        assert_eq!(result["complete"], false);
    }

    #[test]
    fn stale_values_keep_observation_history_and_explain_current_unknown() {
        let result = render_read_states_at(
            &snapshot("2025-12-31T23:59:59Z"),
            &["200".into(), "201".into()],
            now(),
        );
        assert_eq!(
            result["read_state"][0]["last_observed"]["discord_mention_count"],
            0
        );
        assert_eq!(
            result["read_state"][0]["last_observed"]["last_read_message_id"],
            "100"
        );
        assert_eq!(
            result["read_state"][0]["last_read_message_id_reason"],
            "read_state_observation_not_fresh"
        );
        assert!(result["read_state"][1]["last_observed"].is_null());
    }

    fn snapshot(at: &str) -> AccountObservation {
        serde_json::from_value(json!({
            "account_user_id":"7","observed_at":at,"version":"42","partial":false,
            "channels":[],"inventory_complete":false,
            "read_states":[{"channel_id":"200","last_read_message_id":"100",
                "discord_mention_count":0,"observed_at":at,"version":"42",
                "availability":"available","reason":null}]
        }))
        .unwrap()
    }

    fn now() -> DateTime<Utc> {
        "2026-01-01T00:05:00Z".parse().unwrap()
    }

    #[test]
    fn absent_channels_remain_unknown_while_observed_zero_is_preserved() {
        let result = render_read_states_at(
            &snapshot("2026-01-01T00:00:00Z"),
            &["200".into(), "201".into()],
            now(),
        );
        assert_eq!(result["read_state"][0]["discord_mention_count"], 0);
        assert!(result["read_state"][1]["discord_mention_count"].is_null());
        assert_eq!(result["read_state"][1]["availability"], "unobserved");
        assert_eq!(result["scope"]["channel_ids"], json!(["200", "201"]));
        assert!(!result["complete"].as_bool().unwrap());
        assert!(result["read_state"][0]["computed_unread_candidate_count"].is_null());
    }

    #[test]
    fn stale_or_invalid_timestamps_do_not_report_current_zero_or_read_status() {
        for at in ["2025-12-31T23:59:59Z", "invalid", "2026-01-01T00:06:00Z"] {
            let result = render_read_states_at(&snapshot(at), &["200".into()], now());
            assert!(result["read_state"][0]["discord_mention_count"].is_null());
            assert_eq!(result["read_state"][0]["availability"], "stale");
            assert_eq!(
                classify_message_read(&snapshot(at), "200", "99", now())["status"],
                "unknown"
            );
        }
    }

    #[test]
    fn an_observed_read_position_is_independent_of_missing_notification_count() {
        let mut snapshot = snapshot("2026-01-01T00:00:00Z");
        snapshot.read_states[0].discord_mention_count = None;
        snapshot.read_states[0].availability = "partial".into();
        snapshot.read_states[0].reason = Some("mention_count_not_observed".into());
        let result = render_read_states_at(&snapshot, &["200".into()], now());
        assert_eq!(result["read_state"][0]["last_read_message_id"], "100");
        assert!(result["read_state"][0]["discord_mention_count"].is_null());
        assert_eq!(
            classify_message_read(&snapshot, "200", "99", now())["status"],
            "read"
        );
    }

    #[test]
    fn read_classification_uses_numeric_ids_and_missing_ack_is_unknown() {
        let mut snapshot = snapshot("2026-01-01T00:00:00Z");
        assert_eq!(
            classify_message_read(&snapshot, "200", "99", now())["status"],
            "read"
        );
        assert_eq!(
            classify_message_read(&snapshot, "200", "101", now())["status"],
            "unread"
        );
        assert_eq!(
            classify_message_read(&snapshot, "201", "99", now())["status"],
            "unknown"
        );
        assert_eq!(
            classify_message_read(&snapshot, "200", "x", now())["status"],
            "unknown"
        );
        snapshot.read_states[0].last_read_message_id = None;
        assert_eq!(
            classify_message_read(&snapshot, "200", "99", now())["status"],
            "unknown"
        );
    }
}
