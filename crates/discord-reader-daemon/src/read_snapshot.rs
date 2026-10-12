//! Immutable account-scoped read observations, independent of later refreshes.

use crate::rpc::RpcError;
use chrono::{DateTime, Utc};
use discord_store::Store;
use serde_json::{json, Value};

fn cache_error(error: discord_store::StoreError) -> RpcError {
    RpcError::structured(
        -32004,
        json!({"error":{"code":error.code(),"error_source":"cache","operation":"get_read_state","retryable":error.retryable(),"message":"read snapshot storage failed"}}),
    )
}

pub(crate) fn capture(
    store: &Store,
    account: &str,
    query: &str,
    response: Value,
    limit: u32,
) -> Result<Value, RpcError> {
    let id = store
        .save_inbox_snapshot(account, query, &response.to_string())
        .map_err(cache_error)?;
    resume(
        store,
        account,
        query,
        &format!("read-snapshot-v1:{id}:0"),
        limit,
    )
}

pub(crate) fn resume(
    store: &Store,
    account: &str,
    query: &str,
    cursor: &str,
    limit: u32,
) -> Result<Value, RpcError> {
    if !(1..=100).contains(&limit) {
        return Err(RpcError::invalid_params("limit must be 1..100"));
    }
    let (id, offset) = cursor
        .strip_prefix("read-snapshot-v1:")
        .filter(|_| cursor.len() <= 128)
        .and_then(|raw| raw.split_once(':'))
        .ok_or_else(|| RpcError::invalid_params("invalid read snapshot cursor"))?;
    if id.len() != 32 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(RpcError::invalid_params("invalid read snapshot identity"));
    }
    let offset: usize = offset
        .parse()
        .map_err(|_| RpcError::invalid_params("invalid read snapshot offset"))?;
    let saved = store
        .load_inbox_snapshot(id)
        .map_err(cache_error)?
        .ok_or_else(|| RpcError::invalid_params("read snapshot unavailable; restart inspection"))?;
    if saved.account_user_id != account || saved.query_json != query {
        return Err(RpcError::invalid_params(
            "read snapshot account or scope mismatch",
        ));
    }
    let at = DateTime::parse_from_rfc3339(&saved.created_at)
        .map_err(|_| RpcError::internal("invalid read snapshot timestamp"))?;
    let age = Utc::now().signed_duration_since(at);
    if age < chrono::Duration::zero() || age > chrono::Duration::hours(24) {
        return Err(RpcError::invalid_params(
            "read snapshot expired; restart inspection",
        ));
    }
    let mut response: Value = serde_json::from_str(&saved.response_json)
        .map_err(|_| RpcError::internal("invalid read snapshot payload"))?;
    let rows = response["read_state"]
        .as_array()
        .ok_or_else(|| RpcError::internal("invalid read snapshot rows"))?;
    if offset > rows.len() {
        return Err(RpcError::invalid_params(
            "read snapshot offset out of range",
        ));
    }
    let end = offset.saturating_add(limit as usize).min(rows.len());
    let has_more = end < rows.len();
    let page = rows[offset..end].to_vec();
    response["read_state"] = json!(page);
    response["snapshot_id"] = json!(id);
    response["snapshot_created_at"] = json!(saved.created_at);
    response["pagination_mode"] = json!("snapshot");
    response["page_complete"] = json!(true);
    response["has_more"] = json!(has_more);
    response["limit"] = json!(limit);
    response["next_cursor"] = if has_more {
        json!(format!("read-snapshot-v1:{id}:{end}"))
    } else {
        Value::Null
    };
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn saved_pages_pin_values_scope_and_clock_and_reject_reuse() {
        let store = discord_store::Store::open_in_memory().unwrap();
        let response = json!({"read_state":[{"channel_id":"200","discord_mention_count":0},{"channel_id":"201","discord_mention_count":null}],"as_of":"fixed","complete":false});
        let first = capture(&store, "7", "scope", response, 1).unwrap();
        let cursor = first["next_cursor"].as_str().unwrap();
        let next = resume(&store, "7", "scope", cursor, 1).unwrap();
        assert_eq!(first["snapshot_id"], next["snapshot_id"]);
        assert_eq!(next["as_of"], "fixed");
        assert!(next["read_state"][0]["discord_mention_count"].is_null());
        assert!(resume(&store, "8", "scope", cursor, 1).is_err());
        assert!(resume(&store, "7", "other", cursor, 1).is_err());
        assert!(resume(&store, "7", "scope", "read-snapshot-v1:missing:1", 1).is_err());
    }
}
