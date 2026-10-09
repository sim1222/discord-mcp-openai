//! Scope-bound keyset pagination for live status observations.

use crate::rpc::{RpcError, StatusParams};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    operation: String,
    account: String,
    guild: Option<String>,
    channel: Option<String>,
    after: String,
    #[serde(default)]
    basis: Option<String>,
}

pub(crate) struct StatusQuery {
    pub(crate) params: StatusParams,
    pub(crate) after: Option<String>,
    pub(crate) limit: u32,
    pub(crate) details: bool,
    operation: String,
    account: String,
    basis: Option<String>,
}

impl StatusQuery {
    pub(crate) fn new(
        params: StatusParams,
        operation: &str,
        account: &str,
    ) -> Result<Self, RpcError> {
        for id in [params.guild_id.as_deref(), params.channel_id.as_deref()]
            .into_iter()
            .flatten()
        {
            if !valid_id(id) {
                return Err(RpcError::invalid_params(
                    "scope IDs must be positive numeric strings",
                ));
            }
        }
        let limit = params.limit.unwrap_or(50);
        if !(1..=100).contains(&limit) {
            return Err(RpcError::invalid_params("limit must be 1..100"));
        }
        let mut basis = None;
        let after = params
            .cursor
            .as_deref()
            .map(|raw| {
                let parsed: Cursor = raw
                    .strip_prefix("status-v1:")
                    .filter(|_| raw.len() <= 1024)
                    .and_then(|value| serde_json::from_str(value).ok())
                    .ok_or_else(|| RpcError::invalid_params("invalid status cursor"))?;
                if parsed.operation != operation
                    || parsed.account != account
                    || parsed.guild != params.guild_id
                    || parsed.channel != params.channel_id
                    || !valid_id(&parsed.after)
                {
                    return Err(RpcError::invalid_params(
                        "status cursor does not match account, operation or scope",
                    ));
                }
                basis = parsed.basis;
                Ok(parsed.after)
            })
            .transpose()?;
        let details = params.include_details || params.channel_id.is_some() || after.is_some();
        Ok(Self {
            params,
            after,
            limit,
            details,
            operation: operation.into(),
            account: account.into(),
            basis,
        })
    }

    pub(crate) fn next_cursor(&self, after: &str) -> String {
        let cursor = Cursor {
            operation: self.operation.clone(),
            account: self.account.clone(),
            guild: self.params.guild_id.clone(),
            channel: self.params.channel_id.clone(),
            after: after.into(),
            basis: self.basis.clone(),
        };
        format!(
            "status-v1:{}",
            serde_json::to_string(&cursor).expect("serializable status cursor")
        )
    }

    pub(crate) fn bind_basis(&mut self, basis: &str) -> Result<(), RpcError> {
        if self.params.cursor.is_some() && self.basis.as_deref() != Some(basis) {
            return Err(RpcError::invalid_params(
                "status cursor target source changed; restart inspection",
            ));
        }
        self.basis = Some(basis.into());
        Ok(())
    }
}

fn valid_id(id: &str) -> bool {
    id.parse::<u64>().is_ok_and(|n| n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_bind_account_operation_and_scope_and_defaults_are_bounded() {
        let params = StatusParams {
            guild_id: Some("42".into()),
            include_details: true,
            ..Default::default()
        };
        let query = StatusQuery::new(params.clone(), "coverage", "7").unwrap();
        assert_eq!(query.limit, 50);
        let cursor = query.next_cursor("200");
        let resumed = StatusParams {
            cursor: Some(cursor),
            ..params
        };
        assert_eq!(
            StatusQuery::new(resumed.clone(), "coverage", "7")
                .unwrap()
                .after
                .as_deref(),
            Some("200")
        );
        assert!(StatusQuery::new(resumed.clone(), "sync", "7").is_err());
        assert!(StatusQuery::new(resumed.clone(), "coverage", "8").is_err());
        assert!(StatusQuery::new(
            StatusParams {
                guild_id: Some("43".into()),
                ..resumed
            },
            "coverage",
            "7"
        )
        .is_err());
        assert!(
            !StatusQuery::new(StatusParams::default(), "sync", "7")
                .unwrap()
                .details
        );
        assert!(StatusQuery::new(
            StatusParams {
                limit: Some(101),
                ..Default::default()
            },
            "sync",
            "7"
        )
        .is_err());
    }
}
