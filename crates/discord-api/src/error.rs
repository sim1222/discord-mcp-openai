//! Machine-classifiable API errors and request metrics.
//!
//! Every failure that can come out of a Discord call is one of
//! [`DiscordError`]'s variants; [`ApiError`] renders that into a stable,
//! structured form (`error_source`, `code`, `http_status`, `discord_code`,
//! `retryable`, `retry_after_ms`, `operation`) so callers can decide whether
//! and how to retry without parsing prose.
//!
//! [`ClientMetrics`] counts internal HTTP requests and 429 waits so tools can
//! show what a call actually cost.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;

use crate::client::DiscordError;

/// Origin of a failure: whose layer produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorSource {
    /// Discord itself answered with an error status / error body.
    Discord,
    /// HTTP transport failure: DNS, TLS, connection reset, timeout.
    Transport,
    /// Rate limiting gave up (retries exhausted or an absurd wait).
    RateLimit,
    /// The request was malformed before it left the client.
    Client,
}

/// Structured, machine-readable failure description.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ApiError {
    /// Which layer failed.
    pub error_source: ErrorSource,
    /// Stable coarse code: `discord_error`, `not_found`, `forbidden`,
    /// `bot_only`, `rate_limited`, `transport`, `invalid_request`,
    /// `invalid_response`, `write_forbidden`.
    pub code: &'static str,
    /// HTTP status when Discord answered, `null` for transport/client errors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    /// Discord error `code` from the response body (e.g. 50001, 20002).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discord_code: Option<i64>,
    /// Discord error `message` from the response body, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discord_message: Option<String>,
    /// Bounded field paths and validation codes, without submitted values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    /// Whether retrying the same request may succeed later.
    pub retryable: bool,
    /// Suggested wait before retrying, in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    /// The allowlisted operation (`bucket_key`) that failed, e.g.
    /// `GET /channels/{channel.id}/messages`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    /// Human-readable detail (never contains the credential).
    pub message: String,
}

impl ApiError {
    /// Classify a [`DiscordError`] that occurred for `operation`.
    pub fn from_discord_error(error: &DiscordError, operation: Option<String>) -> Self {
        match error {
            DiscordError::Api { status, body, .. } => {
                let (discord_code, discord_message) = parse_discord_error_body(body);
                let status = *status;
                let code = match (status, discord_code) {
                    (_, Some(20002)) => "bot_only",
                    (404, _) => "not_found",
                    (403, _) => "forbidden",
                    (429, _) => "rate_limited",
                    (400, _) => "invalid_request",
                    _ => "discord_error",
                };
                let retryable = matches!(status, 429 | 500 | 502 | 503 | 504);
                Self {
                    error_source: ErrorSource::Discord,
                    code,
                    http_status: Some(status),
                    discord_code,
                    discord_message,
                    details: (discord_code == Some(50035))
                        .then(|| validation_details(body))
                        .flatten(),
                    retryable,
                    retry_after_ms: None,
                    operation,
                    message: format!(
                        "Discord request failed (HTTP {status}, code {})",
                        discord_code
                            .map(|code| code.to_string())
                            .unwrap_or_else(|| "unknown".into())
                    ),
                }
            }
            DiscordError::RateLimited => Self {
                error_source: ErrorSource::RateLimit,
                code: "rate_limited",
                http_status: None,
                discord_code: None,
                discord_message: None,
                details: None,
                retryable: true,
                retry_after_ms: None,
                operation,
                message: error.to_string(),
            },
            DiscordError::RateLimit(inner) => Self {
                error_source: ErrorSource::RateLimit,
                code: "rate_limited",
                http_status: None,
                discord_code: None,
                discord_message: None,
                details: None,
                retryable: !matches!(inner, crate::rate_limit::RateLimitError::ShuttingDown),
                retry_after_ms: None,
                operation,
                message: error.to_string(),
            },
            DiscordError::Transport(_) => Self {
                error_source: ErrorSource::Transport,
                code: "transport",
                http_status: None,
                discord_code: None,
                discord_message: None,
                details: None,
                retryable: true,
                retry_after_ms: None,
                operation,
                message: error.to_string(),
            },
            DiscordError::InvalidResponse(_) => Self {
                error_source: ErrorSource::Discord,
                code: "invalid_response",
                http_status: None,
                discord_code: None,
                discord_message: None,
                details: None,
                retryable: false,
                retry_after_ms: None,
                operation,
                message: error.to_string(),
            },
            DiscordError::WriteMethodForbidden(_)
            | DiscordError::InvalidIdentifier(_)
            | DiscordError::EmptyToken
            | DiscordError::MalformedToken => Self {
                error_source: ErrorSource::Client,
                code: "invalid_request",
                http_status: None,
                discord_code: None,
                discord_message: None,
                details: None,
                retryable: false,
                retry_after_ms: None,
                operation,
                message: error.to_string(),
            },
        }
    }
}

fn validation_details(body: &str) -> Option<serde_json::Value> {
    fn collect(
        value: &serde_json::Value,
        path: &str,
        depth: usize,
        visited: &mut usize,
        errors: &mut Vec<serde_json::Value>,
    ) {
        if depth > 8 || *visited >= 256 || errors.len() >= 16 {
            return;
        }
        *visited += 1;
        let Some(object) = value.as_object() else {
            return;
        };
        if let Some(entries) = object.get("_errors").and_then(|value| value.as_array()) {
            for entry in entries.iter().take(16 - errors.len()) {
                if let Some(code) =
                    entry
                        .get("code")
                        .and_then(|value| value.as_str())
                        .filter(|code| {
                            code.len() <= 64
                                && code.bytes().all(|byte| {
                                    byte.is_ascii_uppercase()
                                        || byte.is_ascii_digit()
                                        || byte == b'_'
                                })
                        })
                {
                    errors.push(serde_json::json!({"path":path,"code":code,"message":"Discord rejected this field."}));
                }
            }
        }
        for (key, value) in object {
            if key == "_errors"
                || key.len() > 64
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                continue;
            }
            let next = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            if next.len() <= 256 {
                collect(value, &next, depth + 1, visited, errors);
            }
            if *visited >= 256 || errors.len() >= 16 {
                break;
            }
        }
    }
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let mut errors = Vec::new();
    collect(value.get("errors")?, "", 0, &mut 0, &mut errors);
    (!errors.is_empty()).then_some(serde_json::Value::Array(errors))
}

/// Parse a Discord error body: `{"message": "...", "code": 50001}`.
fn parse_discord_error_body(body: &str) -> (Option<i64>, Option<String>) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return (None, None);
    };
    let code = value.get("code").and_then(|c| c.as_i64());
    let message = value
        .get("message")
        .and_then(|m| m.as_str())
        .map(|s| s.to_string());
    (code, message)
}

/// Cumulative counters for internal HTTP request behaviour.
///
/// All fields are process-lifetime totals for the daemon: `http_requests` is
/// the number of HTTP requests actually sent to Discord, `rate_limit_waits`
/// how often a 429 forced a wait, and `rate_limit_wait_ms` how long those
/// waits added summed. Tools expose these so bulk operations can be costed.
#[derive(Debug, Default)]
pub struct ClientMetrics {
    http_requests: AtomicU64,
    rate_limit_waits: AtomicU64,
    rate_limit_wait_ms: AtomicU64,
    rate_limit_hits: AtomicU64,
    last_bucket_waits: Mutex<Vec<BucketWait>>,
}

/// One observed rate-limit wait, newest last (bounded ring).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BucketWait {
    /// Route template that was throttled.
    pub bucket: String,
    /// How long the wait was.
    pub wait_ms: u64,
}

/// Snapshot of [`ClientMetrics`], serializable for tool output.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub http_requests: u64,
    pub rate_limit_hits: u64,
    pub rate_limit_waits: u64,
    pub rate_limit_wait_ms: u64,
    pub recent_waits: Vec<BucketWait>,
}

const RECENT_WAITS_KEPT: usize = 16;

impl ClientMetrics {
    /// Record one HTTP request actually sent.
    pub fn record_request(&self) {
        self.http_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rate-limit wait on `bucket`.
    pub fn record_wait(&self, bucket: &str, wait: Duration) {
        self.rate_limit_hits.fetch_add(1, Ordering::Relaxed);
        self.rate_limit_waits.fetch_add(1, Ordering::Relaxed);
        self.rate_limit_wait_ms
            .fetch_add(wait.as_millis() as u64, Ordering::Relaxed);
        let mut recent = self.last_bucket_waits.lock().expect("metrics lock");
        recent.push(BucketWait {
            bucket: bucket.to_string(),
            wait_ms: wait.as_millis() as u64,
        });
        let len = recent.len();
        if len > RECENT_WAITS_KEPT {
            recent.drain(0..len - RECENT_WAITS_KEPT);
        }
    }

    /// Current snapshot.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            http_requests: self.http_requests.load(Ordering::Relaxed),
            rate_limit_hits: self.rate_limit_hits.load(Ordering::Relaxed),
            rate_limit_waits: self.rate_limit_waits.load(Ordering::Relaxed),
            rate_limit_wait_ms: self.rate_limit_wait_ms.load(Ordering::Relaxed),
            recent_waits: self.last_bucket_waits.lock().expect("metrics lock").clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "SECRET-USER-TOKEN-DO-NOT-LEAK";

    #[test]
    fn bad_request_preserves_bounded_validation_paths_without_raw_response_fields() {
        let error = DiscordError::Api {
            status: 400,
            body: serde_json::json!({"code":50035,"message":"Invalid Form Body","token":SECRET,"errors":{"limit":{"_errors":[{"code":"NUMBER_TYPE_MAX","message":"must be less than 100","value":SECRET}]}}}).to_string(),
        };
        let api = ApiError::from_discord_error(
            &error,
            Some("GET /channels/{id}/threads/archived/public".into()),
        );
        assert_eq!(api.code, "invalid_request");
        assert!(!api.retryable);
        let details = api.details.unwrap();
        assert_eq!(details[0]["path"], "limit");
        assert_eq!(details[0]["code"], "NUMBER_TYPE_MAX");
        assert!(details[0]["message"].is_string());
        assert!(!details.to_string().contains(SECRET));
    }

    #[test]
    fn validation_details_are_bounded_and_do_not_echo_submitted_message_values() {
        let entries: Vec<_> = (0..100).map(|_| serde_json::json!({"code":"BASE_TYPE_REQUIRED","message":SECRET,"value":SECRET})).collect();
        let error = DiscordError::Api { status:400, body:serde_json::json!({"code":50035,"message":"Invalid Form Body","errors":{"before":{"_errors":entries}}}).to_string() };
        let api = ApiError::from_discord_error(&error, None);
        let payload = serde_json::to_value(api).unwrap();
        assert_eq!(payload["details"].as_array().unwrap().len(), 16);
        assert!(!payload.to_string().contains(SECRET));
    }

    #[test]
    fn api_errors_carry_discord_codes() {
        let error = DiscordError::Api {
            status: 403,
            body: "{\"message\":\"Missing Access\",\"code\":50001}".to_string(),
        };
        let api = ApiError::from_discord_error(&error, Some("GET /channels/{id}".into()));
        assert_eq!(api.error_source, ErrorSource::Discord);
        assert_eq!(api.code, "forbidden");
        assert_eq!(api.http_status, Some(403));
        assert_eq!(api.discord_code, Some(50001));
        assert_eq!(api.discord_message.as_deref(), Some("Missing Access"));
        assert!(!api.retryable);
        assert!(!api.message.contains(SECRET));
    }

    #[test]
    fn bot_only_20002_is_classified() {
        let error = DiscordError::Api {
            status: 403,
            body: r#"{"message":"This endpoint is bot-only","code":20002}"#.into(),
        };
        let api = ApiError::from_discord_error(&error, None);
        assert_eq!(api.code, "bot_only");
        assert_eq!(api.discord_code, Some(20002));
    }

    #[test]
    fn not_found_and_transport_are_distinguished() {
        let nf = DiscordError::Api {
            status: 404,
            body: r#"{"message":"Unknown Message"}"#.into(),
        };
        let api = ApiError::from_discord_error(&nf, None);
        assert_eq!(api.code, "not_found");
        assert_eq!(api.error_source, ErrorSource::Discord);

        let transport = DiscordError::Transport("connection reset".into());
        let api = ApiError::from_discord_error(&transport, None);
        assert_eq!(api.code, "transport");
        assert_eq!(api.error_source, ErrorSource::Transport);
        assert!(api.retryable);
    }

    #[test]
    fn metrics_count_requests_and_waits() {
        let metrics = ClientMetrics::default();
        metrics.record_request();
        metrics.record_request();
        metrics.record_wait(
            "GET /channels/{channel.id}/messages",
            Duration::from_millis(150),
        );
        let snap = metrics.snapshot();
        assert_eq!(snap.http_requests, 2);
        assert_eq!(snap.rate_limit_waits, 1);
        assert_eq!(snap.rate_limit_wait_ms, 150);
        assert_eq!(snap.recent_waits.len(), 1);
        assert_eq!(
            snap.recent_waits[0].bucket,
            "GET /channels/{channel.id}/messages"
        );
    }
}
