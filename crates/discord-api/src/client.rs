//! Read-only Discord HTTP client.
//!
//! Security invariants enforced here:
//! - only the `GET` method is ever sent ([`ensure_read_only`] is checked at
//!   the lowest layer, on every request, regardless of caller),
//! - URLs are built exclusively from [`DiscordRequest`] (no arbitrary URL
//!   function is exposed),
//! - the credential is wrapped in [`Token`], which redacts itself in `Debug`,
//!   `Display` and serialization, and is never placed in logs or errors.

use std::{fmt, sync::Arc, time::Duration};

use serde_json::Value;
use tokio::time::sleep;
use tracing::{debug, instrument};

use crate::{
    endpoints::{DiscordRequest, API_BASE, READ_ONLY_METHOD},
    error::{ApiError, ClientMetrics},
    rate_limit::{RateLimitError, RateLimiter, DEFAULT_MAX_CONCURRENT_REQUESTS},
};

/// Maximum number of attempts for one logical request (rate limit retries).
const MAX_ATTEMPTS: u32 = 3;

/// Discord user token. Never rendered anywhere.
#[derive(Clone)]
pub struct Token(String);

impl Token {
    /// Build a token from raw input, trimming surrounding whitespace and
    /// rejecting empty values or values with control characters (which could
    /// otherwise smuggle header content into logs or requests).
    pub fn new(raw: impl Into<String>) -> Result<Self, DiscordError> {
        let raw = raw.into();
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(DiscordError::EmptyToken);
        }
        if trimmed.chars().any(|c| c.is_control()) {
            return Err(DiscordError::MalformedToken);
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Token([REDACTED])")
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl serde::Serialize for Token {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("[REDACTED]")
    }
}

/// Errors from the Discord client. None of these ever carry the credential.
#[derive(Debug, thiserror::Error)]
pub enum DiscordError {
    #[error("write HTTP method {0} is forbidden: this client is read-only")]
    WriteMethodForbidden(String),
    #[error("invalid discord identifier: {0:?}")]
    InvalidIdentifier(String),
    #[error("discord credential is empty")]
    EmptyToken,
    #[error("discord credential is malformed")]
    MalformedToken,
    #[error("discord api error: status {status}, body: {body}")]
    Api { status: u16, body: String },
    #[error("discord api rate limited after retries")]
    RateLimited,
    #[error("rate limit error: {0}")]
    RateLimit(#[from] RateLimitError),
    #[error("http transport error: {0}")]
    Transport(String),
    #[error("unexpected response shape: {0}")]
    InvalidResponse(String),
}

/// Reject any method other than GET.
///
/// This is the lowest-layer write guard: even if an upper layer is buggy and
/// somehow asks for `POST`, `PUT`, `PATCH` or `DELETE`, the request is
/// refused before anything is sent on the wire.
pub fn ensure_read_only(method: &str) -> Result<(), DiscordError> {
    match method {
        m if m.eq_ignore_ascii_case(READ_ONLY_METHOD) => Ok(()),
        other => Err(DiscordError::WriteMethodForbidden(other.to_uppercase())),
    }
}

/// Which credential scheme to use in the `Authorization` header.
///
/// User (self) accounts send the raw token; bot accounts use the `Bot ` prefix.
/// The default is [`TokenKind::User`] because this project reads with a user
/// account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TokenKind {
    #[default]
    User,
    Bot,
}

/// Read-only Discord client.
pub struct DiscordClient {
    http: reqwest::Client,
    token: Token,
    token_kind: TokenKind,
    base: String,
    rate: RateLimiter,
    metrics: ClientMetrics,
}

impl fmt::Debug for DiscordClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiscordClient")
            .field("token", &self.token)
            .field("token_kind", &self.token_kind)
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl DiscordClient {
    /// Return the configured authentication scheme without exposing credentials.
    pub fn token_kind(&self) -> TokenKind {
        self.token_kind
    }

    pub fn new(token: Token) -> Result<Self, DiscordError> {
        Self::with_options(
            token,
            TokenKind::default(),
            API_BASE.to_string(),
            DEFAULT_MAX_CONCURRENT_REQUESTS,
        )
    }

    pub fn with_options(
        token: Token,
        token_kind: TokenKind,
        base: String,
        max_concurrent_requests: usize,
    ) -> Result<Self, DiscordError> {
        let http = reqwest::Client::builder()
            .user_agent(concat!(
                "discord-reader/",
                env!("CARGO_PKG_VERSION"),
                " (read-only; +https://github.com/openai/discord-reader)"
            ))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| DiscordError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            token,
            token_kind,
            base,
            rate: RateLimiter::new(max_concurrent_requests),
            metrics: ClientMetrics::default(),
        })
    }

    /// Request / rate-limit counters for diagnostics and tool output.
    pub fn metrics(&self) -> &ClientMetrics {
        &self.metrics
    }

    /// Value of the `Authorization` header. Kept private so the credential
    /// cannot escape through accidental formatting.
    fn authorization(&self) -> String {
        match self.token_kind {
            TokenKind::User => self.token.expose().to_string(),
            TokenKind::Bot => format!("Bot {}", self.token.expose()),
        }
    }

    /// Execute an allowlisted request. `GET` only, always.
    #[instrument(skip(self, req), fields(request = %req.bucket_key()))]
    pub async fn execute(&self, req: DiscordRequest) -> Result<Value, DiscordError> {
        let method = req.method();
        // Lowest-layer guard: re-checked on every request.
        ensure_read_only(method)?;

        let path = req.path()?;
        let url = format!("{}{}", self.base, path);
        let bucket = req.bucket_key();
        let query = req.query();

        let mut last_rate_limited = false;
        for attempt in 1..=MAX_ATTEMPTS {
            let _guard = self.rate.acquire(&bucket).await?;

            debug!(attempt, "dispatching read-only discord request");
            self.metrics.record_request();
            let response = self
                .http
                .get(&url)
                .query(&query)
                .header(reqwest::header::AUTHORIZATION, self.authorization())
                .send()
                .await
                .map_err(|e| DiscordError::Transport(e.to_string()))?;

            let status = response.status();
            let headers = response.headers().clone();
            let remaining = header_u32(&headers, "x-ratelimit-remaining");
            let reset_after = header_secs(&headers, "x-ratelimit-reset-after");
            let retry_after = header_secs(&headers, "retry-after");
            self.rate
                .record(&bucket, remaining, reset_after, retry_after);

            if status.as_u16() == 429 {
                last_rate_limited = true;
                let wait = retry_after.unwrap_or(Duration::from_secs(1));
                if wait > Duration::from_secs(60) {
                    return Err(DiscordError::RateLimited);
                }
                self.metrics.record_wait(&bucket, wait);
                sleep(wait).await;
                continue;
            }

            let text = response
                .text()
                .await
                .map_err(|e| DiscordError::Transport(e.to_string()))?;
            if !status.is_success() {
                return Err(DiscordError::Api {
                    status: status.as_u16(),
                    body: truncate(text, 512),
                });
            }

            return serde_json::from_str(&text)
                .map_err(|e| DiscordError::InvalidResponse(e.to_string()));
        }

        if last_rate_limited {
            Err(DiscordError::RateLimited)
        } else {
            Err(DiscordError::Transport("request attempts exhausted".into()))
        }
    }

    /// Execute and, on failure, return a structured [`ApiError`] with the
    /// request's operation (`bucket_key`) attached.
    #[allow(clippy::result_large_err)]
    pub async fn execute_for(&self, req: &DiscordRequest) -> Result<Value, ApiError> {
        self.execute(req.clone())
            .await
            .map_err(|error| ApiError::from_discord_error(&error, Some(req.bucket_key())))
    }
}

fn truncate(s: String, max: usize) -> String {
    if s.len() <= max {
        s
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s[..end].to_string()
    }
}

fn header_u32(headers: &reqwest::header::HeaderMap, name: &str) -> Option<u32> {
    headers.get(name)?.to_str().ok()?.trim().parse::<u32>().ok()
}

fn header_secs(headers: &reqwest::header::HeaderMap, name: &str) -> Option<Duration> {
    let raw = headers.get(name)?.to_str().ok()?.trim();
    // Discord sends fractional seconds here.
    let secs: f64 = raw.parse().ok()?;
    if secs.is_finite() && secs >= 0.0 {
        Some(Duration::from_secs_f64(secs.min(120.0)))
    } else {
        None
    }
}

/// Shared handle to the client.
pub type SharedDiscordClient = Arc<DiscordClient>;

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "FAKE-TOKEN-abc123-DO-NOT-LOG";

    #[test]
    fn write_methods_are_rejected() {
        for method in ["POST", "PUT", "PATCH", "DELETE", "post", "delete"] {
            let err = ensure_read_only(method).unwrap_err();
            assert!(matches!(err, DiscordError::WriteMethodForbidden(_)));
            let rendered = err.to_string();
            assert!(
                !rendered.contains(SECRET),
                "error text must not contain credentials"
            );
        }
    }

    #[test]
    fn get_is_allowed() {
        assert!(ensure_read_only("GET").is_ok());
        assert!(ensure_read_only("get").is_ok());
    }

    #[test]
    fn token_never_leaks_through_debug_or_display() {
        let token = Token::new(format!("  {SECRET}\n")).unwrap();
        assert_eq!(format!("{token:?}"), "Token([REDACTED])");
        assert_eq!(token.to_string(), "[REDACTED]");
        assert_eq!(
            serde_json::to_string(&token).unwrap(),
            "\"[REDACTED]\"",
            "serialization must redact the token"
        );

        let client =
            DiscordClient::with_options(token, TokenKind::User, API_BASE.to_string(), 4).unwrap();
        let debug = format!("{client:?}");
        assert!(!debug.contains(SECRET), "Debug output leaked the token");
    }

    #[test]
    fn malformed_tokens_are_rejected() {
        assert!(matches!(Token::new(""), Err(DiscordError::EmptyToken)));
        assert!(matches!(Token::new("   "), Err(DiscordError::EmptyToken)));
        assert!(matches!(
            Token::new("abc\ndef"),
            Err(DiscordError::MalformedToken)
        ));
    }

    #[test]
    fn client_hides_token_even_in_struct_debug() {
        let client = DiscordClient::new(Token::new(SECRET).unwrap()).unwrap();
        let dump = format!("{client:#?}");
        assert!(!dump.contains(SECRET));
        assert!(dump.contains("[REDACTED]"));
    }
}
