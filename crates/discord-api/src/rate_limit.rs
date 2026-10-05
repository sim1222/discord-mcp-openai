//! Discord rate limit handling.
//!
//! Rules:
//! - a small global concurrency cap (default 4 in-flight requests),
//! - per-route buckets driven by `X-RateLimit-*` response headers,
//! - `429` responses always honour `Retry-After` / `retry_after`.
//!
//! There is no fixed-sleep fallback and no unbounded parallel fetching.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::Semaphore;

/// Upper bound on how long we are willing to wait for a rate limit window.
const MAX_WAIT: Duration = Duration::from_secs(60);

/// Sanity ceiling for recorded block windows (never sleep longer than this
/// even if a server misbehaves).
const MAX_BLOCK: Duration = Duration::from_secs(600);

#[derive(Debug, Default, Clone, Copy)]
struct Bucket {
    /// When the bucket is expected to have capacity again.
    blocked_until: Option<Instant>,
    /// Remaining requests reported by Discord for the current window.
    remaining: Option<u32>,
}

/// Rate limiter state shared by every request.
pub struct RateLimiter {
    semaphore: Arc<Semaphore>,
    buckets: Arc<Mutex<HashMap<String, Bucket>>>,
}

/// Guard holding the global concurrency permit for one request.
pub struct RateGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl RateLimiter {
    pub fn new(max_concurrent_requests: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent_requests.max(1))),
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Wait until both the global concurrency budget and the route bucket
    /// allow another request.
    pub async fn acquire(&self, bucket_key: &str) -> Result<RateGuard, RateLimitError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RateLimitError::ShuttingDown)?;

        loop {
            let wait = {
                let buckets = self.buckets.lock().expect("rate limit bucket lock");
                buckets
                    .get(bucket_key)
                    .and_then(|b| b.blocked_until)
                    .map(|until| until.saturating_duration_since(Instant::now()))
                    .filter(|d| !d.is_zero())
            };
            match wait {
                None => break,
                Some(wait) if wait > MAX_WAIT => return Err(RateLimitError::WaitTooLong(wait)),
                Some(wait) => tokio::time::sleep(wait).await,
            }
        }

        Ok(RateGuard { _permit: permit })
    }

    /// Record a response and update the bucket state.
    ///
    /// `retry_after` is set when the response is a 429 (or when Discord sends
    /// `Retry-After` on any response).
    pub fn record(
        &self,
        bucket_key: &str,
        remaining: Option<u32>,
        reset_after: Option<Duration>,
        retry_after: Option<Duration>,
    ) {
        let mut buckets = self.buckets.lock().expect("rate limit bucket lock");
        let bucket = buckets.entry(bucket_key.to_string()).or_default();
        bucket.remaining = remaining;

        let now = Instant::now();
        let mut blocked_until = bucket.blocked_until.unwrap_or(now);
        if let Some(reset) = reset_after {
            if remaining == Some(0) {
                blocked_until = blocked_until.max(now + reset);
            }
        }
        if let Some(retry) = retry_after {
            blocked_until = blocked_until.max(now + retry.min(MAX_BLOCK));
        }
        bucket.blocked_until = Some(blocked_until);
    }

    /// Snapshot of the currently blocked buckets (for tests / diagnostics).
    pub fn blocked_buckets(&self) -> Vec<String> {
        let now = Instant::now();
        self.buckets
            .lock()
            .expect("rate limit bucket lock")
            .iter()
            .filter(|(_, b)| b.blocked_until.map(|u| u > now).unwrap_or(false))
            .map(|(k, _)| k.clone())
            .collect()
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CONCURRENT_REQUESTS)
    }
}

/// Default global concurrency for Discord requests.
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum RateLimitError {
    #[error("rate limiter is shutting down")]
    ShuttingDown,
    #[error("refusing to wait {0:?} for a rate limit window")]
    WaitTooLong(Duration),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retry_after_blocks_the_bucket() {
        let limiter = RateLimiter::new(4);
        let key = "GET /channels/{channel.id}/messages";
        limiter.record(key, Some(0), None, Some(Duration::from_millis(120)));

        let started = Instant::now();
        let _guard = limiter.acquire(key).await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn concurrency_is_capped() {
        let limiter = Arc::new(RateLimiter::new(2));
        let g1 = limiter.acquire("a").await.unwrap();
        let g2 = limiter.acquire("a").await.unwrap();

        let third =
            tokio::time::timeout(Duration::from_millis(100), limiter.clone().acquire("a")).await;
        assert!(third.is_err(), "third concurrent request must wait");

        drop(g1);
        drop(g2);
        let _g3 = tokio::time::timeout(Duration::from_secs(1), limiter.acquire("a"))
            .await
            .expect("permit becomes available after release")
            .unwrap();
    }

    #[tokio::test]
    async fn absurd_waits_are_refused() {
        let limiter = RateLimiter::new(4);
        let key = "GET /guilds/{guild.id}";
        limiter.record(key, Some(0), None, Some(Duration::from_secs(120)));
        assert!(matches!(
            limiter.acquire(key).await,
            Err(RateLimitError::WaitTooLong(_))
        ));
    }
}
