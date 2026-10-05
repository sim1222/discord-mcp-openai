//! Read-only Discord REST API support.
//!
//! This crate is intentionally tiny and restrictive:
//!
//! * [`DiscordRequest`] is an allowlist of `GET` endpoints; there is no API to
//!   perform `POST` / `PUT` / `PATCH` / `DELETE`, and no API to request an
//!   arbitrary URL.
//! * [`client::ensure_read_only`] re-checks the HTTP method at the lowest
//!   layer before anything is sent.
//! * The credential ([`client::Token`]) redacts itself in `Debug`, `Display`
//!   and `Serialize`, and is never written to logs or errors.
//! * [`rate_limit::RateLimiter`] enforces a small concurrency cap and honours
//!   Discord's `Retry-After` headers.

pub mod client;
pub mod endpoints;
pub mod rate_limit;
pub mod types;

pub use client::{DiscordClient, DiscordError, SharedDiscordClient, Token, TokenKind};
pub use endpoints::DiscordRequest;
