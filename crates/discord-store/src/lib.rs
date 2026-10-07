//! SQLite cache (with FTS5 full-text search) for Discord reader.
//!
//! The cache only contains data that was fetched on demand. There is no
//! background crawling or backfilling, and the database never stores Discord
//! credentials.

mod migration;
pub mod search;
pub mod sqlite;

pub use search::SearchQuery;
pub use sqlite::{MessageRow, SearchHit, Store, StoreError};
