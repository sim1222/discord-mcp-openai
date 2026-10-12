//! `discord-reader-daemon` library surface (used by tests and by the binary).
#![recursion_limit = "256"]

pub(crate) mod account_sync;
pub mod api;
pub(crate) mod attention;
pub(crate) mod inbox_coverage;
pub mod local_state;
pub(crate) mod message_lookup;
pub(crate) mod metadata_refetch;
pub(crate) mod read_snapshot;
pub(crate) mod read_state;
pub mod rpc;
pub(crate) mod status_query;
pub(crate) mod thread_listing;

pub use api::DaemonApi;
pub use rpc::{ReaderApi, RpcError, RpcRequest, RpcResponse};
