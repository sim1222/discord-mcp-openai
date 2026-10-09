//! `discord-reader-daemon` library surface (used by tests and by the binary).

pub(crate) mod account_sync;
pub mod api;
pub(crate) mod inbox_coverage;
pub(crate) mod message_lookup;
pub(crate) mod metadata_refetch;
pub(crate) mod read_state;
pub mod rpc;
pub(crate) mod thread_listing;

pub use api::DaemonApi;
pub use rpc::{ReaderApi, RpcError, RpcRequest, RpcResponse};
