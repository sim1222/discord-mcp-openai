//! `discord-reader-daemon` library surface (used by tests and by the binary).

pub mod api;
pub(crate) mod message_lookup;
pub(crate) mod metadata_refetch;
pub mod rpc;

pub use api::DaemonApi;
pub use rpc::{ReaderApi, RpcError, RpcRequest, RpcResponse};
