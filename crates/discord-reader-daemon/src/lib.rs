//! `discord-reader-daemon` library surface (used by tests and by the binary).

pub mod api;
pub mod rpc;

pub use api::DaemonApi;
pub use rpc::{ReaderApi, RpcError, RpcRequest, RpcResponse};
