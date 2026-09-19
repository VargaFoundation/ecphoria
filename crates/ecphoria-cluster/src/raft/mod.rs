pub mod network;
pub mod server;
pub mod store;
pub mod tls;
pub mod types;

/// Generated gRPC types for the inter-node Raft transport (from `proto/raft.proto`).
///
/// `result_large_err` fires on every generated service method, because `tonic::Status` is 176
/// bytes and tonic returns it by value. That is tonic's design, not ours, and the code is
/// regenerated on each build — so the lint is silenced here rather than left to fail the build
/// on whichever toolchain version starts enforcing it.
#[allow(clippy::result_large_err)]
pub mod pb {
    tonic::include_proto!("ecphoria.raft");
}
