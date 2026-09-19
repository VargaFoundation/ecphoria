pub mod convert;
pub mod service;

/// Generated protobuf types from proto/ecphoria.proto.
/// `result_large_err` fires on every generated service method: `tonic::Status` is
/// 176 bytes and tonic returns it by value. That is tonic's design, and the code is
/// regenerated on each build — silence it here rather than let it fail the build on
/// whichever toolchain starts enforcing the lint.
#[allow(clippy::result_large_err)]
pub mod proto {
    tonic::include_proto!("ecphoria");
}
