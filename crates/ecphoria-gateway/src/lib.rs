pub mod auth;
pub mod cdc;
pub mod cluster;
pub mod error;
pub mod grpc;
/// OpenAI-/Anthropic-compatible proxy with auto-RAG and the semantic response cache.
/// Feature `llm-proxy`: without it the gateway makes no outbound completion call for a client.
#[cfg(feature = "llm-proxy")]
pub mod llm_proxy;
pub mod mcp;
pub mod pg_wire;
pub mod rest;
pub mod server;

pub use error::{Error, Result};
pub use server::GatewayServer;
