pub mod authz;
pub mod config;
pub mod embedded;
pub mod embedding;
pub mod engine;
pub mod error;
pub mod ingest;
pub mod llm;
pub mod materialized;
pub mod memory;
pub mod query;
pub mod rerank;
/// Agent-runtime substrate: run types (always — they are the Raft wire format), plus the ledger
/// and driver seams behind the `agentic` feature.
pub mod runtime;
pub mod storage;

pub use config::CoreConfig;
pub use embedded::Ecphoria;
pub use engine::{
    ContradictionGroup, EcphoriaEngine, FeedbackAction, MemoryChange, MemoryFeedback,
    MemoryProvenance,
};
pub use error::{Error, Result};
