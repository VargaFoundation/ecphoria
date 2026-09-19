//! Agentic-platform runtime substrate.
//!
//! The durable **agent-run ledger** ([`store::RunStore`]): runs carry status + cursor +
//! input/result, their steps are episodic events (`session_id = run_id`). The orchestration driver
//! (agent loop, scheduler, tool gateway) builds on this, in `engine/agentic.rs`.
//!
//! Only the *types* and the two injection traits compile without the `agentic` feature: they are
//! the Raft wire format and the seams the cluster and gateway implement, and a memory-only node
//! still has to read a log entry it will not act on.

pub mod replicate;
pub mod tools;
pub mod types;

/// The SQLite-backed ledger itself. Feature `agentic`.
#[cfg(feature = "agentic")]
pub mod store;

pub use replicate::RunReplicator;
pub use tools::ToolExecutor;
pub use types::{Run, RunPatch, RunStatus, WorkflowNode};

#[cfg(feature = "agentic")]
pub use store::RunStore;
