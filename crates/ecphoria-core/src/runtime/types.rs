//! The run *types* — plain data, no storage.
//!
//! Split from the ledger because they are also the **Raft wire format**: `AppRequest::RunCreate`
//! and `RunUpdate` carry a `Run`/`RunPatch`, encoded positionally by MessagePack. A node built
//! without the `agentic` feature still has to deserialize an entry it will not act on, so these
//! types compile unconditionally and the variants never move. Gating them would make a memory-only
//! node's log format quietly incompatible with a full node's, which is the kind of divergence that
//! surfaces months later as an unexplained apply failure.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Lifecycle status of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    #[default]
    Pending,
    Running,
    /// Paused awaiting a human-in-the-loop approval.
    WaitingApproval,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Pending => "pending",
            RunStatus::Running => "running",
            RunStatus::WaitingApproval => "waiting_approval",
            RunStatus::Succeeded => "succeeded",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
        }
    }

    /// Parse a stored status. Used by the ledger, which is a sibling module and only exists with
    /// the `agentic` feature — hence the cfg, rather than a dead-code allow that would also hide a
    /// real disuse later.
    #[cfg(feature = "agentic")]
    pub(crate) fn from_str(s: &str) -> Self {
        match s {
            "running" => RunStatus::Running,
            "waiting_approval" => RunStatus::WaitingApproval,
            "succeeded" => RunStatus::Succeeded,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            _ => RunStatus::Pending,
        }
    }

    /// A terminal run no longer makes progress (a dispatcher can stop driving it).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
        )
    }
}

fn default_tenant() -> String {
    "default".into()
}

/// A durable agent/workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: Uuid,
    #[serde(default = "default_tenant")]
    pub tenant_id: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub parent_run_id: Option<Uuid>,
    pub status: RunStatus,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default)]
    pub result: serde_json::Value,
    #[serde(default)]
    pub error: Option<String>,
    /// Opaque driver position (e.g. the next workflow node) — reconstructable run state.
    #[serde(default)]
    pub cursor: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
}

/// A partial update to a run — only the `Some` fields change. Carries materialized values (the
/// `updated_at` is supplied separately by the writer) so it is deterministic to replicate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunPatch {
    #[serde(default)]
    pub status: Option<RunStatus>,
    #[serde(default)]
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub cursor: Option<serde_json::Value>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
}

/// A node in a workflow DAG: a sub-agent invocation gated on `deps` (other node ids).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub id: String,
    pub agent_id: String,
    pub question: String,
    #[serde(default)]
    pub deps: Vec<String>,
}
