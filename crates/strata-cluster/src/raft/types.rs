//! Raft type definitions for openraft integration.

use std::io::Cursor;

use serde::{Deserialize, Serialize};

/// Node identifier in the Raft cluster.
pub type NodeId = u64;

/// Application-level request data sent through Raft consensus.
///
/// Serialized as MessagePack for compact over-the-wire representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppRequest {
    /// Ingest events into episodic memory.
    Ingest {
        source: String,
        events: Vec<serde_json::Value>,
    },
    /// Set agent state.
    StateSet {
        agent_id: String,
        key: String,
        value: serde_json::Value,
    },
    /// Delete agent state.
    StateDelete { agent_id: String, key: String },
    /// Upsert a semantic entry (pre-embedded).
    SemanticUpsert {
        id: uuid::Uuid,
        content: String,
        embedding: Vec<f32>,
        metadata: serde_json::Value,
    },
    /// Delete a semantic entry.
    SemanticDelete { id: uuid::Uuid },
}

/// Application-level response from applying a Raft log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppResponse {
    /// Number of events ingested.
    Ingested(u64),
    /// New version of state entry.
    StateVersion(u64),
    /// State deleted.
    Deleted,
    /// Semantic entry upserted/deleted.
    Ok,
}

/// Cluster node info for openraft membership.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// HTTP address for Raft RPC (e.g., "http://10.0.0.1:9433").
    pub addr: String,
}

impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

// NodeInfo automatically implements openraft::Node via blanket impl
// (requires: Sized + Send + Sync + Eq + Debug + Clone + Default + Serialize + Deserialize)

// Use the openraft macro to declare the type configuration.
openraft::declare_raft_types!(
    /// Strata's Raft type configuration.
    pub TypeConfig:
        D = AppRequest,
        R = AppResponse,
        NodeId = NodeId,
        Node = NodeInfo,
        Entry = openraft::Entry<Self>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = openraft::impls::OneshotResponder<Self>,
);

/// Snapshot data for Raft state transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    pub data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_request_roundtrip() {
        let req = AppRequest::Ingest {
            source: "test".into(),
            events: vec![serde_json::json!({"key": "val"})],
        };
        let bytes = rmp_serde::to_vec(&req).unwrap();
        let decoded: AppRequest = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            AppRequest::Ingest { source, events } => {
                assert_eq!(source, "test");
                assert_eq!(events.len(), 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn app_response_roundtrip() {
        let resp = AppResponse::Ingested(42);
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: AppResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            AppResponse::Ingested(n) => assert_eq!(n, 42),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn node_info_display() {
        let node = NodeInfo {
            addr: "http://10.0.0.1:9433".into(),
        };
        assert_eq!(format!("{node}"), "http://10.0.0.1:9433");
    }
}
