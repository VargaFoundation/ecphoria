//! Raft RPC HTTP endpoints — receives RPCs from peer nodes.
//!
//! These endpoints are called by `NetworkClient` on remote nodes.
//! They forward the requests to the local `openraft::Raft` instance.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use strata_cluster::coordinator::StrataRaft;

/// Shared state for Raft RPC handlers.
#[derive(Clone)]
pub struct RaftState {
    pub raft: Arc<StrataRaft>,
}

/// POST /raft/append — AppendEntries RPC
pub async fn append_entries(
    State(state): State<RaftState>,
    Json(rpc): Json<
        openraft::raft::AppendEntriesRequest<strata_cluster::raft::types::TypeConfig>,
    >,
) -> Result<
    Json<openraft::raft::AppendEntriesResponse<strata_cluster::raft::types::NodeId>>,
    StatusCode,
> {
    state
        .raft
        .append_entries(rpc)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// POST /raft/vote — RequestVote RPC
pub async fn vote(
    State(state): State<RaftState>,
    Json(rpc): Json<openraft::raft::VoteRequest<strata_cluster::raft::types::NodeId>>,
) -> Result<
    Json<openraft::raft::VoteResponse<strata_cluster::raft::types::NodeId>>,
    StatusCode,
> {
    state
        .raft
        .vote(rpc)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// POST /raft/snapshot — InstallSnapshot RPC
pub async fn install_snapshot(
    State(state): State<RaftState>,
    Json(rpc): Json<
        openraft::raft::InstallSnapshotRequest<strata_cluster::raft::types::TypeConfig>,
    >,
) -> Result<
    Json<openraft::raft::InstallSnapshotResponse<strata_cluster::raft::types::NodeId>>,
    StatusCode,
> {
    state
        .raft
        .install_snapshot(rpc)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// GET /cluster/status — Cluster health and Raft metrics
pub async fn cluster_status(
    State(state): State<RaftState>,
) -> Json<serde_json::Value> {
    let metrics = state.raft.metrics().borrow().clone();
    Json(serde_json::json!({
        "node_id": metrics.id,
        "state": format!("{:?}", metrics.state),
        "current_leader": metrics.current_leader,
        "current_term": metrics.current_term,
        "last_log_index": metrics.last_log_index,
        "last_applied": metrics.last_applied.map(|id| id.index),
        "membership": format!("{:?}", metrics.membership_config),
    }))
}

/// Build the Raft RPC router.
pub fn raft_router(raft: Arc<StrataRaft>) -> axum::Router {
    let state = RaftState { raft };

    axum::Router::new()
        .route("/raft/append", axum::routing::post(append_entries))
        .route("/raft/vote", axum::routing::post(vote))
        .route("/raft/snapshot", axum::routing::post(install_snapshot))
        .route("/cluster/status", axum::routing::get(cluster_status))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_state_is_clone() {
        // Can't test without a Raft instance, but verify the struct is Clone
        fn assert_clone<T: Clone>() {}
        assert_clone::<RaftState>();
    }
}
