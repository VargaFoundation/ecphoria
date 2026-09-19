//! Leader forwarding middleware — gets writes to the Raft leader.
//!
//! In a Raft cluster only the leader accepts writes, and a client behind a Service reaches a
//! follower (N-1)/N of the time. So a follower **proxies** the write to the leader and returns its
//! answer: the client neither knows nor cares which pod it hit, which is the only behaviour that
//! works with an ordinary HTTP client.
//!
//! That requires knowing the leader's *HTTP* address, which Raft does not provide — a peer is
//! `id@http://host:9433`, the Raft port. `cluster.peer_http` supplies the mapping (the Helm chart
//! fills it from the same headless DNS it builds `peers` from).
//!
//! Without it, the fallback is the historical behaviour: a 307 carrying `leader_id` in the body and
//! **no `Location`** — which no HTTP client can follow, so the caller has to implement leader
//! discovery itself. That is why the mapping is worth configuring.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use ecphoria_cluster::ClusterCoordinator;
use tokio::sync::RwLock;

/// Marks a request this node already forwarded, so two followers cannot bounce it between them
/// during an election. A forwarded request that lands on a non-leader is answered, not re-sent.
const LEADER_FORWARD_MARKER: &str = "x-ecphoria-leader-forwarded";

/// Shared cluster state for the leader-forwarding middleware.
#[derive(Clone)]
pub struct ClusterState {
    pub coordinator: Arc<RwLock<ClusterCoordinator>>,
    /// `node_id` → HTTP base URL, from `cluster.peer_http`. Empty = fall back to the info-only 307.
    pub peer_http: Arc<HashMap<u64, String>>,
    /// Client for the proxy hop. Shared so connections are reused rather than rebuilt per write.
    pub client: reqwest::Client,
}

impl ClusterState {
    pub fn new(
        coordinator: Arc<RwLock<ClusterCoordinator>>,
        peer_http: HashMap<u64, String>,
    ) -> Self {
        Self {
            coordinator,
            peer_http: Arc::new(peer_http),
            // A write that has to cross one hop should still fail fast rather than hang a client.
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }
}

/// Axum middleware that checks if this node is the Raft leader.
///
/// - For read requests (GET), passes through to the local engine (follower reads).
/// - For write requests (POST, PUT, DELETE), checks leadership:
///   - If leader: passes through.
///   - If follower: returns 307 Temporary Redirect with the leader's address.
///   - If no leader known: returns 503 Service Unavailable.
pub async fn require_leader_for_writes(
    State(state): State<ClusterState>,
    req: Request,
    next: Next,
) -> Response {
    // Reads are always served locally (C6: follower reads).
    if req.method() == axum::http::Method::GET || is_read_only_post(&req) {
        return next.run(req).await;
    }

    // Writes need to go to the leader
    let coordinator = state.coordinator.read().await;

    if coordinator.is_leader() {
        drop(coordinator);
        return next.run(req).await;
    }

    // Not the leader.
    match coordinator.leader_id() {
        Some(leader_id) => {
            let base = state.peer_http.get(&leader_id).cloned();
            drop(coordinator);
            match base {
                // Already forwarded once: answering beats bouncing it around a cluster that is
                // mid-election. The client sees the 307 and can retry, which is a better failure
                // than a request ricocheting between two followers until it times out.
                Some(_) if req.headers().contains_key(LEADER_FORWARD_MARKER) => {
                    metrics::counter!("ecphoria_leader_forward_total", "outcome" => "already_forwarded")
                        .increment(1);
                    not_leader_307(leader_id)
                }
                Some(base) => {
                    metrics::counter!("ecphoria_leader_forward_total", "outcome" => "proxied")
                        .increment(1);
                    crate::cluster::shard_route::proxy(
                        &state.client,
                        &base,
                        req,
                        LEADER_FORWARD_MARKER,
                        None,
                    )
                    .await
                }
                // No mapping configured — the historical answer.
                None => {
                    metrics::counter!("ecphoria_leader_forward_total", "outcome" => "redirected")
                        .increment(1);
                    not_leader_307(leader_id)
                }
            }
        }
        None => {
            metrics::counter!("ecphoria_leader_forward_total", "outcome" => "no_leader")
                .increment(1);
            let body = serde_json::json!({
                "error": "no_leader",
                "message": "No leader elected yet. Retry later.",
            });
            (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response()
        }
    }
}

/// POST routes that only read.
///
/// Classifying by HTTP method alone made every search a "write": `POST /memories/search`,
/// `/context-pack` and `/query` were proxied to the leader, which concentrated the whole fleet's
/// *read* load on one node and — worse — made retrieval unavailable during an election, when the
/// followers holding a perfectly good replica answered 503 for want of a leader. A read should
/// never need a leader.
///
/// The list is an allowlist and stays one: classifying a write as a read would send it to a
/// follower, where it would apply locally and never replicate. Everything not named here is
/// treated as a write.
///
/// One consequence worth naming: with `memory.query_log.enabled`, a search records an episodic
/// event, and that record is written on whichever node served the search. It was already
/// node-local (the query log does not go through Raft), so this spreads those rows across the
/// fleet rather than piling them on the leader — a difference in where telemetry lands, not in
/// replicated state.
fn is_read_only_post(req: &Request) -> bool {
    const READ_ONLY: [&str; 6] = [
        "/query",  // SELECT-only, enforced by the SQL guard
        "/search", // semantic vector search
        "/embed-and-search",
        "/memories/search",
        "/context-pack",
        "/attachments/search-image",
    ];
    if req.method() != axum::http::Method::POST {
        return false;
    }
    // This middleware sits on the router nested under `/api/v1`, so `uri()` is the stripped path.
    // `OriginalUri` is checked too, for the case where the layer is mounted un-nested.
    let stripped = req.uri().path();
    let original = req
        .extensions()
        .get::<axum::extract::OriginalUri>()
        .map(|o| o.0.path().to_string());
    READ_ONLY.iter().any(|p| {
        stripped == *p
            || original
                .as_deref()
                .is_some_and(|o| o == *p || o.ends_with(&format!("/api/v1{p}")))
    })
}

/// The fallback when this node cannot reach the leader on its behalf: a 307 that names the leader
/// by **id**. Deliberately without a `Location` — this node does not know the leader's HTTP
/// address, and inventing one would send clients somewhere that may not answer.
fn not_leader_307(leader_id: u64) -> Response {
    let body = serde_json::json!({
        "error": "not_leader",
        "leader_id": leader_id,
        "message": "This node is not the leader. Retry on the leader node, or configure                     cluster.peer_http so followers can forward writes for you.",
    });
    (StatusCode::TEMPORARY_REDIRECT, axum::Json(body)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_state_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ClusterState>();
    }

    fn post(path: &str) -> Request {
        Request::builder()
            .method("POST")
            .uri(path)
            .body(axum::body::Body::empty())
            .unwrap()
    }

    #[test]
    fn searches_are_reads_even_though_they_are_posts() {
        // The ones that used to be proxied to the leader — and answered 503 during an election —
        // for no reason other than their HTTP method.
        for path in [
            "/query",
            "/search",
            "/embed-and-search",
            "/memories/search",
            "/context-pack",
            "/attachments/search-image",
        ] {
            assert!(is_read_only_post(&post(path)), "{path} should be a read");
        }
    }

    #[test]
    fn everything_else_is_a_write() {
        // An allowlist, and the cost of getting it wrong is asymmetric: a write mistaken for a read
        // is applied on a follower and never replicates. These are the near-misses.
        for path in [
            "/memories",
            "/memories/batch",
            "/memories/by-external-id",
            "/memories/from-template",
            "/memories/contradictions/resolve",
            "/memories/link",
            "/documents",
            "/documents/prune",
            "/ingest",
            "/runs",
            "/agents/run",
            "/pending/abc/accept",
            "/admin/backup",
            "/attachments",
            "/query/something-else",
            "/memories/search/extra",
        ] {
            assert!(!is_read_only_post(&post(path)), "{path} must be a write");
        }
    }

    #[test]
    fn a_read_only_path_under_another_method_is_still_a_write() {
        for method in ["PUT", "DELETE", "PATCH"] {
            let req = Request::builder()
                .method(method)
                .uri("/query")
                .body(axum::body::Body::empty())
                .unwrap();
            assert!(!is_read_only_post(&req), "{method} /query");
        }
    }

    #[test]
    fn the_full_path_is_recognised_when_the_layer_is_not_nested() {
        let mut req = post("/api/v1/memories/search");
        req.extensions_mut().insert(axum::extract::OriginalUri(
            "/api/v1/memories/search".parse().unwrap(),
        ));
        assert!(is_read_only_post(&req));
    }
}
