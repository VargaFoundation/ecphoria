//! Outbound CDC sink — mirror memory + state lifecycle changes to a downstream system.
//!
//! When `gateway.cdc_sink_url` is set, every **memory** change (upserted / superseded / expired,
//! from [`EcphoriaEngine::memory_subscribe`](ecphoria_core::EcphoriaEngine::memory_subscribe)) and
//! every **state** change (set / deleted, from
//! [`EcphoriaEngine::state_subscribe`](ecphoria_core::EcphoriaEngine::state_subscribe)) is POSTed as
//! JSON to that URL. Each payload carries a top-level `"stream"` discriminator (`"memory"` or
//! `"state"`) alongside the change's own fields, so the downstream can route it. Feed it to a search
//! index, warehouse, or event bus.
//!
//! **Coverage:** cognition memories + state KV. Episodic *event ingest* is intentionally NOT mirrored
//! here — it has no change-broadcast stream; consume it via `/query` (SQL) or the
//! `/api/v1/memories/watch` WebSocket for derived memories.
//!
//! Delivery semantics:
//! - **Leader-gated in cluster mode**: both streams fire on *every* node's Raft apply, so without
//!   gating N nodes would each deliver the same event. The sink only ships when this node is the
//!   leader (or when running single-node), giving at-least-once delivery from one emitter.
//! - **Best-effort with bounded retry**: each POST is retried a few times with backoff; a change
//!   that still fails is logged and dropped rather than blocking the stream (a slow sink must not
//!   stall writes). A lagging broadcast receiver likewise drops the oldest events.
//! - **Trusted endpoint**: the URL is operator configuration, not user input, so no SSRF guard is
//!   applied (unlike the MCP tool-gateway, whose targets are agent-controlled).

use std::sync::Arc;
use std::time::Duration;

use ecphoria_core::EcphoriaEngine;
use tokio::sync::RwLock;

use ecphoria_cluster::ClusterCoordinator;

/// One normalized change ready to ship: the JSON `body` (the change's fields plus a `"stream"` tag)
/// with `id`/`event` pulled out for logging.
struct Change {
    id: String,
    event: String,
    body: serde_json::Value,
}

/// Serialize a change to JSON, keeping the change's own fields at the top level and adding a
/// `"stream"` discriminator and a normalized `"event"` — so both streams share a
/// `{"stream":…,"event":…,…}` shape (memory: upserted/superseded/expired; state: set/deleted).
/// Returns `None` if the change can't be serialized (never expected for these types).
fn envelope<T: serde::Serialize>(
    stream: &'static str,
    id: String,
    event: String,
    change: &T,
) -> Option<Change> {
    let mut body = serde_json::to_value(change)
        .map_err(|e| tracing::warn!(error = %e, stream, "CDC change failed to serialize; dropped"))
        .ok()?;
    match &mut body {
        serde_json::Value::Object(map) => {
            map.insert("stream".into(), serde_json::Value::String(stream.into()));
            map.insert("event".into(), serde_json::Value::String(event.clone()));
        }
        // Non-object (never for MemoryChange/StateChange) — wrap so the tags aren't lost.
        other => {
            body = serde_json::json!({ "stream": stream, "event": event, "change": other });
        }
    }
    Some(Change { id, event, body })
}

/// Spawn the outbound CDC sink task. No-op if `url` is empty. Returns immediately; the task runs
/// until the process exits.
pub fn spawn(
    engine: Arc<EcphoriaEngine>,
    url: String,
    coordinator: Option<Arc<RwLock<ClusterCoordinator>>>,
) {
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        let mut mem_rx = engine.memory_subscribe();
        let mut state_rx = engine.state_subscribe();
        tracing::info!(%url, "outbound CDC sink enabled (memory + state)");
        loop {
            // Multiplex both change streams; each arm normalizes to a tagged `Change`.
            let change = tokio::select! {
                r = mem_rx.recv() => match r {
                    Ok(c) => envelope("memory", c.id.to_string(), c.event.to_string(), &c),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(dropped = n, stream = "memory", "CDC sink lagged; some changes not delivered");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
                r = state_rx.recv() => match r {
                    Ok(c) => {
                        let event = if c.deleted { "deleted" } else { "set" };
                        envelope("state", format!("{}/{}", c.agent_id, c.key), event.to_string(), &c)
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(dropped = n, stream = "state", "CDC sink lagged; some changes not delivered");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
            };
            let Some(change) = change else { continue };

            // Deliver from a single emitter: only the leader ships (every node sees the change).
            let is_leader = match &coordinator {
                Some(c) => c.read().await.is_leader(),
                None => true,
            };
            if !is_leader {
                continue;
            }

            deliver(&client, &url, &change).await;
        }
    });
}

/// POST one change with bounded retry + backoff. Best-effort: gives up after the last attempt.
async fn deliver(client: &reqwest::Client, url: &str, change: &Change) {
    const MAX_ATTEMPTS: u32 = 3;
    for attempt in 1..=MAX_ATTEMPTS {
        match client.post(url).json(&change.body).send().await {
            Ok(resp) if resp.status().is_success() => return,
            Ok(resp) => {
                tracing::warn!(
                    status = %resp.status(),
                    attempt,
                    id = %change.id,
                    "CDC sink returned non-success"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, id = %change.id, "CDC sink POST failed");
            }
        }
        if attempt < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
        }
    }
    tracing::warn!(id = %change.id, event = %change.event, "CDC change dropped after retries");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    async fn inmem_engine() -> Arc<EcphoriaEngine> {
        let mut c = ecphoria_core::CoreConfig::default();
        c.memory.episodic.db_path = ":memory:".into();
        c.memory.state.db_path = ":memory:".into();
        c.memory.cognition.db_path = ":memory:".into();
        Arc::new(EcphoriaEngine::new(c).await.unwrap())
    }

    /// End-to-end: a memory_add on the engine is POSTed to the sink URL as a JSON change (single
    /// node → no coordinator → always "leader", so it ships).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sink_delivers_memory_change() {
        use axum::{extract::State, routing::post, Json, Router};

        // Mock sink: record every received change body.
        let received: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route(
                "/cdc",
                post(
                    |State(store): State<Arc<Mutex<Vec<serde_json::Value>>>>,
                     Json(body): Json<serde_json::Value>| async move {
                        store.lock().unwrap().push(body);
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .with_state(received.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let engine = inmem_engine().await;
        spawn(engine.clone(), format!("http://{addr}/cdc"), None);
        // Give the subscriber task a moment to attach before we emit.
        tokio::time::sleep(Duration::from_millis(100)).await;

        engine
            .memory_add(ecphoria_core::memory::cognition::MemoryInput::new(
                ecphoria_core::memory::cognition::MemoryScope::user("alice"),
                "alice likes tea",
            ))
            .await
            .unwrap();

        // Poll for delivery.
        let mut got = None;
        for _ in 0..50 {
            if let Some(v) = received.lock().unwrap().first().cloned() {
                got = Some(v);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let change = got.expect("CDC sink should have received a change");
        assert_eq!(change["stream"], "memory");
        assert_eq!(change["event"], "upserted");
        assert_eq!(change["user_id"], "alice");
    }

    /// End-to-end: a state_set on the engine is POSTed to the sink as a `"state"`-tagged change.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sink_delivers_state_change() {
        use axum::{extract::State, routing::post, Json, Router};

        let received: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route(
                "/cdc",
                post(
                    |State(store): State<Arc<Mutex<Vec<serde_json::Value>>>>,
                     Json(body): Json<serde_json::Value>| async move {
                        store.lock().unwrap().push(body);
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .with_state(received.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let engine = inmem_engine().await;
        spawn(engine.clone(), format!("http://{addr}/cdc"), None);
        tokio::time::sleep(Duration::from_millis(100)).await;

        engine
            .state_set("agent-1", "cursor", serde_json::json!({"page": 7}))
            .await
            .unwrap();

        // Poll for a state-tagged change (ignore any memory changes that might also arrive).
        let mut got = None;
        for _ in 0..50 {
            if let Some(v) = received
                .lock()
                .unwrap()
                .iter()
                .find(|v| v["stream"] == "state")
                .cloned()
            {
                got = Some(v);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let change = got.expect("CDC sink should have received a state change");
        assert_eq!(change["stream"], "state");
        assert_eq!(change["event"], "set"); // normalized from `deleted=false`
        assert_eq!(change["agent_id"], "agent-1");
        assert_eq!(change["key"], "cursor");
        assert_eq!(change["value"]["page"], 7);
        assert_eq!(change["deleted"], false);
    }
}
