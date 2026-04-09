//! REST API handler functions.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use strata_core::StrataEngine;

use super::models::*;

// ── Health (stateless) ──────────────────────────────────────────────

/// Health check endpoint.
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
    })
}

// ── Stub handlers (no engine — for testing router shape) ────────────

pub async fn query_no_engine(Json(_req): Json<QueryRequest>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "rows": [], "count": 0 }))
}

pub async fn ingest_no_engine(Json(_req): Json<IngestRequest>) -> Json<IngestResponse> {
    Json(IngestResponse { ingested: 0 })
}

pub async fn search_no_engine(Json(_req): Json<SearchRequest>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "results": [] }))
}

// ── Engine-backed handlers ──────────────────────────────────────────

/// Execute a SQL query against the engine.
pub async fn query(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<QueryRequest>,
) -> Json<serde_json::Value> {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "query").increment(1);
    let start = std::time::Instant::now();

    let result = match engine.query_sql(&req.sql).await {
        Ok(rows) => {
            let count = rows.len();
            Json(serde_json::json!({ "rows": rows, "count": count }))
        }
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    };

    metrics::histogram!("strata_rest_request_duration_seconds", "endpoint" => "query")
        .record(start.elapsed().as_secs_f64());
    result
}

/// Ingest events into the engine.
pub async fn ingest(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<IngestRequest>,
) -> Json<IngestResponse> {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "ingest").increment(1);
    let events: Vec<strata_core::memory::episodic::Event> = req
        .events
        .into_iter()
        .map(|payload| strata_core::memory::episodic::Event {
            id: uuid::Uuid::new_v4(),
            source: req.source.clone(),
            event_type: payload
                .get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            payload,
            timestamp: chrono::Utc::now(),
        })
        .collect();

    match engine.ingest(events).await {
        Ok(count) => Json(IngestResponse { ingested: count }),
        Err(_) => Json(IngestResponse { ingested: 0 }),
    }
}

/// Webhook ingestion — normalizes vendor payloads into Strata events.
pub async fn webhook(
    State(engine): State<Arc<StrataEngine>>,
    Path(source): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    match strata_core::ingest::webhook::normalize_webhook(&source, &payload) {
        Ok(events) => {
            let count = events.len();
            match engine.ingest(events).await {
                Ok(ingested) => Json(serde_json::json!({
                    "source": source,
                    "normalized": count,
                    "ingested": ingested,
                })),
                Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
            }
        }
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

/// Semantic search against the engine.
pub async fn search(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<SearchRequest>,
) -> Json<serde_json::Value> {
    // For now, we need a vector to search. If the request has a "vector" field, use it.
    // Otherwise, return empty (embedding integration will come later).
    if let Some(vector) = req.vector {
        match engine.semantic_search(&vector, req.k).await {
            Ok(results) => {
                let items: Vec<serde_json::Value> = results
                    .iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.entry.id.to_string(),
                            "content": r.entry.content,
                            "metadata": r.entry.metadata,
                            "score": r.score,
                        })
                    })
                    .collect();
                Json(serde_json::json!({ "results": items }))
            }
            Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
        }
    } else {
        // Text-based search would require embedding first — future enhancement
        Json(serde_json::json!({ "results": [] }))
    }
}

/// Get agent state.
pub async fn state_get(
    State(engine): State<Arc<StrataEngine>>,
    Path((agent_id, key)): Path<(String, String)>,
) -> Json<serde_json::Value> {
    match engine.state_get(&agent_id, &key).await {
        Ok(Some(entry)) => Json(serde_json::json!({
            "agent_id": entry.agent_id,
            "key": entry.key,
            "value": entry.value,
            "version": entry.version,
        })),
        Ok(None) => Json(serde_json::json!({ "error": "not found" })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

/// Set agent state.
pub async fn state_set(
    State(engine): State<Arc<StrataEngine>>,
    Path((agent_id, key)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    match engine.state_set(&agent_id, &key, body).await {
        Ok(version) => Json(serde_json::json!({ "version": version })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

/// Prometheus metrics endpoint.
pub async fn metrics() -> (StatusCode, String) {
    match metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle()
        .render()
    {
        output if !output.is_empty() => (StatusCode::OK, output),
        _ => (StatusCode::OK, "# No metrics collected yet\n".to_string()),
    }
}
