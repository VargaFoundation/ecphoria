//! REST API handler functions with proper HTTP status codes and request IDs.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use strata_core::StrataEngine;

use super::models::*;

// ── Error response helper ──────────────────────────────────────────

/// Structured API error response with proper HTTP status codes.
fn api_error(status: StatusCode, code: &str, message: String) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let body = serde_json::json!({
        "error": {
            "code": code,
            "message": message,
            "request_id": request_id,
        }
    });
    (status, Json(body)).into_response()
}

fn api_ok(body: serde_json::Value) -> Response {
    (StatusCode::OK, Json(body)).into_response()
}

// ── Health (stateless) ──────────────────────────────────────────────

/// Health check endpoint.
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
    })
}

// ── Stub handlers (no engine — for testing router shape) ────────────

pub async fn query_no_engine(Json(_req): Json<QueryRequest>) -> Response {
    api_ok(serde_json::json!({ "rows": [], "count": 0 }))
}

pub async fn ingest_no_engine(Json(_req): Json<IngestRequest>) -> Response {
    api_ok(serde_json::json!({ "ingested": 0 }))
}

pub async fn search_no_engine(Json(_req): Json<SearchRequest>) -> Response {
    api_ok(serde_json::json!({ "results": [] }))
}

// ── Engine-backed handlers ──────────────────────────────────────────

/// Execute a SQL query against the engine.
pub async fn query(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<QueryRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "query").increment(1);
    let start = std::time::Instant::now();

    let result = match engine.query_sql(&req.sql).await {
        Ok(rows) => {
            let count = rows.len();
            api_ok(serde_json::json!({ "rows": rows, "count": count }))
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("only SELECT") || msg.contains("SQL parse") {
                api_error(StatusCode::UNPROCESSABLE_ENTITY, "INVALID_QUERY", msg)
            } else if msg.contains("timed out") {
                api_error(StatusCode::REQUEST_TIMEOUT, "QUERY_TIMEOUT", msg)
            } else {
                api_error(StatusCode::INTERNAL_SERVER_ERROR, "QUERY_ERROR", msg)
            }
        }
    };

    metrics::histogram!("strata_rest_request_duration_seconds", "endpoint" => "query")
        .record(start.elapsed().as_secs_f64());
    result
}

/// Ingest events into the engine.
pub async fn ingest(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<IngestRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "ingest").increment(1);
    let start = std::time::Instant::now();

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
            parent_id: None,
            trace_id: None,
            tags: vec![],
        })
        .collect();

    let result = match engine.ingest(events).await {
        Ok(count) => api_ok(serde_json::json!({ "ingested": count })),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "INGEST_ERROR", e.to_string()),
    };

    metrics::histogram!("strata_rest_request_duration_seconds", "endpoint" => "ingest")
        .record(start.elapsed().as_secs_f64());
    result
}

/// Webhook ingestion — normalizes vendor payloads into Strata events.
pub async fn webhook(
    State(engine): State<Arc<StrataEngine>>,
    Path(source): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "webhook").increment(1);

    match strata_core::ingest::webhook::normalize_webhook(&source, &payload) {
        Ok(events) => {
            let count = events.len();
            match engine.ingest(events).await {
                Ok(ingested) => api_ok(serde_json::json!({
                    "source": source,
                    "normalized": count,
                    "ingested": ingested,
                })),
                Err(e) => api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INGEST_ERROR",
                    e.to_string(),
                ),
            }
        }
        Err(e) => api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "WEBHOOK_NORMALIZE_ERROR",
            e.to_string(),
        ),
    }
}

/// Semantic search against the engine.
pub async fn search(
    State(engine): State<Arc<StrataEngine>>,
    Json(req): Json<SearchRequest>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "search").increment(1);
    let start = std::time::Instant::now();

    let result = if let Some(vector) = req.vector {
        let search_result = if let Some(ref filters) = req.filters {
            engine
                .semantic_search_filtered(
                    &vector,
                    req.k,
                    filters.source.as_deref(),
                    filters.event_type.as_deref(),
                )
                .await
        } else {
            engine.semantic_search(&vector, req.k).await
        };
        match search_result {
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
                api_ok(serde_json::json!({ "results": items }))
            }
            Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "SEARCH_ERROR", e.to_string()),
        }
    } else {
        api_ok(serde_json::json!({ "results": [] }))
    };

    metrics::histogram!("strata_rest_request_duration_seconds", "endpoint" => "search")
        .record(start.elapsed().as_secs_f64());
    result
}

/// Get agent state.
pub async fn state_get(
    State(engine): State<Arc<StrataEngine>>,
    Path((agent_id, key)): Path<(String, String)>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "state_get").increment(1);

    match engine.state_get(&agent_id, &key).await {
        Ok(Some(entry)) => api_ok(serde_json::json!({
            "agent_id": entry.agent_id,
            "key": entry.key,
            "value": entry.value,
            "version": entry.version,
        })),
        Ok(None) => api_error(StatusCode::NOT_FOUND, "NOT_FOUND", format!("state key '{key}' not found for agent '{agent_id}'")),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "STATE_ERROR", e.to_string()),
    }
}

/// Set agent state.
pub async fn state_set(
    State(engine): State<Arc<StrataEngine>>,
    Path((agent_id, key)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "state_set").increment(1);

    match engine.state_set(&agent_id, &key, body).await {
        Ok(version) => api_ok(serde_json::json!({ "version": version })),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "STATE_ERROR", e.to_string()),
    }
}

// ── Admin endpoints ─────────────────────────────────────────────────

/// Enforce data retention policy — delete events older than configured retention period.
pub async fn enforce_retention(
    State(engine): State<Arc<StrataEngine>>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "retention").increment(1);

    match engine.enforce_retention().await {
        Ok(deleted) => api_ok(serde_json::json!({
            "deleted": deleted,
            "retention_days": engine.config().memory.episodic.default_retention_days,
        })),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "RETENTION_ERROR", e.to_string()),
    }
}

/// Trigger a backup of all stores to the configured data directory.
pub async fn backup(
    State(engine): State<Arc<StrataEngine>>,
) -> Response {
    metrics::counter!("strata_rest_requests_total", "endpoint" => "backup").increment(1);

    let backup_dir = std::path::PathBuf::from(&engine.config().storage.data_dir).join("backups");
    let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let target = backup_dir.join(&timestamp);

    match engine.backup(&target).await {
        Ok(()) => api_ok(serde_json::json!({
            "status": "ok",
            "path": target.to_string_lossy(),
            "timestamp": timestamp,
        })),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "BACKUP_ERROR", e.to_string()),
    }
}
