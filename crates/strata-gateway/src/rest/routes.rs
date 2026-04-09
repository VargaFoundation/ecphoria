//! REST API route definitions.

use std::sync::Arc;
use std::time::Duration;

use super::handlers;
use axum::Router;
use strata_core::StrataEngine;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

/// Build a minimal REST API router (no engine state — for basic testing).
pub fn router() -> Router {
    Router::new()
        .route("/health", axum::routing::get(handlers::health))
        .route(
            "/api/v1/query",
            axum::routing::post(handlers::query_no_engine),
        )
        .route(
            "/api/v1/ingest",
            axum::routing::post(handlers::ingest_no_engine),
        )
        .route(
            "/api/v1/search",
            axum::routing::post(handlers::search_no_engine),
        )
}

/// Build the full REST API router with engine state and production middleware.
pub fn router_with_engine(engine: Arc<StrataEngine>) -> Router {
    Router::new()
        // Health (metrics endpoint added by server.rs with PrometheusHandle)
        .route("/health", axum::routing::get(handlers::health))
        // Core API
        .route("/api/v1/query", axum::routing::post(handlers::query))
        .route("/api/v1/ingest", axum::routing::post(handlers::ingest))
        .route(
            "/api/v1/webhook/{source}",
            axum::routing::post(handlers::webhook),
        )
        .route("/api/v1/search", axum::routing::post(handlers::search))
        .route(
            "/api/v1/state/{agent_id}/{key}",
            axum::routing::get(handlers::state_get).put(handlers::state_set),
        )
        // MCP & LLM proxy
        .route(
            "/mcp",
            axum::routing::post(crate::mcp::transport::handle_mcp),
        )
        .route(
            "/v1/chat/completions",
            axum::routing::post(crate::llm_proxy::router::chat_completions),
        )
        .with_state(engine)
        // Middleware stack (applied bottom-up)
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            Duration::from_secs(30),
        ))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}
