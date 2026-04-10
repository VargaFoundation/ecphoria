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
///
/// If `api_key_store` is provided, all `/api/v1/*` routes require authentication.
pub fn router_with_engine(engine: Arc<StrataEngine>) -> Router {
    router_with_engine_and_auth(engine, None, None)
}

/// Build the full REST API router with optional auth and cluster middleware.
pub fn router_with_engine_and_auth(
    engine: Arc<StrataEngine>,
    api_key_store: Option<crate::auth::middleware::ApiKeyStore>,
    cluster_state: Option<crate::cluster::leader_forward::ClusterState>,
) -> Router {
    // Public routes (no auth required)
    let mut app = Router::new()
        .route("/health", axum::routing::get(handlers::health));

    // Protected API routes
    let mut api_routes = Router::new()
        .route("/query", axum::routing::post(handlers::query))
        .route("/ingest", axum::routing::post(handlers::ingest))
        .route(
            "/webhook/{source}",
            axum::routing::post(handlers::webhook),
        )
        .route("/search", axum::routing::post(handlers::search))
        .route(
            "/state/{agent_id}/{key}",
            axum::routing::get(handlers::state_get).put(handlers::state_set),
        )
        .route("/admin/retention", axum::routing::post(handlers::enforce_retention))
        .route("/admin/backup", axum::routing::post(handlers::backup))
        .with_state(engine.clone());

    // Apply auth middleware if configured
    if let Some(store) = api_key_store {
        api_routes = api_routes.route_layer(axum::middleware::from_fn_with_state(
            store,
            crate::auth::middleware::require_auth,
        ));
    }

    // Apply leader-forwarding middleware if cluster mode is active
    if let Some(cluster_state) = cluster_state {
        api_routes = api_routes.route_layer(axum::middleware::from_fn_with_state(
            cluster_state,
            crate::cluster::leader_forward::require_leader_for_writes,
        ));
    }

    app = app.nest("/api/v1", api_routes);

    // MCP & LLM proxy (use engine state, resolved separately)
    let protocol_routes = Router::new()
        .route(
            "/mcp",
            axum::routing::post(crate::mcp::transport::handle_mcp),
        )
        .route(
            "/v1/chat/completions",
            axum::routing::post(crate::llm_proxy::router::chat_completions),
        )
        .with_state(engine);

    app = app.merge(protocol_routes);

    // Global middleware stack (applied bottom-up)
    app.layer(tower_http::timeout::TimeoutLayer::with_status_code(
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            Duration::from_secs(30),
        ))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}
