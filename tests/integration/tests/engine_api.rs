//! Integration tests: REST API backed by a real engine.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use strata_core::{CoreConfig, StrataEngine};
use tower::ServiceExt;

async fn engine_router() -> axum::Router {
    let mut config = CoreConfig::default();
    config.memory.state.db_path = ":memory:".into();
    let engine = Arc::new(StrataEngine::new(config).await.unwrap());
    strata_gateway::rest::router_with_engine(engine)
}

#[tokio::test]
async fn ingest_then_query() {
    let app = engine_router().await;

    // Ingest events
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"source":"test","events":[{"event_type":"click"},{"event_type":"view"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ingested"], 2);

    // Query events back
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"sql":"SELECT count(*)::VARCHAR as cnt FROM episodic WHERE source='test'"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["rows"][0]["cnt"], "2");
}

#[tokio::test]
async fn state_set_then_get() {
    let app = engine_router().await;

    // Set state
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/state/bot-1/mood")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#""happy""#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["version"], 1);

    // Get state back
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/state/bot-1/mood")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["value"], "happy");
    assert_eq!(json["version"], 1);
}

#[tokio::test]
async fn mcp_initialize() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["jsonrpc"], "2.0");
    assert!(json["result"]["serverInfo"]["name"].as_str().is_some());
}

#[tokio::test]
async fn mcp_tools_list() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let tools = json["result"]["tools"].as_array().unwrap();
    assert!(tools.len() >= 5);
}

#[tokio::test]
async fn mcp_tools_call_query() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query","arguments":{"sql":"SELECT 42::VARCHAR as answer"}}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("42"));
}

#[tokio::test]
async fn webhook_github_push() {
    let app = engine_router().await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/webhook/github")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"action":"completed","commits":[{"id":"abc"}],"repository":{"full_name":"org/repo"},"sender":{"login":"dev"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["source"], "github");
    assert_eq!(json["ingested"], 1);
}
