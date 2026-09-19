//! Governed writes, external identity, and context packs — the API an orchestrator needs.
//!
//! These four endpoints exist for clients that hold memory on someone else's behalf: an agent may
//! propose but not decide, an importer must converge on one memory per source record, and a
//! runtime wants one bounded answer instead of three retrieval calls.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use tower::ServiceExt;

/// A router over a **private** store. The default config points every store at `./data`, so
/// tests sharing it would read each other's memories and pass or fail depending on order.
async fn app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_string_lossy().to_string();
    let mut config = ecphoria_core::CoreConfig::default();
    config.storage.data_dir = base.clone();
    config.memory.cognition.db_path = format!("{base}/memories.duckdb");
    config.memory.episodic.db_path = format!("{base}/episodic.duckdb");
    config.memory.state.db_path = format!("{base}/state.db");
    let engine = Arc::new(ecphoria_core::EcphoriaEngine::new(config).await.unwrap());
    (ecphoria_gateway::rest::router_with_engine(engine), dir)
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: &str,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(if body.is_empty() {
            Body::empty()
        } else {
            Body::from(body.to_string())
        })
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

// ── Proposals ────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_proposal_is_not_a_belief() {
    let (app, _data) = app().await;
    let (status, proposed) = call(
        &app,
        "POST",
        "/api/v1/memories?status=pending",
        r#"{"content": "the billing service owns invoice numbering", "subject": "billing.owner"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(proposed["status"], "pending");

    // Invisible to retrieval until accepted — that is the whole point.
    let (_, search) = call(
        &app,
        "POST",
        "/api/v1/memories/search",
        r#"{"query": "invoice numbering", "k": 10}"#,
    )
    .await;
    assert_eq!(search["results"].as_array().unwrap().len(), 0);

    // …but visible in the review queue.
    let (_, pending) = call(&app, "GET", "/api/v1/pending", "").await;
    assert_eq!(pending["count"], 1);
    assert_eq!(
        pending["items"][0]["content"],
        "the billing service owns invoice numbering"
    );
}

#[tokio::test]
async fn accepting_a_proposal_makes_it_retrievable() {
    let (app, _data) = app().await;
    let (_, proposed) = call(
        &app,
        "POST",
        "/api/v1/memories?status=pending",
        r#"{"content": "deploys go out on tuesdays", "subject": "deploy.window"}"#,
    )
    .await;
    let id = proposed["id"].as_str().unwrap().to_string();

    let (status, accepted) = call(&app, "POST", &format!("/api/v1/pending/{id}/accept"), "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(accepted["accepted"], true);

    let (_, search) = call(
        &app,
        "POST",
        "/api/v1/memories/search",
        r#"{"query": "deploys tuesdays", "k": 10}"#,
    )
    .await;
    assert_eq!(search["results"].as_array().unwrap().len(), 1);

    // The queue is empty, and a second accept finds nothing to accept.
    let (_, pending) = call(&app, "GET", "/api/v1/pending", "").await;
    assert_eq!(pending["count"], 0);
    let (again, _) = call(&app, "POST", &format!("/api/v1/pending/{id}/accept"), "").await;
    assert_eq!(again, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rejecting_a_proposal_keeps_the_judgement() {
    let (app, _data) = app().await;
    let (_, proposed) = call(
        &app,
        "POST",
        "/api/v1/memories?status=pending",
        r#"{"content": "the cache can be cleared in production at any time"}"#,
    )
    .await;
    let id = proposed["id"].as_str().unwrap().to_string();

    let (status, rejected) = call(&app, "POST", &format!("/api/v1/pending/{id}/reject"), "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rejected["rejected"], true);

    let (_, pending) = call(&app, "GET", "/api/v1/pending", "").await;
    assert_eq!(pending["count"], 0);

    // The rejected row is kept — a decision is evidence, deleting it loses that.
    let (status, memory) = call(&app, "GET", &format!("/api/v1/memories/{id}"), "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(memory["state"], "expired");
    assert_eq!(memory["metadata"]["review_decision"], "rejected");
}

#[tokio::test]
async fn deciding_on_something_that_is_not_pending_says_so() {
    let (app, _data) = app().await;
    let (status, _) = call(&app, "POST", "/api/v1/pending/not-a-uuid/accept", "").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = call(
        &app,
        "POST",
        "/api/v1/pending/00000000-0000-0000-0000-000000000000/reject",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── External identity ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn re_sending_the_same_external_id_does_not_duplicate() {
    let (app, _data) = app().await;
    let body = r#"{"external_id": "PROJ-42", "source": "jira", "content": "invoices must be immutable once sent"}"#;

    let (status, first) = call(&app, "PUT", "/api/v1/memories/by-external-id", body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["outcome"], "inserted");

    let (_, again) = call(&app, "PUT", "/api/v1/memories/by-external-id", body).await;
    assert_eq!(
        again["outcome"], "confirmed",
        "a redelivery confirms, it does not duplicate"
    );
    assert_eq!(again["id"], first["id"]);

    let (_, list) = call(&app, "GET", "/api/v1/memories?limit=50", "").await;
    let count = list["memories"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(count, 1, "one source record, one memory");
}

#[tokio::test]
async fn a_changed_external_record_supersedes_its_previous_version() {
    let (app, _data) = app().await;
    let (_, _) = call(
        &app,
        "PUT",
        "/api/v1/memories/by-external-id",
        r#"{"external_id": "PROJ-7", "source": "jira", "content": "the retry limit is 3"}"#,
    )
    .await;
    let (_, updated) = call(
        &app,
        "PUT",
        "/api/v1/memories/by-external-id",
        r#"{"external_id": "PROJ-7", "source": "jira", "content": "the retry limit is 5"}"#,
    )
    .await;
    assert_eq!(updated["outcome"], "superseded");
    assert_eq!(updated["memory"]["metadata"]["external_id"], "PROJ-7");
    assert_eq!(updated["memory"]["metadata"]["source"], "jira");
}

#[tokio::test]
async fn an_external_upsert_needs_an_identifier_and_content() {
    let (app, _data) = app().await;
    let (status, _) = call(
        &app,
        "PUT",
        "/api/v1/memories/by-external-id",
        r#"{"external_id": "", "content": "x"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = call(
        &app,
        "PUT",
        "/api/v1/memories/by-external-id",
        r#"{"external_id": "A-1", "content": "   "}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ── Context packs ────────────────────────────────────────────────────────────────────

async fn seed(app: &axum::Router, content: &str, metadata: &str) {
    let body = format!(r#"{{"content": {content:?}, "metadata": {metadata}}}"#);
    let (status, _) = call(app, "POST", "/api/v1/memories", &body).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_context_pack_separates_incidents_from_facts() {
    let (app, _data) = app().await;
    seed(
        &app,
        "payments use idempotency keys on every write",
        r#"{"kind": "convention"}"#,
    )
    .await;
    seed(
        &app,
        "payments outage on 2026-03-02: duplicate charges",
        r#"{"kind": "incident"}"#,
    )
    .await;

    let (status, pack) = call(
        &app,
        "POST",
        "/api/v1/context-pack",
        r#"{"query": "payments", "budget_tokens": 4000}"#,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(pack["memories"].as_array().unwrap().len(), 1);
    assert_eq!(pack["incidents"].as_array().unwrap().len(), 1);
    assert_eq!(pack["truncated"], false);
    assert!(pack["tokens_estimated"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn a_context_pack_never_exceeds_its_budget() {
    let (app, _data) = app().await;
    for index in 0..10 {
        seed(
            &app,
            &format!("retry policy note {index}: {}", "x".repeat(400)),
            r#"{"kind": "convention"}"#,
        )
        .await;
    }

    let (_, pack) = call(
        &app,
        "POST",
        "/api/v1/context-pack",
        r#"{"query": "retry policy", "budget_tokens": 200, "k": 10}"#,
    )
    .await;

    let spent = pack["tokens_estimated"].as_u64().unwrap();
    assert!(
        spent <= 200,
        "pack spent {spent} tokens for a 200-token budget"
    );
    assert_eq!(pack["truncated"], true, "dropping content must be reported");
}

#[tokio::test]
async fn a_context_pack_respects_allowed_paths_and_kinds() {
    let (app, _data) = app().await;
    seed(
        &app,
        "the orders module validates currency codes",
        r#"{"kind": "convention", "paths": ["src/orders/**"]}"#,
    )
    .await;
    seed(
        &app,
        "the billing module rounds half up",
        r#"{"kind": "convention", "paths": ["src/billing/round.py"]}"#,
    )
    .await;

    let (_, scoped) = call(
        &app,
        "POST",
        "/api/v1/context-pack",
        r#"{"query": "module", "paths": ["src/orders/**"], "budget_tokens": 4000}"#,
    )
    .await;
    let kept = scoped["memories"].as_array().unwrap();
    assert_eq!(
        kept.len(),
        1,
        "a memory about other files is not this task's context"
    );
    assert!(kept[0]["content"].as_str().unwrap().contains("orders"));

    // A memory filed against a directory glob is context for a file inside it, and vice versa.
    let (_, glob_side) = call(
        &app,
        "POST",
        "/api/v1/context-pack",
        r#"{"query": "module", "paths": ["src/orders/total.py"], "budget_tokens": 4000}"#,
    )
    .await;
    assert_eq!(glob_side["memories"].as_array().unwrap().len(), 1);

    let (_, by_kind) = call(
        &app,
        "POST",
        "/api/v1/context-pack",
        r#"{"query": "module", "kinds": ["incident"], "budget_tokens": 4000}"#,
    )
    .await;
    assert_eq!(by_kind["memories"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn a_context_pack_needs_a_query() {
    let (app, _data) = app().await;
    let (status, _) = call(&app, "POST", "/api/v1/context-pack", r#"{"query": "  "}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ── Tenants ──────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn creating_a_tenant_is_idempotent() {
    let (app, _data) = app().await;
    let (status, created) = call(
        &app,
        "POST",
        "/api/v1/admin/tenants",
        r#"{"name": "billing-api", "require_provenance": true}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["tenant"], "billing-api");
    assert_eq!(created["created"], true);

    // Write something into it, then confirm it again: the tenant now exists.
    let (_, _) = call(
        &app,
        "POST",
        "/api/v1/memories",
        r#"{"content": "billing rounds half up", "tenant_id": "billing-api"}"#,
    )
    .await;
    let (_, confirmed) = call(
        &app,
        "POST",
        "/api/v1/admin/tenants",
        r#"{"name": "billing-api"}"#,
    )
    .await;
    assert_eq!(confirmed["created"], false);
}

#[tokio::test]
async fn a_tenant_needs_a_name() {
    let (app, _data) = app().await;
    let (status, _) = call(&app, "POST", "/api/v1/admin/tenants", r#"{"name": "  "}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ── Tenant header ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_tenant_header_isolates_two_projects_on_one_key() {
    let (app, _data) = app().await;
    for (tenant, content) in [
        ("project-a", "A uses postgres"),
        ("project-b", "B uses mysql"),
    ] {
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/memories")
            .header(header::CONTENT_TYPE, "application/json")
            .header("X-Ecphoria-Tenant", tenant)
            .body(Body::from(format!(r#"{{"content": "{content}"}}"#)))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/memories/search")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Ecphoria-Tenant", "project-a")
        .body(Body::from(r#"{"query": "database", "k": 10}"#))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let results = json["results"].as_array().unwrap();
    assert_eq!(
        results.len(),
        1,
        "one tenant must not see another's memories"
    );
    assert!(results[0]["memory"]["content"]
        .as_str()
        .unwrap()
        .contains("postgres"));
}
