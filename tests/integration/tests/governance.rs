//! Per-tenant governance over the HTTP surface: attribution (E-10) and typed facts (E-04).
//!
//! The engine decides what is acceptable; these tests pin the part a client actually sees — a
//! **422** naming every problem at once, on every write route, and nothing at all for a tenant
//! that has not asked for any of it.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use ecphoria_core::memory::facts::FactValidation;
use tower::ServiceExt;

/// A router over a private store, with `acme` governed and every other tenant left alone.
async fn app(
    require_provenance: bool,
    validation: FactValidation,
) -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_string_lossy().to_string();
    let mut config = ecphoria_core::CoreConfig::default();
    config.storage.data_dir = base.clone();
    config.memory.cognition.db_path = format!("{base}/memories.duckdb");
    config.memory.episodic.db_path = format!("{base}/episodic.duckdb");
    config.memory.state.db_path = format!("{base}/state.db");
    config.memory.governance.tenants.insert(
        "acme".into(),
        ecphoria_core::config::TenantGovernance {
            require_provenance: Some(require_provenance),
            fact_validation: Some(validation),
        },
    );
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

fn message(body: &serde_json::Value) -> String {
    body["error"]["message"].as_str().unwrap_or_default().into()
}

#[tokio::test]
async fn a_write_with_no_provenance_is_a_422_not_a_500() {
    let (app, _data) = app(true, FactValidation::Off).await;
    let (status, body) = call(
        &app,
        "POST",
        "/api/v1/memories",
        r#"{"tenant_id": "acme", "subject": "deploy.target", "content": "we deploy on Fridays"}"#,
    )
    .await;
    // The distinction matters: a 500 tells a client to retry, a 422 tells it to fix the request.
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], "VALIDATION_FAILED");
    assert!(message(&body).contains("provenance"), "{body}");
}

#[tokio::test]
async fn the_same_write_with_a_source_is_accepted() {
    let (app, _data) = app(true, FactValidation::Off).await;
    let (status, _) = call(
        &app,
        "POST",
        "/api/v1/memories",
        r#"{"tenant_id": "acme", "subject": "deploy.target", "content": "we deploy on Fridays",
            "metadata": {"provenance": {"source": "runbook", "ref": "docs/deploy.md"}}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn an_ungoverned_tenant_is_not_affected() {
    let (app, _data) = app(true, FactValidation::Strict).await;
    let (status, _) = call(
        &app,
        "POST",
        "/api/v1/memories",
        r#"{"tenant_id": "someone-else", "subject": "anything", "content": "no provenance here"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_malformed_fact_is_refused_with_every_problem_at_once() {
    let (app, _data) = app(false, FactValidation::Strict).await;
    let (status, body) = call(
        &app,
        "POST",
        "/api/v1/memories",
        r#"{"tenant_id": "acme", "subject": "the checkout outage",
            "content": "checkout was down for 40 minutes",
            "metadata": {"kind": "incident"}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let m = message(&body);
    // One round-trip, the whole list: a writer fixing one field at a time learns the schema by
    // trial and error.
    assert!(m.contains("service"), "{m}");
    assert!(m.contains("occurred_at"), "{m}");
    assert!(m.contains("incident:<service>:<yyyy-mm-dd>"), "{m}");
}

#[tokio::test]
async fn a_well_formed_fact_goes_through_and_supersedes_by_subject() {
    let (app, _data) = app(false, FactValidation::Strict).await;
    let write = |content: &str| {
        format!(
            r#"{{"tenant_id": "acme", "subject": "incident:checkout-api:2026-09-14",
                 "content": "{content}",
                 "metadata": {{"kind": "incident", "service": "checkout-api",
                               "occurred_at": "2026-09-14T03:12:00Z", "severity": "sev2"}}}}"#
        )
    };
    let (status, _) = call(
        &app,
        "POST",
        "/api/v1/memories",
        &write("40 minutes of 503s"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // The grammar is not decoration: two writers using it land on one subject, and the second
    // write supersedes the first instead of sitting beside it.
    let (status, second) = call(
        &app,
        "POST",
        "/api/v1/memories",
        &write("40 minutes of 503s, caused by a bad deploy"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["outcome"], "superseded", "{second}");
}

#[tokio::test]
async fn warn_mode_lets_the_write_through() {
    let (app, _data) = app(false, FactValidation::Warn).await;
    let (status, _) = call(
        &app,
        "POST",
        "/api/v1/memories",
        r#"{"tenant_id": "acme", "subject": "free text", "content": "x",
            "metadata": {"kind": "incident"}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn proposing_is_not_a_way_around_the_rules() {
    let (app, _data) = app(true, FactValidation::Strict).await;
    let (status, body) = call(
        &app,
        "POST",
        "/api/v1/memories?status=pending",
        r#"{"tenant_id": "acme", "subject": "the checkout outage", "content": "x",
            "metadata": {"kind": "incident"}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

#[tokio::test]
async fn the_batch_route_refuses_the_same_things() {
    let (app, _data) = app(true, FactValidation::Off).await;
    let (status, body) = call(
        &app,
        "POST",
        "/api/v1/memories/batch",
        // The batch route scopes each memory on its own — there is no top-level tenant.
        r#"{"memories": [
             {"tenant_id": "acme", "subject": "a", "content": "one",
              "metadata": {"provenance": {"source": "ci"}}},
             {"tenant_id": "acme", "subject": "b", "content": "two"}
           ]}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(message(&body).contains("provenance"), "{body}");
}

#[tokio::test]
async fn the_external_id_route_refuses_the_same_things() {
    let (app, _data) = app(true, FactValidation::Off).await;
    let (status, body) = call(
        &app,
        "PUT",
        "/api/v1/memories/by-external-id",
        r#"{"tenant_id": "acme", "source": "jira", "external_id": "PROJ-1",
            "content": "the ticket was about invoice rounding"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

#[tokio::test]
async fn an_unknown_kind_names_the_vocabulary_it_should_have_used() {
    let (app, _data) = app(false, FactValidation::Strict).await;
    let (status, body) = call(
        &app,
        "POST",
        "/api/v1/memories",
        r#"{"tenant_id": "acme", "subject": "x:y:z", "content": "c",
            "metadata": {"kind": "brainwave"}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let m = message(&body);
    assert!(m.contains("brainwave"), "{m}");
    assert!(m.contains("ticket_summary"), "{m}");
}

#[tokio::test]
async fn the_kind_column_answers_sql_questions() {
    let (app, _data) = app(false, FactValidation::Strict).await;
    for (subject, metadata) in [
        (
            "incident:payments:2026-01-02",
            r#"{"kind": "incident", "service": "payments", "occurred_at": "2026-01-02T00:00:00Z"}"#,
        ),
        (
            "incident:payments:2026-02-03",
            r#"{"kind": "incident", "service": "payments", "occurred_at": "2026-02-03T00:00:00Z"}"#,
        ),
        ("decision:api:versioning", r#"{"kind": "decision"}"#),
    ] {
        let (status, body) = call(
            &app,
            "POST",
            "/api/v1/memories",
            &format!(
                r#"{{"tenant_id": "acme", "subject": "{subject}", "content": "…",
                     "metadata": {metadata}}}"#
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{subject}: {body}");
    }

    // "How many incidents does payments have?" — one indexed predicate, no JSON extraction.
    let (status, body) = call(
        &app,
        "POST",
        "/api/v1/query",
        r#"{"sql": "SELECT COUNT(*)::VARCHAR AS n FROM memories WHERE kind = 'incident'"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"], "2", "{body}");
}
