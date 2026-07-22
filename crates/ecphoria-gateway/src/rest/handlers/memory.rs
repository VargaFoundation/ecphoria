//! Memory & cognition REST handlers — CRUD, search, grants, contradictions, consolidation,
//! semantic upsert/search, and knowledge-graph edges (split from handlers.rs).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::{Extension, Json};
use ecphoria_core::EcphoriaEngine;

use crate::rest::models::*;

use super::{api_error, api_ok, cluster_write_error, parse_as_of, scope_from};

/// Add a memory through the cognition pipeline (dedup / contradiction / importance).
///
/// POST /api/v1/memories { "content": "...", "subject": "...", "user_id": "..." }
pub async fn memory_add(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryAddRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_add").increment(1);

    if req.content.trim().is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "MISSING_FIELD",
            "content is required".into(),
        );
    }

    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let input = ecphoria_core::memory::cognition::MemoryInput {
        scope,
        subject: req.subject,
        content: req.content,
        importance: req.importance,
        source_event_ids: vec![],
        metadata: req.metadata.unwrap_or_else(|| serde_json::json!({})),
        mem_type: req.mem_type,
    };

    // Cluster mode: run cognition on the leader to materialize the change-set, then replicate it
    // through the Raft log (never apply directly off-leader). Followers replay identical rows.
    if let Some(Extension(coord)) = cluster {
        let (result, rows) = match engine.memory_plan(input).await {
            Ok(pair) => pair,
            Err(e) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "MEMORY_ERROR",
                    e.to_string(),
                )
            }
        };
        let coord = coord.read().await;
        let ar = ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows };
        return match coord.client_write(ar).await {
            Ok(_) => api_ok(serde_json::to_value(result).unwrap_or_default()),
            Err(e) => cluster_write_error(e),
        };
    }

    match engine.memory_add(input).await {
        Ok(added) => api_ok(serde_json::to_value(added).unwrap_or_default()),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Search memories within a scope (semantic when embeddings exist, else recency).
///
/// POST /api/v1/memories/search { "query": "...", "user_id": "...", "k": 5 }
pub async fn memory_search(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<MemorySearchRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_search").increment(1);

    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let result = if req.shared {
        engine.memory_search_shared(&req.query, &scope, req.k).await
    } else {
        engine.memory_search(&req.query, &scope, req.k).await
    };
    match result {
        Ok(hits) => api_ok(serde_json::json!({ "results": hits, "count": hits.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// POST /api/v1/memories/grants — grant a user read access to another user's memories (tenant from
/// the token). GET (?grantee=U) lists a user's grants; DELETE /grants/{id} revokes one.
pub async fn grant_create(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Json(req): Json<MemoryGrantRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "grant_create").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine
        .grant_share(&tenant, &req.grantee_user_id, &req.grantor_user_id)
        .await
    {
        Ok(id) => api_ok(serde_json::json!({ "id": id.to_string(), "status": "granted" })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GRANT_ERROR",
            e.to_string(),
        ),
    }
}

pub async fn grant_list(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<GrantListParams>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "grant_list").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine.list_grants(&tenant, &params.grantee).await {
        Ok(grants) => api_ok(serde_json::json!({ "grants": grants, "count": grants.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GRANT_ERROR",
            e.to_string(),
        ),
    }
}

pub async fn grant_revoke(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "grant_revoke").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                "grant id must be a UUID".into(),
            )
        }
    };
    match engine.revoke_grant(&tenant, uuid).await {
        Ok(removed) => api_ok(serde_json::json!({ "id": id, "revoked": removed })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GRANT_ERROR",
            e.to_string(),
        ),
    }
}

/// List active memories in a scope.
///
/// GET /api/v1/memories?user_id=alice&limit=50
pub async fn memory_list(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<MemoryListParams>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_list").increment(1);

    let scope = scope_from(
        &auth,
        params.tenant_id.as_deref(),
        params.user_id.as_deref(),
        params.agent_id.as_deref(),
        params.session_id.as_deref(),
    );
    let filter = ecphoria_core::memory::cognition::MemoryFilter {
        mem_type: params.mem_type.clone(),
        min_importance: params.min_importance,
        updated_after: parse_as_of(params.updated_after.as_deref()),
        updated_before: parse_as_of(params.updated_before.as_deref()),
        metadata: match (params.metadata_key.clone(), params.metadata_value.clone()) {
            (Some(k), Some(v)) => Some((k, v)),
            _ => None,
        },
    };
    match engine
        .memory_list(&scope, params.limit, params.offset, &filter)
        .await
    {
        Ok(mems) => api_ok(serde_json::json!({
            "memories": mems,
            "count": mems.len(),
            "limit": params.limit,
            "offset": params.offset,
        })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Get a single memory by id (scoped to the caller's tenant).
pub async fn memory_get(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_get").increment(1);
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                format!("'{id}' is not a valid memory id"),
            )
        }
    };
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let got = match tenant {
        Some(t) => engine.memory_get_scoped(uuid, &t).await,
        None => engine.memory_get(uuid).await,
    };
    match got {
        Ok(Some(m)) => api_ok(serde_json::to_value(m).unwrap_or_default()),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            format!("memory '{id}' not found"),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Delete a memory by id (scoped to the caller's tenant).
pub async fn memory_delete(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_delete").increment(1);
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                format!("'{id}' is not a valid memory id"),
            )
        }
    };
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let outcome = match tenant {
        Some(t) => engine.memory_delete_scoped(uuid, &t).await,
        None => engine.memory_delete(uuid).await.map(|()| true),
    };
    match outcome {
        Ok(true) => api_ok(serde_json::json!({ "id": id, "deleted": true })),
        Ok(false) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            format!("memory '{id}' not found"),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Partially correct a memory by id — only the provided fields change (content / importance /
/// mem_type / metadata). Scoped to the caller's tenant. In cluster mode the change-set is
/// materialized on the leader and replicated via the Raft log (`MemoryUpsert`), like memory add.
///
/// PATCH /api/v1/memories/{id}
pub async fn memory_update(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Path(id): Path<String>,
    Json(req): Json<MemoryPatchRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_update").increment(1);
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                format!("'{id}' is not a valid memory id"),
            )
        }
    };
    let patch = ecphoria_core::memory::cognition::MemoryPatch {
        content: req.content,
        importance: req.importance,
        mem_type: req.mem_type,
        metadata: req.metadata,
    };
    if patch.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "EMPTY_PATCH",
            "provide at least one of content / importance / mem_type / metadata".into(),
        );
    }
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());

    // Cluster mode: materialize the update on the leader, then replicate via the Raft log so every
    // node applies the identical row (never mutate off-leader). Followers replay MemoryUpsert.
    if let Some(Extension(coord)) = cluster {
        let plan = match engine
            .memory_update_plan(uuid, patch, tenant.as_deref())
            .await
        {
            Ok(p) => p,
            Err(e) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "MEMORY_ERROR",
                    e.to_string(),
                )
            }
        };
        let Some((updated, rows)) = plan else {
            return api_error(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                format!("memory '{id}' not found"),
            );
        };
        let coord = coord.read().await;
        let ar = ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows };
        return match coord.client_write(ar).await {
            Ok(_) => api_ok(serde_json::to_value(updated).unwrap_or_default()),
            Err(e) => cluster_write_error(e),
        };
    }

    match engine.memory_update(uuid, patch, tenant.as_deref()).await {
        Ok(Some(m)) => api_ok(serde_json::to_value(m).unwrap_or_default()),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            format!("memory '{id}' not found"),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Get the full temporal history of a memory (every superseded version).
///
/// GET /api/v1/memories/{id}/history
pub async fn memory_history(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_history").increment(1);
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                format!("'{id}' is not a valid memory id"),
            )
        }
    };
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    let fetched = match tenant {
        Some(t) => engine.memory_get_scoped(uuid, &t).await,
        None => engine.memory_get(uuid).await,
    };
    let mem = match fetched {
        Ok(Some(m)) => m,
        Ok(None) => {
            return api_error(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                format!("memory '{id}' not found"),
            )
        }
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            )
        }
    };

    match mem.subject.clone() {
        Some(subject) => match engine.memory_history(&mem.scope, &subject).await {
            Ok(history) => api_ok(serde_json::json!({
                "subject": subject,
                "history": history,
                "count": history.len(),
            })),
            Err(e) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            ),
        },
        // No subject → no supersession chain; the memory is its own history.
        None => api_ok(serde_json::json!({ "history": [mem], "count": 1 })),
    }
}

/// GET /api/v1/memories/{id}/provenance — "why do you believe this?".
///
/// Returns the memory, the episodic events it was distilled from, and its bi-temporal
/// supersession chain — the audit trail behind a distilled fact. Tenant-scoped (404 on mismatch).
pub async fn memory_provenance(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_provenance")
        .increment(1);
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                format!("'{id}' is not a valid memory id"),
            )
        }
    };
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());
    match engine.memory_provenance(uuid, tenant.as_deref()).await {
        Ok(Some(prov)) => api_ok(serde_json::json!({
            "memory": prov.memory,
            "source_events": prov.source_events,
            "source_event_count": prov.source_events.len(),
            "history": prov.history,
            "history_count": prov.history.len(),
        })),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            format!("memory '{id}' not found"),
        ),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// POST /api/v1/memories/{id}/feedback — close the RAG loop.
///
/// Body `{"verdict": "helpful" | "wrong" | "obsolete"}`. `helpful` reinforces the memory
/// (importance up); `wrong`/`obsolete` retire it (bi-temporal expire + drop its vector). Lets
/// ranking learn from usage without an LLM. Tenant-scoped; replicated in cluster mode.
pub async fn memory_feedback(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Path(id): Path<String>,
    Json(req): Json<MemoryFeedbackRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_feedback").increment(1);
    let uuid = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_ID",
                format!("'{id}' is not a valid memory id"),
            )
        }
    };
    let verdict = match ecphoria_core::MemoryFeedback::from_str_loose(&req.verdict) {
        Some(v) => v,
        None => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_VERDICT",
                "verdict must be one of: helpful, wrong, obsolete".into(),
            )
        }
    };
    let tenant = auth.as_ref().and_then(|Extension(c)| c.tenant_id.clone());

    let (memory, action) = match engine
        .memory_feedback_plan(uuid, tenant.as_deref(), verdict)
        .await
    {
        Ok(Some(pair)) => pair,
        Ok(None) => {
            return api_error(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                format!("memory '{id}' not found"),
            )
        }
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            )
        }
    };

    // Cluster mode: replicate the materialized change through the Raft log so followers converge.
    if let Some(Extension(coord)) = cluster {
        let ar = match &action {
            ecphoria_core::FeedbackAction::Reinforce(rows) => {
                ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows: rows.clone() }
            }
            ecphoria_core::FeedbackAction::Retire(ids) => {
                ecphoria_cluster::raft::types::AppRequest::MemoryExpire { ids: ids.clone() }
            }
        };
        return match coord.read().await.client_write(ar).await {
            Ok(_) => api_ok(serde_json::json!({ "verdict": req.verdict, "memory": memory })),
            Err(e) => cluster_write_error(e),
        };
    }

    match engine.memory_feedback_apply(action).await {
        Ok(()) => api_ok(serde_json::json!({ "verdict": req.verdict, "memory": memory })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// GET /api/v1/memories/contradictions — the HITL review queue.
///
/// Lists subjects with more than one active memory (only possible under
/// `cognition.contradiction_review`), each group awaiting resolution. Tenant/scope from the token.
pub async fn memory_contradictions(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(q): axum::extract::Query<ContradictionsQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_contradictions")
        .increment(1);
    let scope = scope_from(
        &auth,
        None,
        q.user_id.as_deref(),
        q.agent_id.as_deref(),
        q.session_id.as_deref(),
    );
    match engine.memory_contradictions(&scope).await {
        Ok(groups) => {
            api_ok(serde_json::json!({ "contradictions": groups, "count": groups.len() }))
        }
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// POST /api/v1/memories/contradictions/resolve — resolve a contradiction by keeping one memory and
/// superseding the others for that subject. Replicated in cluster mode.
pub async fn memory_resolve_contradiction(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<ResolveContradictionRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_resolve_contradiction")
        .increment(1);
    let scope = scope_from(
        &auth,
        None,
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let rows = match engine
        .memory_resolve_plan(&scope, &req.subject, req.keep_id)
        .await
    {
        Ok(rows) => rows,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, "RESOLVE_ERROR", e.to_string()),
    };
    let superseded = rows.len();

    if let Some(Extension(coord)) = cluster {
        return match coord
            .read()
            .await
            .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows })
            .await
        {
            Ok(_) => api_ok(serde_json::json!({ "kept": req.keep_id, "superseded": superseded })),
            Err(e) => cluster_write_error(e),
        };
    }
    match engine.memory_apply_rows(rows).await {
        Ok(_) => api_ok(serde_json::json!({ "kept": req.keep_id, "superseded": superseded })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Re-embed active memories with the currently-configured provider (admin).
///
/// Run after switching embedding model/dimension so existing memories are searchable under the new
/// vectors. Recomputes up to `limit` memories' vectors (oldest-updated first, so repeated calls page
/// forward through the corpus) and re-indexes them. In cluster mode the leader computes the fresh
/// vectors and replicates the rows via Raft so every node re-indexes identically.
///
/// POST /api/v1/admin/memory/reembed  { "limit": 1000 }
pub async fn memory_reembed(
    State(engine): State<Arc<EcphoriaEngine>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryReembedRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_reembed").increment(1);
    let limit = req.limit.unwrap_or(1000);

    let rows = match engine.memory_reembed_plan(limit).await {
        Ok(rows) => rows,
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "REEMBED_ERROR",
                e.to_string(),
            )
        }
    };
    let reembedded = rows.len();
    if reembedded == 0 {
        return api_ok(serde_json::json!({ "reembedded": 0 }));
    }

    if let Some(Extension(coord)) = cluster {
        return match coord
            .read()
            .await
            .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows })
            .await
        {
            Ok(_) => api_ok(serde_json::json!({ "reembedded": reembedded })),
            Err(e) => cluster_write_error(e),
        };
    }
    match engine.memory_apply_rows(rows).await {
        Ok(_) => api_ok(serde_json::json!({ "reembedded": reembedded })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Forget low-value memories via time-decay of importance (admin).
///
/// POST /api/v1/admin/memory/decay
pub async fn memory_consolidate(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryConsolidateRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_consolidate")
        .increment(1);
    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let keep = req.keep.unwrap_or(20);

    // Cluster mode: plan on the leader (summary + originals to expire), then replicate both through
    // the Raft log (summary as MemoryUpsert, originals as MemoryExpire) so followers converge.
    if let Some(Extension(coord)) = cluster {
        let plan = match engine.memory_consolidate_plan(&scope, keep).await {
            Ok(Some(p)) => p,
            Ok(None) => return api_ok(serde_json::json!({ "consolidated": null })),
            Err(e) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "MEMORY_ERROR",
                    e.to_string(),
                )
            }
        };
        let (input, expired) = plan;
        let (result, rows) = match engine.memory_plan(input).await {
            Ok(pair) => pair,
            Err(e) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "MEMORY_ERROR",
                    e.to_string(),
                )
            }
        };
        let coord = coord.read().await;
        if let Err(e) = coord
            .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows })
            .await
        {
            return cluster_write_error(e);
        }
        return match coord
            .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryExpire { ids: expired })
            .await
        {
            Ok(_) => api_ok(serde_json::json!({ "consolidated": result })),
            Err(e) => cluster_write_error(e),
        };
    }

    match engine.memory_consolidate(&scope, keep).await {
        Ok(Some(m)) => api_ok(serde_json::json!({ "consolidated": m })),
        Ok(None) => api_ok(serde_json::json!({ "consolidated": null })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// POST /api/v1/admin/memory/consolidate-similar — fold semantically-similar memory clusters into
/// abstractions (the "near-duplicate cluster" consolidation). Cluster-replicated.
pub async fn memory_consolidate_similar(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryConsolidateSimilarRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_consolidate_similar")
        .increment(1);
    let scope = scope_from(
        &auth,
        req.tenant_id.as_deref(),
        req.user_id.as_deref(),
        req.agent_id.as_deref(),
        req.session_id.as_deref(),
    );
    let threshold = req.threshold.unwrap_or(0.92);

    let plans = match engine
        .memory_consolidate_similar_plan(&scope, threshold)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            )
        }
    };
    let clusters = plans.len();

    // Cluster mode: for each fold, replicate the summary (MemoryUpsert) + the originals (MemoryExpire).
    if let Some(Extension(coord)) = cluster {
        let coord = coord.read().await;
        for (input, expired) in plans {
            let (_result, rows) = match engine.memory_plan(input).await {
                Ok(p) => p,
                Err(e) => {
                    return api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "MEMORY_ERROR",
                        e.to_string(),
                    )
                }
            };
            if let Err(e) = coord
                .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryUpsert { rows })
                .await
            {
                return cluster_write_error(e);
            }
            if let Err(e) = coord
                .client_write(ecphoria_cluster::raft::types::AppRequest::MemoryExpire {
                    ids: expired,
                })
                .await
            {
                return cluster_write_error(e);
            }
        }
        return api_ok(serde_json::json!({ "clusters_folded": clusters }));
    }

    for (input, expired) in plans {
        if let Err(e) = engine.memory_add(input).await {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            );
        }
        if let Err(e) = engine.memory_expire(&expired).await {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MEMORY_ERROR",
                e.to_string(),
            );
        }
    }
    api_ok(serde_json::json!({ "clusters_folded": clusters }))
}

/// Upsert a pre-computed multi-modal embedding (text/image/audio/…).
pub async fn semantic_upsert(
    State(engine): State<Arc<EcphoriaEngine>>,
    Json(req): Json<SemanticUpsertRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "semantic_upsert").increment(1);
    let id = req
        .id
        .and_then(|s| uuid::Uuid::parse_str(&s).ok())
        .unwrap_or_else(uuid::Uuid::new_v4);
    match engine
        .semantic_upsert_modal(
            id,
            &req.modality,
            req.content,
            req.embedding,
            req.metadata.unwrap_or_else(|| serde_json::json!({})),
        )
        .await
    {
        Ok(()) => api_ok(serde_json::json!({ "id": id.to_string() })),
        Err(e) => api_error(StatusCode::BAD_REQUEST, "SEMANTIC_ERROR", e.to_string()),
    }
}

/// Vector search optionally restricted to one modality.
pub async fn semantic_modal_search(
    State(engine): State<Arc<EcphoriaEngine>>,
    Json(req): Json<ModalSearchRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "semantic_modal_search")
        .increment(1);
    match engine
        .semantic_search_modal(&req.vector, req.k.unwrap_or(5), req.modality.as_deref())
        .await
    {
        Ok(results) => api_ok(serde_json::json!({ "results": results })),
        Err(e) => api_error(StatusCode::BAD_REQUEST, "SEMANTIC_ERROR", e.to_string()),
    }
}

/// Add a graph edge between two entities (tenant-scoped).
pub async fn memory_link(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    cluster: Option<
        Extension<std::sync::Arc<tokio::sync::RwLock<ecphoria_cluster::ClusterCoordinator>>>,
    >,
    Json(req): Json<MemoryLinkRequest>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_link").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());

    // Cluster mode: generate the edge (id) on the leader and replicate it through the Raft log so
    // followers apply the identical row (was previously snapshot-only).
    if let Some(Extension(coord)) = cluster {
        let at = chrono::Utc::now();
        let id = uuid::Uuid::new_v4();
        let coord = coord.read().await;
        // Functional relation: close the prior active (src, relation) edge first, replicated with
        // the leader-supplied `at`/`by` so every node applies the identical close.
        if req.supersede {
            let sup = ecphoria_cluster::raft::types::AppRequest::GraphSupersede {
                tenant: Some(tenant.clone()),
                src: req.src.clone(),
                relation: req.relation.clone(),
                at,
                by: Some(id),
            };
            if let Err(e) = coord.client_write(sup).await {
                return cluster_write_error(e);
            }
        }
        let edge = ecphoria_core::memory::cognition::Edge {
            id,
            src: req.src,
            relation: req.relation,
            dst: req.dst,
            weight: 1.0,
            source_memory_id: None,
            valid_from: Some(at),
            ..Default::default()
        };
        let ar = ecphoria_cluster::raft::types::AppRequest::GraphAddEdge {
            tenant: Some(tenant),
            edge,
        };
        return match coord.client_write(ar).await {
            Ok(_) => api_ok(serde_json::json!({ "status": "ok" })),
            Err(e) => cluster_write_error(e),
        };
    }

    let result = if req.supersede {
        engine
            .memory_link_functional(&tenant, &req.src, &req.relation, &req.dst, None)
            .await
    } else {
        engine
            .memory_link(&tenant, &req.src, &req.relation, &req.dst, None)
            .await
    };
    match result {
        Ok(()) => api_ok(serde_json::json!({ "status": "ok" })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}

/// Get an entity's 1-hop neighborhood in the memory graph (tenant-scoped).
/// List all knowledge-graph edges for the tenant (bulk graph view / export).
///
/// GET /api/v1/memories/edges?limit=N
pub async fn memory_edges(
    State(engine): State<Arc<EcphoriaEngine>>,
    auth: Option<Extension<crate::auth::middleware::AuthContext>>,
    axum::extract::Query(params): axum::extract::Query<MemoryEdgesQuery>,
) -> Response {
    metrics::counter!("ecphoria_rest_requests_total", "endpoint" => "memory_edges").increment(1);
    let tenant = auth
        .as_ref()
        .and_then(|Extension(c)| c.tenant_id.clone())
        .unwrap_or_else(|| "default".into());
    match engine
        .memory_edges(&tenant, params.limit.unwrap_or(10_000))
        .await
    {
        Ok(edges) => api_ok(serde_json::json!({ "edges": edges, "count": edges.len() })),
        Err(e) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEMORY_ERROR",
            e.to_string(),
        ),
    }
}
