//! Tests for the agent runtime. Live beside `agentic.rs` and compile only with the `agentic`
//! feature — a memory-only build has nothing here to test.

use super::super::*;
use super::*;
use crate::engine::tests::inmem_config;

#[tokio::test]
async fn run_ledger_lifecycle() {
    use crate::runtime::{RunPatch, RunStatus};
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let run = engine
        .run_create(
            "default",
            Some("agent-1".into()),
            None,
            serde_json::json!({"q": "hi"}),
        )
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Pending);

    engine
        .run_update(
            run.id,
            RunPatch {
                status: Some(RunStatus::Running),
                started_at: Some(chrono::Utc::now()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    engine
        .run_update(
            run.id,
            RunPatch {
                status: Some(RunStatus::Succeeded),
                result: Some(serde_json::json!({"a": 42})),
                ended_at: Some(chrono::Utc::now()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let got = engine.run_get(run.id).await.unwrap().unwrap();
    assert_eq!(got.status, RunStatus::Succeeded);
    assert_eq!(got.result, serde_json::json!({"a": 42}));
    assert!(got.started_at.is_some() && got.ended_at.is_some());
    assert_eq!(got.input, serde_json::json!({"q": "hi"}));

    assert_eq!(engine.run_list("default", None, 10).await.unwrap().len(), 1);
    assert!(engine
        .run_list("default", Some(RunStatus::Running), 10)
        .await
        .unwrap()
        .is_empty());
    // A fresh run has an empty step trace (no episodic events tagged with its id yet).
    assert!(engine.run_trace(run.id).await.unwrap().is_empty());
}

#[tokio::test]
async fn run_apply_is_deterministic_across_engines() {
    // The same materialized run + patch applied on two engines yields identical rows — the
    // replication-safety contract (no now()/uuid at apply time), ready for a GraphSupersede-style
    // RunCreate/RunUpdate AppRequest.
    use crate::runtime::{Run, RunPatch, RunStatus};
    let now = chrono::Utc::now();
    let id = uuid::Uuid::new_v4();
    let seed = Run {
        id,
        tenant_id: "default".into(),
        agent_id: None,
        parent_run_id: None,
        status: RunStatus::Pending,
        input: serde_json::json!({"x": 1}),
        result: serde_json::Value::Null,
        error: None,
        cursor: serde_json::Value::Null,
        created_at: now,
        updated_at: now,
        started_at: None,
        ended_at: None,
    };
    let patch = RunPatch {
        status: Some(RunStatus::Succeeded),
        result: Some(serde_json::json!({"ok": true})),
        ended_at: Some(now),
        ..Default::default()
    };

    let mut rows = Vec::new();
    for _ in 0..2 {
        let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
        engine.run_apply_create(&seed).await.unwrap();
        engine.run_apply_update(id, &patch, now).await.unwrap();
        rows.push(engine.run_get(id).await.unwrap().unwrap());
    }
    assert_eq!(rows[0].status, RunStatus::Succeeded);
    assert_eq!(rows[0].result, rows[1].result);
    assert_eq!(rows[0].updated_at, rows[1].updated_at);
    assert_eq!(rows[0].ended_at, rows[1].ended_at);
}

#[tokio::test]
async fn run_agent_loops_tool_then_answers_and_journals_steps() {
    use crate::llm::CompletionProvider;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Scripted model: first turn calls the search tool, second turn answers.
    struct Scripted {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl CompletionProvider for Scripted {
        async fn complete(&self, _system: &str, _user: &str) -> crate::Result<String> {
            Ok(match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => "TOOL search: cats".to_string(),
                _ => "Cats are fluffy companions.".to_string(),
            })
        }
        fn model_name(&self) -> &str {
            "scripted"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Scripted {
        calls: AtomicUsize::new(0),
    }));
    let scope = MemoryScope {
        tenant_id: "default".into(),
        agent_id: Some("a1".into()),
        ..Default::default()
    };
    engine
        .memory_add(MemoryInput::new(scope, "cats are fluffy"))
        .await
        .unwrap();

    let run = engine
        .run_agent("default", "a1", "tell me about cats", 5)
        .await
        .unwrap();

    assert_eq!(run.status, crate::runtime::RunStatus::Succeeded);
    assert_eq!(run.result["answer"], "Cats are fluffy companions.");

    // The trace journaled run_start + tool_call + llm_answer (3 steps), recallable by run id.
    let steps = engine.run_trace(run.id).await.unwrap();
    assert_eq!(steps.len(), 3);
    assert!(steps.iter().any(|s| s["event_type"] == "tool_call"));
    assert!(steps.iter().any(|s| s["event_type"] == "llm_answer"));
}

#[tokio::test]
async fn triggers_register_match_and_fire_runs() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine
        .trigger_register(
            "default",
            "on_pr",
            "github",
            "pull_request.opened",
            "pr-agent",
        )
        .await
        .unwrap();
    engine
        .trigger_register("default", "on_any", "*", "*", "catch-all")
        .await
        .unwrap();
    assert_eq!(engine.trigger_list("default").await.unwrap().len(), 2);

    // A GitHub PR event matches both the exact and the wildcard trigger → 2 runs.
    let fired = engine
        .fire_triggers(
            "default",
            "github",
            "pull_request.opened",
            serde_json::json!({"pr": 42}),
        )
        .await
        .unwrap();
    assert_eq!(fired.len(), 2);

    // A different source matches only the wildcard → 1 run.
    let fired2 = engine
        .fire_triggers("default", "sentry", "issue.created", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(fired2.len(), 1);

    assert_eq!(engine.run_list("default", None, 10).await.unwrap().len(), 3);
}

#[tokio::test]
async fn hitl_request_then_approve() {
    use crate::runtime::RunStatus;
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let run = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();

    engine
        .run_request_approval(run.id, "default", "ship it?")
        .await
        .unwrap();
    assert_eq!(
        engine.run_get(run.id).await.unwrap().unwrap().status,
        RunStatus::WaitingApproval
    );
    assert_eq!(
        engine.run_approval_status(run.id).await.unwrap().unwrap()["state"],
        "pending"
    );

    engine
        .run_resolve_approval(run.id, "default", true)
        .await
        .unwrap();
    assert_eq!(
        engine.run_get(run.id).await.unwrap().unwrap().status,
        RunStatus::Running
    );
    assert_eq!(
        engine.run_approval_status(run.id).await.unwrap().unwrap()["state"],
        "approved"
    );
}

#[tokio::test]
async fn run_workflow_executes_subagents_in_dep_order() {
    use crate::llm::CompletionProvider;
    use crate::runtime::{RunStatus, WorkflowNode};

    struct Echo;
    #[async_trait::async_trait]
    impl CompletionProvider for Echo {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok("done".to_string())
        }
        fn model_name(&self) -> &str {
            "echo"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Echo));

    // Node "b" depends on "a"; topo order must run a before b.
    let nodes = vec![
        WorkflowNode {
            id: "b".into(),
            agent_id: "agent".into(),
            question: "second".into(),
            deps: vec!["a".into()],
        },
        WorkflowNode {
            id: "a".into(),
            agent_id: "agent".into(),
            question: "first".into(),
            deps: vec![],
        },
    ];
    let parent = engine.run_workflow("default", nodes).await.unwrap();
    assert_eq!(parent.status, RunStatus::Succeeded);

    // Parent + 2 sub-agent children; children link back to the parent.
    let runs = engine.run_list("default", None, 10).await.unwrap();
    assert_eq!(runs.len(), 3);
    let children: Vec<_> = runs
        .iter()
        .filter(|r| r.parent_run_id == Some(parent.id))
        .collect();
    assert_eq!(children.len(), 2);
}

#[tokio::test]
async fn run_agent_pauses_for_approval_then_resumes() {
    use crate::llm::CompletionProvider;
    use crate::runtime::RunStatus;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Scripted {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl CompletionProvider for Scripted {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok(match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => "TOOL approve: deploy to prod?".to_string(),
                _ => "deployed".to_string(),
            })
        }
        fn model_name(&self) -> &str {
            "scripted"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Scripted {
        calls: AtomicUsize::new(0),
    }));

    // The agent asks for approval → run pauses.
    let run = engine
        .run_agent("default", "a", "ship it", 5)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::WaitingApproval);

    // Approve, then resume → the agent continues and finishes.
    engine
        .run_resolve_approval(run.id, "default", true)
        .await
        .unwrap();
    let resumed = engine.run_resume(run.id, "default").await.unwrap();
    assert_eq!(resumed.status, RunStatus::Succeeded);
    assert_eq!(resumed.result["answer"], "deployed");
}

#[tokio::test]
async fn rebuild_transcript_is_faithful_and_keeps_tool_seq_stable() {
    // Regression for the resume path: every journaled step type must re-render as the exact line
    // the live loop emitted. Otherwise the idempotency counter (which counts "TOOL call ") resets
    // to 0 and external-tool results are erased — defeating effectively-once across a resume.
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let run = engine
        .run_create("default", Some("a1".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    let rid = run.id;
    for (et, payload) in [
        ("run_start", serde_json::json!({ "question": "Q?" })),
        (
            "tool_call",
            serde_json::json!({ "tool": "search", "query": "cats", "results": ["fluffy", "cute"] }),
        ),
        (
            "tool_call",
            serde_json::json!({ "tool": "remember", "content": "cats are fluffy" }),
        ),
        (
            "tool_call",
            serde_json::json!({ "tool": "billing/charge", "result": { "ok": true } }),
        ),
        (
            "tool_call",
            serde_json::json!({ "tool": "email/send", "result": { "sent": 1 } }),
        ),
    ] {
        engine
            .run_log_step(rid, "default", et, payload)
            .await
            .unwrap();
    }

    let t = engine.rebuild_agent_transcript(rid).await.unwrap();

    assert!(t.contains("Question: Q?"), "{t}");
    assert!(t.contains("TOOL search: cats"), "{t}");
    assert!(
        t.contains("fluffy | cute"),
        "search results must be replayed: {t}"
    );
    assert!(t.contains("TOOL remember: cats are fluffy"), "{t}");
    // External calls must re-render as `TOOL call …` with their real result replayed…
    assert!(t.contains("TOOL call billing charge"), "{t}");
    assert!(t.contains("TOOL call email send"), "{t}");
    assert!(
        t.contains("\"ok\":true"),
        "external result must be replayed: {t}"
    );
    // …so on resume the idempotency counter is 2 (the two prior external calls), not 0.
    assert_eq!(
        t.matches("TOOL call ").count(),
        2,
        "tool_seq must resume at the count of prior external calls: {t}"
    );
}

#[tokio::test]
async fn approval_cannot_be_resolved_twice() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let run = engine
        .run_create("default", Some("a1".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_request_approval(run.id, "default", "ok to proceed?")
        .await
        .unwrap();
    // First resolution succeeds...
    engine
        .run_resolve_approval(run.id, "default", true)
        .await
        .unwrap();
    // ...a second (double-approve / late reject) is rejected — approval is no longer pending.
    assert!(engine
        .run_resolve_approval(run.id, "default", false)
        .await
        .is_err());
}

#[tokio::test]
async fn erroring_agent_run_is_marked_failed_not_left_running() {
    use crate::llm::CompletionProvider;
    // A provider that always errors → the loop returns Err on the first turn.
    struct Boom;
    #[async_trait::async_trait]
    impl CompletionProvider for Boom {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Err(crate::Error::Llm("boom".into()))
        }
        fn model_name(&self) -> &str {
            "boom"
        }
    }
    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Boom));
    let run = engine
        .run_create("default", Some("a1".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    let res = engine
        .drive_agent_loop(run.id, "default", "a1", String::new(), 8)
        .await;
    assert!(res.is_err(), "the erroring loop must surface the error");
    // The run must be terminal (Failed), so the dispatcher won't resume it forever (poison run).
    let after = engine.run_get(run.id).await.unwrap().unwrap();
    assert_eq!(after.status, RunStatus::Failed);
}

#[tokio::test]
async fn resume_reuses_recorded_tool_result_without_re_executing() {
    use crate::llm::CompletionProvider;
    use crate::runtime::{RunStatus, ToolExecutor};
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Scripted model: issues the external call, then answers.
    struct Scripted {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl CompletionProvider for Scripted {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok(match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => "TOOL call billing charge: {}".to_string(),
                _ => "done".to_string(),
            })
        }
        fn model_name(&self) -> &str {
            "scripted"
        }
    }
    // Counts how many times the external tool ACTUALLY runs.
    struct CountingTool {
        runs: std::sync::Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl ToolExecutor for CountingTool {
        async fn call_tool(
            &self,
            _server: &str,
            _tool: &str,
            _args: serde_json::Value,
        ) -> crate::Result<serde_json::Value> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!({ "charged": true }))
        }
    }

    let runs = std::sync::Arc::new(AtomicUsize::new(0));
    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Scripted {
        calls: AtomicUsize::new(0),
    }));
    engine.set_tool_executor(std::sync::Arc::new(CountingTool { runs: runs.clone() }));

    // Pre-journal the tool_call (idempotency key :tool:0) as if it already ran in a prior
    // attempt — the server-side ledger must reuse its result and NOT re-execute the side effect.
    let run = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_log_step(
            run.id,
            "default",
            "tool_call",
            serde_json::json!({
                "tool": "billing/charge",
                "idempotency_key": format!("{}:tool:0", run.id),
                "result": { "charged": true },
            }),
        )
        .await
        .unwrap();

    let out = engine
        .drive_agent_loop(run.id, "default", "a", String::new(), 5)
        .await
        .unwrap();
    assert_eq!(out.status, RunStatus::Succeeded);
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "the external tool must not re-execute — its recorded result is reused"
    );
}

#[tokio::test]
async fn run_agent_calls_external_tool_via_executor() {
    use crate::llm::CompletionProvider;
    use crate::runtime::{RunStatus, ToolExecutor};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Scripted {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl CompletionProvider for Scripted {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok(match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => "TOOL call gh create_issue: {\"title\":\"bug\"}".to_string(),
                _ => "issue created".to_string(),
            })
        }
        fn model_name(&self) -> &str {
            "scripted"
        }
    }

    struct MockTool;
    #[async_trait::async_trait]
    impl ToolExecutor for MockTool {
        async fn call_tool(
            &self,
            server: &str,
            tool: &str,
            args: serde_json::Value,
        ) -> crate::Result<serde_json::Value> {
            Ok(serde_json::json!({ "server": server, "tool": tool, "args": args, "ok": true }))
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Scripted {
        calls: AtomicUsize::new(0),
    }));
    engine.set_tool_executor(std::sync::Arc::new(MockTool));

    let run = engine
        .run_agent("default", "a", "make an issue", 5)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(run.result["answer"], "issue created");

    // The downstream tool call was executed and journaled.
    let steps = engine.run_trace(run.id).await.unwrap();
    // The tool call was journaled with a deterministic idempotency key (first call → :tool:0).
    assert!(steps.iter().any(|s| s["event_type"] == "tool_call"
        && s["payload"]["tool"] == "gh/create_issue"
        && s["payload"]["idempotency_key"] == format!("{}:tool:0", run.id)));
}

#[tokio::test]
async fn run_writes_route_through_replicator_when_set() {
    use crate::runtime::{Run, RunPatch, RunReplicator};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Recorder {
        creates: AtomicUsize,
        updates: AtomicUsize,
        steps: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl RunReplicator for Recorder {
        async fn replicate_run_create(&self, _run: &Run) -> crate::Result<()> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn replicate_run_update(
            &self,
            _id: uuid::Uuid,
            _patch: &RunPatch,
            _updated_at: chrono::DateTime<chrono::Utc>,
        ) -> crate::Result<()> {
            self.updates.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn replicate_step(
            &self,
            _event: crate::memory::episodic::Event,
        ) -> crate::Result<()> {
            self.steps.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn replicate_state_set(
            &self,
            _agent_id: &str,
            _key: &str,
            _value: serde_json::Value,
        ) -> crate::Result<()> {
            Ok(())
        }
    }

    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let rec = std::sync::Arc::new(Recorder::default());
    engine.set_run_replicator(rec.clone());

    let run = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_update(
            run.id,
            RunPatch {
                status: Some(RunStatus::Running),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    engine
        .run_log_step(run.id, "default", "test", serde_json::json!({}))
        .await
        .unwrap();

    assert_eq!(rec.creates.load(Ordering::SeqCst), 1);
    assert_eq!(rec.updates.load(Ordering::SeqCst), 1);
    assert_eq!(rec.steps.load(Ordering::SeqCst), 1);
    // The recorder doesn't apply, so the local store was bypassed (apply happens via Raft).
    assert!(engine.run_get(run.id).await.unwrap().is_none());
}

#[tokio::test]
async fn dispatcher_resumes_orphaned_running_run() {
    use crate::llm::CompletionProvider;
    use crate::runtime::{RunPatch, RunStatus};

    struct Echo;
    #[async_trait::async_trait]
    impl CompletionProvider for Echo {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok("done".to_string())
        }
        fn model_name(&self) -> &str {
            "echo"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Echo));

    // A run that was left "running" with a journaled start step but a STALE updated_at — as if
    // the leader driving it crashed mid-loop.
    let run = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_log_step(
            run.id,
            "default",
            "run_start",
            serde_json::json!({ "question": "hi" }),
        )
        .await
        .unwrap();
    let stale = chrono::Utc::now() - chrono::Duration::seconds(120);
    engine
        .run_apply_update(
            run.id,
            &RunPatch {
                status: Some(RunStatus::Running),
                ..Default::default()
            },
            stale,
        )
        .await
        .unwrap();

    // A fresh (non-stale) running run must NOT be picked up.
    let fresh = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_update(
            fresh.id,
            RunPatch {
                status: Some(RunStatus::Running),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let resumed = engine.run_dispatch_once(60, 10).await.unwrap();
    assert_eq!(resumed, 1);
    assert_eq!(
        engine.run_get(run.id).await.unwrap().unwrap().status,
        RunStatus::Succeeded
    );
    // The fresh run was skipped (still running, not driven).
    assert_eq!(
        engine.run_get(fresh.id).await.unwrap().unwrap().status,
        RunStatus::Running
    );
}

/// One tenant's triggers must never fire on — or be visible to — another tenant.
///
/// Triggers were stored under a single unscoped state key, so `fire_triggers` iterated every
/// tenant's triggers on every webhook. On a shared deployment that is both a cross-tenant
/// information leak (the trigger names and target agents are listable) and a cross-tenant *action*
/// trigger: tenant B's webhook would start tenant A's agent, with tenant B's payload as input.
#[tokio::test]
async fn triggers_are_isolated_between_tenants() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine
        .trigger_register(
            "acme",
            "on_pr",
            "github",
            "pull_request.opened",
            "acme-agent",
        )
        .await
        .unwrap();
    engine
        .trigger_register("globex", "on_any", "*", "*", "globex-agent")
        .await
        .unwrap();

    // Listing is per-tenant.
    assert_eq!(engine.trigger_list("acme").await.unwrap().len(), 1);
    assert_eq!(engine.trigger_list("globex").await.unwrap().len(), 1);
    assert!(engine
        .trigger_list("someone-else")
        .await
        .unwrap()
        .is_empty());

    // Acme's event fires only Acme's trigger, even though globex has a catch-all `*`/`*`.
    let fired = engine
        .fire_triggers(
            "acme",
            "github",
            "pull_request.opened",
            serde_json::json!({"pr": 42}),
        )
        .await
        .unwrap();
    assert_eq!(fired.len(), 1, "globex's catch-all fired on acme's event");
    let run = engine.run_get(fired[0]).await.unwrap().unwrap();
    assert_eq!(run.tenant_id, "acme");
    assert_eq!(run.agent_id.as_deref(), Some("acme-agent"));

    // And a tenant with no triggers gets none, however busy its neighbours are.
    assert!(engine
        .fire_triggers(
            "initech",
            "github",
            "pull_request.opened",
            serde_json::json!({})
        )
        .await
        .unwrap()
        .is_empty());
}

/// `run_agent_start` + `run_agent_drive` must equal `run_agent`.
///
/// The split exists so the gateway can answer `202 Accepted` and drive the loop in the background
/// — a multi-turn run outlives an HTTP request, and running it inline under the 30 s request
/// timeout returned 504 to the caller while the run kept executing invisibly. If the two halves
/// diverged from the one-shot path, background runs would behave differently from synchronous
/// ones for no visible reason.
#[tokio::test]
async fn split_agent_start_and_drive_match_the_one_shot_path() {
    use crate::llm::CompletionProvider;

    struct Fixed;
    #[async_trait::async_trait]
    impl CompletionProvider for Fixed {
        async fn complete(&self, _system: &str, _user: &str) -> crate::Result<String> {
            Ok("The deploy target is kubernetes.".to_string())
        }
        fn model_name(&self) -> &str {
            "fixed"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Fixed));

    let started = engine
        .run_agent_start("default", "assistant", "what is the deploy target?", None)
        .await
        .unwrap();
    assert_eq!(started.status, RunStatus::Running);
    // The opening step is journaled before any driving, so the transcript can be rebuilt — this
    // is what lets the background task (or the crash dispatcher) pick the run up.
    let trace = engine.run_trace(started.id).await.unwrap();
    assert_eq!(trace.len(), 1);
    assert_eq!(trace[0]["event_type"], "run_start");

    let finished = engine.run_agent_drive(started.id, 4).await.unwrap();
    assert_eq!(finished.id, started.id, "driving must not create a new run");
    assert_eq!(finished.status, RunStatus::Succeeded);
    assert!(
        format!("{:?}", finished.result).contains("kubernetes"),
        "{:?}",
        finished.result
    );
}
