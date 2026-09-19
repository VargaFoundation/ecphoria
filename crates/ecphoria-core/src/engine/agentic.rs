//! The agentic half of the engine: the durable run ledger, the agent driver, HITL approvals,
//! DAG workflows, event triggers and the downstream tool gateway.
//!
//! Split out from `engine/mod.rs` and gated behind the `agentic` feature so that an Ecphoria built
//! purely as a memory substrate does not carry it. That build — `ecphoria:memory` — has no run
//! ledger, no driver leases, and no way to make an outbound LLM call, which is a smaller thing to
//! reason about when the deployment's job is to hold facts rather than to act on them.
//!
//! Nothing here is reachable from the memory paths: the split is a move, not a rewrite, and the
//! compiler enforces it — if a memory path ever needed a run, this file would not compile out.

use std::sync::Arc;

use crate::memory::cognition::MemoryScope;
use crate::memory::episodic::Event;
use crate::runtime::{Run, RunPatch, RunReplicator, RunStatus, ToolExecutor, WorkflowNode};
use crate::{EcphoriaEngine, Result};

impl EcphoriaEngine {
    /// Inject a tool executor (e.g. the gateway's MCP tool-gateway) so the agent loop can invoke
    /// external tools via `TOOL call <server> <tool>: {args}`. Replaces any previous executor.
    pub fn set_tool_executor(&self, executor: Arc<dyn ToolExecutor>) {
        *self.tool_executor.write() = Some(executor);
    }

    /// Inject a run replicator (cluster mode) so the agent driver's run/step writes go through Raft
    /// and survive leader failover. Absent → writes are local.
    pub fn set_run_replicator(&self, replicator: Arc<dyn RunReplicator>) {
        *self.run_replicator.write() = Some(replicator);
    }

    // ---- Agent-run ledger (agentic-platform substrate) ----

    /// Create a run (leader-materialized id + timestamps), persisted as `Pending`. Steps are
    /// episodic events tagged `session_id = run_id`; the full trace is [`Self::run_trace`].
    pub async fn run_create(
        &self,
        tenant: &str,
        agent_id: Option<String>,
        parent_run_id: Option<uuid::Uuid>,
        input: serde_json::Value,
    ) -> Result<Run> {
        let now = chrono::Utc::now();
        let run = Run {
            id: uuid::Uuid::new_v4(),
            tenant_id: if tenant.is_empty() {
                "default".into()
            } else {
                tenant.to_string()
            },
            agent_id,
            parent_run_id,
            status: RunStatus::Pending,
            input,
            result: serde_json::Value::Null,
            error: None,
            cursor: serde_json::Value::Null,
            created_at: now,
            updated_at: now,
            started_at: None,
            ended_at: None,
        };
        // Cluster mode: replicate through Raft (apply writes on every node). Else write locally.
        let replicator = self.run_replicator.read().clone();
        match replicator {
            Some(r) => r.replicate_run_create(&run).await?,
            None => self.run_apply_create(&run).await?,
        }
        Ok(run)
    }

    /// Apply a fully-materialized run (deterministic — used by Raft apply).
    pub async fn run_apply_create(&self, run: &Run) -> Result<()> {
        metrics::counter!("ecphoria_runs_created_total").increment(1);
        self.runs.create(run).await
    }

    /// Patch a run, stamping `updated_at = now`. Replicates through Raft in cluster mode.
    pub async fn run_update(&self, id: uuid::Uuid, patch: RunPatch) -> Result<bool> {
        let now = chrono::Utc::now();
        let replicator = self.run_replicator.read().clone();
        match replicator {
            Some(r) => {
                r.replicate_run_update(id, &patch, now).await?;
                Ok(true)
            }
            None => self.run_apply_update(id, &patch, now).await,
        }
    }

    /// Apply a run patch with a leader-supplied `updated_at` (deterministic — used by Raft apply).
    pub async fn run_apply_update(
        &self,
        id: uuid::Uuid,
        patch: &RunPatch,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool> {
        if let Some(s) = patch.status {
            if s.is_terminal() {
                metrics::counter!("ecphoria_runs_completed_total", "status" => s.as_str())
                    .increment(1);
            }
        }
        self.runs.update(id, patch, updated_at).await
    }

    /// Get a run by id.
    pub async fn run_get(&self, id: uuid::Uuid) -> Result<Option<Run>> {
        self.runs.get(id).await
    }

    /// List a tenant's runs (newest first), optionally filtered by status.
    pub async fn run_list(
        &self,
        tenant: &str,
        status: Option<RunStatus>,
        limit: usize,
    ) -> Result<Vec<Run>> {
        let tenant = if tenant.is_empty() { "default" } else { tenant };
        self.runs
            .list(tenant, status, limit.min(self.config.query.max_rows))
            .await
    }

    /// Full step trace of a run = the episodic events tagged with `session_id = run_id`.
    pub async fn run_trace(&self, id: uuid::Uuid) -> Result<Vec<serde_json::Value>> {
        self.session_recall(&id.to_string()).await
    }

    // ── Event triggers (event-driven agent runs) ─────────────────────

    /// State-store agent id holding one tenant's triggers.
    ///
    /// The tenant is part of the key, not a field inside the value, because both the listing and
    /// the firing loop enumerate keys — a tenant field would have to be filtered correctly at
    /// every call site, and missing one leaks. Namespacing makes cross-tenant access impossible to
    /// express rather than merely incorrect.
    fn trigger_agent(tenant: &str) -> String {
        let t = if tenant.is_empty() { "default" } else { tenant };
        format!("__trigger:{t}")
    }

    /// State-store agent id holding the downstream MCP tool catalog.
    ///
    /// Deliberately **not** tenant-namespaced, unlike triggers: this is an operator-level catalog
    /// of which external servers exist, not per-tenant data. Restrict who may write it with RBAC
    /// (`/api/v1/tools` is a normal authenticated route).
    const TOOL_CATALOG_AGENT: &'static str = "__tools";

    /// Persist a downstream MCP server registration.
    ///
    /// The gateway keeps an in-memory map for the call path; this is the durable copy. Without it
    /// the catalog was lost on restart and diverged between cluster nodes, so an agent's tool call
    /// succeeded or 404'd depending on which node served it. Routing through the state store means
    /// it replicates through Raft like any other state write.
    pub async fn tool_server_register(&self, name: &str, url: &str) -> Result<()> {
        self.state_set_via_driver(
            Self::TOOL_CATALOG_AGENT,
            name,
            serde_json::json!({ "url": url }),
        )
        .await
        .map(|_| ())
    }

    /// Forget a downstream MCP server.
    pub async fn tool_server_remove(&self, name: &str) -> Result<()> {
        self.state_delete(Self::TOOL_CATALOG_AGENT, name).await
    }

    /// Every persisted `(name, url)` — used to repopulate the gateway's map at startup.
    pub async fn tool_server_list(&self) -> Result<Vec<(String, String)>> {
        let mut out = Vec::new();
        for name in self.state_list_keys(Self::TOOL_CATALOG_AGENT).await? {
            if let Some(entry) = self.state_get(Self::TOOL_CATALOG_AGENT, &name).await? {
                if let Some(url) = entry.value.get("url").and_then(|v| v.as_str()) {
                    out.push((name, url.to_string()));
                }
            }
        }
        Ok(out)
    }

    /// Register an event trigger: when an event matching `source` + `event_type` (each `*` = any)
    /// is observed for **this tenant**, [`Self::fire_triggers`] starts a run of `agent_id`.
    /// Persisted in the state store (so it replicates via `StateSet`).
    pub async fn trigger_register(
        &self,
        tenant: &str,
        name: &str,
        source: &str,
        event_type: &str,
        agent_id: &str,
    ) -> Result<()> {
        self.state_set_via_driver(
            &Self::trigger_agent(tenant),
            name,
            serde_json::json!({ "source": source, "event_type": event_type, "agent_id": agent_id }),
        )
        .await
        .map(|_| ())
    }

    /// List a tenant's registered event triggers.
    pub async fn trigger_list(&self, tenant: &str) -> Result<Vec<serde_json::Value>> {
        let agent = Self::trigger_agent(tenant);
        let mut out = Vec::new();
        for name in self.state_list_keys(&agent).await? {
            if let Some(entry) = self.state_get(&agent, &name).await? {
                let mut v = entry.value;
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("name".into(), name.clone().into());
                }
                out.push(v);
            }
        }
        Ok(out)
    }

    /// Fire all triggers matching an event, starting a run per match. Returns the new run ids. The
    /// hook for event-driven agents (e.g. call this after a webhook ingest).
    pub async fn fire_triggers(
        &self,
        tenant: &str,
        source: &str,
        event_type: &str,
        input: serde_json::Value,
    ) -> Result<Vec<uuid::Uuid>> {
        let mut fired = Vec::new();
        let agent = Self::trigger_agent(tenant);
        for name in self.state_list_keys(&agent).await.unwrap_or_default() {
            let Ok(Some(entry)) = self.state_get(&agent, &name).await else {
                continue;
            };
            let v = entry.value;
            let want_src = v.get("source").and_then(|x| x.as_str()).unwrap_or("*");
            let want_evt = v.get("event_type").and_then(|x| x.as_str()).unwrap_or("*");
            if (want_src == "*" || want_src == source)
                && (want_evt == "*" || want_evt == event_type)
            {
                let agent = v
                    .get("agent_id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("trigger")
                    .to_string();
                let run = self
                    .run_create(tenant, Some(agent), None, input.clone())
                    .await?;
                fired.push(run.id);
            }
        }
        Ok(fired)
    }

    // ── Human-in-the-loop (HITL) ─────────────────────────────────────

    /// Pause a run for human approval: set it `WaitingApproval`, record a `pending` approval in the
    /// state store (keyed by run id, so a watcher can wake the driver), and journal a `hitl_request`.
    pub async fn run_request_approval(
        &self,
        run_id: uuid::Uuid,
        tenant: &str,
        prompt: &str,
    ) -> Result<()> {
        self.state_set_via_driver(
            &format!("__approval:{run_id}"),
            "status",
            serde_json::json!({ "state": "pending", "prompt": prompt }),
        )
        .await?;
        self.run_update(
            run_id,
            RunPatch {
                status: Some(RunStatus::WaitingApproval),
                ..Default::default()
            },
        )
        .await?;
        self.run_log_step(
            run_id,
            tenant,
            "hitl_request",
            serde_json::json!({ "prompt": prompt }),
        )
        .await?;
        Ok(())
    }

    /// Resolve a pending approval: record the verdict and move the run back to `Running` (approved)
    /// or `Cancelled` (rejected); journal a `hitl_resolve` step.
    pub async fn run_resolve_approval(
        &self,
        run_id: uuid::Uuid,
        tenant: &str,
        approved: bool,
    ) -> Result<()> {
        // Only a *pending* approval may be resolved: reject a double-approve / approve-then-reject
        // race and any resolve of a run that isn't actually awaiting approval (which would otherwise
        // flip a terminal run back to Running, or resolve the same approval twice).
        let is_pending = self
            .run_approval_status(run_id)
            .await?
            .as_ref()
            .and_then(|v| v.get("state"))
            .and_then(|s| s.as_str())
            == Some("pending");
        if !is_pending {
            return Err(crate::Error::State(
                "no pending approval to resolve for this run".into(),
            ));
        }
        self.state_set_via_driver(
            &format!("__approval:{run_id}"),
            "status",
            serde_json::json!({ "state": if approved { "approved" } else { "rejected" } }),
        )
        .await?;
        let patch = if approved {
            RunPatch {
                status: Some(RunStatus::Running),
                ..Default::default()
            }
        } else {
            RunPatch {
                status: Some(RunStatus::Cancelled),
                ended_at: Some(chrono::Utc::now()),
                ..Default::default()
            }
        };
        self.run_update(run_id, patch).await?;
        self.run_log_step(
            run_id,
            tenant,
            "hitl_resolve",
            serde_json::json!({ "approved": approved }),
        )
        .await?;
        Ok(())
    }

    /// Current approval state for a run (`pending` / `approved` / `rejected`), if any.
    pub async fn run_approval_status(
        &self,
        run_id: uuid::Uuid,
    ) -> Result<Option<serde_json::Value>> {
        Ok(self
            .state_get(&format!("__approval:{run_id}"), "status")
            .await?
            .map(|e| e.value))
    }

    /// Append one durable step to a run's trace: an episodic event tagged `_session_id = run_id`
    /// (so `run_trace` recalls it) and `_tenant_id`. The step is the unit of agent observability.
    pub async fn run_log_step(
        &self,
        run_id: uuid::Uuid,
        tenant: &str,
        event_type: &str,
        mut payload: serde_json::Value,
    ) -> Result<()> {
        metrics::counter!("ecphoria_run_steps_total", "type" => event_type.to_string())
            .increment(1);
        if !payload.is_object() {
            payload = serde_json::json!({ "value": payload });
        }
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("_session_id".into(), run_id.to_string().into());
            obj.insert(
                "_tenant_id".into(),
                if tenant.is_empty() { "default" } else { tenant }.into(),
            );
        }
        let ev = Event {
            id: uuid::Uuid::new_v4(),
            source: "agent".into(),
            event_type: event_type.into(),
            payload,
            timestamp: chrono::Utc::now(),
            parent_id: None,
            trace_id: Some(run_id.to_string()),
            tags: vec![],
            idempotency_key: None,
        };
        // Cluster mode: replicate the step through Raft so the trace survives failover.
        let replicator = self.run_replicator.read().clone();
        match replicator {
            Some(r) => r.replicate_step(ev).await,
            None => self.ingest(vec![ev]).await.map(|_| ()),
        }
    }

    /// Run a minimal **durable agent loop** on the leader: drive an LLM↔tool loop until it answers,
    /// journaling every step (`run_start` / `tool_call` / `llm_answer`) as part of the run's trace,
    /// and transitioning the run's status. The one built-in tool is `search` (memory retrieval): the
    /// model invokes it by replying `TOOL search: <query>`; any other reply is the final answer.
    ///
    /// Single-node today (writes runs + steps locally); the cluster driver replicates via
    /// `RunCreate`/`RunUpdate` + `Ingest`. Requires a completion provider.
    pub async fn run_agent(
        &self,
        tenant: &str,
        agent_id: &str,
        question: &str,
        max_turns: usize,
    ) -> Result<Run> {
        self.run_agent_with_parent(tenant, agent_id, question, max_turns, None)
            .await
    }

    /// Like [`Self::run_agent`] but links the run to `parent_run_id` — a **sub-agent** of a workflow.
    pub async fn run_agent_with_parent(
        &self,
        tenant: &str,
        agent_id: &str,
        question: &str,
        max_turns: usize,
        parent_run_id: Option<uuid::Uuid>,
    ) -> Result<Run> {
        if self.completion.is_none() {
            return Err(crate::Error::Llm(
                "run_agent requires a completion provider".into(),
            ));
        }
        let run = self
            .run_agent_start(tenant, agent_id, question, parent_run_id)
            .await?;
        self.drive_agent_loop(
            run.id,
            tenant,
            agent_id,
            format!("Question: {question}\n"),
            max_turns,
        )
        .await
    }

    /// Create a run and journal its opening step, **without** driving the loop.
    ///
    /// Split out of [`Self::run_agent_with_parent`] so a caller can answer immediately and drive
    /// the run in the background — a multi-turn loop takes longer than an HTTP request should, and
    /// running it inline under the gateway's request timeout meant a real run returned 504 while
    /// continuing to execute invisibly.
    ///
    /// The returned run is `Running` with a journaled `run_start`, which is exactly the state
    /// [`Self::run_agent_drive`] and the crash-recovery dispatcher expect.
    pub async fn run_agent_start(
        &self,
        tenant: &str,
        agent_id: &str,
        question: &str,
        parent_run_id: Option<uuid::Uuid>,
    ) -> Result<Run> {
        if self.completion.is_none() {
            return Err(crate::Error::Llm(
                "run_agent requires a completion provider".into(),
            ));
        }
        let mut run = self
            .run_create(
                tenant,
                Some(agent_id.to_string()),
                parent_run_id,
                serde_json::json!({ "question": question }),
            )
            .await?;
        let started_at = chrono::Utc::now();
        let _ = self
            .run_update(
                run.id,
                RunPatch {
                    status: Some(RunStatus::Running),
                    started_at: Some(started_at),
                    ..Default::default()
                },
            )
            .await;
        self.run_log_step(
            run.id,
            tenant,
            "run_start",
            serde_json::json!({ "question": question }),
        )
        .await?;
        // `run_create` returned the row as it was *before* the patch. Reflect what was actually
        // written, or the caller (and the `202 Accepted` body) reports a run that never started.
        run.status = RunStatus::Running;
        run.started_at = Some(started_at);
        Ok(run)
    }

    /// Drive an already-started run to completion, rebuilding its transcript from the journal.
    ///
    /// Same path the dispatcher uses for crash recovery, with `max_turns` under the caller's
    /// control. Safe to call on a run that has already made progress — the transcript replay is
    /// what makes the loop re-entrant.
    pub async fn run_agent_drive(&self, run_id: uuid::Uuid, max_turns: usize) -> Result<Run> {
        let run = self
            .run_get(run_id)
            .await?
            .ok_or_else(|| crate::Error::State("run not found".into()))?;
        let agent_id = run.agent_id.clone().unwrap_or_default();
        let tenant = run.tenant_id.clone();
        let transcript = self.rebuild_agent_transcript(run_id).await?;
        self.drive_agent_loop(run_id, &tenant, &agent_id, transcript, max_turns)
            .await
    }

    /// Resume a run paused at human approval: if the approval is `approved`, rebuild the transcript
    /// from the run's journaled trace and continue the agent loop (durable resume after HITL).
    pub async fn run_resume(&self, run_id: uuid::Uuid, tenant: &str) -> Result<Run> {
        let approved = self
            .run_approval_status(run_id)
            .await?
            .and_then(|v| v.get("state").and_then(|s| s.as_str()).map(String::from))
            .as_deref()
            == Some("approved");
        if !approved {
            return Err(crate::Error::State("run is not approved for resume".into()));
        }
        let run = self
            .run_get(run_id)
            .await?
            .ok_or_else(|| crate::Error::State("run not found".into()))?;
        let agent_id = run.agent_id.clone().unwrap_or_default();
        let transcript = self.rebuild_agent_transcript(run_id).await?;
        let _ = self
            .run_update(
                run_id,
                RunPatch {
                    status: Some(RunStatus::Running),
                    ..Default::default()
                },
            )
            .await;
        self.drive_agent_loop(run_id, tenant, &agent_id, transcript, 8)
            .await
    }

    /// Resume driving a non-terminal run from its journaled trace (crash / failover recovery).
    /// Unlike [`Self::run_resume`] it requires no approval — used by the [`Self::run_dispatch_once`]
    /// dispatcher. Claims the run first (bumps `updated_at`) so a concurrent tick won't re-pick it.
    pub async fn run_resume_driver(&self, run_id: uuid::Uuid) -> Result<Run> {
        let run = self
            .run_get(run_id)
            .await?
            .ok_or_else(|| crate::Error::State("run not found".into()))?;
        let agent_id = run.agent_id.clone().unwrap_or_default();
        let tenant = run.tenant_id.clone();
        let _ = self
            .run_update(
                run_id,
                RunPatch {
                    status: Some(RunStatus::Running),
                    ..Default::default()
                },
            )
            .await;
        let transcript = self.rebuild_agent_transcript(run_id).await?;
        self.drive_agent_loop(run_id, &tenant, &agent_id, transcript, 8)
            .await
    }

    /// One dispatcher tick: resume up to `limit` non-terminal runs untouched for `stale_secs`
    /// (orphaned by a crash / leader failover). Returns how many were resumed. No-op without a
    /// completion provider. **At-least-once**: a step interrupted mid-flight may re-run, so mutating
    /// tools should be idempotent. `waiting_approval` runs are excluded (they need a human).
    pub async fn run_dispatch_once(&self, stale_secs: i64, limit: usize) -> Result<usize> {
        if self.completion.is_none() {
            return Ok(0);
        }
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(stale_secs);
        let runs = self.runs.list_resumable(cutoff, limit).await?;
        let mut resumed = 0;
        for run in runs {
            match self.run_resume_driver(run.id).await {
                Ok(_) => resumed += 1,
                Err(e) => {
                    tracing::warn!(run_id = %run.id, error = %e, "dispatcher: resume failed")
                }
            }
        }
        if resumed > 0 {
            tracing::info!(resumed, "run dispatcher resumed orphaned runs");
        }
        Ok(resumed)
    }

    /// Rebuild an agent transcript from a run's journaled steps (for durable resume).
    async fn rebuild_agent_transcript(&self, run_id: uuid::Uuid) -> Result<String> {
        let mut t = String::new();
        for step in self.run_trace(run_id).await? {
            let et = step
                .get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let p = step.get("payload").cloned().unwrap_or_default();
            match et {
                "run_start" => t.push_str(&format!(
                    "Question: {}\n",
                    p.get("question").and_then(|v| v.as_str()).unwrap_or("")
                )),
                "tool_call" => {
                    // Reconstruct the EXACT line the live loop emitted for this step, dispatching on
                    // the journaled `tool`. Getting this right is what makes resume correct: the
                    // idempotency counter is `transcript.matches("TOOL call ").count()`, so external
                    // calls MUST re-render as `TOOL call …` (else the counter resets and keys shift),
                    // and the real observations must be replayed (else the LLM re-issues calls blindly).
                    let tool = p.get("tool").and_then(|v| v.as_str()).unwrap_or("");
                    match tool {
                        "search" => {
                            let q = p.get("query").and_then(|v| v.as_str()).unwrap_or("");
                            let results: Vec<String> = p
                                .get("results")
                                .and_then(|v| v.as_array())
                                .map(|a| {
                                    a.iter()
                                        .filter_map(|x| x.as_str().map(str::to_string))
                                        .collect()
                                })
                                .unwrap_or_default();
                            t.push_str(&format!(
                                "Assistant: TOOL search: {q}\nObservation: {}\n",
                                results.join(" | ")
                            ));
                        }
                        "remember" => {
                            let text = p.get("content").and_then(|v| v.as_str()).unwrap_or("");
                            t.push_str(&format!(
                                "Assistant: TOOL remember: {text}\nObservation: stored\n"
                            ));
                        }
                        // Downstream MCP tool, journaled as `tool = "<server>/<tool>"` + a `result`.
                        other => {
                            let (server, tool_name) = other.split_once('/').unwrap_or((other, ""));
                            let result = p.get("result").map(|v| v.to_string()).unwrap_or_default();
                            t.push_str(&format!(
                                "Assistant: TOOL call {server} {tool_name}\nObservation: {result}\n"
                            ));
                        }
                    }
                }
                "hitl_request" => t.push_str(&format!(
                    "Assistant: requested approval for: {}\nObservation: approved\n",
                    p.get("prompt").and_then(|v| v.as_str()).unwrap_or("")
                )),
                _ => {}
            }
        }
        Ok(t)
    }

    /// Drive an agent run, marking it `Failed` if the loop returns an error — so a poison run (e.g.
    /// an LLM/tool call that keeps erroring) is NOT resumed forever by the dispatcher. A genuine
    /// process crash never returns here: the run stays `Running` and is resumed after failover, as
    /// intended. Shared by [`Self::run_agent`] and resume.
    async fn drive_agent_loop(
        &self,
        run_id: uuid::Uuid,
        tenant: &str,
        agent_id: &str,
        transcript: String,
        max_turns: usize,
    ) -> Result<Run> {
        // Claim the driver lease so two concurrent drivers (a duplicate resume, or the dispatcher and
        // a manual resume) don't both execute this run. If another worker holds a valid lease, don't
        // drive — return the run's current state (not an error; don't mark it failed).
        if !self.run_try_claim(run_id).await? {
            tracing::debug!(%run_id, "run already leased by another worker — not driving");
            return self
                .run_get(run_id)
                .await?
                .ok_or_else(|| crate::Error::State("run vanished".into()));
        }
        let result = self
            .drive_agent_loop_inner(run_id, tenant, agent_id, transcript, max_turns)
            .await;
        self.run_release_lease(run_id).await;
        match result {
            Ok(run) => Ok(run),
            Err(e) => {
                let now = chrono::Utc::now();
                let _ = self
                    .run_update(
                        run_id,
                        RunPatch {
                            status: Some(RunStatus::Failed),
                            error: Some(e.to_string()),
                            ended_at: Some(now),
                            ..Default::default()
                        },
                    )
                    .await;
                Err(e)
            }
        }
    }

    /// Agent-run driver lease TTL. Renewed each turn; a run whose lease is older than this is
    /// considered orphaned and may be re-claimed by another worker.
    const LEASE_TTL_SECS: i64 = 300;

    /// Try to claim (or renew) this instance's driver lease on a run. `false` → another worker holds it.
    async fn run_try_claim(&self, run_id: uuid::Uuid) -> Result<bool> {
        let now = chrono::Utc::now();
        let expires = now + chrono::Duration::seconds(Self::LEASE_TTL_SECS);
        self.runs
            .try_claim_lease(run_id, &self.driver_id, now, expires)
            .await
    }

    /// Release this instance's driver lease on a run (best-effort; a no-op if it isn't ours).
    async fn run_release_lease(&self, run_id: uuid::Uuid) {
        let _ = self.runs.release_lease(run_id, &self.driver_id).await;
    }

    /// The agent loop over an **existing** run: LLM↔tool turns until a final answer, a pause for
    /// approval (`TOOL approve: <reason>` → `WaitingApproval`, resumable via [`Self::run_resume`]),
    /// or max turns. Journals every step.
    async fn drive_agent_loop_inner(
        &self,
        run_id: uuid::Uuid,
        tenant: &str,
        agent_id: &str,
        mut transcript: String,
        max_turns: usize,
    ) -> Result<Run> {
        let completion = self
            .completion
            .clone()
            .ok_or_else(|| crate::Error::Llm("run_agent requires a completion provider".into()))?;
        let scope = MemoryScope {
            tenant_id: if tenant.is_empty() {
                "default".into()
            } else {
                tenant.to_string()
            },
            agent_id: Some(agent_id.to_string()),
            ..Default::default()
        };
        let system = "You are an agent answering the user's question. Reply with EXACTLY ONE of: \
             `TOOL search: <query>` (search your memory), `TOOL remember: <fact>` (save a fact), \
             `TOOL call <server> <tool>: {json args}` (call an external tool), \
             `TOOL approve: <reason>` (request human approval), or the final answer.";

        // Stable count of already-issued tool calls (from the replayed transcript). A tool call
        // interrupted before its result was journaled does not appear in the transcript, so on
        // resume it gets the SAME idempotency key — idempotent downstream tools then run it once.
        let mut tool_seq = transcript.matches("TOOL call ").count();
        // Server-side idempotency ledger: results of external tool calls already executed in a prior
        // attempt (read from the journaled trace, keyed by the stable `_idempotency_key`). On resume,
        // re-issuing the same call reuses the recorded result instead of running the external side
        // effect again — effectively-once, without a per-turn consensus write.
        let mut executed: std::collections::HashMap<String, serde_json::Value> =
            std::collections::HashMap::new();
        for step in self.run_trace(run_id).await.unwrap_or_default() {
            if let Some(p) = step.get("payload") {
                if let (Some(k), Some(r)) = (
                    p.get("idempotency_key").and_then(|v| v.as_str()),
                    p.get("result"),
                ) {
                    executed.insert(k.to_string(), r.clone());
                }
            }
        }
        let mut final_answer = None;
        for _turn in 0..max_turns.max(1) {
            // Renew the lease each turn; if we've lost it (a stale lease re-claimed by another
            // worker), stop driving to avoid concurrent execution — return the current run state.
            if !self.run_try_claim(run_id).await? {
                tracing::warn!(%run_id, "lost the run lease mid-loop — another worker took over");
                return self
                    .run_get(run_id)
                    .await?
                    .ok_or_else(|| crate::Error::State("run vanished".into()));
            }
            let reply = completion.complete(system, &transcript).await?;
            let trimmed = reply.trim().to_string();
            if let Some(q) = trimmed.strip_prefix("TOOL search:") {
                let q = q.trim();
                let hits = self.memory_search(q, &scope, 5).await.unwrap_or_default();
                let results: Vec<String> = hits.iter().map(|h| h.memory.content.clone()).collect();
                self.run_log_step(
                    run_id,
                    tenant,
                    "tool_call",
                    serde_json::json!({ "tool": "search", "query": q, "results": results }),
                )
                .await?;
                transcript.push_str(&format!(
                    "Assistant: TOOL search: {q}\nObservation: {}\n",
                    results.join(" | ")
                ));
            } else if let Some(text) = trimmed.strip_prefix("TOOL remember:") {
                let text = text.trim();
                let _ = self
                    .memory_add(crate::memory::cognition::MemoryInput::new(
                        scope.clone(),
                        text,
                    ))
                    .await;
                self.run_log_step(
                    run_id,
                    tenant,
                    "tool_call",
                    serde_json::json!({ "tool": "remember", "content": text }),
                )
                .await?;
                transcript.push_str(&format!(
                    "Assistant: TOOL remember: {text}\nObservation: stored\n"
                ));
            } else if let Some(rest) = trimmed.strip_prefix("TOOL call ") {
                // Downstream MCP tool: `TOOL call <server> <tool>: {json args}`.
                let (head, args_str) = rest.split_once(':').unwrap_or((rest, "{}"));
                let mut parts = head.split_whitespace();
                let server = parts.next().unwrap_or("").to_string();
                let tool = parts.next().unwrap_or("").to_string();
                let mut args: serde_json::Value =
                    serde_json::from_str(args_str.trim()).unwrap_or_else(|_| serde_json::json!({}));
                // Deterministic idempotency key, stable across resume (`run_id:tool:<n>`).
                let idem = format!("{run_id}:tool:{tool_seq}");
                let result = if let Some(prev) = executed.get(&idem) {
                    // Server-side effectively-once: this call already ran in a prior attempt (its
                    // result is in the journaled trace) — reuse it instead of running the external
                    // side effect again.
                    tracing::info!(%run_id, idem, "tool already executed — reusing recorded result");
                    prev.clone()
                } else {
                    // Don't execute a side-effecting external tool if we're no longer the leader (a
                    // stale ex-leader mid-partition) — stop BEFORE the side effect. Cheap local
                    // metric check (no consensus round-trip).
                    let replicator = self.run_replicator.read().clone();
                    let is_leader = match replicator {
                        Some(r) => r.is_leader().await,
                        None => true,
                    };
                    if !is_leader {
                        tracing::warn!(%run_id, "no longer the leader — stopping before the external tool call");
                        return self
                            .run_get(run_id)
                            .await?
                            .ok_or_else(|| crate::Error::State("run vanished".into()));
                    }
                    if let Some(obj) = args.as_object_mut() {
                        obj.insert("_idempotency_key".into(), idem.clone().into());
                    }
                    let executor = self.tool_executor.read().clone();
                    let r = match executor {
                        Some(ex) => ex
                            .call_tool(&server, &tool, args)
                            .await
                            .unwrap_or_else(|e| serde_json::json!({ "error": e.to_string() })),
                        None => serde_json::json!({ "error": "no tool executor configured" }),
                    };
                    executed.insert(idem.clone(), r.clone());
                    r
                };
                self.run_log_step(
                    run_id,
                    tenant,
                    "tool_call",
                    serde_json::json!({ "tool": format!("{server}/{tool}"), "idempotency_key": idem, "result": result }),
                )
                .await?;
                tool_seq += 1;
                transcript.push_str(&format!(
                    "Assistant: TOOL call {server} {tool}\nObservation: {result}\n"
                ));
            } else if let Some(prompt) = trimmed.strip_prefix("TOOL approve:") {
                // Pause for human-in-the-loop approval; resume with run_resume after approval.
                self.run_request_approval(run_id, tenant, prompt.trim())
                    .await?;
                return self
                    .run_get(run_id)
                    .await?
                    .ok_or_else(|| crate::Error::State("run vanished".into()));
            } else {
                self.run_log_step(
                    run_id,
                    tenant,
                    "llm_answer",
                    serde_json::json!({ "answer": trimmed }),
                )
                .await?;
                final_answer = Some(trimmed);
                break;
            }
        }

        let now = chrono::Utc::now();
        let patch = match &final_answer {
            Some(ans) => RunPatch {
                status: Some(RunStatus::Succeeded),
                result: Some(serde_json::json!({ "answer": ans })),
                ended_at: Some(now),
                ..Default::default()
            },
            None => RunPatch {
                status: Some(RunStatus::Failed),
                error: Some("max turns exceeded without a final answer".into()),
                ended_at: Some(now),
                ..Default::default()
            },
        };
        self.run_update(run_id, patch).await?;
        self.run_get(run_id)
            .await?
            .ok_or_else(|| crate::Error::State("run vanished".into()))
    }

    /// Run a **workflow DAG**: create a parent run, then execute each node (a sub-agent) once its
    /// `deps` have completed, in topological order, linking children via `parent_run_id` and
    /// journaling a `subagent` step per node. Returns the parent run. Requires a completion provider.
    pub async fn run_workflow(&self, tenant: &str, nodes: Vec<WorkflowNode>) -> Result<Run> {
        let order = Self::topo_order(&nodes).ok_or_else(|| {
            crate::Error::Ingest("workflow has a dependency cycle or unknown dep".into())
        })?;
        let parent = self
            .run_create(
                tenant,
                Some("workflow".into()),
                None,
                serde_json::json!({ "nodes": nodes.len() }),
            )
            .await?;
        let _ = self
            .run_update(
                parent.id,
                RunPatch {
                    status: Some(RunStatus::Running),
                    started_at: Some(chrono::Utc::now()),
                    ..Default::default()
                },
            )
            .await;

        let mut children = Vec::new();
        let mut failed = false;
        for i in order {
            let node = &nodes[i];
            let child = self
                .run_agent_with_parent(tenant, &node.agent_id, &node.question, 4, Some(parent.id))
                .await?;
            self.run_log_step(
                parent.id,
                tenant,
                "subagent",
                serde_json::json!({ "node": node.id, "child_run": child.id, "status": child.status.as_str() }),
            )
            .await?;
            children.push(serde_json::json!({ "node": node.id, "run_id": child.id }));
            if child.status != RunStatus::Succeeded {
                failed = true;
                break;
            }
        }

        let now = chrono::Utc::now();
        let patch = if failed {
            RunPatch {
                status: Some(RunStatus::Failed),
                result: Some(serde_json::json!({ "children": children })),
                error: Some("a workflow node did not succeed".into()),
                ended_at: Some(now),
                ..Default::default()
            }
        } else {
            RunPatch {
                status: Some(RunStatus::Succeeded),
                result: Some(serde_json::json!({ "children": children })),
                ended_at: Some(now),
                ..Default::default()
            }
        };
        self.run_update(parent.id, patch).await?;
        Ok(self.run_get(parent.id).await?.unwrap_or(parent))
    }

    /// Kahn topological sort of workflow nodes by their `deps`. Returns node indices in execution
    /// order (deterministic), or `None` on a cycle or an unknown dependency id.
    fn topo_order(nodes: &[WorkflowNode]) -> Option<Vec<usize>> {
        use std::collections::HashMap;
        let index: HashMap<&str, usize> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.id.as_str(), i))
            .collect();
        let mut indeg = vec![0usize; nodes.len()];
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
        for (i, n) in nodes.iter().enumerate() {
            for d in &n.deps {
                let j = *index.get(d.as_str())?;
                adj[j].push(i);
                indeg[i] += 1;
            }
        }
        let mut queue: Vec<usize> = (0..nodes.len()).filter(|&i| indeg[i] == 0).collect();
        queue.sort_unstable();
        let mut order = Vec::new();
        let mut qi = 0;
        while qi < queue.len() {
            let i = queue[qi];
            qi += 1;
            order.push(i);
            let mut newly = Vec::new();
            for &k in &adj[i] {
                indeg[k] -= 1;
                if indeg[k] == 0 {
                    newly.push(k);
                }
            }
            newly.sort_unstable();
            queue.extend(newly);
        }
        (order.len() == nodes.len()).then_some(order)
    }

    /// Set state from the agent driver (e.g. a HITL approval key). In cluster mode this replicates
    /// through Raft (so it survives failover); otherwise it writes locally. Distinct from
    /// [`Self::state_set`], which stays local — the Raft apply path calls `state_set` directly, so
    /// routing that through the replicator would loop.
    pub async fn state_set_via_driver(
        &self,
        agent_id: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        let replicator = self.run_replicator.read().clone();
        match replicator {
            Some(r) => r.replicate_state_set(agent_id, key, value).await,
            None => self.state_set(agent_id, key, value).await.map(|_| ()),
        }
    }
}

#[cfg(test)]
#[path = "agentic_tests.rs"]
mod tests;
