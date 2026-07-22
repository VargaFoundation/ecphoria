# Architecture

Ecphoria is an **open-source agentic memory platform**: a single Rust binary that gives AI agents a
durable, HA memory *and* runs the agents on top of it. This document is the detailed map — the
pillars, where LLMs and embeddings actually fit, the crate/module layout, the memory-retrieval
pipeline, the agent runtime, the clustering layer, and the request flows.

> One line: **"the memory engine that also runs — and remembers — your agents."**

---

## 1. The three pillars

Ecphoria is not one thing; it's three layers stacked, in one process:

```
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │  PROTOCOLS  (ecphoria-gateway)                                                    │
 │  REST/MCP/LLM-proxy :8432   ·   PostgreSQL wire :5432   ·   gRPC :9432          │
 │  auth (API key / JWT / OIDC) · RBAC · rate-limit · audit · multi-tenant         │
 └───────────────┬────────────────────────────────────────────────────────────────┘
                 │  every request → EcphoriaEngine (tenant-scoped)
 ┌───────────────▼────────────────────────────────────────────────────────────────┐
 │  ENGINE  (ecphoria-core :: EcphoriaEngine)                                          │
 │                                                                                  │
 │  ┌──────────────────────────────┐        ┌──────────────────────────────────┐   │
 │  │  AGENT RUNTIME               │ uses → │  MEMORY SUBSTRATE                │   │
 │  │  (the "brain")               │        │  (the "storage + recall")       │   │
 │  │                              │        │                                  │   │
 │  │  · RunStore (durable runs)   │        │  · Episodic  (DuckDB, SQL)       │   │
 │  │  · run_agent driver (LLM↔    │        │  · Semantic  (USearch HNSW +     │   │
 │  │      tool loop)              │        │      embedding provider)         │   │
 │  │  · tool-gateway (downstream  │        │  · State     (SQLite + DashMap)  │   │
 │  │      MCP)                    │        │  · Cognition (bi-temporal        │   │
 │  │  · HITL approvals            │        │      memories + knowledge graph  │   │
 │  │  · DAG workflows + subagents │        │      + hybrid retrieval + rerank)│   │
 │  │  · RunDispatcher (auto-      │        │                                  │   │
 │  │      resume after failover)  │        │  LLM (opt-in): fact extraction   │   │
 │  │  · event triggers            │        │  Embedding: vectorize for recall │   │
 │  └──────────────────────────────┘        └──────────────────────────────────┘   │
 └───────────────┬────────────────────────────────────────────────────────────────┘
                 │  writes proposed through consensus (cluster mode)
 ┌───────────────▼────────────────────────────────────────────────────────────────┐
 │  CLUSTER / HA  (ecphoria-cluster)                                                 │
 │  Raft (openraft) · gRPC+MessagePack transport :9433 · leader-forward ·           │
 │  sharding (N Raft groups) · snapshots · k8s operator                            │
 └──────────────────────────────────────────────────────────────────────────────┘
```

The agent runtime **uses** the memory substrate: when `run_agent` drives an agent, its loop calls
`memory_search` (via the built-in `search` tool) to recall context. So memory-retrieval quality is
not a side quest — it directly determines how good the agents are.

There are **three** components, not two. The middle box **is** `ecphoria-core` — the engine that holds
all business logic; the gateway above only *exposes* it, and the cluster below only *replicates* its
writes. So `gateway → core → cluster`, with the core at the center (core knows nothing of either).

On the wire: the **client gRPC API (:9432) is protobuf** (`google.protobuf.Struct`), **REST (:8432)
is JSON**, and **only the inter-node Raft transport (:9433) uses MessagePack** (gRPC-enveloped). Don't
conflate the client gRPC with the Raft transport — they're different ports and different encodings.

---

## 2. Where LLMs and embeddings fit (this trips people up)

There are several models in play with **very different roles** — some are core product, one is
eval-only:

| Model role | What it does | Product or test? |
|------------|--------------|------------------|
| **Embedding provider** (`nomic-embed-text`, OpenAI `text-embedding-3`) | vectorize text so semantic recall works (`memory_search`, ingest) | **Product — permanent.** Semantic memory can't exist without it. |
| **Agent-loop LLM** (any completion provider) | the model the agent itself reasons with in `run_agent` | **Product — the runtime.** |
| **Extraction LLM** (`extraction=llm`) | at ingest, distill raw text into **atomic facts** before storing | **Product — optional** (a memory-quality lever). |
| **Reranker** (LLM judge *or* local ONNX cross-encoder) | re-score the top candidates from hybrid search | **Product — optional** (read-path). |
| **Bench answerer + judge** (`ops/bench`, via the Claude CLI) | simulate an agent asking questions + grade answers | **Eval-only.** Never in the product path. |

Completion providers are pluggable (`crates/ecphoria-core/src/llm/`): **Ollama**, **OpenAI**,
**Anthropic** (HTTP API), and **Claude via the logged-in CLI** (`claude -p`, no API key). Embedding
providers: **Ollama**, **OpenAI**.

---

## 3. Crate structure

Cargo workspace; dependencies flow **downward** (`core ← cluster ← gateway ← server`). `ecphoria-core`
knows nothing of protocols or Raft.

```
ecphoria-server (bin)   ── wiring: config → engine → coordinator → gateway → RunDispatcher → signals
  ├── ecphoria-gateway  → ecphoria-core, ecphoria-cluster
  ├── ecphoria-cluster  → ecphoria-core
  └── ecphoria-core
ecphoria-cli (bin)      → ecphoria-core (shared types; talks to the server over HTTP)
```

### `ecphoria-core` — the engine (business logic, zero protocol/cluster knowledge)
| Module | Purpose |
|--------|---------|
| `memory::episodic` | DuckDB event store — SQL, connection pool, batch txns, `session_id`/`tenant_id`, TIMESTAMPTZ/JSON |
| `memory::semantic` | USearch HNSW vector index + `EntryMetadata` (no vector duplication), save/load |
| `memory::state` | SQLite + DashMap KV, CAS, TTL, watchers |
| `memory::cognition` | **bi-temporal `memories`** (valid_from/valid_to, supersession), **knowledge graph edges**, **hybrid retrieval** (BM25 + vector via RRF), `tokenize` (stop-words + light stemming) |
| `memory::migrations` | versioned schema migration framework |
| `embedding` | `EmbeddingProvider` trait + Ollama/OpenAI |
| `llm` | `CompletionProvider` trait + Ollama / OpenAI / Anthropic / **Claude-CLI** |
| `rerank` | `Reranker` trait + `LlmReranker` + `CrossEncoderReranker` (feature `rerank-local`, ONNX bge) |
| `runtime` | **agentic substrate**: `RunStore` (durable runs), `ToolExecutor` + `RunReplicator` traits |
| `ingest::pipeline` | validate → episodic → auto-embed (batched) → semantic index |
| `storage` (+ `tiering`) | `StorageBackend` (local FS / S3-MinIO) + hot/warm/cold tiering |
| `engine` | `EcphoriaEngine` — wires everything; `memory_search`, `run_agent`, `run_workflow`, `run_dispatch_once`, … |

### `ecphoria-gateway` — protocols
`rest` (axum), `pg_wire` (pgwire, tenant-auth: password = API key/JWT), `grpc` (tonic, shard-aware),
`mcp` (Streamable HTTP), `llm_proxy` (OpenAI-compatible + auto-RAG), `auth` (API key / JWT HS256 /
OIDC RS256, RBAC, rate-limit, audit), `cluster` (`leader_forward`, `shard_route`, `raft_routes`).

### `ecphoria-cluster` — distribution
`raft::{types,store,network,server,tls}` (openraft 0.9; **gRPC + MessagePack** transport),
`coordinator` (`ClusterCoordinator`, `client_write`, `CoordinatorRunReplicator`), `shard`
(`ShardRouter`, `reconcile_plan`, `scale_plan`, `ShardedCluster`), `replication::snapshot`.

### `ecphoria-server` / `ecphoria-cli`
Thin binary wiring / HTTP admin CLI. The **k8s operator** lives standalone in `ops/operator/`
(outside the workspace).

---

## 4. Memory substrate (the "recall" half)

Four stores, one engine:

| Store | Backend | Holds | Key ops |
|-------|---------|-------|---------|
| **Episodic** | DuckDB | events (what happened) — `source`, `event_type`, `payload`, `ts`, `session_id`, `tenant_id` | SQL query, batch ingest |
| **Semantic** | USearch HNSW | vectors for similarity search | k-NN by cosine |
| **State** | SQLite + DashMap | live key-value per agent | get/set, CAS, watch, TTL |
| **Cognition** | DuckDB (`memories`) + USearch | **bi-temporal facts** + **knowledge-graph edges** | `memory_add/search/history/as_of`, `memory_link` |

**Cognition** is the differentiator (Mem0/Zep-class): deterministic contradiction resolution (a newer
fact about the same `subject` supersedes the old one, kept for history), dedup, importance + decay,
`as_of` time-travel, and a bi-temporal knowledge graph.

### 4.1 The retrieval pipeline (`memory_search`)

This is the read path an agent hits on every recall. Hybrid, read-only (no Raft/determinism impact):

```
query ──► tokenize (lowercase · drop stop-words · light stemming: run(ning)→run, agenc(ies)→agency)
       │
       ├─ (A) LEXICAL  BM25 over the candidate universe          [list_active(scope, retrieval_scan_cap=2048)]
       ├─ (B) VECTOR   embed(query) → HNSW k-NN                    [fetch ~retrieval_pool candidates]
       └─ (C) GRAPH    query entities → edges → linked memories    [optional: cognition.graph_expansion]
                          │
                          ▼
        RRF FUSION  score = Σ 1/(60 + rank_i)  over {A,B,C}       [keep top retrieval_pool=50]
                          │
                          ▼
        BLEND  score ·= (1 + 0.3·importance + 0.2·recency)        [recency = 0.5^(age_days/30)]
                          │
                          ▼
        RERANK (optional)  LlmReranker  OR  CrossEncoderReranker  [re-score the pool; ms with ONNX]
                          │
                          ▼
        top-k  ──►  MemoryHit[]  (returned to the agent / caller)
```

Widths are **configurable** (`cognition.retrieval_scan_cap`, `retrieval_pool`) — read-path knobs for
tuning/A-B. *Measured note:* widening the pool alone is neutral on recall@5; the levers that move it
are `extraction=llm` (atomic facts) and reranking. See `docs/benchmarks-locomo.md` and `ops/bench/`.

### 4.2 Ingest

```
events (REST/webhook/gRPC/MCP)
   → validate (SELECT-only SQL guard where relevant)
   → append to Episodic (batch txn, tagged _session_id/_tenant_id)
   → if embedding provider: chunk text → batch embed → upsert Semantic   (failures non-fatal)
```

`memory_add` (cognition) additionally runs dedup / supersession / optional auto-graph edge extraction
— on the leader it materializes the rows, then replicates them (see §6).

---

## 5. Agent runtime (the "brain" half)

Built on the memory substrate; this is the P2 platform.

| Component | What it is | Where |
|-----------|-----------|-------|
| **RunStore** | durable ledger of runs — status (pending→running→waiting_approval→succeeded/failed/cancelled), input/result/cursor, `parent_run_id` (subagent tree). SQLite. | `runtime::store` |
| **Steps** | every LLM/tool/HITL step = an **episodic event** tagged `session_id = run_id` → the trace is `GET /runs/{id}/trace`, and analytics are plain SQL | `engine::run_log_step` |
| **Agent driver** | `run_agent` / `drive_agent_loop`: LLM↔tool loop with built-in tools `search`, `remember`, downstream `TOOL call <srv> <tool>`, and `TOOL approve` (HITL pause). Re-entrant: resumes from the journaled trace. | `engine.rs` |
| **Tool-gateway** | register/list/call **downstream MCP servers**; injected into the loop via `ToolExecutor` so agents call external tools (governed by auth/RBAC/audit) | `rest::tool_gateway` + `runtime::tools` |
| **HITL** | `run_request_approval`/`run_resolve_approval`; `WaitingApproval` + a state key; `run_resume` continues after approval | `engine.rs` |
| **Workflows** | `run_workflow`: DAG of sub-agents (Kahn topo-sort, `parent_run_id`) | `engine.rs` |
| **RunDispatcher** | leader-gated background loop that **auto-resumes runs orphaned by a crash/failover** (`run_dispatch_once`) | `ecphoria-server/main.rs` |
| **Triggers** | `trigger_register` + `fire_triggers`; the webhook handler fires matching triggers → starts runs | `engine.rs` + `rest` |
| **Idempotency** | tool calls carry `_idempotency_key = run_id:tool:<n>` (stable across resume) | `drive_agent_loop` |

Metrics: `ecphoria_runs_created_total`, `ecphoria_runs_completed_total{status}`, `ecphoria_run_steps_total{type}`.

---

## 6. Cluster / HA (`ecphoria-cluster`)

Multi-node via Raft (openraft 0.9). **Every mutation is proposed as an `AppRequest` through the log**
and applied deterministically on every node, so committed writes survive leader failover.

```
        client (write)
           │  (follower → 307 leader-forward)
           ▼
   ┌─────────────┐  Raft: AppendEntries / Vote / InstallSnapshot
   │  LEADER      │◄──────── gRPC (tonic, HTTP/2) + MessagePack ────────►┐
   │ client_write │                                                       │
   └──────┬───────┘                                                       │
          │ commit → apply on ALL nodes (deterministic)                   │
   ┌──────▼───────┐            ┌──────────────┐            ┌──────────────┐
   │  apply →      │            │  Follower 1  │            │  Follower 2  │
   │  EcphoriaEngine │            │  apply →eng. │            │  apply →eng. │
   └──────────────┘            └──────────────┘            └──────────────┘
```

**`AppRequest` variants** (all carry *materialized* values → deterministic apply): `Ingest`,
`StateSet`/`StateDelete`, `SemanticUpsert`/`Delete`, `MemoryUpsert`/`MemoryExpire`, `GraphAddEdge`/
`GraphSupersede`, **`RunCreate`/`RunUpdate`**. The agent driver's run/step/state writes replicate via
the injected **`RunReplicator`** (`CoordinatorRunReplicator` → `client_write`), so a run started via
`/agents/run` — and its full trace — survive failover; the **RunDispatcher** then resumes it.

**Determinism invariant (the core design constraint):** anything non-deterministic (uuid, `now()`,
LLM calls, embeddings) must run **once on the leader** and be baked into the `AppRequest`; `apply`
must be a pure function of the request. This is why memory cognition uses a compute-then-replicate
(`memory_plan` → `client_write` → `apply_rows`) split.

**Serialization gotcha (learned the hard way):** the transport is positional MessagePack, so structs
reachable from `AppRequest` must **not** use `#[serde(skip_serializing_if)]` (it shifts the array and
misaligns the decoder). Regression-tested in `raft::types`.

**Sharding:** `cluster.shards = N` runs N independent Raft groups; `ShardRouter` consistent-hashes a
tenant → shard; the gateway routes each request to the owning shard (HTTP reverse-proxy, gRPC/PG
reject-with-owner-hint). `scale_plan` computes the safe up/down sequence (create-then-move /
drain-then-delete) which the **k8s operator** (`ops/operator/`) applies.

**Snapshots** pack all four stores + the runs table as the backstop.

---

## 7. Protocols & auth (`ecphoria-gateway`)

| Protocol | Port | Notes |
|----------|------|-------|
| REST + MCP + LLM-proxy + `/metrics` | 8432 | axum; auth on `/api/v1/*`; MCP Streamable HTTP; `/v1/chat/completions` auto-RAG |
| PostgreSQL wire | 5432 | pgwire; **password = API key / JWT** → tenant-scoped queries; shard-aware |
| gRPC | 9432 | tonic; typed `protobuf.Struct`; tenant-scoped; shard-aware |
| Raft (inter-node) | 9433 | gRPC + MessagePack; shared-secret + optional mTLS |

Auth: API key (no tenant), JWT HS256 / OIDC RS256 (carry `tenant_id`), RBAC (admin/writer/reader/agent),
per-key rate-limit, durable audit log, row-level tenant isolation on **every** read path.

Middleware order in cluster mode: `auth → shard-route → leader-forward`.

---

## 8. Request flows

**Agent run** (`POST /api/v1/agents/run`): auth → (shard-route) → leader-forward → `run_agent` on the
leader → `run_create` (replicated) → loop { LLM → parse → `search`/`remember`/`TOOL call`/`approve` →
`run_log_step` (replicated) } → `run_update(succeeded)` (replicated). Crash mid-loop → the new
leader's RunDispatcher resumes from the trace.

**Memory search** (agent tool or `POST /memories/search`): §4.1 — read served locally on any node.

**HA write** (ingest/state/memory): follower → 307 → leader → `client_write(AppRequest)` → commit →
apply on all nodes.

---

## 9. Key technology choices

| Concern | Tech | Why |
|---------|------|-----|
| Language | Rust | single binary, no runtime, safety |
| Analytics SQL | DuckDB | columnar, embedded, native JSON/TIMESTAMPTZ |
| Vector index | USearch (HNSW) | compact, persistent, Rust |
| State KV | SQLite | ACID, embedded |
| Consensus | openraft 0.9 | Raft in Rust |
| Raft transport | gRPC + MessagePack | ~1.8× smaller than JSON on embedding-heavy batches |
| Reranker (prod) | ONNX cross-encoder (fastembed) | ms/query vs ~140 s for an LLM reranker |
| Protocols | axum · pgwire · tonic · MCP | psql/BI tools + gRPC + native agent clients |
| Object storage | S3 / MinIO | tiering, cost |

---

## 10. Additional subsystems (recent)

- **SQL over memories** — the cognition tables (`memories`/`memory_edges`/`memory_grants`/
  `memory_attachments`) are reachable from `SELECT` via `query::sql_guard` (read-only, per-tenant
  view rewrite), so bi-temporal `SELECT … FROM memories` works over PG-wire/REST/gRPC/MCP.
- **Multimodal attachments** — binary blobs (image/PDF/audio) in the configured storage backend +
  metadata in cognition; optional image embedding (`ImageEmbeddingProvider`, `embed-image`) for
  image-similarity search.
- **Graph analytics** (`memory::graph_analytics`) — degree + PageRank centrality, community
  detection, shortest path, all with temporal (as-of) snapshots.
- **Embedded mode** (`embedded::Ecphoria`) — the engine in-process, no server ("the SQLite of agent
  memory"); a **Python binding** (`bindings/python`, pyo3) wraps it.
- **Public publish** (`/public`, opt-in) — read-only view of `metadata.published=true` memories.
- **Memory templates**; **Obsidian round-trip** (import/export + live `--watch` sync).

## Related docs
- [Agentic platform](agentic-platform.md) — the run/agent/HITL/workflow/trigger/tool API.
- [Benchmarks](benchmarks-locomo.md) + [`ops/bench/`](../ops/bench/) — memory-quality evaluation.
- [Deployment](deployment.md) · [Security](security.md) · [Operator](operator.md) · [Migrate from Mem0](migrate-from-mem0.md).
