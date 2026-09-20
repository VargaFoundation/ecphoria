# Changelog

All notable changes to Ecphoria will be documented in this file.

## [0.2.1] - 2026-09-20

### Bug Fixes

- *(docker)* The image has never been buildable since `tests/integration` joined
- *(release)* Cosign could not parse the image reference

## [0.2.0] - 2026-09-20

### Bug Fixes

- *(cluster)* Make Raft apply deterministic (carry materialized values)
- *(security)* Close 3 tenant-isolation holes found in review
- *(proxy)* Buffer raw bytes in the Anthropic SSE relay (UTF-8 split-char safety)
- *(test)* Isolate engine_router integration tests (in-memory episodic)
- *(core)* Clamp unbounded list limits to query.max_rows (OOM safety)
- *(bench)* Measure real inserts in ingest bench (fresh ids via iter_batched)
- *(test)* Clippy len_zero in webhook trigger integration test
- *(cluster)* Env-var config loading + run replication over the real Raft transport
- *(grpc)* Enforce RBAC role on mutating RPCs (was bypassed)
- *(security)* Close 8 critical multi-tenant, consensus & runtime holes
- *(security)* Enforce tenant ownership on HITL approve/resume/request-approval
- *(runtime)* Mark a run Failed on loop error instead of leaving it Running
- *(runtime)* Reject resolving a non-pending HITL approval
- *(helm)* Pod/container securityContext, readiness->/ready, per-shard PDB
- *(llm)* Claude-cli provider honours the system prompt (was overridden by the agent persona)
- *(operator)* Verify the drain before deleting a shard
- *(memory)* Reload the event vector index on startup (file-backed mode)
- *(auth)* Authenticate the shard-forward rate-limit marker
- *(cluster)* Propagate apply errors instead of swallowing them
- *(cluster)* Don't fast-forward last_applied on an empty/failed snapshot
- *(sdk-python)* Escape SQL string literals + validate order/limit in events()
- *(cluster)* Move embedding out of the Raft apply path (deterministic ingest)
- *(shard)* Forward full path on cross-shard proxy + live sharded CI harness
- *(security+correctness)* Wave 0 — XSS, /public, semantic-index persistence
- *(helm)* Wire llmProxyEnabled + surface missing gateway config keys
- *(cli)* Skip vendored trees, and retract documents that left the source
- *(sdk-python)* The package could not build — missing README, undeclared packages
- *(clippy)* `result_large_err` on tonic- and axum-shaped signatures
- *(security)* Record denials, bound the PG handshake, fuzz the parsers (E-13)
- *(cluster)* A write that lands on a follower now reaches the leader
- *(release)* `latest` was never published, and the chart defaulted to it

### Documentation

- Make crate CLAUDE.md status accurate (cognition, tenant isolation, HA reality)
- Reflect Streamable-HTTP MCP + proxy SSE streaming; correct cluster test notes
- Add security & hardening guide; document new endpoints/knobs
- Add OpenAPI 3.0 spec for the REST API
- *(bench)* Reproducible real-LoCoMo baseline + harness graph toggles & mode fix
- Agentic platform guide + Mem0 migration guide (adoption)
- Rewrite architecture.md (current, detailed) + realign CLAUDE.md to the agentic platform
- *(bench)* Record blend-neutral and extraction-vs-metric findings
- *(bench)* LLM-judge QA results — extraction is net-negative on this pipeline
- *(bench)* Bge-m3 tested worse than nomic on LoCoMo — a stronger model isn't a free win
- *(bench)* Cross-encoder reranker measured +4.2pts recall@5 (the biggest lever after the index fix)
- *(bench)* RRF vector-arm weighting measured +1.2pts recall@5
- *(wave0)* Encryption-at-rest recipe, make bench/cluster targets, LoCoMo turnkey pointer
- Wave 2 — positioning, honesty & doc integrity
- *(deployment)* Complete the config reference
- *(examples)* End-to-end product tour (one-command value demo)
- *(examples)* Richer product tour with real in-process embeddings
- Knowledge-base guide, daily-use kit, Hermes provider, API reference
- Record the triage outcome — 19 open PRs down to 10
- How Choregos uses Ecphoria (E-14)
- *(triage)* The second wave, and why all nine were red for two reasons

### Features

- Memory-intelligence layer + multi-tenant isolation + Claude integration
- *(security)* Complete tenant isolation (state/schema/webhook/sessions) + fix test isolation
- *(security)* Tenant-scope and authenticate gRPC (closes last isolation hole)
- *(cluster)* Include memories + state in backup/restore and Raft snapshots
- *(cluster)* Replicate memory writes through Raft (AppRequest memory variants)
- *(cluster)* Route ingest writes through the Raft log (real log-based replication)
- *(cluster)* Route state writes through Raft + harden cluster write path
- *(mcp)* Streamable HTTP transport (GET/SSE + Mcp-Session-Id) for native Claude Desktop
- *(proxy)* SSE streaming for /v1/chat/completions (incl. Anthropic→OpenAI translation)
- *(cluster)* Replicate memory writes through Raft (compute/apply split, no failover divergence)
- *(eval)* Richer LoCoMo harness — recall@{1,3,5} + MRR + ingest/query percentiles
- *(cluster)* Automatic multi-node cluster formation (deployable HA)
- *(cluster)* GRPC (tonic, HTTP/2 + MessagePack) Raft transport + binary log persistence
- *(sdk)* Expose the memory cognition + sessions API in Python & TypeScript clients
- *(proxy)* Multi-turn tool-use + streaming tool-call deltas (agentic loops via the proxy)
- *(grpc)* Typed payloads via protobuf Struct/Value (no more JSON-in-string)
- *(mcp)* Replicate MCP write tools through Raft in cluster mode
- *(security)* Fail-closed auth, JWT min-length, _FILE for LLM keys
- *(security)* Verify webhook HMAC signatures (GitHub-style X-Hub-Signature-256)
- *(security)* Authenticate inter-node Raft RPCs with a shared secret
- *(gdpr)* Cascade tenant deletion across all stores (right to be forgotten)
- *(retrieval)* Blend memory relevance with importance + recency
- *(grpc)* Memory cognition + sessions RPCs (protocol parity with REST/MCP)
- *(sdk)* Go client parity — memory cognition + sessions
- *(core)* Count-based forgetting + per-scope memory quota
- *(core)* Memory typing — episodic / semantic / procedural
- *(core)* Versioned schema-migration framework
- *(memory)* Summarization / consolidation (first increment)
- *(memory)* Graph layer — entity/relation edges + deterministic extraction (first increment)
- *(core)* Cross-store ingest atomicity — embedded marker + reindex/repair
- *(cluster)* TLS + mutual TLS for the inter-node Raft transport
- *(memory)* Multi-modal embeddings — store + search any-modality vectors (first increment)
- *(cluster)* Consistent-hash shard router — write-scaling foundation (first increment)
- *(memory)* Graph — multi-hop traversal + LLM triple extraction
- *(memory)* Per-modality vector indexes (mixed-dimension multi-modal)
- *(cluster)* Multi-group sharding — route writes to per-shard Raft groups
- *(cluster)* Log-replicate graph edges through Raft (was snapshot-only)
- *(cluster)* Cross-shard reads — scatter-gather over sharded engines
- *(cluster)* Log-replicate memory consolidation (was snapshot-only)
- *(deploy)* Multi-shard Helm deployment (N independent Raft groups)
- *(cluster)* Shard rebalancing — reshard planner + tenant data movement
- *(gateway)* Runtime shard routing — route requests to the owning shard by tenant
- *(gateway)* Shard-route MCP/LLM-proxy + skip double rate-limit on forwarded requests
- *(gateway)* Admin served locally + cross-shard audit scatter-gather
- *(cluster)* Operator reconcile logic + fix tenant-migration data-loss bug
- *(ops)* Cert-rotation via Reloader + Go SDK CI build + sharding/gRPC docs
- *(cluster)* Cross-pod tenant rebalance execution (full move: events+memories+state)
- *(core)* Add reranker module + config to complete memory_search reranking
- *(core)* Per-category metrics + QA-accuracy mode in locomo_eval
- *(core)* Query-time knowledge-graph expansion in memory_search
- *(core)* Locomo_convert — real LoCoMo/LongMemEval -> harness converter
- *(core)* Bi-temporal knowledge-graph edges (valid_from/valid_to + supersession + as-of)
- Deterministic auto-graph population + functional-relation supersession (replicated)
- *(eval)* LLM fact-extraction toggles + LLM-judge metric in the harness
- *(core)* Broaden deterministic triple extraction (richer auto-graph)
- *(core)* Durable agent-run ledger (P2.0 agentic-platform substrate)
- *(cluster)* Replicate the agent-run ledger through Raft (HA runs)
- *(gateway)* REST endpoints for the agent-run ledger
- *(core)* Durable agent-loop driver (run_agent) + fix episodic session_id
- *(gateway)* POST /api/v1/agents/run — run the durable agent loop via REST
- *(gateway)* MCP tool-gateway — register + call downstream MCP servers (P2.1)
- *(core)* Prometheus metrics for agent runs/steps (P2.6 observability)
- *(core)* Event triggers — fire agent runs on matching events (P2.5)
- Human-in-the-loop approvals for runs (P2.3)
- *(core)* Workflow DAG + sub-agents (P2.4)
- *(core)* Recognize 'cross_encoder' rerank provider + document the production path
- *(gateway)* Webhook auto-fires event triggers + REST trigger management (P2.5 follow-up)
- Durable HITL pause/resume in the agent driver (P2.3 follow-up)
- *(sdk)* Cover the agentic platform API in the Python + TypeScript SDKs (v0.3.0)
- *(ui)* Single-file web Explorer (SQL, memory search, runs+traces, run agent)
- *(deploy)* Render blueprint (one-click) + README agentic/docs/migration links
- *(sdk)* Framework-agnostic agent tools (OpenAI Agents SDK / Pydantic AI / LangChain / CrewAI)
- Agent loop can call downstream MCP tools (connect tool-gateway to the driver)
- *(ops)* Local N-node cluster tooling (configurable ports, failover proof)
- *(cluster)* Replicate the agent-run driver through Raft (survives leader failover)
- RunDispatcher — auto-resume orphaned agent runs after failover (durable execution)
- *(cluster)* Replicate driver state writes (HITL approvals + triggers survive failover)
- *(rerank)* Real local cross-encoder reranker (bge-reranker via fastembed/ort)
- *(agent)* Idempotency keys on tool calls — effectively-once across resume
- *(cluster)* Ordered scale-up/down planning for the operator (drain-before-delete)
- *(gateway)* Shard-aware gRPC routing — reject non-owned tenants with owner hint
- *(pg-wire)* Tenant auth (password = token) + shard routing
- *(operator)* Implement the k8s live apply loop (scale up/down + rebalance)
- *(operator)* --crd emitter + verified live apply on a real k8s cluster
- *(llm)* Anthropic (Claude) completion provider — extraction, rerank, eval
- *(llm)* Claude Code CLI completion provider (no API key — uses the logged-in CLI)
- *(bench)* Turnkey LoCoMo runner using the Claude CLI (no API key)
- *(retrieval)* Configurable candidate widths + tokenizer hygiene (stopwords + stemming)
- *(auth)* Tenant- and role-scopable API keys
- *(cli)* Admin commands + POST /admin/restore endpoint
- *(security)* Per-tenant rate-limit, tenant/IP audit, Raft secret _FILE
- *(memory)* Fix retrieval recall — asymmetric embedding prefixes + per-scope vector index
- *(memory)* Make the retrieval importance/recency blend weights configurable
- *(memory)* Surface embedding failures instead of silently degrading to BM25
- *(helm)* Add Ingress and HorizontalPodAutoscaler templates
- *(gateway)* Built-in admin console served at /ui
- *(observability)* Describe all Prometheus metrics (HELP/TYPE)
- *(operator)* Kubernetes deploy manifests (Dockerfile, CRD, RBAC, Deployment)
- *(grafana)* Dashboard panels for agent runtime + reliability + Raft
- *(helm)* PrometheusRule with alerts (no-leader, embed failures, lag, latency, run failures)
- *(memory)* Weighted-RRF arm weights for hybrid retrieval
- *(operator)* Leader election, admin token from a Secret, SQL tenant discovery
- *(operator)* Release the lease on SIGTERM + emit Kubernetes Events
- *(auth)* Opt-in require_tenant — reject bare (tenant-less) credentials
- *(runtime)* Driver lease to prevent concurrent double-execution of a run
- *(helm)* Opt-in NetworkPolicy locking PG/gRPC/Raft to intra-fleet traffic
- *(runtime)* Server-side idempotency ledger + pre-tool leadership check
- *(security+platform)* P0/P1 hardening + memory-platform features
- *(memory-platform)* Feedback loop, memory CDC stream, native Anthropic /v1/messages
- *(cognition+hardening)* Fuzz tests, subject normalization, HITL contradiction review
- *(cli)* Strata doctor — static config linter
- *(consolidation+docs)* Session distillation, background decay scheduler, docs refresh
- Config env fix, Obsidian import, semantic-cluster consolidation
- *(embedding)* In-process ONNX embeddings via fastembed (feature `embed-local`)
- *(admin-ui)* Extend embedded console with graph, contradictions, provenance, feedback
- *(sharing)* Cross-scope memory grants (tenant-strict)
- *(wave1)* Cluster HA in CI, replicate MCP remember, PG-wire TLS ergonomics
- *(authz)* Pluggable AuthzBackend seam for cross-scope reads
- *(cdc)* Outbound CDC sink to mirror memory changes downstream
- *(memory)* Versioned re-embedding job (admin-triggered)
- *(cli)* Mem0 and Zep importers for `strata import`
- *(otlp)* Optional OTLP trace export (feature `otlp`)
- *(shard)* Cluster-wide admin writes (scatter-gather) + sharding ops docs
- *(export)* Obsidian markdown round-trip (export memories → vault)
- *(embedded)* In-process library facade ("SQLite of agent memory")
- *(attachments)* Multimodal attachment storage (images/PDF/audio)
- *(admin-ui)* Memory curation — bi-temporal timeline, browse, delete, files
- *(graph)* Temporal knowledge-graph analytics (centrality, communities, path)
- *(publish+templates)* Public read-only view + memory templates
- *(python)* Embedded engine Python binding (pyo3)
- *(multimodal)* Image-embedding hook + offline image search
- *(admin-ui)* Interactive knowledge-graph visualization
- *(sync)* Live Obsidian import (`import --watch`)
- *(sql)* Make "SQL over memories" real — SELECT ... FROM memories works
- *(mcp)* Parity — expose the differentiator tools (17 → 23)
- *(cli)* Parity — memory CRUD + graph query verbs
- *(sdk-python)* Parity — cognition, graph, attachments, templates, grants
- *(sdk-typescript)* Parity — cognition, graph, attachments, templates
- *(sdk-go)* Parity — cognition, graph, attachments, templates
- *(backup)* Retention — prune old backups (bounded disk use)
- *(cdc)* Mirror state changes to the outbound sink (not just memory)
- *(metrics)* Domain counters/histograms for publish, attachments, templates
- *(grpc)* Graph analytics + memory provenance RPCs (Wave 3 parity)
- *(core)* Memory UPDATE (partial patch) + filtered/paged listing
- *(rest)* PATCH /memories/{id} + filters/pagination on memory list
- *(mcp+cli)* Update_memory + list filters/pagination parity
- *(sdk)* Update_memory + list filters/pagination across Py/TS/Go
- *(grpc)* UpdateMemory RPC + GetMemories filters/offset (parity)
- *(memory-scopes)* List distinct memory scopes across all surfaces
- *(core)* Engineering knowledge base — scalable retrieval, chunking, projects
- *(core)* Report per-arm relevance on hits, and gate retrieval quality in CI
- *(core)* Record searches, so retrieval can be measured against real questions
- *(gateway)* Governed writes, external identity, context packs
- *(embedding)* OpenAI-compatible endpoints, not just api.openai.com
- *(memory)* Typed facts and per-tenant governance (E-04, E-10)
- *(build)* Split the binary in two — `ecphoria:memory` and `ecphoria:full` (E-02)
- *(ops)* Scheduled backups that cannot go green having uploaded nothing (E-11)
- *(chart)* The retrieval knobs and per-tenant governance are settable from Helm

### Merge

- Automatic multi-node cluster formation (deployable HA)
- GRPC Raft transport (HTTP/2 + MessagePack) + binary log persistence
- 4 API improvements (SDK memory/sessions, proxy multi-turn tool-use, gRPC typed payloads, MCP→Raft)
- Production hardening (security, GDPR, OOM-safety, retrieval, ops)
- Roadmap batch — gRPC parity, OpenAPI, Go SDK, forgetting, memory typing, migrations
- Large roadmap — consolidation, graph, atomicity, TLS, multimodal, sharding
- Roadmap next increments — multi-hop graph+LLM, per-modality, live mTLS, multi-group sharding, graph log-replication
- Cross-shard reads, consolidation log-replication, multi-shard Helm
- Shard rebalancing — reshard planner + tenant data movement
- Fix broken query bench + cognition/graph benchmarks + reference numbers
- Ingest bench measures real inserts (fresh ids)
- Runtime shard routing in the gateway (route by tenant to owning shard)
- Shard-route MCP/LLM + skip double rate-limit
- Admin local + cross-shard audit scatter-gather
- DuckDB Appender ingest fast-path (~72x) + ts parse fix
- Operator reconcile logic + migrate data-loss fix
- Cert-rotation hooks + Go SDK CI + sharding/gRPC docs
- Cross-pod tenant rebalance execution API
- Reranking + eval harness + graph expansion (fix non-compiling main)
- Bi-temporal knowledge-graph edges (valid_from/valid_to + as-of + supersession primitive)
- Auto-graph population + functional-relation supersession + real-LoCoMo baseline
- GraphSupersede 3-node convergence test + extraction/judge harness knobs
- Broaden deterministic triple extraction for auto-graph
- Durable agent-run ledger (P2.0 agentic-platform substrate)
- REST endpoints for the agent-run ledger (completes P2.0)
- Agentic platform P2.1–P2.6 (agent driver, tool-gateway, HITL, DAG, triggers, metrics)
- Finish P2 follow-ups (webhook triggers, durable HITL) + adoption (SDKs, docs, UI, deploy, framework tools)
- Agent loop calls downstream MCP tools (tool-gateway ↔ driver via ToolExecutor)
- Live-cluster fixes (env-var config + run replication over gRPC) + local cluster tooling
- Agent-run driver replicates through Raft (survives failover) + Event serialization fix
- RunDispatcher — durable agent execution (auto-resume after failover)
- Driver state writes replicate (HITL + triggers HA)
- Real local cross-encoder reranker (rerank-local feature)
- SDK publish jobs (PyPI + npm) in release workflow
- The last three features — tool idempotency, operator scale planning, gRPC shard routing
- PG-wire tenant auth + shard routing, and the k8s operator live apply loop
- Operator --crd emitter + live apply loop verified on a real k8s cluster
- Anthropic (Claude) LLM provider for extraction/rerank/eval
- Tunable retrieval widths + tokenizer hygiene (measured neutral on recall@5; enables A/B)
- Refreshed architecture doc + CLAUDE.md realigned to the agentic platform
- Enforce RBAC on gRPC writes
- Tenant/role-scopable API keys
- Admin CLI + restore endpoint
- Secondary security hardening (rate-limit/audit/Raft-secret) + docs
- Retrieval recall fixes (embedding prefixes + per-scope vector index) + comparative LoCoMo bench

### Miscellaneous

- *(build)* Lighter dev debug info (line-tables-only) to shrink target/
- Add RUSTSEC security audit job (cargo-audit)
- Untrack runtime data stores + gitignore them
- *(release)* Publish Python (PyPI) + TypeScript (npm) SDKs on tag
- Gitignore .fastembed_cache (rerank-local model download)
- Gate the operator crate + helm lint/template; publish the operator image on release
- Least-privilege GITHUB_TOKEN + Dependabot for actions/deps
- *(governance+ci)* SECURITY.md, CODEOWNERS, ROADMAP, DCO, SDK CI jobs
- *(cluster)* Remove dead LogShipper stub (replication is via openraft AppendEntries)
- Guard the otlp feature (compile + mock-collector export test)
- Fix YAML parse error + honest feature guards
- *(sdk-typescript)* Untrack build output (dist/)
- Stop failing the build when the Actions cache is down; clear 10 advisories
- *(bench)* Loosen the nightly p95 gate until it has a baseline
- *(bench)* The benchmark job was red on every Dependabot pull request
- *(bench)* The benchmark job has never measured anything
- *(bench)* The comparison had no baseline to compare against
- *(bench)* Keep the profile bench off every commit on `main`

### Performance

- *(core)* Configurable read-connection pool (was hardcoded to 4)
- *(bench)* Fix broken query bench + add cognition/graph benchmarks
- *(core)* DuckDB Appender fast-path for ingest (~72× faster) + fix latent ts parsing

### Refactoring

- *(core)* Split engine.rs god-object — extract test module
- *(gateway)* Split handlers.rs god-object — extract runtime module
- *(gateway)* Split handlers.rs — extract memory/cognition module

### Security

- Memory-intelligence engine, multi-tenant isolation, real HA, Claude turnkey

### Styling

- Rustfmt locomo_convert.rs (CI fmt fix)
- Rustfmt locomo_convert.rs
- Normalize blank lines in the extracted modules

### Testing

- *(cluster)* Prove log-based write path end-to-end through consensus
- *(cluster)* Real 3-node multi-node replication + convergence test
- *(cluster)* Live 3-node mutual-TLS handshake over real sockets
- *(cluster)* 3-node in-process convergence test for GraphSupersede
- *(bench)* Comparative LoCoMo harness (naive-RAG + Mem0) and results doc
- *(bench)* Add STRATA_EMBEDDING__DIMENSION knob to locomo_eval
- *(publish+ui)* Harden public-publish + node tests for console logic
- *(sdk-go)* Add httptest coverage for the Go client
- *(bench)* The Choregos profile, measured (E-09)
- *(cluster)* The mTLS convergence wait was a count, not a deadline

### Build

- *(rerank-local)* Use rustls for hf-hub so the feature builds without system OpenSSL
- Disk guardrail — strip dep debug info + auto-clean over a cap
- *(deps)* Drop the AWS SDK's legacy HTTP client — five advisories with it
- *(audit)* Document the one advisory that stays, with what ends it
- *(deps)* Bump serde_json from 1.0.150 to 1.0.151 in /ops/operator (#21)
- *(deps)* Bump tokio from 1.52.3 to 1.53.1 in /ops/operator (#20)
- *(deps-dev)* Update langchain-core requirement in /sdk/python (#19)
- *(deps)* Update websockets requirement in /sdk/python (#18)
- *(deps)* Httpx >= 0.28.1, llama-index-core >= 0.14.23, setup-python v7
- *(operator)* Kube 4, k8s-openapi 0.28, schemars 1
- *(ci)* Node 24 action majors — upload/download-artifact, buildx, gh-release, setup-helm
- *(deps)* Toml 1.x
- *(deps)* Fastembed 5
- *(deps)* Prost 0.14 + tonic 0.14
- *(deps)* Group Dependabot updates
- *(deps)* Sha2 0.11 and hmac 0.13 — together, and without jsonwebtoken 11 (#31)
- *(deps)* Bump anyhow from 1.0.103 to 1.0.104 in /ops/operator (#22)
- *(deps)* Bump futures from 0.3.32 to 0.3.34 in /ops/operator (#24)
- *(deps)* Bump serde from 1.0.228 to 1.0.229 in /ops/operator (#23)
- *(deps)* Bump the github-actions group across 1 directory with 10 updates (#25)
- *(deps)* Bump rand from 0.9.2 to 0.10.2 (#27)
- *(deps)* Bump the cargo-minor group with 18 updates (#26)

### Rename

- Strata → Ecphoria across the codebase

### Security

- *(ui)* Escape all HTML entities and drop inline event handlers (XSS hardening)

## [0.1.0] - 2026-04-16

### Bug Fixes

- Wire multi-tenancy, semantic cache, MCP sessions + add integration tests
- Viable defaults for first-run experience

### Features

- Enterprise features — OIDC SSO, multi-tenancy, per-source retention
- Hybrid query rewriting for strata_search/strata_state in JOINs

### Miscellaneous

- Rename all GroundDB references to Strata in PRODUCT-SPEC.md
- Gitignore data files (duckdb, sqlite WAL)


