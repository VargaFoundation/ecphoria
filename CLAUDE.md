# Strata — Claude Code Agent Guide

## What is Strata?

Strata is an open-source context lake for AI agents — a unified data layer combining
episodic memory (events), semantic memory (embeddings), and state memory (live key-value)
in a single Rust binary. PostgreSQL wire-compatible, MCP-native, deployable via Docker
or Kubernetes with Raft-based clustering for high availability.

## Architecture Overview

Single binary (`strata-server`) with embedded DuckDB (analytics), USearch (vector HNSW),
and SQLite B-tree (state KV). Exposes PostgreSQL wire protocol on port 5432,
HTTP (REST + MCP + LLM proxy + Prometheus metrics) on port 8432, gRPC on port 9432,
and Raft inter-node RPC on port 9433.

## Workspace Structure

```
Cargo.toml                 Workspace root
├── crates/
│   ├── strata-core/       Core engine: memories, query, storage, ingest, embedding
│   ├── strata-gateway/    Protocol layer: pg_wire, REST, gRPC, MCP, LLM proxy, auth, cluster routes
│   ├── strata-cluster/    Distributed: Raft consensus (openraft), replication, coordination
│   └── strata-cli/        CLI admin tool (binary: `strata`)
├── strata-server/         Main binary: `strata-server`
├── deploy/                Helm chart, Docker Compose cluster config
├── tests/                 Integration tests
└── docs/                  Documentation
```

## Crate Dependency Graph

```
strata-server (bin)
  ├── strata-gateway → strata-core, strata-cluster
  ├── strata-cluster → strata-core
  └── strata-core

strata-cli (bin)
  └── strata-core (shared types only; talks to server via HTTP)
```

**Rule**: dependencies go DOWN. Core has zero knowledge of gateway or cluster.
Gateway may depend on cluster for Raft RPC routing and leader forwarding.

## Build & Test Commands

```bash
cargo fmt --all                                         # Format
cargo fmt --all -- --check                              # Check format (CI)
cargo clippy --workspace --all-targets -- -D warnings   # Lint
cargo test --workspace                                  # All tests (~197 tests)
cargo test -p strata-core                               # Single crate tests
cargo build --release                                   # Release build
cargo run --bin strata-server                           # Run server
cargo run --bin strata -- status                        # Run CLI
```

## Coding Conventions

- **Error handling**: `thiserror` for library errors, `anyhow` only in binaries.
  Every crate has its own `Error` enum in `error.rs`. Propagate with `?`.
- **Async runtime**: Tokio (multi-thread). All public async APIs use `async fn`.
  Blocking operations (DuckDB, SQLite, USearch) are wrapped in `spawn_blocking`.
- **Logging**: `tracing` crate. Use `#[instrument]` on public functions.
  Levels: error (broken), warn (degraded), info (lifecycle), debug (flow), trace (data).
- **Metrics**: `metrics` crate with Prometheus exporter. Record counters and histograms
  at key operations (ingest, query, search). Exposed at `/metrics`.
- **Configuration**: `serde` + TOML deserialization. Env var overrides via `STRATA_` prefix.
  Nested keys use double underscore: `STRATA_STORAGE__ENGINE=s3`.
- **Testing**: Unit tests in `#[cfg(test)] mod tests` at bottom of each file.
  Integration tests in `tests/` directory. Use `#[tokio::test]` for async tests.
- **Naming**: snake_case for functions/variables, PascalCase for types, SCREAMING_SNAKE for constants.
- **Dependencies**: Workspace-level version pinning in root Cargo.toml. Crates use `dep.workspace = true`.

## Adding a New Feature

1. Identify which crate owns the feature (core engine vs protocol vs cluster).
2. If touching the public API of `strata-core`, update both core and gateway.
3. Add types/structs in a new module or extend existing module.
4. Write unit tests first (`cargo test -p <crate>`).
5. If adding an API endpoint, add it in `strata-gateway/src/rest/` and document the route.
6. If adding a CLI command, add it in `strata-cli/src/commands/`.
7. Run `cargo clippy` and `cargo fmt` before considering work done.

## Key Dependencies

| Crate | Purpose | Used in |
|-------|---------|---------|
| tokio | Async runtime | All |
| axum | HTTP framework | gateway |
| pgwire | PostgreSQL wire protocol | gateway |
| duckdb | Embedded analytics SQL (file-backed or in-memory) | core |
| usearch | HNSW vector index (persistent save/load) | core |
| rusqlite | Embedded SQLite for state KV | core |
| openraft | Raft consensus (v0.9, serde feature) | cluster |
| sqlparser | SQL validation (SELECT-only whitelist) | core |
| metrics / metrics-exporter-prometheus | Observability | core, gateway, server |
| serde/serde_json | Serialization | All |
| tracing | Structured logging | All |
| thiserror | Error derive | All libs |
| clap | CLI argument parsing | cli, server |
| reqwest | HTTP client | cli, core (embedding), cluster (Raft RPC) |

## Environment Variables

All prefixed with `STRATA_`. Nested keys use `__`. Examples:
- `STRATA_STORAGE__DATA_DIR` — data directory (default: `./data`)
- `STRATA_STORAGE__ENGINE` — `local` or `s3`
- `STRATA_GATEWAY__LISTEN` — HTTP listen address (default: `0.0.0.0:8432`)
- `STRATA_GATEWAY__PG_LISTEN` — PG wire listen address (default: `0.0.0.0:5432`)
- `STRATA_GATEWAY__AUTH_ENABLED` — enable API key authentication
- `STRATA_EMBEDDING__PROVIDER` — `ollama` or `openai`
- `STRATA_EMBEDDING__OLLAMA_URL` — Ollama URL (default: `http://localhost:11434`)
- `STRATA_CLUSTER__ENABLED` — enable Raft cluster mode
- `STRATA_CLUSTER__NODE_ID` — this node's Raft ID
- `STRATA_CLUSTER__PEERS` — comma-separated peer addresses

## Implementation Status

| Component | Status | Details |
|-----------|--------|---------|
| **EpisodicStore** | Working | DuckDB file-backed or in-memory, connection pool (4 readers), batch transactions, typed schema (TIMESTAMPTZ, JSON) |
| **SemanticStore** | Working | USearch HNSW index, upsert/search/delete, cosine similarity, persistent save/load, memory-efficient (EntryMetadata without vector duplication) |
| **StateStore** | Working | rusqlite + DashMap hot cache, CRUD + compare-and-swap, race-safe cache population |
| **IngestPipeline** | Working | Auto-embed via EmbeddingProvider (batched by config.batch_size), SQL injection protection (sqlparser whitelist) |
| **StrataEngine** | Working | Wires subsystems, async query_sql with spawn_blocking + timeout, configurable max_rows pagination |
| **REST API** | Working | Health, query, ingest, search, state, webhook endpoints with auth middleware and Prometheus metrics |
| **HTTP Server** | Working | axum with graceful shutdown, 30s request timeout, CORS, tracing |
| **PG wire protocol** | Working | pgwire SimpleQuery+ExtendedQuery, routes SQL to DuckDB, connection limit (Semaphore) |
| **MCP server** | Working | JSON-RPC at /mcp, tools/call for query/ingest/state, resources/list, prompts/list |
| **LLM Proxy** | Working | /v1/chat/completions with auto-RAG from episodic context, multi-provider (OpenAI/Ollama/Anthropic) |
| **Ollama embedding** | Working | HTTP client to /api/embed, auto-wired from config |
| **OpenAI embedding** | Working | HTTP client to /v1/embeddings, auto-wired from config |
| **Auth middleware** | Working | API key Bearer token on /api/v1/* routes, configurable via gateway.api_keys |
| **Prometheus metrics** | Working | /metrics endpoint, counters (events_ingested, queries_total), histograms (append_duration, query_duration) |
| **Config loading** | Working | TOML + env vars layered |
| **S3 Storage** | Working | aws-sdk-s3, put/get/delete/list, MinIO-compatible |
| **MaterializedViews** | Working | DuckDB CREATE TABLE AS, refresh, drop, list, SQL-injection-safe |
| **gRPC** | Working | tonic, proto/strata.proto, Query/Ingest/Search/State/Health RPCs |
| **Webhook normalizers** | Working | GitHub, Sentry, Slack, PagerDuty + generic |
| **Raft consensus** | Working | openraft 0.9, TypeConfig, in-memory MemStore, HTTP network transport |
| **ClusterCoordinator** | Working | Raft lifecycle, client_write, leader detection, single-node init, graceful shutdown |
| **Raft RPC endpoints** | Working | /raft/append, /raft/vote, /raft/snapshot for inter-node communication |
| **Cluster status** | Working | /cluster/status endpoint with Raft metrics (term, leader, log index) |
| **Leader forwarding** | Working | Middleware redirects writes (POST/PUT) to leader, serves reads locally (follower reads) |
| **Helm chart** | Working | StatefulSet with auto node_id, headless service for Raft DNS, PDB, ServiceMonitor |

## Parallel Development Guidelines

Each crate is designed for independent development by different agents:

- **strata-core**: No network dependencies for unit tests. Mock storage and embedding.
- **strata-gateway**: Depends on core + cluster via struct interfaces. Can mock engine for testing.
- **strata-cluster**: Depends on core. Can be tested with in-memory Raft (single-node).
- **strata-cli**: Pure HTTP client — test against mock HTTP server.
- **strata-server**: Thin wiring layer, minimal logic.

When working on a crate, read that crate's `CLAUDE.md` for specific guidance.
