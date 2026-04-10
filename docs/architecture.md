# Architecture

This document describes Strata's internal architecture, crate structure, data model, and key design decisions.

> **Current status**: All three memory stores are production-ready with persistence, connection pooling,
> and Prometheus metrics. The gateway serves REST API (with auth middleware), PostgreSQL wire protocol,
> MCP JSON-RPC, LLM proxy, and gRPC. Embedding providers (Ollama, OpenAI) are wired into the ingest
> pipeline with automatic batching. Raft-based clustering is implemented with leader forwarding and
> follower reads. Kubernetes deployment is supported via Helm chart.

## System Overview

Strata is a **context lake** — a unified data layer for AI agents that combines three types of memory in a single Rust binary:

```
                         ┌──────────────────────────────────────────┐
                         │              strata-server                │
                         │  (config, signals, Prometheus recorder)   │
                         └──────────┬───────────────────────────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
   ┌──────────▼──────────┐ ┌───────▼───────┐ ┌───────────▼──────────┐
   │   strata-gateway     │ │strata-cluster │ │  Raft RPC endpoints  │
   │                      │ │               │ │  /raft/append        │
   │ ┌──────┐ ┌────┐     │ │ Coordinator   │ │  /raft/vote          │
   │ │PGWire│ │REST│     │ │ MemStore      │ │  /raft/snapshot      │
   │ └──┬───┘ └─┬──┘     │ │ NetworkClient │ │  /cluster/status     │
   │    │       │         │ └───────┬───────┘ └──────────────────────┘
   │ ┌──┴───────┴──┐      │         │
   │ │Auth / Leader │      │         │
   │ │  Forwarding  │      │         │
   │ └──────┬──────┘      │         │
   └────────┼─────────────┘         │
            │                       │
   ┌────────▼───────────────────────▼───────────────────┐
   │                    strata-core                      │
   │                                                     │
   │  ┌──────────────┐  ┌─────────────────────────────┐ │
   │  │ Query Engine │  │    Ingest Pipeline           │ │
   │  │ (SQL filter, │  │ events → episodic → embed →  │ │
   │  │  max_rows,   │  │          semantic (batched)   │ │
   │  │  timeout)    │  └──────────────┬───────────────┘ │
   │  └──────┬───────┘                 │                  │
   │         │                         │                  │
   │  ┌──────▼─────────────────────────▼───────────────┐ │
   │  │              Memory Stores                      │ │
   │  │                                                  │ │
   │  │ ┌──────────────┐ ┌──────────┐ ┌──────────────┐ │ │
   │  │ │  Episodic    │ │ Semantic │ │    State     │ │ │
   │  │ │  DuckDB      │ │ USearch  │ │  SQLite +   │ │ │
   │  │ │  file-backed │ │ HNSW     │ │  DashMap    │ │ │
   │  │ │  4-conn pool │ │ save/load│ │  hot cache  │ │ │
   │  │ └──────────────┘ └──────────┘ └──────────────┘ │ │
   │  └────────────────────────────────────────────────┘ │
   │                                                      │
   │  ┌──────────────────────────────────────────────────┐│
   │  │            Storage Backends                       ││
   │  │   Local FS   │   S3/MinIO   │   Tiering          ││
   │  └──────────────────────────────────────────────────┘│
   └──────────────────────────────────────────────────────┘
```

## Crate Structure

Strata is organized as a Cargo workspace with five crates:

### `strata-core`

The engine. Contains all business logic with zero knowledge of transport protocols.

| Module | Purpose |
|--------|---------|
| `memory::episodic` | DuckDB-backed event store (file-backed, connection pool, batch transactions, typed schema) |
| `memory::semantic` | USearch HNSW vector index + lightweight metadata (no vector duplication), persistent save/load |
| `memory::state` | Transactional KV store with MVCC (SQLite + DashMap hot cache, race-safe) |
| `query::planner` | Routes SQL to DuckDB, vector search, or hybrid |
| `query::executor` | Executes query plans against memory stores |
| `query::functions` | Custom SQL UDFs: `embed()`, `cosine_similarity()`, `strata_search()` |
| `storage` | `StorageBackend` trait + local/S3 implementations |
| `storage::tiering` | Hot/warm/cold data movement between tiers |
| `ingest::pipeline` | Event ingestion → episodic → auto-embed (batched) → semantic index |
| `embedding` | `EmbeddingProvider` trait + Ollama/OpenAI implementations (auto-wired from config) |
| `materialized` | Materialized views over DuckDB (SQL-injection-safe) |

### `strata-gateway`

Protocol layer. Translates external protocols into calls on `strata-core::StrataEngine`.

| Module | Purpose |
|--------|---------|
| `pg_wire` | PostgreSQL wire protocol via `pgwire` crate (connection-limited via Semaphore) |
| `rest` | REST API via axum (`/health`, `/api/v1/*`, `/metrics`) with timeout and CORS |
| `grpc` | gRPC server via tonic |
| `mcp` | MCP server — Streamable HTTP transport, tools, resources, prompts |
| `llm_proxy` | OpenAI-compatible `/v1/chat/completions` with auto-RAG |
| `auth` | API key middleware (Bearer token), JWT types, RBAC roles |
| `cluster` | Raft RPC endpoints (`/raft/*`), leader-forwarding middleware, `/cluster/status` |

### `strata-cluster`

Distributed mode. Implements Raft consensus via `openraft` v0.9.

| Module | Purpose |
|--------|---------|
| `raft::types` | `TypeConfig` (AppRequest, AppResponse, NodeInfo), MessagePack serialization |
| `raft::store` | `MemStore` — full `RaftStorage` impl, applies entries to StrataEngine |
| `raft::network` | `NetworkClient` + `NetworkFactory` — HTTP JSON transport between nodes |
| `coordinator` | `ClusterCoordinator` — Raft lifecycle, `client_write()`, leader detection, shutdown |
| `replication` | WAL segment shipping, snapshot transfer (planned) |

### `strata-cli`

CLI admin tool. Communicates with the server via HTTP. Binary name: `strata`.

### `strata-server`

Main binary. Thin wiring layer: config → engine → cluster coordinator → gateway → signal handling.

## Dependency Graph

```
strata-server (binary)
  ├── strata-core
  ├── strata-gateway → strata-core, strata-cluster
  └── strata-cluster → strata-core

strata-cli (binary)
  └── strata-core (shared types)
```

**Rule**: dependencies flow downward. `strata-core` has zero knowledge of the protocol or cluster layers.

## Data Model

### Three Memory Types

**Episodic Memory** — What happened.
- Append-only event store backed by DuckDB (file-backed or in-memory)
- Each event has: `id` (UUID), `source`, `event_type`, `payload` (JSON native), `timestamp` (TIMESTAMPTZ native)
- Connection pool with 4 reader connections (via `try_clone`) for concurrent queries
- Batch transactions (BEGIN/COMMIT/ROLLBACK) for high-throughput ingest
- SQL injection protection via `sqlparser` (only SELECT queries allowed)
- Configurable max_rows pagination and query timeout

**Semantic Memory** — What it means.
- Vector embeddings stored in a USearch HNSW index (persistent save/load)
- Each entry has: `id`, `content`, `embedding` (f32 vector), `metadata` (JSON)
- Memory-efficient: `EntryMetadata` in DashMap stores only content+metadata (no vector duplication)
- Supports k-nearest-neighbor search with cosine similarity
- Auto-populated from episodic events via the batched ingestion pipeline

**State Memory** — Where things stand.
- Transactional key-value store with MVCC (multi-version concurrency control)
- Each entry has: `agent_id`, `key`, `value` (JSON), `version`
- Supports compare-and-swap (CAS) for lock-free coordination
- Race-safe hot cache via DashMap (`or_insert_with`), persistent storage via SQLite

### Ingestion Pipeline

```
Events (HTTP/Webhook/gRPC/MCP)
    │
    ▼
IngestPipeline
    │
    ├── 1. Append to EpisodicStore (batch transaction)
    │
    ├── 2. If embedding provider configured:
    │      ├── Format text: "[source] event_type: payload"
    │      ├── Batch embed via provider (chunks of batch_size)
    │      └── Upsert to SemanticStore
    │
    └── 3. Return count (embedding failures are non-fatal)
```

## Cluster Architecture

Strata supports multi-node deployment via Raft consensus (openraft v0.9).

```
              ┌──────────────────┐
              │  Load Balancer   │
              └──────┬───────────┘
       ┌─────────────┼─────────────┐
  ┌────┴────┐  ┌─────┴───┐  ┌─────┴───┐
  │ Node 1  │  │ Node 2  │  │ Node 3  │
  │ Leader  │◄─│Follower │◄─│Follower │
  │   RW    │  │ RO read │  │ RO read │
  └────┬────┘  └────┬────┘  └────┬────┘
       └────────────┼────────────┘
              Raft consensus
              (HTTP JSON RPC)
```

**Write path**: Client → any node → if not leader, 307 redirect → leader → Raft commit → apply to StrataEngine → replicate to followers.

**Read path**: Client → any node → read from local engine (eventual consistency). GET requests are always served locally for low-latency reads.

**Raft RPCs**: AppendEntries, Vote, InstallSnapshot — all via HTTP POST to `/raft/*` endpoints on each node.

## Concurrency Model

Strata runs on the Tokio multi-threaded runtime. Key concurrency patterns:

- **DuckDB**: 1 write connection + 4 read connections (via `try_clone`), `parking_lot::Mutex` per connection
- **USearch**: `parking_lot::Mutex` around the HNSW index
- **SQLite**: `parking_lot::Mutex` around the connection + `DashMap` lock-free hot cache
- **Engine queries**: Wrapped in `tokio::task::spawn_blocking` to avoid starving async workers
- **PG wire**: `tokio::sync::Semaphore` limits concurrent connections (default 256)
- **HTTP**: `tower_http::TimeoutLayer` enforces 30s request timeout

## Security Model

Authentication is handled at the gateway layer:
- **API Keys**: Bearer token in `Authorization` header, validated against configured `gateway.api_keys`
- **JWT**: Stateless token-based for user sessions (types defined, validation pending)
- **RBAC**: Four roles — Admin, Writer, Reader, Agent

Auth middleware is applied to `/api/v1/*` routes. Health, metrics, and Raft RPC endpoints are unauthenticated.

## Observability

- **Structured logging**: `tracing` crate with env-filter (`RUST_LOG=info,strata=debug`)
- **Prometheus metrics**: counters (events_ingested_total, queries_total, rest_requests_total), histograms (append_duration_seconds, query_duration_seconds, rest_request_duration_seconds)
- **Health endpoint**: `GET /health` returns `{"status":"ok","version":"0.1.0"}`
- **Cluster status**: `GET /cluster/status` returns Raft metrics (node_id, state, leader, term, log_index)
- **Metrics endpoint**: `GET /metrics` returns Prometheus text format

## Key Technology Choices

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Language | Rust | Performance, safety, single binary, no runtime |
| Analytics SQL | DuckDB (embedded) | Columnar, zero-config, native JSON/TIMESTAMPTZ types |
| Vector Index | USearch | HNSW, compact, persistent save/load, Rust bindings |
| State Storage | SQLite (via rusqlite) | ACID, embedded, battle-tested |
| Object Storage | S3/MinIO (via aws-sdk-s3) | Standard, tiered, cost-effective |
| Consensus | openraft v0.9 | Raft in Rust, production-grade |
| SQL Validation | sqlparser | Prevents SQL injection, SELECT-only whitelist |
| PG Protocol | pgwire | PostgreSQL wire protocol in Rust |
| HTTP Framework | axum | Async, tower-compatible, high performance |
| gRPC | tonic | HTTP/2, codegen from proto |
| Config | TOML + env vars | Convention over configuration |
| Metrics | metrics + Prometheus exporter | Industry standard observability |
