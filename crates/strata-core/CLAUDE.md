# strata-core

## Responsibility

Core engine crate. Contains all business logic: three memory stores (episodic,
semantic, state), query planning/execution, storage backends, ingestion pipeline,
and embedding providers. Has ZERO knowledge of protocols (HTTP, PG wire, gRPC).

## Implementation Status

| Component | Status | Backend |
|-----------|--------|---------|
| `EpisodicStore` | **Working** | DuckDB in-memory, INSERT/SELECT/COUNT |
| `StateStore` | **Working** | rusqlite + DashMap hot cache, full CRUD + CAS |
| `LocalStorage` | **Working** | tokio::fs, put/get/delete/list with tempfile tests |
| `IngestPipeline` | **Working** | Validates and appends to EpisodicStore |
| `StrataEngine` | **Working** | Wires episodic + state + ingest, exposes public API |
| `OllamaProvider` | **Working** | HTTP client to Ollama /api/embed |
| `OpenAiProvider` | **Working** | HTTP client to OpenAI /v1/embeddings |
| `SemanticStore` | Stub | USearch integration pending |
| `S3Storage` | Stub | aws-sdk-s3 integration pending |
| `QueryPlanner` | Stub | SQL parsing/routing pending |
| `MaterializedViews` | Stub | DuckDB views pending |

## Public API Surface

- `StrataEngine` — main entry point, owns all subsystems
  - `ingest(events)` → stores in DuckDB via pipeline
  - `query_sql(sql)` → executes raw SQL against DuckDB
  - `query_by_source(source, limit)` → filtered event query
  - `event_count()` → total event count
  - `state_get/set/delete(agent_id, key)` → KV operations
  - `state_list_keys(agent_id)` → list keys for agent
- `EpisodicStore` — append-only event storage (DuckDB)
- `StateStore` — transactional KV (SQLite + DashMap)
- `IngestPipeline` — event ingestion with EpisodicStore backend
- `EmbeddingProvider` (trait) — pluggable embedding backends
- `StorageBackend` (trait) — pluggable storage backends

## Testing

- Unit tests: `cargo test -p strata-core` (77 tests)
- LocalStorage tests use `tempfile::TempDir` for isolation
- EpisodicStore tests use in-memory DuckDB
- StateStore tests use in-memory SQLite
- All tests run without network access

## Key Design Rules

- This crate must compile and test without any network access
- All public methods return `Result<T, Error>`
- `StrataEngine` is Send + Sync (verified at compile time)
- DuckDB and SQLite connections wrapped in `Arc<Mutex<Connection>>`
- DashMap used for lock-free hot cache reads on StateStore
