# strata-core

## Responsibility

Core engine crate. Contains all business logic: three memory stores (episodic,
semantic, state), query execution, storage backends, ingestion pipeline,
and embedding providers. Has ZERO knowledge of protocols (HTTP, PG wire, gRPC)
or clustering (Raft).

## Implementation Status

| Component | Status | Backend |
|-----------|--------|---------|
| `EpisodicStore` | **Working** | DuckDB file-backed or in-memory, connection pool (4 readers via try_clone), batch transactions, typed schema (TIMESTAMPTZ, JSON), SQL injection protection (sqlparser SELECT whitelist), configurable max_rows pagination |
| `SemanticStore` | **Working** | USearch HNSW, upsert/search/delete, cosine similarity, persistent save/load to disk, memory-efficient EntryMetadata (no vector duplication in DashMap) |
| `StateStore` | **Working** | rusqlite + DashMap hot cache, full CRUD + CAS + list_keys, race-safe cache population (or_insert_with) |
| `LocalStorage` | **Working** | tokio::fs, put/get/delete/list with tempfile tests |
| `IngestPipeline` | **Working** | Validates → appends to EpisodicStore → auto-embed (batched by config.batch_size) → upsert to SemanticStore. Embedding failures are non-fatal |
| `StrataEngine` | **Working** | Wires all 3 memories + ingest + embedding provider (auto-instantiated from config), async query_sql with spawn_blocking + timeout |
| `OllamaProvider` | **Working** | HTTP POST to Ollama /api/embed, auto-wired from config |
| `OpenAiProvider` | **Working** | HTTP POST to OpenAI /v1/embeddings, auto-wired from config |
| `S3Storage` | Working | aws-sdk-s3, put/get/delete/list, MinIO-compatible |
| `MaterializedViews` | Working | DuckDB CREATE TABLE AS, refresh, drop, list, SQL-injection-safe (name validation + SELECT whitelist) |
| `QueryPlanner` | Stub | SQL parsing/routing pending |

## Public API Surface

### StrataEngine (main entry point)

**Episodic Memory:**
- `ingest(events)` → stores in DuckDB via pipeline, auto-embeds if provider configured
- `query_sql(sql)` → async, spawn_blocking, timeout, max_rows limit, SELECT-only
- `query_by_source(source, limit)` → filtered event query
- `event_count()` → total event count

**Semantic Memory:**
- `semantic_upsert(entry)` → add/update vector entry
- `semantic_search(vector, k)` → k-NN search, returns scored EntryMetadata results
- `semantic_delete(id)` → remove entry
- `semantic_count()` → entry count

**State Memory:**
- `state_get(agent_id, key)` → get value (cache-first, fallback to SQLite)
- `state_set(agent_id, key, value)` → set value, returns version
- `state_delete(agent_id, key)` → delete key
- `state_list_keys(agent_id)` → list all keys (limited)

## Testing

- Unit tests: `cargo test -p strata-core` (98 tests)
- LocalStorage tests use `tempfile::TempDir` for isolation
- EpisodicStore tests use both in-memory and file-backed DuckDB
- SemanticStore tests use in-memory USearch with dimension=4 for speed, plus save/load persistence test
- StateStore tests use in-memory SQLite
- All tests run without network access
