# strata-gateway

## Responsibility

Protocol layer. Translates external protocols (PostgreSQL wire, REST, gRPC, MCP,
LLM proxy) into calls on `strata_core::StrataEngine`. Also handles authentication.

## Implementation Status

| Component | Status | Details |
|-----------|--------|---------|
| REST API | **Working** | axum router, health/query/ingest/search/state endpoints |
| HTTP Server | **Working** | Binds port, graceful shutdown via oneshot channel |
| MCP definitions | Defined | Tools/resources/prompts listed, transport stub |
| PG wire | Stub | pgwire handler skeleton |
| gRPC | Stub | tonic service skeleton |
| LLM proxy | Stub | Router/providers/cache skeletons |
| Auth | Stub | API key/JWT validation, middleware types defined |

## Public API

- `GatewayServer::start(engine, config)` — binds HTTP port, starts serving
- `GatewayServer::shutdown()` — graceful shutdown
- `rest::router()` — stateless router for testing
- `rest::router_with_engine(engine)` — full router with engine state

## REST Routes

| Method | Path | Handler | Status |
|--------|------|---------|--------|
| GET | `/health` | health check | **Working** |
| POST | `/api/v1/query` | SQL query via DuckDB | **Working** |
| POST | `/api/v1/ingest` | event ingestion | **Working** |
| POST | `/api/v1/search` | semantic search | Stub |
| GET | `/api/v1/state/{agent_id}/{key}` | get state | **Working** |
| PUT | `/api/v1/state/{agent_id}/{key}` | set state | **Working** |

## Testing

- `cargo test -p strata-gateway` (32 tests)
- Integration tests in `tests/integration/` test full router with tower::ServiceExt::oneshot
- Gateway lifecycle tests verify start/shutdown with port 0
