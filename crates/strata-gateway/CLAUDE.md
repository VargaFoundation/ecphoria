# strata-gateway

## Responsibility

Protocol layer. Translates external protocols (PostgreSQL wire, REST, gRPC, MCP,
LLM proxy) into calls on `strata_core::StrataEngine`. Also handles authentication,
leader forwarding (cluster mode), and Raft RPC routing.

## Implementation Status

| Component | Status | Details |
|-----------|--------|---------|
| REST API | **Working** | axum router with health/query/ingest/search/state/webhook endpoints, Prometheus metrics per endpoint |
| HTTP Server | **Working** | Binds port, graceful shutdown, 30s timeout (TimeoutLayer), CORS, tracing |
| PG Wire | **Working** | pgwire SimpleQuery + ExtendedQuery, routes SQL to engine, connection limit (Semaphore, default 256) |
| MCP Server | **Working** | JSON-RPC at /mcp: initialize, tools/list, tools/call, resources/list, prompts/list |
| MCP tools | **Working** | query, ingest, get_state, set_state callable via tools/call |
| gRPC | Working | tonic service for Query/Ingest/Search/State/Health RPCs |
| LLM proxy | Working | /v1/chat/completions with auto-RAG from episodic, multi-provider (OpenAI/Ollama/Anthropic) |
| Auth | **Working** | API key Bearer token middleware on /api/v1/* routes, configurable via gateway.api_keys |
| Cluster routes | **Working** | /raft/append, /raft/vote, /raft/snapshot (inter-node RPC), /cluster/status |
| Leader forwarding | **Working** | Middleware returns 307 redirect for writes on follower nodes, serves reads locally |
| Prometheus | **Working** | /metrics endpoint (via PrometheusHandle from server) |

## Public API

- `GatewayServer::start(engine, config, prometheus, coordinator)` — binds HTTP + PG wire + gRPC, starts serving
- `GatewayServer::shutdown()` — graceful shutdown
- `rest::router()` — stateless router for testing
- `rest::router_with_engine(engine)` — full router with engine state
- `rest::router_with_engine_and_auth(engine, api_keys, cluster_state)` — full router with auth + cluster

## REST Routes

| Method | Path | Handler | Auth | Status |
|--------|------|---------|------|--------|
| GET | `/health` | health check | No | **Working** |
| GET | `/metrics` | Prometheus metrics | No | **Working** |
| POST | `/api/v1/query` | SQL query via DuckDB | Yes* | **Working** |
| POST | `/api/v1/ingest` | event ingestion | Yes* | **Working** |
| POST | `/api/v1/webhook/{source}` | webhook ingestion | Yes* | **Working** |
| POST | `/api/v1/search` | semantic vector search | Yes* | **Working** |
| GET | `/api/v1/state/{agent_id}/{key}` | get state | Yes* | **Working** |
| PUT | `/api/v1/state/{agent_id}/{key}` | set state | Yes* | **Working** |
| POST | `/mcp` | MCP JSON-RPC endpoint | No | **Working** |
| POST | `/v1/chat/completions` | LLM proxy with auto-RAG | No | **Working** |
| GET | `/cluster/status` | Raft cluster metrics | No | **Working** |
| POST | `/raft/append` | Raft AppendEntries RPC | No | **Working** |
| POST | `/raft/vote` | Raft RequestVote RPC | No | **Working** |
| POST | `/raft/snapshot` | Raft InstallSnapshot RPC | No | **Working** |

*Auth required only when `gateway.auth_enabled = true`.

## Testing

- `cargo test -p strata-gateway` (52 tests)
- Integration tests in `tests/integration/` test full router
- Gateway lifecycle tests verify start/shutdown with port 0
- Auth middleware tests verify API key validation
- Cluster route tests verify state clonability
