# Ecphoria — end-to-end product tour

A single command that boots a real `ecphoria-server` and walks an AI support agent (“Aria”)
through what an **agentic memory platform** gives you that a vector database doesn’t.

```bash
./examples/product-tour/tour.sh
```

No API keys, no Ollama, no cloud. It builds the server if needed, runs it locally with **auth on and
two tenants** (`acme`, `globex`) against a throwaway temp dir, runs the tour, and tears everything
down. Runs fully deterministically (BM25 + the deterministic cognition core — configure an embedding
provider for semantic recall on top). Only needs `curl` + `jq`.

## What it shows (≈60 seconds)

| # | Act | The value |
|---|-----|-----------|
| 1 | **Remember + contradiction** | Same `subject`, new value → the old fact is **superseded, not overwritten** (bi-temporal). The agent never sees stale facts. |
| 2 | **Recall in a new session** | Memory outlives the conversation *and* the process (stores are on disk). Hybrid retrieval returns the *current* fact. |
| 3 | **SQL over memories** | `SELECT … FROM memories WHERE valid_to IS NULL` — the agent’s memory is a real, **PostgreSQL-wire** queryable, bi-temporal table. |
| 4 | **Correct / filter / enumerate** | `PATCH` a fact in place, filter by importance/type with offset pagination, and list the **directory of scopes** (who/what has memory). |
| 5 | **Provenance + audit** | Show *why* the agent believes a fact and *what it believed before* — compliance & trust. |
| 6 | **Knowledge graph** | Facts link into a graph you can traverse (centrality / shortest path / communities). |
| 7 | **Agent runtime** | Ecphoria doesn’t just store memory — it **runs the agents on it**, with a durable, crash-safe run ledger. |
| 8 | **Multi-tenant isolation** | `globex` cannot see a single byte of `acme` — enforced on **every** read path (search, SQL, state, sessions). |
| 9 | **Protocol-native** | One store speaks REST · PostgreSQL wire · gRPC · **MCP** (connect Claude directly). Prometheus metrics built in. |

## The one-liner

> **Ecphoria is durable, queryable, auditable, multi-tenant memory for AI agents — and the runtime
> that runs the agents on top of it.**

## Going further

- **Put Claude in the loop:** connect via MCP (`/mcp`, 25 tools) — see [`docs/connect-claude.md`](../../docs/connect-claude.md).
- **From code, no server:** the embedded facade (`crates/ecphoria-core/examples/embedded.rs`) — “the SQLite of agent memory”.
- **A focused “Claude remembers across sessions” cut:** [`examples/claude-memory-demo`](../claude-memory-demo).
- **Framework integrations:** `examples/{langchain-rag, crewai-with-ecphoria, autogen-with-ecphoria, multi-agent-support}`.
