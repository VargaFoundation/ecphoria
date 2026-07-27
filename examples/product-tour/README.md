# Ecphoria — end-to-end product tour

A single command that builds + boots a real `ecphoria-server` **with in-process semantic embeddings**
and walks an AI Customer-Success agent (“Aria”) through what an **agentic memory platform** gives you
that a vector database doesn’t.

```bash
./examples/product-tour/tour.sh
```

No API keys, no Ollama, no cloud. Embeddings run **in-process via ONNX** (fastembed / `bge-small-en`,
feature `embed-local`). It runs the server locally with **auth on and two tenants** (`acme`, `globex`)
against a throwaway temp dir, plays the tour, and tears everything down. Only needs `curl` + `jq`.

> **First run is slow:** the initial build compiles onnxruntime (~7 min) and the first launch downloads
> the embedding model (~130 MB from HuggingFace). Both are cached — subsequent runs take a few seconds.

## The scenario

**Aria**, the AI Customer-Success agent at a SaaS company (tenant `acme`), builds durable memory about
two accounts — **Northwind** and **Contoso** — over many conversations. Tenant `globex` exists only to
prove isolation.

## What it shows (≈1 min once built)

| # | Act | The value |
|---|-----|-----------|
| 1 | **Onboard accounts** | Facts arrive across separate chats; the platform dedups, ranks, and (below) reconciles + audits them. |
| 2 | **Recall by *meaning*** ⭐ | Questions phrased naturally (“which cloud region?”) find the right fact (“Kubernetes in AWS eu-west-1”) **with no shared keywords** — real embeddings, not string match. |
| 3 | **Reconcile a change** | A plan upgrade **supersedes** the old fact bi-temporally (kept as history, not overwritten). |
| 4 | **SQL + time travel** ⭐ | `SELECT … WHERE valid_to IS NULL` over a PostgreSQL-wire table — and an **as-of** query: *what did we believe before the upgrade?* |
| 5 | **Curate** | `PATCH` a fact in place, filter by importance with offset pagination, list the **account directory**. |
| 6 | **Knowledge graph** ⭐ | Facts link into a graph; a **multi-hop path** query traces `Dana Lee → Northwind → AWS eu-west-1 → EU`. |
| 7 | **Agent runtime** | Ecphoria doesn’t just store memory — it **runs the agents on it**, with a durable, crash-safe run ledger. |
| 8 | **Multi-tenant isolation** | `globex` sees **zero** of `acme` — enforced on every read path (search, SQL, graph, …). |
| 9 | **Protocol-native** | One store speaks REST · PostgreSQL wire · gRPC · **MCP** (connect Claude directly). Prometheus metrics built in. |

## The one-liner

> **Ecphoria is durable, semantic, auditable, multi-tenant memory for AI agents — and the runtime that
> runs the agents on top of it.**

## Notes (honest defaults)

- **Retrieval is set to pure relevance** for the tour (`retrieval_importance_weight=0`,
  `retrieval_recency_weight=0`) so semantic recall is crisp to watch. In production you’d usually keep
  the default blend (importance + 30-day recency nudge on top of relevance) — it’s a per-use-case knob,
  not a fixed behavior.
- **Swap the embedder freely:** set `ECPHORIA_EMBEDDING__PROVIDER=ollama` (or `openai`) instead of the
  in-process `local` backend — the rest of the tour is identical.
- Everything runs against the **real server binary** (rebuilt each run), so the demo always reflects the
  current code — not a mock.

## Going further

- **Put Claude in the loop:** connect via MCP (`/mcp`, 25 tools) — see [`docs/connect-claude.md`](../../docs/connect-claude.md).
- **From code, no server:** the embedded facade (`crates/ecphoria-core/examples/embedded.rs`) — “the SQLite of agent memory”.
- **A focused “Claude remembers across sessions” cut:** [`examples/claude-memory-demo`](../claude-memory-demo).
- **Framework integrations:** `examples/{langchain-rag, crewai-with-ecphoria, autogen-with-ecphoria, multi-agent-support}`.
