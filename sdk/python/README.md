# ecphoria-client

Python client for [Ecphoria](https://github.com/VargaFoundation/ecphoria) — the open-source
agentic memory platform.

Async, typed (`py.typed`), one dependency (`httpx`). Every call retries idempotent requests with
exponential backoff and surfaces failures as `EcphoriaError` rather than raw HTTP.

## Install

```bash
pip install ecphoria-client
```

Optional integrations:

```bash
pip install "ecphoria-client[langchain]"    # langchain_ecphoria
pip install "ecphoria-client[llama-index]"  # llama_index_ecphoria
```

## Quick start

```python
import asyncio

from ecphoria import EcphoriaClient


async def main() -> None:
    async with EcphoriaClient(url="http://localhost:8432", api_key="…") as client:
        # Remember a fact. `subject` is the contradiction key: a later, different value for the
        # same subject supersedes this one instead of piling up next to it.
        await client.memory_add(
            "The deploy target is the EKS cluster",
            subject="deploy.target",
            user_id="alice",
        )

        # Hybrid retrieval: BM25 fused with vector k-NN, blended with importance and recency.
        for hit in await client.memory_search("where do we deploy", k=5, user_id="alice"):
            print(hit["memory"]["content"], hit["score"])


asyncio.run(main())
```

## What the client covers

| Area | Methods |
| :-- | :-- |
| Memory | `memory_add`, `memory_search`, `memory_list`, `memory_get`, `memory_update`, `memory_delete`, `memory_history`, `memory_scopes` |
| Provenance | `memory_provenance` ("why do you believe this?"), `memory_feedback` |
| Contradictions | `memory_contradictions`, `memory_resolve_contradiction` |
| Knowledge graph | `memory_link`, `graph_neighbors`, `graph_edges`, `graph_centrality`, `graph_path`, `graph_communities` |
| Episodic | `ingest`, `batch_ingest`, `events`, `sources`, `agents`, `query` (read-only SQL) |
| Semantic | `search`, `find`, `embed` |
| Agent state | `state_get`, `state_set`, `state_delete` |
| Health | `health` |

## Scopes

Every memory lives in a scope: `(tenant_id, user_id, agent_id, session_id)`. The tuple is matched
**exactly** — a search for `user_id="alice"` does not see memories written with an extra
`session_id`. Use `project` to narrow *within* a scope instead: it filters without splitting the
store, so one query can still span projects.

## Errors and retries

`EcphoriaError` carries the status code and the server's message. Timeouts, connection errors and
5xx are retried with exponential backoff (`max_retries`, `backoff_base`, `backoff_max`); 4xx are
not — a bad request does not get better by being sent again.

## Development

```bash
pip install -e ".[dev]"
python -m pytest -q      # offline: no server required
python -m build          # sdist + wheel
```

Apache-2.0. See the [repository](https://github.com/VargaFoundation/ecphoria) for the server,
the CLI and the other SDKs (TypeScript, Go).
