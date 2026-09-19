# Two builds: `memory` and `full`

Ecphoria is one binary, and most deployments want all of it. Some do not: a team that runs Ecphoria
as the memory substrate behind its own orchestrator has no use for the agent runtime, and would
rather the server it exposes could not start an agent or call an LLM at all.

That is a build choice, not a config flag. Configuration can be changed by whoever can edit the
config; a capability that is not compiled in cannot be switched on by anyone.

| | `ecphoria:memory` | `ecphoria:full` |
| :-- | :-- | :-- |
| Memory substrate — episodic, semantic, state, cognition, hybrid retrieval | ✅ | ✅ |
| Protocols — REST, PG-wire, gRPC, MCP | ✅ | ✅ |
| Typed facts, governance, context pack, governed writes | ✅ | ✅ |
| Cluster (Raft), sharding, backup/restore | ✅ | ✅ |
| Run ledger, agent driver, HITL approvals, DAG workflows, triggers | — | ✅ |
| Downstream MCP tool gateway (`/tools`) | — | ✅ |
| LLM proxy (`/v1/chat/completions`, `/v1/embeddings`, `/v1/messages`) | — | ✅ |

```bash
docker pull ghcr.io/vargafoundation/ecphoria:latest          # full
docker pull ghcr.io/vargafoundation/ecphoria:latest-memory   # memory only
```

Both images are built from the same Dockerfile, signed with cosign and carry an SBOM attestation.
`docker inspect` shows which is which through `dev.ecphoria.build.features`.

## Building it yourself

```bash
cargo build --release --bin ecphoria-server                       # full
cargo build --release --bin ecphoria-server --no-default-features # memory only

docker build -t ecphoria:full .
docker build -t ecphoria:memory --build-arg FEATURES=--no-default-features .
```

The two features are independent, so an intermediate build is available if you want one:

```bash
cargo build --release --bin ecphoria-server --no-default-features --features llm-proxy
```

## What a memory-only server does with an agent request

It answers **404**. The routes are not mounted, which is the honest answer: the capability is
absent, not broken or misconfigured.

```bash
$ curl -s -o /dev/null -w '%{http_code}\n' localhost:8432/api/v1/memories   # 200
$ curl -s -o /dev/null -w '%{http_code}\n' localhost:8432/api/v1/runs       # 404
$ curl -s -o /dev/null -w '%{http_code}\n' localhost:8432/v1/chat/completions  # 404
```

It also opens no run ledger: `runtime.db_path` is never touched, so `/data` holds the memory stores
and nothing else. `POST /api/v1/webhook/{source}` still ingests and still promotes durable outcomes
to memories; its `triggered_runs` is simply always empty, so a client does not have to know which
build it is talking to.

## In a cluster

`AppRequest::RunCreate` and `RunUpdate` stay in the Raft log format in both builds — they are the
wire format, and removing a variant would shift MessagePack's positional encoding and make the two
builds' logs quietly incompatible. A memory-only node deserializes such an entry, has nothing to
apply it to, and logs:

```
WARN agent-run log entry on a node built without the `agentic` feature — skipped
```

Which is to say: **do not mix the two builds in one cluster**. Each node should be the same edition,
and the run ledger only exists where the runtime does.

## How it is enforced

The split is a compile-time one, with the feature graph arranged so nothing re-enables it by
accident:

| Crate | Feature | Gates |
| :-- | :-- | :-- |
| `ecphoria-core` | `agentic` | `engine/agentic.rs` (ledger, driver, approvals, workflows, triggers), `runtime::store`, the engine's run fields |
| `ecphoria-gateway` | `agentic` | `/runs`, `/agents/run`, `/tools`, `/triggers`, the downstream tool gateway, trigger firing on webhooks |
| `ecphoria-gateway` | `llm-proxy` | the `llm_proxy` module and the `/v1/*` routes |
| `ecphoria-cluster` | `agentic` | applying run entries (the variants themselves stay) |
| `ecphoria-server` | both | the dispatcher loop and the replicator wiring; re-exports both to the crates above |

Every internal dependency spells out `default-features = false` so one crate's default cannot turn
the feature back on for the whole binary through Cargo's feature unification — the usual way a
"disabled" feature ends up compiled in anyway.

CI builds and tests both editions on every push (`memory-only` job), including each feature on its
own, so a memory path that starts reaching into the agent runtime fails there rather than at the
next release.
