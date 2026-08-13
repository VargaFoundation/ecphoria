# Ecphoria as a Hermes Agent memory provider

[Hermes Agent](https://github.com/NousResearch/hermes-agent) ships a deliberately small built-in
memory — `MEMORY.md` (~2 200 characters) and `USER.md` (~1 375), injected as a frozen snapshot at
session start — plus FTS5 search over past conversations. Anything larger is delegated to a
**memory provider plugin**. This is that plugin.

## Why point Hermes at Ecphoria

Hermes' provider slot already has cloud options. What this one adds:

- **Bi-temporal recall.** Every fact carries `valid_from`/`valid_to` and a supersession chain, so
  *"what did we believe in March"* is a query. A contradicting fact supersedes its predecessor
  rather than overwriting it, and both stay readable.
- **Self-hosted.** One Rust binary on your own infrastructure. No third party sees your agent's
  memory.
- **Shared across agents.** The same server backs Claude Code (over MCP), CI jobs and this plugin,
  so what one agent learns the others can recall. Your documentation, ADRs, closed tickets and
  resolved incidents live in the same store as the conversational memory.

## Install

```bash
# 1. Run a server (see docs/getting-started.md)
docker run -d -p 8432:8432 -v ecphoria-data:/data ghcr.io/vargafoundation/ecphoria:latest

# 2. Install the plugin
cp -r examples/hermes-memory-provider ~/.hermes/plugins/ecphoria
pip install httpx

# 3. Activate it
hermes memory setup      # choose "ecphoria"
hermes memory status
```

Or set it directly in `~/.hermes/config.yaml`:

```yaml
memory:
  provider: ecphoria
```

Environment:

| Variable | Default | Notes |
|---|---|---|
| `ECPHORIA_URL` | `http://localhost:8432` | Server address |
| `ECPHORIA_API_KEY` | — | Only when the server runs with `gateway.auth_enabled` |
| `ECPHORIA_USER` | `hermes` | Memory scope; share it across agents to share recall |

## What it wires up

| Hermes hook | What happens |
|---|---|
| `prefetch` / `queue_prefetch` | Hybrid search (BM25 + vector) injected into the turn's context |
| `sync_turn` | The exchange is journalled as an episodic event — **on a daemon thread**, since Hermes requires this hook to be non-blocking |
| `on_session_end` | `POST /sessions/{id}/distill` turns the session into durable facts, server-side |
| tools | `ecphoria_remember`, `ecphoria_recall`, `ecphoria_history` |

Three tools rather than the server's full 25-tool surface: an agent chooses better from a short
list, and everything else remains reachable over MCP for callers that want it.

`ecphoria_history` is the one with no equivalent in the other providers — it returns every
superseded version of a subject with the period each was believed.

## Failure behaviour

Every call is best-effort. A memory server that is down, slow or misconfigured degrades the
agent's recall; it never breaks its turn. `is_available()` performs no network call, per Hermes'
contract, so an unreachable server surfaces as a degraded turn rather than a failed boot.

## Also worth knowing

Hermes has a built-in MCP client, so you can additionally point it at Ecphoria's MCP endpoint for
the full tool surface (SQL over memories, the knowledge graph, provenance):

```yaml
mcp_servers:
  ecphoria:
    url: http://localhost:8432/mcp
```

Built-in memory keeps running alongside either integration — providers are additive, not
replacements.
