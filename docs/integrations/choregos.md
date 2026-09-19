# Choregos

[Choregos](https://github.com/VargaFoundation/choregos) is an orchestrator: it plans work, runs
coding agents against a repository, gates what they produce and files the result in a tracker.
Ecphoria is where it keeps what it learned.

The division is deliberate. Choregos owns the *workflow* — what to do next, who may approve it, what
a run costs. Ecphoria owns the *corpus* — what is true, since when, and on whose authority. Neither
tries to be the other, and the seam between them is four endpoints.

```
                    ┌──────────────────────────────────┐
  a task starts ──► │ context pack     POST /context-pack
                    │   ↓ what the agent should know    │
  the task ends ──► │ facts            POST /memories   │
                    │                  PUT  /memories/by-external-id
  an agent claims ► │ proposals        POST /memories?status=pending
  a human decides ► │                  POST /pending/{id}/accept | /reject
                    └──────────────────────────────────┘
```

## Before a task: the context pack

An orchestrator does not want to run three retrieval calls and then guess how much of the result
fits in a prompt. It wants one bounded answer.

```bash
POST /api/v1/context-pack
{
  "query": "the checkout service returns 503 under load",
  "paths": ["services/checkout/**"],
  "kinds": ["incident", "decision", "convention"],
  "budget_tokens": 4000,
  "k": 20
}
```

```jsonc
{
  "memories":  [ { "id": "…", "kind": "decision", "subject": "decision:checkout:retry-budget",
                   "content": "…", "score": 0.81, "valid_from": "…", "provenance": {…} } ],
  "incidents": [ { "id": "…", "kind": "incident", "subject": "incident:checkout-api:2026-09-14", … } ],
  "tokens_estimated": 3820,
  "budget_tokens": 4000,
  "truncated": true,
  "candidates": 17
}
```

Three things make this usable by a planner rather than by a human:

- **`paths` is a filter, not a hint.** A memory that names paths is offered only to a task allowed
  to touch one of them; a memory that names none is general knowledge and always passes. Matching is
  symmetric and glob-aware, so a memory filed against `services/checkout/**` reaches a task allowed
  `services/checkout/handler.rs`, and the reverse.
- **Incidents are separated from everything else.** "What is true" and "what went wrong" are
  different inputs to a prompt, and the split is by `kind`, not by a heuristic on the text.
- **The budget is a ceiling, not a target.** Truncation drops the lowest-ranked items, never the
  best, and `truncated` says whether anything was dropped. A pack that is 5 % under budget is
  harmless; one that overflows the prompt is not.

A failure should give an agent *less* context, never an error: on the Choregos side the call is
wrapped in a timeout and a circuit breaker, and any failure returns an **empty pack**. The task then
runs on the repository alone, which is how it worked before there was a memory at all.

## After a task: facts

What a run learned is written as a [typed fact](../facts.md). Choregos' `MemoryKind` and Ecphoria's
`FactKind` are the same vocabulary, on purpose:

| Choregos writes | Subject | When |
| :-- | :-- | :-- |
| `decision` | `decision:<area>:<slug>` | an ADR is merged |
| `convention` | `convention:<area>:<slug>` | a rule is added to the review policy |
| `incident` | `incident:<service>:<yyyy-mm-dd>` | an incident is resolved |
| `ticket_summary` | `ticket:<tracker>:<key>` | a work item is closed |
| `run_lesson` | `run_lesson:<workflow>:<slug>` | a run fails in a way worth remembering |
| `flaky_test` | `flaky_test:<path>::<name>` | the flake detector confirms one |
| `hotspot` | `hotspot:<path>` | the churn/defect analysis runs |
| `finding` | `finding:<tool>:<rule>` | findings triage accepts one |

`kind`, `paths` and `provenance` travel in `metadata` — they are Choregos notions, and that is where
the context pack reads them back to filter by type and by allowed path. Sending them at the root
loses them silently.

```jsonc
{
  "subject": "incident:checkout-api:2026-09-14",
  "content": "checkout returned 503 for 40 minutes after the 03:05 deploy",
  "valid_from": "2026-09-14T03:12:00Z",
  "metadata": {
    "kind": "incident",
    "service": "checkout-api",
    "occurred_at": "2026-09-14T03:12:00Z",
    "paths": ["services/checkout/**"],
    "provenance": {"source": "choregos", "run_id": "…", "work_item_key": "PROJ-412"}
  }
}
```

`valid_from` matters for a backfill: a decision taken in March and imported in September is valid
from March. Without it, every imported decision reads as if it was taken the day of the import, and
the bi-temporal history says nothing useful.

### Idempotent re-delivery

A webhook redelivers. A rerun replays. A sync restarts. All three must converge on **one** memory
per source record, which is what `PUT /api/v1/memories/by-external-id` is for:

```bash
PUT /api/v1/memories/by-external-id
{ "source": "jira", "external_id": "PROJ-412", "content": "…", "metadata": {…} }
```

The pair maps to a stable subject, so unchanged content *confirms* the existing memory (importance
and version go up, nothing is duplicated) and changed content *supersedes* it — the same cognition
as any other write, with no duplicate row. Choregos uses this for everything that has an id in
another system; `POST /memories` only for facts it derives itself.

## Governed writes: an agent may propose, a human decides

A coding agent is a plausible source of facts and a poor judge of them. Ecphoria splits the two:

```bash
POST /api/v1/memories?status=pending     # the agent proposes
GET  /api/v1/pending                     # the review queue, oldest first
POST /api/v1/pending/{id}/accept         # …runs the normal cognition path
POST /api/v1/pending/{id}/reject         # …keeps the row, expired, with the judgement
```

Same request body either way — which of the two a client may do is a **permission** question, not a
different endpoint, so the deployment decides it with a token rather than the client with a URL.

A pending memory is invisible to retrieval by construction: every read path filters
`state = 'active'`, so a proposal cannot leak into a context pack before someone has looked at it.
Accepting runs the *normal* write, so it supersedes what it contradicts exactly like a direct write.
Rejecting keeps the row — a rejected proposal is evidence of a judgement, and deleting it loses
that.

## Governance

For a curated corpus, turn the tenant's rules on ([facts.md](../facts.md)):

```toml
[memory.governance.tenants.choregos]
require_provenance = true      # every fact says where it came from
fact_validation = "strict"     # …and matches its kind's schema
```

With `require_provenance`, a write needs `metadata.provenance.source` or `source_event_ids`. An
empty `provenance: {}` — what a client sends when it has nothing — does not count, so make sure the
orchestrator fills it: `{"source": "choregos", "run_id": …, "work_item_key": …}` is enough, and it
is what makes a fact checkable six months later.

Start at `fact_validation = "warn"`, watch `ecphoria_fact_validation_failures_total`, then switch to
`strict` once the writers are clean.

## MCP

The same corpus is reachable as MCP tools, which is how a coding agent talks to it directly rather
than through the orchestrator: `POST /mcp` (Streamable HTTP; `initialize` returns an
`Mcp-Session-Id`) exposes `search_memory`, `add_memory`, `remember`, `get_memories`,
`memory_history`, `memory_provenance`, `link_memory`, `graph_neighbors` and the query/state tools.
See [connect-claude.md](../connect-claude.md).

Two ways to wire it, and they are not equivalent:

- **Through the orchestrator** — Choregos calls the REST endpoints above and hands the agent a
  prepared pack. The agent cannot write, cannot widen its own context, and the run is reproducible.
- **Directly to the agent** — the agent holds an MCP connection and searches as it goes. More
  capable, less reproducible; if you do this, give it a token that may only *propose*.

## Tenancy

One tenant per Choregos installation, one `project` per repository. `project` is a filter inside a
scope, not a scope of its own: a query can narrow to one repository *or* span them all with a single
ranking. Putting the repository in the scope tuple instead would isolate repositories from each
other and make cross-repository search impossible, which is the opposite of what an orchestrator
running twenty services wants.

A client that serves several tenants sends `X-Ecphoria-Tenant` per request; a tenant-scoped token
still wins over the header.

## Operational notes

| | |
| :-- | :-- |
| **Edition** | `ecphoria:memory` is the right image here — Choregos owns the workflow and runs the agents, so the agent runtime and the LLM proxy are surface it does not use. See [editions.md](../editions.md). |
| **Failure** | Every Ecphoria call is optional by construction. An unreachable memory yields an empty pack and a dropped fact write to retry, never a failed run. |
| **Cost** | Choregos measures model cost at its own gateway, not here. Ecphoria's embedding calls are its own and are configured in `[embedding]`. |
| **Backfill** | `ecphoria import --from git` for documents (valid-time = the file's last commit date) and `--from github` for closed issues/PRs, keyed on the same subject scheme as webhook promotion so backfill and live events converge on one record per ticket. |
