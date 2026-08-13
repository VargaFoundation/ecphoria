# Running Ecphoria as an engineering knowledge base

How to point Ecphoria at a team's real material — documentation that changes, ADRs, incidents,
tickets, and what gets learned during development — and query it from the tools you already use.

Measurements behind the choices here are in [`benchmarks-kb.md`](benchmarks-kb.md).

## Instruction files vs. a knowledge base

Instruction files — `CLAUDE.md`, `AGENTS.md`, `.cursorrules` — do two jobs, and only one of them
they do well. Understanding the split is the difference between a working setup and a file nobody
reads.

|  | Instruction file — **pushed** | Ecphoria — **pulled** |
|---|---|---|
| How it reaches the agent | Injected into every session's context, always present | The agent queries it when a question needs it |
| Best for | *Instructions* — build commands, conventions, rules, "always do X" | *Knowledge* — why a decision was made, what broke last quarter, which ticket covered this |
| Cost of growth | Every token is paid on every request, forever | Nothing until asked |
| Staleness | Someone has to remember to edit it | Re-imported from git, tickets and incidents; contradictions supersede automatically |
| Sharing | Copied between repos, drifts | One server, many projects, tenant-scoped |

Ecphoria does **not** replace the instruction file. It replaces the half of it that grows and
rots: accumulated history, past decisions, project context — the part people stop reading because
it is three screens long and half of it is no longer true.

The pattern that works: keep the instruction file short and purely instructional, and add one
paragraph pointing the agent at the knowledge.

```markdown
## Memory

An Ecphoria server holds this project's documentation, ADRs, closed tickets and past incidents.
Search it (`search_memory`) before answering *why* something is built the way it is, and record
durable decisions with `add_memory` — give a `subject` so a later decision supersedes it rather
than duplicating it.
```

Same paragraph works in `AGENTS.md` for OpenCode, or any other agent's instruction file.

## 1. Start a server

```bash
export ECPHORIA_API_KEY=$(openssl rand -hex 32)
docker run -d --name ecphoria -p 5432:5432 -p 8432:8432 -v ecphoria-data:/data \
  -e ECPHORIA_GATEWAY__AUTH_ENABLED=true \
  -e ECPHORIA_GATEWAY__API_KEYS="$ECPHORIA_API_KEY" \
  ghcr.io/vargafoundation/ecphoria:latest
```

### Configuration that matters for this workload

```toml
[memory.cognition]
retrieval_scan_cap     = 2048   # ranked candidates carried forward, NOT a cap on the corpus searched
retrieval_vector_weight = 0.5   # ← the single most important setting here; see below
graph_expansion        = false  # measured neutral on this corpus — no reason to pay for it
extraction             = "none" # LLM fact extraction measured net-negative at this operating point

[memory.promotion]
enabled = true                  # closed tickets and resolved incidents become searchable memories

[embedding]
provider  = "ollama"            # without this, retrieval is BM25-only
model     = "nomic-embed-text"
dimension = 768
```

**Turn embeddings on.** BM25 alone reaches 90.3% recall@5 on the eval set; hybrid reaches **100%**.
The gold set deliberately includes questions sharing no vocabulary with their target document —
that is what the vector arm is for. Cost is ~115 ms per query against ~8 ms, almost entirely the
query-embedding round-trip.

**Then set `retrieval_vector_weight = 0.5`.** The default weights both retrieval arms equally, and
the vector arm's precision falls as the corpus grows while the lexical arm's holds — so at scale
it injects noise with the same authority as the arm that is still right. Measured:

| corpus | w = 1.0 (default) | **w = 0.5** |
|---|---|---|
| 501 sections | 100.0% / MRR 0.781 | 98.6% / 0.778 |
| + 5 000 | 90.3% / 0.668 | **97.2% / 0.752** |
| + 20 000 | 79.2% / 0.497 | **93.1% / 0.723** |

It costs about one question on a toy corpus and gains 14 points at 20k. It is not the shipped
default because that default also governs the conversational workload, which has not been measured
with this knob — so this is a deliberate per-deployment choice rather than a silent global change.
Full numbers in [`benchmarks-kb.md`](benchmarks-kb.md).

**On scoping.** `MemoryScope` matches the exact `(tenant, user, agent, session)` tuple: a memory
written with a `user_id` is invisible to a tenant-scoped search. For a shared knowledge base write
everything at one consistent scope — tenant-only is the natural choice — and use per-user scopes
only for genuinely personal memory. This is the single most common way to end up with a corpus
that silently returns nothing.

**On forgetting.** Decay measures age from `updated_at`, not from when a fact became true, so
anything re-confirmed by an import stays fresh however old its content is. The background job is
off by default (`decay_interval_secs = 0`); documents removed from their source are expired by the
document sweep regardless.

## 2. Load the corpus

### Documentation and ADRs, from git

```bash
ecphoria import --from git --path /path/to/repo
# Importing 51 Markdown files from /path/to/repo…
# Imported 51 files → 479 sections (479 new, 0 updated, 0 unchanged, 0 removed)
```

Every tracked Markdown file is chunked on its heading hierarchy and stored one memory per section,
addressed by `<project>/<path>#<heading trail>` — e.g.
`strata/docs/deployment.md#Kubernetes > Production Values`. The project namespace defaults to the
repository's directory name; override it with `ECPHORIA_PROJECT`.

> **The namespace is load-bearing for multi-project setups.** A document's path is its identity, and
> identity drives both supersession and the removal sweep. Without it, two repositories that each
> contain `README.md` are the same document, and importing the second expires the first one's
> sections. Each file's
**last commit date** becomes the memory's valid-time, so the timeline reflects when the
documentation changed rather than when you ran the importer.

Re-run it after every merge — from CI, a post-merge hook, or `--watch` locally:

```bash
ecphoria import --from git --path . --watch
```

Re-import is idempotent and cheap: unchanged sections are `Confirmed` with no write. Editing one
paragraph reports `0 new, 1 updated, 478 unchanged` and supersedes exactly that section; the
previous text stays queryable. A section deleted from the file is expired, not orphaned.


> **Vendored trees are skipped.** Committed dependency directories (`node_modules`, `vendor`,
> `third_party`, `.venv`, `target`, `dist`) are tracked files, so `git ls-files` returns them and
> somebody else's `README.md` competes with your own — observed in practice, with Microsoft's
> `typescript/SECURITY.md` reaching rank 1 on a policy question. `ECPHORIA_IMPORT_ALL=1` disables
> the skip.

> **Deleted files are retracted.** After each run the importer reports the full set of documents it
> sent, and anything else in the project is expired. The per-document sweep only removes *sections*
> of a file it was given; a file that vanished is never mentioned again, and silence cannot be told
> from "not imported this run". Without this the corpus keeps answering from documentation that no
> longer exists. Only `mem_type = 'chunk'` in that project is touched, so hand-written facts and
> promoted tickets are never swept by a documentation import.

> Backfill runs oldest-first. Replaying history in reverse pushes every version to now rather than
> splitting intervals around it — out-of-order valid-time insertion is not supported.

### Tickets, historical

The webhook path below only ever sees events from the moment it is wired up. Pull everything before
that once:

```bash
export GITHUB_TOKEN=ghp_…      # public repos work without it, at 60 requests/hour
ecphoria import --from github --path owner/repo
```

Closed issues and pull requests (merged *and* closed-unmerged — "we decided not to do this" is
often the more useful memory) become memories keyed exactly as the webhook path keys them
(`owner/repo#issue-7`), so a backfilled ticket that later receives a webhook is confirmed or
superseded rather than duplicated. Valid-time is the close date, so the backfill reconstructs the
real timeline instead of stacking years of history at import time.

### Incidents and tickets, from webhooks

Point GitHub, Sentry and PagerDuty at `/api/v1/webhook/{source}` (sign them —
see [`security.md`](security.md)). Every event lands in the episodic store for SQL analysis; with
`memory.promotion.enabled` the ones that mark a durable outcome — merged PRs, closed issues,
resolved incidents — additionally become memories, so `search_memory` reaches them.

Promoted memories are keyed deterministically (`acme/api#pr-42`), so redelivery confirms rather
than duplicates, and a ticket that reopens and closes again supersedes itself into a history.
Tune which events promote with `memory.promotion.rules` (`"<vendor>:<event_type>"`, `*` allowed).

### What gets learned while working

```bash
./ops/daily/setup.sh          # --dry-run first to see every change
```

Installs a `SessionEnd` hook that journals each Claude Code session, plus the MCP registration.
Hook payloads arrive as **JSON on stdin** (not environment variables), so the hook is a small
script rather than a `curl` one-liner — see [`ops/daily/`](../ops/daily/) for what it keeps and,
more importantly, what it drops: on a real session the filtering takes a 3.9 MB transcript down to
25 KB of actual conversation.

Journalling is the default; distilling those turns into facts is opt-in and needs a completion
provider — without one, `distill` concatenates raw events into a single useless memory.

### Events from a file

```bash
ecphoria ingest --source deploys --file events.jsonl
```

Accepts a JSON array, a single object, or JSON Lines. Read client-side and sent as events — the
server has no file-reading path, and should not grow one: a path in the request body would resolve
against the *server's* filesystem.

### Anything else

`POST /api/v1/memories/batch` takes up to 10 000 memories per request and is ~4.8× faster than a
loop over `/memories` — use it for any bulk import you write yourself. `POST /api/v1/documents`
does the same for one Markdown document, chunking and sweeping server-side.

## 3. Query it

### From Claude Code

```bash
claude mcp add --transport http ecphoria http://localhost:8432/mcp \
  --header "Authorization: Bearer $ECPHORIA_API_KEY"
```

The `--transport http` flag is required — there is no stdio binary. This exposes the full tool
surface: `search_memory`, `add_memory`, `memory_history`, `memory_provenance`, the graph tools, and
`query` for arbitrary read-only SQL over `memories`.

### From Hermes Agent

See [`examples/hermes-memory-provider/`](../examples/hermes-memory-provider/) — a `MemoryProvider`
plugin that puts Ecphoria in Hermes' provider slot alongside Mem0 and Hindsight, adding
self-hosting and bi-temporal history.

### From SQL

The PostgreSQL wire protocol serves the whole store, so any PG client works — psql, Grafana, a
notebook. The password is your API key.

```sql
-- What did this runbook say about failover in March?
SELECT content FROM memories
WHERE subject = 'platform/docs/runbook.md#Runbook > Failover'
  AND valid_from <= '2026-03-15' AND (valid_to IS NULL OR valid_to > '2026-03-15');

-- Which documentation changed most often this quarter?
SELECT split_part(subject, '#', 1) AS doc, count(*) AS versions
FROM memories WHERE mem_type = 'chunk' AND valid_from > '2026-04-01'
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Every version of a decision, with the period each was believed.
SELECT valid_from, valid_to, state, content FROM memories
WHERE subject LIKE 'platform/docs/adr/adr-002%' ORDER BY valid_from;
```

### Over REST

```bash
curl -sX POST localhost:8432/api/v1/memories/search -H "$AUTH" \
  -d '{"query":"why did we choose USearch over pgvector","k":5}'

curl -sG localhost:8432/api/v1/memories/history -H "$AUTH" \
  --data-urlencode 'subject=platform/docs/runbook.md#Runbook > Failover'
```

## Several projects, one team

Import each repository and they share one corpus, so a question asked while working on project A
can be answered from project B's decisions. Each memory also carries a **project**, so a search can
narrow to one without splitting the store:

```bash
ecphoria import --from git --path ~/src/payments     # project = "payments"
ecphoria import --from git --path ~/src/platform     # project = "platform"

ecphoria memory search "refund window"                      # every project, ranked together
ecphoria memory search "refund window" --project payments   # just that one
```

Over REST and MCP it is a `project` field on `add_memory` and `search_memory`; omit it to search
everything.

### Why a filter and not a scope

The scope tuple (`tenant`, `user`, `agent`, `session`) is an **exact match**, so putting the project
there would isolate projects *and* make cross-project search impossible — which is the situation the
`memory_grants` mechanism exists to work around. Grants do it by running one search per scope and
concatenating the result lists, and concatenation is not fusion: results from different projects are
never ranked against each other, so the "best" answer is whichever project happened to come first.

Keeping one scope and filtering gives both properties at once. Unfiltered, a single RRF fusion ranks
the whole corpus; filtered, you get one project exactly.

The project is also part of a memory's identity, so `deploy.target` in two repositories are two
facts rather than a contradiction — and superseding one leaves the other alone.

### Teams

Give each team a tenant-scoped API key (`<secret>@<tenant>:<role>`). The tenant comes from the token
and overrides anything a client sends, so teams share a deployment without seeing each other's
memory. Verified end to end: two engineers on one tenant, each having imported only their own
repository, both retrieve the other's ADRs, while a second tenant on the same server retrieves
nothing.

The CLI reads `ECPHORIA_API_KEY` (or `ECPHORIA_TOKEN`).

## Known limits

- **Backfill must be oldest-first** (above).
- **Long agent runs need `background: true`.** `POST /api/v1/agents/run` runs the loop inline by
  default, under the server's 30 s request timeout; pass `{"background": true}` for `202 Accepted`
  plus a run id to poll (`/runs/{id}`, `/runs/{id}/trace`).
- **Vector dedup does not see within a batch.** Two subject-less near-duplicates submitted in one
  `/memories/batch` call are both stored; contradiction resolution by subject is unaffected.
- **Cluster mode has no batched write path.** `/memories/batch` falls back to per-memory Raft
  replication — correct, but without the speedup. Bulk imports should target a single node.
- **Only Markdown is chunked.** PDFs and Office documents are stored as opaque attachments; their
  text is not extracted.
- **No Jira or Confluence connector.** GitHub, Sentry, PagerDuty and Slack have webhook
  normalizers; other sources need `/api/v1/memories/batch` and a script.
