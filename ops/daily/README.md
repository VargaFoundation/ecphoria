# Daily use — Ecphoria + Claude Code

No Hermes required. Hermes is one of three ways in; this is the one you'd actually use every day.

```bash
./ops/daily/setup.sh --dry-run   # show every change
./ops/daily/setup.sh             # make them
```

Idempotent, and it merges into `~/.claude/settings.json` rather than overwriting it.

## What you get

**1. Your knowledge base, in every Claude Code session.** The MCP server exposes 25 tools —
`search_memory`, `add_memory`, `memory_history`, `memory_provenance`, the graph tools, and `query`
for read-only SQL over `memories`. Ask a question in plain language and the answer comes from your
own documentation, ADRs, closed tickets and resolved incidents.

**2. Deliberate memory.** "Remember that the shard router only rebuilds its ring after the lease
expires" writes a fact. Give it a subject and the next contradicting fact supersedes it, with the
old version still queryable — so a decision that changes leaves a trail instead of a mystery.

**3. Automatic session journalling.** A `SessionEnd` hook records what each working session
covered, so `GET /sessions/{id}/recall` replays it and SQL answers "what did I work on last week".

## What the session hook does and does not store

It reads the hook payload from **stdin** (Claude Code passes hook data as JSON on stdin — not as
environment variables, which is worth stating because it is the easy thing to get wrong).

Kept: your prompts, and the assistant's prose replies.

Dropped: tool calls, tool results, file contents, diffs, subagent chatter, slash-command wrappers,
and interstitial narration ("let me check the config"). Measured on a real session, that filtering
takes a **3.9 MB transcript down to 25 KB** — the 1% that is actually the conversation. Without it
you would be filling a knowledge base with the assistant's own tool output.

Sessions shorter than four turns are skipped; a quick question is not something to remember.

### Journalling, not distilling

By default the hook journals the turns and stops. That is immediately useful and costs nothing.

Turning turns into *facts* is opt-in (`ECPHORIA_CAPTURE_DISTILL=1`) because it needs a completion
provider configured on the server. Without one, `/sessions/{id}/distill` falls back to
concatenating the raw events into a single memory — about 15 KB of JSON that pollutes the corpus
and answers nothing. Journalling plus deliberate `remember` calls beats automatic dumping.

Failures are silent with exit 0. A memory server that is down must never break the exit of an
editing session.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `ECPHORIA_URL` | `http://localhost:8432` | Server address |
| `ECPHORIA_API_KEY` | — | Only when the server runs with `gateway.auth_enabled` |
| `ECPHORIA_CAPTURE_DISTILL` | off | Distil journalled sessions into facts (needs an LLM provider) |
| `ECPHORIA_CAPTURE_MIN_TURNS` | `4` | Skip sessions shorter than this |
| `ECPHORIA_CAPTURE_MIN_ASSISTANT_CHARS` | `280` | Drop interstitial narration |
| `ECPHORIA_CAPTURE_MAX_CHARS` | `4000` | Truncate one turn |

## Keeping the corpus fresh

```bash
ecphoria import --from git --path .            # after every merge, or from CI
ecphoria import --from git --path . --watch    # live, while you work
```

Re-import is idempotent: unchanged sections are confirmed with no write, an edited section
supersedes only itself, a deleted one is expired. See [`../../docs/knowledge-base.md`](../../docs/knowledge-base.md)
for loading tickets and incidents too, and for the retrieval settings that matter
(`retrieval_vector_weight = 0.5` in particular).

## Removing it

```bash
claude mcp remove ecphoria
```

and delete the `SessionEnd` entry from `~/.claude/settings.json`. Nothing else is touched.
