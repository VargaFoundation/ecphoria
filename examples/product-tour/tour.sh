#!/usr/bin/env bash
#
# Ecphoria — end-to-end product tour.
#
# One command. It boots a real ecphoria-server (auth on, two tenants, all stores on a throwaway
# temp dir), walks an AI support agent through the platform's differentiators, then tears the
# server down. No API keys, no Ollama, no cloud — everything runs locally and deterministically.
#
#   ./examples/product-tour/tour.sh
#
# The story: "Aria", an AI support agent for a SaaS company (tenant `acme`), uses Ecphoria as its
# durable memory + runtime. We show what a *memory platform* gives an agent that a vector DB doesn't.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT/target/debug/ecphoria-server"
PORT=18432 ; PGPORT=15432 ; GRPCPORT=19432 ; RAFT=19433
DATA="$(mktemp -d)"
BASE="http://127.0.0.1:$PORT"
ACME="acme-secret" ; GLOBEX="globex-secret"

bold() { printf '\033[1m%s\033[0m\n' "$1"; }
act()  { printf '\n\033[1;36m━━ %s ━━\033[0m\n' "$1"; }
note() { printf '   \033[2m%s\033[0m\n' "$1"; }
run()  { printf '   \033[2m$ %s\033[0m\n' "$1"; }

# curl wrappers: acme/globex METHOD PATH [json-body]
_call() { local key=$1 m=$2 p=$3 body=${4:-}
  if [ -n "$body" ]; then
    curl -s -X "$m" "$BASE$p" -H "Authorization: Bearer $key" -H 'Content-Type: application/json' -d "$body"
  else
    curl -s -X "$m" "$BASE$p" -H "Authorization: Bearer $key"
  fi
}
acme()   { _call "$ACME"   "$@"; }
globex() { _call "$GLOBEX" "$@"; }

cleanup() { [ -n "${SRV:-}" ] && kill "$SRV" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT

# ── Boot ────────────────────────────────────────────────────────────────────
[ -x "$BIN" ] || { bold "Building ecphoria-server (first run)…"; (cd "$ROOT" && cargo build -q --bin ecphoria-server) || exit 1; }
bold "Starting Ecphoria — auth on, tenants {acme, globex}, throwaway data in $DATA"
ECPHORIA_STORAGE__DATA_DIR="$DATA" \
ECPHORIA_MEMORY__EPISODIC__DB_PATH="$DATA/episodic.duckdb" \
ECPHORIA_MEMORY__COGNITION__DB_PATH="$DATA/memories.duckdb" \
ECPHORIA_MEMORY__STATE__DB_PATH="$DATA/state.db" \
ECPHORIA_MEMORY__SEMANTIC__INDEX_DIR="$DATA/vectors" \
ECPHORIA_RUNTIME__DB_PATH="$DATA/runs.db" \
ECPHORIA_GATEWAY__LISTEN="127.0.0.1:$PORT" \
ECPHORIA_GATEWAY__PG_LISTEN="127.0.0.1:$PGPORT" \
ECPHORIA_GATEWAY__GRPC_LISTEN="127.0.0.1:$GRPCPORT" \
ECPHORIA_CLUSTER__LISTEN="127.0.0.1:$RAFT" \
ECPHORIA_GATEWAY__AUTH_ENABLED=true \
ECPHORIA_GATEWAY__API_KEYS="${ACME}@acme:admin,${GLOBEX}@globex:admin" \
ECPHORIA_EMBEDDING__PROVIDER=none \
  "$BIN" >"$DATA/server.log" 2>&1 &
SRV=$!

for i in $(seq 1 50); do
  curl -sf "$BASE/health" >/dev/null 2>&1 && break
  kill -0 "$SRV" 2>/dev/null || { bold "server died — log:"; cat "$DATA/server.log"; exit 1; }
  sleep 0.2
done
note "Ready. HTTP :$PORT · PostgreSQL wire :$PGPORT · gRPC :$GRPCPORT · MCP at /mcp"

C="acme-cust-7"  # the customer Aria is helping

# ── 1. Cognition: remember + resolve contradictions (bi-temporal) ────────────
act "1 · Aria remembers facts about a customer — and reconciles contradictions"
note "A vector DB stores text. A memory platform *reasons* about it: dedup, contradiction, decay."
acme POST /api/v1/memories "{\"user_id\":\"$C\",\"subject\":\"plan\",\"content\":\"On the Pro plan\",\"importance\":0.8}" >/dev/null
acme POST /api/v1/memories "{\"user_id\":\"$C\",\"subject\":\"contact_pref\",\"content\":\"Prefers email over phone\"}" >/dev/null
acme POST /api/v1/memories "{\"user_id\":\"$C\",\"content\":\"Team is based in Paris (CET)\"}" >/dev/null
note "The customer upgrades. Same subject 'plan', new value → the old fact is SUPERSEDED, not lost:"
UP=$(acme POST /api/v1/memories "{\"user_id\":\"$C\",\"subject\":\"plan\",\"content\":\"Upgraded to the Enterprise plan\"}")
PLAN_ID=$(echo "$UP" | jq -r '.memory.id')
echo "$UP" | jq -c '{outcome, now_active: .memory.content}'

# ── 2. Recall in a brand-new session (hybrid retrieval) ──────────────────────
act "2 · A NEW conversation starts — Aria recalls the customer instantly"
note "This is the whole point: memory survives the session (and process restarts — stores are on disk)."
run "POST /api/v1/memories/search  {query: 'what plan & how to contact them?'}"
acme POST /api/v1/memories/search "{\"user_id\":\"$C\",\"query\":\"what plan are they on and how do they want to be contacted?\"}" \
  | jq -c '.results[] | {score, fact: .memory.content}'
note "Note the Enterprise plan is returned; the superseded 'Pro plan' is NOT — Aria never sees stale facts."

# ── 3. Query memory like a database (SQL over memories) ──────────────────────
act "3 · The agent's memory IS a database — real SQL over bi-temporal memories"
note "PostgreSQL-wire compatible. Point BI tools / psql / any pg driver at it. SELECT-only, tenant-scoped."
run "POST /api/v1/query  SELECT subject, content, valid_from FROM memories WHERE valid_to IS NULL"
acme POST /api/v1/query \
  '{"sql":"SELECT subject, content, importance FROM memories WHERE valid_to IS NULL ORDER BY importance DESC"}' \
  | jq -c '.rows[] | {subject, content, importance}'

# ── 4. Correct, filter, and enumerate (curation surface) ─────────────────────
act "4 · Curate memory — correct in place, filter, and see the directory of who has memories"
note "PATCH corrects a fact (re-embedded, version-bumped) without losing the audit trail."
run "PATCH /api/v1/memories/$PLAN_ID  {importance: 0.95}"
acme PATCH "/api/v1/memories/$PLAN_ID" '{"importance":0.95}' | jq -c '{corrected: .content, importance}'
run "GET /api/v1/memories?min_importance=0.9   (filters + offset pagination)"
acme GET "/api/v1/memories?user_id=$C&min_importance=0.9" | jq -c '{count, top: [.memories[].content]}'
run "GET /api/v1/schema/memory-scopes   (who/what has memory — the directory)"
acme GET /api/v1/schema/memory-scopes | jq -c '.scopes[] | {user_id, memories: .count}'

# ── 5. Provenance & bi-temporal audit ───────────────────────────────────────
act "5 · Every fact is auditable — provenance + full version history"
note "Compliance & trust: show *why* the agent believes something, and what it believed before."
run "GET /api/v1/memories/$PLAN_ID/provenance"
acme GET "/api/v1/memories/$PLAN_ID/provenance" \
  | jq -c '{fact: .memory.content, believed_over_time: [.history[].content]}'

# ── 6. Knowledge graph ───────────────────────────────────────────────────────
act "6 · Facts connect into a knowledge graph you can traverse"
acme POST /api/v1/memories/link '{"src":"Acme Corp","relation":"subscribes_to","dst":"Enterprise plan"}' >/dev/null
acme POST /api/v1/memories/link '{"src":"Acme Corp","relation":"headquartered_in","dst":"Paris"}' >/dev/null
acme POST /api/v1/memories/link '{"src":"Paris","relation":"in_region","dst":"EU"}' >/dev/null
run "GET /api/v1/memories/graph/centrality   (most-connected entities)"
acme GET /api/v1/memories/graph/centrality | jq -c '.nodes[] | {entity: .node, in_degree, out_degree}' | head -5

# ── 7. The agent runtime (durable runs) ──────────────────────────────────────
act "7 · Ecphoria doesn't just store memory — it RUNS the agents on top of it"
note "Durable run ledger: every agent run is journaled and survives a crash/failover (Raft-replicated in a cluster)."
RUN=$(acme POST /api/v1/runs "{\"agent_id\":\"aria\",\"input\":{\"ticket\":\"upgrade question\",\"customer\":\"$C\"}}")
echo "$RUN" | jq -c '{run: .run.id, agent: .run.agent_id, status: .run.status}'
run "GET /api/v1/runs   (the durable ledger)"
acme GET /api/v1/runs | jq -c '.runs[]? // .[]? | {id, agent_id, status}' | head -3

# ── 8. Multi-tenant isolation (the platform guarantee) ───────────────────────
act "8 · Hard multi-tenant isolation — 'globex' cannot see a byte of 'acme'"
note "Same server, same tables. Isolation is enforced on EVERY read path (search, SQL, state, sessions)."
run "globex searches for the same customer…"
G1=$(globex POST /api/v1/memories/search "{\"user_id\":\"$C\",\"query\":\"plan and contact\"}" | jq '.results | length')
run "globex runs SQL over memories…"
G2=$(globex POST /api/v1/query '{"sql":"SELECT count(*) AS n FROM memories"}' | jq -r '.rows[0].n')
printf '   globex search hits: \033[1m%s\033[0m   ·   globex sees \033[1m%s\033[0m memories (acme has several)\n' "${G1:-0}" "${G2:-0}"

# ── 9. Protocol-native (bonus) ───────────────────────────────────────────────
act "9 · One store, every protocol — REST · PostgreSQL wire · gRPC · MCP-native"
note "Connect Claude/Claude Desktop directly via MCP (/mcp, 25 tools). Observability is built in:"
run "GET /metrics | grep ecphoria_"
curl -s "$BASE/metrics" | grep -E '^ecphoria_(rest_requests_total|memory|publish|attachments)' | head -4 | sed 's/^/   /'

printf '\n\033[1;32m✓ Tour complete.\033[0m Ecphoria = durable, queryable, auditable, multi-tenant memory — and the runtime that runs agents on it.\n'
note "Everything above ran locally against a real server with zero external services. Data dir is discarded on exit."
