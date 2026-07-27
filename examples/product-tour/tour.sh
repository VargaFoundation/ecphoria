#!/usr/bin/env bash
#
# Ecphoria — end-to-end product tour (with real in-process embeddings).
#
# One command. It builds + boots a real ecphoria-server with **in-process ONNX embeddings**
# (fastembed / bge-small-en, feature `embed-local`) — no Ollama, no cloud, no API keys — then walks
# a realistic AI Customer-Success scenario end to end and tears everything down.
#
#   ./examples/product-tour/tour.sh
#
# Story: "Aria", the AI Customer-Success agent at a SaaS company (tenant `acme`). Over many
# conversations she builds durable memory about her accounts (Northwind, Contoso), recalls it by
# MEANING, reconciles changes bi-temporally, reasons over a knowledge graph, and runs agents on it.
# Tenant `globex` is there only to prove hard isolation.
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

_call() { local key=$1 m=$2 p=$3 body=${4:-}
  if [ -n "$body" ]; then
    curl -s -X "$m" "$BASE$p" -H "Authorization: Bearer $key" -H 'Content-Type: application/json' -d "$body"
  else
    curl -s -X "$m" "$BASE$p" -H "Authorization: Bearer $key"
  fi
}
acme()   { _call "$ACME"   "$@"; }
globex() { _call "$GLOBEX" "$@"; }
remember() { acme POST /api/v1/memories "$1" >/dev/null; }   # Aria records a fact about an account

cleanup() { [ -n "${SRV:-}" ] && kill "$SRV" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT

# ── Boot ────────────────────────────────────────────────────────────────────
bold "Building ecphoria-server (--features embed-local)…"
(cd "$ROOT" && cargo build -q --bin ecphoria-server --features embed-local) || exit 1
bold "Starting Ecphoria — auth on, tenants {acme, globex}, in-process embeddings (bge-small-en, 384-d)"
note "First run downloads the ONNX model (~130 MB from HuggingFace), then it's cached. Data in $DATA (discarded on exit)."
ECPHORIA_MEMORY__EPISODIC__DB_PATH="$DATA/episodic.duckdb" \
ECPHORIA_MEMORY__COGNITION__DB_PATH="$DATA/memories.duckdb" \
ECPHORIA_MEMORY__STATE__DB_PATH="$DATA/state.db" \
ECPHORIA_MEMORY__SEMANTIC__INDEX_DIR="$DATA/vectors" \
ECPHORIA_MEMORY__SEMANTIC__DEFAULT_DIMENSION=384 \
ECPHORIA_MEMORY__COGNITION__RETRIEVAL_IMPORTANCE_WEIGHT=0 \
ECPHORIA_MEMORY__COGNITION__RETRIEVAL_RECENCY_WEIGHT=0 \
ECPHORIA_RUNTIME__DB_PATH="$DATA/runs.db" \
ECPHORIA_GATEWAY__LISTEN="127.0.0.1:$PORT" \
ECPHORIA_GATEWAY__PG_LISTEN="127.0.0.1:$PGPORT" \
ECPHORIA_GATEWAY__GRPC_LISTEN="127.0.0.1:$GRPCPORT" \
ECPHORIA_CLUSTER__LISTEN="127.0.0.1:$RAFT" \
ECPHORIA_GATEWAY__AUTH_ENABLED=true \
ECPHORIA_GATEWAY__API_KEYS="${ACME}@acme:admin,${GLOBEX}@globex:admin" \
ECPHORIA_EMBEDDING__PROVIDER=local \
ECPHORIA_EMBEDDING__MODEL=bge-small-en \
ECPHORIA_EMBEDDING__DIMENSION=384 \
  "$BIN" >"$DATA/server.log" 2>&1 &
SRV=$!
for i in $(seq 1 180); do
  curl -sf "$BASE/health" >/dev/null 2>&1 && break
  kill -0 "$SRV" 2>/dev/null || { bold "server died — log:"; cat "$DATA/server.log"; exit 1; }
  sleep 1
done
note "Ready. HTTP :$PORT · PostgreSQL wire :$PGPORT · gRPC :$GRPCPORT · MCP at /mcp"

# ── 1. Onboard accounts — Aria records what she learns, across conversations ──
act "1 · Aria builds durable memory about her accounts"
note "These arrive over weeks of separate chats. A vector DB stores the text; a memory platform will"
note "dedup, reconcile contradictions, rank by importance, and let you *query & audit* it (below)."
remember '{"user_id":"northwind","subject":"plan","content":"Northwind is on the Pro plan.","importance":0.7}'
remember '{"user_id":"northwind","subject":"renewal","content":"Their contract auto-renews every January on an annual term."}'
remember '{"user_id":"northwind","subject":"contact","content":"Main contact is Dana Lee — reach her on Slack; she ignores email."}'
remember '{"user_id":"northwind","content":"Production runs on Kubernetes in AWS eu-west-1."}'
remember '{"user_id":"northwind","content":"Very sensitive about API quotas after a rate-limit outage during Black Friday.","importance":0.6}'
remember '{"user_id":"contoso","subject":"plan","content":"Contoso is on the Enterprise plan."}'
remember '{"user_id":"contoso","subject":"renewal","content":"Renews in Q3 (July)."}'
remember '{"user_id":"contoso","content":"Integrates through the Salesforce connector."}'
note "Recorded 8 facts across 2 accounts."

# ── 2. Recall by MEANING — the payoff of real embeddings ─────────────────────
act "2 · A new conversation about Northwind — Aria recalls by meaning, not keywords"
note "The questions are phrased naturally — not with the words we stored. Keyword search would miss"
note "e.g. 'cloud region' → 'Kubernetes in AWS eu-west-1' (no shared words). Embeddings recall by meaning."
ask() { run "search: \"$1\""; acme POST /api/v1/memories/search "{\"user_id\":\"northwind\",\"query\":\"$1\",\"k\":1}" | jq -r '.results[0].memory.content | "      → " + .'; }
ask "when does their subscription come up for renewal?"
ask "which cloud region are they hosted in?"
ask "what is the best way to reach their point of contact?"
ask "is there anything sensitive I should be careful about?"

# ── 3. Reconcile a change — contradiction handled bi-temporally ──────────────
act "3 · Northwind upgrades — the old fact is superseded, not overwritten"
AS_OF="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; sleep 1   # remember this instant for the time-travel query below
UP=$(acme POST /api/v1/memories '{"user_id":"northwind","subject":"plan","content":"Northwind upgraded to the Enterprise plan."}')
PLAN_ID=$(echo "$UP" | jq -r '.memory.id')
echo "$UP" | jq -c '{outcome, now_active: .memory.content}'
run "GET /api/v1/memories/$PLAN_ID/history"
acme GET "/api/v1/memories/$PLAN_ID/history" | jq -c '.history[] | {state, content}'

# ── 4. Query memory like a database — incl. bi-temporal TIME TRAVEL ──────────
act "4 · The agent's memory is a queryable, bi-temporal database (PostgreSQL-wire)"
run "POST /query  SELECT ... FROM memories WHERE valid_to IS NULL   (everything true *now*)"
acme POST /api/v1/query \
  '{"sql":"SELECT user_id, subject, content FROM memories WHERE valid_to IS NULL ORDER BY user_id"}' \
  | jq -c '.rows[] | {account: .user_id, subject, content}'
run "…and AS OF $AS_OF  (what did we believe *before* the upgrade?)"
acme POST /api/v1/query \
  "{\"sql\":\"SELECT content FROM memories WHERE user_id='northwind' AND subject='plan' AND valid_from <= '$AS_OF' AND (valid_to IS NULL OR valid_to > '$AS_OF')\"}" \
  | jq -c '.rows[] | {believed_then: .content}'

# ── 5. Curate — correct in place, filter, and see the account directory ──────
act "5 · Curate the memory — correct, filter, enumerate"
run "PATCH /api/v1/memories/$PLAN_ID  {importance: 0.95}"
acme PATCH "/api/v1/memories/$PLAN_ID" '{"importance":0.95}' | jq -c '{corrected: .content, importance}'
run "GET /api/v1/memories?user_id=northwind&min_importance=0.9   (filters + offset pagination)"
acme GET "/api/v1/memories?user_id=northwind&min_importance=0.9" | jq -c '{count, kept: [.memories[].content]}'
run "GET /api/v1/schema/memory-scopes   (the directory of accounts with memory)"
acme GET /api/v1/schema/memory-scopes | jq -c '.scopes[] | {account: .user_id, facts: .count}'

# ── 6. Knowledge graph — connect the dots and traverse them ──────────────────
act "6 · Facts become a knowledge graph you can traverse (multi-hop)"
acme POST /api/v1/memories/link '{"src":"Dana Lee","relation":"works_at","dst":"Northwind"}' >/dev/null
acme POST /api/v1/memories/link '{"src":"Northwind","relation":"subscribes_to","dst":"Enterprise plan"}' >/dev/null
acme POST /api/v1/memories/link '{"src":"Northwind","relation":"hosted_in","dst":"AWS eu-west-1"}' >/dev/null
acme POST /api/v1/memories/link '{"src":"AWS eu-west-1","relation":"in_region","dst":"EU"}' >/dev/null
run "GET /api/v1/memories/graph/centrality   (most-connected entities)"
acme GET /api/v1/memories/graph/centrality | jq -c '.nodes[] | {entity: .node, links: (.in_degree + .out_degree)}' | head -4
run "GET /api/v1/memories/graph/path?src=Dana%20Lee&dst=EU   (how is Dana connected to the EU?)"
acme GET "/api/v1/memories/graph/path?src=Dana%20Lee&dst=EU" | jq -c '{reachable, hops: .path}'

# ── 7. The agent runtime — Ecphoria runs the agents on top of the memory ─────
act "7 · Not just storage — Ecphoria runs the agents on it (durable run ledger)"
note "e.g. a scheduled 'renewal brief' agent. Runs are journaled + Raft-replicated → survive a crash/failover."
RUN=$(acme POST /api/v1/runs '{"agent_id":"renewal-brief","input":{"account":"northwind","goal":"draft a renewal talking-points brief"}}')
echo "$RUN" | jq -c '{run: .run.id, agent: .run.agent_id, status: .run.status}'

# ── 8. Multi-tenant isolation — the platform guarantee ───────────────────────
act "8 · Hard multi-tenant isolation — 'globex' sees nothing of 'acme'"
note "Same server, same tables. Enforced on every read path — search, SQL, state, sessions, graph."
G1=$(globex POST /api/v1/memories/search '{"user_id":"northwind","query":"renewal date and cloud region","k":5}' | jq '.results | length')
G2=$(globex POST /api/v1/query '{"sql":"SELECT count(*) AS n FROM memories"}' | jq -r '.rows[0].n')
printf '   globex semantic hits for Northwind: \033[1m%s\033[0m   ·   memories globex can see: \033[1m%s\033[0m  (acme holds 8+)\n' "${G1:-0}" "${G2:-0}"

# ── 9. Protocol-native + observability ───────────────────────────────────────
act "9 · One store, every protocol — REST · PostgreSQL wire · gRPC · MCP-native"
note "Point psql/any pg driver at :$PGPORT, gRPC clients at :$GRPCPORT, or connect Claude via MCP (/mcp, 25 tools)."
run "GET /metrics | grep ecphoria_"
curl -s "$BASE/metrics" | grep -E '^ecphoria_(rest_requests_total|memory|publish)' | head -4 | sed 's/^/   /'

printf '\n\033[1;32m✓ Tour complete.\033[0m Durable, semantic, auditable, multi-tenant memory — and the runtime that runs agents on it.\n'
note "All local, real embeddings, zero external services. The temp data dir is discarded now."
