#!/usr/bin/env bash
# Wire Ecphoria into Claude Code for daily use: MCP tools + session capture.
#
# Idempotent — safe to re-run. Pass --dry-run to see every change without making it.
#
#   ./ops/daily/setup.sh --dry-run
#   ./ops/daily/setup.sh
set -euo pipefail

URL="${ECPHORIA_URL:-http://localhost:8432}"
API_KEY="${ECPHORIA_API_KEY:-}"
SETTINGS="${CLAUDE_SETTINGS:-$HOME/.claude/settings.json}"
HOOK="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/session-capture.py"
DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

say() { printf '  %s\n' "$*"; }
step() { printf '\n\033[1m%s\033[0m\n' "$*"; }

step "1. Server"
if curl -sf --max-time 5 "$URL/health" >/dev/null 2>&1; then
  say "reachable at $URL"
else
  say "NOT reachable at $URL — start one first:"
  say ""
  say "    docker run -d --name ecphoria -p 5432:5432 -p 8432:8432 \\"
  say "      -v ecphoria-data:/data ghcr.io/vargafoundation/ecphoria:latest"
  say ""
  say "  or from a checkout:  cargo run --release --bin ecphoria-server"
  exit 1
fi

step "2. MCP tools in Claude Code"
if command -v claude >/dev/null 2>&1; then
  if claude mcp list 2>/dev/null | grep -q '^ecphoria'; then
    say "already registered"
  else
    args=(mcp add --transport http --scope user ecphoria "$URL/mcp")
    [[ -n "$API_KEY" ]] && args+=(--header "Authorization: Bearer $API_KEY")
    if (( DRY_RUN )); then
      say "would run: claude ${args[*]}"
    else
      claude "${args[@]}" && say "registered — 25 memory/graph/SQL tools now available in every session"
    fi
  fi
else
  say "the 'claude' CLI is not on PATH; add this to your MCP config by hand:"
  say "    { \"mcpServers\": { \"ecphoria\": { \"url\": \"$URL/mcp\" } } }"
fi

step "3. Session capture (SessionEnd hook)"
# Merge rather than overwrite: settings.json is shared with everything else the user configures.
python3 - "$SETTINGS" "$HOOK" "$DRY_RUN" <<'PY'
import json, os, sys

settings_path, hook_path, dry = sys.argv[1], sys.argv[2], sys.argv[3] == "1"
cmd = f"{hook_path}"

data = {}
if os.path.exists(settings_path):
    try:
        with open(settings_path, encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError:
        print(f"  {settings_path} is not valid JSON — refusing to touch it")
        sys.exit(1)

hooks = data.setdefault("hooks", {})
entries = hooks.setdefault("SessionEnd", [])
already = any(
    cmd in (h.get("command") or "")
    for entry in entries
    for h in (entry.get("hooks") or [])
)
if already:
    print("  already installed")
    sys.exit(0)

entries.append({"hooks": [{"type": "command", "command": cmd}]})
if dry:
    print(f"  would add to {settings_path}:")
    print(json.dumps({"hooks": {"SessionEnd": entries}}, indent=2)[:400])
    sys.exit(0)

os.makedirs(os.path.dirname(settings_path), exist_ok=True)
with open(settings_path, "w", encoding="utf-8") as fh:
    json.dump(data, fh, indent=2)
    fh.write("\n")
print(f"  installed in {settings_path}")
PY

step "4. Keep the documentation corpus fresh"
say "one-off:   ecphoria import --from git --path ."
say "live:      ecphoria import --from git --path . --watch"
say "in CI:     run the one-off on every merge to your default branch"

step "Done"
say "Try it: ask Claude Code \"what do we know about <something in your docs>?\""
say "Session capture journals turns only. To distil them into facts, configure a"
say "completion provider on the server and set ECPHORIA_CAPTURE_DISTILL=1."
