"""Ecphoria memory provider for Hermes Agent.

Hermes ships a deliberately small built-in memory — `MEMORY.md` (~2 200 characters) and
`USER.md` (~1 375), injected as a frozen snapshot at session start — and delegates anything
larger to a memory provider plugin. This is that plugin, backed by a self-hosted Ecphoria server.

What it adds over the built-in files, and over the cloud providers in the same slot:

* **Bi-temporal recall.** Every fact has `valid_from`/`valid_to` and a supersession chain, so
  "what did we believe in March" is a query rather than an archaeology exercise. Nothing is
  overwritten; a contradicting fact supersedes its predecessor and both stay readable.
* **Self-hosted.** One binary on your own infrastructure. No third-party service sees the
  contents of your agent's memory.
* **Shared across agents.** The same server backs Claude Code (over MCP), CI jobs and this
  plugin, so what one learns the others can recall.

Install::

    cp -r examples/hermes-memory-provider ~/.hermes/plugins/ecphoria
    hermes memory setup            # choose "ecphoria"

or configure directly in ``~/.hermes/config.yaml``::

    memory:
      provider: ecphoria

Environment: ``ECPHORIA_URL`` (default http://localhost:8432) and ``ECPHORIA_API_KEY``
(only needed when the server runs with ``gateway.auth_enabled``).
"""

from __future__ import annotations

import os
import threading
from typing import Any

import httpx

try:  # Present when running inside Hermes; absent when the file is imported for testing.
    from agent.memory_provider import MemoryProvider
except ImportError:  # pragma: no cover - exercised only outside Hermes
    class MemoryProvider:  # type: ignore[no-redef]
        """Stand-in so the module imports (and is testable) outside a Hermes install."""


DEFAULT_URL = "http://localhost:8432"
# Enough context to be useful, small enough not to crowd out the conversation itself.
PREFETCH_LIMIT = 6
# Hermes injects this before each turn; a slow memory server must not stall the agent.
HTTP_TIMEOUT = 5.0


class EcphoriaProvider(MemoryProvider):
    """Persist and recall Hermes' memory in a self-hosted Ecphoria server."""

    # ── identity & availability ────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "ecphoria"

    def is_available(self) -> bool:
        """Activation check. Hermes calls this synchronously during startup and the contract
        forbids network calls, so this only reports whether a URL has been configured — an
        unreachable server surfaces later as a degraded turn, not a failed boot."""
        return bool(os.environ.get("ECPHORIA_URL", DEFAULT_URL))

    # ── lifecycle ──────────────────────────────────────────────────────────

    def initialize(self, session_id: str, **kwargs: Any) -> None:
        self.session_id = session_id
        self.url = os.environ.get("ECPHORIA_URL", DEFAULT_URL).rstrip("/")
        self.user_id = os.environ.get("ECPHORIA_USER", "hermes")
        key = os.environ.get("ECPHORIA_API_KEY", "")
        headers = {"Authorization": f"Bearer {key}"} if key else {}
        self._http = httpx.Client(base_url=self.url, headers=headers, timeout=HTTP_TIMEOUT)
        self._prefetched: list[str] = []
        self._lock = threading.Lock()

    def shutdown(self) -> None:
        client = getattr(self, "_http", None)
        if client is not None:
            client.close()

    # ── recall ─────────────────────────────────────────────────────────────

    def prefetch(self, query: str = "", **kwargs: Any) -> str:
        """Return memories to inject into the prompt for this turn.

        Empty string on any failure rather than an exception: losing recall degrades the agent,
        raising here would stop it.
        """
        hits = self._search(query, PREFETCH_LIMIT)
        if not hits:
            return ""
        lines = [f"- {h}" for h in hits]
        return "Relevant memories from previous sessions:\n" + "\n".join(lines)

    def queue_prefetch(self, query: str = "", **kwargs: Any) -> None:
        """Warm the prefetch off the critical path. Hermes calls this early so `prefetch()` can
        return immediately when the turn is actually assembled."""
        self._in_background(self._do_queue_prefetch, query)

    def _do_queue_prefetch(self, query: str) -> None:
        hits = self._search(query, PREFETCH_LIMIT)
        with self._lock:
            self._prefetched = hits

    # ── capture ────────────────────────────────────────────────────────────

    def sync_turn(self, user_message: str = "", assistant_message: str = "", **kwargs: Any) -> None:
        """Record one exchange. MUST NOT block — Hermes' contract — so the write runs on a daemon
        thread and the agent proceeds regardless of how slow the server is."""
        if not user_message.strip():
            return
        self._in_background(self._do_sync_turn, user_message, assistant_message)

    def _do_sync_turn(self, user_message: str, assistant_message: str) -> None:
        self._post(
            "/api/v1/ingest",
            {
                "source": "hermes",
                "events": [
                    {
                        "event_type": "conversation.turn",
                        "_session_id": self.session_id,
                        "user": user_message,
                        "assistant": assistant_message,
                    }
                ],
            },
        )

    def on_session_end(self, **kwargs: Any) -> None:
        """Distil the session into durable facts.

        Server-side (`/sessions/{id}/distill`) rather than in the plugin: distillation needs the
        whole journalled session, and doing it here would mean shipping it back over the wire only
        to send the conclusions again.
        """
        self._post(f"/api/v1/sessions/{self.session_id}/distill", {})

    # ── tools ──────────────────────────────────────────────────────────────

    def get_tool_schemas(self) -> list[dict[str, Any]]:
        """Tools Hermes registers alongside its own.

        Deliberately three, not the server's full surface: an agent picks better from a short
        list, and everything else is reachable over MCP for callers that want it.
        """
        return [
            {
                "name": "ecphoria_remember",
                "description": (
                    "Remember a durable fact for future sessions. Give a `subject` when the fact "
                    "can change (e.g. 'deploy.target'): a later fact with the same subject "
                    "supersedes it, and the old one stays queryable as history."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "content": {"type": "string", "description": "The fact to remember."},
                        "subject": {
                            "type": "string",
                            "description": "Stable key this fact is about; enables supersession.",
                        },
                    },
                    "required": ["content"],
                },
            },
            {
                "name": "ecphoria_recall",
                "description": "Search remembered facts, documentation, tickets and incidents.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "k": {"type": "integer", "description": "How many results (default 5)."},
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "ecphoria_history",
                "description": (
                    "Show how a fact changed over time — every superseded version of a subject, "
                    "with the period each was believed."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {"subject": {"type": "string"}},
                    "required": ["subject"],
                },
            },
        ]

    def handle_tool_call(self, tool_name: str, args: dict[str, Any], **kwargs: Any) -> str:
        if tool_name == "ecphoria_remember":
            body: dict[str, Any] = {"content": args["content"], "user_id": self.user_id}
            if args.get("subject"):
                body["subject"] = args["subject"]
            res = self._post("/api/v1/memories", body)
            if res is None:
                return "Could not reach the memory server; the fact was not stored."
            return f"Remembered ({res.get('outcome', 'stored')})."

        if tool_name == "ecphoria_recall":
            hits = self._search(args["query"], int(args.get("k", 5)))
            return "\n".join(f"- {h}" for h in hits) if hits else "Nothing relevant remembered."

        if tool_name == "ecphoria_history":
            res = self._get(
                "/api/v1/memories/history",
                {"subject": args["subject"], "user_id": self.user_id},
            )
            versions = (res or {}).get("memories", [])
            if not versions:
                return f"No history for subject {args['subject']!r}."
            return "\n".join(
                f"- [{m.get('valid_from', '?')} → {m.get('valid_to') or 'now'}] {m.get('content', '')}"
                for m in versions
            )

        return f"Unknown tool {tool_name!r}."

    # ── configuration ──────────────────────────────────────────────────────

    def get_config_schema(self) -> list[dict[str, Any]]:
        return [
            {
                "key": "url",
                "description": "Ecphoria server URL",
                "secret": False,
                "required": False,
                "env_var": "ECPHORIA_URL",
            },
            {
                "key": "api_key",
                "description": "Ecphoria API key (only if the server has auth enabled)",
                "secret": True,
                "required": False,
                "env_var": "ECPHORIA_API_KEY",
            },
        ]

    def save_config(self, values: dict[str, Any], hermes_home: str) -> None:
        """Persist non-secret config. Secrets go to `.env` via the `secret: True` flag above and
        never reach this method."""
        path = os.path.join(hermes_home, "ecphoria.json")
        import json

        with open(path, "w", encoding="utf-8") as fh:
            json.dump({k: v for k, v in values.items() if k != "api_key"}, fh, indent=2)

    # ── HTTP helpers ───────────────────────────────────────────────────────
    #
    # Every call is best-effort. A memory server that is down, slow or misconfigured must degrade
    # the agent's recall, never break its turn — so failures are swallowed and reported as absence.

    def _search(self, query: str, k: int) -> list[str]:
        if not query.strip():
            with self._lock:
                return list(self._prefetched)
        res = self._post(
            "/api/v1/memories/search",
            {"query": query, "user_id": self.user_id, "k": k},
        )
        if not res:
            return []
        return [
            r["memory"]["content"]
            for r in res.get("results", [])
            if r.get("memory", {}).get("content")
        ]

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any] | None:
        try:
            resp = self._http.post(path, json=body)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any] | None:
        try:
            resp = self._http.get(path, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    @staticmethod
    def _in_background(fn: Any, *args: Any) -> None:
        threading.Thread(target=fn, args=args, daemon=True).start()


def register(ctx: Any) -> None:
    """Hermes plugin entry point."""
    ctx.register_memory_provider(EcphoriaProvider())
