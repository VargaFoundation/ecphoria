"""Strata client — async HTTP client for the Strata context lake API."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Optional

import httpx


class StrataClient:
    """Async client for the Strata context lake REST API.

    Usage::

        async with StrataClient("http://localhost:8432") as client:
            # Ingest events
            count = await client.ingest("my-app", [
                {"event_type": "user.signup", "user_id": "u1"},
            ])

            # Query with SQL
            rows = await client.query("SELECT * FROM episodic LIMIT 10")

            # Semantic search
            results = await client.search(vector=[0.1, 0.2, ...], k=5)

            # Agent state
            await client.state_set("bot-1", "mood", "happy")
            entry = await client.state_get("bot-1", "mood")
    """

    def __init__(
        self,
        url: str = "http://localhost:8432",
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.url = url.rstrip("/")
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._client = httpx.AsyncClient(
            base_url=self.url,
            headers=headers,
            timeout=timeout,
        )

    async def __aenter__(self) -> "StrataClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    # ── Health ───────────────────────────────────────────────────────

    async def health(self) -> dict[str, Any]:
        """Check server health."""
        resp = await self._client.get("/health")
        resp.raise_for_status()
        return resp.json()

    # ── Query ────────────────────────────────────────────────────────

    async def query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL query against the episodic store.

        Returns a list of row dicts.
        """
        resp = await self._client.post("/api/v1/query", json={"sql": sql})
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("rows", [])

    # ── Ingest ───────────────────────────────────────────────────────

    async def ingest(
        self,
        source: str,
        events: list[dict[str, Any]],
    ) -> int:
        """Ingest events into episodic memory.

        Returns the number of events ingested.
        """
        resp = await self._client.post(
            "/api/v1/ingest",
            json={"source": source, "events": events},
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("ingested", 0)

    # ── Search ───────────────────────────────────────────────────────

    async def search(
        self,
        vector: list[float],
        k: int = 5,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Semantic search by pre-computed vector.

        For text-based search, use `find()` instead.
        """
        body: dict[str, Any] = {"vector": vector, "k": k}
        filters = {}
        if source:
            filters["source"] = source
        if event_type:
            filters["event_type"] = event_type
        if filters:
            body["filters"] = filters

        resp = await self._client.post("/api/v1/search", json=body)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("results", [])

    async def find(
        self,
        text: str,
        k: int = 5,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Semantic search by natural language text (embed + search in one call).

        This is the recommended search method. Strata embeds the text
        using the configured provider and searches the vector index.

        Usage::

            results = await client.find("frustrated customer billing issue", k=5)
        """
        body: dict[str, Any] = {"text": text, "k": k}
        filters = {}
        if source:
            filters["source"] = source
        if event_type:
            filters["event_type"] = event_type
        if filters:
            body["filters"] = filters

        resp = await self._client.post("/api/v1/embed-and-search", json=body)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("results", [])

    # ── Query Builder ────────────────────────────────────────────────

    async def events(
        self,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        order: str = "DESC",
    ) -> list[dict[str, Any]]:
        """Query episodic events with a fluent API (no raw SQL needed).

        Usage::

            # Get last 10 events from 'my-app'
            events = await client.events(source="my-app", limit=10)

            # Get all 'user.signup' events
            signups = await client.events(event_type="user.signup")
        """
        conditions = []
        if source:
            conditions.append(f"source = '{source}'")
        if event_type:
            conditions.append(f"event_type = '{event_type}'")

        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM episodic{where} ORDER BY ts {order} LIMIT {limit}"
        return await self.query(sql)

    async def sources(self) -> list[str]:
        """List all event sources."""
        resp = await self._client.get("/api/v1/schema/sources")
        resp.raise_for_status()
        return resp.json().get("sources", [])

    async def agents(self) -> list[str]:
        """List all agent IDs."""
        resp = await self._client.get("/api/v1/schema/agents")
        resp.raise_for_status()
        return resp.json().get("agents", [])

    # ── State ────────────────────────────────────────────────────────

    async def state_get(
        self, agent_id: str, key: str
    ) -> Optional[dict[str, Any]]:
        """Get agent state. Returns None if not found."""
        resp = await self._client.get(f"/api/v1/state/{agent_id}/{key}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            return None
        return data

    async def state_set(
        self, agent_id: str, key: str, value: Any
    ) -> int:
        """Set agent state. Returns the new version number."""
        resp = await self._client.put(
            f"/api/v1/state/{agent_id}/{key}",
            json=value,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("version", 0)

    async def state_delete(self, agent_id: str, key: str) -> None:
        """Delete agent state."""
        resp = await self._client.request(
            "DELETE", f"/api/v1/state/{agent_id}/{key}"
        )
        resp.raise_for_status()

    # ── Admin ────────────────────────────────────────────────────────

    async def backup(self) -> dict[str, Any]:
        """Trigger a backup of all stores."""
        resp = await self._client.post("/api/v1/admin/backup")
        resp.raise_for_status()
        return resp.json()

    async def enforce_retention(self) -> dict[str, Any]:
        """Enforce data retention policy."""
        resp = await self._client.post("/api/v1/admin/retention")
        resp.raise_for_status()
        return resp.json()

    # ── Cluster ──────────────────────────────────────────────────────

    async def cluster_status(self) -> dict[str, Any]:
        """Get Raft cluster status."""
        resp = await self._client.get("/cluster/status")
        resp.raise_for_status()
        return resp.json()

    # ── WebSocket Watcher ────────────────────────────────────────────

    async def watch_state(
        self, agent_id: str
    ) -> AsyncIterator[dict[str, Any]]:
        """Watch state changes for an agent via WebSocket.

        Yields StateChange dicts as they occur.

        Usage::

            async for change in client.watch_state("bot-1"):
                print(f"{change['key']} = {change['value']}")
        """
        import json
        import websockets

        ws_url = self.url.replace("http://", "ws://").replace(
            "https://", "wss://"
        )
        uri = f"{ws_url}/api/v1/state/{agent_id}/watch"

        async with websockets.connect(uri) as ws:
            async for message in ws:
                yield json.loads(message)


class StrataError(Exception):
    """Error returned by the Strata API."""

    def __init__(self, error: Any) -> None:
        if isinstance(error, dict):
            self.code = error.get("code", "UNKNOWN")
            self.message = error.get("message", str(error))
            self.request_id = error.get("request_id")
            super().__init__(self.message)
        else:
            self.code = "UNKNOWN"
            self.message = str(error)
            self.request_id = None
            super().__init__(self.message)
