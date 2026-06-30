"""Framework-agnostic agent tools backed by Strata.

These are plain async callables you can register with any agent framework's function-tool
mechanism — the OpenAI Agents SDK (``@function_tool``), Pydantic AI (``Tool``), LangChain
(``StructuredTool``), CrewAI, etc. Each takes a :class:`~strata.client.StrataClient`; bind it with
:func:`memory_toolset` or ``functools.partial``.

Example — OpenAI Agents SDK::

    from agents import function_tool
    from strata import StrataClient
    from strata.tools import search_memory, remember

    client = StrataClient("http://localhost:8432")

    @function_tool
    async def memory_search(query: str) -> list:
        '''Search the user's long-term memory.'''
        return await search_memory(client, query)

Example — Pydantic AI::

    from pydantic_ai import Agent, Tool
    from strata import StrataClient
    from strata.tools import memory_toolset

    tools = memory_toolset(StrataClient("http://localhost:8432"))
    agent = Agent("openai:gpt-4o", tools=[Tool(tools["search_memory"]), Tool(tools["remember"])])
"""

from __future__ import annotations

import functools
from typing import Any, Callable, Optional

from .client import StrataClient


async def search_memory(
    client: StrataClient,
    query: str,
    *,
    user_id: Optional[str] = None,
    k: int = 5,
) -> list[dict[str, Any]]:
    """Search the agent's long-term memory for facts relevant to ``query`` (hybrid retrieval)."""
    return await client.memory_search(query, k=k, user_id=user_id)


async def remember(
    client: StrataClient,
    content: str,
    *,
    user_id: Optional[str] = None,
    subject: Optional[str] = None,
) -> dict[str, Any]:
    """Store a fact in long-term memory. Deduped; a newer fact about the same ``subject``
    supersedes the old one (kept as history)."""
    return await client.memory_add(content, user_id=user_id, subject=subject)


async def run_subagent(
    client: StrataClient, agent_id: str, question: str
) -> dict[str, Any]:
    """Delegate a question to a durable Strata sub-agent; returns its run (status + result)."""
    return await client.run_agent(agent_id, question)


def memory_toolset(client: StrataClient) -> dict[str, Callable[..., Any]]:
    """Bind the tools to a client, returning ``{name: async_callable}`` — framework-agnostic."""
    return {
        "search_memory": functools.partial(search_memory, client),
        "remember": functools.partial(remember, client),
        "run_subagent": functools.partial(run_subagent, client),
    }
