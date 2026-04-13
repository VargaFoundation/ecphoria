"""CrewAI + Strata: Using Strata as a persistent memory backend for AI agent crews.

This example demonstrates how CrewAI agents can use Strata for:
- Episodic memory (ingesting and querying events)
- Semantic memory (similarity search over past events)
- State memory (persisting agent state across runs)
"""

import asyncio

from crewai import Agent, Crew, Task
from strata_tools import StrataIngestTool, StrataQueryTool, StrataSearchTool, StrataStateTool

STRATA_URL = "http://localhost:8432"

# ── Sample data ──────────────────────────────────────────────────

SAMPLE_INCIDENTS = [
    {
        "event_type": "incident",
        "severity": "high",
        "service": "payments",
        "title": "Payment processing timeout",
        "description": "Stripe webhook handler exceeded 30s timeout during peak traffic. "
        "Root cause: N+1 query in order validation. Fixed by batching DB lookups.",
    },
    {
        "event_type": "incident",
        "severity": "critical",
        "service": "auth",
        "title": "Auth service OOM crash",
        "description": "Auth service ran out of memory due to unbounded session cache. "
        "Affected 15% of login attempts for 12 minutes. Fixed with LRU eviction.",
    },
    {
        "event_type": "incident",
        "severity": "medium",
        "service": "search",
        "title": "Search index lag",
        "description": "Elasticsearch replication lag caused stale search results for 45 minutes. "
        "Root cause: large bulk indexing job saturated I/O. Fixed with rate limiting.",
    },
    {
        "event_type": "incident",
        "severity": "high",
        "service": "payments",
        "title": "Double charge bug",
        "description": "Race condition in retry logic caused double charges for 23 customers. "
        "Root cause: missing idempotency key check. Fixed with Redis-based dedup.",
    },
]


async def seed_strata() -> None:
    """Ingest sample incidents into Strata."""
    from strata import StrataClient

    async with StrataClient(STRATA_URL) as client:
        count = await client.ingest("incidents", SAMPLE_INCIDENTS)
        print(f"Seeded {count} incidents into Strata")


# ── CrewAI agents and tasks ──────────────────────────────────────

def build_crew() -> Crew:
    # Tools backed by Strata
    search_tool = StrataSearchTool(url=STRATA_URL)
    query_tool = StrataQueryTool(url=STRATA_URL)
    state_tool = StrataStateTool(url=STRATA_URL)
    ingest_tool = StrataIngestTool(url=STRATA_URL)

    # Agent: Incident Researcher
    researcher = Agent(
        role="Incident Researcher",
        goal="Find relevant past incidents using semantic search over Strata memory",
        backstory="You are an SRE who searches through incident history to find patterns.",
        tools=[search_tool],
        verbose=True,
    )

    # Agent: Pattern Analyst
    analyst = Agent(
        role="Pattern Analyst",
        goal="Analyze incident patterns using SQL queries against Strata's episodic store",
        backstory="You are a data analyst who identifies trends in incident data.",
        tools=[query_tool, state_tool],
        verbose=True,
    )

    # Agent: Report Writer
    reporter = Agent(
        role="Report Writer",
        goal="Write a concise incident summary report and save findings back to Strata",
        backstory="You compile findings from researchers and analysts into actionable reports.",
        tools=[ingest_tool, state_tool],
        verbose=True,
    )

    # Tasks
    research_task = Task(
        description=(
            "Search Strata for incidents related to 'payment failures and timeouts'. "
            "Use the search tool to find semantically similar past incidents. "
            "Summarize the top findings."
        ),
        expected_output="A list of relevant past incidents with summaries",
        agent=researcher,
    )

    analysis_task = Task(
        description=(
            "Query Strata to count incidents by severity and service. "
            "Use SQL: SELECT source, event_type, COUNT(*) as cnt FROM episodic GROUP BY source, event_type. "
            "Save your analysis state using the state tool with agent_id='analyst'."
        ),
        expected_output="Statistical breakdown of incidents by severity and service",
        agent=analyst,
    )

    report_task = Task(
        description=(
            "Based on the research and analysis, write a brief incident trend report. "
            "Ingest the report as a new event with event_type='report' into Strata. "
            "Save the report timestamp to state with agent_id='reporter', key='last_report'."
        ),
        expected_output="A written incident trend report",
        agent=reporter,
        context=[research_task, analysis_task],
    )

    return Crew(
        agents=[researcher, analyst, reporter],
        tasks=[research_task, analysis_task, report_task],
        verbose=True,
    )


def main() -> None:
    # Seed data
    asyncio.run(seed_strata())

    # Run the crew
    crew = build_crew()
    result = crew.kickoff()
    print("\n" + "=" * 60)
    print("CREW RESULT:")
    print("=" * 60)
    print(result)


if __name__ == "__main__":
    main()
