# CrewAI with Strata Memory Backend

This example shows how to use [Strata](https://github.com/VargaFoundation/strata) as a persistent memory backend for [CrewAI](https://github.com/crewAIInc/crewAI) agents.

Strata provides episodic, semantic, and state memory — giving your crew long-term recall across runs.

## Prerequisites

1. A running Strata server:
   ```bash
   docker run -p 8432:8432 ghcr.io/vargafoundation/strata:latest
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## What This Example Does

- **Ingest phase**: Loads sample incident reports into Strata's episodic memory
- **Research agent**: Queries Strata to find relevant past incidents using semantic search
- **Analyst agent**: Analyzes patterns across incidents using SQL queries
- **Reporter agent**: Writes a summary report using state memory to track progress

## Run

```bash
python main.py
```

## Architecture

```
CrewAI Agents
    │
    ├── strata.find()       → semantic search over past events
    ├── strata.query()      → SQL analytics on episodic store
    ├── strata.state_set()  → persist agent state across runs
    └── strata.ingest()     → log new findings back to memory
```

## Key Concepts

- **Episodic memory** (events): Raw incident reports, log entries, and agent observations
- **Semantic memory** (vectors): Strata auto-embeds ingested events for similarity search
- **State memory** (KV): Agent-specific state (e.g., last analysis timestamp, running tallies)
