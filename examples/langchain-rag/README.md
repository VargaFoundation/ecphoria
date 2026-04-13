# RAG Pipeline with LangChain + Strata

A complete Retrieval-Augmented Generation pipeline that ingests documents into Strata, retrieves relevant chunks via semantic search, and generates answers using an LLM.

## Architecture

```
                    ┌──────────────────────────────────────────┐
                    │                 Strata                    │
 Documents          │  ┌─────────────────┬──────────────────┐  │
 ─────────►ingest.py──►│ Episodic Store  │  Semantic Store   │  │
 (.md/.txt)         │  │ (DuckDB)        │  (USearch HNSW)   │  │
                    │  │ document.chunk  │  vector embeddings │  │
                    │  └────────┬────────┴────────┬─────────┘  │
                    │           │  embed-and-search│            │
                    └───────────┼──────────────────┼────────────┘
                                │                  │
                    ┌───────────┼──────────────────┼────────────┐
                    │ rag_chain.py                              │
                    │           │                  │            │
                    │  Question ▼    StrataRetriever▼           │
                    │  ─────► Prompt + Context ─────► LLM      │
                    │                                ─────►    │
                    │                                Answer    │
                    └──────────────────────────────────────────┘
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (to run Strata)
- Python 3.10+
- [Ollama](https://ollama.ai/) running locally (default) or an OpenAI API key

## Quick Start

**1. Start Strata with Ollama for embeddings**

```bash
docker run -d --name strata -p 8432:8432 -p 5432:5432 \
  -e STRATA_EMBEDDING__PROVIDER=ollama \
  -e STRATA_EMBEDDING__OLLAMA_URL=http://host.docker.internal:11434 \
  ghcr.io/varga-foundation/strata:latest
```

**2. Install dependencies**

```bash
cd examples/langchain-rag
pip install -r requirements.txt
```

**3. Ingest the sample documents**

```bash
python ingest.py
```

```
Ingesting 3 files from sample_docs into Strata (http://localhost:8432)

  product-faq.md: 6 chunks ingested
  troubleshooting.md: 7 chunks ingested
  changelog.md: 5 chunks ingested

Done — 18 total chunks ingested from 3 files.
```

**4. Run the RAG chain**

```bash
python rag_chain.py
```

```
RAG pipeline ready (LLM: ollama, Strata: http://localhost:8432)
Type a question (or 'quit' to exit).

Question: What are the API rate limits for the Pro plan?

Sources:
  [1] product-faq.md (score: 0.91) — API rate limits depend on your plan: ...
  [2] changelog.md (score: 0.74) — API rate limit dashboard: Real-time vis...

Answer: The Pro plan has a rate limit of 10,000 requests per day and 50
requests per second. Rate limit headers (X-RateLimit-Remaining and
X-RateLimit-Reset) are included in every API response.
```

## How Embed-and-Search Works

The `StrataRetriever` calls Strata's `/api/v1/embed-and-search` endpoint, which:

1. **Embeds** your query text using the configured provider (Ollama or OpenAI)
2. **Searches** the USearch HNSW index for the k-nearest vectors
3. **Returns** matching documents with content, metadata, and cosine similarity scores

This single API call replaces the typical "embed query → call vector DB → fetch metadata" chain.

## Using OpenAI Instead of Ollama

```bash
export LLM_PROVIDER=openai
export OPENAI_API_KEY=sk-...
python rag_chain.py
```

For OpenAI embeddings on the Strata side:

```bash
docker run -d --name strata -p 8432:8432 -p 5432:5432 \
  -e STRATA_EMBEDDING__PROVIDER=openai \
  -e STRATA_EMBEDDING__OPENAI_API_KEY=sk-... \
  ghcr.io/varga-foundation/strata:latest
```

## Customization

| What | How |
|------|-----|
| Change number of results | `StrataRetriever(k=10)` |
| Filter by source | `StrataRetriever(source_filter="my-docs")` |
| Ingest custom directory | `python ingest.py /path/to/docs` |
| Change LLM model | `OLLAMA_MODEL=mistral python rag_chain.py` |
| Change Strata URL | `STRATA_URL=http://my-server:8432 python rag_chain.py` |

## Ingest Your Own Documents

```bash
# Any directory of .txt and .md files
python ingest.py /path/to/your/documents

# Then query them
python rag_chain.py
```

The ingestion script splits files into paragraph-based chunks and stores each chunk as an episodic event with metadata (filename, chunk index). Strata auto-embeds them for semantic search.
