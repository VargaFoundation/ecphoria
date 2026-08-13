# Roadmap

Ecphoria is an open-source agentic memory platform. This roadmap is directional, not a
commitment — priorities shift with feedback. File an issue to propose or reprioritize.

## Versioning & stability

Ecphoria is **pre-1.0 (`0.x`)**: minor versions may contain breaking changes to the API,
config, wire/Raft formats, and on-disk layout. We call out breaking changes in the
release notes. **API stability (SemVer with a deprecation policy) begins at `1.0`.**

## Now (shipping on `main`)

- **Secure by default** — refuses to start unauthenticated on a public bind; hashed API
  keys; per-vendor webhook signatures; SSRF-guarded tool gateway; `ecphoria doctor`.
- **Memory substrate** — bi-temporal memories, contradiction resolution, dedup, hybrid
  retrieval (BM25 + vector), decay, knowledge graph.
- **Cognition APIs** — provenance, feedback loop, CDC stream, HITL contradiction review,
  session distillation, semantic-cluster consolidation, cross-scope sharing (tenant-strict grants).
- **Protocols** — PostgreSQL wire (+TLS), REST, gRPC, MCP (incl. graph tools),
  LLM proxy: OpenAI `/v1/chat/completions` + `/v1/embeddings`, Anthropic `/v1/messages`.
- **Runtime** — durable agent runs, HITL approvals, DAG workflows, triggers, dispatcher.
- **SQL over memories** — `SELECT … FROM memories` (incl. bi-temporal `valid_from`/`valid_to`)
  over PostgreSQL wire / REST / gRPC / MCP, tenant-scoped and read-only.
- **Multimodal** — attachments (image/PDF/audio) with image-similarity search; in-process image
  embedding (`embed-image`).
- **Graph analytics** — degree + PageRank centrality, community detection, shortest path, all with
  temporal (as-of) snapshots; interactive graph view in the console.
- **Embedded mode** — in-process library (`embedded::Ecphoria`, "the SQLite of agent memory") + a
  Python binding (pyo3).
- **In-process embeddings** (fastembed/ONNX, `embed-local`) so the single binary needs no sidecar.
- **Admin console** — memory browser, bi-temporal timeline, graph view, contradiction queue.
- **Engineering knowledge base** — a team's own documentation, ADRs, incidents and tickets as a
  queryable corpus. Markdown chunked on its heading hierarchy so an evolving document keeps history
  *per section*; an FTS5 inverted index so recall stays flat from 50 to 200k memories; per-project
  isolation that still fuses across projects. See `docs/knowledge-base.md`.
- **Import/export** — `git` (repo Markdown, commit dates as valid-time, `--watch`) and `github`
  (closed issues/PRs backfill); Obsidian round-trip (vault ↔ memories + graph edges), live
  `--watch` sync; Mem0/Zep importers.
- **Webhook → memory promotion** — merged PRs, closed issues and resolved incidents additionally
  become searchable memories, keyed so redelivery confirms rather than duplicates.
- **Observability** — OTLP trace export alongside Prometheus; outbound CDC sink.
- **Benchmarks** — reproducible LoCoMo baseline (`docs/benchmarks-locomo.md`) and a knowledge-base
  eval over this repository's own documentation (`docs/benchmarks-kb.md`), the latter gated in CI so
  a retrieval regression fails the build.
- **Ops** — Docker/Compose/Helm, Raft HA, sharding + operator, cosign/SBOM releases.

## Known limits (documented, not hidden)

- **Backfill must run oldest-first.** Out-of-order valid-time insertion would need interval
  splitting; replaying history in reverse pushes every version to now.
- **Vector dedup does not see within a batch** — contradiction resolution by subject does.
- **Cluster mode has no batched write path**: `/memories/batch` falls back to per-memory Raft
  replication. Correct, without the speedup.
- **Only Markdown is chunked.** PDFs and Office documents are stored as opaque attachments.
- **No absolute "the corpus has no answer" signal.** Hits carry their vector `similarity`, but
  measured on the reference corpus the answered (p50 0.70) and unanswerable (p50 0.60)
  distributions overlap, so `min_similarity` is a coarse floor rather than a test. Judging the
  returned content remains the reliable path.

### Upgrading

`project` became part of the contradiction key. A corpus imported before that has `project = NULL`,
so the first import after upgrading creates a parallel project-tagged set rather than superseding
the old one. Either re-import into a fresh store, or expire the untagged rows
(`UPDATE memories SET state='expired' WHERE project IS NULL AND mem_type='chunk'`) after checking
what they contain.

## Next (targeted)

- **Full client parity** — bring MCP, gRPC, and the Go/Python/TS SDKs + CLI up to the full REST
  surface (graph analytics, attachments, provenance/feedback/contradictions, templates, …).
- **Native multimodal embeddings** — CLIP/SigLIP image encoder behind the `ImageEmbeddingProvider`
  hook (the histogram embedder ships today; semantic image search is the upgrade).
- **Encryption at rest** — per-tenant envelope keys (KMS/age) for the on-disk stores.
- **ReBAC authz backend** — a pluggable policy backend (e.g. SpiceDB) on top of the grants
  primitive, for richer team/role-based sharing.
- **Registry publishing** — crates.io, the pyo3 wheel (PyPI), and `@ecphoria/client` (npm).

## Later / exploring

- Two-way **live Obsidian sync** as a native plugin (the CLI `--watch` importer ships today).
- Advanced consolidation ("sleep-time" episodic→semantic compression).

## Non-goals

- Being a general-purpose database — Ecphoria is a memory platform for agents.
- A hosted/managed offering in this repository (self-hosted first).
