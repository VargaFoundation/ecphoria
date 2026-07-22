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
- **Import/export** — Obsidian round-trip (vault ↔ memories + graph edges), live `--watch` sync;
  Mem0/Zep importers.
- **Observability** — OTLP trace export alongside Prometheus; outbound CDC sink.
- **Benchmarks** — reproducible LoCoMo baseline with the exact recipe (`docs/benchmarks-locomo.md`).
- **Ops** — Docker/Compose/Helm, Raft HA, sharding + operator, cosign/SBOM releases.

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
