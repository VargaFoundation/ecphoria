# Changelog

All notable changes to Ecphoria are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); the project adheres to SemVer from `1.0` onward
(pre-`1.0`, minor versions may include breaking changes — see `ROADMAP.md`). Per-release notes are
also generated from Conventional Commits via `cliff.toml`.

## [Unreleased]

### Security
- Attachment download is hardened against stored-XSS: untrusted content-types are served as a
  non-executing `octet-stream` attachment, always with `nosniff` + a locked-down CSP.

### Added
- **SQL over memories** — `SELECT … FROM memories` (with bi-temporal `valid_from`/`valid_to`) over
  PostgreSQL wire / REST / gRPC / MCP, tenant-scoped and read-only.
- **Multimodal** — attachments (image/PDF/audio) with image-similarity search; in-process image
  embedding behind the `embed-image` feature.
- **Graph analytics** — degree + PageRank centrality, community detection, shortest path, all with
  temporal (as-of) snapshots; interactive graph view in the console.
- **Embedded library mode** (`embedded::Ecphoria`) + a **Python binding** (pyo3).
- **Public read-only publish** (`/public`, opt-in) and **memory templates**.
- **Obsidian round-trip** — vault import/export + live `--watch` sync; **Mem0/Zep importers**.
- **Outbound CDC sink**, **OTLP trace export**, versioned re-embedding, pluggable `AuthzBackend`.

### Fixed
- `/public` no longer drops published memories beyond the result cap (SQL-side filter); added a
  short-TTL cache to bound the unauthenticated scan.
- The event/RAG semantic index is now persisted periodically and on shutdown (was lost on exit for
  file-backed servers and embedded/Python users).
- `TIMESTAMPTZ` columns now serialize as RFC3339 in SQL results (were `null`).

### Changed
- Canonical positioning is now **"the open-source agentic memory platform"** across README, code,
  SDKs, and docs.

## [0.1.0]

Initial public foundation: bi-temporal cognition memory (dedup, contradiction resolution, hybrid
BM25+vector retrieval, decay, knowledge graph); episodic/semantic/state stores; PostgreSQL wire +
REST + gRPC + MCP + LLM proxy; durable agent runtime (runs, HITL, DAG workflows, triggers);
Raft-based HA with sharding + a Kubernetes operator; secure-by-default posture; Docker/Compose/Helm;
cosign/SBOM releases.
