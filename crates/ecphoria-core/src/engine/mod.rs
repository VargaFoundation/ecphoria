use std::path::Path;
use std::sync::Arc;

use crate::config::CoreConfig;
use crate::embedding::ollama::OllamaProvider;
use crate::embedding::openai::OpenAiProvider;
use crate::embedding::EmbeddingProvider;
use crate::ingest::IngestPipeline;
use crate::llm::CompletionProvider;
use crate::memory::cognition::{
    Memory, MemoryAdd, MemoryHit, MemoryInput, MemoryOutcome, MemoryRow, MemoryScope, MemoryState,
    MemoryStore,
};
use crate::memory::episodic::{EpisodicStore, Event};
use crate::memory::lexical::LexicalIndex;
use crate::memory::query_log::{QueryLogger, QueryRecord};
use crate::memory::semantic::{ScopedVectorIndex, SearchResult, SemanticEntry, SemanticStore};
use crate::memory::state::StateStore;
use crate::query::{QueryExecutor, QueryPlanner};
use crate::rerank::Reranker;
#[cfg(feature = "agentic")]
use crate::runtime::{RunReplicator, RunStore, ToolExecutor};
use crate::Result;

/// One document to ingest (see [`EcphoriaEngine::document_ingest`]).
#[derive(Debug, Clone, Default)]
pub struct DocumentIngest<'a> {
    /// Stable identity — normally `<project>/<repo-relative path>`. Drives supersession *and* the
    /// removal sweep, so two documents must never share one.
    pub path: &'a str,
    /// Raw Markdown.
    pub content: &'a str,
    /// Attached to every section.
    pub metadata: serde_json::Value,
    /// Valid-time for this version, e.g. the source commit date. Defaults to now.
    pub valid_from: Option<chrono::DateTime<chrono::Utc>>,
    /// Project this document belongs to, so a search can narrow to it.
    pub project: Option<&'a str>,
}

/// What one document re-import changed (see [`EcphoriaEngine::document_ingest`]).
///
/// The counts are the point of the API: on a steady-state re-import of an unchanged corpus every
/// section should be `confirmed` and nothing else, which is how a `--watch` loop stays cheap and
/// how a caller can tell whether a document actually moved.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DocumentIngestResult {
    /// Repo-relative path the document is addressed by.
    pub path: String,
    /// Sections the document split into.
    pub chunks: usize,
    /// Sections that did not exist before.
    pub inserted: usize,
    /// Sections whose text changed — the previous version is kept in history.
    pub superseded: usize,
    /// Sections that were byte-identical to the stored version.
    pub confirmed: usize,
    /// Sections that vanished from the source and were expired.
    pub removed: usize,
}

/// Provenance for a memory — the evidence chain behind a distilled fact (see
/// [`EcphoriaEngine::memory_provenance`]).
#[derive(Debug, Clone, serde::Serialize)]
pub struct MemoryProvenance {
    /// The memory being explained.
    pub memory: Memory,
    /// The episodic events it was distilled from (resolved from `source_event_ids`).
    pub source_events: Vec<Event>,
    /// The bi-temporal supersession chain (every version, oldest first) for a subject-keyed
    /// memory; a single-element list otherwise.
    pub history: Vec<Memory>,
}

/// A group of conflicting active memories for one subject, awaiting human resolution (HITL
/// contradiction review — see [`EcphoriaEngine::memory_contradictions`]).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ContradictionGroup {
    pub subject: String,
    pub memories: Vec<Memory>,
}

/// A memory lifecycle change emitted on the CDC stream (see [`EcphoriaEngine::memory_subscribe`]).
#[derive(Debug, Clone, serde::Serialize)]
pub struct MemoryChange {
    pub id: uuid::Uuid,
    pub tenant_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    /// Lifecycle event: `"upserted"` (created or reinforced/merged, now active), `"superseded"`, or
    /// `"expired"`.
    pub event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
}

/// A caller's verdict on a retrieved memory — the feedback loop that lets ranking learn without an
/// LLM (see [`EcphoriaEngine::memory_feedback_plan`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryFeedback {
    /// The memory was useful → reinforce it (importance up).
    Helpful,
    /// The memory was factually wrong → retire it.
    Wrong,
    /// The memory is stale/no-longer-true → retire it.
    Obsolete,
}

impl MemoryFeedback {
    /// Parse a verdict string (case-insensitive).
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "helpful" | "useful" | "up" => Some(Self::Helpful),
            "wrong" | "incorrect" | "down" => Some(Self::Wrong),
            "obsolete" | "stale" | "outdated" => Some(Self::Obsolete),
            _ => None,
        }
    }
}

/// The materialized effect of feedback, so cluster mode replicates a deterministic change.
#[derive(Debug, Clone)]
pub enum FeedbackAction {
    /// Reinforce: (re)persist these rows with bumped importance.
    Reinforce(Vec<MemoryRow>),
    /// Retire: bi-temporally expire these memory ids.
    Retire(Vec<uuid::Uuid>),
}

/// Top-level engine that owns all subsystems of the Ecphoria context lake.
pub struct EcphoriaEngine {
    config: CoreConfig,
    episodic: Arc<EpisodicStore>,
    semantic: Arc<SemanticStore>,
    state: Arc<StateStore>,
    /// Bi-temporal store of distilled memories (cognition layer).
    memory_store: Arc<MemoryStore>,
    /// Vector index over memories only (kept separate from event embeddings).
    memory_index: Arc<ScopedVectorIndex>,
    /// Inverted (FTS5) index over memories — the lexical arm of hybrid retrieval. Advisory: it
    /// yields candidate ids that are re-read from `memory_store` under the active/scope filter.
    lexical_index: Arc<LexicalIndex>,
    /// Records searches so retrieval quality can be measured against real questions rather than
    /// hand-written ones. `None` unless `memory.query_log.enabled`.
    query_log: Option<QueryLogger>,
    /// Per-modality vector indexes (mixed-dimension multi-modal embeddings).
    modal: crate::memory::semantic::MultiModalStore,
    ingest: IngestPipeline,
    /// Shared embedding provider for embed-and-search operations.
    embedding: Option<Arc<dyn EmbeddingProvider>>,
    /// Optional completion provider for opt-in LLM fact extraction (cognition layer).
    completion: Option<Arc<dyn CompletionProvider>>,
    /// Optional second-stage reranker applied to `memory_search` results (read-path only).
    reranker: Option<Arc<dyn Reranker>>,
    /// Durable agent-run ledger (agentic-platform substrate).
    #[cfg(feature = "agentic")]
    runs: Arc<RunStore>,
    /// Unique id for THIS engine instance — the owner of agent-run driver leases (concurrency guard).
    #[cfg(feature = "agentic")]
    driver_id: String,
    /// Broadcast of memory lifecycle changes (created/superseded/expired) — the CDC stream that
    /// lets clients build reactive UIs / integrations without polling.
    memory_change_tx: tokio::sync::broadcast::Sender<MemoryChange>,
    /// Optional executor for external tools (e.g. downstream MCP servers), injected by the gateway.
    #[cfg(feature = "agentic")]
    tool_executor: parking_lot::RwLock<Option<Arc<dyn ToolExecutor>>>,
    /// Optional replicator routing run-ledger writes through Raft (cluster mode), injected by the
    /// server. Absent → the agent driver writes runs/steps locally.
    #[cfg(feature = "agentic")]
    run_replicator: parking_lot::RwLock<Option<Arc<dyn RunReplicator>>>,
    /// Cross-scope read authorization backend. Defaults to `LocalGrants` (the tenant-strict
    /// `memory_grants` table); swappable for a richer/external policy engine via
    /// [`Self::set_authz_backend`].
    authz: parking_lot::RwLock<Arc<dyn crate::authz::AuthzBackend>>,
    /// Storage backend for multimodal attachment blobs (local dir or S3, per `storage.engine`).
    attachments: Arc<dyn crate::storage::StorageBackend>,
    /// Optional image-embedding backend (multimodal). When set, `image/*` attachments are embedded
    /// and indexed for image search. Injected via [`Self::set_image_embedding`].
    image_embedding: parking_lot::RwLock<Option<Arc<dyn crate::embedding::ImageEmbeddingProvider>>>,
}

impl std::fmt::Debug for EcphoriaEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EcphoriaEngine")
            .field("has_embedding", &self.embedding.is_some())
            .finish()
    }
}

impl EcphoriaEngine {
    /// Create and initialize a new Ecphoria engine.
    pub async fn new(config: CoreConfig) -> Result<Self> {
        // Initialize episodic store (file-backed or in-memory DuckDB)
        let episodic_path = Path::new(&config.memory.episodic.db_path);
        let episodic = Arc::new(
            EpisodicStore::open(episodic_path, config.memory.episodic.read_pool_size)
                .unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "falling back to in-memory episodic store");
                    EpisodicStore::new()
                }),
        );
        if config.memory.episodic.db_path != ":memory:" {
            tracing::info!(path = %config.memory.episodic.db_path, "episodic store: file-backed");
        }

        // Initialize the event vector index. In file-backed (persistent) mode, reload the index that
        // was saved on the last shutdown/backup so semantic search over pre-restart events survives a
        // restart. Previously it was created empty and never reloaded, and because indexed events are
        // flagged `embedded=true` the reindex path skipped them — so every pre-restart event silently
        // dropped out of semantic/RAG search until re-ingested.
        let semantic = Arc::new({
            let index_dir = &config.memory.semantic.index_dir;
            let reloaded = if config.memory.episodic.db_path != ":memory:"
                && !index_dir.is_empty()
                && index_dir != ":memory:"
            {
                SemanticStore::load(std::path::Path::new(index_dir)).ok()
            } else {
                None
            };
            match reloaded {
                Some(s) => {
                    tracing::info!(entries = s.len(), dir = %index_dir, "reloaded event vector index from disk");
                    s
                }
                None => SemanticStore::with_dimension(config.embedding.dimension)
                    .unwrap_or_else(|_| SemanticStore::new()),
            }
        });

        // Initialize state store
        let state_path = Path::new(&config.memory.state.db_path);
        let state = Arc::new(StateStore::open(state_path).unwrap_or_else(|_| {
            tracing::warn!("falling back to in-memory state store");
            StateStore::new()
        }));

        // Initialize memory-cognition store (bi-temporal facts) + its dedicated vector index
        let memory_path = Path::new(&config.memory.cognition.db_path);
        let memory_store = Arc::new(
            MemoryStore::open(memory_path, config.memory.cognition.read_pool_size).unwrap_or_else(
                |e| {
                    tracing::warn!(error = %e, "falling back to in-memory cognition store");
                    MemoryStore::new()
                },
            ),
        );
        // Vector index for memories, partitioned by exact scope so per-scope recall isn't starved
        // by a global top-K post-filter (see [`ScopedVectorIndex`]).
        let memory_index = Arc::new(ScopedVectorIndex::with_dimension(
            config.embedding.dimension,
        ));
        // Rebuild the in-memory vector index from persisted embeddings (no provider call).
        match memory_store.load_active_with_embeddings().await {
            Ok(rows) => {
                let n = rows.len();
                for (mem, emb) in rows {
                    let key = crate::memory::cognition::scope_partition_key(&mem.scope);
                    let _ = memory_index.upsert(&key, &mem.to_semantic_entry(emb)).await;
                }
                if n > 0 {
                    tracing::info!(memories = n, "rebuilt memory vector index from disk");
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to rebuild memory index"),
        }

        // Inverted index for the lexical arm. Lives beside the cognition DuckDB file (or in
        // memory when that is), and is rebuilt from it — DuckDB stays the single source of truth,
        // so a deleted or corrupt index file costs a rebuild, never data.
        let lexical_index = Arc::new(
            LexicalIndex::open(&Self::lexical_index_path(&config.memory.cognition.db_path))
                .unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "falling back to in-memory lexical index");
                    LexicalIndex::in_memory().expect("in-memory lexical index")
                }),
        );
        if let Err(e) = Self::rebuild_lexical_index(&memory_store, &lexical_index).await {
            tracing::warn!(error = %e, "failed to rebuild lexical index");
        }

        // Search telemetry. Off unless configured: a query is text a human typed.
        let query_log = config.memory.query_log.enabled.then(|| {
            tracing::info!(
                include_query = config.memory.query_log.include_query,
                "query log enabled — searches are recorded under source '{}'",
                crate::memory::query_log::QUERY_LOG_SOURCE
            );
            QueryLogger::spawn(episodic.clone(), config.memory.query_log.include_query)
        });

        // Initialize embedding provider from config
        let embedding: Option<Arc<dyn EmbeddingProvider>> = match config.embedding.provider.as_str()
        {
            "ollama" => {
                let (qp, dp) = config.embedding.resolved_prefixes();
                tracing::info!(
                    model = %config.embedding.model,
                    url = %config.embedding.ollama_url,
                    query_prefix = %qp,
                    document_prefix = %dp,
                    "embedding provider: ollama"
                );
                Some(Arc::new(
                    OllamaProvider::new(
                        config.embedding.ollama_url.clone(),
                        config.embedding.model.clone(),
                        config.embedding.dimension,
                    )
                    .with_prefixes(qp, dp),
                ))
            }
            // `openai_compatible` is the same wire protocol pointed at someone else's endpoint
            // — a LiteLLM proxy, vLLM, an Ollama shim — where the key may be a gateway key or
            // nothing at all. `openai` keeps requiring a key, because api.openai.com does.
            "openai" | "openai_compatible" | "openai-compatible"
                if !config.embedding.openai_api_key.is_empty()
                    || config.embedding.provider != "openai" =>
            {
                let (qp, dp) = config.embedding.resolved_prefixes();
                tracing::info!(
                    model = %config.embedding.model,
                    base_url = %config.embedding.openai_base_url,
                    "embedding provider: openai-compatible"
                );
                Some(Arc::new(
                    OpenAiProvider::new(
                        config.embedding.openai_api_key.clone(),
                        config.embedding.model.clone(),
                        config.embedding.dimension,
                    )
                    .with_base_url(config.embedding.openai_base_url.clone())
                    .with_prefixes(qp, dp),
                ))
            }
            // In-process ONNX embeddings (no sidecar) — requires the `embed-local` build feature.
            "local" | "fastembed" => {
                #[cfg(feature = "embed-local")]
                {
                    let (qp, dp) = config.embedding.resolved_prefixes();
                    match crate::embedding::local::FastEmbedProvider::new(
                        &config.embedding.model,
                        qp,
                        dp,
                    ) {
                        Ok(p) => {
                            tracing::info!(
                                model = %config.embedding.model,
                                dimension = p.dimension(),
                                "embedding provider: local (in-process ONNX)"
                            );
                            Some(Arc::new(p) as Arc<dyn EmbeddingProvider>)
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "failed to load local embedding model — auto-embedding disabled");
                            None
                        }
                    }
                }
                #[cfg(not(feature = "embed-local"))]
                {
                    tracing::warn!(
                        "embedding.provider='local' requires building with `--features embed-local`; auto-embedding disabled"
                    );
                    None
                }
            }
            "none" | "" => {
                tracing::info!("embedding provider: none (semantic search disabled)");
                tracing::info!("  → to enable: set ECPHORIA_EMBEDDING__PROVIDER=ollama or openai");
                None
            }
            other => {
                tracing::warn!(
                    provider = %other,
                    "unknown embedding provider, auto-embedding disabled"
                );
                tracing::info!("  → supported providers: ollama, openai, local, none");
                None
            }
        };

        // Initialize optional completion provider for opt-in LLM fact extraction.
        let completion: Option<Arc<dyn CompletionProvider>> =
            match config.memory.cognition.extraction_provider.as_str() {
                "ollama" => {
                    tracing::info!(
                        model = %config.memory.cognition.extraction_model,
                        "cognition extraction provider: ollama"
                    );
                    Some(Arc::new(crate::llm::ollama::OllamaCompletion::new(
                        config.embedding.ollama_url.clone(),
                        config.memory.cognition.extraction_model.clone(),
                    )))
                }
                "openai" if !config.embedding.openai_api_key.is_empty() => {
                    tracing::info!(
                        model = %config.memory.cognition.extraction_model,
                        "cognition extraction provider: openai"
                    );
                    Some(Arc::new(crate::llm::openai::OpenAiCompletion::new(
                        config.embedding.openai_api_key.clone(),
                        config.memory.cognition.extraction_model.clone(),
                    )))
                }
                "anthropic" if !config.embedding.anthropic_api_key.is_empty() => {
                    tracing::info!(
                        model = %config.memory.cognition.extraction_model,
                        "cognition extraction provider: anthropic"
                    );
                    Some(Arc::new(crate::llm::anthropic::AnthropicCompletion::new(
                        config.embedding.anthropic_api_key.clone(),
                        config.memory.cognition.extraction_model.clone(),
                    )))
                }
                "claude-cli" => {
                    tracing::info!(
                        model = %config.memory.cognition.extraction_model,
                        "cognition extraction provider: claude CLI"
                    );
                    Some(Arc::new(crate::llm::claude_cli::ClaudeCliCompletion::new(
                        config.memory.cognition.extraction_model.clone(),
                    )))
                }
                _ => None,
            };

        // Initialize optional reranker (read-path only; no Raft/determinism impact). Reuses the
        // same chat-completion backends as cognition extraction; off unless `rerank.provider` set.
        let reranker: Option<Arc<dyn Reranker>> = match config.rerank.provider.as_str() {
            "llm" => {
                let backend: Option<Arc<dyn CompletionProvider>> =
                    match config.rerank.backend.as_str() {
                        "ollama" => Some(Arc::new(crate::llm::ollama::OllamaCompletion::new(
                            config.embedding.ollama_url.clone(),
                            config.rerank.model.clone(),
                        ))),
                        "openai" if !config.embedding.openai_api_key.is_empty() => {
                            Some(Arc::new(crate::llm::openai::OpenAiCompletion::new(
                                config.embedding.openai_api_key.clone(),
                                config.rerank.model.clone(),
                            )))
                        }
                        "anthropic" if !config.embedding.anthropic_api_key.is_empty() => {
                            Some(Arc::new(crate::llm::anthropic::AnthropicCompletion::new(
                                config.embedding.anthropic_api_key.clone(),
                                config.rerank.model.clone(),
                            )))
                        }
                        "claude-cli" => {
                            Some(Arc::new(crate::llm::claude_cli::ClaudeCliCompletion::new(
                                config.rerank.model.clone(),
                            )))
                        }
                        _ => None,
                    };
                match backend {
                    Some(c) => {
                        tracing::info!(
                            model = %config.rerank.model,
                            backend = %config.rerank.backend,
                            "reranker: llm"
                        );
                        Some(Arc::new(crate::rerank::LlmReranker::new(c)))
                    }
                    None => {
                        tracing::warn!(
                            backend = %config.rerank.backend,
                            "reranker: llm requested but backend unavailable — disabled"
                        );
                        None
                    }
                }
            }
            "cross_encoder" => {
                // Local ONNX bge-reranker — no LLM latency. Requires building with the
                // `rerank-local` feature (pulls onnxruntime); otherwise degrades gracefully.
                #[cfg(feature = "rerank-local")]
                {
                    match crate::rerank::CrossEncoderReranker::new() {
                        Ok(r) => {
                            tracing::info!("reranker: local cross-encoder (bge-reranker-base)");
                            Some(Arc::new(r) as Arc<dyn Reranker>)
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "cross-encoder load failed — rerank disabled");
                            None
                        }
                    }
                }
                #[cfg(not(feature = "rerank-local"))]
                {
                    tracing::warn!(
                        "reranker: 'cross_encoder' requires building with --features rerank-local \
                         — falling back to no rerank (use provider='llm' meanwhile)"
                    );
                    None
                }
            }
            "none" | "" => None,
            other => {
                tracing::warn!(provider = %other, "unknown reranker provider — disabled");
                None
            }
        };

        // Initialize the durable agent-run ledger. Built only with `agentic`: a memory-only
        // Ecphoria opens no runtime database at all, rather than opening one nothing writes to.
        #[cfg(feature = "agentic")]
        let runs = Arc::new(
            RunStore::open(Path::new(&config.runtime.db_path)).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "falling back to in-memory run store");
                RunStore::new()
            }),
        );

        // Initialize ingest pipeline (keep a reference to embedding for embed-and-search)
        let embedding_ref = embedding.clone();
        let ingest = match embedding {
            Some(emb) => IngestPipeline::with_embedding(
                episodic.clone(),
                semantic.clone(),
                emb,
                config.embedding.batch_size,
            ),
            None => IngestPipeline::new(episodic.clone()),
        };

        tracing::info!("Ecphoria engine initialized");

        // Default authorization backend (built before the struct literal moves `memory_store`).
        let authz: Arc<dyn crate::authz::AuthzBackend> =
            Arc::new(crate::authz::LocalGrants::new(memory_store.clone()));

        // Attachment blob storage: S3 when the storage engine is s3, else a local `attachments/`
        // directory under the data dir. Uses the same backend abstraction as the rest of storage.
        let attachments: Arc<dyn crate::storage::StorageBackend> =
            if config.storage.engine.eq_ignore_ascii_case("s3") {
                Arc::new(crate::storage::s3::S3Storage::from_config(&config.storage.s3).await?)
            } else {
                let dir = std::path::PathBuf::from(&config.storage.data_dir).join("attachments");
                Arc::new(crate::storage::local::LocalStorage::new(dir))
            };

        Ok(Self {
            config,
            episodic,
            embedding: embedding_ref,
            semantic,
            state,
            memory_store,
            memory_index,
            lexical_index,
            query_log,
            modal: crate::memory::semantic::MultiModalStore::new(),
            ingest,
            completion,
            reranker,
            #[cfg(feature = "agentic")]
            runs,
            #[cfg(feature = "agentic")]
            driver_id: uuid::Uuid::new_v4().to_string(),
            memory_change_tx: tokio::sync::broadcast::channel(1024).0,
            #[cfg(feature = "agentic")]
            tool_executor: parking_lot::RwLock::new(None),
            #[cfg(feature = "agentic")]
            run_replicator: parking_lot::RwLock::new(None),
            authz: parking_lot::RwLock::new(authz),
            attachments,
            image_embedding: parking_lot::RwLock::new(None),
        })
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &CoreConfig {
        &self.config
    }

    /// Where the FTS5 index file lives, given the cognition DuckDB path.
    ///
    /// `:memory:` stays in memory; otherwise it is a sibling of the DuckDB file
    /// (`…/memories.duckdb` → `…/memories.fts.sqlite`) so one data dir holds the whole layer.
    fn lexical_index_path(cognition_db_path: &str) -> std::path::PathBuf {
        if cognition_db_path == ":memory:" {
            return std::path::PathBuf::from(":memory:");
        }
        Path::new(cognition_db_path).with_extension("fts.sqlite")
    }

    /// Rebuild the lexical index from DuckDB, the source of truth. Returns the number indexed.
    ///
    /// Paged so a large corpus never materializes at once. Called on startup and by
    /// `/admin/reindex`; safe to run at any time (it clears first, so it converges rather than
    /// accumulating duplicates).
    async fn rebuild_lexical_index(store: &MemoryStore, index: &LexicalIndex) -> Result<usize> {
        const PAGE: usize = 2_000;
        index.clear()?;
        let mut offset = 0usize;
        let mut total = 0usize;
        loop {
            let page = store.lexical_page(offset, PAGE).await?;
            if page.is_empty() {
                break;
            }
            let n = page.len();
            for (id, scope, project, subject, content) in page {
                let key = crate::memory::cognition::scope_partition_key(&scope);
                index.upsert(id, &key, project.as_deref(), subject.as_deref(), &content)?;
            }
            total += n;
            offset += n;
            if n < PAGE {
                break;
            }
        }
        if total > 0 {
            tracing::info!(memories = total, "rebuilt lexical index from disk");
        }
        Ok(total)
    }

    /// Rebuild the lexical index on demand (admin reindex).
    pub async fn lexical_reindex(&self) -> Result<usize> {
        Self::rebuild_lexical_index(&self.memory_store, &self.lexical_index).await
    }

    /// Swap the cross-scope authorization backend (default: `LocalGrants`). Lets a deployment plug
    /// in a richer/external policy engine (teams/roles, ReBAC/SpiceDB) without touching the read
    /// path. The backend MUST stay tenant-strict (never widen access across tenants).
    pub fn set_authz_backend(&self, backend: Arc<dyn crate::authz::AuthzBackend>) {
        *self.authz.write() = backend;
    }

    // ── Episodic Memory ──────────────────────────────────────────────

    /// Ingest events via the pipeline.
    pub async fn ingest(&self, events: Vec<Event>) -> Result<u64> {
        self.ingest.ingest(events).await
    }

    /// Number of events whose vectors are not yet in the semantic index (cross-store drift gauge).
    pub async fn unembedded_count(&self) -> Result<u64> {
        self.episodic.unembedded_count().await
    }

    /// Re-embed and index up to `limit` events that were left unembedded (e.g. the embedding
    /// provider was down at ingest). Returns the number newly indexed. Closes the cross-store gap.
    pub async fn reindex_unembedded(&self, limit: usize) -> Result<usize> {
        let events = self.episodic.unembedded_events(limit).await?;
        if events.is_empty() {
            return Ok(0);
        }
        Ok(self.ingest.embed_and_index(&events).await)
    }

    /// Ingest events scoped to a specific tenant.
    ///
    /// Sets the tenant_id on all events before ingestion so that
    /// tenant-scoped queries only see their own data.
    pub async fn ingest_for_tenant(
        &self,
        mut events: Vec<Event>,
        tenant: &crate::config::TenantContext,
    ) -> Result<u64> {
        // Tag each event's payload with the tenant so the episodic store sets the tenant_id
        // column per-row AT INSERT TIME (atomic, race-free, deterministic for Raft apply),
        // and so the embedding metadata carries the tenant. No post-insert UPDATE — that was
        // a cross-tenant leak under concurrency and a non-determinism hazard in cluster apply.
        for event in &mut events {
            if let serde_json::Value::Object(ref mut map) = event.payload {
                map.insert(
                    "_tenant_id".to_string(),
                    serde_json::Value::String(tenant.tenant_id.clone()),
                );
            }
        }
        self.ingest.ingest(events).await
    }

    /// Append fully-materialized events to episodic ONLY (no embedding) — the deterministic write
    /// used on the Raft apply path. Tags `_tenant_id` (if scoped) at insert time like
    /// `ingest_for_tenant`, but leaves vector indexing to the local background reindex loop, so
    /// apply stays a pure function of the request (no external, non-deterministic embedding call
    /// that would diverge the index across nodes or stall the apply loop on a hung provider).
    pub async fn ingest_replicated(
        &self,
        mut events: Vec<Event>,
        tenant: Option<&str>,
    ) -> Result<u64> {
        if let Some(t) = tenant {
            for event in &mut events {
                if let serde_json::Value::Object(ref mut map) = event.payload {
                    map.insert(
                        "_tenant_id".to_string(),
                        serde_json::Value::String(t.to_string()),
                    );
                }
            }
        }
        self.ingest.ingest_episodic_only(events).await
    }

    /// Query events by source.
    pub async fn query_by_source(&self, source: &str, limit: usize) -> Result<Vec<Event>> {
        let limit = limit.min(self.config.query.max_rows);
        self.episodic.query_by_source(source, limit).await
    }

    /// Execute SQL against the engine, intercepting ecphoria_search() and ecphoria_state()
    /// virtual functions via the query planner/executor pipeline.
    ///
    /// Pure SQL SELECT queries run on a blocking thread against DuckDB.
    /// ecphoria_search('text', k) embeds the text and searches semantic memory.
    /// ecphoria_state('agent_id', 'key') looks up a state key.
    /// Enforces the configured query timeout.
    pub async fn query_sql(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let plan = QueryPlanner::plan(sql)?;
        let max_rows = self.config.query.max_rows;
        let timeout = std::time::Duration::from_millis(self.config.query.timeout_ms);

        let executor = QueryExecutor::new(
            self.episodic.clone(),
            self.semantic.clone(),
            self.state.clone(),
            self.embedding.clone(),
        )
        .with_memory(self.memory_store.clone());

        tokio::time::timeout(timeout, executor.execute(plan, max_rows))
            .await
            .map_err(|_| crate::Error::Query("query timed out".into()))?
    }

    /// Execute SQL scoped to a single tenant — every `episodic`/`memories`/… reference is rewritten
    /// to a per-tenant filtered view, so the caller can only read its own rows (row-level isolation).
    pub async fn query_sql_for_tenant(
        &self,
        sql: &str,
        tenant: &str,
    ) -> Result<Vec<serde_json::Value>> {
        let plan = QueryPlanner::plan(sql)?;
        let max_rows = self.config.query.max_rows;
        let timeout = std::time::Duration::from_millis(self.config.query.timeout_ms);

        let executor = QueryExecutor::new(
            self.episodic.clone(),
            self.semantic.clone(),
            self.state.clone(),
            self.embedding.clone(),
        )
        .with_memory(self.memory_store.clone())
        .with_tenant(tenant);

        tokio::time::timeout(timeout, executor.execute(plan, max_rows))
            .await
            .map_err(|_| crate::Error::Query("query timed out".into()))?
    }

    /// Count total events.
    pub async fn event_count(&self) -> Result<u64> {
        self.episodic.count().await
    }

    // ── Semantic Memory ──────────────────────────────────────────────

    /// Upsert a semantic entry.
    pub async fn semantic_upsert(&self, entry: &SemanticEntry) -> Result<()> {
        self.semantic.upsert(entry).await
    }

    /// Search semantic memory by vector.
    pub async fn semantic_search(&self, vector: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.semantic.search(vector, k).await
    }

    /// Upsert a pre-computed embedding of any modality (text/image/audio/…). Ecphoria becomes the
    /// multi-modal memory store; callers bring their own modality encoder (CLIP, etc.). The vector
    /// must match the index dimension — mixed-dimension modalities need separate indexes (future).
    pub async fn semantic_upsert_modal(
        &self,
        id: uuid::Uuid,
        modality: &str,
        content: impl Into<String>,
        embedding: Vec<f32>,
        mut metadata: serde_json::Value,
    ) -> Result<()> {
        if !metadata.is_object() {
            metadata = serde_json::json!({});
        }
        metadata["modality"] = serde_json::json!(modality);
        // Route to the per-modality index so mixed dimensions (e.g. 512-d CLIP, 768-d text) coexist.
        self.modal
            .upsert(
                modality,
                &SemanticEntry {
                    id,
                    content: content.into(),
                    embedding,
                    metadata,
                },
            )
            .await
    }

    /// Vector search restricted to one modality (or all matching-dimension modalities when None).
    pub async fn semantic_search_modal(
        &self,
        vector: &[f32],
        k: usize,
        modality: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        match modality {
            Some(m) => self.modal.search(m, vector, k).await,
            None => self.modal.search_all(vector, k).await,
        }
    }

    /// Modalities that currently have a vector index.
    pub fn modalities(&self) -> Vec<String> {
        self.modal.modalities()
    }

    /// Search semantic memory with metadata filters.
    ///
    /// Filters can match on source, event_type, or any metadata field.
    pub async fn semantic_search_filtered(
        &self,
        vector: &[f32],
        k: usize,
        source: Option<&str>,
        event_type: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        let source_owned = source.map(|s| s.to_string());
        let event_type_owned = event_type.map(|s| s.to_string());

        self.semantic
            .search_filtered(vector, k, move |entry| {
                if let Some(ref src) = source_owned {
                    if let Some(meta_src) = entry.metadata.get("source").and_then(|v| v.as_str()) {
                        if meta_src != src {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                if let Some(ref et) = event_type_owned {
                    if let Some(meta_et) = entry.metadata.get("event_type").and_then(|v| v.as_str())
                    {
                        if meta_et != et {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            })
            .await
    }

    /// Delete a semantic entry by UUID.
    pub async fn semantic_delete(&self, id: uuid::Uuid) -> Result<()> {
        self.semantic.delete(id).await
    }

    /// Number of entries in semantic memory.
    pub fn semantic_count(&self) -> usize {
        self.semantic.len()
    }

    // ── State Memory ─────────────────────────────────────────────────

    /// Get agent state.
    pub async fn state_get(
        &self,
        agent_id: &str,
        key: &str,
    ) -> Result<Option<crate::memory::state::StateEntry>> {
        self.state.get(agent_id, key).await
    }

    /// Set agent state.
    pub async fn state_set(
        &self,
        agent_id: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<u64> {
        self.state.set(agent_id, key, value).await
    }

    /// Delete agent state.
    pub async fn state_delete(&self, agent_id: &str, key: &str) -> Result<()> {
        self.state.delete(agent_id, key).await
    }

    /// Subscribe to state change notifications (for WebSocket watchers).
    pub fn state_subscribe(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::memory::state::StateChange> {
        self.state.subscribe()
    }

    /// List state keys for an agent.
    pub async fn state_list_keys(&self, agent_id: &str) -> Result<Vec<String>> {
        self.state.list_keys(agent_id).await
    }

    // Tenant-scoped state: the agent_id is namespaced by tenant so one tenant can never
    // read/write another tenant's agent state, even with a colliding agent_id.

    /// Tenant-scoped state get (the returned `agent_id` has the tenant prefix stripped).
    pub async fn state_get_for_tenant(
        &self,
        tenant: &str,
        agent_id: &str,
        key: &str,
    ) -> Result<Option<crate::memory::state::StateEntry>> {
        let mut entry = self.state.get(&scoped_agent(tenant, agent_id), key).await?;
        if let Some(ref mut e) = entry {
            e.agent_id = agent_id.to_string();
        }
        Ok(entry)
    }

    /// Tenant-scoped state set.
    pub async fn state_set_for_tenant(
        &self,
        tenant: &str,
        agent_id: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<u64> {
        self.state
            .set(&scoped_agent(tenant, agent_id), key, value)
            .await
    }

    /// Tenant-scoped state delete.
    pub async fn state_delete_for_tenant(
        &self,
        tenant: &str,
        agent_id: &str,
        key: &str,
    ) -> Result<()> {
        self.state
            .delete(&scoped_agent(tenant, agent_id), key)
            .await
    }

    /// Tenant-scoped state key listing.
    pub async fn state_list_keys_for_tenant(
        &self,
        tenant: &str,
        agent_id: &str,
    ) -> Result<Vec<String>> {
        self.state.list_keys(&scoped_agent(tenant, agent_id)).await
    }

    // ── Sessions ─────────────────────────────────────────────────────

    /// Start a new conversation session.
    pub async fn session_start(
        &self,
        session_id: &str,
        agent_id: &str,
        parent_session_id: Option<&str>,
        metadata: Option<serde_json::Value>,
    ) -> Result<()> {
        self.episodic
            .start_session(session_id, agent_id, parent_session_id, metadata)
            .await
    }

    /// End a conversation session with an optional summary.
    pub async fn session_end(&self, session_id: &str, summary: Option<&str>) -> Result<()> {
        self.episodic.end_session(session_id, summary).await
    }

    /// Get details of a session.
    pub async fn session_get(&self, session_id: &str) -> Result<Option<serde_json::Value>> {
        self.episodic.get_session(session_id).await
    }

    /// List sessions for an agent.
    pub async fn session_list(
        &self,
        agent_id: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let limit = limit.min(self.config.query.max_rows);
        self.episodic.list_sessions(agent_id, limit).await
    }

    /// Recall all events in a session.
    pub async fn session_recall(&self, session_id: &str) -> Result<Vec<serde_json::Value>> {
        self.episodic.recall_session(session_id).await
    }

    /// Start a session scoped to a tenant.
    pub async fn session_start_for_tenant(
        &self,
        session_id: &str,
        agent_id: &str,
        parent_session_id: Option<&str>,
        metadata: Option<serde_json::Value>,
        tenant: &str,
    ) -> Result<()> {
        self.episodic
            .start_session_for_tenant(session_id, agent_id, parent_session_id, metadata, tenant)
            .await
    }

    /// End a session scoped to a tenant (true iff a session was updated).
    pub async fn session_end_for_tenant(
        &self,
        session_id: &str,
        summary: Option<&str>,
        tenant: &str,
    ) -> Result<bool> {
        self.episodic
            .end_session_for_tenant(session_id, summary, tenant)
            .await
    }

    /// Recall a session's events scoped to a tenant.
    pub async fn session_recall_for_tenant(
        &self,
        session_id: &str,
        tenant: &str,
    ) -> Result<Vec<serde_json::Value>> {
        self.episodic
            .recall_session_for_tenant(session_id, tenant)
            .await
    }

    /// Distill a closed session's events into memory — the consolidation half of the cycle: a
    /// session's episodic trace becomes durable semantic memory. Recalls the session's events,
    /// builds a digest, and (with LLM extraction on) distills atomic facts / (off) stores one
    /// memory, scoped to the session. Returns the created memories. Local write; the plan variant
    /// [`Self::session_distill_plan`] is for cluster replication.
    pub async fn session_distill(
        &self,
        session_id: &str,
        scope: &MemoryScope,
    ) -> Result<Vec<MemoryAdd>> {
        let plans = self.session_distill_plan(session_id, scope).await?;
        let mut out = Vec::with_capacity(plans.len());
        for (result, rows) in plans {
            self.memory_apply_rows(rows).await?;
            out.push(result);
        }
        Ok(out)
    }

    /// Plan session distillation without writing: one `(MemoryAdd, rows)` per distilled fact, so the
    /// cluster leader can replicate each through the Raft log (`MemoryUpsert`). Empty if the session
    /// has no events.
    pub async fn session_distill_plan(
        &self,
        session_id: &str,
        scope: &MemoryScope,
    ) -> Result<Vec<(MemoryAdd, Vec<MemoryRow>)>> {
        let tenant = if scope.tenant_id.is_empty() {
            "default"
        } else {
            scope.tenant_id.as_str()
        };
        let events = self
            .episodic
            .recall_session_for_tenant(session_id, tenant)
            .await?;
        if events.is_empty() {
            return Ok(vec![]);
        }
        let digest = Self::session_digest(session_id, &events);
        let facts = self.extract_facts(&digest).await;
        // Scope the distilled memories to this session.
        let mut session_scope = scope.clone();
        session_scope.session_id = Some(session_id.to_string());

        let mut out = Vec::with_capacity(facts.len());
        for (subject, content) in facts {
            let mut input = MemoryInput::new(session_scope.clone(), content);
            input.subject = subject;
            input.mem_type = Some("episodic".into());
            out.push(self.memory_plan(input).await?);
        }
        Ok(out)
    }

    /// Build a compact, human-readable digest of a session's events for distillation.
    fn session_digest(session_id: &str, events: &[serde_json::Value]) -> String {
        let mut s = format!("Session {session_id} summary. Events:\n");
        for ev in events.iter().take(200) {
            let etype = ev
                .get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or("event");
            let source = ev.get("source").and_then(|v| v.as_str()).unwrap_or("");
            let payload = ev.get("payload").map(|p| p.to_string()).unwrap_or_default();
            let payload: String = payload.chars().take(300).collect();
            s.push_str(&format!("- [{source}] {etype}: {payload}\n"));
        }
        s
    }

    // ── Embed & Search ────────────────────────────────────────────────

    /// Test-only: inject an embedding provider after construction so cognition paths that require
    /// vectors (semantic dedup/merge) can be exercised without a live Ollama/OpenAI backend.
    #[cfg(test)]
    fn set_embedding_for_test(&mut self, provider: Arc<dyn EmbeddingProvider>) {
        self.embedding = Some(provider);
    }

    /// Embed a text string using the configured embedding provider.
    pub async fn embed_text(&self, text: &str) -> Result<Vec<f32>> {
        let provider = self
            .embedding
            .as_ref()
            .ok_or_else(|| crate::Error::Embedding("no embedding provider configured".into()))?;
        // Query path → apply the model's *query* task prefix (asymmetric retrieval).
        let results = provider.embed_query(&[text.to_string()]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| crate::Error::Embedding("embedding returned empty result".into()))
    }

    /// Embed a batch of texts with **no** task prefix — the symmetric behavior an OpenAI-compatible
    /// `/v1/embeddings` caller expects (they manage any prefixing themselves). Returns one vector
    /// per input, in order.
    pub async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let provider = self
            .embedding
            .as_ref()
            .ok_or_else(|| crate::Error::Embedding("no embedding provider configured".into()))?;
        provider.embed(texts).await
    }

    /// The configured embedding model name (for the `/v1/embeddings` response `model` field).
    pub fn embedding_model(&self) -> Option<String> {
        self.embedding.as_ref().map(|p| p.model_name().to_string())
    }

    /// Embed a **document** to be indexed (write/ingest path) — applies the model's *document* task
    /// prefix, the counterpart to [`Self::embed_text`]'s query prefix. Using the right side for each
    /// role is what makes asymmetric retrieval models (nomic, e5, …) actually work.
    pub async fn embed_document_text(&self, text: &str) -> Result<Vec<f32>> {
        let provider = self
            .embedding
            .as_ref()
            .ok_or_else(|| crate::Error::Embedding("no embedding provider configured".into()))?;
        let results = provider.embed_documents(&[text.to_string()]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| crate::Error::Embedding("embedding returned empty result".into()))
    }

    /// Embed text and search semantic memory in a single call.
    ///
    /// This is the primary DX-friendly search method: text in, results out.
    pub async fn embed_and_search(
        &self,
        text: &str,
        k: usize,
        source: Option<&str>,
        event_type: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        let vector = self.embed_text(text).await?;
        if source.is_some() || event_type.is_some() {
            self.semantic_search_filtered(&vector, k, source, event_type)
                .await
        } else {
            self.semantic_search(&vector, k).await
        }
    }

    /// Vector search over event embeddings, scoped to a tenant (row-level isolation).
    pub async fn semantic_search_for_tenant(
        &self,
        vector: &[f32],
        k: usize,
        tenant: &str,
        source: Option<&str>,
        event_type: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        let tenant = tenant.to_string();
        let source_owned = source.map(|s| s.to_string());
        let event_type_owned = event_type.map(|s| s.to_string());
        self.semantic
            .search_filtered(vector, k, move |entry| {
                let mtenant = entry
                    .metadata
                    .get("tenant_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");
                if mtenant != tenant {
                    return false;
                }
                if let Some(ref src) = source_owned {
                    if entry.metadata.get("source").and_then(|v| v.as_str()) != Some(src.as_str()) {
                        return false;
                    }
                }
                if let Some(ref et) = event_type_owned {
                    if entry.metadata.get("event_type").and_then(|v| v.as_str())
                        != Some(et.as_str())
                    {
                        return false;
                    }
                }
                true
            })
            .await
    }

    /// Embed text and vector-search event embeddings scoped to a tenant.
    pub async fn embed_and_search_for_tenant(
        &self,
        text: &str,
        k: usize,
        tenant: &str,
        source: Option<&str>,
        event_type: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        let vector = self.embed_text(text).await?;
        self.semantic_search_for_tenant(&vector, k, tenant, source, event_type)
            .await
    }

    // ── Memory Cognition ──────────────────────────────────────────────

    /// Apply the tenant's governance rules to one write: attribution, then fact shape.
    ///
    /// Both are off by default, and the common case — an unconfigured tenant — costs one map
    /// lookup and returns. See [`crate::config::GovernanceConfig`].
    ///
    /// `warn` exists because turning validation on in a store that already holds untyped memories
    /// would otherwise mean choosing between breaking every writer and learning nothing: in `warn`
    /// the write succeeds, and `ecphoria_fact_validation_failures_total` counts what `strict`
    /// would have refused.
    fn check_governance(
        &self,
        scope: &crate::memory::cognition::MemoryScope,
        subject: Option<&str>,
        metadata: &serde_json::Value,
        source_event_ids: &[uuid::Uuid],
    ) -> Result<()> {
        let rules = self.config.memory.governance.for_tenant(&scope.tenant_id);
        if rules.is_permissive() {
            return Ok(());
        }

        if rules.require_provenance
            && !crate::memory::facts::has_provenance(metadata, source_event_ids)
        {
            metrics::counter!(
                "ecphoria_memory_writes_refused_total",
                "reason" => "provenance"
            )
            .increment(1);
            return Err(crate::Error::Validation(
                "this tenant requires provenance: set `metadata.provenance.source` (or supply                  `source_event_ids`) so the fact can be traced back to where it came from"
                    .into(),
            ));
        }

        if rules.fact_validation.is_off() {
            return Ok(());
        }
        let errors = crate::memory::facts::validate(subject, metadata);
        if errors.is_empty() {
            return Ok(());
        }
        let mode = if rules.fact_validation == crate::memory::facts::FactValidation::Strict {
            "strict"
        } else {
            "warn"
        };
        metrics::counter!("ecphoria_fact_validation_failures_total", "mode" => mode).increment(1);
        if mode == "warn" {
            tracing::warn!(
                tenant = %scope.tenant_id,
                subject = subject.unwrap_or("<none>"),
                errors = %errors.join("; "),
                "fact does not validate — accepted because this tenant is in `warn` mode"
            );
            return Ok(());
        }
        Err(crate::Error::Validation(errors.join("; ")))
    }

    /// Add a memory through the deterministic cognition pipeline.
    ///
    /// Behaviour (deterministic core, no LLM required):
    /// - If `subject` is set and an active memory with the same `(scope, subject)` exists:
    ///   identical content → reinforce (`Confirmed`); different content → supersede the old
    ///   and insert the new (`Superseded`, bi-temporal — the old one keeps its history and
    ///   stays answerable via [`Self::memory_as_of`]).
    /// - Else, when an embedding provider is configured, a near-duplicate (cosine ≥
    ///   `dedup_threshold`) in the same scope is merged/updated (`Merged`).
    /// - Otherwise a fresh memory is inserted (`Inserted`).
    pub async fn memory_add(&self, input: MemoryInput) -> Result<MemoryAdd> {
        let (result, rows) = self.memory_plan(input).await?;
        let scope = result.memory.scope.clone();
        self.memory_apply_rows(rows).await?;
        // Count-based forgetting / per-tenant quota: evict the lowest-importance memories beyond cap.
        let cap = self.config.memory.cognition.max_memories_per_scope;
        if cap > 0 {
            if let Ok(evicted) = self.memory_store.enforce_scope_cap(&scope, cap).await {
                for id in evicted {
                    self.forget_indexes(id).await;
                }
            }
        }
        Ok(result)
    }

    /// Compute the materialized change-set for adding a memory **without writing anything**.
    ///
    /// Runs the same deterministic cognition as [`Self::memory_add`] (subject contradiction →
    /// semantic merge → insert) but returns the resulting [`MemoryRow`]s instead of applying
    /// them. This lets the cluster leader run cognition once, propose the rows through Raft, and
    /// have every node apply an identical result via [`Self::memory_apply_rows`] — avoiding the
    /// failover-divergence of re-running non-deterministic logic (new uuids/timestamps) per node.
    pub async fn memory_plan(&self, input: MemoryInput) -> Result<(MemoryAdd, Vec<MemoryRow>)> {
        let embedding = self.embed_memory_content(&input.content).await;
        self.memory_plan_with_embedding(input, embedding).await
    }

    /// Embed one memory's content for storage, best-effort.
    ///
    /// The deterministic cognition paths work without it, but a failure silently drops the memory
    /// out of vector search (BM25-only), so it is made observable (metric + warn) rather than
    /// degrading quietly. Memory content is an indexed *document*, so this uses the document task
    /// prefix, not the query one.
    async fn embed_memory_content(&self, content: &str) -> Option<Vec<f32>> {
        self.embedding.as_ref()?;
        match self.embed_document_text(content).await {
            Ok(v) => Some(v),
            Err(e) => {
                metrics::counter!("ecphoria_memory_embed_failures_total", "op" => "ingest")
                    .increment(1);
                tracing::warn!(error = %e, "memory embedding failed — stored without a vector (search degraded to BM25)");
                None
            }
        }
    }

    /// [`Self::memory_plan`] with a caller-supplied embedding.
    ///
    /// Split out so bulk ingest can embed a whole batch in one provider round-trip and still run
    /// cognition one memory at a time — which it must, because dedup and contradiction resolution
    /// for memory *n* depend on the effects of memories *0..n*.
    pub async fn memory_plan_with_embedding(
        &self,
        mut input: MemoryInput,
        embedding: Option<Vec<f32>>,
    ) -> Result<(MemoryAdd, Vec<MemoryRow>)> {
        if input.scope.tenant_id.is_empty() {
            input.scope.tenant_id = "default".into();
        }
        // Canonicalize the subject once, up front: both the contradiction lookup
        // (`find_active_by_subject`) and the stored `mem.subject` then use the same normalized key,
        // so "Plan"/"plan"/" plan " resolve to a single active memory instead of coexisting.
        input.subject = input
            .subject
            .map(|s| crate::memory::cognition::normalize_subject(&s))
            .filter(|s| !s.is_empty());
        self.check_governance(
            &input.scope,
            input.subject.as_deref(),
            &input.metadata,
            &input.source_event_ids,
        )?;
        let cog = &self.config.memory.cognition;
        let importance = input.importance.unwrap_or(cog.default_importance);

        // 1. Subject-based contradiction resolution (authoritative, no embedding required).
        if let Some(subject) = input.subject.clone() {
            let actives = self
                .memory_store
                .find_active_by_subject(&input.scope, &subject, input.project.as_deref())
                .await?;
            if let Some(existing) = actives.first() {
                if existing.content.trim() == input.content.trim() {
                    // Confirmed: bump importance + version; preserve the existing embedding when
                    // we couldn't re-embed (else upsert_raw would NULL the stored vector).
                    let mut m = existing.clone();
                    m.importance = importance.max(existing.importance);
                    m.version += 1;
                    m.updated_at = chrono::Utc::now();
                    let emb = match embedding.clone() {
                        Some(e) => Some(e),
                        None => self.memory_store.get_embedding(existing.id).await?,
                    };
                    return Ok((
                        MemoryAdd {
                            memory: m.clone(),
                            outcome: MemoryOutcome::Confirmed,
                        },
                        vec![MemoryRow {
                            memory: m,
                            embedding: emb,
                        }],
                    ));
                }
                // HITL review mode: a contradiction does NOT auto-supersede. Insert the new memory
                // as active alongside the prior one(s); the (scope, subject) now has multiple active
                // memories, which surfaces in the review queue (`memory_contradictions`) for a human
                // to resolve. Deterministic (no supersession rows), so Raft apply stays identical.
                if cog.contradiction_review {
                    let mut mem = Memory::new(input.scope.clone(), input.content.clone());
                    mem.subject = Some(subject);
                    mem.importance = importance;
                    mem.source_event_ids = input.source_event_ids.clone();
                    mem.metadata = input.metadata.clone();
                    if let Some(t) = &input.mem_type {
                        mem.mem_type = t.clone();
                    }
                    return Ok((
                        MemoryAdd {
                            memory: mem.clone(),
                            outcome: MemoryOutcome::Conflict,
                        },
                        vec![MemoryRow {
                            memory: mem,
                            embedding: embedding.clone(),
                        }],
                    ));
                }

                // Contradiction: supersede every active memory for this subject, insert new.
                // `valid_at` is the valid-time boundary — when the new fact became true and the
                // old one stopped being true. `recorded_at` is transaction time, always now.
                let recorded_at = chrono::Utc::now();
                let valid_at = Self::valid_boundary(
                    input.valid_from,
                    actives.iter().map(|a| a.valid_from).max(),
                    recorded_at,
                );
                let mut rows: Vec<MemoryRow> = Vec::with_capacity(actives.len() + 1);
                for a in &actives {
                    let mut old = a.clone();
                    old.state = MemoryState::Superseded;
                    old.valid_to = Some(valid_at);
                    old.updated_at = recorded_at;
                    rows.push(MemoryRow {
                        memory: old,
                        embedding: None,
                    });
                }
                let mut mem = Memory::new(input.scope.clone(), input.content.clone());
                mem.subject = Some(subject);
                mem.project = input.project.clone();
                mem.importance = importance;
                mem.supersedes = Some(actives[0].id);
                mem.valid_from = valid_at;
                mem.source_event_ids = input.source_event_ids.clone();
                mem.metadata = input.metadata.clone();
                if let Some(t) = &input.mem_type {
                    mem.mem_type = t.clone();
                }
                rows.push(MemoryRow {
                    memory: mem.clone(),
                    embedding: embedding.clone(),
                });
                return Ok((
                    MemoryAdd {
                        memory: mem,
                        outcome: MemoryOutcome::Superseded,
                    },
                    rows,
                ));
            }
        } else if let Some(emb) = embedding.as_deref() {
            // 2. Semantic dedup/merge (subjectless facts, only when embeddings are available).
            let hits = self.memory_index_search(emb, &input.scope, 1).await?;
            if let Some(top) = hits.first() {
                if top.score >= cog.dedup_threshold {
                    // Bi-temporal merge: rather than overwriting the old row's content in place
                    // (which would erase the prior text with no trace), close the old row as
                    // superseded at `valid_to` and insert a new one pointing back to it. This keeps
                    // `memory_as_of(T)` and `/history` exact on the dedup path too — honoring the
                    // "nothing is silently hard-deleted" guarantee on every write path, not just the
                    // subject-contradiction one. The outcome stays `Merged` so callers can still
                    // distinguish a near-duplicate consolidation from a true contradiction.
                    let recorded_at = chrono::Utc::now();
                    let valid_at = Self::valid_boundary(
                        input.valid_from,
                        Some(top.memory.valid_from),
                        recorded_at,
                    );
                    let mut old = top.memory.clone();
                    old.state = MemoryState::Superseded;
                    old.valid_to = Some(valid_at);
                    old.updated_at = recorded_at;

                    let mut mem = Memory::new(input.scope.clone(), input.content.clone());
                    mem.subject = top.memory.subject.clone();
                    mem.project = input.project.clone().or(top.memory.project.clone());
                    mem.importance = importance.max(top.memory.importance);
                    mem.supersedes = Some(top.memory.id);
                    mem.valid_from = valid_at;
                    // Provenance survives the merge: the old memory's source events still support
                    // the consolidated fact, so carry them forward (deduped) with the new ones.
                    let mut src = top.memory.source_event_ids.clone();
                    for id in &input.source_event_ids {
                        if !src.contains(id) {
                            src.push(*id);
                        }
                    }
                    mem.source_event_ids = src;
                    mem.metadata = input.metadata.clone();
                    if let Some(t) = &input.mem_type {
                        mem.mem_type = t.clone();
                    }
                    return Ok((
                        MemoryAdd {
                            memory: mem.clone(),
                            outcome: MemoryOutcome::Merged,
                        },
                        vec![
                            MemoryRow {
                                memory: old,
                                embedding: None,
                            },
                            MemoryRow {
                                memory: mem,
                                embedding: embedding.clone(),
                            },
                        ],
                    ));
                }
            }
        }

        // 3. Insert a fresh memory.
        let mut mem = Memory::new(input.scope.clone(), input.content.clone());
        mem.subject = input.subject.clone();
        mem.project = input.project.clone();
        mem.importance = importance;
        if let Some(at) = input.valid_from {
            mem.valid_from = at;
        }
        mem.source_event_ids = input.source_event_ids.clone();
        mem.metadata = input.metadata.clone();
        if let Some(t) = &input.mem_type {
            mem.mem_type = t.clone();
        }
        Ok((
            MemoryAdd {
                memory: mem.clone(),
                outcome: MemoryOutcome::Inserted,
            },
            vec![MemoryRow {
                memory: mem,
                embedding,
            }],
        ))
    }

    /// Add many memories in one call — the bulk-ingest path for loading a corpus.
    ///
    /// Semantically identical to calling [`Self::memory_add`] in a loop: cognition still runs one
    /// memory at a time and in order, because dedup and contradiction resolution for memory *n*
    /// depend on the effects of memories *0..n*. What changes is the I/O around it:
    ///
    /// - **embeddings** are computed for the whole batch in provider-sized chunks
    ///   (`embedding.batch_size`) rather than one HTTP round-trip per memory;
    /// - **DuckDB rows** are written in one transaction instead of one per memory — the dominant
    ///   cost, ~4.3 ms/row vs ~59 µs batched (`docs/benchmarks.md`);
    /// - **lexical index** entries likewise commit once.
    ///
    /// Rows are buffered and applied together, so cognition for memory *n* would not normally see
    /// memories *0..n-1* — which would silently break contradiction resolution *within* a batch
    /// (two versions of the same subject would both insert instead of one superseding the other).
    /// The buffer is therefore **flushed whenever an incoming memory's subject collides with one
    /// already pending**, so the deterministic guarantee holds exactly as it does one-at-a-time.
    /// Distinct subjects — the normal case when importing a document corpus — never trigger it.
    ///
    /// Known limitation: *semantic* (vector) dedup still cannot see within a batch, because the
    /// vector index is updated at apply time. Two subject-less near-duplicates in one batch are
    /// both stored. Contradiction resolution by subject is the deterministic guarantee and is
    /// preserved; vector dedup is a best-effort consolidation and is not.
    pub async fn memory_add_batch(&self, inputs: Vec<MemoryInput>) -> Result<Vec<MemoryAdd>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        // 1. Embed everything up front, in provider-sized chunks.
        let embeddings: Vec<Option<Vec<f32>>> = match self.embedding.as_ref() {
            Some(provider) => {
                let mut out: Vec<Option<Vec<f32>>> = Vec::with_capacity(inputs.len());
                let size = self.config.embedding.batch_size.max(1);
                for chunk in inputs.chunks(size) {
                    let texts: Vec<String> = chunk.iter().map(|i| i.content.clone()).collect();
                    match provider.embed_documents(&texts).await {
                        Ok(vs) if vs.len() == chunk.len() => out.extend(vs.into_iter().map(Some)),
                        Ok(_) | Err(_) => {
                            metrics::counter!("ecphoria_memory_embed_failures_total", "op" => "ingest")
                                .increment(chunk.len() as u64);
                            tracing::warn!(
                                batch = chunk.len(),
                                "batch embedding failed — memories stored without vectors (search degraded to BM25)"
                            );
                            out.extend(std::iter::repeat_n(None, chunk.len()));
                        }
                    }
                }
                out
            }
            None => vec![None; inputs.len()],
        };

        // 2. Plan each memory in order — cognition is sequential by necessity — buffering the
        //    rows, and flushing early whenever a subject repeats so the contradiction check can
        //    see the version it must supersede.
        let mut adds = Vec::with_capacity(inputs.len());
        let mut pending: Vec<MemoryRow> = Vec::new();
        let mut pending_subjects: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        for (input, embedding) in inputs.into_iter().zip(embeddings) {
            // Key the overlay exactly as `memory_plan_with_embedding` will: default tenant filled
            // in, subject normalized. A mismatch here would silently miss a collision.
            let key = input.subject.as_deref().map(|s| {
                let mut scope = input.scope.clone();
                if scope.tenant_id.is_empty() {
                    scope.tenant_id = "default".into();
                }
                (
                    crate::memory::cognition::scope_partition_key(&scope),
                    crate::memory::cognition::normalize_subject(s),
                )
            });
            if let Some(k) = &key {
                if pending_subjects.contains(k) {
                    self.memory_apply_rows_batch(std::mem::take(&mut pending))
                        .await?;
                    pending_subjects.clear();
                }
            }
            let (add, planned) = self.memory_plan_with_embedding(input, embedding).await?;
            if let Some(k) = key {
                pending_subjects.insert(k);
            }
            adds.push(add);
            pending.extend(planned);
        }

        // 3. Apply what is left.
        self.memory_apply_rows_batch(pending).await?;
        Ok(adds)
    }

    /// Batched [`Self::memory_apply_rows`]: one DuckDB transaction, one lexical transaction, and
    /// the vector index updated per row (USearch has no batch API and is already in-memory).
    async fn memory_apply_rows_batch(&self, rows: Vec<MemoryRow>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.memory_store.upsert_raw_batch(&rows).await?;

        let mut lexical_upserts: Vec<crate::memory::lexical::LexicalEntry> = Vec::new();
        for row in &rows {
            let scope_key = crate::memory::cognition::scope_partition_key(&row.memory.scope);
            match row.memory.state {
                MemoryState::Active => {
                    if let Some(emb) = &row.embedding {
                        // A rejected vector (usually `embedding.dimension` not matching the
                        // provider's actual width) would otherwise leave the memory silently
                        // absent from the vector arm — search degrades to BM25-only with nothing
                        // anywhere saying why.
                        if let Err(e) = self
                            .memory_index
                            .upsert(&scope_key, &row.memory.to_semantic_entry(emb.clone()))
                            .await
                        {
                            metrics::counter!("ecphoria_memory_vector_index_failures_total")
                                .increment(1);
                            tracing::warn!(
                                error = %e, id = %row.memory.id, dim = emb.len(),
                                "vector index upsert rejected — this memory is not searchable by \
                                 vector; check embedding.dimension matches the provider"
                            );
                        }
                    }
                    lexical_upserts.push(crate::memory::lexical::LexicalEntry {
                        id: row.memory.id,
                        scope_key,
                        project: row.memory.project.clone(),
                        subject: row.memory.subject.clone(),
                        content: row.memory.content.clone(),
                    });
                }
                _ => self.forget_indexes(row.memory.id).await,
            }
            self.publish_memory_change(&row.memory);
        }
        if let Err(e) = self.lexical_index.upsert_batch(&lexical_upserts) {
            tracing::warn!(error = %e, rows = lexical_upserts.len(), "lexical batch upsert failed");
        }
        metrics::counter!("ecphoria_memories_added_total").increment(rows.len() as u64);
        Ok(rows.len() as u64)
    }

    /// Ingest (or re-ingest) a Markdown document as a set of independently-addressed chunks.
    ///
    /// This is what makes "documentation that evolves" work as a first-class case rather than a
    /// pile of duplicates. The document is split on its heading hierarchy
    /// ([`crate::ingest::chunk`]) and each section is stored as its own memory keyed by a stable
    /// subject — `docs/deployment.md#Kubernetes > Production Values`. Re-importing the file then
    /// routes each section through the *existing* deterministic contradiction path:
    ///
    /// | the section is… | outcome |
    /// |---|---|
    /// | unchanged | `Confirmed` — importance bumped, no new row |
    /// | edited | `Superseded` — the previous text stays queryable via history / `as_of` |
    /// | new | `Inserted` |
    /// | deleted from the file | expired by the sweep below |
    ///
    /// So a runbook keeps its history *per section*, and "what did this say about failover in
    /// March" is answerable. No new cognition was needed for that — only a stable subject key.
    ///
    /// The sweep is the one piece the subject path cannot do by itself: a section that disappears
    /// produces no memory, so nothing supersedes its previous version and it would linger as a
    /// stale active fact. Every previously-active subject under this document that the new version
    /// no longer produces is therefore expired (not deleted — it stays in history).
    ///
    /// `valid_from` sets the **valid-time** boundary for every section written by this import —
    /// pass the source commit's date and the timeline reflects when the *documentation* changed
    /// rather than when the importer ran. Transaction time (`created_at`/`updated_at`) stays
    /// wall-clock either way, so "what was true at T" and "what did we know at T" stay separable.
    pub async fn document_ingest(
        &self,
        doc: DocumentIngest<'_>,
        scope: &MemoryScope,
        opts: &crate::ingest::chunk::ChunkOptions,
    ) -> Result<DocumentIngestResult> {
        use crate::ingest::chunk::chunk_markdown;
        let DocumentIngest {
            path: doc_path,
            content,
            metadata,
            valid_from,
            project,
        } = doc;
        let project = project.map(str::to_string);

        let chunks = chunk_markdown(content, opts);
        let inputs: Vec<MemoryInput> = chunks
            .iter()
            .map(|(chunk, part)| {
                let mut meta = metadata.clone();
                if let Some(obj) = meta.as_object_mut() {
                    obj.insert("doc_path".into(), doc_path.into());
                    // A section's origin is the document it was cut from. Set unless the importer
                    // said something more precise, so `require_provenance` does not turn the
                    // document importer off.
                    obj.entry("provenance").or_insert_with(
                        || serde_json::json!({"source": "document", "ref": doc_path}),
                    );
                    obj.insert("heading_path".into(), chunk.heading_path.clone().into());
                    obj.insert("ordinal".into(), chunk.ordinal.into());
                }
                MemoryInput {
                    scope: scope.clone(),
                    subject: Some(chunk.subject(doc_path, Some(*part))),
                    content: chunk.text.clone(),
                    importance: None,
                    source_event_ids: vec![],
                    metadata: meta,
                    mem_type: Some("chunk".into()),
                    valid_from,
                    project: project.clone(),
                }
            })
            .collect();

        // Which subjects this version produces — normalized the same way the store will store
        // them, so the sweep compares like with like.
        let produced: std::collections::HashSet<String> = inputs
            .iter()
            .filter_map(|i| i.subject.as_deref())
            .map(crate::memory::cognition::normalize_subject)
            .collect();

        let previous = self
            .memory_store
            .find_active_by_document(scope, doc_path)
            .await?;

        let added = self.memory_add_batch(inputs).await?;
        let mut result = DocumentIngestResult {
            path: doc_path.to_string(),
            chunks: chunks.len(),
            ..Default::default()
        };
        for a in &added {
            match a.outcome {
                MemoryOutcome::Inserted => result.inserted += 1,
                MemoryOutcome::Confirmed => result.confirmed += 1,
                MemoryOutcome::Superseded | MemoryOutcome::Merged => result.superseded += 1,
                MemoryOutcome::Conflict => {}
            }
        }

        // Sweep: expire sections that no longer exist in the source.
        let now = chrono::Utc::now();
        let stale: Vec<MemoryRow> = previous
            .into_iter()
            .filter(|m| m.subject.as_deref().is_some_and(|s| !produced.contains(s)))
            .map(|mut m| {
                m.state = MemoryState::Expired;
                m.valid_to = Some(now);
                m.updated_at = now;
                MemoryRow {
                    memory: m,
                    embedding: None,
                }
            })
            .collect();
        result.removed = stale.len();
        if !stale.is_empty() {
            self.memory_apply_rows_batch(stale).await?;
        }
        Ok(result)
    }

    /// Pick the valid-time instant at which a new version takes over from `previous`.
    ///
    /// A supersession splits the timeline: the old version's `valid_to` and the new one's
    /// `valid_from` are the same instant. If that instant were not **strictly after** the old
    /// version's own `valid_from`, the old version would occupy a zero-width interval — present in
    /// the history listing but unreachable by any `as_of(T)` query, which is a silent hole in
    /// exactly the guarantee the bi-temporal layer exists to provide.
    ///
    /// That is not hypothetical: re-importing a document edited but not yet committed reports the
    /// same commit date for both versions. When the supplied valid-time cannot separate them, the
    /// honest answer is that the change became known *now*, so transaction time is used instead.
    ///
    /// Consequence worth knowing: backfill must run **oldest-first**. Replaying history in reverse
    /// would push every version to now rather than splitting intervals around it — proper
    /// out-of-order valid-time insertion needs interval splitting, which this does not do.
    fn valid_boundary(
        requested: Option<chrono::DateTime<chrono::Utc>>,
        previous: Option<chrono::DateTime<chrono::Utc>>,
        recorded_at: chrono::DateTime<chrono::Utc>,
    ) -> chrono::DateTime<chrono::Utc> {
        let candidate = requested.unwrap_or(recorded_at);
        match previous {
            Some(prev) if candidate <= prev => {
                recorded_at.max(prev + chrono::Duration::microseconds(1))
            }
            _ => candidate,
        }
    }

    /// Write memories for the webhook events that represent a durable outcome.
    ///
    /// Called alongside episodic ingest, not instead of it: the full event stream stays queryable
    /// in SQL, and only the closes/merges/resolutions additionally become recallable facts. Off
    /// unless `memory.promotion.enabled`.
    ///
    /// Promoted memories are keyed by a deterministic subject (`acme/api#pr-42`), so a webhook
    /// redelivery — which providers do freely — resolves as `Confirmed` rather than a duplicate,
    /// and a ticket that reopens and closes again supersedes itself into a real history.
    ///
    /// Best-effort by design: a promotion failure must not fail the webhook, or a provider will
    /// retry the whole delivery and duplicate the episodic events.
    pub async fn promote_events(&self, tenant: &str, events: &[Event]) -> usize {
        let cfg = &self.config.memory.promotion;
        if !cfg.enabled {
            return 0;
        }
        let scope = MemoryScope::tenant(tenant);
        let inputs: Vec<MemoryInput> = events
            .iter()
            .filter(|e| crate::ingest::promote::matches(e, &cfg.rules))
            .filter_map(|e| {
                crate::ingest::promote::promote(e).map(|p| MemoryInput {
                    scope: scope.clone(),
                    subject: Some(p.subject),
                    content: p.content,
                    importance: None,
                    // Provenance: the promoted fact points back at the event it came from, so
                    // `/memories/{id}/provenance` shows the webhook delivery behind it.
                    source_event_ids: vec![e.id],
                    metadata: serde_json::json!({
                        "source": e.source,
                        "event_type": e.event_type,
                        "promoted": true,
                    }),
                    mem_type: Some("episodic".into()),
                    valid_from: Some(e.timestamp),
                    project: None,
                })
            })
            .collect();
        if inputs.is_empty() {
            return 0;
        }
        let n = inputs.len();
        match self.memory_add_batch(inputs).await {
            Ok(added) => added.len(),
            Err(e) => {
                tracing::warn!(error = %e, events = n, "promoting webhook events to memories failed");
                0
            }
        }
    }

    /// Expire every document in `project` that the caller did not just import.
    ///
    /// `document_ingest`'s sweep is per-document: it removes sections that vanished from a file it
    /// was given. Nothing removes a *whole* file that vanished — deleted from the repository, moved,
    /// or newly excluded from import — because the importer simply stops mentioning it, and silence
    /// is indistinguishable from "not imported this run". The corpus then keeps answering from
    /// documentation that no longer exists.
    ///
    /// This closes that loop: the importer reports the full set of document paths it sent, and
    /// anything else in the project is expired (kept as history, as everywhere else).
    ///
    /// Only touches `mem_type = 'chunk'` in the given project, so hand-written facts and promoted
    /// tickets sharing the project are never swept by a documentation import.
    pub async fn document_prune(
        &self,
        scope: &MemoryScope,
        project: &str,
        keep_paths: &[String],
    ) -> Result<usize> {
        let keep: std::collections::HashSet<String> = keep_paths
            .iter()
            .map(|p| crate::memory::cognition::normalize_subject(p))
            .collect();
        let active = self
            .memory_store
            .list_project_chunks(scope, project)
            .await?;
        let now = chrono::Utc::now();
        let stale: Vec<MemoryRow> = active
            .into_iter()
            .filter(|m| {
                // A chunk's subject is `<doc path>#<heading trail>`; the document identity is the
                // part before the first `#`.
                m.subject
                    .as_deref()
                    .map(|s| s.split_once('#').map_or(s, |(p, _)| p))
                    .is_some_and(|path| !keep.contains(path))
            })
            .map(|mut m| {
                m.state = MemoryState::Expired;
                m.valid_to = Some(now);
                m.updated_at = now;
                MemoryRow {
                    memory: m,
                    embedding: None,
                }
            })
            .collect();
        let n = stale.len();
        if n > 0 {
            self.memory_apply_rows_batch(stale).await?;
            tracing::info!(
                project,
                expired = n,
                "pruned documents no longer in the source"
            );
        }
        Ok(n)
    }

    /// CDC: publish a memory's lifecycle change. Best-effort — no receivers means dropped.
    fn publish_memory_change(&self, memory: &Memory) {
        let event = match memory.state {
            MemoryState::Active => "upserted",
            MemoryState::Superseded => "superseded",
            MemoryState::Expired => "expired",
            MemoryState::Pending => "proposed",
        };
        let _ = self.memory_change_tx.send(MemoryChange {
            id: memory.id,
            tenant_id: memory.scope.tenant_id.clone(),
            user_id: memory.scope.user_id.clone(),
            event,
            subject: memory.subject.clone(),
        });
    }

    /// Drop a memory from **both** derived indexes.
    ///
    /// Every deletion path must go through this rather than calling `memory_index.delete`
    /// directly. The FTS5 index keeps its own copy of the memory's text, so an erasure that
    /// skipped it would leave the content readable on disk after a GDPR delete — and would let a
    /// stale candidate occupy a retrieval slot forever. Both removals are best-effort: the DuckDB
    /// row is already gone, and `/admin/reindex` converges the indexes.
    async fn forget_indexes(&self, id: uuid::Uuid) {
        let _ = self.memory_index.delete(id).await;
        let _ = self.lexical_index.remove(id);
    }

    /// Apply a materialized memory change-set: persist each row and maintain the vector index
    /// (active rows are (re)indexed when they carry an embedding; superseded/expired rows are
    /// removed from the index). Deterministic — used by both [`Self::memory_add`] and Raft apply.
    pub async fn memory_apply_rows(&self, rows: Vec<MemoryRow>) -> Result<u64> {
        let n = rows.len() as u64;
        for row in &rows {
            self.memory_store
                .upsert_raw(&row.memory, row.embedding.as_deref())
                .await?;
            let scope_key = crate::memory::cognition::scope_partition_key(&row.memory.scope);
            match row.memory.state {
                MemoryState::Active => {
                    if let Some(emb) = &row.embedding {
                        // A rejected vector (usually `embedding.dimension` not matching the
                        // provider's actual width) would otherwise leave the memory silently
                        // absent from the vector arm — search degrades to BM25-only with nothing
                        // anywhere saying why.
                        if let Err(e) = self
                            .memory_index
                            .upsert(&scope_key, &row.memory.to_semantic_entry(emb.clone()))
                            .await
                        {
                            metrics::counter!("ecphoria_memory_vector_index_failures_total")
                                .increment(1);
                            tracing::warn!(
                                error = %e, id = %row.memory.id, dim = emb.len(),
                                "vector index upsert rejected — this memory is not searchable by \
                                 vector; check embedding.dimension matches the provider"
                            );
                        }
                    }
                    // Lexical index is best-effort (like the vector index): a failure degrades
                    // recall until the next reindex, it must never fail the write.
                    if let Err(e) = self.lexical_index.upsert(
                        row.memory.id,
                        &scope_key,
                        row.memory.project.as_deref(),
                        row.memory.subject.as_deref(),
                        &row.memory.content,
                    ) {
                        tracing::warn!(error = %e, id = %row.memory.id, "lexical index upsert failed");
                    }
                }
                _ => self.forget_indexes(row.memory.id).await,
            }

            self.publish_memory_change(&row.memory);

            // Auto-populate graph edges from the memory's content: deterministic triple extraction
            // + uuidv5 edge ids derived from the memory id, so every replica produces the identical
            // graph during apply (no payload change needed); idempotent via add_edge ON CONFLICT.
            if self.config.memory.cognition.auto_graph && row.memory.state == MemoryState::Active {
                let mem = &row.memory;
                let tenant = if mem.scope.tenant_id.is_empty() {
                    "default"
                } else {
                    mem.scope.tenant_id.as_str()
                };
                for (s, r, o) in crate::memory::cognition::extract_triples(&mem.content) {
                    let id = uuid::Uuid::new_v5(&mem.id, format!("{s}|{r}|{o}").as_bytes());
                    let edge = crate::memory::cognition::Edge {
                        id,
                        src: s,
                        relation: r,
                        dst: o,
                        weight: 1.0,
                        source_memory_id: Some(mem.id),
                        valid_from: Some(mem.valid_from),
                        ..Default::default()
                    };
                    let _ = self.memory_store.add_edge(tenant, &edge).await;
                }
            }
        }
        Ok(n)
    }

    /// Scoped semantic search over the memory vector index.
    async fn memory_index_search(
        &self,
        vector: &[f32],
        scope: &MemoryScope,
        k: usize,
    ) -> Result<Vec<MemoryHit>> {
        // The scope is the partition key: the search only ever traverses this scope's vectors, so
        // there is no oversample/post-filter (and no cross-scope starvation).
        let key = crate::memory::cognition::scope_partition_key(scope);
        let results = self.memory_index.search(&key, vector, k).await?;
        let mut hits = Vec::with_capacity(results.len());
        for r in results {
            if let Some(mem) = self.memory_store.get(r.entry.id).await? {
                if mem.state == MemoryState::Active {
                    hits.push(MemoryHit {
                        memory: mem,
                        score: r.score,
                        similarity: Some(r.score),
                        lexical: None,
                    });
                }
            }
        }
        Ok(hits)
    }

    /// Reciprocal Rank Fusion constant (standard default).
    const MEMORY_RRF_K: f32 = 60.0;

    /// Hybrid search over a scope's memories: deterministic BM25 lexical ranking fused
    /// (via Reciprocal Rank Fusion) with vector search when an embedding provider is
    /// configured. Lexical ranking is always on — so quality beats pure-recency even with
    /// no provider, and no external/FTS dependency is required. Empty query → recency.
    pub async fn memory_search(
        &self,
        query: &str,
        scope: &MemoryScope,
        k: usize,
    ) -> Result<Vec<MemoryHit>> {
        self.memory_search_in_project(query, scope, k, None).await
    }

    /// [`Self::memory_search`] restricted to one project, or across all of them when `None`.
    ///
    /// Projects share a scope on purpose. The scope tuple is an exact match, so putting a project
    /// there would isolate projects *and* make cross-project search impossible — which is the
    /// situation the `memory_grants` mechanism exists to work around, and it does so by running one
    /// search per grantor and concatenating the lists. Concatenation is not fusion: results from
    /// different scopes are never ranked against each other.
    ///
    /// Keeping one scope and filtering instead means a team gets both: `Some("payments")` narrows
    /// to one project, `None` searches everything with a single RRF fusion over the whole corpus.
    pub async fn memory_search_in_project(
        &self,
        query: &str,
        scope: &MemoryScope,
        k: usize,
        project: Option<&str>,
    ) -> Result<Vec<MemoryHit>> {
        self.memory_search_filtered(query, scope, k, project, None)
            .await
    }

    /// [`Self::memory_search_in_project`] with an optional floor on vector similarity.
    ///
    /// A caller often wants to know whether the corpus covers a question at all, and the fused
    /// `score` cannot answer that — it is rank-derived. `min_similarity` is the closest available
    /// proxy, and it is deliberately opt-in with no default because it is **a weak separator**:
    /// measured on the reference corpus, questions the documentation answers score p10 0.63 /
    /// p50 0.70, and questions it has nothing on score p10 0.56 / p50 0.60. Those overlap, so any
    /// threshold trades real answers for filtered noise rather than cleanly separating the two.
    ///
    /// Shipping a default would state a confidence the data does not support. The reliable signal
    /// remains the caller reading the returned `similarity` and content and judging.
    pub async fn memory_search_filtered(
        &self,
        query: &str,
        scope: &MemoryScope,
        k: usize,
        project: Option<&str>,
        min_similarity: Option<f32>,
    ) -> Result<Vec<MemoryHit>> {
        let started = std::time::Instant::now();
        let outcome = self
            .memory_search_inner(query, scope, k, project, min_similarity)
            .await;
        if let (Some(log), Ok((hits, matched))) = (&self.query_log, &outcome) {
            let top = hits.first();
            log.record(QueryRecord {
                tenant_id: if scope.tenant_id.is_empty() {
                    "default".into()
                } else {
                    scope.tenant_id.clone()
                },
                user_id: scope.user_id.clone(),
                agent_id: scope.agent_id.clone(),
                project: project.map(str::to_string),
                query: Some(query.to_string()),
                k,
                results: hits.len(),
                // "Nothing matched" is the fact worth recording, and it is not the same as "zero
                // rows": with no lexical or vector hit the search still returns the most
                // important/recent memories, so an unanswerable question comes back full.
                matched: *matched,
                top_subject: top.and_then(|h| h.memory.subject.clone()),
                top_similarity: top.and_then(|h| h.similarity),
                duration_ms: started.elapsed().as_secs_f64() * 1000.0,
            });
        }
        outcome.map(|(hits, _)| hits)
    }

    /// Returns `(hits, matched)`. `matched` is false when neither the lexical nor the vector arm
    /// produced anything and the results are the importance/recency fallback — which is the
    /// difference between "here is your answer" and "here is something".
    async fn memory_search_inner(
        &self,
        query: &str,
        scope: &MemoryScope,
        k: usize,
        project: Option<&str>,
        min_similarity: Option<f32>,
    ) -> Result<(Vec<MemoryHit>, bool)> {
        use crate::memory::cognition::{lexical_rank, rrf_fuse_weighted};

        if query.trim().is_empty() || k == 0 {
            let mems = self.memory_store.list_active(scope, k.max(1)).await?;
            return Ok((
                mems.into_iter()
                    .map(|memory| MemoryHit {
                        memory,
                        score: 0.0,
                        similarity: None,
                        lexical: None,
                    })
                    .collect::<Vec<_>>(),
                false,
            ));
        }

        // Retrieval widths (configurable; read-path only). `scan_cap` is the candidate universe for
        // BOTH BM25 and the vector fetch (kept symmetric so RRF isn't dominated by the lexical arm).
        let cog = &self.config.memory.cognition;
        let scan_cap = cog.retrieval_scan_cap.max(k);
        // Fused candidates kept after RRF for the importance blend + rerank + top-k.
        let pool = if self.reranker.is_some() {
            self.config.rerank.candidates.max(cog.retrieval_pool)
        } else {
            cog.retrieval_pool.max(k)
        };

        // Lexical (BM25) ranking — always available, and the arm that has to scale: a knowledge
        // base is searched by rare exact terms (error codes, service names, env vars, ticket keys)
        // far more than by paraphrase.
        //
        // The FTS5 inverted index ranks the **whole** scope. The previous implementation scored
        // BM25 in Rust over `list_active(scope, scan_cap)` — the top `scan_cap` memories by
        // importance/recency — which made every memory below that cutoff unreachable by keyword,
        // however well it matched. `scan_cap` now bounds only how many ranked candidates are
        // carried forward, not how much of the corpus is considered.
        // It runs in two stages, because the two halves have different jobs:
        //
        //   1. **Candidate generation** — FTS5 proposes up to `scan_cap` matching ids from the
        //      *whole* scope. This is what removes the recall ceiling: previously the candidate
        //      set was `list_active(scope, scan_cap)`, the top memories by importance/recency, so
        //      anything below that cutoff could not be found by keyword however well it matched.
        //   2. **Scoring** — the candidates are then ranked by [`lexical_rank`], the in-Rust BM25
        //      that was already here. Measured on the KB eval set it ranks better than SQLite's
        //      `bm25()` on this corpus (its tokenizer drops stop words before computing document
        //      length, so prose is not penalised against terse reference docs), and it costs
        //      almost nothing: these rows have already been read.
        //
        // So the index decides *what is considered*, and the existing ranker decides *in what
        // order* — neither ceiling nor ranking regression.
        let scope_key = crate::memory::cognition::scope_partition_key(scope);
        let mut by_id: std::collections::HashMap<uuid::Uuid, Memory> =
            std::collections::HashMap::new();
        let candidates: Vec<Memory> = match self
            .lexical_index
            .search(&scope_key, project, query, scan_cap)
        {
            Ok(ranked) if !ranked.is_empty() => {
                let ids: Vec<uuid::Uuid> = ranked.into_iter().map(|(id, _)| id).collect();
                // Re-read from DuckDB under the active + exact-scope filter: the index is
                // advisory, so a stale entry costs a candidate slot and never a wrong result.
                self.memory_store.get_active_by_ids(&ids, scope).await?
            }
            Ok(_) => Vec::new(),
            Err(e) => {
                // Index unavailable → fall back to the capped scan so search still works, rather
                // than losing the lexical arm entirely. The project filter is re-applied here so a
                // degraded index cannot leak another project's memories into the results.
                tracing::warn!(error = %e, "lexical index search failed — falling back to scan");
                self.memory_store.list_active(scope, scan_cap).await?
            }
        };
        // Applies to both branches: the fallback scan is unfiltered, and the FTS index is advisory
        // so a stale entry could carry an out-of-date project.
        let candidates: Vec<Memory> = match project {
            Some(p) => candidates
                .into_iter()
                .filter(|m| m.project.as_deref() == Some(p))
                .collect(),
            None => candidates,
        };
        // Keep the per-arm signals, not just the ordering: the fused score is rank-derived and
        // says nothing about how well anything actually matched.
        let ranked = lexical_rank(query, &candidates);
        let mut lex_scores: std::collections::HashMap<uuid::Uuid, f32> =
            std::collections::HashMap::with_capacity(ranked.len());
        let mut lex_ids: Vec<uuid::Uuid> = Vec::with_capacity(ranked.len());
        for (i, score) in ranked {
            let id = candidates[i].id;
            lex_scores.insert(id, score);
            lex_ids.push(id);
        }
        by_id.extend(candidates.into_iter().map(|m| (m.id, m)));

        // Vector ranking — best-effort, only when embeddings are configured.
        let mut vec_ids: Vec<uuid::Uuid> = Vec::new();
        let mut vec_scores: std::collections::HashMap<uuid::Uuid, f32> =
            std::collections::HashMap::new();
        if !self.memory_index.is_empty() {
            let embedded = self.embed_text(query).await;
            if let Err(e) = &embedded {
                // The index has vectors but we couldn't embed the query → this search silently
                // falls back to BM25-only. Surface it instead of degrading quietly.
                metrics::counter!("ecphoria_memory_embed_failures_total", "op" => "query")
                    .increment(1);
                tracing::warn!(error = %e, "query embedding failed — search degraded to BM25-only");
            }
            if let Ok(vector) = embedded {
                // Fetch enough vector candidates to fill the fused pool (feeds the reranker) without
                // the cost of scanning the whole scope — measured neutral on recall@5 beyond this.
                if let Ok(hits) = self
                    .memory_index_search(&vector, scope, pool.max(k * 4))
                    .await
                {
                    for h in hits {
                        // USearch has no metadata filtering, so the project predicate is applied
                        // after the k-NN fetch. Over-fetching is already the norm here.
                        if project.is_some() && h.memory.project.as_deref() != project {
                            continue;
                        }
                        vec_ids.push(h.memory.id);
                        if let Some(sim) = h.similarity {
                            vec_scores.insert(h.memory.id, sim);
                        }
                        by_id.entry(h.memory.id).or_insert(h.memory);
                    }
                }
            }
        }

        // Graph expansion (read-path, gated): also pull memories connected by a knowledge-graph
        // edge to an entity mentioned in the query, surfacing facts lexical/vector retrieval miss.
        let mut graph_ids: Vec<uuid::Uuid> = Vec::new();
        if self.config.memory.cognition.graph_expansion {
            use crate::memory::cognition::tokenize;
            let tenant = if scope.tenant_id.is_empty() {
                "default"
            } else {
                scope.tenant_id.as_str()
            };
            if let Ok(edges) = self.memory_store.list_edges(tenant, scan_cap).await {
                let q_terms: std::collections::HashSet<String> =
                    tokenize(query).into_iter().collect();
                let mut seen = std::collections::HashSet::new();
                for e in &edges {
                    let Some(mid) = e.source_memory_id else {
                        continue;
                    };
                    let matches = tokenize(&e.src).iter().any(|t| q_terms.contains(t))
                        || tokenize(&e.dst).iter().any(|t| q_terms.contains(t));
                    if !matches || !seen.insert(mid) {
                        continue;
                    }
                    // Scope-safe: only surface a graph-linked memory if it is in this exact scope.
                    let mem = match by_id.get(&mid) {
                        Some(m) => Some(m.clone()),
                        None => self.memory_store.get(mid).await.ok().flatten(),
                    };
                    if let Some(m) = mem {
                        if m.state == MemoryState::Active && m.scope == *scope {
                            by_id.entry(mid).or_insert(m);
                            graph_ids.push(mid);
                        }
                    }
                }
            }
        }

        // Weighted-RRF arms: the vector arm gets `retrieval_vector_weight`, the lexical (BM25) and
        // graph arms share `retrieval_lexical_weight` (both keyword-derived). Defaults are 1/1 =
        // plain equal-weight RRF; raise the vector weight when the embedder is strong and BM25 noisy.
        let w_vec = cog.retrieval_vector_weight;
        let w_lex = cog.retrieval_lexical_weight;
        let mut rankings: Vec<(Vec<uuid::Uuid>, f32)> = Vec::new();
        if !vec_ids.is_empty() {
            rankings.push((vec_ids, w_vec));
        }
        if !lex_ids.is_empty() {
            rankings.push((lex_ids, w_lex));
        }
        if !graph_ids.is_empty() {
            rankings.push((graph_ids, w_lex));
        }

        // Nothing matched lexically or by vector → fall back to importance/recency.
        if rankings.is_empty() {
            let mems = self.memory_store.list_active(scope, k).await?;
            return Ok((
                mems.into_iter()
                    .filter(|m| project.is_none() || m.project.as_deref() == project)
                    .map(|memory| MemoryHit {
                        memory,
                        score: 0.0,
                        similarity: None,
                        lexical: None,
                    })
                    .collect::<Vec<_>>(),
                false,
            ));
        }

        // Over-fetch, then re-rank by relevance blended with importance + recency, so a recent or
        // important memory can outrank a marginally-more-relevant stale one. Weights are
        // configurable (0/0 = pure relevance): recall benchmarks want them low, "prefer fresh
        // facts" assistants want them higher.
        let w_imp = cog.retrieval_importance_weight;
        let w_rec = cog.retrieval_recency_weight;
        let fused = rrf_fuse_weighted(&rankings, Self::MEMORY_RRF_K, pool);
        let now = chrono::Utc::now();
        let mut scored: Vec<MemoryHit> = Vec::with_capacity(fused.len());
        for (id, rrf) in fused {
            let memory = match by_id.remove(&id) {
                Some(m) => m,
                None => match self.memory_store.get(id).await {
                    Ok(Some(m)) => m,
                    _ => continue,
                },
            };
            // Recency in [0,1] with a 30-day half-life; importance in [0,1].
            let age_days = (now - memory.updated_at).num_seconds().max(0) as f32 / 86_400.0;
            let recency = 0.5_f32.powf(age_days / 30.0);
            let score = rrf * (1.0 + w_imp * memory.importance + w_rec * recency);
            let similarity = vec_scores.get(&memory.id).copied();
            let lexical = lex_scores.get(&memory.id).copied();
            scored.push(MemoryHit {
                memory,
                score,
                similarity,
                lexical,
            });
        }
        if let Some(floor) = min_similarity {
            // A hit with no similarity was matched lexically only (or there is no vector arm at
            // all); dropping it would silently turn the filter into "vector-only search".
            scored.retain(|h| h.similarity.is_none_or(|s| s >= floor));
        }
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Optional second-stage reranking over the fused pool (read-path, best-effort): a
        // cross-encoder/LLM relevance model reorders the candidates. Failures (e.g. network)
        // degrade gracefully to the fused order computed above.
        if let Some(reranker) = &self.reranker {
            if !scored.is_empty() {
                let docs: Vec<String> = scored.iter().map(|h| h.memory.content.clone()).collect();
                match reranker.rerank(query, &docs).await {
                    Ok(rscores) if rscores.len() == scored.len() => {
                        for (hit, rs) in scored.iter_mut().zip(rscores) {
                            let age_days = (now - hit.memory.updated_at).num_seconds().max(0)
                                as f32
                                / 86_400.0;
                            let recency = 0.5_f32.powf(age_days / 30.0);
                            // Rerank relevance dominates; a tiny recency nudge only breaks ties.
                            hit.score = rs * (1.0 + 0.1 * recency);
                        }
                        scored.sort_by(|a, b| {
                            b.score
                                .partial_cmp(&a.score)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                    }
                    Ok(_) => {
                        tracing::warn!(
                            "reranker returned mismatched score count — keeping fused order"
                        )
                    }
                    Err(e) => tracing::debug!(error = %e, "reranker failed — keeping fused order"),
                }
            }
        }

        scored.truncate(k);
        Ok((scored, true))
    }

    // ── Cross-scope sharing (grants) ─────────────────────────────────

    /// Grant `grantee` read access to `grantor`'s memories within `tenant` (idempotent).
    pub async fn grant_share(
        &self,
        tenant: &str,
        grantee: &str,
        grantor: &str,
    ) -> Result<uuid::Uuid> {
        self.memory_store.add_grant(tenant, grantee, grantor).await
    }

    /// List the grants a `grantee` holds within `tenant` (whose memories they may read).
    pub async fn list_grants(
        &self,
        tenant: &str,
        grantee: &str,
    ) -> Result<Vec<crate::memory::cognition::Grant>> {
        self.memory_store.list_grants(tenant, grantee).await
    }

    /// Revoke a grant by id within `tenant`. Returns whether a row was removed.
    pub async fn revoke_grant(&self, tenant: &str, id: uuid::Uuid) -> Result<bool> {
        self.memory_store.revoke_grant(tenant, id).await
    }

    /// Search the caller's own memories **plus** any memories shared with them via a grant — the
    /// team/shared-memory read path. Runs the full hybrid search per scope (own + each grantor
    /// within the SAME tenant) and merges by best score. Grants never cross tenants, so this can
    /// only ever widen access within one tenant. Falls back to a plain [`Self::memory_search`] for
    /// a scope with no `user_id` (grants are user-to-user).
    pub async fn memory_search_shared(
        &self,
        query: &str,
        scope: &MemoryScope,
        k: usize,
    ) -> Result<Vec<MemoryHit>> {
        let mut hits = self.memory_search(query, scope, k).await?;
        if let Some(grantee) = scope.user_id.clone() {
            let tenant = if scope.tenant_id.is_empty() {
                "default"
            } else {
                scope.tenant_id.as_str()
            };
            // Resolve the readable grantor users through the pluggable authz backend (default:
            // LocalGrants over the tenant-strict grants table). A future ReBAC/SpiceDB backend
            // plugs in here with no change to the read path.
            let backend = self.authz.read().clone();
            for grantor in backend.granted_read_scopes(tenant, &grantee).await? {
                let grantor_scope = MemoryScope {
                    tenant_id: scope.tenant_id.clone(),
                    user_id: Some(grantor),
                    agent_id: None,
                    session_id: None,
                };
                hits.extend(self.memory_search(query, &grantor_scope, k).await?);
            }
            // Dedupe by memory id (keep the best score), then sort desc and take top-k.
            let mut best: std::collections::HashMap<uuid::Uuid, MemoryHit> =
                std::collections::HashMap::new();
            for h in hits {
                best.entry(h.memory.id)
                    .and_modify(|e| {
                        if h.score > e.score {
                            e.score = h.score;
                        }
                    })
                    .or_insert(h);
            }
            hits = best.into_values().collect();
            hits.sort_by(|a, b| b.score.total_cmp(&a.score));
            hits.truncate(k);
        }
        Ok(hits)
    }

    /// Get a memory by id.
    pub async fn memory_get(&self, id: uuid::Uuid) -> Result<Option<Memory>> {
        self.memory_store.get(id).await
    }

    /// List active memories in a scope (importance/recency order).
    pub async fn memory_all(&self, scope: &MemoryScope, limit: usize) -> Result<Vec<Memory>> {
        // Clamp to the configured cap so a caller can't request an unbounded result set (OOM).
        let limit = limit.min(self.config.query.max_rows);
        self.memory_store.list_active(scope, limit).await
    }

    /// List active memories for a scope with optional filters (`mem_type`/importance/date-range/
    /// metadata-key) and offset pagination — the paged, filterable counterpart of
    /// [`Self::memory_all`]. Read-only, tenant-scoped by the caller's `scope`.
    pub async fn memory_list(
        &self,
        scope: &MemoryScope,
        limit: usize,
        offset: usize,
        filter: &crate::memory::cognition::MemoryFilter,
    ) -> Result<Vec<Memory>> {
        let limit = limit.min(self.config.query.max_rows);
        self.memory_store
            .list_paged(scope, limit, offset, filter)
            .await
    }

    /// Memories a tenant has explicitly **published** (`metadata.published == true`) — the read set
    /// behind the opt-in public read-only view. The published predicate is applied in SQL (see
    /// [`MemoryStore::list_published`]) so the `limit` never truncates a published memory in favor of
    /// unrelated newer ones. Only the published subset is ever returned.
    pub async fn memory_published(&self, tenant: &str, limit: usize) -> Result<Vec<Memory>> {
        let cap = limit.min(self.config.query.max_rows);
        self.memory_store.list_published(tenant, cap).await
    }

    /// GDPR erasure: delete ALL of a tenant's data across every store — episodic events + sessions,
    /// memories + their vectors, agent state, and event embeddings. Sequential best-effort (the
    /// stores are independent engines, so it isn't a single transaction); returns a per-store
    /// summary. Idempotent.
    pub async fn delete_tenant(&self, tenant: &str) -> Result<serde_json::Value> {
        let events = self.episodic.delete_by_tenant(tenant).await?;
        let mem_ids = self.memory_store.delete_by_tenant(tenant).await?;
        for id in &mem_ids {
            self.forget_indexes(*id).await;
        }
        let state = self
            .state
            .delete_by_prefix(&format!("{tenant}{TENANT_AGENT_SEP}"))
            .await?;
        let vectors = self.semantic.delete_by_tenant(tenant).await?;
        Ok(serde_json::json!({
            "tenant": tenant,
            "events_deleted": events,
            "memories_deleted": mem_ids.len(),
            "state_deleted": state,
            "vectors_deleted": vectors,
        }))
    }

    /// GDPR erasure at the **person** level: delete a user's memories (and their vectors) within a
    /// tenant. The cognition layer is where user-scoped data lives (memories carry a first-class
    /// `user_id`); episodic events and agent state are NOT user-scoped (no `user_id` column), so
    /// they are out of scope here — the response states this explicitly so a data-controller isn't
    /// misled into thinking event payloads were scrubbed. To erase everything for a tenant, use
    /// [`Self::delete_tenant`].
    pub async fn delete_user(&self, tenant: &str, user_id: &str) -> Result<serde_json::Value> {
        let mem_ids = self.memory_store.delete_by_user(tenant, user_id).await?;
        for id in &mem_ids {
            self.forget_indexes(*id).await;
        }
        Ok(serde_json::json!({
            "tenant": tenant,
            "user_id": user_id,
            "memories_deleted": mem_ids.len(),
            "note": "episodic events and agent state are not user-scoped (no user_id column); \
                     erase user data carried in event payloads at the source, or use tenant erasure",
        }))
    }

    /// Full temporal history for a `(scope, subject)` — every version, oldest first. The subject is
    /// normalized to match the canonical key used at write time.
    pub async fn memory_history(&self, scope: &MemoryScope, subject: &str) -> Result<Vec<Memory>> {
        let subject = crate::memory::cognition::normalize_subject(subject);
        self.memory_store.history(scope, &subject).await
    }

    /// The memory that was valid for a `(scope, subject)` at instant `at` (bi-temporal). The subject
    /// is normalized to match the canonical key used at write time.
    pub async fn memory_as_of(
        &self,
        scope: &MemoryScope,
        subject: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Memory>> {
        let subject = crate::memory::cognition::normalize_subject(subject);
        self.memory_store.as_of(scope, &subject, at).await
    }

    /// The contradiction review queue for a scope: subjects that currently have **more than one**
    /// active memory (only possible under `cognition.contradiction_review`, where contradictions are
    /// not auto-superseded). Each group is the set of conflicting active memories for one subject,
    /// awaiting human resolution via [`Self::memory_resolve_plan`].
    pub async fn memory_contradictions(
        &self,
        scope: &MemoryScope,
    ) -> Result<Vec<ContradictionGroup>> {
        let actives = self
            .memory_store
            .list_active(scope, self.config.query.max_rows)
            .await?;
        let mut by_subject: std::collections::HashMap<String, Vec<Memory>> =
            std::collections::HashMap::new();
        for m in actives {
            if let Some(subj) = m.subject.clone() {
                by_subject.entry(subj).or_default().push(m);
            }
        }
        let mut groups: Vec<ContradictionGroup> = by_subject
            .into_iter()
            .filter(|(_, ms)| ms.len() > 1)
            .map(|(subject, memories)| ContradictionGroup { subject, memories })
            .collect();
        groups.sort_by(|a, b| a.subject.cmp(&b.subject));
        Ok(groups)
    }

    /// Resolve a contradiction by keeping `keep_id` and superseding the other active memories for
    /// `(scope, subject)`. Returns the materialized superseded rows (apply with
    /// [`Self::memory_apply_rows`] locally, or replicate as `MemoryUpsert` in cluster mode). Errors
    /// if `keep_id` is not one of the subject's active memories (fail-closed — no partial resolve).
    pub async fn memory_resolve_plan(
        &self,
        scope: &MemoryScope,
        subject: &str,
        keep_id: uuid::Uuid,
    ) -> Result<Vec<MemoryRow>> {
        let subject = crate::memory::cognition::normalize_subject(subject);
        // A subject can now exist in several projects, so the group being resolved is the one the
        // kept memory belongs to. Deriving it from `keep_id` keeps the signature stable and makes
        // it impossible for the filter to disagree with the memory the caller chose.
        let project = self
            .memory_store
            .get(keep_id)
            .await?
            .and_then(|m| m.project);
        let actives = self
            .memory_store
            .find_active_by_subject(scope, &subject, project.as_deref())
            .await?;
        if !actives.iter().any(|m| m.id == keep_id) {
            return Err(crate::Error::State(format!(
                "keep_id {keep_id} is not an active memory for subject '{subject}' in this scope"
            )));
        }
        let now = chrono::Utc::now();
        let rows = actives
            .into_iter()
            .filter(|m| m.id != keep_id)
            .map(|mut loser| {
                loser.state = MemoryState::Superseded;
                loser.valid_to = Some(now);
                loser.updated_at = now;
                loser.supersedes = Some(keep_id);
                MemoryRow {
                    memory: loser,
                    embedding: None,
                }
            })
            .collect();
        Ok(rows)
    }

    /// Delete a memory (and its vector).
    pub async fn memory_delete(&self, id: uuid::Uuid) -> Result<()> {
        self.forget_indexes(id).await;
        self.memory_store.delete(id).await
    }

    /// Expire memories by id (bi-temporal soft-delete + drop their vectors). Deterministic — used by
    /// Raft apply to replicate consolidation's retirement of the folded originals.
    pub async fn memory_expire(&self, ids: &[uuid::Uuid]) -> Result<()> {
        for id in ids {
            // Capture scope before expiring so the CDC event carries tenant/user/subject.
            let meta = self.memory_store.get(*id).await.ok().flatten();
            let _ = self.memory_store.expire(*id).await;
            self.forget_indexes(*id).await;
            if let Some(m) = meta {
                let _ = self.memory_change_tx.send(MemoryChange {
                    id: *id,
                    tenant_id: m.scope.tenant_id.clone(),
                    user_id: m.scope.user_id.clone(),
                    event: "expired",
                    subject: m.subject.clone(),
                });
            }
        }
        Ok(())
    }

    /// Subscribe to the memory CDC stream (created/superseded/expired). Receives every memory
    /// lifecycle change across scopes; consumers filter by tenant. A slow consumer that lags is
    /// signalled via `RecvError::Lagged` (bounded 1024-event buffer).
    pub fn memory_subscribe(&self) -> tokio::sync::broadcast::Receiver<MemoryChange> {
        self.memory_change_tx.subscribe()
    }

    /// Plan a feedback action on a memory — the read side of the feedback loop, materialized so the
    /// cluster leader can replicate a deterministic change (rows to upsert / ids to expire) through
    /// Raft instead of re-deriving it per node. Scoped: `None` if the id isn't the tenant's.
    ///
    /// - `Helpful`  → reinforce (bump importance toward 1.0, preserving the vector).
    /// - `Wrong`/`Obsolete` → retire (bi-temporal expire + drop the vector).
    pub async fn memory_feedback_plan(
        &self,
        id: uuid::Uuid,
        tenant: Option<&str>,
        verdict: MemoryFeedback,
    ) -> Result<Option<(Memory, FeedbackAction)>> {
        let memory = match tenant {
            Some(t) => self.memory_store.get_scoped(id, t).await?,
            None => self.memory_store.get(id).await?,
        };
        let Some(mut memory) = memory else {
            return Ok(None);
        };
        match verdict {
            MemoryFeedback::Helpful => {
                // Reinforce: importance drifts up (capped), version bumps. Keep the existing vector
                // so re-indexing doesn't NULL it.
                memory.importance = (memory.importance + 0.1).min(1.0);
                memory.version += 1;
                memory.updated_at = chrono::Utc::now();
                let embedding = self.memory_store.get_embedding(memory.id).await?;
                Ok(Some((
                    memory.clone(),
                    FeedbackAction::Reinforce(vec![MemoryRow { memory, embedding }]),
                )))
            }
            MemoryFeedback::Wrong | MemoryFeedback::Obsolete => Ok(Some((
                memory.clone(),
                FeedbackAction::Retire(vec![memory.id]),
            ))),
        }
    }

    /// Apply a planned feedback action locally (single-node path; the cluster path replicates the
    /// equivalent `MemoryUpsert`/`MemoryExpire` through Raft instead).
    pub async fn memory_feedback_apply(&self, action: FeedbackAction) -> Result<()> {
        match action {
            FeedbackAction::Reinforce(rows) => {
                self.memory_apply_rows(rows).await?;
                Ok(())
            }
            FeedbackAction::Retire(ids) => self.memory_expire(&ids).await,
        }
    }

    // ── Governed writes: propose → review → accept/reject ──────────────────────────────

    /// Record a **proposal**: a memory a client may suggest but not decide.
    ///
    /// Deliberately not the cognition path. A proposal changes no belief, so it supersedes
    /// nothing, merges with nothing, and gets no vector: it is written `pending` and is invisible
    /// to retrieval until someone accepts it. That is the point — an agent that can write
    /// directly into memory can talk itself into anything on the next run.
    pub async fn memory_propose(&self, mut input: MemoryInput) -> Result<Memory> {
        if input.scope.tenant_id.is_empty() {
            input.scope.tenant_id = "default".into();
        }
        // Normalize here too, not only on the accept path: the proposal's subject is what a
        // reviewer reads and what `memory_pending` groups by, and a proposal whose key differs in
        // casing from the memory it would supersede reads as unrelated to the one fact it is about.
        input.subject = input
            .subject
            .map(|s| crate::memory::cognition::normalize_subject(&s))
            .filter(|s| !s.is_empty());
        // A proposal is still a write: an agent that may only suggest should not be the one path
        // that escapes the tenant's rules, or `warn`/`strict` would be trivially bypassed by
        // proposing and accepting.
        self.check_governance(
            &input.scope,
            input.subject.as_deref(),
            &input.metadata,
            &input.source_event_ids,
        )?;
        let now = chrono::Utc::now();
        let memory = Memory {
            id: uuid::Uuid::new_v4(),
            scope: input.scope,
            subject: input.subject,
            content: input.content,
            importance: input.importance.unwrap_or(0.5).clamp(0.0, 1.0),
            valid_from: input.valid_from.unwrap_or(now),
            valid_to: None,
            state: MemoryState::Pending,
            supersedes: None,
            source_event_ids: input.source_event_ids,
            version: 1,
            created_at: now,
            updated_at: now,
            metadata: input.metadata,
            mem_type: input.mem_type.unwrap_or_else(|| "semantic".into()),
            project: input.project,
        };
        self.memory_store.upsert_raw(&memory, None).await?;
        self.publish_memory_change(&memory);
        metrics::counter!("ecphoria_memory_proposed_total").increment(1);
        Ok(memory)
    }

    /// The review queue for a scope: proposals waiting for a decision, oldest first.
    pub async fn memory_pending(&self, scope: &MemoryScope, limit: usize) -> Result<Vec<Memory>> {
        let limit = limit.min(self.config.query.max_rows);
        self.memory_store.list_pending(scope, limit).await
    }

    /// Accept a proposal: it goes through the **normal** cognition path, so it supersedes what it
    /// contradicts exactly like a direct write, and the proposal row is retired.
    ///
    /// `Ok(None)` means the id is not a pending proposal of this tenant — already decided, never
    /// existed, or someone else's. Idempotent in effect: a second accept finds nothing pending.
    pub async fn memory_accept(&self, id: uuid::Uuid, tenant: &str) -> Result<Option<MemoryAdd>> {
        let Some(proposal) = self.pending_of(id, tenant).await? else {
            return Ok(None);
        };
        let input = MemoryInput {
            scope: proposal.scope.clone(),
            subject: proposal.subject.clone(),
            content: proposal.content.clone(),
            importance: Some(proposal.importance),
            source_event_ids: proposal.source_event_ids.clone(),
            metadata: proposal.metadata.clone(),
            mem_type: Some(proposal.mem_type.clone()),
            valid_from: Some(proposal.valid_from),
            project: proposal.project.clone(),
        };
        let added = self.memory_add(input).await?;
        self.retire_proposal(proposal, "accepted", Some(added.memory.id))
            .await?;
        metrics::counter!("ecphoria_memory_proposals_resolved_total", "decision" => "accepted")
            .increment(1);
        Ok(Some(added))
    }

    /// Reject a proposal. The row is kept, expired, with the decision recorded in its metadata:
    /// a rejected proposal is evidence of a judgement, and deleting it loses that.
    pub async fn memory_reject(&self, id: uuid::Uuid, tenant: &str) -> Result<bool> {
        let Some(proposal) = self.pending_of(id, tenant).await? else {
            return Ok(false);
        };
        self.retire_proposal(proposal, "rejected", None).await?;
        metrics::counter!("ecphoria_memory_proposals_resolved_total", "decision" => "rejected")
            .increment(1);
        Ok(true)
    }

    /// The pending proposal `id` of `tenant`, or `None` if it is not pending / not theirs.
    async fn pending_of(&self, id: uuid::Uuid, tenant: &str) -> Result<Option<Memory>> {
        Ok(self
            .memory_store
            .get_scoped(id, tenant)
            .await?
            .filter(|m| m.state == MemoryState::Pending))
    }

    async fn retire_proposal(
        &self,
        mut proposal: Memory,
        decision: &str,
        accepted_as: Option<uuid::Uuid>,
    ) -> Result<()> {
        let now = chrono::Utc::now();
        proposal.state = MemoryState::Expired;
        proposal.valid_to = Some(now);
        proposal.updated_at = now;
        proposal.version += 1;
        if let Some(map) = proposal.metadata.as_object_mut() {
            map.insert("review_decision".into(), serde_json::json!(decision));
            map.insert("review_decided_at".into(), serde_json::json!(now));
            if let Some(accepted) = accepted_as {
                map.insert(
                    "review_accepted_as".into(),
                    serde_json::json!(accepted.to_string()),
                );
            }
        }
        self.memory_store.upsert_raw(&proposal, None).await?;
        self.publish_memory_change(&proposal);
        Ok(())
    }

    /// Get a memory by id, scoped to a tenant (None if owned by another tenant).
    pub async fn memory_get_scoped(&self, id: uuid::Uuid, tenant: &str) -> Result<Option<Memory>> {
        self.memory_store.get_scoped(id, tenant).await
    }

    /// Provenance for a memory — "why do you believe this?".
    ///
    /// Assembles the answer from data already on the memory: (1) the memory itself, (2) the
    /// **source episodic events** it was distilled from (`source_event_ids`, resolved to real
    /// events, tenant-scoped), and (3) its **supersession chain** — the full bi-temporal history
    /// for a subject-keyed memory (every version, oldest first), or the single row otherwise. Lets a
    /// deployment audit exactly what evidence backs an agent's answer. `None` if the id isn't the
    /// tenant's.
    pub async fn memory_provenance(
        &self,
        id: uuid::Uuid,
        tenant: Option<&str>,
    ) -> Result<Option<MemoryProvenance>> {
        let memory = match tenant {
            Some(t) => self.memory_store.get_scoped(id, t).await?,
            None => self.memory_store.get(id).await?,
        };
        let Some(memory) = memory else {
            return Ok(None);
        };

        // 1. Resolve source events (tenant-scoped so a forged id can't pull another tenant's event).
        let source_events = self
            .episodic
            .events_by_ids(&memory.source_event_ids, tenant)
            .await
            .unwrap_or_default();

        // 2. Supersession chain: the full subject history when keyed, else just this row.
        let history = match &memory.subject {
            Some(subject) => self.memory_store.history(&memory.scope, subject).await?,
            None => vec![memory.clone()],
        };

        Ok(Some(MemoryProvenance {
            memory,
            source_events,
            history,
        }))
    }

    /// Delete a memory by id, scoped to a tenant. Returns true iff a row was deleted.
    pub async fn memory_delete_scoped(&self, id: uuid::Uuid, tenant: &str) -> Result<bool> {
        let deleted = self.memory_store.delete_scoped(id, tenant).await?;
        if deleted {
            self.forget_indexes(id).await;
        }
        Ok(deleted)
    }

    /// Compute the change-set for a partial in-place correction of an **active** memory WITHOUT
    /// writing — the plan half of the plan/apply split (mirrors [`Self::memory_plan`]) so a cluster
    /// leader can replicate the result via `MemoryUpsert` and every node applies the identical row.
    /// Returns `None` if the id doesn't exist within `tenant`, or the memory isn't active
    /// (superseded/expired rows are immutable history). Editing `content` re-embeds; other fields
    /// keep the stored vector.
    pub async fn memory_update_plan(
        &self,
        id: uuid::Uuid,
        patch: crate::memory::cognition::MemoryPatch,
        tenant: Option<&str>,
    ) -> Result<Option<(Memory, Vec<MemoryRow>)>> {
        let existing = match tenant {
            Some(t) => self.memory_store.get_scoped(id, t).await?,
            None => self.memory_store.get(id).await?,
        };
        let Some(mut mem) = existing else {
            return Ok(None);
        };
        if mem.state != MemoryState::Active {
            return Ok(None);
        }

        let content_changed = patch
            .content
            .as_ref()
            .is_some_and(|c| c.trim() != mem.content.trim());
        if let Some(c) = patch.content {
            mem.content = c;
        }
        if let Some(imp) = patch.importance {
            mem.importance = imp.clamp(0.0, 1.0);
        }
        if let Some(mt) = patch.mem_type {
            mem.mem_type = mt;
        }
        if let Some(md) = patch.metadata {
            mem.metadata = md;
        }
        mem.version += 1;
        mem.updated_at = chrono::Utc::now();

        // Re-embed only when the content actually changed. On a re-embed failure, keep the stored
        // vector (search degrades to BM25 for this row rather than NULLing it — the same convention
        // as `memory_plan`'s confirmation path, and observable via the metric + warn).
        let embedding = if content_changed {
            match self.embedding.as_ref() {
                Some(_) => match self.embed_document_text(&mem.content).await {
                    Ok(v) => Some(v),
                    Err(e) => {
                        metrics::counter!("ecphoria_memory_embed_failures_total", "op" => "update")
                            .increment(1);
                        tracing::warn!(error = %e, id = %id, "memory re-embed failed on update — kept prior vector (search degraded)");
                        self.memory_store.get_embedding(id).await?
                    }
                },
                None => None,
            }
        } else {
            self.memory_store.get_embedding(id).await?
        };

        Ok(Some((
            mem.clone(),
            vec![MemoryRow {
                memory: mem,
                embedding,
            }],
        )))
    }

    /// Apply a partial correction locally (single-node path). In cluster mode call
    /// [`Self::memory_update_plan`] and replicate the returned rows via `MemoryUpsert` instead.
    /// Returns the updated memory, or `None` when it doesn't exist within `tenant` / isn't active.
    pub async fn memory_update(
        &self,
        id: uuid::Uuid,
        patch: crate::memory::cognition::MemoryPatch,
        tenant: Option<&str>,
    ) -> Result<Option<Memory>> {
        let Some((mem, rows)) = self.memory_update_plan(id, patch, tenant).await? else {
            return Ok(None);
        };
        self.memory_apply_rows(rows).await?;
        Ok(Some(mem))
    }

    /// Total memory count (all states).
    pub async fn memory_count(&self) -> Result<u64> {
        self.memory_store.count().await
    }

    /// Distinct memory scopes (user/agent/session) with active-memory counts, most-populated first —
    /// the memory counterpart of [`Self::list_agents`]/schema enumeration. Tenant-scoped when set.
    pub async fn memory_scopes(
        &self,
        tenant: Option<&str>,
    ) -> Result<Vec<crate::memory::cognition::MemoryScopeCount>> {
        self.memory_store.scopes(tenant).await
    }

    /// Export all active memories for a tenant (for moving a tenant between shards on a reshard).
    pub async fn export_tenant_memories(&self, tenant: &str) -> Result<Vec<Memory>> {
        self.memory_store
            .list_by_tenant(tenant, self.config.query.max_rows)
            .await
    }

    /// Import memories verbatim (ids/scope/timestamps preserved) — the receiving side of a tenant
    /// move. Vectors are rebuilt lazily (via reindex). Returns the count imported.
    pub async fn import_memories(&self, memories: &[Memory]) -> Result<usize> {
        for m in memories {
            self.memory_store.upsert_raw(m, None).await?;
        }
        Ok(memories.len())
    }

    /// Move a tenant's memories from this engine to `dest`, then remove them here (a rebalance move).
    /// Returns the number moved. Best-effort across two independent engines (not a single txn).
    pub async fn migrate_tenant_memories_to(
        &self,
        dest: &EcphoriaEngine,
        tenant: &str,
    ) -> Result<usize> {
        let memories = self.export_tenant_memories(tenant).await?;
        let n = dest.import_memories(&memories).await?;
        // Remove ONLY the moved memories (+ their vectors and graph edges) from the source — NOT the
        // tenant's episodic events / state. A cascade `delete_tenant` here would lose events/state
        // that were never copied to the destination.
        let removed = self.memory_store.delete_by_tenant(tenant).await?;
        for id in removed {
            self.forget_indexes(id).await;
        }
        Ok(n)
    }

    /// Export a tenant's FULL data (episodic events + memories + agent state) as a JSON snapshot, for
    /// moving the tenant to another shard. Counterpart: [`import_tenant`].
    pub async fn export_tenant(&self, tenant: &str) -> Result<serde_json::Value> {
        let events = self.episodic.events_by_tenant(tenant, 10_000_000).await?;
        let memories = self.memory_store.list_by_tenant(tenant, 10_000_000).await?;
        let state = self
            .state
            .export_by_prefix(&format!("{tenant}{TENANT_AGENT_SEP}"))
            .await?;
        Ok(serde_json::json!({
            "events": events,
            "memories": memories,
            "state": state,
        }))
    }

    /// Import a tenant snapshot produced by [`export_tenant`] (events re-ingested under `tenant`,
    /// memories upserted verbatim, state set with its tenant-prefixed agent ids). Returns counts.
    pub async fn import_tenant(
        &self,
        tenant: &str,
        snapshot: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let mut ev_count = 0u64;
        if let Some(events) = snapshot
            .get("events")
            .and_then(|v| serde_json::from_value::<Vec<Event>>(v.clone()).ok())
        {
            if !events.is_empty() {
                ev_count = self
                    .ingest_for_tenant(events, &crate::config::TenantContext::new(tenant))
                    .await?;
            }
        }
        let mut mem_count = 0usize;
        if let Some(mems) = snapshot
            .get("memories")
            .and_then(|v| serde_json::from_value::<Vec<Memory>>(v.clone()).ok())
        {
            mem_count = self.import_memories(&mems).await?;
        }
        let mut state_count = 0u64;
        if let Some(state) = snapshot.get("state").and_then(|v| v.as_array()) {
            for item in state {
                if let (Some(agent), Some(key), Some(value)) = (
                    item.get(0).and_then(|x| x.as_str()),
                    item.get(1).and_then(|x| x.as_str()),
                    item.get(2),
                ) {
                    // agent_id retains its tenant prefix → set verbatim on the raw store.
                    if self.state.set(agent, key, value.clone()).await.is_ok() {
                        state_count += 1;
                    }
                }
            }
        }
        Ok(serde_json::json!({
            "events": ev_count,
            "memories": mem_count,
            "state": state_count,
        }))
    }

    /// Move a tenant's FULL data (events + memories + state) from this engine to `dest`, then erase
    /// it here. Best-effort across two independent engines (not a single transaction).
    pub async fn migrate_tenant_to(
        &self,
        dest: &EcphoriaEngine,
        tenant: &str,
    ) -> Result<serde_json::Value> {
        let snapshot = self.export_tenant(tenant).await?;
        let imported = dest.import_tenant(tenant, &snapshot).await?;
        // Full move → the cascade delete is correct here (everything was copied to dest).
        self.delete_tenant(tenant).await?;
        Ok(imported)
    }

    /// Forget low-value memories via time-decay of importance (configurable half-life /
    /// threshold). Forgotten memories are expired (kept for history) and dropped from the
    /// vector index. Returns the number forgotten.
    pub async fn memory_enforce_decay(&self) -> Result<u64> {
        let cog = &self.config.memory.cognition;
        let forgotten = self
            .memory_store
            .decay(cog.decay_half_life_days, cog.forget_threshold)
            .await?;
        for id in &forgotten {
            self.forget_indexes(*id).await;
        }
        if !forgotten.is_empty() {
            tracing::info!(
                forgotten = forgotten.len(),
                "memory decay forgot low-value memories"
            );
        }
        Ok(forgotten.len() as u64)
    }

    /// Read-only: the memory ids that decay would forget right now (no write). The cluster leader
    /// replicates this set via `MemoryExpire` so every node forgets the same rows; single-node just
    /// applies it with [`Self::memory_expire`].
    pub async fn memory_decay_plan(&self) -> Result<Vec<uuid::Uuid>> {
        let cog = &self.config.memory.cognition;
        self.memory_store
            .decay_candidates(cog.decay_half_life_days, cog.forget_threshold)
            .await
    }

    /// System prompt for opt-in LLM fact extraction.
    const EXTRACT_SYSTEM: &'static str = "You extract atomic, durable facts from the user's text \
        for an agent memory store. Return ONLY a JSON array of objects with keys \"subject\" (a \
        short stable key like \"favorite_color\", or null) and \"content\" (the fact as a \
        standalone sentence). Extract only meaningful, lasting facts; ignore pleasantries. \
        Example: [{\"subject\":\"favorite_color\",\"content\":\"Their favorite color is blue\"}]";

    /// Remember raw text as one or more memories.
    ///
    /// When `cognition.extraction = "llm"` and a completion provider is configured, the text
    /// is distilled into atomic facts (each routed through [`Self::memory_add`], so dedup and
    /// contradiction resolution still apply). Otherwise the text is stored as a single memory
    /// (deterministic fallback — no LLM dependency).
    pub async fn memory_remember(
        &self,
        raw_text: &str,
        scope: &MemoryScope,
    ) -> Result<Vec<MemoryAdd>> {
        let facts = self.extract_facts(raw_text).await;
        let mut out = Vec::with_capacity(facts.len());
        for (subject, content) in facts {
            let mut input = MemoryInput::new(scope.clone(), content);
            input.subject = subject;
            out.push(self.memory_add(input).await?);
        }
        Ok(out)
    }

    /// Plan `remember` WITHOUT writing: extract facts (LLM or fallback) once on the leader and return
    /// one `(MemoryAdd, rows)` per fact, so cluster mode can replicate each through the Raft log
    /// (`MemoryUpsert`) — closing the gap where the MCP `remember` tool wrote only locally. Mirrors
    /// [`Self::session_distill_plan`].
    pub async fn memory_remember_plan(
        &self,
        raw_text: &str,
        scope: &MemoryScope,
    ) -> Result<Vec<(MemoryAdd, Vec<MemoryRow>)>> {
        let facts = self.extract_facts(raw_text).await;
        let mut out = Vec::with_capacity(facts.len());
        for (subject, content) in facts {
            let mut input = MemoryInput::new(scope.clone(), content);
            input.subject = subject;
            out.push(self.memory_plan(input).await?);
        }
        Ok(out)
    }

    /// Re-embed active memories with the currently-configured provider — run this after switching
    /// embedding model or dimension so existing memories are searchable under the new vectors.
    ///
    /// Fetches up to `limit` active memories (oldest-updated first, so repeated calls page forward),
    /// recomputes each vector from the memory's content, and returns the refreshed [`MemoryRow`]s
    /// (state unchanged = Active, so applying re-persists the row and re-indexes its vector). Writes
    /// nothing — the gateway applies locally via [`Self::memory_apply_rows`] or, in cluster mode,
    /// replicates the rows through Raft (`MemoryUpsert`) so every node re-indexes identically. Empty
    /// if no embedding provider is configured. A per-memory embedding failure skips that memory
    /// (leaving its old vector intact) rather than dropping it.
    pub async fn memory_reembed_plan(&self, limit: usize) -> Result<Vec<MemoryRow>> {
        if self.embedding.is_none() {
            return Ok(Vec::new());
        }
        let memories = self.memory_store.all_active(limit).await?;
        let mut rows = Vec::with_capacity(memories.len());
        for memory in memories {
            match self.embed_document_text(&memory.content).await {
                Ok(embedding) => rows.push(MemoryRow {
                    memory,
                    embedding: Some(embedding),
                }),
                Err(e) => {
                    metrics::counter!("ecphoria_memory_embed_failures_total", "op" => "reembed")
                        .increment(1);
                    tracing::warn!(id = %memory.id, error = %e, "re-embed failed; keeping prior vector");
                }
            }
        }
        Ok(rows)
    }

    /// Re-embed active memories and apply the refreshed vectors locally (single-node convenience).
    /// Returns the number of memories re-embedded. In cluster mode use [`Self::memory_reembed_plan`]
    /// on the leader and replicate the rows instead, so followers re-index the identical vectors.
    pub async fn memory_reembed(&self, limit: usize) -> Result<usize> {
        let rows = self.memory_reembed_plan(limit).await?;
        let n = rows.len();
        if n > 0 {
            self.memory_apply_rows(rows).await?;
        }
        Ok(n)
    }

    /// Consolidate a scope's lowest-importance memories into one summary memory.
    ///
    /// When the scope has more than `keep` active memories, the lowest-importance tail is summarized
    /// (LLM if `extraction = "llm"` + provider, else a deterministic bullet list), inserted as a new
    /// memory citing its sources in metadata, and the originals are expired (bi-temporal — history is
    /// kept). Returns the consolidated memory, or `None` if there was nothing to fold.
    pub async fn memory_consolidate(
        &self,
        scope: &MemoryScope,
        keep: usize,
    ) -> Result<Option<MemoryAdd>> {
        let Some((input, expired)) = self.memory_consolidate_plan(scope, keep).await? else {
            return Ok(None);
        };
        let added = self.memory_add(input).await?;
        self.memory_expire(&expired).await?;
        Ok(Some(added))
    }

    /// Compute a consolidation **without writing**: the summary memory input + the ids of the
    /// originals to expire. Returns None if the scope is within `keep`. The gateway uses this in
    /// cluster mode to replicate consolidation through the Raft log (summary `MemoryUpsert` + the
    /// originals via `MemoryExpire`) instead of applying it only locally.
    pub async fn memory_consolidate_plan(
        &self,
        scope: &MemoryScope,
        keep: usize,
    ) -> Result<Option<(MemoryInput, Vec<uuid::Uuid>)>> {
        let actives = self
            .memory_store
            .list_active(scope, self.config.query.max_rows)
            .await?;
        if actives.len() <= keep {
            return Ok(None);
        }
        // list_active is ordered importance DESC, so the tail past `keep` is the lowest-importance set.
        let to_fold: Vec<Memory> = actives[keep..].to_vec();
        if to_fold.is_empty() {
            return Ok(None);
        }
        let summary = self.summarize_memories(&to_fold).await;
        let source_ids: Vec<String> = to_fold.iter().map(|m| m.id.to_string()).collect();
        let mut input = MemoryInput::new(scope.clone(), summary);
        input.importance = Some(0.6);
        input.metadata = serde_json::json!({
            "consolidated": true,
            "source_memory_ids": source_ids,
            // The engine's own writes are attributable too — otherwise `require_provenance` would
            // refuse consolidation, which is Ecphoria refusing its own housekeeping.
            "provenance": {"source": "ecphoria:consolidation"},
        });
        let expired: Vec<uuid::Uuid> = to_fold.iter().map(|m| m.id).collect();
        Ok(Some((input, expired)))
    }

    /// Consolidate **semantically-similar** active memories in a scope into abstractions — the
    /// "fold near-duplicate clusters" half of consolidation (the importance-tail variant is
    /// [`Self::memory_consolidate`]). Greedily clusters active memories whose cosine similarity is
    /// ≥ `threshold`, folds each cluster (size ≥ 2) into one summary memory citing its sources, and
    /// expires the originals (bi-temporal — history kept). Applies locally; the plan variant
    /// [`Self::memory_consolidate_similar_plan`] is for cluster replication. Returns clusters folded.
    pub async fn memory_consolidate_similar(
        &self,
        scope: &MemoryScope,
        threshold: f32,
    ) -> Result<u64> {
        let plans = self
            .memory_consolidate_similar_plan(scope, threshold)
            .await?;
        let n = plans.len() as u64;
        for (input, expired) in plans {
            self.memory_add(input).await?;
            self.memory_expire(&expired).await?;
        }
        Ok(n)
    }

    /// Plan semantic-cluster consolidation without writing: one `(summary input, originals to
    /// expire)` per cluster. Empty when nothing clusters. Requires an embedding provider (memories
    /// without a vector are left untouched).
    pub async fn memory_consolidate_similar_plan(
        &self,
        scope: &MemoryScope,
        threshold: f32,
    ) -> Result<Vec<(MemoryInput, Vec<uuid::Uuid>)>> {
        let actives = self
            .memory_store
            .list_active(scope, self.config.query.max_rows)
            .await?;
        // Pair each memory with its vector; memories without one can't be clustered.
        let mut with_vec: Vec<(Memory, Vec<f32>)> = Vec::new();
        for m in actives {
            if let Some(v) = self.memory_store.get_embedding(m.id).await? {
                with_vec.push((m, v));
            }
        }

        let mut grouped: std::collections::HashSet<uuid::Uuid> = std::collections::HashSet::new();
        let mut plans = Vec::new();
        let k = self.config.memory.cognition.retrieval_scan_cap.max(8);
        for (m, v) in &with_vec {
            if grouped.contains(&m.id) {
                continue;
            }
            let hits = self.memory_index_search(v, scope, k).await?;
            let cluster: Vec<Memory> = hits
                .into_iter()
                .filter(|h| h.score >= threshold && !grouped.contains(&h.memory.id))
                .map(|h| h.memory)
                .collect();
            if cluster.len() < 2 {
                grouped.insert(m.id);
                continue;
            }
            for c in &cluster {
                grouped.insert(c.id);
            }
            let summary = self.summarize_memories(&cluster).await;
            let source_ids: Vec<String> = cluster.iter().map(|m| m.id.to_string()).collect();
            // The abstraction inherits the strongest importance in the cluster + provenance.
            let importance = cluster.iter().map(|m| m.importance).fold(0.0_f32, f32::max);
            let mut source_events: Vec<uuid::Uuid> = Vec::new();
            for c in &cluster {
                for e in &c.source_event_ids {
                    if !source_events.contains(e) {
                        source_events.push(*e);
                    }
                }
            }
            let mut input = MemoryInput::new(scope.clone(), summary);
            input.importance = Some(importance);
            input.source_event_ids = source_events;
            input.metadata = serde_json::json!({
                "consolidated": true,
                "consolidation": "semantic",
                "source_memory_ids": source_ids,
                "provenance": {"source": "ecphoria:consolidation"},
            });
            let expired: Vec<uuid::Uuid> = cluster.iter().map(|m| m.id).collect();
            plans.push((input, expired));
        }
        Ok(plans)
    }

    /// Summarize a set of memories (opt-in LLM, else a deterministic bullet list).
    async fn summarize_memories(&self, mems: &[Memory]) -> String {
        let joined = mems
            .iter()
            .map(|m| format!("- {}", m.content))
            .collect::<Vec<_>>()
            .join("\n");
        if self.config.memory.cognition.extraction == "llm" {
            if let Some(provider) = &self.completion {
                if let Ok(text) = provider
                    .complete(
                        "Summarize the following memories into a concise paragraph capturing the \
                         durable facts. Output only the summary.",
                        &joined,
                    )
                    .await
                {
                    let t = text.trim();
                    if !t.is_empty() {
                        return t.to_string();
                    }
                }
            }
        }
        format!("Consolidated {} memories:\n{}", mems.len(), joined)
    }

    /// Add a graph edge (entity → relation → entity) for a tenant.
    pub async fn memory_link(
        &self,
        tenant: &str,
        src: &str,
        relation: &str,
        dst: &str,
        source: Option<uuid::Uuid>,
    ) -> Result<()> {
        let edge = crate::memory::cognition::Edge {
            id: uuid::Uuid::new_v4(),
            src: src.to_string(),
            relation: relation.to_string(),
            dst: dst.to_string(),
            weight: 1.0,
            source_memory_id: source,
            valid_from: Some(chrono::Utc::now()),
            ..Default::default()
        };
        self.memory_store.add_edge(tenant, &edge).await
    }

    /// Edges incident to `entity` that were valid at instant `at` — the bi-temporal "what did the
    /// graph look like at time T" query (parallels [`Self::memory_as_of`] for memories).
    pub async fn memory_neighbors_as_of(
        &self,
        tenant: &str,
        entity: &str,
        at: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> Result<Vec<crate::memory::cognition::Edge>> {
        self.memory_store
            .neighbors_as_of(tenant, entity, at, limit.min(self.config.query.max_rows))
            .await
    }

    /// Deterministic apply primitive: close active edges matching `(tenant, src, relation)` as of
    /// `at`, attributing them to `by`. Carries `at`/`by` (no `now()`/uuid here) so every replica
    /// applies the identical close — used by Raft apply (`GraphSupersede`) and the single-node
    /// functional-link path. Returns how many edges were closed.
    pub async fn graph_supersede_apply(
        &self,
        tenant: Option<&str>,
        src: &str,
        relation: &str,
        at: chrono::DateTime<chrono::Utc>,
        by: Option<uuid::Uuid>,
    ) -> Result<usize> {
        self.memory_store
            .supersede_edges(tenant.unwrap_or("default"), src, relation, at, by)
            .await
    }

    /// Link a **functional** relation: close any active `(src, relation)` edge, then add the new
    /// one (so the graph holds only the latest value, with the old kept for as-of queries). The
    /// single-node counterpart of the cluster's `GraphSupersede` + `GraphAddEdge` pair.
    pub async fn memory_link_functional(
        &self,
        tenant: &str,
        src: &str,
        relation: &str,
        dst: &str,
        source: Option<uuid::Uuid>,
    ) -> Result<()> {
        let at = chrono::Utc::now();
        let id = uuid::Uuid::new_v4();
        self.memory_store
            .supersede_edges(tenant, src, relation, at, Some(id))
            .await?;
        let edge = crate::memory::cognition::Edge {
            id,
            src: src.to_string(),
            relation: relation.to_string(),
            dst: dst.to_string(),
            weight: 1.0,
            source_memory_id: source,
            valid_from: Some(at),
            ..Default::default()
        };
        self.memory_store.add_edge(tenant, &edge).await
    }

    /// Apply a fully-materialized graph edge (deterministic — used by Raft apply so every node
    /// inserts the identical row, edge id included).
    pub async fn graph_apply_edge(
        &self,
        tenant: Option<&str>,
        edge: &crate::memory::cognition::Edge,
    ) -> Result<()> {
        self.memory_store
            .add_edge(tenant.unwrap_or("default"), edge)
            .await
    }

    /// Edges incident to `entity` (its 1-hop neighborhood) for a tenant.
    pub async fn memory_neighbors(
        &self,
        tenant: &str,
        entity: &str,
        limit: usize,
    ) -> Result<Vec<crate::memory::cognition::Edge>> {
        self.memory_store
            .neighbors(tenant, entity, limit.min(self.config.query.max_rows))
            .await
    }

    /// List **all** active knowledge-graph edges for a tenant (capped), ordered deterministically.
    /// The bulk-graph read behind the markdown/Obsidian export and whole-graph views.
    pub async fn memory_edges(
        &self,
        tenant: &str,
        limit: usize,
    ) -> Result<Vec<crate::memory::cognition::Edge>> {
        self.memory_store
            .list_edges(tenant, limit.min(self.config.query.max_rows))
            .await
    }

    /// Edge triples `(src, dst, weight)` for graph analytics. With `as_of = None` this is the
    /// currently-active graph; with `Some(t)` it is the graph **as it was at time `t`** (bi-temporal:
    /// edges whose validity window contains `t`) — the temporal analytics Obsidian-class tools lack.
    async fn graph_triples(
        &self,
        tenant: &str,
        as_of: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<(String, String, f64)>> {
        let cap = self.config.query.max_rows;
        let edges = match as_of {
            None => self.memory_store.list_edges(tenant, cap).await?,
            Some(t) => self
                .memory_store
                .list_edges_all(tenant, cap)
                .await?
                .into_iter()
                .filter(|e| {
                    e.valid_from.map(|f| f <= t).unwrap_or(true)
                        && e.valid_to.map(|to| to > t).unwrap_or(true)
                })
                .collect(),
        };
        Ok(edges
            .into_iter()
            .map(|e| (e.src, e.dst, e.weight as f64))
            .collect())
    }

    /// Degree centrality + PageRank per graph node (optionally **as-of** a time). Ranks the most
    /// central entities in the knowledge graph.
    pub async fn graph_centrality(
        &self,
        tenant: &str,
        as_of: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<crate::memory::graph_analytics::NodeCentrality>> {
        let triples = self.graph_triples(tenant, as_of).await?;
        Ok(crate::memory::graph_analytics::centrality(&triples))
    }

    /// Shortest directed path between two entities (optionally as-of). None if unreachable.
    pub async fn graph_path(
        &self,
        tenant: &str,
        src: &str,
        dst: &str,
        as_of: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Option<Vec<String>>> {
        let triples = self.graph_triples(tenant, as_of).await?;
        Ok(crate::memory::graph_analytics::shortest_path(
            &triples, src, dst,
        ))
    }

    /// Community detection (connected components on the undirected projection), optionally as-of.
    pub async fn graph_communities(
        &self,
        tenant: &str,
        as_of: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<Vec<String>>> {
        let triples = self.graph_triples(tenant, as_of).await?;
        Ok(crate::memory::graph_analytics::communities(&triples))
    }

    /// Store a multimodal attachment (image / PDF / audio) — its blob goes to the configured storage
    /// backend (local dir or S3), its metadata to the cognition DB — optionally linked to a memory.
    /// Pair with a caption memory (`memory_add` citing the returned id in metadata) to make the
    /// attachment's content retrievable via hybrid search. If an [`ImageEmbeddingProvider`] is wired
    /// (see [`Self::set_image_embedding`]) and this is an `image/*`, the blob is also embedded and
    /// indexed so it's searchable by image ([`Self::attachment_search_image`]). Returns the record.
    ///
    /// [`ImageEmbeddingProvider`]: crate::embedding::ImageEmbeddingProvider
    pub async fn attachment_put(
        &self,
        tenant: &str,
        memory_id: Option<uuid::Uuid>,
        content_type: &str,
        filename: Option<String>,
        bytes: bytes::Bytes,
    ) -> Result<crate::memory::cognition::AttachmentMeta> {
        let id = uuid::Uuid::new_v4();
        let key = format!("attachments/{tenant}/{id}");
        let size = bytes.len() as u64;
        // Keep a cheap (ref-counted) handle for embedding before the blob is moved into storage.
        let for_embed = content_type
            .starts_with("image/")
            .then(|| self.image_embedding.read().clone())
            .flatten()
            .map(|provider| (provider, bytes.clone()));
        self.attachments.put(&key, bytes).await?;
        let meta = crate::memory::cognition::AttachmentMeta {
            id,
            tenant_id: tenant.to_string(),
            memory_id,
            content_type: content_type.to_string(),
            filename,
            size,
            storage_key: key,
            created_at: chrono::Utc::now(),
        };
        self.memory_store.attachment_insert(&meta).await?;

        // Multimodal index (best-effort — a failed embed never fails the upload).
        if let Some((provider, img)) = for_embed {
            match provider.embed_image(&img).await {
                Ok(vec) => {
                    let entry = crate::memory::semantic::SemanticEntry {
                        id,
                        content: meta.filename.clone().unwrap_or_default(),
                        embedding: vec,
                        metadata: serde_json::json!({ "tenant_id": tenant, "attachment": true }),
                    };
                    if let Err(e) = self.modal.upsert("image", &entry).await {
                        tracing::warn!(error = %e, "image attachment index failed");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "image embedding failed"),
            }
        }
        Ok(meta)
    }

    /// Search image attachments by an example image (requires a wired [`ImageEmbeddingProvider`]).
    /// Embeds the query image, searches the image index, and returns the matching attachments'
    /// metadata (tenant-scoped). Empty if no image provider is configured.
    ///
    /// [`ImageEmbeddingProvider`]: crate::embedding::ImageEmbeddingProvider
    pub async fn attachment_search_image(
        &self,
        tenant: &str,
        query: &[u8],
        k: usize,
    ) -> Result<Vec<crate::memory::cognition::AttachmentMeta>> {
        let Some(provider) = self.image_embedding.read().clone() else {
            return Ok(Vec::new());
        };
        let vec = provider.embed_image(query).await?;
        let hits = self
            .modal
            .search("image", &vec, k.min(self.config.query.max_rows))
            .await?;
        let mut out = Vec::new();
        for hit in hits {
            // attachment_get is tenant-scoped, so this also enforces isolation across tenants.
            if let Some(meta) = self
                .memory_store
                .attachment_get(tenant, hit.entry.id)
                .await?
            {
                out.push(meta);
            }
        }
        Ok(out)
    }

    /// Wire an image-embedding backend so `image/*` attachments are embedded + indexed for image
    /// search. Replaces any previous provider.
    pub fn set_image_embedding(&self, provider: Arc<dyn crate::embedding::ImageEmbeddingProvider>) {
        *self.image_embedding.write() = Some(provider);
    }

    /// Fetch an attachment's metadata + bytes (tenant-scoped). None if absent or the blob is gone.
    pub async fn attachment_get(
        &self,
        tenant: &str,
        id: uuid::Uuid,
    ) -> Result<Option<(crate::memory::cognition::AttachmentMeta, bytes::Bytes)>> {
        let Some(meta) = self.memory_store.attachment_get(tenant, id).await? else {
            return Ok(None);
        };
        match self.attachments.get(&meta.storage_key).await? {
            Some(bytes) => Ok(Some((meta, bytes))),
            None => Ok(None),
        }
    }

    /// List a tenant's attachments, optionally only those linked to `memory_id`.
    pub async fn attachment_list(
        &self,
        tenant: &str,
        memory_id: Option<uuid::Uuid>,
        limit: usize,
    ) -> Result<Vec<crate::memory::cognition::AttachmentMeta>> {
        self.memory_store
            .attachment_list(tenant, memory_id, limit.min(self.config.query.max_rows))
            .await
    }

    /// Delete an attachment — its metadata and its blob. Returns whether it existed.
    pub async fn attachment_delete(&self, tenant: &str, id: uuid::Uuid) -> Result<bool> {
        match self.memory_store.attachment_delete(tenant, id).await? {
            Some(key) => {
                let _ = self.attachments.delete(&key).await;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Multi-hop neighborhood: BFS from `entity` out to `depth` hops, returning the reachable edges
    /// (deduplicated), capped at `max_edges`.
    pub async fn memory_subgraph(
        &self,
        tenant: &str,
        entity: &str,
        depth: usize,
        max_edges: usize,
    ) -> Result<Vec<crate::memory::cognition::Edge>> {
        let cap = max_edges.min(self.config.query.max_rows).max(1);
        let mut seen_entities = std::collections::HashSet::new();
        let mut seen_edges = std::collections::HashSet::new();
        let mut frontier = vec![entity.to_string()];
        seen_entities.insert(entity.to_string());
        let mut edges = Vec::new();
        for _ in 0..depth.max(1) {
            let mut next = Vec::new();
            for e in &frontier {
                for edge in self.memory_store.neighbors(tenant, e, cap).await? {
                    if seen_edges.insert(edge.id) {
                        for endpoint in [edge.src.clone(), edge.dst.clone()] {
                            if seen_entities.insert(endpoint.clone()) {
                                next.push(endpoint);
                            }
                        }
                        edges.push(edge);
                        if edges.len() >= cap {
                            return Ok(edges);
                        }
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        Ok(edges)
    }

    /// Extract `(subject, relation, object)` triples from text and add them as graph edges.
    /// Returns the number of edges added.
    pub async fn memory_graph_from_text(
        &self,
        tenant: &str,
        text: &str,
        source: Option<uuid::Uuid>,
    ) -> Result<usize> {
        let triples = self.extract_triples_any(text).await;
        let n = triples.len();
        for (s, r, o) in triples {
            self.memory_link(tenant, &s, &r, &o, source).await?;
        }
        Ok(n)
    }

    /// Extract triples via LLM when `extraction = "llm"` + a provider is configured, else fall back
    /// to the deterministic verb-pattern extractor.
    async fn extract_triples_any(&self, text: &str) -> Vec<(String, String, String)> {
        if self.config.memory.cognition.extraction == "llm" {
            if let Some(provider) = &self.completion {
                if let Ok(out) = provider
                    .complete(
                        "Extract factual relationships from the text as lines of \
                         `subject | relation | object`. Output only those lines, nothing else.",
                        text,
                    )
                    .await
                {
                    let parsed = parse_triple_lines(&out);
                    if !parsed.is_empty() {
                        return parsed;
                    }
                }
            }
        }
        crate::memory::cognition::extract_triples(text)
    }

    /// Extract facts from raw text (opt-in LLM, else single-memory fallback).
    async fn extract_facts(&self, raw_text: &str) -> Vec<(Option<String>, String)> {
        if self.config.memory.cognition.extraction == "llm" {
            if let Some(provider) = &self.completion {
                match provider.complete(Self::EXTRACT_SYSTEM, raw_text).await {
                    Ok(text) => match parse_extracted_facts(&text) {
                        Some(facts) if !facts.is_empty() => return facts,
                        _ => tracing::warn!("LLM extraction returned no parseable facts"),
                    },
                    Err(e) => tracing::warn!(error = %e, "LLM extraction failed"),
                }
            }
        }
        // Deterministic fallback: store the text as a single memory.
        vec![(None, raw_text.to_string())]
    }

    // ── Schema Introspection ────────────────────────────────────────

    /// List all distinct event sources in the episodic store.
    pub async fn list_sources(&self) -> Result<Vec<String>> {
        let episodic = self.episodic.clone();
        tokio::task::spawn_blocking(move || {
            let db = episodic.write_conn();
            let mut stmt = db
                .prepare("SELECT DISTINCT source FROM episodic ORDER BY source")
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let sources: Vec<String> = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| crate::Error::Query(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(sources)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?
    }

    /// List all distinct agent IDs in the state store.
    pub async fn list_agents(&self) -> Result<Vec<String>> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let db = state.db_conn();
            let mut stmt = db
                .prepare("SELECT DISTINCT agent_id FROM state ORDER BY agent_id")
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let agents: Vec<String> = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| crate::Error::Query(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(agents)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?
    }

    /// List event sources for a single tenant.
    pub async fn list_sources_for_tenant(&self, tenant: &str) -> Result<Vec<String>> {
        let episodic = self.episodic.clone();
        let tenant = tenant.to_string();
        tokio::task::spawn_blocking(move || {
            let db = episodic.write_conn();
            let mut stmt = db
                .prepare("SELECT DISTINCT source FROM episodic WHERE tenant_id = ? ORDER BY source")
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let sources: Vec<String> = stmt
                .query_map(duckdb::params![tenant], |row| row.get(0))
                .map_err(|e| crate::Error::Query(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(sources)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?
    }

    /// List agent IDs for a single tenant (the tenant prefix is stripped from the result).
    pub async fn list_agents_for_tenant(&self, tenant: &str) -> Result<Vec<String>> {
        let state = self.state.clone();
        let prefix = format!("{tenant}{TENANT_AGENT_SEP}");
        tokio::task::spawn_blocking(move || {
            let db = state.db_conn();
            let like = format!("{prefix}%");
            let mut stmt = db
                .prepare(
                    "SELECT DISTINCT agent_id FROM state WHERE agent_id LIKE ?1 ORDER BY agent_id",
                )
                .map_err(|e| crate::Error::Query(e.to_string()))?;
            let agents: Vec<String> = stmt
                .query_map(rusqlite::params![like], |row| row.get::<_, String>(0))
                .map_err(|e| crate::Error::Query(e.to_string()))?
                .filter_map(|r| r.ok())
                .map(|a| a.strip_prefix(&prefix).unwrap_or(&a).to_string())
                .collect();
            Ok(agents)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?
    }

    // ── Retention ─────────────────────────────────────────────────────

    /// Clean up expired state entries (TTL).
    ///
    /// Returns the number of entries deleted.
    pub async fn cleanup_expired_state(&self) -> Result<u64> {
        self.state.cleanup_expired().await
    }

    /// Delete events older than the configured retention period.
    ///
    /// Checks per-source retention policies first (stored in state),
    /// then falls back to the default retention period.
    /// Returns the number of events deleted.
    pub async fn enforce_retention(&self) -> Result<u64> {
        let default_days = self.config.memory.episodic.default_retention_days;

        // Load per-source retention policies from state store
        let policies = self.retention_policies().await?;

        let episodic = self.episodic.clone();
        tokio::task::spawn_blocking(move || {
            let db = episodic.write_conn();
            let mut total_deleted = 0u64;

            // Apply per-source policies first
            for (source, days) in &policies {
                if *days == 0 {
                    continue;
                }
                let cutoff = chrono::Utc::now() - chrono::Duration::days(*days as i64);
                let cutoff_str = cutoff.to_rfc3339();
                let deleted = db
                    .execute(
                        "DELETE FROM episodic WHERE source = ? AND ts < ?::TIMESTAMPTZ",
                        duckdb::params![source, cutoff_str],
                    )
                    .map_err(|e| crate::Error::Storage(format!("retention delete: {e}")))?;
                total_deleted += deleted as u64;
            }

            // Apply default retention to sources without a specific policy
            if default_days > 0 {
                let cutoff = chrono::Utc::now() - chrono::Duration::days(default_days as i64);
                let cutoff_str = cutoff.to_rfc3339();

                if policies.is_empty() {
                    // No per-source policies — apply globally
                    let deleted = db
                        .execute(
                            "DELETE FROM episodic WHERE ts < ?::TIMESTAMPTZ",
                            duckdb::params![cutoff_str],
                        )
                        .map_err(|e| crate::Error::Storage(format!("retention delete: {e}")))?;
                    total_deleted += deleted as u64;
                } else {
                    // Apply default only to sources without a specific policy
                    let policy_sources: Vec<&str> =
                        policies.iter().map(|(s, _)| s.as_str()).collect();
                    let placeholders: Vec<String> =
                        policy_sources.iter().map(|_| "?".to_string()).collect();
                    if !placeholders.is_empty() {
                        let sql = format!(
                            "DELETE FROM episodic WHERE ts < ?::TIMESTAMPTZ AND source NOT IN ({})",
                            placeholders.join(", ")
                        );
                        let mut stmt = db
                            .prepare(&sql)
                            .map_err(|e| crate::Error::Storage(format!("prepare: {e}")))?;

                        // Build params: cutoff + source names
                        let mut params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();
                        params.push(Box::new(cutoff_str));
                        for s in &policy_sources {
                            params.push(Box::new(s.to_string()));
                        }
                        let param_refs: Vec<&dyn duckdb::ToSql> =
                            params.iter().map(|p| p.as_ref()).collect();
                        let deleted = stmt
                            .execute(param_refs.as_slice())
                            .map_err(|e| crate::Error::Storage(format!("retention delete: {e}")))?;
                        total_deleted += deleted as u64;
                    }
                }
            }

            Ok(total_deleted)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))?
    }

    /// Get all per-source retention policies.
    pub async fn retention_policies(&self) -> Result<Vec<(String, u32)>> {
        match self.state.get("_system", "retention_policies").await? {
            Some(entry) => {
                let policies: Vec<(String, u32)> =
                    serde_json::from_value(entry.value).unwrap_or_default();
                Ok(policies)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Set a retention policy for a specific source.
    pub async fn set_retention_policy(&self, source: &str, retention_days: u32) -> Result<()> {
        let mut policies = self.retention_policies().await?;
        // Update or insert
        if let Some(existing) = policies.iter_mut().find(|(s, _)| s == source) {
            existing.1 = retention_days;
        } else {
            policies.push((source.to_string(), retention_days));
        }
        self.state
            .set(
                "_system",
                "retention_policies",
                serde_json::to_value(&policies).map_err(|e| crate::Error::State(e.to_string()))?,
            )
            .await?;
        Ok(())
    }

    /// Remove a retention policy for a source (falls back to default).
    pub async fn remove_retention_policy(&self, source: &str) -> Result<()> {
        let mut policies = self.retention_policies().await?;
        policies.retain(|(s, _)| s != source);
        self.state
            .set(
                "_system",
                "retention_policies",
                serde_json::to_value(&policies).map_err(|e| crate::Error::State(e.to_string()))?,
            )
            .await?;
        Ok(())
    }

    // ── Backup / Restore ─────────────────────────────────────────────

    /// Save all persistent stores to a directory for backup.
    ///
    /// Creates: episodic.duckdb (EXPORT), vectors/ (USearch index), state.db (SQLite copy).
    pub async fn backup(&self, dir: &std::path::Path) -> Result<()> {
        std::fs::create_dir_all(dir).map_err(|e| crate::Error::Storage(format!("mkdir: {e}")))?;

        // Backup episodic store via DuckDB EXPORT
        let export_dir = dir.join("episodic_export");
        let export_dir_str = export_dir.to_string_lossy().to_string();
        let episodic = self.episodic.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&export_dir)
                .map_err(|e| crate::Error::Storage(format!("mkdir: {e}")))?;
            let db = episodic.write_conn();
            db.execute_batch(&format!("EXPORT DATABASE '{export_dir_str}'"))
                .map_err(|e| crate::Error::Storage(format!("duckdb export: {e}")))?;
            Ok::<(), crate::Error>(())
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;

        // Backup semantic index
        let vectors_dir = dir.join("vectors");
        self.semantic.save(&vectors_dir)?;

        // Backup the memories store (DuckDB EXPORT)
        let mem_export = dir.join("memories_export");
        let memory_store = self.memory_store.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&mem_export)
                .map_err(|e| crate::Error::Storage(format!("mkdir: {e}")))?;
            memory_store.export_to(&mem_export)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;

        // Backup the state store (SQLite VACUUM INTO)
        let state_path = dir.join("state.db");
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || state.backup_to(&state_path))
            .await
            .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;

        // Write a manifest: per-artifact SHA-256 checksums + store counts + a capture timestamp, so
        // a restore can detect a corrupted/truncated backup before trusting it.
        //
        // Consistency note: the four exports above are NOT wrapped in a single global write barrier,
        // so a backup taken while writes are in flight can be *fuzzy* at the edges (an event landing
        // between the episodic and memories export). For Raft-replicated restore this is harmless
        // (the log replays to a consistent point); for standalone disaster recovery, quiesce writes
        // (or snapshot the data volume) if you need a strict point-in-time image. The manifest
        // records what was captured and lets integrity be verified.
        let manifest = BackupManifest {
            format_version: BACKUP_FORMAT_VERSION,
            ecphoria_version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: chrono::Utc::now(),
            counts: BackupCounts {
                episodic_events: self.event_count().await.unwrap_or(0),
                memories: self.memory_count().await.unwrap_or(0),
                semantic_vectors: self.semantic_count() as u64,
            },
            artifacts: {
                let dir = dir.to_path_buf();
                tokio::task::spawn_blocking(move || {
                    let mut out = Vec::new();
                    for name in ["episodic_export", "memories_export", "vectors", "state.db"] {
                        let p = dir.join(name);
                        if p.exists() {
                            out.push(BackupArtifact {
                                path: name.to_string(),
                                sha256: sha256_path(&p)?,
                            });
                        }
                    }
                    Ok::<_, crate::Error>(out)
                })
                .await
                .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??
            },
        };
        let manifest_json = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| crate::Error::Storage(format!("manifest encode: {e}")))?;
        std::fs::write(dir.join("manifest.json"), manifest_json)
            .map_err(|e| crate::Error::Storage(format!("manifest write: {e}")))?;

        tracing::info!(path = %dir.display(), "backup complete");
        Ok(())
    }

    /// Prune old local backup directories under `backups_dir`, keeping the newest
    /// `backup.max_backups` (by directory name — the timestamped names sort chronologically). Returns
    /// how many were removed. No-op when `max_backups == 0` (keep all). Only removes directories that
    /// look like a backup (contain a `manifest.json`), so a stray file is never deleted.
    pub async fn prune_backups(&self, backups_dir: &std::path::Path) -> Result<usize> {
        let max = self.config.backup.max_backups as usize;
        if max == 0 || !backups_dir.is_dir() {
            return Ok(0);
        }
        let dir = backups_dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut entries: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
                .map_err(|e| crate::Error::Storage(format!("read backups dir: {e}")))?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.is_dir() && p.join("manifest.json").exists())
                .collect();
            if entries.len() <= max {
                return Ok(0);
            }
            // Oldest first (timestamp names sort lexicographically = chronologically).
            entries.sort();
            let to_remove = entries.len() - max;
            let mut removed = 0;
            for p in entries.into_iter().take(to_remove) {
                match std::fs::remove_dir_all(&p) {
                    Ok(()) => removed += 1,
                    Err(e) => {
                        tracing::warn!(path = %p.display(), error = %e, "backup prune failed")
                    }
                }
            }
            Ok(removed)
        })
        .await
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("prune join: {e}")))?
    }

    /// Restore all stores from a backup directory produced by [`Self::backup`].
    ///
    /// Used by the Raft snapshot-install path and for disaster recovery. Episodic and memories
    /// are restored atomically (stage-then-swap); the memory vector index is rebuilt afterward.
    pub async fn restore_from_backup(&self, dir: &std::path::Path) -> Result<()> {
        // Verify the manifest checksums first (when present) so we never restore a corrupted or
        // truncated backup. Backups without a manifest (produced before this format) are still
        // accepted for backward compatibility, with a warning.
        let manifest_path = dir.join("manifest.json");
        if manifest_path.exists() {
            let raw = std::fs::read(&manifest_path)
                .map_err(|e| crate::Error::Storage(format!("manifest read: {e}")))?;
            let manifest: BackupManifest = serde_json::from_slice(&raw)
                .map_err(|e| crate::Error::Storage(format!("manifest parse: {e}")))?;
            if manifest.format_version > BACKUP_FORMAT_VERSION {
                return Err(crate::Error::Storage(format!(
                    "backup manifest format v{} is newer than this build supports (v{}); upgrade Ecphoria",
                    manifest.format_version, BACKUP_FORMAT_VERSION
                )));
            }
            let dir_owned = dir.to_path_buf();
            let artifacts = manifest.artifacts.clone();
            tokio::task::spawn_blocking(move || {
                for a in &artifacts {
                    let p = dir_owned.join(&a.path);
                    let actual = sha256_path(&p)?;
                    if actual != a.sha256 {
                        return Err(crate::Error::Storage(format!(
                            "backup integrity check failed for '{}': manifest {} != actual {}",
                            a.path, a.sha256, actual
                        )));
                    }
                }
                Ok::<_, crate::Error>(())
            })
            .await
            .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;
            tracing::info!(
                artifacts = manifest.artifacts.len(),
                captured_at = %manifest.created_at,
                "backup manifest verified"
            );
        } else {
            tracing::warn!(
                path = %dir.display(),
                "backup has no manifest.json — restoring without integrity verification"
            );
        }

        let ep_export = dir.join("episodic_export");
        if ep_export.exists() {
            let episodic = self.episodic.clone();
            let staging = dir.join("episodic_staging.duckdb");
            tokio::task::spawn_blocking(move || episodic.restore_from_export(&ep_export, &staging))
                .await
                .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;
        }

        let mem_export = dir.join("memories_export");
        if mem_export.exists() {
            let memory_store = self.memory_store.clone();
            let staging = dir.join("memories_staging.duckdb");
            tokio::task::spawn_blocking(move || {
                memory_store.restore_from_export(&mem_export, &staging)
            })
            .await
            .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;
        }

        let state_path = dir.join("state.db");
        if state_path.exists() {
            let state = self.state.clone();
            tokio::task::spawn_blocking(move || state.restore_from(&state_path))
                .await
                .map_err(|e| crate::Error::Internal(anyhow::anyhow!("join: {e}")))??;
        }

        let vectors_dir = dir.join("vectors");
        if vectors_dir.exists() {
            self.semantic
                .load_from(&vectors_dir)
                .map_err(|e| crate::Error::Storage(format!("semantic restore: {e}")))?;
        }

        // Rebuild the memory vector index from the restored memories (no provider call).
        if let Ok(rows) = self.memory_store.load_active_with_embeddings().await {
            for (mem, emb) in rows {
                let key = crate::memory::cognition::scope_partition_key(&mem.scope);
                let _ = self
                    .memory_index
                    .upsert(&key, &mem.to_semantic_entry(emb))
                    .await;
            }
        }

        tracing::info!(path = %dir.display(), "restore complete");
        Ok(())
    }

    /// Backup all stores to S3 using the configured StorageBackend.
    ///
    /// Creates a local backup first, then uploads each file to S3 under the
    /// configured prefix with a timestamp directory.
    pub async fn backup_to_s3(&self) -> Result<()> {
        use crate::storage::StorageBackend;

        let s3_config = &self.config.storage.s3;
        if s3_config.bucket.is_empty() {
            return Err(crate::Error::Config(
                "S3 bucket not configured for backup".into(),
            ));
        }

        let s3 = crate::storage::s3::S3Storage::from_config(s3_config).await?;

        // Create a temporary local backup
        let tmp = std::env::temp_dir().join(format!(
            "ecphoria-backup-{}",
            chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
        ));
        self.backup(&tmp).await?;

        let prefix = &self.config.backup.s3_prefix;
        let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();

        // Walk the temp directory and upload each file
        let mut entries = Vec::new();
        Self::walk_dir(&tmp, &mut entries)?;

        for file_path in &entries {
            let relative = file_path
                .strip_prefix(&tmp)
                .map_err(|e| crate::Error::Storage(format!("path strip: {e}")))?;
            let s3_key = format!("{prefix}{timestamp}/{}", relative.to_string_lossy());
            let data = tokio::fs::read(file_path)
                .await
                .map_err(|e| crate::Error::Storage(format!("read backup file: {e}")))?;
            s3.put(&s3_key, bytes::Bytes::from(data)).await?;
        }

        // Clean up temp directory
        let _ = tokio::fs::remove_dir_all(&tmp).await;

        tracing::info!(
            prefix = %prefix,
            timestamp = %timestamp,
            files = entries.len(),
            "S3 backup complete"
        );
        Ok(())
    }

    /// Recursively walk a directory, collecting file paths.
    fn walk_dir(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) -> Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }
        for entry in
            std::fs::read_dir(dir).map_err(|e| crate::Error::Storage(format!("readdir: {e}")))?
        {
            let entry = entry.map_err(|e| crate::Error::Storage(format!("entry: {e}")))?;
            let path = entry.path();
            if path.is_dir() {
                Self::walk_dir(&path, out)?;
            } else {
                out.push(path);
            }
        }
        Ok(())
    }

    // ── Store Accessors (for snapshot/restore) ────────────────────────

    /// Access the episodic store directly (for snapshot operations).
    pub fn episodic_store(&self) -> Arc<EpisodicStore> {
        self.episodic.clone()
    }

    /// Access the semantic store directly (for snapshot operations).
    pub fn semantic_store(&self) -> Arc<SemanticStore> {
        self.semantic.clone()
    }

    // ── Health Checks ────────────────────────────────────────────────

    /// Check if the DuckDB episodic store is accessible.
    pub async fn check_episodic(&self) -> bool {
        let episodic = self.episodic.clone();
        tokio::task::spawn_blocking(move || {
            let db = episodic.write_conn();
            db.execute_batch("SELECT 1").is_ok()
        })
        .await
        .unwrap_or(false)
    }

    /// Check if the SQLite state store is accessible.
    pub async fn check_state(&self) -> bool {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let db = state.db_conn();
            db.execute_batch("SELECT 1").is_ok()
        })
        .await
        .unwrap_or(false)
    }

    // ── Lifecycle ────────────────────────────────────────────────────

    /// Persist in-memory indexes that are NOT rebuilt from a durable store — currently the **event
    /// semantic (USearch) index** (`self.semantic`, feeding `semantic_search`/RAG). It is loaded on
    /// startup but otherwise only written by `backup()`/`shutdown()`; without periodic persistence a
    /// file-backed server (or an embedded/Python user) loses event embeddings on exit. Takes `&self`
    /// so it can be called on a live `Arc<Engine>` (periodically and on SIGTERM); no-op for an
    /// in-memory index dir. (The memory-cognition vector index is rebuilt from DuckDB, so it's safe.)
    pub async fn persist(&self) -> Result<()> {
        let index_dir = self.config.memory.semantic.index_dir.clone();
        if index_dir.is_empty() || index_dir == ":memory:" {
            return Ok(());
        }
        let semantic = self.semantic.clone();
        tokio::task::spawn_blocking(move || semantic.save(std::path::Path::new(&index_dir)))
            .await
            .map_err(|e| crate::Error::Internal(anyhow::anyhow!("persist join: {e}")))?
    }

    /// Gracefully shut down the engine, persisting the semantic index. (Consumes `self`; the server
    /// calls [`Self::persist`] on its `Arc` instead — see `ecphoria-server`.)
    pub async fn shutdown(self) -> Result<()> {
        if let Err(e) = self.persist().await {
            tracing::warn!(error = %e, "failed to persist semantic index on shutdown");
        }
        tracing::info!("Ecphoria engine shutting down");
        Ok(())
    }
}

/// Backup manifest format version — bumped when the on-disk backup layout changes.
const BACKUP_FORMAT_VERSION: u32 = 1;

/// Integrity + provenance manifest written alongside a backup (`manifest.json`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackupManifest {
    /// On-disk backup format version.
    pub format_version: u32,
    /// Ecphoria version that produced the backup.
    pub ecphoria_version: String,
    /// When the backup was captured (UTC).
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Row/entry counts at capture time (for a sanity check on restore).
    pub counts: BackupCounts,
    /// Per-artifact SHA-256 checksums.
    pub artifacts: Vec<BackupArtifact>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackupCounts {
    pub episodic_events: u64,
    pub memories: u64,
    pub semantic_vectors: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackupArtifact {
    /// Path relative to the backup directory (a file or a directory).
    pub path: String,
    /// Lowercase hex SHA-256. For a directory, a hash over its files (sorted, path + bytes).
    pub sha256: String,
}

/// SHA-256 of a file, or a deterministic digest over a directory tree (files sorted by relative
/// path; each contributes its path and bytes) — hex-encoded. Used for backup integrity.
fn sha256_path(path: &std::path::Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    if path.is_dir() {
        let mut files: Vec<std::path::PathBuf> = Vec::new();
        collect_files(path, &mut files)?;
        files.sort();
        for f in files {
            let rel = f.strip_prefix(path).unwrap_or(&f);
            hasher.update(rel.to_string_lossy().as_bytes());
            hasher.update([0u8]);
            let bytes = std::fs::read(&f).map_err(|e| {
                crate::Error::Storage(format!("checksum read {}: {e}", f.display()))
            })?;
            hasher.update(&bytes);
        }
    } else {
        let bytes = std::fs::read(path)
            .map_err(|e| crate::Error::Storage(format!("checksum read {}: {e}", path.display())))?;
        hasher.update(&bytes);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// Recursively collect regular files under `dir` into `out`.
fn collect_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) -> Result<()> {
    for entry in
        std::fs::read_dir(dir).map_err(|e| crate::Error::Storage(format!("readdir: {e}")))?
    {
        let entry = entry.map_err(|e| crate::Error::Storage(format!("direntry: {e}")))?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, out)?;
        } else {
            out.push(path);
        }
    }
    Ok(())
}

/// Separator that namespaces an `agent_id` by tenant for state isolation. A control char
/// (unit separator) that is extremely unlikely to occur in a real agent id.
pub(crate) const TENANT_AGENT_SEP: char = '\u{1f}';

/// Namespace an agent id by tenant so agent state is isolated per tenant.
pub(crate) fn scoped_agent(tenant: &str, agent_id: &str) -> String {
    format!("{tenant}{TENANT_AGENT_SEP}{agent_id}")
}

/// Leniently parse an LLM extraction response into `(subject, content)` facts.
///
/// Tolerates surrounding prose / Markdown fences by extracting the outermost `[...]` array.
/// Parse LLM output of `subject | relation | object` lines into triples (relations normalized).
fn parse_triple_lines(text: &str) -> Vec<(String, String, String)> {
    text.lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split('|').map(|p| p.trim()).collect();
            if parts.len() == 3 && !parts[0].is_empty() && !parts[2].is_empty() {
                Some((
                    parts[0].to_string(),
                    parts[1].to_lowercase().replace(' ', "_"),
                    parts[2].to_string(),
                ))
            } else {
                None
            }
        })
        .collect()
}

fn parse_extracted_facts(text: &str) -> Option<Vec<(Option<String>, String)>> {
    let start = text.find('[')?;
    let end = text.rfind(']')?;
    if end <= start {
        return None;
    }
    let arr: Vec<serde_json::Value> = serde_json::from_str(&text[start..=end]).ok()?;
    let mut facts = Vec::new();
    for item in arr {
        let Some(content) = item
            .get("content")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        let subject = item
            .get("subject")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        facts.push((subject, content));
    }
    Some(facts)
}

// Compile-time assertion: EcphoriaEngine must be Send + Sync for Arc usage.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<EcphoriaEngine>();
};

/// The agent runtime — see [`agentic`]. Gated: `--no-default-features` builds a memory-only
/// engine that cannot run an agent at all.
#[cfg(feature = "agentic")]
mod agentic;

#[cfg(test)]
mod tests;
