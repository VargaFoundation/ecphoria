use std::path::Path;
use std::sync::Arc;

use crate::config::CoreConfig;
use crate::embedding::ollama::OllamaProvider;
use crate::embedding::openai::OpenAiProvider;
use crate::embedding::EmbeddingProvider;
use crate::ingest::IngestPipeline;
use crate::memory::episodic::{EpisodicStore, Event};
use crate::memory::semantic::{SearchResult, SemanticEntry, SemanticStore};
use crate::memory::state::StateStore;
use crate::Result;

/// Top-level engine that owns all subsystems of the Strata context lake.
#[derive(Debug)]
pub struct StrataEngine {
    config: CoreConfig,
    episodic: Arc<EpisodicStore>,
    semantic: Arc<SemanticStore>,
    state: Arc<StateStore>,
    ingest: IngestPipeline,
}

impl StrataEngine {
    /// Create and initialize a new Strata engine.
    pub async fn new(config: CoreConfig) -> Result<Self> {
        // Initialize episodic store (file-backed or in-memory DuckDB)
        let episodic_path = Path::new(&config.memory.episodic.db_path);
        let episodic = Arc::new(EpisodicStore::open(episodic_path).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "falling back to in-memory episodic store");
            EpisodicStore::new()
        }));
        if config.memory.episodic.db_path != ":memory:" {
            tracing::info!(path = %config.memory.episodic.db_path, "episodic store: file-backed");
        }

        // Initialize semantic store
        let semantic = Arc::new(
            SemanticStore::with_dimension(config.embedding.dimension)
                .unwrap_or_else(|_| SemanticStore::new()),
        );

        // Initialize state store
        let state_path = Path::new(&config.memory.state.db_path);
        let state = Arc::new(StateStore::open(state_path).unwrap_or_else(|_| {
            tracing::warn!("falling back to in-memory state store");
            StateStore::new()
        }));

        // Initialize embedding provider from config
        let embedding: Option<Arc<dyn EmbeddingProvider>> = match config.embedding.provider.as_str()
        {
            "ollama" => {
                tracing::info!(
                    model = %config.embedding.model,
                    url = %config.embedding.ollama_url,
                    "embedding provider: ollama"
                );
                Some(Arc::new(OllamaProvider::new(
                    config.embedding.ollama_url.clone(),
                    config.embedding.model.clone(),
                    config.embedding.dimension,
                )))
            }
            "openai" if !config.embedding.openai_api_key.is_empty() => {
                tracing::info!(model = %config.embedding.model, "embedding provider: openai");
                Some(Arc::new(OpenAiProvider::new(
                    config.embedding.openai_api_key.clone(),
                    config.embedding.model.clone(),
                    config.embedding.dimension,
                )))
            }
            other => {
                tracing::warn!(
                    provider = %other,
                    "unknown or unconfigured embedding provider, auto-embedding disabled"
                );
                None
            }
        };

        // Initialize ingest pipeline
        let ingest = match embedding {
            Some(emb) => IngestPipeline::with_embedding(
                episodic.clone(),
                semantic.clone(),
                emb,
                config.embedding.batch_size,
            ),
            None => IngestPipeline::new(episodic.clone()),
        };

        tracing::info!("Strata engine initialized");

        Ok(Self {
            config,
            episodic,
            semantic,
            state,
            ingest,
        })
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &CoreConfig {
        &self.config
    }

    // ── Episodic Memory ──────────────────────────────────────────────

    /// Ingest events via the pipeline.
    pub async fn ingest(&self, events: Vec<Event>) -> Result<u64> {
        self.ingest.ingest(events).await
    }

    /// Query events by source.
    pub async fn query_by_source(&self, source: &str, limit: usize) -> Result<Vec<Event>> {
        self.episodic.query_by_source(source, limit).await
    }

    /// Execute raw SQL against the episodic store.
    ///
    /// Runs on a blocking thread to avoid starving the tokio runtime,
    /// since the underlying DuckDB operations hold a parking_lot Mutex.
    /// Enforces the configured query timeout.
    pub async fn query_sql(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let episodic = self.episodic.clone();
        let sql = sql.to_string();
        let max_rows = self.config.query.max_rows;
        let timeout = std::time::Duration::from_millis(self.config.query.timeout_ms);

        tokio::time::timeout(
            timeout,
            tokio::task::spawn_blocking(move || episodic.query_sql_limited(&sql, max_rows)),
        )
        .await
        .map_err(|_| crate::Error::Query("query timed out".into()))?
        .map_err(|e| crate::Error::Internal(anyhow::anyhow!("task join error: {e}")))?
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

    /// List state keys for an agent.
    pub async fn state_list_keys(&self, agent_id: &str) -> Result<Vec<String>> {
        self.state.list_keys(agent_id).await
    }

    // ── Lifecycle ────────────────────────────────────────────────────

    /// Gracefully shut down the engine.
    pub async fn shutdown(self) -> Result<()> {
        tracing::info!("Strata engine shutting down");
        Ok(())
    }
}

// Compile-time assertion: StrataEngine must be Send + Sync for Arc usage.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StrataEngine>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn engine_lifecycle() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn engine_ingest_and_count() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();

        let events = vec![Event {
            id: uuid::Uuid::new_v4(),
            source: "test".into(),
            event_type: "click".into(),
            payload: serde_json::json!({"page": "/home"}),
            timestamp: chrono::Utc::now(),
        }];

        let count = engine.ingest(events).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(engine.event_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn engine_state_crud() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();

        let v = engine
            .state_set("bot", "mood", serde_json::json!("happy"))
            .await
            .unwrap();
        assert_eq!(v, 1);

        let entry = engine.state_get("bot", "mood").await.unwrap().unwrap();
        assert_eq!(entry.value, serde_json::json!("happy"));

        engine.state_delete("bot", "mood").await.unwrap();
        assert!(engine.state_get("bot", "mood").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn engine_query_sql() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();
        let rows = engine
            .query_sql("SELECT 42::VARCHAR as answer")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["answer"], "42");
    }

    #[tokio::test]
    async fn engine_semantic_search() {
        let engine = StrataEngine::new(CoreConfig::default()).await.unwrap();

        // Use distinct vectors so cosine similarity clearly differentiates them
        let mut rust_vec = vec![0.0f32; 768];
        rust_vec[0] = 1.0; // points strongly in dimension 0

        let mut python_vec = vec![0.0f32; 768];
        python_vec[1] = 1.0; // points strongly in dimension 1

        let entry1 = SemanticEntry {
            id: uuid::Uuid::new_v4(),
            content: "Rust programming language".into(),
            embedding: rust_vec.clone(),
            metadata: serde_json::json!({}),
        };
        engine.semantic_upsert(&entry1).await.unwrap();

        let entry2 = SemanticEntry {
            id: uuid::Uuid::new_v4(),
            content: "Python scripting".into(),
            embedding: python_vec,
            metadata: serde_json::json!({}),
        };
        engine.semantic_upsert(&entry2).await.unwrap();

        assert_eq!(engine.semantic_count(), 2);

        // Search for vector close to "Rust"
        let results = engine.semantic_search(&rust_vec, 1).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].entry.content, "Rust programming language");
    }
}
