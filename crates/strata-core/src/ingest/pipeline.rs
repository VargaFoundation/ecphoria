//! Ingestion pipeline — receives events, stores them, optionally embeds.

use std::sync::Arc;

use crate::config::EmbeddingConfig;
use crate::embedding::EmbeddingProvider;
use crate::memory::episodic::{EpisodicStore, Event};
use crate::memory::semantic::{SemanticEntry, SemanticStore};

/// Pipeline that processes incoming events into the memory stores.
pub struct IngestPipeline {
    episodic: Arc<EpisodicStore>,
    semantic: Option<Arc<SemanticStore>>,
    embedding: Option<Arc<dyn EmbeddingProvider>>,
    batch_size: usize,
}

impl std::fmt::Debug for IngestPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestPipeline")
            .field("has_semantic", &self.semantic.is_some())
            .field("has_embedding", &self.embedding.is_some())
            .finish()
    }
}

impl IngestPipeline {
    /// Create a pipeline with only episodic storage.
    pub fn new(episodic: Arc<EpisodicStore>) -> Self {
        Self {
            episodic,
            semantic: None,
            embedding: None,
            batch_size: EmbeddingConfig::default().batch_size,
        }
    }

    /// Create a pipeline with auto-embedding support.
    pub fn with_embedding(
        episodic: Arc<EpisodicStore>,
        semantic: Arc<SemanticStore>,
        embedding: Arc<dyn EmbeddingProvider>,
        batch_size: usize,
    ) -> Self {
        Self {
            episodic,
            semantic: Some(semantic),
            embedding: Some(embedding),
            batch_size: if batch_size == 0 { 64 } else { batch_size },
        }
    }

    /// Ingest a batch of events.
    ///
    /// 1. Append all events to episodic store
    /// 2. If embedding provider is configured, embed event payloads and upsert to semantic store
    pub async fn ingest(&self, events: Vec<Event>) -> crate::Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        // Step 1: Store in episodic memory
        let count = self.episodic.append(&events).await?;
        tracing::debug!(count, "ingested events into episodic store");

        // Step 2: Auto-embed if provider is available (batched)
        if let (Some(semantic), Some(embedding)) = (&self.semantic, &self.embedding) {
            let texts: Vec<String> = events
                .iter()
                .map(|e| {
                    format!(
                        "[{}] {}: {}",
                        e.source,
                        e.event_type,
                        serde_json::to_string(&e.payload).unwrap_or_default()
                    )
                })
                .collect();

            // Process embeddings in batches to respect API limits
            let mut embedded = 0usize;
            let paired: Vec<(&Event, &String)> = events.iter().zip(texts.iter()).collect();

            for chunk in paired.chunks(self.batch_size) {
                let chunk_texts: Vec<String> = chunk.iter().map(|(_, t)| (*t).clone()).collect();

                match embedding.embed(&chunk_texts).await {
                    Ok(embeddings) => {
                        for ((event, text), emb) in chunk.iter().zip(embeddings) {
                            let entry = SemanticEntry {
                                id: event.id,
                                content: (*text).clone(),
                                embedding: emb,
                                metadata: serde_json::json!({
                                    "source": event.source,
                                    "event_type": event.event_type,
                                    "timestamp": event.timestamp.to_rfc3339(),
                                }),
                            };
                            if let Err(e) = semantic.upsert(&entry).await {
                                tracing::warn!(error = %e, "failed to upsert semantic entry");
                            }
                        }
                        embedded += chunk.len();
                    }
                    Err(e) => {
                        // Non-fatal: continue with next batch
                        tracing::warn!(
                            error = %e,
                            batch_size = chunk.len(),
                            "auto-embedding batch failed, skipping"
                        );
                    }
                }
            }

            if embedded > 0 {
                tracing::debug!(embedded, "auto-embedded events into semantic store");
            }
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_event(source: &str) -> Event {
        Event {
            id: Uuid::new_v4(),
            source: source.into(),
            event_type: "test.event".into(),
            payload: serde_json::json!({"data": 1}),
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn ingest_empty_batch() {
        let store = Arc::new(EpisodicStore::new());
        let pipeline = IngestPipeline::new(store);
        let count = pipeline.ingest(vec![]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn ingest_events_persisted() {
        let store = Arc::new(EpisodicStore::new());
        let pipeline = IngestPipeline::new(store.clone());

        let count = pipeline
            .ingest(vec![make_event("app"), make_event("app")])
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(store.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn ingest_multiple_batches() {
        let store = Arc::new(EpisodicStore::new());
        let pipeline = IngestPipeline::new(store.clone());

        pipeline.ingest(vec![make_event("a")]).await.unwrap();
        pipeline.ingest(vec![make_event("b")]).await.unwrap();
        assert_eq!(store.count().await.unwrap(), 2);
    }
}
