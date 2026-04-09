//! Ingestion pipeline — receives events, stores them, triggers embedding.

use std::sync::Arc;

use crate::memory::episodic::{EpisodicStore, Event};

/// Pipeline that processes incoming events into the memory stores.
#[derive(Debug)]
pub struct IngestPipeline {
    episodic: Arc<EpisodicStore>,
}

impl IngestPipeline {
    pub fn new(episodic: Arc<EpisodicStore>) -> Self {
        Self { episodic }
    }

    /// Ingest a batch of events.
    pub async fn ingest(&self, events: Vec<Event>) -> crate::Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let count = self.episodic.append(&events).await?;
        tracing::debug!(count, "ingested events");

        // TODO: auto-embed events via EmbeddingProvider and upsert to SemanticStore

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
