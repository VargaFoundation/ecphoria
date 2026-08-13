//! What was asked, and whether the corpus answered.
//!
//! Retrieval quality is only measurable against real questions. The reference eval in
//! `examples/kb_eval.rs` is hand-written by the people who wrote the corpus — useful as a
//! regression alarm, weak as evidence that a *team's* questions get answered. Without a record of
//! what was actually asked, a month of use produces anecdotes rather than a dataset.
//!
//! This records each search as an episodic event, so the questions that returned nothing are a SQL
//! query away and the next eval set can be built from them:
//!
//! ```sql
//! SELECT payload->>'query' AS question, count(*) AS asked
//! FROM episodic
//! WHERE event_type = 'memory.search' AND (payload->>'empty')::BOOLEAN
//! GROUP BY 1 ORDER BY 2 DESC LIMIT 50;
//! ```
//!
//! ## Why it is off by default
//!
//! Queries are text a human typed. On a shared server, recording them is a decision a team should
//! make deliberately rather than discover. `include_query = false` is the middle ground: keep the
//! shape (result counts, similarity, latency, whether anything matched) without the text, which
//! still answers "how often do we come up empty" without storing what was asked.
//!
//! ## Why it is queued rather than written inline
//!
//! A search costs ~14 ms; a single DuckDB row insert costs ~4 ms. Writing the log on the request
//! path would tax every read by a quarter to observe it. Records go to a bounded channel drained by
//! one background task that writes in batches through the Appender fast path, so the search pays
//! only a `try_send`. When the channel is full, records are **dropped and counted** — telemetry
//! must never apply backpressure to the thing it is measuring.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::episodic::{EpisodicStore, Event};

/// Source stamped on every record, so retention can target it (`ecphoria retention set --source
/// ecphoria/query-log --days 90`) and it can be excluded from analysis of real events.
pub const QUERY_LOG_SOURCE: &str = "ecphoria/query-log";

/// How many records the background task writes in one batch.
const BATCH: usize = 128;
/// How long it waits to fill a batch before writing what it has.
const FLUSH_MS: u64 = 500;
/// Bounded so a burst cannot grow memory without limit.
const CAPACITY: usize = 4096;

/// One search, as it happened.
#[derive(Debug, Clone)]
pub struct QueryRecord {
    pub tenant_id: String,
    pub user_id: Option<String>,
    pub agent_id: Option<String>,
    pub project: Option<String>,
    /// The question. `None` when `include_query` is off.
    pub query: Option<String>,
    pub k: usize,
    pub results: usize,
    /// Did any retrieval arm actually match?
    ///
    /// Not the same as `results > 0`: when neither the lexical nor the vector arm produces
    /// anything, the search still returns the most important/recent memories, so an unanswerable
    /// question comes back full of plausible-looking rows. This is the field that separates
    /// "answered" from "handed something".
    pub matched: bool,
    /// Subject of the best hit — usually a document path, and the most useful single field when
    /// reading the log back.
    pub top_subject: Option<String>,
    /// Vector similarity of the best hit, when an embedding provider is configured.
    pub top_similarity: Option<f32>,
    pub duration_ms: f64,
}

/// Queued writer for search records.
pub struct QueryLogger {
    tx: mpsc::Sender<Event>,
    include_query: bool,
    dropped: Arc<AtomicU64>,
}

impl std::fmt::Debug for QueryLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryLogger")
            .field("dropped", &self.dropped.load(Ordering::Relaxed))
            .finish()
    }
}

impl QueryLogger {
    /// Start the background writer.
    pub fn spawn(store: Arc<EpisodicStore>, include_query: bool) -> Self {
        let (tx, mut rx) = mpsc::channel::<Event>(CAPACITY);
        tokio::spawn(async move {
            let mut buf: Vec<Event> = Vec::with_capacity(BATCH);
            loop {
                // Block for the first record so an idle server does no work at all.
                let Some(first) = rx.recv().await else {
                    break; // sender dropped — engine shutting down
                };
                buf.push(first);
                let deadline =
                    tokio::time::Instant::now() + std::time::Duration::from_millis(FLUSH_MS);
                while buf.len() < BATCH {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(ev)) => buf.push(ev),
                        Ok(None) => break,
                        Err(_) => break, // window elapsed
                    }
                }
                if let Err(e) = store.append(&buf).await {
                    // Losing telemetry must not be loud enough to drown real logs.
                    tracing::debug!(error = %e, records = buf.len(), "query log write failed");
                }
                buf.clear();
            }
            // Drain whatever is left so a clean shutdown does not lose the last window.
            while let Ok(ev) = rx.try_recv() {
                buf.push(ev);
            }
            if !buf.is_empty() {
                let _ = store.append(&buf).await;
            }
        });
        Self {
            tx,
            include_query,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Queue one record. Never blocks, never fails the search.
    pub fn record(&self, r: QueryRecord) {
        let mut payload = serde_json::json!({
            "k": r.k,
            "results": r.results,
            "matched": r.matched,
            // The field the whole feature exists for: which questions the corpus could not answer.
            // Keyed on `matched`, not on the row count, because the fallback always returns rows.
            "empty": !r.matched || r.results == 0,
            "duration_ms": (r.duration_ms * 100.0).round() / 100.0,
        });
        let obj = payload.as_object_mut().expect("object literal");
        if self.include_query {
            if let Some(q) = r.query {
                obj.insert("query".into(), q.into());
            }
        }
        for (key, value) in [
            ("user_id", r.user_id),
            ("agent_id", r.agent_id),
            ("project", r.project),
            ("top_subject", r.top_subject),
        ] {
            if let Some(v) = value {
                obj.insert(key.into(), v.into());
            }
        }
        if let Some(sim) = r.top_similarity {
            obj.insert(
                "top_similarity".into(),
                ((sim * 1000.0).round() / 1000.0).into(),
            );
        }
        // Tenant travels in the payload under the key `ingest_for_tenant` uses, so the row is
        // tenant-scoped like any other event and the isolation tests cover it.
        obj.insert("_tenant_id".into(), r.tenant_id.into());

        let event = Event {
            id: Uuid::new_v4(),
            source: QUERY_LOG_SOURCE.into(),
            event_type: "memory.search".into(),
            payload,
            timestamp: Utc::now(),
            parent_id: None,
            trace_id: None,
            tags: vec![],
            idempotency_key: None,
        };
        if self.tx.try_send(event).is_err() {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            metrics::counter!("ecphoria_query_log_dropped_total").increment(1);
            // One line per thousand, so a sustained overload is visible without flooding.
            if n % 1000 == 1 {
                tracing::warn!(
                    dropped = n,
                    "query log queue full — records are being dropped"
                );
            }
        }
    }

    /// Records dropped because the queue was full.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn drain(store: &EpisodicStore) -> Vec<serde_json::Value> {
        // The writer batches on a 500 ms window; give it room.
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let events = store
                .query_by_source(QUERY_LOG_SOURCE, 100)
                .await
                .unwrap_or_default();
            if !events.is_empty() {
                return events.into_iter().map(|e| e.payload).collect();
            }
        }
        Vec::new()
    }

    fn record(query: &str, results: usize) -> QueryRecord {
        QueryRecord {
            tenant_id: "acme".into(),
            user_id: Some("alice".into()),
            agent_id: None,
            project: Some("platform".into()),
            query: Some(query.into()),
            k: 5,
            results,
            matched: results > 0,
            top_subject: (results > 0).then(|| "platform/docs/runbook.md#failover".to_string()),
            top_similarity: (results > 0).then_some(0.712),
            duration_ms: 14.25,
        }
    }

    #[tokio::test]
    async fn records_land_as_queryable_events() {
        let store = Arc::new(EpisodicStore::new());
        let log = QueryLogger::spawn(store.clone(), true);
        log.record(record("why did we choose USearch", 3));
        let events = drain(&store).await;
        assert_eq!(events.len(), 1, "record did not reach the store");
        let e = &events[0];
        assert_eq!(e["query"], "why did we choose USearch");
        assert_eq!(e["results"], 3);
        assert_eq!(e["empty"], false);
        assert_eq!(e["project"], "platform");
        assert_eq!(e["user_id"], "alice");
        assert_eq!(
            e["_tenant_id"], "acme",
            "must be tenant-scoped like any event"
        );
        assert!(e["top_similarity"].as_f64().is_some());
    }

    /// The field the feature exists for: which questions found nothing.
    #[tokio::test]
    async fn empty_results_are_flagged() {
        let store = Arc::new(EpisodicStore::new());
        let log = QueryLogger::spawn(store.clone(), true);
        log.record(record("our S3 key rotation policy", 0));
        let events = drain(&store).await;
        assert_eq!(events[0]["empty"], true);
        assert_eq!(events[0]["results"], 0);
        assert!(
            events[0].get("top_subject").is_none(),
            "nothing matched — there is no top subject to report"
        );
    }

    /// Metrics without content: a team that will not store what people typed can still measure how
    /// often the corpus comes up empty.
    #[tokio::test]
    async fn query_text_is_omitted_when_disabled() {
        let store = Arc::new(EpisodicStore::new());
        let log = QueryLogger::spawn(store.clone(), false);
        log.record(record("something sensitive", 2));
        let events = drain(&store).await;
        assert!(
            events[0].get("query").is_none(),
            "query text was recorded despite include_query = false"
        );
        assert_eq!(events[0]["results"], 2, "the shape is still recorded");
    }

    /// Telemetry must never apply backpressure to the thing it measures.
    #[tokio::test]
    async fn a_full_queue_drops_rather_than_blocks() {
        let store = Arc::new(EpisodicStore::new());
        let log = QueryLogger::spawn(store, true);
        // Far more than the channel holds, submitted without yielding, so the writer cannot keep
        // up. If `record` blocked, this would hang instead of returning.
        for i in 0..(CAPACITY * 2) {
            log.record(record(&format!("q{i}"), 1));
        }
        assert!(
            log.dropped() > 0,
            "expected drops once the bounded queue filled"
        );
    }
}
