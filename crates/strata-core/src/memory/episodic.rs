//! Episodic memory store — append-only event storage backed by DuckDB.

use chrono::{DateTime, Utc};
use duckdb::Connection;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

/// A single episodic event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

/// Append-only event store backed by DuckDB.
#[derive(Debug)]
pub struct EpisodicStore {
    db: Arc<Mutex<Connection>>,
}

impl EpisodicStore {
    /// Create an in-memory episodic store.
    pub fn new() -> Self {
        Self::open(Path::new(":memory:")).expect("failed to create in-memory episodic store")
    }

    /// Open or create an episodic store at the given path.
    ///
    /// Use `:memory:` for an in-memory store (testing) or a file path for persistence.
    pub fn open(path: &Path) -> crate::Result<Self> {
        let conn = if path.as_os_str() == ":memory:" {
            Connection::open_in_memory()
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    crate::Error::Storage(format!("failed to create directory: {e}"))
                })?;
            }
            Connection::open(path)
        }
        .map_err(|e| crate::Error::Storage(format!("failed to open duckdb: {e}")))?;

        Self::init_schema(&conn)?;
        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
        })
    }

    fn init_schema(conn: &Connection) -> crate::Result<()> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS episodic (
                id         VARCHAR PRIMARY KEY,
                source     VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                payload    VARCHAR NOT NULL,
                ts         VARCHAR NOT NULL
            );",
        )
        .map_err(|e| crate::Error::Storage(format!("failed to create table: {e}")))?;
        Ok(())
    }

    /// Append events to the episodic store.
    pub async fn append(&self, events: &[Event]) -> crate::Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let start = std::time::Instant::now();
        let db = self.db.lock();

        db.execute_batch("BEGIN TRANSACTION")
            .map_err(|e| crate::Error::Ingest(format!("begin transaction: {e}")))?;

        let result = (|| {
            let mut stmt = db
                .prepare(
                    "INSERT INTO episodic (id, source, event_type, payload, ts)
                     VALUES (?, ?, ?, ?, ?)",
                )
                .map_err(|e| crate::Error::Ingest(format!("prepare error: {e}")))?;

            for event in events {
                let payload_str = serde_json::to_string(&event.payload)
                    .map_err(|e| crate::Error::Ingest(e.to_string()))?;
                let ts_str = event.timestamp.to_rfc3339();
                stmt.execute(duckdb::params![
                    event.id.to_string(),
                    event.source,
                    event.event_type,
                    payload_str,
                    ts_str,
                ])
                .map_err(|e| crate::Error::Ingest(format!("insert error: {e}")))?;
            }

            Ok(events.len() as u64)
        })();

        match &result {
            Ok(_) => {
                db.execute_batch("COMMIT")
                    .map_err(|e| crate::Error::Ingest(format!("commit: {e}")))?;
            }
            Err(_) => {
                let _ = db.execute_batch("ROLLBACK");
            }
        }

        // Record metrics
        metrics::histogram!("strata_episodic_append_duration_seconds")
            .record(start.elapsed().as_secs_f64());
        if let Ok(count) = &result {
            metrics::counter!("strata_episodic_events_ingested_total").increment(*count);
        }

        result
    }

    /// Query events within a time range.
    pub async fn query_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> crate::Result<Vec<Event>> {
        let start_str = start.to_rfc3339();
        let end_str = end.to_rfc3339();

        let db = self.db.lock();
        let mut stmt = db
            .prepare(
                "SELECT id, source, event_type, payload, ts
                 FROM episodic
                 WHERE ts >= ? AND ts <= ?
                 ORDER BY ts ASC",
            )
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        let rows = stmt
            .query_map(duckdb::params![start_str, end_str], |row| {
                let id_str: String = row.get(0)?;
                let payload_str: String = row.get(3)?;
                let ts_str: String = row.get(4)?;
                Ok(Event {
                    id: Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::nil()),
                    source: row.get(1)?,
                    event_type: row.get(2)?,
                    payload: serde_json::from_str(&payload_str).unwrap_or(serde_json::Value::Null),
                    timestamp: DateTime::parse_from_rfc3339(&ts_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                })
            })
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Query events by source.
    pub async fn query_by_source(&self, source: &str, limit: usize) -> crate::Result<Vec<Event>> {
        let db = self.db.lock();
        let mut stmt = db
            .prepare(
                "SELECT id, source, event_type, payload, ts
                 FROM episodic
                 WHERE source = ?
                 ORDER BY ts DESC
                 LIMIT ?",
            )
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        let rows = stmt
            .query_map(duckdb::params![source, limit as i64], |row| {
                let id_str: String = row.get(0)?;
                let payload_str: String = row.get(3)?;
                let ts_str: String = row.get(4)?;
                Ok(Event {
                    id: Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::nil()),
                    source: row.get(1)?,
                    event_type: row.get(2)?,
                    payload: serde_json::from_str(&payload_str).unwrap_or(serde_json::Value::Null),
                    timestamp: DateTime::parse_from_rfc3339(&ts_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                })
            })
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Validate that a SQL string contains only SELECT statements.
    fn validate_read_only(sql: &str) -> crate::Result<()> {
        use sqlparser::dialect::DuckDbDialect;
        use sqlparser::parser::Parser;

        let statements = Parser::parse_sql(&DuckDbDialect {}, sql)
            .map_err(|e| crate::Error::Query(format!("SQL parse error: {e}")))?;

        if statements.is_empty() {
            return Err(crate::Error::Query("empty SQL statement".into()));
        }

        for stmt in &statements {
            match stmt {
                sqlparser::ast::Statement::Query(_) => {}
                other => {
                    return Err(crate::Error::Query(format!(
                        "only SELECT queries are allowed, got: {}",
                        other
                    )));
                }
            }
        }

        Ok(())
    }

    /// Execute a read-only SQL query and return results as JSON rows.
    ///
    /// Only SELECT queries are permitted. DDL/DML (DROP, INSERT, etc.) is rejected.
    /// Results are capped at `max_rows` to prevent unbounded memory allocation.
    pub fn query_sql(&self, sql: &str) -> crate::Result<Vec<serde_json::Value>> {
        self.query_sql_limited(sql, 10_000)
    }

    /// Execute a read-only SQL query with an explicit row limit.
    pub fn query_sql_limited(
        &self,
        sql: &str,
        max_rows: usize,
    ) -> crate::Result<Vec<serde_json::Value>> {
        let start = std::time::Instant::now();
        Self::validate_read_only(sql)?;

        let db = self.db.lock();
        let mut stmt = db
            .prepare(sql)
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        let mut rows_iter = stmt
            .query([])
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        // Get column names after execution
        let column_count = rows_iter.as_ref().unwrap().column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| {
                rows_iter
                    .as_ref()
                    .unwrap()
                    .column_name(i)
                    .map_or("?".to_string(), |v| v.to_string())
            })
            .collect();

        let mut results = Vec::with_capacity(max_rows.min(1024));
        while let Some(row) = rows_iter
            .next()
            .map_err(|e| crate::Error::Query(e.to_string()))?
        {
            if results.len() >= max_rows {
                break;
            }
            let mut obj = serde_json::Map::new();
            for (i, name) in column_names.iter().enumerate() {
                let val: String = row.get::<_, String>(i).unwrap_or_default();
                obj.insert(name.clone(), serde_json::Value::String(val));
            }
            results.push(serde_json::Value::Object(obj));
        }

        metrics::histogram!("strata_episodic_query_duration_seconds")
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("strata_episodic_queries_total").increment(1);

        Ok(results)
    }

    /// Return the total number of stored events.
    pub async fn count(&self) -> crate::Result<u64> {
        let db = self.db.lock();
        let count: i64 = db
            .query_row("SELECT count(*) FROM episodic", [], |row| row.get(0))
            .map_err(|e| crate::Error::Query(e.to_string()))?;
        Ok(count as u64)
    }
}

impl Default for EpisodicStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(source: &str, event_type: &str) -> Event {
        Event {
            id: Uuid::new_v4(),
            source: source.into(),
            event_type: event_type.into(),
            payload: serde_json::json!({"key": "value"}),
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn new_store_has_zero_count() {
        let store = EpisodicStore::new();
        assert_eq!(store.count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn append_and_count() {
        let store = EpisodicStore::new();
        let events = vec![make_event("app", "click"), make_event("app", "view")];
        let count = store.append(&events).await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(store.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn append_empty_batch() {
        let store = EpisodicStore::new();
        let count = store.append(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn query_by_source() {
        let store = EpisodicStore::new();
        store
            .append(&[
                make_event("app-a", "click"),
                make_event("app-b", "view"),
                make_event("app-a", "submit"),
            ])
            .await
            .unwrap();

        let events = store.query_by_source("app-a", 10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e.source == "app-a"));
    }

    #[tokio::test]
    async fn query_time_range() {
        let store = EpisodicStore::new();
        let past = Utc::now() - chrono::Duration::hours(1);
        let future = Utc::now() + chrono::Duration::hours(1);

        store.append(&[make_event("app", "event")]).await.unwrap();

        let events = store.query_time_range(past, future).await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn query_sql_count() {
        let store = EpisodicStore::new();
        store.append(&[make_event("src", "type")]).await.unwrap();

        let rows = store
            .query_sql("SELECT count(*)::VARCHAR as cnt FROM episodic")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["cnt"], "1");
    }

    #[test]
    fn event_serialization_roundtrip() {
        let event = make_event("my-app", "order.placed");
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, event.id);
        assert_eq!(deserialized.source, "my-app");
    }

    #[tokio::test]
    async fn large_batch_append() {
        let store = EpisodicStore::new();
        let events: Vec<Event> = (0..500)
            .map(|i| make_event("bench", &format!("event.{i}")))
            .collect();
        let count = store.append(&events).await.unwrap();
        assert_eq!(count, 500);
        assert_eq!(store.count().await.unwrap(), 500);
    }

    #[test]
    fn query_sql_rejects_drop_table() {
        let store = EpisodicStore::new();
        let result = store.query_sql("DROP TABLE episodic");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("only SELECT"));
    }

    #[test]
    fn query_sql_rejects_insert() {
        let store = EpisodicStore::new();
        let result = store.query_sql("INSERT INTO episodic VALUES ('a','b','c','d','e')");
        assert!(result.is_err());
    }

    #[test]
    fn query_sql_allows_select() {
        let store = EpisodicStore::new();
        let result = store.query_sql("SELECT 1::VARCHAR as v");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn file_backed_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("episodic.duckdb");

        // Write data
        {
            let store = EpisodicStore::open(&db_path).unwrap();
            store.append(&[make_event("app", "click")]).await.unwrap();
            assert_eq!(store.count().await.unwrap(), 1);
        }

        // Reopen and verify data survived
        {
            let store = EpisodicStore::open(&db_path).unwrap();
            assert_eq!(store.count().await.unwrap(), 1);
            let events = store.query_by_source("app", 10).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_type, "click");
        }
    }
}
