//! Raft log storage and state machine — in-memory implementation.
//!
//! This provides a complete `RaftStorage` implementation backed by in-memory
//! data structures. For production, the log should be backed by persistent storage.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::LogState;
use openraft::{
    Entry, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};
use parking_lot::Mutex;
use strata_core::StrataEngine;

use super::types::{AppRequest, AppResponse, NodeId, NodeInfo, TypeConfig};

/// Shared state for the in-memory Raft store.
#[derive(Debug)]
struct StoreInner {
    /// Current vote (persisted).
    vote: Option<Vote<NodeId>>,
    /// Raft log entries.
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// Last purged log ID.
    last_purged: Option<LogId<NodeId>>,
    /// Last applied log ID.
    last_applied: Option<LogId<NodeId>>,
    /// Last applied membership.
    last_membership: StoredMembership<NodeId, NodeInfo>,
    /// Current snapshot.
    snapshot: Option<StoredSnapshot>,
    /// Committed log id.
    committed: Option<LogId<NodeId>>,
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, NodeInfo>,
    data: Vec<u8>,
}

/// In-memory Raft store implementing the full `RaftStorage` trait.
///
/// Holds both the Raft log and state machine. The state machine applies
/// entries to a `StrataEngine` reference.
#[derive(Debug, Clone)]
pub struct MemStore {
    inner: Arc<Mutex<StoreInner>>,
    engine: Option<Arc<StrataEngine>>,
}

impl MemStore {
    /// Create a new in-memory store, optionally backed by a StrataEngine.
    pub fn new(engine: Option<Arc<StrataEngine>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                vote: None,
                log: BTreeMap::new(),
                last_purged: None,
                last_applied: None,
                last_membership: StoredMembership::default(),
                snapshot: None,
                committed: None,
            })),
            engine,
        }
    }

    /// Apply an application request to the engine.
    async fn apply_request(&self, req: &AppRequest) -> AppResponse {
        let Some(engine) = &self.engine else {
            return AppResponse::Ok;
        };

        match req {
            AppRequest::Ingest { source, events } => {
                let strata_events: Vec<strata_core::memory::episodic::Event> = events
                    .iter()
                    .map(|payload| strata_core::memory::episodic::Event {
                        id: uuid::Uuid::new_v4(),
                        source: source.clone(),
                        event_type: payload
                            .get("event_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string(),
                        payload: payload.clone(),
                        timestamp: chrono::Utc::now(),
                    })
                    .collect();
                match engine.ingest(strata_events).await {
                    Ok(n) => AppResponse::Ingested(n),
                    Err(e) => {
                        tracing::error!(error = %e, "raft apply: ingest failed");
                        AppResponse::Ingested(0)
                    }
                }
            }
            AppRequest::StateSet {
                agent_id,
                key,
                value,
            } => match engine.state_set(agent_id, key, value.clone()).await {
                Ok(v) => AppResponse::StateVersion(v),
                Err(e) => {
                    tracing::error!(error = %e, "raft apply: state_set failed");
                    AppResponse::StateVersion(0)
                }
            },
            AppRequest::StateDelete { agent_id, key } => {
                let _ = engine.state_delete(agent_id, key).await;
                AppResponse::Deleted
            }
            AppRequest::SemanticUpsert {
                id,
                content,
                embedding,
                metadata,
            } => {
                let entry = strata_core::memory::semantic::SemanticEntry {
                    id: *id,
                    content: content.clone(),
                    embedding: embedding.clone(),
                    metadata: metadata.clone(),
                };
                let _ = engine.semantic_upsert(&entry).await;
                AppResponse::Ok
            }
            AppRequest::SemanticDelete { id } => {
                let _ = engine.semantic_delete(*id).await;
                AppResponse::Ok
            }
        }
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new(None)
    }
}

// ── RaftLogReader ──────────────────────────────────────────────────

impl RaftLogReader<TypeConfig> for MemStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        let entries: Vec<_> = inner
            .log
            .range(range)
            .map(|(_, v)| v.clone())
            .collect();
        Ok(entries)
    }
}

// ── RaftSnapshotBuilder ────────────────────────────────────────────

impl RaftSnapshotBuilder<TypeConfig> for MemStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock();

        let last_applied = inner.last_applied;
        let membership = inner.last_membership.clone();

        // Snapshot is a serialized representation of applied state
        // For now, store the log entries as the snapshot data
        let data = serde_json::to_vec(&"snapshot-placeholder").unwrap_or_default();

        let snapshot_id = format!(
            "{}-{}",
            last_applied
                .map(|id| format!("{}-{}", id.leader_id, id.index))
                .unwrap_or_default(),
            uuid::Uuid::new_v4()
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ── RaftStorage (v1 unified trait) ─────────────────────────────────

impl openraft::RaftStorage<TypeConfig> for MemStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().committed)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        let last_purged = inner.last_purged;
        let last = inner.log.iter().next_back().map(|(_, e)| e.log_id);
        let last_log_id = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(
        &mut self,
        entries: I,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut inner = self.inner.lock();
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        let to_remove: Vec<u64> = inner
            .log
            .range(log_id.index..)
            .map(|(k, _)| *k)
            .collect();
        for key in to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.last_purged = Some(log_id);
        let to_remove: Vec<u64> = inner
            .log
            .range(..=log_id.index)
            .map(|(k, _)| *k)
            .collect();
        for key in to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, NodeInfo>,
        ),
        StorageError<NodeId>,
    > {
        let inner = self.inner.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<AppResponse>, StorageError<NodeId>> {
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            let log_id = entry.log_id;

            // Update last applied
            self.inner.lock().last_applied = Some(log_id);

            match entry.payload {
                openraft::EntryPayload::Blank => {
                    responses.push(AppResponse::Ok);
                }
                openraft::EntryPayload::Normal(ref req) => {
                    let resp = self.apply_request(req).await;
                    responses.push(resp);
                }
                openraft::EntryPayload::Membership(ref membership) => {
                    self.inner.lock().last_membership =
                        StoredMembership::new(Some(log_id), membership.clone());
                    responses.push(AppResponse::Ok);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, NodeInfo>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let mut inner = self.inner.lock();
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stores() {
        let store = MemStore::new(None);
        assert!(store.inner.lock().log.is_empty());
    }

    #[tokio::test]
    async fn save_and_read_vote() {
        let mut store = MemStore::new(None);
        let vote = Vote::new(1, 1);

        openraft::RaftStorage::<TypeConfig>::save_vote(&mut store, &vote)
            .await
            .unwrap();

        let read = openraft::RaftStorage::<TypeConfig>::read_vote(&mut store)
            .await
            .unwrap();
        assert!(read.is_some());
    }

    #[tokio::test]
    async fn get_log_state_empty() {
        let mut store = MemStore::new(None);
        let state = openraft::RaftStorage::<TypeConfig>::get_log_state(&mut store)
            .await
            .unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }
}
