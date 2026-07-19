//! Embedded, no-server API — **the "SQLite of agent memory."**
//!
//! Run Ecphoria's memory intelligence *in-process*: no server, no network, no config to write.
//! [`Ecphoria::open`] gives a file-backed store (like opening a SQLite file);
//! [`Ecphoria::open_in_memory`] an ephemeral one. The full engine ([`Self::engine`]) is always
//! there for the advanced API (sessions, SQL, graph, agent runtime).
//!
//! ```no_run
//! use ecphoria_core::embedded::Ecphoria;
//!
//! # async fn demo() -> ecphoria_core::Result<()> {
//! let mem = Ecphoria::open("./agent-memory").await?;
//! mem.remember("alice", "Alice prefers window seats").await?;
//! let hits = mem.recall("alice", "seating preference", 5).await?;
//! for hit in hits {
//!     println!("{}", hit.memory.content);
//! }
//! # Ok(())
//! # }
//! ```

use std::path::Path;
use std::sync::Arc;

use crate::config::CoreConfig;
use crate::engine::EcphoriaEngine;
use crate::memory::cognition::{Memory, MemoryAdd, MemoryHit, MemoryInput, MemoryScope};
use crate::Result;

/// An embedded Ecphoria memory. Cheap to clone (shares one engine).
#[derive(Clone)]
pub struct Ecphoria {
    engine: Arc<EcphoriaEngine>,
}

impl Ecphoria {
    /// Open a **file-backed** memory rooted at `dir` (created if missing). Everything — episodic
    /// events, distilled memories, agent state, and the vector index — persists under `dir`, so a
    /// later `open` on the same path resumes where you left off.
    pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).map_err(|e| crate::Error::Storage(e.to_string()))?;
        let p = |f: &str| dir.join(f).to_string_lossy().to_string();
        let mut cfg = CoreConfig::default();
        cfg.storage.data_dir = dir.to_string_lossy().to_string();
        cfg.memory.episodic.db_path = p("episodic.duckdb");
        cfg.memory.state.db_path = p("state.db");
        cfg.memory.cognition.db_path = p("memories.duckdb");
        cfg.memory.semantic.index_dir = p("vectors");
        cfg.runtime.db_path = p("runtime.db");
        Self::with_config(cfg).await
    }

    /// Open a purely **in-memory** instance — nothing is persisted. Ideal for tests and ephemeral
    /// agents.
    pub async fn open_in_memory() -> Result<Self> {
        let mut cfg = CoreConfig::default();
        cfg.memory.episodic.db_path = ":memory:".into();
        cfg.memory.state.db_path = ":memory:".into();
        cfg.memory.cognition.db_path = ":memory:".into();
        cfg.runtime.db_path = ":memory:".into();
        Self::with_config(cfg).await
    }

    /// Open with a fully custom [`CoreConfig`] — e.g. to wire an embedding provider for vector
    /// recall, tune retention, or point at S3 storage.
    pub async fn with_config(config: CoreConfig) -> Result<Self> {
        Ok(Self {
            engine: Arc::new(EcphoriaEngine::new(config).await?),
        })
    }

    /// The underlying [`EcphoriaEngine`] — the full API (sessions, SQL, graph, agent runtime,
    /// tenant-scoped ops, bi-temporal history, …) when the convenience methods aren't enough.
    pub fn engine(&self) -> &Arc<EcphoriaEngine> {
        &self.engine
    }

    /// Distill `text` into atomic memories for `user` — LLM extraction if an extraction backend is
    /// configured, else stored as one memory — with dedup + deterministic contradiction resolution.
    pub async fn remember(&self, user: &str, text: &str) -> Result<Vec<MemoryAdd>> {
        self.engine
            .memory_remember(text, &MemoryScope::user(user))
            .await
    }

    /// Store one memory verbatim for `user` (dedup + contradiction-supersede applied).
    pub async fn add(&self, user: &str, content: &str) -> Result<MemoryAdd> {
        self.engine
            .memory_add(MemoryInput::new(MemoryScope::user(user), content))
            .await
    }

    /// Hybrid recall (BM25 fused with vector via RRF) of `user`'s memories for `query`.
    pub async fn recall(&self, user: &str, query: &str, k: usize) -> Result<Vec<MemoryHit>> {
        self.engine
            .memory_search(query, &MemoryScope::user(user), k)
            .await
    }

    /// All active memories for `user`, importance-ordered.
    pub async fn all(&self, user: &str, limit: usize) -> Result<Vec<Memory>> {
        self.engine
            .memory_all(&MemoryScope::user(user), limit)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn embedded_add_recall_and_supersede() {
        let mem = Ecphoria::open_in_memory().await.unwrap();

        mem.add("alice", "Alice lives in Paris").await.unwrap();
        mem.remember("alice", "Alice loves espresso").await.unwrap();

        // Recall finds a memory (BM25 works with no embedding provider).
        let hits = mem
            .recall("alice", "where does alice live", 5)
            .await
            .unwrap();
        assert!(
            hits.iter().any(|h| h.memory.content.contains("Paris")),
            "recall should surface the Paris memory"
        );

        // Scoping is per-user: bob sees nothing of alice's.
        assert!(mem.all("bob", 10).await.unwrap().is_empty());
        assert!(!mem.all("alice", 10).await.unwrap().is_empty());
    }
}
