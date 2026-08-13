//! Inverted lexical index over the `memories` corpus — SQLite FTS5.
//!
//! ## Why this exists
//!
//! The original lexical arm ([`super::cognition::lexical_rank`]) scores BM25 in Rust over a
//! candidate window fetched with `list_active(scope, retrieval_scan_cap)` — the top *N* memories
//! by `importance DESC, valid_from DESC`. That is a hard recall ceiling, not a latency knob: once
//! a scope holds more than `retrieval_scan_cap` memories, everything below the cutoff is
//! **structurally invisible** to keyword search, no matter how well it matches. A knowledge base
//! that keeps growing keeps burying its own history.
//!
//! FTS5 replaces the window with a real inverted index: every term, every document, ranked by
//! SQLite's built-in `bm25()`. No new dependency — `rusqlite` is already in the tree for the state
//! store, and its bundled SQLite is compiled with `-DSQLITE_ENABLE_FTS5` unconditionally.
//!
//! ## Tokenization
//!
//! [`super::cognition::tokenize`] splits on every non-alphanumeric character, which shreds exactly
//! the terms an engineering corpus is searched by: `ECPHORIA_STORAGE__DATA_DIR` becomes four
//! common words, `ecphoria-core` becomes two, `PROJ-1234` becomes a stopword-ish `proj` plus a
//! bare number. This index configures `unicode61` with `tokenchars '-_.'` so identifiers, env
//! vars, ticket keys and dotted paths survive as single terms — worth 20 points of identifier
//! recall@5 on the KB eval set (`examples/kb_eval.rs`).
//!
//! `porter` stems on top of that. It is applied to documents and queries alike, so it can only
//! merge terms, never desynchronise the two sides; measured on the KB set it is worth ~1.5 points
//! at 5k memories and costs ~1.5 at 50 documents.
//!
//! ## What this index does and does not decide
//!
//! It decides **which** memories are considered, not in **what order** they come back. The engine
//! runs it as stage one of a two-stage lexical arm: FTS5 proposes candidates from the whole
//! corpus, then [`super::cognition::lexical_rank`] — the in-Rust BM25 that predates this module —
//! scores them. That split is deliberate and measured: SQLite's `bm25()` ranks this corpus a few
//! points worse (it counts stop words toward document length, penalising prose against terse
//! reference docs), while `lexical_rank` alone cannot see past a fixed candidate window. Together
//! recall stays flat from 50 to 200k memories with no ranking regression.
//!
//! ## Consistency contract: the index is *advisory*
//!
//! Entries are written best-effort alongside the DuckDB row, exactly like the USearch vector
//! index. The index is never the source of truth: it returns candidate **ids**, and the caller
//! re-reads those ids from DuckDB filtered to `state = 'active'` and to the exact scope. A stale
//! entry therefore costs one wasted candidate slot and can never surface a deleted, superseded or
//! out-of-scope memory. Rebuild-on-boot and `/admin/reindex` converge it.

use std::path::Path;

use parking_lot::Mutex;
use rusqlite::Connection;
use uuid::Uuid;

/// One memory as the index stores it.
#[derive(Debug, Clone)]
pub struct LexicalEntry {
    pub id: Uuid,
    /// Exact-scope partition key (see [`super::cognition::scope_partition_key`]).
    pub scope_key: String,
    /// Project within that scope, for narrowing a search without splitting the store.
    pub project: Option<String>,
    pub subject: Option<String>,
    pub content: String,
}

/// Inverted index over memory `subject` + `content`, partitioned by exact scope.
pub struct LexicalIndex {
    db: Mutex<Connection>,
}

impl std::fmt::Debug for LexicalIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LexicalIndex").finish()
    }
}

impl LexicalIndex {
    /// Open (or create) the index. `:memory:` gives a private in-process index.
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
        .map_err(|e| crate::Error::Storage(format!("failed to open lexical index: {e}")))?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS mem_docs (
                 rowid   INTEGER PRIMARY KEY AUTOINCREMENT,
                 id      TEXT NOT NULL UNIQUE,
                 scope   TEXT NOT NULL,
                 project TEXT
             );
             CREATE INDEX IF NOT EXISTS mem_docs_scope ON mem_docs(scope);
             CREATE INDEX IF NOT EXISTS mem_docs_project ON mem_docs(scope, project);
             CREATE VIRTUAL TABLE IF NOT EXISTS mem_fts USING fts5(
                 subject,
                 content,
                 tokenize = \"porter unicode61 tokenchars '-_.'\"
             );",
        )
        .map_err(|e| crate::Error::Storage(format!("failed to init lexical index: {e}")))?;

        Ok(Self {
            db: Mutex::new(conn),
        })
    }

    /// In-memory index (tests, embedded mode without a data dir).
    pub fn in_memory() -> crate::Result<Self> {
        Self::open(Path::new(":memory:"))
    }

    /// Index (or re-index) one memory. Idempotent on `id`.
    pub fn upsert(
        &self,
        id: Uuid,
        scope_key: &str,
        project: Option<&str>,
        subject: Option<&str>,
        content: &str,
    ) -> crate::Result<()> {
        let mut db = self.db.lock();
        let tx = db
            .transaction()
            .map_err(|e| crate::Error::Storage(format!("lexical tx: {e}")))?;
        let id_s = id.to_string();
        // Drop any previous body for this id before re-inserting: FTS5 has no upsert.
        tx.execute(
            "DELETE FROM mem_fts WHERE rowid = (SELECT rowid FROM mem_docs WHERE id = ?1)",
            [&id_s],
        )
        .map_err(|e| crate::Error::Storage(format!("lexical delete: {e}")))?;
        tx.execute(
            "INSERT INTO mem_docs(id, scope, project) VALUES(?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET scope = excluded.scope, project = excluded.project",
            rusqlite::params![&id_s, scope_key, project],
        )
        .map_err(|e| crate::Error::Storage(format!("lexical doc upsert: {e}")))?;
        let rowid: i64 = tx
            .query_row("SELECT rowid FROM mem_docs WHERE id = ?1", [&id_s], |r| {
                r.get(0)
            })
            .map_err(|e| crate::Error::Storage(format!("lexical rowid: {e}")))?;
        tx.execute(
            "INSERT INTO mem_fts(rowid, subject, content) VALUES(?1, ?2, ?3)",
            rusqlite::params![rowid, subject.unwrap_or(""), content],
        )
        .map_err(|e| crate::Error::Storage(format!("lexical fts insert: {e}")))?;
        tx.commit()
            .map_err(|e| crate::Error::Storage(format!("lexical commit: {e}")))?;
        Ok(())
    }

    /// Index many memories in a single transaction.
    ///
    /// SQLite commits (and fsyncs) per transaction, so one-at-a-time `upsert` pays that cost per
    /// memory. Bulk ingest of a document corpus is the case this exists for.
    pub fn upsert_batch(&self, rows: &[LexicalEntry]) -> crate::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut db = self.db.lock();
        let tx = db
            .transaction()
            .map_err(|e| crate::Error::Storage(format!("lexical tx: {e}")))?;
        for LexicalEntry {
            id,
            scope_key,
            project,
            subject,
            content,
        } in rows
        {
            let id_s = id.to_string();
            tx.execute(
                "DELETE FROM mem_fts WHERE rowid = (SELECT rowid FROM mem_docs WHERE id = ?1)",
                [&id_s],
            )
            .map_err(|e| crate::Error::Storage(format!("lexical delete: {e}")))?;
            tx.execute(
                "INSERT INTO mem_docs(id, scope, project) VALUES(?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET scope = excluded.scope, project = excluded.project",
                rusqlite::params![&id_s, scope_key, project],
            )
            .map_err(|e| crate::Error::Storage(format!("lexical doc upsert: {e}")))?;
            let rowid: i64 = tx
                .query_row("SELECT rowid FROM mem_docs WHERE id = ?1", [&id_s], |r| {
                    r.get(0)
                })
                .map_err(|e| crate::Error::Storage(format!("lexical rowid: {e}")))?;
            tx.execute(
                "INSERT INTO mem_fts(rowid, subject, content) VALUES(?1, ?2, ?3)",
                rusqlite::params![rowid, subject.as_deref().unwrap_or(""), content],
            )
            .map_err(|e| crate::Error::Storage(format!("lexical fts insert: {e}")))?;
        }
        tx.commit()
            .map_err(|e| crate::Error::Storage(format!("lexical commit: {e}")))?;
        Ok(())
    }

    /// Remove a memory from the index. Absent ids are a no-op.
    pub fn remove(&self, id: Uuid) -> crate::Result<()> {
        let mut db = self.db.lock();
        let tx = db
            .transaction()
            .map_err(|e| crate::Error::Storage(format!("lexical tx: {e}")))?;
        let id_s = id.to_string();
        tx.execute(
            "DELETE FROM mem_fts WHERE rowid = (SELECT rowid FROM mem_docs WHERE id = ?1)",
            [&id_s],
        )
        .map_err(|e| crate::Error::Storage(format!("lexical delete: {e}")))?;
        tx.execute("DELETE FROM mem_docs WHERE id = ?1", [&id_s])
            .map_err(|e| crate::Error::Storage(format!("lexical doc delete: {e}")))?;
        tx.commit()
            .map_err(|e| crate::Error::Storage(format!("lexical commit: {e}")))?;
        Ok(())
    }

    /// Drop every entry (used before a full rebuild).
    pub fn clear(&self) -> crate::Result<()> {
        let db = self.db.lock();
        db.execute_batch("DELETE FROM mem_fts; DELETE FROM mem_docs;")
            .map_err(|e| crate::Error::Storage(format!("lexical clear: {e}")))?;
        Ok(())
    }

    /// Number of indexed memories.
    pub fn len(&self) -> usize {
        let db = self.db.lock();
        db.query_row("SELECT COUNT(*) FROM mem_docs", [], |r| r.get::<_, i64>(0))
            .map(|n| n as usize)
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Rank memories in `scope_key` against `query`, best first.
    ///
    /// Returns `(id, score)` with score > 0 and larger = better (SQLite's `bm25()` returns
    /// negative values where more-negative is a better match, so it is negated here to match the
    /// convention of [`super::cognition::lexical_rank`]).
    ///
    /// An empty result is returned — rather than an error — when the query has no usable terms,
    /// so callers can treat "no lexical signal" and "index unavailable" the same way.
    pub fn search(
        &self,
        scope_key: &str,
        project: Option<&str>,
        query: &str,
        limit: usize,
    ) -> crate::Result<Vec<(Uuid, f32)>> {
        let match_expr = match build_match_expr(query) {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };
        let db = self.db.lock();
        let mut stmt = db
            .prepare(
                // `?3 IS NULL` keeps one prepared statement for both the filtered and unfiltered
                // case; the index on (scope, project) serves either.
                "SELECT d.id, bm25(mem_fts) AS score
                 FROM mem_fts
                 JOIN mem_docs d ON d.rowid = mem_fts.rowid
                 WHERE mem_fts MATCH ?1 AND d.scope = ?2
                   AND (?3 IS NULL OR d.project = ?3)
                 ORDER BY score
                 LIMIT ?4",
            )
            .map_err(|e| crate::Error::Query(format!("lexical prepare: {e}")))?;
        let rows = stmt
            .query_map(
                rusqlite::params![match_expr, scope_key, project, limit as i64],
                |row| {
                    let id: String = row.get(0)?;
                    let score: f64 = row.get(1)?;
                    Ok((id, score))
                },
            )
            .map_err(|e| crate::Error::Query(format!("lexical query: {e}")))?;
        Ok(rows
            .filter_map(|r| r.ok())
            .filter_map(|(id, score)| Uuid::parse_str(&id).ok().map(|u| (u, -score as f32)))
            .collect())
    }
}

/// Turn free text into a safe FTS5 `MATCH` expression.
///
/// FTS5's MATCH argument is a query *language* (`AND`, `OR`, `NOT`, `NEAR`, `*`, `^`, column
/// filters, parentheses). Passing raw user text through would either error on stray punctuation or
/// silently reinterpret words like `OR` as operators. Every term is therefore extracted, quoted as
/// a literal phrase, and joined with explicit `OR` — bag-of-words semantics matching what
/// [`super::cognition::lexical_rank`] does, with `bm25()` doing the weighting.
///
/// Returns `None` when nothing usable survives (empty query, all stop words, all punctuation).
fn build_match_expr(query: &str) -> Option<String> {
    let terms: Vec<String> = query
        .split(|c: char| !(c.is_alphanumeric() || c == '-' || c == '_' || c == '.'))
        .filter(|t| !t.is_empty())
        // Trim punctuation that is a token char mid-word but noise at the edges ("v1." → "v1").
        .map(|t| t.trim_matches(|c| c == '.' || c == '-' || c == '_'))
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .filter(|t| !super::cognition::is_stop_word(t))
        .collect();
    if terms.is_empty() {
        return None;
    }
    // Escape embedded double quotes by doubling them (FTS5 string-literal rules).
    Some(
        terms
            .iter()
            .map(|t| format!("\"{}\"", t.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" OR "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idx() -> LexicalIndex {
        LexicalIndex::in_memory().expect("index")
    }

    #[test]
    fn indexes_and_ranks_by_relevance() {
        let ix = idx();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        ix.upsert(
            a,
            "t",
            None,
            Some("adr-002"),
            "USearch was chosen over pgvector for the index",
        )
        .unwrap();
        ix.upsert(
            b,
            "t",
            None,
            Some("readme"),
            "A general overview of the project",
        )
        .unwrap();
        let hits = ix
            .search("t", None, "why usearch over pgvector", 10)
            .unwrap();
        assert_eq!(hits[0].0, a, "the matching document ranks first");
        assert!(hits[0].1 > 0.0, "scores are positive, larger = better");
    }

    #[test]
    fn identifiers_survive_tokenization() {
        // The whole point of `tokenchars '-_.'`: these must not shred into common words.
        let ix = idx();
        let target = Uuid::new_v4();
        let decoy = Uuid::new_v4();
        ix.upsert(
            target,
            "t",
            None,
            None,
            "Set ECPHORIA_STORAGE__DATA_DIR to change the data directory",
        )
        .unwrap();
        ix.upsert(
            decoy,
            "t",
            None,
            None,
            "The storage engine writes data to a directory on disk",
        )
        .unwrap();
        let hits = ix
            .search("t", None, "ECPHORIA_STORAGE__DATA_DIR", 10)
            .unwrap();
        assert_eq!(hits.len(), 1, "only the exact identifier matches: {hits:?}");
        assert_eq!(hits[0].0, target);

        let hits = ix.search("t", None, "ecphoria-core crate", 10).unwrap();
        assert!(
            hits.is_empty(),
            "hyphenated identifier is one term, not two"
        );
    }

    #[test]
    fn scope_partitions_are_isolated() {
        let ix = idx();
        let mine = Uuid::new_v4();
        ix.upsert(mine, "tenant-a", None, None, "shared secret plan")
            .unwrap();
        ix.upsert(Uuid::new_v4(), "tenant-b", None, None, "shared secret plan")
            .unwrap();
        let hits = ix
            .search("tenant-a", None, "shared secret plan", 10)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, mine);
    }

    #[test]
    fn upsert_replaces_rather_than_duplicates() {
        let ix = idx();
        let id = Uuid::new_v4();
        ix.upsert(id, "t", None, None, "the original body").unwrap();
        ix.upsert(id, "t", None, None, "a completely rewritten body")
            .unwrap();
        assert_eq!(ix.len(), 1);
        assert!(ix.search("t", None, "original", 10).unwrap().is_empty());
        assert_eq!(ix.search("t", None, "rewritten", 10).unwrap().len(), 1);
    }

    #[test]
    fn remove_and_clear() {
        let ix = idx();
        let id = Uuid::new_v4();
        ix.upsert(id, "t", None, None, "transient note").unwrap();
        ix.remove(id).unwrap();
        assert!(ix.is_empty());
        assert!(ix.search("t", None, "transient", 10).unwrap().is_empty());

        ix.upsert(Uuid::new_v4(), "t", None, None, "another note")
            .unwrap();
        ix.clear().unwrap();
        assert!(ix.is_empty());
    }

    #[test]
    fn query_operators_are_treated_as_literal_text() {
        // FTS5 would otherwise parse these as syntax and error or mis-rank.
        let ix = idx();
        let id = Uuid::new_v4();
        ix.upsert(id, "t", None, None, "the release notes mention a fix")
            .unwrap();
        for q in [
            "release OR notes",
            "release AND notes",
            "\"release\" NEAR notes",
            "release*",
            "(release notes)",
            "release -notes",
            "NOT release",
        ] {
            let hits = ix
                .search("t", None, q, 10)
                .expect("query must not error: {q}");
            assert_eq!(hits.len(), 1, "query {q:?} should match the one document");
        }
    }

    #[test]
    fn empty_and_stopword_only_queries_return_nothing() {
        let ix = idx();
        ix.upsert(Uuid::new_v4(), "t", None, None, "some content")
            .unwrap();
        assert!(ix.search("t", None, "", 10).unwrap().is_empty());
        assert!(ix.search("t", None, "   ", 10).unwrap().is_empty());
        assert!(ix.search("t", None, "the a of and", 10).unwrap().is_empty());
        assert!(ix.search("t", None, "!!! ??? ...", 10).unwrap().is_empty());
    }

    #[test]
    fn recall_does_not_degrade_with_corpus_size() {
        // The regression this module exists to prevent: the old lexical arm scanned a fixed
        // window ordered by importance/recency, so a document added early became unreachable once
        // enough newer memories existed. Here the needle is indexed *first*, then buried.
        let ix = idx();
        let needle = Uuid::new_v4();
        ix.upsert(
            needle,
            "t",
            None,
            Some("adr-007"),
            "we adopted quorum leases for shard handoff",
        )
        .unwrap();
        for i in 0..5_000 {
            ix.upsert(
                Uuid::new_v4(),
                "t",
                None,
                None,
                &format!("routine background compaction completed for partition {i}"),
            )
            .unwrap();
        }
        let hits = ix
            .search("t", None, "quorum leases shard handoff", 10)
            .unwrap();
        assert_eq!(
            hits[0].0, needle,
            "buried document is still rank 1 among 5001"
        );
    }
}
