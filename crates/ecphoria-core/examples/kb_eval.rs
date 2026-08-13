//! Engineering knowledge-base retrieval evaluation.
//!
//! `locomo_eval` measures *conversational* memory: short personal facts, chit-chat vocabulary,
//! a few hundred memories per user. An engineering knowledge base is a different workload —
//! long structured documents, rare and precise vocabulary (`ECPHORIA_STORAGE__DATA_DIR`,
//! `openraft`, `ADR-002`), and a corpus that grows to six figures. Numbers from one do not
//! transfer to the other, so this harness measures the second directly.
//!
//! The corpus is **this repository's own documentation** — `docs/`, `docs/adr/`, the `CLAUDE.md`
//! files, the SDK and example READMEs. The gold set below is hand-written against that content,
//! so the harness is self-contained and offline: no dataset download, no API key.
//!
//! Run it:
//!   cargo run -p ecphoria-core --example kb_eval
//!
//! Hybrid (BM25 + vector) instead of lexical-only:
//!   ECPHORIA_EMBEDDING__PROVIDER=ollama ECPHORIA_EMBEDDING__MODEL=nomic-embed-text \
//!     cargo run -p ecphoria-core --example kb_eval
//!
//! **The scale test.** `KB_PAD=N` injects N synthetic technical memories into the same scope
//! *after* the real docs, so the docs sit below the newer filler in `importance DESC,
//! valid_from DESC` order. This reproduces the real failure mode of a growing knowledge base —
//! old documents buried under newer content — and is the measurement Phase 1 has to fix:
//!
//!   for n in 0 5000 50000 200000; do KB_PAD=$n cargo run -p ecphoria-core --example kb_eval; done
//!
//! A retrieval layer that scales holds its recall flat across that sweep. One that scans a
//! capped candidate window (`memory.cognition.retrieval_scan_cap`) falls off a cliff as soon as
//! N exceeds the cap.
//!
//! **CI gate.** `KB_MIN_RECALL5=<pct>` makes the harness exit non-zero when overall recall@5 falls
//! below it, so a retrieval regression fails the build instead of shipping silently. The floor is
//! deliberately set below the measured BM25-only baseline, not at it: this is a regression alarm,
//! not a target to optimise against.
//!
//! Env: `KB_ROOT` (corpus root, default = workspace root), `KB_PAD` (filler count, default 0),
//! `KB_K` (retrieval depth, default 10), plus the `ECPHORIA_EMBEDDING__*` / `ECPHORIA_RERANK__*` /
//! `ECPHORIA_COGNITION__*` overrides shared with `locomo_eval`.

use std::path::{Path, PathBuf};

use ecphoria_core::engine::DocumentIngest;
use ecphoria_core::ingest::chunk::ChunkOptions;
use ecphoria_core::memory::cognition::{MemoryInput, MemoryScope};
use ecphoria_core::{CoreConfig, EcphoriaEngine};

/// One evaluation question: the query, the document that answers it, and its shape.
///
/// `doc` is matched as a **case-insensitive** substring against the retrieved memory's `subject`
/// (the document's repo-relative path), not against its text. Matching on identity rather than on
/// an answer substring keeps the metric stable when documentation is reworded. Case-insensitive
/// because `memory_add` normalizes subjects to lowercase (`normalize_subject`), so a stored
/// `ADR-002-…` comes back as `adr-002-…`.
struct Question {
    q: &'static str,
    doc: &'static str,
    category: Category,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Category {
    /// Exact identifier lookup — env var, config key, port, crate, tool name. The shape where a
    /// tokenizer that splits `ECPHORIA_STORAGE__DATA_DIR` into four common words does most damage.
    Identifier,
    /// "Why is it built this way" — the questions ADRs exist to answer.
    Conceptual,
    /// Dates, status, and how a decision evolved.
    Temporal,
    /// The answer lives in a specific document among several plausible near-neighbours.
    MultiDoc,
}

impl Category {
    fn label(self) -> &'static str {
        match self {
            Category::Identifier => "identifier",
            Category::Conceptual => "conceptual",
            Category::Temporal => "temporal",
            Category::MultiDoc => "multi-doc",
        }
    }
}

/// Hand-written gold set over this repo's documentation.
///
/// Each entry was checked against the file it points at. Keep questions phrased the way an
/// engineer would actually ask them — including the ones that share no vocabulary with the target
/// document, since those are precisely what the vector arm is for.
const GOLD: &[Question] = &[
    // ── identifier lookups ────────────────────────────────────────────────────────────────
    Question {
        q: "which env var sets the data directory",
        doc: "deployment.md",
        category: Category::Identifier,
    },
    Question {
        q: "ECPHORIA_MEMORY__COGNITION__RETRIEVAL_SCAN_CAP default value",
        doc: "deployment.md",
        category: Category::Identifier,
    },
    Question {
        q: "what is the default dedup_threshold",
        doc: "deployment.md",
        category: Category::Identifier,
    },
    Question {
        q: "decay_half_life_days default",
        doc: "deployment.md",
        category: Category::Identifier,
    },
    Question {
        q: "how do I set the forget_threshold",
        doc: "deployment.md",
        category: Category::Identifier,
    },
    Question {
        q: "ECPHORIA_CLUSTER__PEERS",
        doc: "CLAUDE.md",
        category: Category::Identifier,
    },
    Question {
        q: "which port does the PostgreSQL wire protocol listen on",
        doc: "CLAUDE.md",
        category: Category::Identifier,
    },
    Question {
        q: "what port is used for Raft inter-node RPC",
        doc: "CLAUDE.md",
        category: Category::Identifier,
    },
    Question {
        q: "ECPHORIA_GATEWAY__AUTH_ENABLED",
        doc: "CLAUDE.md",
        category: Category::Identifier,
    },
    Question {
        q: "openraft version used",
        doc: "CLAUDE.md",
        category: Category::Identifier,
    },
    Question {
        q: "what does the sqlparser crate do here",
        doc: "CLAUDE.md",
        category: Category::Identifier,
    },
    Question {
        q: "cargo clippy command used in CI",
        doc: "CLAUDE.md",
        category: Category::Identifier,
    },
    Question {
        q: "memory.semantic.index_dir",
        doc: "ADR-002",
        category: Category::Identifier,
    },
    Question {
        q: "gateway.rate_limit_per_key token bucket",
        doc: "security.md",
        category: Category::Identifier,
    },
    Question {
        q: "scripts/gen-pg-tls.sh",
        doc: "security.md",
        category: Category::Identifier,
    },
    Question {
        q: "what is the /metrics endpoint for",
        doc: "api-reference.md",
        category: Category::Identifier,
    },
    Question {
        q: "embed-and-search REST endpoint",
        doc: "api-reference.md",
        category: Category::Identifier,
    },
    Question {
        q: "ECPHORIA_EMBEDDING__OLLAMA_URL default",
        doc: "CLAUDE.md",
        category: Category::Identifier,
    },
    Question {
        q: "rerank-local feature flag",
        doc: "benchmarks-locomo.md",
        category: Category::Identifier,
    },
    Question {
        q: "LOCOMO_PATH environment variable",
        doc: "benchmarks-locomo.md",
        category: Category::Identifier,
    },
    // ── conceptual / ADR ──────────────────────────────────────────────────────────────────
    Question {
        q: "why did we pick DuckDB instead of SQLite for events",
        doc: "ADR-001",
        category: Category::Conceptual,
    },
    Question {
        q: "why not embedded Postgres for episodic memory",
        doc: "ADR-001",
        category: Category::Conceptual,
    },
    Question {
        q: "reason we rejected ClickHouse and TimescaleDB",
        doc: "ADR-001",
        category: Category::Conceptual,
    },
    Question {
        q: "why USearch rather than pgvector",
        doc: "ADR-002",
        category: Category::Conceptual,
    },
    Question {
        q: "why was FAISS rejected for vector search",
        doc: "ADR-002",
        category: Category::Conceptual,
    },
    Question {
        q: "why can't we use Annoy for the vector index",
        doc: "ADR-002",
        category: Category::Conceptual,
    },
    Question {
        q: "what is the EntryMetadata pattern and why does it save memory",
        doc: "ADR-002",
        category: Category::Conceptual,
    },
    Question {
        q: "why Raft instead of a gossip protocol",
        doc: "ADR-003",
        category: Category::Conceptual,
    },
    Question {
        q: "why didn't we use etcd or ZooKeeper for coordination",
        doc: "ADR-003",
        category: Category::Conceptual,
    },
    Question {
        q: "what is wrong with primary-replica replication for us",
        doc: "ADR-003",
        category: Category::Conceptual,
    },
    Question {
        q: "why are odd-numbered clusters required",
        doc: "ADR-003",
        category: Category::Conceptual,
    },
    Question {
        q: "why one binary instead of microservices",
        doc: "ADR-004",
        category: Category::Conceptual,
    },
    Question {
        q: "why we rejected a plugin architecture",
        doc: "ADR-004",
        category: Category::Conceptual,
    },
    Question {
        q: "what is the blast radius risk of the current design",
        doc: "ADR-004",
        category: Category::Conceptual,
    },
    Question {
        q: "how big is the server binary and why",
        doc: "ADR-004",
        category: Category::Conceptual,
    },
    Question {
        q: "how does the retrieval pipeline fuse its arms",
        doc: "architecture.md",
        category: Category::Conceptual,
    },
    Question {
        q: "where do LLMs and embeddings actually fit in the system",
        doc: "architecture.md",
        category: Category::Conceptual,
    },
    Question {
        q: "which direction are crate dependencies allowed to go",
        doc: "CLAUDE.md",
        category: Category::Conceptual,
    },
    Question {
        q: "what happens if an agent's memory gets poisoned",
        doc: "threat-model.md",
        category: Category::Conceptual,
    },
    Question {
        q: "what does the project explicitly not guarantee",
        doc: "threat-model.md",
        category: Category::Conceptual,
    },
    Question {
        q: "why should I switch away from Mem0",
        doc: "migrate-from-mem0.md",
        category: Category::Conceptual,
    },
    Question {
        q: "what are the stated non-goals of the project",
        doc: "ROADMAP.md",
        category: Category::Conceptual,
    },
    Question {
        q: "what motivated building this in the first place",
        doc: "why-we-built-ecphoria.md",
        category: Category::Conceptual,
    },
    Question {
        q: "how should errors be handled in library code versus binaries",
        doc: "contributing.md",
        category: Category::Conceptual,
    },
    // ── temporal ──────────────────────────────────────────────────────────────────────────
    Question {
        q: "when was the DuckDB decision accepted",
        doc: "ADR-001",
        category: Category::Temporal,
    },
    Question {
        q: "what date did we decide on Raft",
        doc: "ADR-003",
        category: Category::Temporal,
    },
    Question {
        q: "which was decided first, the vector index or the clustering approach",
        doc: "ADR-002",
        category: Category::Temporal,
    },
    Question {
        q: "has the single-binary ADR been updated since it was written",
        doc: "ADR-004",
        category: Category::Temporal,
    },
    Question {
        q: "what shipped recently versus what is still planned",
        doc: "ROADMAP.md",
        category: Category::Temporal,
    },
    Question {
        q: "when does API stability begin",
        doc: "ROADMAP.md",
        category: Category::Temporal,
    },
    Question {
        q: "what changed in the most recent release",
        doc: "CHANGELOG.md",
        category: Category::Temporal,
    },
    Question {
        q: "which benchmark run produced the current published numbers",
        doc: "benchmarks-locomo.md",
        category: Category::Temporal,
    },
    Question {
        q: "what did we learn after fixing the retrieval index",
        doc: "benchmarks-compare.md",
        category: Category::Temporal,
    },
    Question {
        q: "is the rebalancing operator finished or still a design",
        doc: "operator.md",
        category: Category::Temporal,
    },
    // ── multi-doc disambiguation ──────────────────────────────────────────────────────────
    Question {
        q: "how do I connect Claude Code to this server",
        doc: "connect-claude.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "how is a tenant isolated from another tenant",
        doc: "security.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "how are API keys stored at rest",
        doc: "security.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "what happens when auth is enabled but no keys are configured",
        doc: "security.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "how do I run a three node cluster locally",
        doc: "deployment.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "what values should I set for a production Helm install",
        doc: "deployment.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "how do I get the server running in under five minutes",
        doc: "getting-started.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "how do I build from source",
        doc: "getting-started.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "how fast is a hybrid memory search",
        doc: "benchmarks.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "does graph expansion help or hurt recall",
        doc: "benchmarks-compare.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "did LLM fact extraction improve the numbers",
        doc: "benchmarks-compare.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "how do I reproduce the LoCoMo evaluation",
        doc: "benchmarks-locomo.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "what does the agent runtime give me beyond memory",
        doc: "agentic-platform.md",
        category: Category::MultiDoc,
    },
    Question {
        q: "how do I use this from Python",
        doc: "sdk",
        category: Category::MultiDoc,
    },
    Question {
        q: "is there a Go client",
        doc: "sdk/go",
        category: Category::MultiDoc,
    },
    Question {
        q: "how do I wire this into LangChain",
        doc: "langchain-rag",
        category: Category::MultiDoc,
    },
    Question {
        q: "how do I run the turnkey benchmark suite",
        doc: "ops/bench",
        category: Category::MultiDoc,
    },
    Question {
        q: "what does the Kubernetes operator reconcile loop do",
        doc: "operator.md",
        category: Category::MultiDoc,
    },
];

/// Questions this corpus genuinely cannot answer.
///
/// The gold set only contains questions the documentation *does* cover, so it can measure ranking
/// but says nothing about the case that matters for an agent: the corpus has no answer and the
/// caller has to be told. These are plausible engineering questions about subjects this repository
/// simply does not discuss — the comparison that decides whether a relevance threshold can separate
/// "answered" from "nothing here".
const OUT_OF_CORPUS: &[&str] = &[
    "what is our policy for rotating S3 bucket encryption keys",
    "which team owns the payroll integration",
    "how do we handle GDPR data subject access requests from EU customers",
    "what is the SLA for the mobile push notification service",
    "how is the Kafka consumer group rebalance timeout configured",
    "who approves changes to the Terraform production workspace",
    "what is the on-call escalation path for the billing team",
    "how do we rotate the Datadog API keys",
    "what is the retention policy for CCTV footage in the London office",
    "which vendor provides our SOC 2 audit",
    "how do we provision laptops for new starters",
    "what is the maximum photo upload size in the mobile app",
];

/// Walk `root` for tracked-looking Markdown, skipping build output and vendored trees.
fn collect_docs(root: &Path) -> Vec<(String, String)> {
    const SKIP: &[&str] = &["target", "node_modules", ".git", "data", ".cargo"];
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if path.is_dir() {
                if !SKIP.contains(&name.as_str()) {
                    stack.push(path);
                }
            } else if name.ends_with(".md") {
                if let Ok(body) = std::fs::read_to_string(&path) {
                    let rel = path
                        .strip_prefix(root)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .to_string();
                    out.push((rel, body));
                }
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Deterministic filler that *competes* with the corpus instead of being trivially separable:
/// same domain vocabulary, same register, no gold answers. Gibberish padding would leave BM25
/// scores untouched and make the scale test meaningless.
fn filler(n: usize) -> Vec<(String, String)> {
    // Topic areas, not one topic. The first version of this drew from a single ops vocabulary,
    // which competed with the corpus on BM25 terms as intended — but collapsed into a handful of
    // tight clusters in embedding space, so *any* query landed near some filler and the vector arm
    // looked far worse at scale than it is. Padding has to be diverse on both axes or it measures
    // the padding rather than the retriever.
    const TOPICS: &[(&str, &[&str], &[&str])] = &[
        (
            "infrastructure",
            &[
                "the ingest worker",
                "the shard router",
                "the snapshot writer",
                "the connection pool",
                "the retention job",
                "the metrics exporter",
                "the backup task",
                "the audit log",
            ],
            &[
                "completed a compaction of the primary partition",
                "renewed its lease during a routine handover",
                "throttled inbound traffic from a noisy client",
                "flushed pending writes before the rolling restart",
            ],
        ),
        (
            "frontend",
            &[
                "the checkout form",
                "the navigation sidebar",
                "the image carousel",
                "the date picker",
                "the notification badge",
                "the onboarding modal",
                "the settings panel",
            ],
            &[
                "was migrated to the new design tokens",
                "no longer re-renders on every keystroke",
                "now announces its state to screen readers",
                "lost a stray margin that broke the mobile layout",
            ],
        ),
        (
            "finance",
            &[
                "the invoicing run",
                "the tax rounding rule",
                "the refund workflow",
                "the currency table",
                "the dunning schedule",
                "the revenue report",
            ],
            &[
                "was reconciled against the ledger for the quarter",
                "now handles half-cent rounding consistently",
                "was extended to cover the new jurisdiction",
                "produced a variance that turned out to be a duplicate entry",
            ],
        ),
        (
            "hiring",
            &[
                "the interview loop",
                "the take-home exercise",
                "the referral bonus",
                "the onboarding buddy",
                "the offer template",
                "the scorecard rubric",
            ],
            &[
                "was shortened after candidate feedback",
                "now includes a written debrief before the decision",
                "was standardised across the two teams",
                "moved to a structured rubric to reduce noise",
            ],
        ),
        (
            "biology",
            &[
                "the sample freezer",
                "the assay protocol",
                "the reagent batch",
                "the microscope stage",
                "the culture medium",
                "the centrifuge rotor",
            ],
            &[
                "was recalibrated after the temperature excursion",
                "produced inconsistent results across replicates",
                "was replaced ahead of the scheduled maintenance",
                "is now logged automatically instead of on paper",
            ],
        ),
        (
            "logistics",
            &[
                "the loading dock",
                "the route planner",
                "the pallet scanner",
                "the cold chain sensor",
                "the customs form",
                "the returns desk",
            ],
            &[
                "was re-sequenced to cut empty mileage",
                "flagged a temperature breach in transit",
                "cleared inspection without a manual override",
                "now reconciles counts against the manifest",
            ],
        ),
    ];
    const QUALIFIERS: &[&str] = &[
        "during the morning window",
        "over the weekend",
        "after the last deployment",
        "under peak load",
        "in the staging environment",
        "following the incident review",
        "once the backlog cleared",
        "before the quarterly freeze",
    ];
    const OUTCOMES: &[&str] = &[
        "No operator action was required.",
        "The owning team was notified and acknowledged.",
        "A follow-up task was filed for next sprint.",
        "The change was reverted and re-applied cleanly.",
        "Monitoring confirmed the metric returned to baseline.",
    ];
    // Small LCG — reproducible across runs so two sweeps are comparable.
    let mut seed: u64 = 0x5eed_1234;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as usize
    };
    (0..n)
        .map(|i| {
            let (topic, subjects, predicates) = TOPICS[next() % TOPICS.len()];
            let s = subjects[next() % subjects.len()];
            let pred = predicates[next() % predicates.len()];
            let q = QUALIFIERS[next() % QUALIFIERS.len()];
            let o = OUTCOMES[next() % OUTCOMES.len()];
            (
                format!("filler/{topic}/note-{i}.md"),
                format!("{s} {pred} {q}. {o} (record {i})"),
            )
        })
        .collect()
}

/// Apply the `ECPHORIA_*` overrides this harness understands (mirrors `locomo_eval::apply_env`,
/// kept to the knobs that matter for a document corpus).
fn apply_env(config: &mut CoreConfig) {
    let set = |dst: &mut String, key: &str| {
        if let Ok(v) = std::env::var(key) {
            *dst = v;
        }
    };
    set(
        &mut config.embedding.provider,
        "ECPHORIA_EMBEDDING__PROVIDER",
    );
    set(&mut config.embedding.model, "ECPHORIA_EMBEDDING__MODEL");
    set(
        &mut config.embedding.ollama_url,
        "ECPHORIA_EMBEDDING__OLLAMA_URL",
    );
    set(
        &mut config.embedding.openai_api_key,
        "ECPHORIA_EMBEDDING__OPENAI_API_KEY",
    );
    set(&mut config.rerank.provider, "ECPHORIA_RERANK__PROVIDER");
    set(&mut config.rerank.backend, "ECPHORIA_RERANK__BACKEND");
    set(&mut config.rerank.model, "ECPHORIA_RERANK__MODEL");
    let set_usize = |dst: &mut usize, key: &str| {
        if let Ok(v) = std::env::var(key) {
            if let Ok(n) = v.parse() {
                *dst = n;
            }
        }
    };
    set_usize(
        &mut config.embedding.dimension,
        "ECPHORIA_EMBEDDING__DIMENSION",
    );
    set_usize(
        &mut config.memory.cognition.retrieval_scan_cap,
        "ECPHORIA_COGNITION__RETRIEVAL_SCAN_CAP",
    );
    set_usize(
        &mut config.memory.cognition.retrieval_pool,
        "ECPHORIA_COGNITION__RETRIEVAL_POOL",
    );
    // Weighted-RRF arm weights. The vector arm's precision falls as the corpus grows while the
    // lexical arm's holds, so the balance that is right at 500 memories is not right at 200k —
    // this is the knob for that, and it needs measuring rather than assuming.
    let set_f32 = |dst: &mut f32, key: &str| {
        if let Ok(v) = std::env::var(key) {
            if let Ok(n) = v.parse() {
                *dst = n;
            }
        }
    };
    set_f32(
        &mut config.memory.cognition.retrieval_vector_weight,
        "ECPHORIA_COGNITION__RETRIEVAL_VECTOR_WEIGHT",
    );
    set_f32(
        &mut config.memory.cognition.retrieval_lexical_weight,
        "ECPHORIA_COGNITION__RETRIEVAL_LEXICAL_WEIGHT",
    );
    let flag = |key: &str| matches!(std::env::var(key).as_deref(), Ok("1") | Ok("true"));
    if std::env::var("ECPHORIA_COGNITION__GRAPH_EXPANSION").is_ok() {
        config.memory.cognition.graph_expansion = flag("ECPHORIA_COGNITION__GRAPH_EXPANSION");
    }
}

/// Per-question outcome.
struct Record {
    category: Category,
    /// 1-indexed rank of the first memory whose subject matches the gold document.
    rank: Option<usize>,
}

/// A miss, with what came back instead — the difference between "recall is 48%" and knowing why.
struct Miss {
    question: &'static str,
    gold: &'static str,
    got: Vec<String>,
}

fn report(label: &str, recs: &[&Record]) {
    if recs.is_empty() {
        return;
    }
    let n = recs.len() as f64;
    let recall_at = |k: usize| {
        recs.iter()
            .filter(|r| matches!(r.rank, Some(x) if x <= k))
            .count()
    };
    let mrr: f64 = recs
        .iter()
        .map(|r| r.rank.map_or(0.0, |x| 1.0 / x as f64))
        .sum::<f64>()
        / n;
    println!(
        "{label:<12} n={:<4} R@1={:>5.1}% R@3={:>5.1}% R@5={:>5.1}% R@10={:>5.1}% MRR={:.3}",
        recs.len(),
        100.0 * recall_at(1) as f64 / n,
        100.0 * recall_at(3) as f64 / n,
        100.0 * recall_at(5) as f64 / n,
        100.0 * recall_at(10) as f64 / n,
        mrr,
    );
}

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(run());
}

async fn run() {
    let mut config = CoreConfig::default();
    config.memory.episodic.db_path = ":memory:".into();
    config.memory.state.db_path = ":memory:".into();
    config.memory.cognition.db_path = ":memory:".into();
    apply_env(&mut config);
    let scan_cap = config.memory.cognition.retrieval_scan_cap;
    let engine = EcphoriaEngine::new(config).await.expect("engine");

    let root = std::env::var("KB_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            // The example runs from the workspace root under `cargo run`, but resolve it from the
            // manifest so it also works when invoked from elsewhere.
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(Path::parent)
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."))
        });
    let pad: usize = std::env::var("KB_PAD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let k: usize = std::env::var("KB_K")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let docs = collect_docs(&root);
    if docs.is_empty() {
        eprintln!("no markdown found under {} — set KB_ROOT", root.display());
        std::process::exit(2);
    }

    // One shared, tenant-level scope — the shape a team knowledge base actually takes (and the
    // one that avoids the exact-tuple scope trap, where a user-scoped write is invisible to a
    // tenant-scoped read).
    let scope = MemoryScope::tenant("default");

    // Real documents first, filler after, so the filler is *newer*. `list_active` orders by
    // `importance DESC, valid_from DESC`, so this buries the corpus exactly the way a growing
    // knowledge base buries its own history.
    //
    // Loaded through `memory_add_batch` — the bulk path a corpus import actually uses. Set
    // `KB_BULK=0` to compare against one-at-a-time `memory_add`.
    const BATCH: usize = 1_000;
    let bulk = !matches!(std::env::var("KB_BULK").as_deref(), Ok("0") | Ok("false"));
    // `KB_CHUNK=0` stores each document whole (one memory per file) — the shape before
    // structure-aware chunking, kept so the two can be measured against each other.
    let chunked = !matches!(std::env::var("KB_CHUNK").as_deref(), Ok("0") | Ok("false"));
    let ingest_start = std::time::Instant::now();
    let mut stored = 0usize;

    if chunked {
        let opts = ChunkOptions::default();
        for (path, body) in &docs {
            let r = engine
                .document_ingest(
                    DocumentIngest {
                        path,
                        content: body,
                        ..Default::default()
                    },
                    &scope,
                    &opts,
                )
                .await
                .expect("document ingest");
            stored += r.chunks;
        }
    }
    // Filler is always flat (it has no structure), as is the corpus when chunking is off.
    let flat: Vec<(String, String)> = if chunked {
        filler(pad)
    } else {
        docs.iter().cloned().chain(filler(pad)).collect()
    };
    stored += flat.len();
    if bulk {
        for chunk in flat.chunks(BATCH) {
            let inputs: Vec<MemoryInput> = chunk
                .iter()
                .map(|(path, body)| {
                    MemoryInput::new(scope.clone(), body.clone()).with_subject(path.clone())
                })
                .collect();
            engine.memory_add_batch(inputs).await.expect("bulk add");
        }
    } else {
        for (path, body) in &flat {
            let input = MemoryInput::new(scope.clone(), body.clone()).with_subject(path.clone());
            engine.memory_add(input).await.expect("add");
        }
    }
    let ingest_elapsed = ingest_start.elapsed();
    let total_docs = stored;

    let mut records: Vec<Record> = Vec::new();
    let mut query_ms: Vec<f64> = Vec::new();
    let mut misses: Vec<Miss> = Vec::new();
    // Characters a caller would have to paste into a prompt to use the top-5. Document-identity
    // recall says nothing about this, and it is the main thing chunking buys.
    let mut ctx_chars: Vec<f64> = Vec::new();
    // Top-hit vector similarity, split by whether the gold document was actually found. If the two
    // distributions overlap, no threshold can separate "answered" from "nothing here" and the
    // caller has no way to tell them apart.
    let mut sim_hit: Vec<f64> = Vec::new();
    let mut sim_miss: Vec<f64> = Vec::new();
    // (hits carrying a similarity, hits total) — if the vector arm rarely covers the top results,
    // a similarity threshold cannot be the primary guard.
    let mut sim_coverage: (usize, usize) = (0, 0);
    for question in GOLD {
        let start = std::time::Instant::now();
        let hits = engine
            .memory_search(question.q, &scope, k)
            .await
            .expect("search");
        query_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        ctx_chars.push(
            hits.iter()
                .take(5)
                .map(|h| h.memory.content.len())
                .sum::<usize>() as f64,
        );
        let gold = question.doc.to_lowercase();
        let rank = hits
            .iter()
            .position(|h| {
                h.memory
                    .subject
                    .as_deref()
                    .is_some_and(|s| s.to_lowercase().contains(&gold))
            })
            .map(|i| i + 1);

        // How usable is the vector similarity as a "does the corpus cover this" signal? Two things
        // decide that: how often a top result carries one at all, and whether the answered and
        // not-found distributions are separable. If they overlap, no threshold can tell them apart.
        sim_coverage.0 += hits
            .iter()
            .take(5)
            .filter(|h| h.similarity.is_some())
            .count();
        sim_coverage.1 += hits.iter().take(5).count();
        if let Some(best) = hits
            .iter()
            .take(5)
            .filter_map(|h| h.similarity)
            .fold(None, |a: Option<f32>, s| Some(a.map_or(s, |m| m.max(s))))
        {
            if matches!(rank, Some(r) if r <= 5) {
                sim_hit.push(best as f64);
            } else {
                sim_miss.push(best as f64);
            }
        }
        if rank.is_none() {
            misses.push(Miss {
                question: question.q,
                gold: question.doc,
                got: hits
                    .iter()
                    .take(3)
                    .map(|h| {
                        format!(
                            "{} ({:.4})",
                            h.memory.subject.as_deref().unwrap_or("<no subject>"),
                            h.score
                        )
                    })
                    .collect(),
            });
        }
        records.push(Record {
            category: question.category,
            rank,
        });
    }

    let pct_of = |v: &mut Vec<f64>, p: f64| {
        if v.is_empty() {
            return 0.0;
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[((p * v.len() as f64) as usize).min(v.len() - 1)]
    };

    println!("\n── KB retrieval eval ──────────────────────────────");
    println!(
        "corpus documents: {} ({})",
        docs.len(),
        if chunked {
            format!("chunked into {} sections", total_docs - pad)
        } else {
            "whole-file, unchunked".into()
        }
    );
    println!("filler memories:  {pad}");
    println!("total memories:   {total_docs}");
    println!("questions:        {}\n", records.len());

    report("OVERALL", &records.iter().collect::<Vec<_>>());
    for cat in [
        Category::Identifier,
        Category::Conceptual,
        Category::Temporal,
        Category::MultiDoc,
    ] {
        let subset: Vec<&Record> = records.iter().filter(|r| r.category == cat).collect();
        report(cat.label(), &subset);
    }

    println!(
        "\ningest  total:    {:.1} s for {} memories ({:.0} µs each, {})",
        ingest_elapsed.as_secs_f64(),
        total_docs,
        ingest_elapsed.as_secs_f64() * 1e6 / total_docs as f64,
        if bulk {
            format!("memory_add_batch, {BATCH}/batch")
        } else {
            "memory_add, one at a time".into()
        }
    );
    println!(
        "query   p50/p95:  {:.2} / {:.2} ms",
        pct_of(&mut query_ms, 0.50),
        pct_of(&mut query_ms, 0.95)
    );
    println!(
        "context top-5:    {:.0} chars median (what you would paste into a prompt)",
        pct_of(&mut ctx_chars, 0.50)
    );
    println!(
        "similarity coverage: {}/{} of top-5 hits carry one",
        sim_coverage.0, sim_coverage.1
    );
    // The comparison that decides whether a threshold is usable at all.
    let mut sim_absent: Vec<f64> = Vec::new();
    for q in OUT_OF_CORPUS {
        if let Ok(hits) = engine.memory_search(q, &scope, k).await {
            if let Some(best) = hits
                .iter()
                .take(5)
                .filter_map(|h| h.similarity)
                .fold(None, |a: Option<f32>, s| Some(a.map_or(s, |m| m.max(s))))
            {
                sim_absent.push(best as f64);
            }
        }
    }
    if !sim_hit.is_empty() || !sim_miss.is_empty() {
        let stat = |v: &mut Vec<f64>| {
            if v.is_empty() {
                return "n/a".to_string();
            }
            format!(
                "p10={:.3} p50={:.3} p90={:.3} (n={})",
                pct_of(v, 0.10),
                pct_of(v, 0.50),
                pct_of(v, 0.90),
                v.len()
            )
        };
        println!("best similarity, answered:     {}", stat(&mut sim_hit));
        println!("best similarity, not found:    {}", stat(&mut sim_miss));
        println!("best similarity, NOT IN CORPUS:{}", stat(&mut sim_absent));
    }
    let provider = engine.config().embedding.provider.as_str();
    println!(
        "mode:             {}",
        if !provider.is_empty() && provider != "none" {
            "hybrid (BM25 + vector)"
        } else {
            "lexical (BM25 only — set ECPHORIA_EMBEDDING__PROVIDER for hybrid)"
        }
    );
    println!("scan cap:         {scan_cap}");
    if docs.len() + pad > scan_cap {
        println!(
            "                  ⚠ corpus ({}) exceeds the candidate window — the lexical arm\n\
             \x20                 cannot see {} of its memories",
            docs.len() + pad,
            docs.len() + pad - scan_cap
        );
    }
    // Machine-checkable gate for CI.
    let recall5 = 100.0
        * records
            .iter()
            .filter(|r| matches!(r.rank, Some(x) if x <= 5))
            .count() as f64
        / records.len().max(1) as f64;
    if let Some(floor) = std::env::var("KB_MIN_RECALL5")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
    {
        if recall5 + 1e-9 < floor {
            eprintln!(
                "\nFAIL: recall@5 {recall5:.1}% is below the floor of {floor:.1}% — retrieval regressed"
            );
            std::process::exit(1);
        }
        println!("\ngate: recall@5 {recall5:.1}% >= floor {floor:.1}% — OK");
    }

    if !misses.is_empty() {
        println!("\nmisses ({}) — what came back instead:", misses.len());
        for m in &misses {
            println!("  ✗ {:?}  want *{}*", m.question, m.gold);
            for (i, got) in m.got.iter().enumerate() {
                println!("      {}. {got}", i + 1);
            }
        }
    }
}
