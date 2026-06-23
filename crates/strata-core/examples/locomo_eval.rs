//! LoCoMo-style memory retrieval evaluation harness.
//!
//! Measures the cognition layer the way the agent-memory market is benchmarked: ingest a
//! multi-session "conversation", then answer questions by retrieving memories and checking
//! whether the answer-bearing memory was recalled (recall@k), plus retrieval latency.
//!
//! Run it (synthetic dataset, offline):
//!   cargo run -p strata-core --example locomo_eval
//!
//! Run it on a REAL dataset with an embedding provider (true hybrid retrieval, closer to the
//! published LoCoMo setups):
//!   LOCOMO_PATH=examples/locomo-sample.json \
//!   STRATA_EMBEDDING__PROVIDER=ollama \
//!   cargo run -p strata-core --example locomo_eval
//!
//! Dataset schema (JSON): an array of conversations, each:
//!   { "user": "alice",
//!     "turns": ["...session text...", "..."],
//!     "qa": [ { "question": "...", "expected": "substring of the answer-bearing memory" } ] }
//! Convert a real LoCoMo export into this shape to reproduce leaderboard-style numbers.

use serde::Deserialize;
use strata_core::memory::cognition::MemoryScope;
use strata_core::{CoreConfig, StrataEngine};

#[derive(Deserialize)]
struct Qa {
    question: String,
    expected: String,
}

#[derive(Deserialize)]
struct Conversation {
    user: String,
    turns: Vec<String>,
    qa: Vec<Qa>,
}

fn embedded_dataset() -> Vec<Conversation> {
    let c = |user: &str, turns: &[&str], qa: &[(&str, &str)]| Conversation {
        user: user.into(),
        turns: turns.iter().map(|s| s.to_string()).collect(),
        qa: qa
            .iter()
            .map(|(q, e)| Qa {
                question: q.to_string(),
                expected: e.to_string(),
            })
            .collect(),
    };
    vec![
        c(
            "alice",
            &[
                "Alice mentioned she works as a data scientist at Acme Corp.",
                "Alice said her favorite programming language is Rust.",
                "Alice is planning a trip to Japan next spring.",
                "Alice has a golden retriever named Max.",
                "Alice recently moved from Berlin to Amsterdam.",
            ],
            &[
                ("What does Alice do for work?", "data scientist"),
                ("Where is Alice traveling next spring?", "Japan"),
                ("What is the name of Alice's dog?", "Max"),
                ("Which city does Alice live in now?", "Amsterdam"),
            ],
        ),
        c(
            "bob",
            &[
                "Bob is a high school chemistry teacher.",
                "Bob plays the saxophone in a jazz band on weekends.",
                "Bob is allergic to peanuts.",
                "Bob's daughter Mia just started college in Boston.",
            ],
            &[
                ("What instrument does Bob play?", "saxophone"),
                ("What is Bob allergic to?", "peanuts"),
                ("Where did Bob's daughter start college?", "Boston"),
            ],
        ),
    ]
}

fn load_dataset() -> Vec<Conversation> {
    if let Ok(path) = std::env::var("LOCOMO_PATH") {
        match std::fs::read_to_string(&path) {
            Ok(s) => match serde_json::from_str::<Vec<Conversation>>(&s) {
                Ok(d) => {
                    println!("loaded {} conversations from {path}", d.len());
                    return d;
                }
                Err(e) => eprintln!("failed to parse {path}: {e} — using synthetic dataset"),
            },
            Err(e) => eprintln!("failed to read {path}: {e} — using synthetic dataset"),
        }
    }
    embedded_dataset()
}

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(run());
}

async fn run() {
    // In-memory stores so the harness is self-contained.
    let mut config = CoreConfig::default();
    config.memory.episodic.db_path = ":memory:".into();
    config.memory.state.db_path = ":memory:".into();
    config.memory.cognition.db_path = ":memory:".into();
    let engine = StrataEngine::new(config).await.expect("engine");

    let dataset = load_dataset();
    const K: usize = 5;
    let mut total = 0usize;
    let mut hits = 0usize;
    let mut latencies_ms: Vec<f64> = Vec::new();

    for convo in &dataset {
        let scope = MemoryScope::user(&convo.user);
        for turn in &convo.turns {
            engine
                .memory_remember(turn, &scope)
                .await
                .expect("remember");
        }
        for qa in &convo.qa {
            let start = std::time::Instant::now();
            let results = engine
                .memory_search(&qa.question, &scope, K)
                .await
                .expect("search");
            latencies_ms.push(start.elapsed().as_secs_f64() * 1000.0);

            let needle = qa.expected.to_lowercase();
            let recalled = results
                .iter()
                .any(|h| h.memory.content.to_lowercase().contains(&needle));
            total += 1;
            if recalled {
                hits += 1;
            } else {
                println!("  MISS: q={:?} expected={:?}", qa.question, qa.expected);
            }
        }
    }

    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| {
        if latencies_ms.is_empty() {
            0.0
        } else {
            latencies_ms[((p * latencies_ms.len() as f64) as usize).min(latencies_ms.len() - 1)]
        }
    };

    println!("\n── LoCoMo-style eval ──────────────────────────────");
    println!("conversations: {}", dataset.len());
    println!("questions:     {total}");
    println!(
        "recall@{K}:      {hits}/{total} = {:.1}%",
        100.0 * hits as f64 / total.max(1) as f64
    );
    println!("latency p50:   {:.2} ms", pct(0.50));
    println!("latency p95:   {:.2} ms", pct(0.95));
    println!(
        "mode:          {}",
        if engine.semantic_count() > 0 {
            "hybrid (BM25 + vector)"
        } else {
            "lexical (BM25 only — set STRATA_EMBEDDING__PROVIDER for hybrid)"
        }
    );
}
