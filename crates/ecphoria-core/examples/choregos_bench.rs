//! The Choregos profile: does Ecphoria hold up as the memory behind an orchestrator?
//!
//! `kb_eval` measures retrieval *quality*; this measures *service* — latency under a sustained,
//! mixed load on a corpus the size of a real engineering estate. The shape is taken from how
//! Choregos actually uses the store:
//!
//! - **20 000 typed facts** — decisions, conventions, incidents, ticket summaries, run lessons,
//!   flaky tests, hotspots, findings — with the subject grammar and the `paths` metadata a context
//!   pack filters on, spread over a few hundred projects.
//! - **200 000 episodic events** — the webhook firehose those facts were distilled from.
//! - **50 reads/s and 5 writes/s**, sustained. Reads are what an agent asks before a task
//!   (a context-pack retrieval, a subject lookup, an analytical SQL question); writes are a fact
//!   landing at the end of a run.
//!
//! The load is **open-loop**: requests are issued on a schedule and never wait for the previous
//! one to finish, so queueing shows up as latency instead of quietly lowering the offered rate.
//! A closed-loop harness measures a system that is never overloaded, which is not the question.
//!
//! Run it:
//!   cargo run --release -p ecphoria-core --example choregos_bench
//!
//! Smaller, for a laptop or a smoke test:
//!   BENCH_FACTS=2000 BENCH_EVENTS=20000 BENCH_SECONDS=20 \
//!     cargo run --release -p ecphoria-core --example choregos_bench
//!
//! Env: `BENCH_FACTS` (20000), `BENCH_EVENTS` (200000), `BENCH_SECONDS` (60), `BENCH_READS_PER_SEC`
//! (50), `BENCH_WRITES_PER_SEC` (5), `BENCH_DIR` (a temp dir by default), `BENCH_P95_MS` (fail the
//! run if read p95 exceeds this — the CI gate), `BENCH_SCAN_CAP` and `BENCH_READ_POOL` (the two
//! knobs that move read latency — see docs/benchmarks-choregos.md).
//!
//! Embeddings are off by default: the point is to measure Ecphoria, and with a provider configured
//! the numbers are mostly that provider's HTTP latency. Set `ECPHORIA_EMBEDDING__PROVIDER` to
//! measure the hybrid path instead.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ecphoria_core::memory::cognition::{MemoryInput, MemoryScope};
use ecphoria_core::memory::episodic::Event;
use ecphoria_core::{CoreConfig, EcphoriaEngine};

const KINDS: [&str; 8] = [
    "decision",
    "convention",
    "incident",
    "ticket_summary",
    "run_lesson",
    "flaky_test",
    "hotspot",
    "finding",
];

const SERVICES: [&str; 12] = [
    "checkout-api",
    "billing",
    "inventory",
    "search",
    "notifications",
    "identity",
    "gateway",
    "reporting",
    "scheduler",
    "ingest",
    "recommendations",
    "payments",
];

const WORDS: [&str; 24] = [
    "retry",
    "budget",
    "timeout",
    "idempotent",
    "migration",
    "rollback",
    "canary",
    "quota",
    "deadlock",
    "throttle",
    "checksum",
    "partition",
    "backfill",
    "replica",
    "cursor",
    "schema",
    "tenant",
    "webhook",
    "latency",
    "cache",
    "index",
    "lease",
    "shard",
    "digest",
];

/// Deterministic pseudo-randomness: the corpus must be identical between runs, or two runs measure
/// two different workloads and the comparison is meaningless.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Resident set size in MiB, read from `/proc/self/statm` (Linux). `None` elsewhere.
fn rss_mib() -> Option<f64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: f64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096.0 / (1024.0 * 1024.0))
}

/// Latency samples for one operation kind.
#[derive(Default)]
struct Samples {
    micros: Vec<u64>,
    errors: usize,
}

impl Samples {
    fn percentile(&self, p: f64) -> f64 {
        if self.micros.is_empty() {
            return f64::NAN;
        }
        let mut sorted = self.micros.clone();
        sorted.sort_unstable();
        let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
        sorted[idx] as f64 / 1000.0
    }
    fn mean_ms(&self) -> f64 {
        if self.micros.is_empty() {
            return f64::NAN;
        }
        self.micros.iter().sum::<u64>() as f64 / self.micros.len() as f64 / 1000.0
    }
    fn report(&self, name: &str) {
        println!(
            "  {name:<22} n={:<7} p50={:>7.2}ms  p95={:>7.2}ms  p99={:>7.2}ms  max={:>8.2}ms  mean={:>7.2}ms  errors={}",
            self.micros.len(),
            self.percentile(0.50),
            self.percentile(0.95),
            self.percentile(0.99),
            self.percentile(1.0),
            self.mean_ms(),
            self.errors,
        );
    }
}

fn fact(rng: &mut Rng, i: usize) -> MemoryInput {
    let kind = *rng.pick(&KINDS);
    let service = *rng.pick(&SERVICES);
    let project = format!("svc-{}", rng.below(200));
    let slug = format!("{}-{}", rng.pick(&WORDS), i);
    let subject = match kind {
        "incident" => format!(
            "incident:{service}:2026-{:02}-{:02}",
            1 + rng.below(12),
            1 + rng.below(28)
        ),
        "ticket_summary" => format!("ticket:jira:proj-{i}"),
        "flaky_test" => format!(
            "flaky_test:tests/{service}/test_{}.py::test_{slug}",
            rng.below(40)
        ),
        "hotspot" => format!("hotspot:services/{service}/src/{}.rs", rng.pick(&WORDS)),
        "finding" => format!("finding:semgrep:{}.{}", rng.pick(&WORDS), rng.pick(&WORDS)),
        other => format!("{other}:{service}:{slug}"),
    };
    // Content in the register a real fact has: identifiers, a service, a couple of domain words.
    let content = format!(
        "{} in {service}: the {} path must {} before the {} is {} — see run {}",
        kind.replace('_', " "),
        rng.pick(&WORDS),
        rng.pick(&WORDS),
        rng.pick(&WORDS),
        rng.pick(&WORDS),
        rng.below(10_000),
    );
    let mut metadata = serde_json::json!({
        "kind": kind,
        "paths": [format!("services/{service}/**")],
        "provenance": {"source": "choregos", "run_id": format!("run-{}", rng.below(100_000))},
    });
    // The kind-specific required fields, so the corpus would pass `fact_validation = "strict"`.
    match kind {
        "incident" => {
            metadata["service"] = service.into();
            metadata["occurred_at"] = "2026-06-01T00:00:00Z".into();
        }
        "ticket_summary" => {
            metadata["tracker"] = "jira".into();
            metadata["key"] = format!("PROJ-{i}").into();
        }
        "run_lesson" => metadata["workflow"] = "release-train".into(),
        "flaky_test" => {
            metadata["test_path"] = format!("tests/{service}/test_x.py").into();
            metadata["test_name"] = format!("test_{slug}").into();
        }
        "hotspot" => metadata["path"] = format!("services/{service}/src/x.rs").into(),
        "finding" => {
            metadata["tool"] = "semgrep".into();
            metadata["rule"] = "python.lang.security".into();
        }
        _ => {}
    }
    let mut input = MemoryInput::new(MemoryScope::tenant("choregos"), content);
    input.subject = Some(subject);
    input.metadata = metadata;
    input.project = Some(project);
    input
}

fn event(rng: &mut Rng, i: usize) -> Event {
    let service = *rng.pick(&SERVICES);
    Event {
        id: uuid::Uuid::new_v4(),
        source: format!("github/{service}"),
        event_type: (*rng.pick(&[
            "push",
            "pull_request.opened",
            "pull_request.merged",
            "issues.closed",
            "check_run.completed",
        ]))
        .to_string(),
        payload: serde_json::json!({
            "n": i,
            "service": service,
            "ref": format!("refs/heads/{}", rng.pick(&WORDS)),
            "sha": format!("{:040x}", rng.next()),
            "summary": format!("{} {} in {service}", rng.pick(&WORDS), rng.pick(&WORDS)),
        }),
        timestamp: chrono::Utc::now(),
        parent_id: None,
        trace_id: None,
        tags: vec!["bench".into()],
        idempotency_key: None,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let facts = env_usize("BENCH_FACTS", 20_000);
    let events = env_usize("BENCH_EVENTS", 200_000);
    let seconds = env_usize("BENCH_SECONDS", 60);
    let reads_per_sec = env_usize("BENCH_READS_PER_SEC", 50);
    let writes_per_sec = env_usize("BENCH_WRITES_PER_SEC", 5);

    let tmp = tempfile::tempdir()?;
    let dir = std::env::var("BENCH_DIR").unwrap_or_else(|_| tmp.path().to_string_lossy().into());
    std::fs::create_dir_all(&dir)?;

    let mut config = CoreConfig::default();
    config.storage.data_dir = dir.clone();
    config.memory.episodic.db_path = format!("{dir}/episodic.duckdb");
    config.memory.cognition.db_path = format!("{dir}/memories.duckdb");
    config.memory.state.db_path = format!("{dir}/state.db");
    config.memory.semantic.index_dir = format!("{dir}/vectors");
    config.runtime.db_path = format!("{dir}/runs.db");
    // The two knobs that actually move read latency, exposed so the trade-off can be measured
    // rather than guessed. `retrieval_scan_cap` is the candidate width per arm (recall vs work);
    // `read_pool_size` is how many searches can touch DuckDB at once.
    config.memory.cognition.retrieval_scan_cap =
        env_usize("BENCH_SCAN_CAP", config.memory.cognition.retrieval_scan_cap);
    config.memory.cognition.read_pool_size =
        env_usize("BENCH_READ_POOL", config.memory.cognition.read_pool_size);

    println!("Choregos profile — {facts} facts, {events} events, {reads_per_sec} r/s + {writes_per_sec} w/s for {seconds}s");
    println!("data dir: {dir}");
    if config.embedding.provider != "none" && std::env::var("ECPHORIA_EMBEDDING__PROVIDER").is_err()
    {
        config.embedding.provider = "none".into();
    }
    println!(
        "embedding: {}   scan_cap: {}   read_pool: {}",
        config.embedding.provider,
        config.memory.cognition.retrieval_scan_cap,
        config.memory.cognition.read_pool_size
    );
    println!();

    let engine = Arc::new(EcphoriaEngine::new(config).await?);

    // ── Corpus ───────────────────────────────────────────────────────────────────
    let mut rng = Rng(0x5EED_1234_ABCD_0001);
    let t0 = Instant::now();
    let mut written = 0usize;
    while written < facts {
        let batch: Vec<MemoryInput> = (0..2_000.min(facts - written))
            .map(|k| fact(&mut rng, written + k))
            .collect();
        written += batch.len();
        engine.memory_add_batch(batch).await?;
        print!("\r  facts:  {written}/{facts}");
        use std::io::Write;
        std::io::stdout().flush().ok();
    }
    let facts_elapsed = t0.elapsed();
    println!(
        "\r  facts:  {written} in {:.1}s ({:.0}/s)",
        facts_elapsed.as_secs_f64(),
        written as f64 / facts_elapsed.as_secs_f64()
    );

    let t1 = Instant::now();
    let mut ingested = 0usize;
    while ingested < events {
        let batch: Vec<Event> = (0..5_000.min(events - ingested))
            .map(|k| event(&mut rng, ingested + k))
            .collect();
        ingested += batch.len();
        engine.ingest(batch).await?;
        print!("\r  events: {ingested}/{events}");
        use std::io::Write;
        std::io::stdout().flush().ok();
    }
    let events_elapsed = t1.elapsed();
    println!(
        "\r  events: {ingested} in {:.1}s ({:.0}/s)",
        events_elapsed.as_secs_f64(),
        ingested as f64 / events_elapsed.as_secs_f64()
    );
    if let Some(rss) = rss_mib() {
        println!("  rss after load: {rss:.0} MiB");
    }
    println!();

    // ── Sustained mixed load ─────────────────────────────────────────────────────
    let scope = MemoryScope::tenant("choregos");
    let search = Arc::new(tokio::sync::Mutex::new(Samples::default()));
    let subject_lookup = Arc::new(tokio::sync::Mutex::new(Samples::default()));
    let analytics = Arc::new(tokio::sync::Mutex::new(Samples::default()));
    let writes = Arc::new(tokio::sync::Mutex::new(Samples::default()));
    let issued = Arc::new(AtomicUsize::new(0));

    let deadline = Instant::now() + Duration::from_secs(seconds as u64);
    let read_period = Duration::from_micros(1_000_000 / reads_per_sec.max(1) as u64);
    let write_period = Duration::from_micros(1_000_000 / writes_per_sec.max(1) as u64);

    let reader = {
        let (engine, scope, search, subject_lookup, analytics, issued) = (
            engine.clone(),
            scope.clone(),
            search.clone(),
            subject_lookup.clone(),
            analytics.clone(),
            issued.clone(),
        );
        tokio::spawn(async move {
            if reads_per_sec == 0 {
                return;
            }
            let mut rng = Rng(0xBEEF_0000_0000_0001);
            let mut ticker = tokio::time::interval(read_period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let mut tasks = Vec::new();
            while Instant::now() < deadline {
                ticker.tick().await;
                issued.fetch_add(1, Ordering::Relaxed);
                // The three read shapes an orchestrator actually issues, in the proportion it
                // issues them: mostly retrieval, some subject lookups, the occasional report.
                let roll = rng.below(100);
                let query = format!(
                    "{} {} in {}",
                    rng.pick(&WORDS),
                    rng.pick(&WORDS),
                    rng.pick(&SERVICES)
                );
                let subject = format!("ticket:jira:proj-{}", rng.below(facts.max(1)));
                let (engine, scope) = (engine.clone(), scope.clone());
                let (search, subject_lookup, analytics) =
                    (search.clone(), subject_lookup.clone(), analytics.clone());
                tasks.push(tokio::spawn(async move {
                    if roll < 70 {
                        let started = Instant::now();
                        let r = engine
                            .memory_search_filtered(&query, &scope, 20, None, None)
                            .await;
                        let mut s = search.lock().await;
                        s.micros.push(started.elapsed().as_micros() as u64);
                        if r.is_err() {
                            s.errors += 1;
                        }
                    } else if roll < 90 {
                        let started = Instant::now();
                        let r = engine.memory_history(&scope, &subject).await;
                        let mut s = subject_lookup.lock().await;
                        s.micros.push(started.elapsed().as_micros() as u64);
                        if r.is_err() {
                            s.errors += 1;
                        }
                    } else {
                        let started = Instant::now();
                        let r = engine
                            .query_sql(
                                "SELECT kind, COUNT(*)::VARCHAR AS n FROM memories \
                                 WHERE state = 'active' GROUP BY kind ORDER BY 2 DESC",
                            )
                            .await;
                        let mut s = analytics.lock().await;
                        s.micros.push(started.elapsed().as_micros() as u64);
                        if r.is_err() {
                            s.errors += 1;
                        }
                    }
                }));
            }
            for t in tasks {
                let _ = t.await;
            }
        })
    };

    // `BENCH_WRITES_PER_SEC=0` means *no writer*, not "one per second": the period arithmetic
    // clamps to 1/s, so a baseline run was quietly measuring reads next to a write every second.
    let writer = {
        let (engine, writes) = (engine.clone(), writes.clone());
        tokio::spawn(async move {
            if writes_per_sec == 0 {
                return;
            }
            let mut rng = Rng(0xFACE_0000_0000_0001);
            let mut ticker = tokio::time::interval(write_period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let mut n = 1_000_000usize;
            let mut tasks = Vec::new();
            while Instant::now() < deadline {
                ticker.tick().await;
                n += 1;
                let input = fact(&mut rng, n);
                let (engine, writes) = (engine.clone(), writes.clone());
                tasks.push(tokio::spawn(async move {
                    let started = Instant::now();
                    let r = engine.memory_add(input).await;
                    let mut s = writes.lock().await;
                    s.micros.push(started.elapsed().as_micros() as u64);
                    if r.is_err() {
                        s.errors += 1;
                    }
                }));
            }
            for t in tasks {
                let _ = t.await;
            }
        })
    };

    let load_started = Instant::now();
    let _ = tokio::join!(reader, writer);
    let load_elapsed = load_started.elapsed();

    // ── Report ───────────────────────────────────────────────────────────────────
    let search = search.lock().await;
    let subject_lookup = subject_lookup.lock().await;
    let analytics = analytics.lock().await;
    let writes = writes.lock().await;

    println!("Latency over {:.0}s:", load_elapsed.as_secs_f64());
    search.report("memory_search");
    subject_lookup.report("subject history");
    analytics.report("analytics SQL");
    writes.report("memory_add");

    let reads_done = search.micros.len() + subject_lookup.micros.len() + analytics.micros.len();
    println!();
    println!(
        "Achieved: {:.1} reads/s (target {reads_per_sec}), {:.1} writes/s (target {writes_per_sec})",
        reads_done as f64 / load_elapsed.as_secs_f64(),
        writes.micros.len() as f64 / load_elapsed.as_secs_f64(),
    );
    println!(
        "Corpus:   {} memories, {} events",
        engine.memory_count().await.unwrap_or(0),
        engine.event_count().await.unwrap_or(0)
    );
    if let Some(rss) = rss_mib() {
        println!("RSS:      {rss:.0} MiB");
    }

    let errors = search.errors + subject_lookup.errors + analytics.errors + writes.errors;
    if errors > 0 {
        eprintln!("\n{errors} operation(s) failed");
        std::process::exit(1);
    }

    // CI gate: a retrieval regression should fail the build, not ship quietly. Deliberately set
    // above the measured baseline rather than at it — this is an alarm, not a target.
    if let Ok(limit) = std::env::var("BENCH_P95_MS") {
        let limit: f64 = limit.parse().unwrap_or(f64::MAX);
        let p95 = search.percentile(0.95);
        if p95 > limit {
            eprintln!("\nread p95 {p95:.1}ms exceeds BENCH_P95_MS={limit:.1}ms");
            std::process::exit(1);
        }
        println!("\nread p95 {p95:.1}ms within BENCH_P95_MS={limit:.1}ms");
    }
    Ok(())
}
