//! Criterion benchmarks for the Strata core engine.
//!
//! Run locally: cargo bench -p strata-core
//! CI runs these on every PR and posts a comparison comment.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use strata_core::{CoreConfig, StrataEngine};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn make_engine(rt: &tokio::runtime::Runtime) -> StrataEngine {
    rt.block_on(StrataEngine::new(CoreConfig::default()))
        .unwrap()
}

fn bench_ingest(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);

    let events: Vec<strata_core::memory::episodic::Event> = (0..100)
        .map(|i| strata_core::memory::episodic::Event {
            id: uuid::Uuid::new_v4(),
            source: "bench".into(),
            event_type: "test".into(),
            payload: serde_json::json!({"i": i, "data": "benchmark payload"}),
            timestamp: chrono::Utc::now(),
            parent_id: None,
            trace_id: None,
            tags: vec!["bench".into()],
            idempotency_key: None,
        })
        .collect();

    c.bench_function("ingest_100_events", |b| {
        b.iter(|| {
            rt.block_on(engine.ingest(black_box(events.clone())))
                .unwrap();
        });
    });
}

fn bench_query(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);

    // Seed data
    let events: Vec<strata_core::memory::episodic::Event> = (0..500)
        .map(|i| strata_core::memory::episodic::Event {
            id: uuid::Uuid::new_v4(),
            source: "bench".into(),
            event_type: "test".into(),
            payload: serde_json::json!({"i": i}),
            timestamp: chrono::Utc::now(),
            parent_id: None,
            trace_id: None,
            tags: vec![],
            idempotency_key: None,
        })
        .collect();
    rt.block_on(engine.ingest(events)).unwrap();

    c.bench_function("query_select_100", |b| {
        b.iter(|| {
            rt.block_on(engine.query_sql(black_box(
                "SELECT * FROM events ORDER BY timestamp DESC LIMIT 100",
            )))
            .unwrap();
        });
    });
}

fn bench_state(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);

    c.bench_function("state_set_get", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine
                    .state_set("agent-1", "key-1", serde_json::json!({"v": 1}))
                    .await
                    .unwrap();
                engine.state_get("agent-1", "key-1").await.unwrap();
            });
        });
    });
}

fn bench_semantic_search(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);

    // Seed vectors
    rt.block_on(async {
        for i in 0..200 {
            let vec = vec![i as f32 / 200.0; 768];
            let entry = strata_core::memory::semantic::SemanticEntry {
                id: uuid::Uuid::new_v4(),
                content: format!("entry {i}"),
                embedding: vec,
                metadata: serde_json::json!({}),
            };
            engine.semantic_upsert(&entry).await.unwrap();
        }
    });

    let query_vec = vec![0.5_f32; 768];

    c.bench_function("semantic_search_k10", |b| {
        b.iter(|| {
            rt.block_on(engine.semantic_search(black_box(&query_vec), 10))
                .unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_ingest,
    bench_query,
    bench_state,
    bench_semantic_search
);
criterion_main!(benches);
