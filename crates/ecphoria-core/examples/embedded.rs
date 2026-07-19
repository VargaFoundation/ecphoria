//! Embedded, no-server usage of Ecphoria — "the SQLite of agent memory."
//!
//! Run with:  cargo run -p ecphoria-core --example embedded
//!
//! No server, no network, no config: open a memory, remember facts, recall them. Uses an in-memory
//! store here; swap `open_in_memory()` for `open("./agent-memory")` to persist to disk.

use ecphoria_core::embedded::Ecphoria;

#[tokio::main]
async fn main() -> ecphoria_core::Result<()> {
    // In-process memory (nothing to deploy). Use `Ecphoria::open("./agent-memory")` to persist.
    let mem = Ecphoria::open_in_memory().await?;

    // Teach the agent some facts about a user.
    mem.remember("alice", "Alice is a backend engineer who prefers Rust")
        .await?;
    mem.add("alice", "Alice's timezone is CET").await?;
    mem.add("alice", "Alice works on the platform team").await?;

    // Note: contradiction resolution (a newer fact superseding an older one) keys on a stable
    // `subject`; use `mem.engine().memory_add(MemoryInput { subject: Some(...), .. })` to opt in.

    // Recall (hybrid BM25 + vector; BM25 alone works with no embedding provider).
    println!("Recall for \"what does alice work on\":");
    for hit in mem.recall("alice", "what does alice work on", 3).await? {
        println!("  • {}", hit.memory.content);
    }

    println!("\nAll of alice's active memories:");
    for m in mem.all("alice", 10).await? {
        println!("  • {}", m.content);
    }

    Ok(())
}
