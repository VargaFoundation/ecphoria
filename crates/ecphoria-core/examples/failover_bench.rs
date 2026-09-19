//! What losing a node costs, measured rather than asserted.
//!
//! `ops/cluster-local/failover-test.sh` proves a run *survives* a leader kill. It does not say what
//! the clients saw while it happened, and that is the number an operator actually needs: how long
//! writes fail, how long reads keep working, and how long until latency is normal again.
//!
//! So: drive a steady read/write load at a live cluster, kill the leader partway through, and print
//! a per-second timeline around the event.
//!
//! Run it against a cluster from `ops/cluster-local/run-cluster.sh`:
//!   cargo run --release -p ecphoria-core --example failover_bench
//!
//! Env:
//!   `NODES`        comma-separated base URLs (default the three local-cluster ports)
//!   `RUN_DIR`      where the cluster's `node-<i>.pid` files are (default /tmp/ecphoria-cluster)
//!   `SECONDS`      total run length (default 60)
//!   `KILL_AFTER`   seconds before killing the leader (default 20); 0 = never kill (baseline)
//!   `READS_PER_SEC` / `WRITES_PER_SEC` (default 50 / 5)
//!   `API_KEY`      bearer token, if the cluster has auth on
//!
//! Reads go to any node — a follower serves them locally, which is the point of follower reads.
//! Writes are sent to any node too and follow the 307 the followers issue, so the harness measures
//! what a client that simply talks to the Service sees, not what a client that tracks leadership
//! sees. Those are different numbers and the first one is the honest one.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// One second of the timeline, per operation kind.
#[derive(Default)]
struct Bucket {
    ok: AtomicU64,
    failed: AtomicU64,
    micros_total: AtomicU64,
    micros_max: AtomicU64,
}

impl Bucket {
    fn record(&self, ok: bool, micros: u64) {
        if ok {
            self.ok.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
        self.micros_total.fetch_add(micros, Ordering::Relaxed);
        self.micros_max.fetch_max(micros, Ordering::Relaxed);
    }
    fn total(&self) -> u64 {
        self.ok.load(Ordering::Relaxed) + self.failed.load(Ordering::Relaxed)
    }
    fn mean_ms(&self) -> f64 {
        let n = self.total();
        if n == 0 {
            return 0.0;
        }
        self.micros_total.load(Ordering::Relaxed) as f64 / n as f64 / 1000.0
    }
    fn max_ms(&self) -> f64 {
        self.micros_max.load(Ordering::Relaxed) as f64 / 1000.0
    }
}

struct Timeline {
    reads: Vec<Bucket>,
    writes: Vec<Bucket>,
}

impl Timeline {
    fn new(seconds: usize) -> Self {
        Self {
            reads: (0..seconds + 2).map(|_| Bucket::default()).collect(),
            writes: (0..seconds + 2).map(|_| Bucket::default()).collect(),
        }
    }
}

/// Ask each node who the leader is, and note which node id lives at which URL.
///
/// Both halves matter: `current_leader` is a node **id**, and the harness has to turn that into the
/// leader's address to remove it from the rotation after killing it. Removing the URL of whichever
/// node happened to answer the status query — which is not the same node — silently takes a live
/// node out and leaves the dead one in, and then half the run's "failures" are the harness's.
async fn survey(client: &Client, nodes: &[String]) -> (Option<u64>, HashMap<u64, String>) {
    let mut leader = None;
    let mut by_id = HashMap::new();
    for node in nodes {
        let Ok(resp) = client
            .get(format!("{node}/cluster/status"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
        else {
            continue;
        };
        let Ok(body) = resp.json::<serde_json::Value>().await else {
            continue;
        };
        if let Some(id) = body.get("node_id").and_then(|v| v.as_u64()) {
            by_id.insert(id, node.clone());
        }
        if leader.is_none() {
            leader = body.get("current_leader").and_then(|v| v.as_u64());
        }
    }
    (leader, by_id)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nodes: Vec<String> = std::env::var("NODES")
        .unwrap_or_else(|_| {
            "http://127.0.0.1:18001,http://127.0.0.1:18002,http://127.0.0.1:18003".into()
        })
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let run_dir = std::env::var("RUN_DIR").unwrap_or_else(|_| "/tmp/ecphoria-cluster".into());
    let seconds = env_usize("SECONDS", 60);
    let kill_after = env_usize("KILL_AFTER", 20);
    let reads_per_sec = env_usize("READS_PER_SEC", 50);
    let writes_per_sec = env_usize("WRITES_PER_SEC", 5);
    let api_key = std::env::var("API_KEY").ok();

    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    let (leader, addresses) = survey(&client, &nodes).await;
    let Some(leader_id) = leader else {
        eprintln!("no leader — is the cluster up? (ops/cluster-local/run-cluster.sh)");
        std::process::exit(1);
    };
    println!(
        "cluster: {} nodes, leader = node {leader_id}{}",
        nodes.len(),
        addresses
            .get(&leader_id)
            .map(|u| format!(" ({u})"))
            .unwrap_or_default()
    );
    println!("load:    {reads_per_sec} reads/s + {writes_per_sec} writes/s for {seconds}s");
    if kill_after > 0 {
        println!("kill:    the leader, at t+{kill_after}s");
    } else {
        println!("kill:    none (baseline run)");
    }
    println!();

    // The nodes the load rotates over. When the leader is killed its address is removed, because
    // a real deployment sits behind a Service whose readiness probe stops routing to a dead pod
    // within a few seconds. Leaving it in would fill the timeline with connection-refused errors
    // for the rest of the run and bury the signal we are here for — how long the *cluster* takes to
    // recover, rather than how long DNS takes to notice.
    let live: Arc<tokio::sync::RwLock<Vec<String>>> =
        Arc::new(tokio::sync::RwLock::new(nodes.clone()));
    let timeline = Arc::new(Timeline::new(seconds));
    let started = Instant::now();
    let bucket_of = move |t: Instant| -> usize {
        (t.duration_since(started).as_secs() as usize).min(seconds + 1)
    };

    let mut tasks = Vec::new();

    // Reads.
    for i in 0..reads_per_sec {
        let (client, live, timeline, api_key) = (
            client.clone(),
            live.clone(),
            timeline.clone(),
            api_key.clone(),
        );
        tasks.push(tokio::spawn(async move {
            // Stagger starts so the load is spread through the second rather than a spike on it.
            tokio::time::sleep(Duration::from_micros(
                (1_000_000 / reads_per_sec.max(1) * i) as u64,
            ))
            .await;
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let mut n = 0usize;
            while started.elapsed().as_secs() < seconds as u64 {
                ticker.tick().await;
                n += 1;
                let node = {
                    let live = live.read().await;
                    if live.is_empty() {
                        break;
                    }
                    live[(i + n) % live.len()].clone()
                };
                let at = Instant::now();
                let mut req = client.post(format!("{node}/api/v1/memories/search")).json(
                    &serde_json::json!({
                        "query": "retry budget timeout",
                        "k": 10,
                        "tenant_id": "bench"
                    }),
                );
                if let Some(ref key) = api_key {
                    req = req.bearer_auth(key);
                }
                let ok = matches!(req.send().await, Ok(r) if r.status().is_success());
                timeline.reads[bucket_of(at)].record(ok, at.elapsed().as_micros() as u64);
            }
        }));
    }

    // Writes.
    for i in 0..writes_per_sec {
        let (client, live, timeline, api_key) = (
            client.clone(),
            live.clone(),
            timeline.clone(),
            api_key.clone(),
        );
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_micros(
                (1_000_000 / writes_per_sec.max(1) * i) as u64,
            ))
            .await;
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let mut n = 0usize;
            while started.elapsed().as_secs() < seconds as u64 {
                ticker.tick().await;
                n += 1;
                let node = {
                    let live = live.read().await;
                    if live.is_empty() {
                        break;
                    }
                    live[(i + n) % live.len()].clone()
                };
                let at = Instant::now();
                let mut req = client.post(format!("{node}/api/v1/memories")).json(
                    &serde_json::json!({
                        "tenant_id": "bench",
                        "subject": format!("decision:bench:w{i}-{n}"),
                        "content": format!("write {i}-{n} during the failover drill"),
                        "metadata": {"kind": "decision", "provenance": {"source": "failover_bench"}}
                    }),
                );
                if let Some(ref key) = api_key {
                    req = req.bearer_auth(key);
                }
                let ok = matches!(req.send().await, Ok(r) if r.status().is_success());
                timeline.writes[bucket_of(at)].record(ok, at.elapsed().as_micros() as u64);
            }
        }));
    }

    // The kill, on the same clock as the samples.
    let killed_at = if kill_after > 0 {
        let client = client.clone();
        let nodes = nodes.clone();
        let live = live.clone();
        let run_dir = run_dir.clone();
        Some(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(kill_after as u64)).await;
            let (leader, addresses) = survey(&client, &nodes).await;
            let Some(id) = leader else {
                eprintln!("no leader to kill at t+{kill_after}s");
                return None;
            };
            // The leader's OWN address, not the address of whoever answered the survey.
            let url = match addresses.get(&id) {
                Some(u) => u.clone(),
                None => {
                    eprintln!("leader {id} did not report its address — not killing blind");
                    return None;
                }
            };
            let pid_file = format!("{run_dir}/node-{id}.pid");
            match std::fs::read_to_string(&pid_file) {
                Ok(pid) => {
                    let pid = pid.trim();
                    println!("t+{kill_after}s  killing leader node {id} ({url}, pid {pid})");
                    let _ = std::process::Command::new("kill")
                        .arg("-9")
                        .arg(pid)
                        .status();
                    live.write().await.retain(|n| n != &url);
                    Some(kill_after)
                }
                Err(e) => {
                    eprintln!("cannot read {pid_file}: {e}");
                    None
                }
            }
        }))
    } else {
        None
    };

    for t in tasks {
        let _ = t.await;
    }
    let killed = match killed_at {
        Some(h) => h.await.ok().flatten(),
        None => None,
    };

    // ── Timeline ─────────────────────────────────────────────────────────────────
    println!();
    println!("  t     reads ok/fail   mean    max      writes ok/fail   mean    max");
    let mut first_write_failure: Option<usize> = None;
    let mut last_write_failure: Option<usize> = None;
    for t in 0..seconds {
        let r = &timeline.reads[t];
        let w = &timeline.writes[t];
        if r.total() == 0 && w.total() == 0 {
            continue;
        }
        if w.failed.load(Ordering::Relaxed) > 0 {
            first_write_failure.get_or_insert(t);
            last_write_failure = Some(t);
        }
        let marker = if Some(t) == killed {
            " ← leader killed"
        } else {
            ""
        };
        println!(
            "  {t:<4}  {:>4}/{:<4}  {:>6.1}ms {:>7.1}ms    {:>4}/{:<4}  {:>6.1}ms {:>7.1}ms{marker}",
            r.ok.load(Ordering::Relaxed),
            r.failed.load(Ordering::Relaxed),
            r.mean_ms(),
            r.max_ms(),
            w.ok.load(Ordering::Relaxed),
            w.failed.load(Ordering::Relaxed),
            w.mean_ms(),
            w.max_ms(),
        );
    }

    // ── Summary ──────────────────────────────────────────────────────────────────
    let sum = |bs: &[Bucket], f: fn(&Bucket) -> u64| bs.iter().map(f).sum::<u64>();
    let reads_ok = sum(&timeline.reads, |b| b.ok.load(Ordering::Relaxed));
    let reads_failed = sum(&timeline.reads, |b| b.failed.load(Ordering::Relaxed));
    let writes_ok = sum(&timeline.writes, |b| b.ok.load(Ordering::Relaxed));
    let writes_failed = sum(&timeline.writes, |b| b.failed.load(Ordering::Relaxed));

    println!();
    println!("reads : {reads_ok} ok, {reads_failed} failed");
    println!("writes: {writes_ok} ok, {writes_failed} failed");
    if let (Some(first), Some(last)) = (first_write_failure, last_write_failure) {
        println!(
            "write unavailability: t+{first}s … t+{last}s ({}s)",
            last - first + 1
        );
    } else if killed.is_some() {
        println!("write unavailability: none observed at 1s resolution");
    }
    if reads_failed == 0 && killed.is_some() {
        println!("reads were served throughout — followers answer locally, so losing the leader is a write-path event");
    }
    Ok(())
}
