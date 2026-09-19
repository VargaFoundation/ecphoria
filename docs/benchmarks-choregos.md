# The Choregos profile — service under a sustained mixed load

[`benchmarks-kb.md`](./benchmarks-kb.md) measures retrieval *quality*: does the right document come
back. This page measures *service*: what a request costs when 50 of them arrive every second against
a corpus the size of a real engineering estate, and what losing a node does to that.

The two questions are independent, and a system can pass one while failing the other.

Harness: [`crates/ecphoria-core/examples/choregos_bench.rs`](../crates/ecphoria-core/examples/choregos_bench.rs)
and [`failover_bench.rs`](../crates/ecphoria-core/examples/failover_bench.rs).

```bash
make bench-choregos      # 20k facts + 200k events, 50 r/s + 5 w/s for 60s
make bench-failover      # 3-node cluster, leader killed under load
```

## The profile

Taken from how an orchestrator actually uses the store, not from what is convenient to generate:

| | |
| :-- | :-- |
| **20 000 typed facts** | decisions, conventions, incidents, ticket summaries, run lessons, flaky tests, hotspots, findings — with the subject grammar and the `paths` metadata a context pack filters on, spread over 200 projects and 12 services |
| **200 000 episodic events** | the webhook firehose those facts were distilled from |
| **50 reads/s** | 70 % context-pack retrieval, 20 % subject history, 10 % analytical SQL — the shape of what an agent asks before a task |
| **5 writes/s** | a fact landing at the end of a run |

The load is **open-loop**: requests are issued on a schedule and never wait for the previous one to
finish, so queueing shows up as latency instead of quietly lowering the offered rate. A closed-loop
harness measures a system that is never overloaded, which is not the question.

The corpus is generated from a fixed seed, so two runs measure the same workload. Embeddings are
**off** — with a provider configured, most of the number is that provider's HTTP latency, and the
question here is what Ecphoria costs.

## Machine

A development workstation, not a server: 24 threads, 78 GB RAM, NVMe, WSL2 on Linux 6.18. Absolute
numbers will differ on your hardware; the *shape* — which operations are cheap, which knob moves
which number, what a failover costs — is what transfers.

## Results

### Loading the corpus

| | Rate |
| :-- | --: |
| 20 000 typed facts through the full cognition path (`memory_add_batch`, 2 000 per call) | **410/s** (48.7 s) |
| 200 000 events (`ingest`, 5 000 per call) | **56 566/s** (3.5 s) |
| RSS after loading | 210 MiB |

The two differ by two orders of magnitude, and that is the correct shape: an event is appended,
while a fact goes through subject-contradiction resolution, dedup, the lexical index and the
importance blend. Facts are the expensive, meaningful write; events are the cheap firehose.

### Under load — 50 reads/s + 5 writes/s, 60 s

| Operation | n | p50 | p95 | p99 | max |
| :-- | --: | --: | --: | --: | --: |
| `memory_search` (hybrid retrieval, k=20) | 2 054 | **249 ms** | **540 ms** | 614 ms | 740 ms |
| subject history | 618 | 5.2 ms | 20.1 ms | 32.2 ms | 50.2 ms |
| analytics SQL (`GROUP BY kind`) | 329 | 11.6 ms | 28.2 ms | 42.7 ms | 73.3 ms |
| `memory_add` | 301 | 117 ms | 416 ms | 468 ms | 546 ms |

Achieved 49.8 reads/s and 5.0 writes/s against targets of 50 and 5 — the offered load was served,
with **zero errors**. RSS 966 MiB.

**Read p95 misses the 300 ms target from the plan.** That is the finding, and it is the reason to
run a benchmark rather than assume: retrieval at this corpus size and rate costs about a quarter of
a second at the median and half a second at p95 on this machine. Everything else — subject lookups,
analytical SQL, even writes at their median — is comfortably inside budget.

### The same corpus, barely loaded

Five reads/s instead of fifty, one write/s, same 20 000 facts and 200 000 events:

| Operation | p50 | p95 |
| :-- | --: | --: |
| `memory_search` | **70 ms** | 74 ms |
| subject history | 2.5 ms | 2.9 ms |
| analytics SQL | 4.6 ms | 4.8 ms |
| `memory_add` | 11 ms | 16 ms |

So one search *costs* 70 ms at this corpus size, and at 35 searches/s it takes 250 ms. The
difference is queueing: the system is past its knee well below the target rate, not slow per
request. That distinction matters, because the fix for the first is a knob and the fix for the
second is a rewrite.

### The knob, measured both ways

`retrieval_scan_cap` is the candidate width per retrieval arm — how many active memories BM25
scans and how many vector neighbours are fetched. Default 2 048. At 512, same corpus, same
50 reads/s + 5 writes/s:

| | scan_cap 2048 | scan_cap 512 |
| :-- | --: | --: |
| `memory_search` p50 | 249 ms | **22 ms** |
| `memory_search` p95 | 540 ms | **24 ms** |
| `memory_add` p50 | 117 ms | 9.7 ms |
| RSS | 966 MiB | 566 MiB |

A 23× improvement at p95 is not a tuning nudge; it is the difference between comfortably inside the
budget and missing it. The obvious question is what it costs in recall, so here is that too —
`kb_eval` on the repository's own documentation, buried under 20 000 filler memories:

| | scan_cap 2048 | scan_cap 512 |
| :-- | --: | --: |
| Recall@1 | 50.0 % | 50.0 % |
| Recall@5 | 84.7 % | **86.1 %** |
| Recall@10 | 94.4 % | 94.4 % |
| MRR | 0.660 | 0.653 |

**At 20 000 memories the narrower window costs nothing measurable.** The differences are inside the
noise of a 72-question set, in both directions.

That is not a licence to lower it everywhere. The default is 2 048 because
[benchmarks-kb.md](./benchmarks-kb.md) measured a recall cliff as a corpus grows — at 200 000
memories a narrow window does start missing documents. What these two tables say together is
narrower: **for a corpus of this size, 2 048 is paying for recall it is not buying**, and the
default is sized for the larger case. Measure it on your corpus with `kb_eval` before changing it;
the harness exists precisely so this is an experiment rather than an opinion.

## Losing a node, under load

A three-node cluster on one host, 50 reads/s + 5 writes/s, leader killed with `SIGKILL` at t+25 s.
The harness drops the dead address from its rotation at the moment of the kill, because a real
deployment sits behind a Service whose readiness probe does the same within a few seconds — leaving
it in would fill the timeline with connection-refused noise and bury the thing being measured.

```
  t     reads ok/fail   mean     writes ok/fail   mean
  24      50/0        4.9ms        5/0       12.4ms
  25      49/1        5.7ms        0/5        8.8ms   ← leader killed
  26      50/0        5.0ms        0/5        1.8ms
  27      50/0        5.1ms        0/5        0.7ms
  28      50/0        4.9ms        0/5        2.5ms
  29      50/0        5.2ms        0/5        1.8ms
  30      50/0        5.2ms        2/3        8.1ms
  31      50/0        5.0ms        5/0       13.3ms
```

| | |
| :-- | --: |
| Reads over the whole run | **3 849 ok, 1 failed** |
| Writes | 357 ok, 28 failed |
| Write unavailability | **≈ 5 s** (t+25 → t+30) |

**Reads do not notice.** One request failed — the one in flight to the node being killed — and the
rate never dropped: followers hold a full replica and answer from it. **Writes stop for about five
seconds**, which is the Raft election, and come back on their own without anything being restarted
or reconfigured.

### Two things this measurement found

The first two runs of this drill did not look like that at all, and both causes were real defects
rather than harness noise.

**Writes failed two times out of three, all the time.** A follower answered a write with `307` and
`leader_id` in the body — and **no `Location` header**, because a node knows its peers' *Raft*
addresses and the REST API does not live there. No HTTP client can follow that, and neither could
Ecphoria's own SDKs. Behind a Service, that is (N-1)/N of writes failing for any ordinary client.
Followers now **proxy** the write to the leader and return its answer (`cluster.peer_http` maps node
id → HTTP base URL; the Helm chart fills it from the same headless DNS it builds `peers` from).

**Every search was treated as a write**, because the middleware classified by HTTP method and a
search is a `POST`. So retrieval was proxied to the leader — concentrating the fleet's read load on
one node — and, during an election, answered `503` by followers holding a perfectly good replica.
Reads are now classified by route: `/query`, `/search`, `/embed-and-search`, `/memories/search`,
`/context-pack` and `/attachments/search-image` are served locally. The row at t+26 above, 50 reads
served while there is no leader at all, is that fix.

This is the argument for running a benchmark rather than reasoning about one: both defects were in
`main`, both were invisible to the test suite (which never puts an HTTP client in front of a
three-node cluster), and both are the kind that show up in production as "it works in staging".

## What to do about read latency

Three levers, in the order worth trying:

1. **`memory.cognition.retrieval_scan_cap`** — measured above: 23× at p95, and at this corpus size
   no measurable recall cost. Measure it on *your* corpus with `kb_eval` before changing it; a
   knowledge base with rare vocabulary tolerates a narrower window than one where every document
   uses the same words, and a much larger corpus does not tolerate it at all.
2. **`memory.cognition.read_pool_size`** — how many searches may touch DuckDB at once (default 8).
   Raising it helps only if the machine has cores to spare and the queries are not already
   disk-bound.
3. **Shard** (`cluster.shards > 1`) — each shard is an independent Raft group over a disjoint slice
   of tenants, so read *and* write work divides. This is the answer when one tenant's corpus is not
   the problem but the sum of all of them is.

What does **not** help: adding replicas to one Raft group. Followers serve reads, so read capacity
does scale with replicas — but a single tenant's query still does the same work on whichever node
answers it.

## Honest caveats

- **One machine, one process.** The single-node numbers are an embedded engine, with no HTTP,
  serialization or TLS in the path. A real client adds a few milliseconds and its own queueing.
- **No embeddings.** With a provider configured, retrieval also pays an embedding round-trip per
  query (typically 10–50 ms for a local Ollama, more for a hosted API), and the vector arm adds
  work. The BM25-only numbers here are a floor, not a forecast.
- **A generated corpus.** The facts share a vocabulary of 24 domain words, which makes BM25 work
  harder than a real corpus of English prose would — closer to the pessimistic end than the
  optimistic one, which is the right direction for a benchmark to err.
- **60 seconds is short.** Long enough to see queueing, too short to see compaction, decay sweeps or
  index growth. Run it longer before trusting it for capacity planning.
- **The failover timeline is at 1 s resolution.** An outage shorter than a second shows as elevated
  latency in one bucket rather than as failures, which is why the per-second table is printed rather
  than just a summary.
