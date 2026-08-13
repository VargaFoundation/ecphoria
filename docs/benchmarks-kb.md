# Knowledge-base retrieval — evaluation and baseline

Companion to [`benchmarks-locomo.md`](benchmarks-locomo.md). LoCoMo measures *conversational*
memory: short personal facts, everyday vocabulary, a few hundred memories per user. An engineering
knowledge base is a different workload — long structured documents, rare and precise vocabulary
(`ECPHORIA_STORAGE__DATA_DIR`, `openraft`, `ADR-002`), and a corpus that grows to six figures.
Numbers from one do not transfer to the other, so this page measures the second directly.

Harness: [`crates/ecphoria-core/examples/kb_eval.rs`](../crates/ecphoria-core/examples/kb_eval.rs).

## Reproduce

Fully offline — no dataset download, no API key. The corpus is **this repository's own
documentation** (`docs/`, `docs/adr/`, the `CLAUDE.md` files, SDK and example READMEs, 49 files)
and the 72-question gold set is hand-written against it and version-controlled in the harness.

```bash
cargo run --release -p ecphoria-core --example kb_eval          # 49 documents
KB_PAD=200000 cargo run --release -p ecphoria-core --example kb_eval   # buried under 200k memories
```

`KB_PAD=N` injects N synthetic technical memories into the same scope **after** the real
documents, so the corpus sits below newer filler in `importance DESC, valid_from DESC` order. That
is the real failure mode of a knowledge base that keeps growing: it buries its own history. The
filler shares the corpus's domain vocabulary and register, so it genuinely competes for BM25 rather
than being trivially separable.

Questions are graded by **document identity**, not answer substring: a hit means the memory whose
`subject` is the gold document appeared in the top-k. That keeps the metric stable when
documentation is reworded. Four categories: `identifier` (env var / config key / port lookups),
`conceptual` ("why is it built this way" — what ADRs exist to answer), `temporal` (dates, status,
what changed), `multi-doc` (right answer among several plausible near-neighbours).

## The candidate-window ceiling, and its fix

Before this work, the lexical arm scored BM25 in Rust over `list_active(scope,
retrieval_scan_cap)` — the top *N* active memories by importance then recency. That is not a
latency knob but a **hard recall ceiling**: past `retrieval_scan_cap` (default 2048), everything
below the cutoff is invisible to keyword search however well it matches.

It does not degrade gracefully. It stops:

| corpus | R@1 | R@3 | R@5 | MRR | query p50 |
|---|---|---|---|---|---|
| 49 documents | 62.5% | 80.6% | 93.1% | 0.739 | 57 ms |
| + 2 000 filler | 40.3% | 72.2% | 83.3% | 0.584 | 133 ms |
| + 5 000 filler | **0.0%** | **0.0%** | **0.0%** | **0.000** | 89 ms |

At 5 049 memories the knowledge base returns nothing correct at all — the 49 real documents have
fallen out of the window entirely. (Query latency *drops*, which confirms the mechanism: it is
scanning 2 048 short filler notes instead of 2 048 long documents.)

The fix is a two-stage lexical arm ([`memory/lexical.rs`](../crates/ecphoria-core/src/memory/lexical.rs)):

1. **Candidate generation** — a SQLite FTS5 inverted index over the whole scope. No new
   dependency: `rusqlite` was already in the tree for the state store, and its bundled SQLite is
   compiled with `-DSQLITE_ENABLE_FTS5` unconditionally.
2. **Scoring** — the existing in-Rust `lexical_rank` ranks those candidates.

Both halves are load-bearing. FTS5 alone removes the ceiling but ranks this corpus a few points
worse (its tokenizer counts stop words toward document length, penalising prose against terse
reference docs). `lexical_rank` alone ranks well but cannot see past its window. Measured
separately at 49 / 5 049 documents: FTS5-only 81.9% / 76.4% R@5, two-stage **88.9% / 86.1%**.

## Bulk ingest

Retrieval was the first ceiling; write throughput was the next. `memory_add` costs ~8 ms per
memory regardless of corpus size, so loading 200k took ~30 minutes. Profiling it against this
harness (release build, 20 050 memories, no embedding provider):

| path | µs / memory | 20k total | R@5 | MRR |
|---|---|---|---|---|
| `memory_add`, one at a time | 8 323 | 167 s | 83.3% | 0.708 |
| `memory_add_batch`, transaction only | 7 161 | 144 s | 83.3% | 0.708 |
| **`memory_add_batch` + DuckDB Appender** | **1 730** | **34.7 s** | **83.3%** | **0.708** |

**4.8× faster, with retrieval metrics unchanged** — the batch path changes I/O shape, not
cognition. 200k now loads in ~6 minutes instead of ~30.

The interesting result is the middle row: wrapping the writes in one transaction bought only 14%.
Batching is not the lever — the **Appender** is. A DuckDB row `INSERT` costs ~4.3 ms against
~59 µs appended (`docs/benchmarks.md`), because DuckDB is columnar and per-row inserts are its
documented weak spot (ADR-001 lists exactly this under Negative consequences). `upsert_raw_batch`
therefore checks which ids already exist, appends the new ones, and keeps `INSERT OR REPLACE` only
for rows that genuinely replace one — the same trade the episodic ingest path already makes.

Two correctness properties are pinned by tests, because both are easy to break here:

- **In-batch contradiction resolution.** Buffering rows means memory *n*'s contradiction check
  would not see memories *0..n-1*, silently turning a `Superseded` into a second `Inserted`. The
  buffer is flushed whenever an incoming subject collides with a pending one, so the deterministic
  guarantee holds exactly as it does one-at-a-time. (`memory_add_batch_matches_sequential_semantics`
  runs the same inputs both ways and compares outcomes *and* the resulting corpus.)
- **Derived indexes are maintained.** The batch write path is separate code, so it could persist
  rows while forgetting the lexical/vector indexes — invisible until someone searched.
  (`bulk_ingested_memories_are_immediately_searchable`.)

Known limitation: *semantic* (vector) dedup still cannot see within a batch, since the vector index
updates at apply time. Two subject-less near-duplicates in one batch are both stored. Contradiction
resolution by subject is the deterministic guarantee and is preserved; vector dedup is best-effort
consolidation and is not.

Exposed as `POST /api/v1/memories/batch` (max 10 000 per request). In cluster mode it falls back to
per-memory Raft replication — correct, but without the speedup.

The index is **advisory**. It returns candidate ids; the engine re-reads them from DuckDB filtered
to `state = 'active'` and the exact scope tuple. A stale entry therefore costs one candidate slot
and can never surface a deleted, superseded or out-of-scope memory. DuckDB stays the single source
of truth — deleting the index file costs a rebuild, never data.

### After

Release build, same 72 questions, corpus grown 4 000× :

| total memories | R@1 | R@3 | R@5 | R@10 | MRR | query p50/p95 |
|---|---|---|---|---|---|---|
| 50 | 61.1% | 80.6% | 88.9% | 97.2% | 0.735 | 11 / 13 ms |
| 50 049 | 59.7% | 77.8% | 83.3% | 91.7% | 0.706 | 19 / 118 ms |
| **200 050** | **61.1%** | **77.8%** | **83.3%** | **91.7%** | **0.710** | **27 / 264 ms** |

**Recall is flat from 50k to 200k** (83.3% → 83.3%, MRR 0.706 → 0.710) — that is the property
being bought, and it was the exit criterion. Latency grows sub-linearly: 4 000× the corpus for
2.4× the median query. Every one of these rows was 0.0% before the change.

Per category at 200 050 memories:

| category | R@1 | R@5 | MRR |
|---|---|---|---|
| identifier | 85.0% | **100.0%** | 0.910 |
| conceptual | 66.7% | 87.5% | 0.752 |
| multi-doc | 44.4% | 72.2% | 0.560 |
| temporal | 30.0% | 60.0% | 0.481 |

Identifier lookups are perfect at every size measured, and actually *sharpen* with scale (MRR
0.885 at 50 documents → 0.910 at 200k) because IDF discriminates better in a larger corpus.
Temporal is the weak category — dates and "what changed when" are answered by document *content*
that BM25 has no special handle on; this is what bi-temporal `valid_from` filtering and the vector
arm are for, neither of which is exercised here.

The p95 of 264 ms is the tail where a broad query matches many candidates and re-scores 2 048 of
them in Rust. That is `retrieval_scan_cap` doing its (new) job as a work bound; lower it to trade
recall for tail latency.

Intermediate sizes (debug build, so compare shapes not milliseconds): 2 049 memories → 86.1% R@5 /
0.728 MRR; 5 049 → 86.1% / 0.727. The before-table above is also a debug build.

### Honest caveats

- **R@5 at 49 documents regressed 93.1% → 88.9%** (three questions). Below the old window the
  previous implementation ranked slightly better; R@1, R@3 and R@10 are equal or better, and MRR
  is within noise (0.739 → 0.735). The trade buys 86.1% instead of 0.0% at 5k. It is a real
  regression at a corpus size that was never the problem.
- **These are BM25-only numbers.** `embedding.provider` defaults to `none`, so the vector arm is
  inactive. Configure a provider for hybrid; the gold set deliberately includes questions sharing
  no vocabulary with their target document, which is what the vector arm is for.
- **72 questions is a small set.** Single-question swings are 1.4 points. Treat differences under
  ~3 points as noise.
- **The corpus is this repo's own docs**, written by the same people as the system under test.
  Expect it to flatter vocabulary the authors find natural.
- The gold set encodes one judgement of the "right" document per question. A few questions have a
  defensible second answer (e.g. "how do I use this from Python" matches `bindings/python/` as
  well as `sdk/`), which caps achievable recall slightly below 100%.

## Document chunking

Stored whole, a document is one memory: one vector, one BM25 document, one retrieval unit. A hit
therefore returns the *entire file*. `document_ingest` splits Markdown on its heading hierarchy
instead ([`ingest/chunk.rs`](../crates/ecphoria-core/src/ingest/chunk.rs)), one memory per section.
Same 50-document corpus, 480 sections:

| | whole-file | **chunked** |
|---|---|---|
| R@1 | 59.7% | 55.6% |
| R@5 | 87.5% | **90.3%** |
| R@10 | 97.2% | 93.1% |
| MRR | 0.726 | 0.703 |
| query p50 | 50 ms | **24 ms** |
| **context for top-5** | **44 384 chars** | **4 042 chars** |

The last row is the point, and it is the one the recall metric cannot see. Answering a single
question by pasting the top-5 results into a prompt costs ~11 000 tokens unchunked and ~1 000
chunked — an **11× reduction**. That is the difference between a retrieval layer an agent can use
in a loop and one it cannot.

Document-level recall is roughly a wash (R@5 +2.8, R@1 −4.1, R@10 −4.1): with ~10 sections per
document competing for the same top-k slots, a wrong document's section can displace a right one.
Note also that the gold set grades *document* identity, so it gives no credit for returning the
right **section** — the improvement it does show is despite the metric, not because of it.
Temporal questions gain most (R@5 70% → 90%), which fits: dates and status live in a specific
section that no longer has to out-compete the rest of its file.

### Sections are addressed, not offset

Each chunk's subject is its heading trail — `docs/deployment.md#Kubernetes > Production Values` —
not a character range. That address survives edits elsewhere in the file, which is what lets
re-import route through the existing deterministic contradiction path:

| the section is… | outcome |
|---|---|
| unchanged | `Confirmed` — importance bumped, no new row |
| edited | `Superseded` — previous text stays queryable via history / `as_of` |
| new | `Inserted` |
| deleted from the file | expired by the document sweep |

So an evolving runbook keeps history *per section*, and "what did this say about failover in March"
is answerable. No new cognition was required — only a stable subject key. Three tests pin it:
`reimporting_an_edited_document_supersedes_only_the_changed_section` (an unchanged re-import is a
no-op; one edit moves exactly one section), `removing_a_section_expires_it_without_losing_history`
(the sweep, which supersession cannot do because a deleted section produces no memory to supersede
it), and `chunked_sections_are_retrievable_individually`.

Deliberately absent: any rule merging short sections into neighbours. Size-based merging makes a
chunk's address depend on its neighbours' lengths, so editing one paragraph would silently
re-address unrelated sections — and the address is what history is keyed on. A small chunk costs
one embedding; an unstable address costs the feature. Code fences are never split, and every chunk
carries its heading trail as a prefix so a section is findable by its ancestors' words.

`valid_from` is currently ingest time. Backdating it to the source commit date — so the bi-temporal
axis reflects when the *documentation* changed rather than when it was imported — belongs with the
git connector and is not done yet.

## Hybrid retrieval, and the arm-weight cliff

Everything above is BM25-only (`embedding.provider = "none"`, the default). With an embedding
provider configured the vector arm joins the fusion. Measured on the same 72 questions, Ollama
`nomic-embed-text` (768-d), chunked corpus:

| corpus | BM25-only R@5 / MRR | hybrid R@5 / MRR |
|---|---|---|
| 501 sections | 90.3% / 0.690 | **100.0% / 0.788** |
| + 5 000 filler | 88.9% / 0.677 | 90.3% / 0.668 |
| + 20 000 filler | 87.5% / 0.678 | 79.2% / 0.497 |

Hybrid is decisively better on a small corpus — **100% recall@5 in every category** — and then
degrades past it, crossing under BM25-only somewhere between 5k and 20k. BM25-only, by contrast,
is flat (90.3 → 88.9 → 87.5).

The cause is the fusion, not the vectors. Weighted RRF gives both arms weight `1.0` by default;
the vector arm contributes a fixed ~50 nearest neighbours whose precision falls as the corpus
grows, so at scale it is injecting noise with the same authority as the lexical arm.
`retrieval_vector_weight` exists for exactly this and had never been measured:

| corpus | w = 1.0 (default) | **w = 0.5** | w = 0.25 |
|---|---|---|---|
| 501 | **100.0%** / 0.781 | 98.6% / 0.778 | — |
| 5 501 | 90.3% / 0.668 | **97.2% / 0.752** | — |
| 20 501 | 79.2% / 0.497 | **93.1% / 0.723** | 91.7% / 0.729 |

**`retrieval_vector_weight = 0.5` is the recommendation for a knowledge base.** It costs 1.4
points at toy scale and gains 7 points at 5k and 14 points at 20k, and it beats BM25-only at every
size. It is not the shipped default because the default also governs the conversational workload,
which has not been measured with this knob — see `docs/knowledge-base.md` for the config.

Cost: hybrid queries are ~115 ms against ~8 ms for BM25-only, almost entirely the query-embedding
round-trip.

> **Methodology note.** The first version of the filler drew from a single ops vocabulary. That
> competed on BM25 terms as intended, but collapsed into a handful of tight clusters in embedding
> space, so any query landed near some filler and hybrid looked far worse than it is (68.1% rather
> than 79.2% at 20k). The filler now spans six unrelated topic areas. Padding has to be diverse on
> *both* axes or it measures the padding rather than the retriever.

### Graph expansion

Re-measured here because the LoCoMo verdict (−5 pts) was taken on a different workload and the
`chunk_of` links that chunking creates are far more precise than the token-overlap matching that
regression came from. On this corpus it is **neutral**: R@5 identical at 100%, MRR 0.788 → 0.781.
No reason to enable it, but the earlier verdict does not transfer — it was not re-earned here.

## Tokenization

`cognition::tokenize` splits on every non-alphanumeric character, which shreds exactly the terms an
engineering corpus is searched by: `ECPHORIA_STORAGE__DATA_DIR` becomes four common words,
`ecphoria-core` becomes two, `PROJ-1234` becomes `proj` plus a bare number. A query for the env var
then matches any document mentioning "data" or "storage".

The FTS5 index uses `unicode61` with `tokenchars '-_.'`, so env vars, crate names, ticket keys and
dotted config paths survive as single terms — identifier recall@5 is 100% at every corpus size
measured. `porter` stems on top, applied to documents and queries alike (so it can only merge
terms, never desynchronise the two sides); measured worth ~1.5 points at 5k and costing ~1.5 at 49
documents, i.e. roughly neutral, kept for the scale case.

## Retrieval width

With FTS5 pre-filtering by relevance, `retrieval_scan_cap` no longer bounds *what is considered* —
only how many ranked candidates are carried into scoring. Measured at 5 049 memories:

| `retrieval_scan_cap` | R@5 | MRR | query p50 |
|---|---|---|---|
| 128 | 81.9% | 0.690 | 45 ms |
| 512 | 83.3% | 0.696 | 51 ms |
| **2048** (default) | **86.1%** | **0.727** | 56 ms |

Wider is still better, so the default stands. (Debug build; latency inflated ~5×.)


## Can a threshold tell "no answer" from "a weak answer"?

An agent consuming search results needs to know when the corpus simply does not cover a question.
The fused `score` cannot tell it: that score is Reciprocal Rank Fusion, so it encodes *position*,
not match strength — the top hit scores about the same whether it answered the question or merely
shared a word. Driving a live Claude Code session made the consequence concrete: five weakly-related
documents came back at 0.028 against a real answer's 0.033.

Every hit now carries the per-arm signals — `similarity` (vector cosine, comparable across queries)
and `lexical` (BM25, comparable only within one query). The question is whether a floor on
`similarity` separates the two cases. Measured on the reference corpus, comparing the 72 gold
questions against 12 plausible engineering questions this repository genuinely does not discuss:

| best top-5 similarity | p10 | p50 | p90 | n |
|---|---|---|---|---|
| question **is** answered | 0.630 | 0.703 | 0.785 | 70 |
| question **not in corpus** | 0.563 | 0.601 | 0.661 | 12 |

**They overlap.** A floor at 0.63 keeps ~90% of real answers but still admits about a quarter of the
unanswerable ones; a floor at 0.66 cuts most of the noise and loses real answers with it. There is
no clean cut.

So `min_similarity` ships **opt-in with no default**. Shipping a default would state a confidence
the measurement does not support — the kind of silent, plausible-looking failure this whole page
exists to avoid. It is a coarse floor for callers who want one, and the tool description says as
much so an agent does not over-trust it.

What does work is the caller reading the results. In the live session, asked a question with no
answer in the corpus, the agent said so plainly, explained *why* by citing what the corpus does
contain, and correctly identified a `platform/docs/runbook.md` hit as an illustrative path from a
code sample rather than a real runbook. Judgement over threshold.

Similarity is available on 355 of 360 top-5 hits with an embedding provider configured, and on none
without one — where there is no absolute relevance signal at all.

## Regression gate

`kb_eval` runs in CI (`.github/workflows/ci.yml`, job `retrieval`) against this repository's own
documentation — offline, BM25-only, no dataset download — and fails the build when overall recall@5
drops below `KB_MIN_RECALL5`. The floor is 85%, deliberately under the measured BM25-only baseline
of 90.3%: an alarm for regressions, not a target to optimise against.

The unit tests pin specific properties (recall survives corpus growth, a document sweep stays within
its document, identifiers outrank prose). None of them measured *overall* recall, so a ranking
regression would have shipped silently.

## Regression guards

Three tests in `engine/tests.rs` pin the behaviour this page measures, so the ceiling cannot come
back silently:

- `memory_search_recall_survives_corpus_growth` — a memory written first, then buried under 10× the
  candidate window, must stay rank 1.
- `superseded_memories_never_resurface_from_the_index` — the advisory-index contract.
- `exact_identifier_lookup_beats_prose_that_merely_shares_words` — the tokenizer fix, end to end.
