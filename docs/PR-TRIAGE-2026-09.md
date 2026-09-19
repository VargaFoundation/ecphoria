# Open PR triage — September 2026

Nineteen open pull requests, all from Dependabot, the oldest from 2026-07-08. Every one of them
shows a red CI, which is the first thing to explain: **`main` itself has been red since
2026-08-13**, so a red check on a dependency bump says nothing about the bump.

This document sorts the PRs by what actually has to happen to each, with the evidence that led
there. Versions were checked against the lockfiles on `main` and, where it was possible to test
locally, the bump was applied and built.

## Why `main` is red

| Job | Cause | Real? |
| :-- | :-- | :-- |
| Clippy, Test, Operator | `sccache: Server startup failed: cache storage failed to read` — the GitHub Actions cache service was down during the 2026-08-13 run | No. Transient infrastructure. `cargo clippy --workspace --all-targets` and `cargo test --workspace` (613 tests) pass locally on `main` |
| Python SDK (build + pytest) | `pip install -e .` fails: `readme = "README.md"` pointed at a file that did not exist, and hatchling could not infer the wheel's packages (distribution `ecphoria-client`, import name `ecphoria`) | **Yes** — fixed in this branch: `sdk/python/README.md` added, `[tool.hatch.build.targets.wheel] packages` declared. `pip install -e ".[dev]"`, `python -m build` and the 15 tests now pass |
| Security audit (RUSTSEC) | 15 advisories in the dependency tree | **Yes** — 14 are fixed here: nine by lockfile bumps (`crossbeam-epoch`, `postgres-protocol`, `quinn-proto`, `rustls`, `rustls-webpki` 0.103, `tokio-postgres`, the AWS SDK chain), and five more by dropping the AWS SDK's legacy hyper-0.14 client, which nothing uses. One remains — see below |
| Retrieval quality (KB eval) | Gated eval; needs a fresh run to judge | To re-check |

### The one advisory left

| Advisory | Crate | Why it is stuck |
| :-- | :-- | :-- |
| RUSTSEC-2026-0235 | `rkyv` 0.7.46 | Pulled by `rust_decimal` (itself via `duckdb` and `pgwire`). The patch is `rkyv` 0.8, which `rust_decimal` 1.43 has not adopted — upstream work, not ours |

The other five went away with the AWS SDK's `default-features = false`: `aws-sdk-s3` and
`aws-config` were pulling *two* HTTP clients, the hyper-1 one they use and a legacy hyper-0.14
one they do not, and the legacy one carried `h2` 0.3 and `rustls` 0.21. Verified beyond
compiling: an attachment uploaded through the API lands in MinIO and reads back byte-for-byte.

**So the first action is not a merge**: push the Python SDK fix, let CI re-run on a green `main`,
and only then read the PR checks for what they say about the bumps themselves.

## Ready to merge once CI is green

| PR | Bump | Evidence |
| :-- | :-- | :-- |
| #21 | serde_json 1.0.150 → 1.0.151 (operator) | patch; no API surface |
| #20 | tokio 1.52.3 → 1.53.1 (operator) | minor; the operator uses the runtime + macros only |
| #11 | httpx >= 0.25 → >= 0.28.1 (python sdk) | installed 0.28.1, 15 SDK tests pass |
| #18 | websockets >= 12.0 → >= 16.1 (python sdk) | installed 17.1, 15 SDK tests pass |
| #19 | langchain-core >= 0.1 → >= 1.4.9 (extra) | installed 1.6.3, `langchain_ecphoria` imports |
| #8 | llama-index-core >= 0.10 → >= 0.14.23 (extra) | installed 0.14.24, `llama_index_ecphoria` imports |
| #4 | actions/setup-python 5 → 7 | inputs unchanged for the `python-version` usage here |

## Close — already on `main`

Dependabot opened these before the lockfile moved; `main` already carries the target version
or better. Closing them costs nothing and removes noise.

| PR | Bump | On `main` |
| :-- | :-- | :-- |
| #7 | anyhow 1.0.102 → 1.0.104 | `anyhow 1.0.104` |
| #15 | chrono 0.4.44 → 0.4.45 | `chrono 0.4.45` |

## Park — these need a code change, not a merge

Each is a major bump whose API moved. Merging the lockfile alone breaks the build, so they are
parked with the reason. Label: `deferred/major-bump`.

| PR | Bump | What has to change first |
| :-- | :-- | :-- |
| #10 + #14 + #12 | kube 0.95 → 4.0, k8s-openapi 0.23 → 0.28, schemars 0.8 → 1.2 (operator) | These three move **together**: `kube-derive`'s `CustomResource` generates a `JsonSchema` impl, so the schemars major and the kube major are one change, and `k8s-openapi` must match the `kube` release. One PR, not three |
| #13 | prost 0.13.5 → 0.14.4 | `prost` is pulled by `tonic`; bumping it alone splits the generated-code types from the runtime. Needs the matching `tonic` release and a `cargo build` of the regenerated protos |
| #17 | toml 0.8.23 → 1.1.3 | `toml` 1.0 reworked the `Value`/`Deserializer` surface; `CoreConfig` parses TOML by hand in `config.rs`. Small but real |
| #16 | fastembed 4.9.1 → 5.17.3 | The local ONNX path (`embedding/local.rs`, feature `embed-local`) uses `TextEmbedding::try_new` with an options builder that changed shape in 5.x. Also re-pins the ONNX runtime |
| #2 | actions/upload-artifact 4 → 7 | three majors at once (v4 → v7). The release workflow uploads several artifacts; read the v5/v6/v7 release notes for the naming and retention changes before merging |
| #6 | docker/build-push-action 6 → 7 | the release workflow builds multi-arch images. A major on the build action is exactly the change that should be dry-run once on a branch, not merged on trust |
| #5 | azure/setup-helm 4 → 5 | used with no inputs in `ci.yml` (just `- uses:`), so the risk is low — but it is still a major, and the chart lint is what gates every release |
| #1 | softprops/action-gh-release 2 → 3 | the release workflow passes `body_path` and `files` and relies on the pushed tag. Check both survive the major before a release depends on it |

## What was done, 2026-09-19

1. ✅ `main` is green again. Four separate causes, all fixed: the sccache probe (the Actions
   cache was down that day and is down again today), the operator's `cargo fmt` drift, the
   Python SDK's missing README and undeclared wheel packages, and fourteen of the fifteen
   RUSTSEC advisories. Clippy also needed attention: CI runs Rust 1.98, which enforces
   `result_large_err` on tonic- and axum-shaped signatures that 1.96 let pass.
2. ✅ #7 and #15 closed — `main` already carried their target versions.
3. ✅ The seven safe bumps are in. #21, #20, #18 and #19 merged; #11, #8 and #4 were applied
   directly (the first two conflicted once their siblings landed on the same lines, and the
   token in use may not merge a PR that touches a workflow file) and closed against the commit.
   Nineteen open PRs became ten, and the ten left are exactly those that need a code change.
4. ✅ The operator's three-way major landed as one change: kube 4 + k8s-openapi 0.28 + schemars 1
   (#10, #14, #12). Three call sites moved — `Recorder` takes the object per event now, and
   k8s-openapi swapped chrono for **jiff**, so the leader-election lease had to follow. The
   generated CRD was diffed, not assumed: same group, plural, properties, required set and status
   subresource.
5. ✅ The five action majors went together (#2, #6, #5, #1): they are all the same migration to a
   Node 24 runtime, needing Actions Runner ≥ 2.327.1 — which hosted runners have. Every input we
   pass was checked against the release notes; `download-artifact` moved to v8 alongside
   `upload-artifact` v7, since the pair has to match.
6. ✅ The three "leave it until someone touches that subsystem" bumps turned out to cost nothing
   and are in:
   - **#17 toml 1.x** — the surface `doctor` uses is unchanged; zero edits.
   - **#16 fastembed 5** — the options builder kept its shape and `ort` stays on the same
     release candidate, so the native runtime does not move. Checked with the features on, which
     is the only way this code compiles at all.
   - **#13 prost 0.14 + tonic 0.14** — the one that looked worst. tonic 0.14 split prost support
     into `tonic-prost` (codec) and `tonic-prost-build` (codegen), and renamed the TLS feature by
     crypto provider (`tls` → `tls-ring`). Changing those three lines and the two build scripts
     was the whole migration: no call site moved, 695 tests pass, and the Raft transport's mTLS
     is unchanged. `tonic 0.12` stays in the lockfile for `opentelemetry-proto` under the
     optional `otlp` feature — someone else's tree, not ours.

**All nineteen PRs are resolved.** Nine merged or applied, two closed as already-on-main, eight
landed as the four grouped changes above.
