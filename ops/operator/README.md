# Strata sharding operator

A Kubernetes controller (kube-rs) that reconciles a `StrataShardPlan` custom resource: it keeps the
number of shard StatefulSets equal to `spec.shards` and, after scaling, drives per-tenant data moves
so each tenant lives on its consistent-hash-owning shard (by calling each shard's
`POST /api/v1/admin/rebalance`).

## Status (read this)

- Kept **outside the Cargo workspace** (its own empty `[workspace]` table) — heavy k8s deps, and its
  runtime can only be exercised against a real cluster.
- **What IS verified:** this crate **compiles** (kube 0.95 / k8s-openapi 0.23), `clippy` is clean, and
  the decision logic (`reconcile_moves`) has a passing unit test mirroring the workspace's unit-tested
  `strata_cluster::{reconcile_plan, scale_plan}` (on main).
- **What IS now implemented (live apply loop):** scale-**up** creates the new shard StatefulSets by
  cloning `<release>-shard-0`'s spec and setting each one's `STRATA_CLUSTER__SHARD_INDEX`
  (server-side apply); scale-**down** deletes the drained shard StatefulSets; tenant **rebalance
  moves** are driven via each shard's `POST /api/v1/admin/rebalance`. The order is safe — up:
  create-then-move; down: **drain-then-delete** (never lose data).
- **What still needs a cluster:** the actual runtime behavior of the control loop (watches, patches,
  pod rollout) — run it against kind/k3d/minikube or a real cluster to exercise end-to-end.

## Build / run

```bash
cd ops/operator
cargo build --release
# In-cluster (uses the pod's ServiceAccount) or with a local kubeconfig:
./target/release/strata-operator
```

Apply the CRD + a plan (sketch):

```yaml
apiVersion: strata.io/v1
kind: StrataShardPlan
metadata:
  name: prod
  annotations:
    strata.io/tenants: "tenant-a,tenant-b,tenant-c"   # or have the operator discover via SQL
spec:
  shards: 4
  release: strata
  shardBaseUrls:
    - http://strata-shard-0-headless:8432
    - http://strata-shard-1-headless:8432
    - http://strata-shard-2-headless:8432
    - http://strata-shard-3-headless:8432
  adminToken: "<bearer>"   # use a Secret ref in a production build
```

## Remaining work for production

- Render + server-side-apply the shard StatefulSets/Services on scale-up (reuse the Helm template),
  and delete drained shards on scale-down.
- Discover tenants from the cluster (`SELECT DISTINCT tenant_id`) instead of an annotation.
- Read `adminToken` from a Secret; add RBAC + leader election.
