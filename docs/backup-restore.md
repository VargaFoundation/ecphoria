# Backup and restore

A backup nobody has restored is a hope, not a backup. This page is written to be *run*: the drill at
the end is the part that matters, and it takes about ten minutes.

## What a backup contains

One backup is a directory — locally under `<data_dir>/backups/<timestamp>/`, in object storage under
`<s3_prefix><timestamp>/` — holding all four stores plus a manifest:

| Artifact | Store |
| :-- | :-- |
| `episodic.duckdb` | events |
| `memories.duckdb` | the cognition layer (bi-temporal memories, graph edges, grants, attachments metadata) |
| `state.db` | agent state KV |
| `vectors/` | the USearch index |
| `runs.db` | the agent-run ledger (`ecphoria:full` only) |
| `manifest.json` | **the commit record** |

```jsonc
{
  "format_version": 1,
  "ecphoria_version": "0.1.0",
  "created_at": "2026-09-19T02:00:03Z",
  "counts": { "episodic_events": 184213, "memories": 20114, "semantic_vectors": 20114 },
  "artifacts": [
    { "path": "memories.duckdb", "sha256": "9f2c…" },
    { "path": "vectors",         "sha256": "31ab…" }
  ]
}
```

The manifest is written **last**, after every artifact. That ordering is the integrity property: a
run that dies halfway leaves objects behind but no manifest, so a backup without one is visibly
incomplete rather than quietly truncated. For an S3 backup the server then re-reads the manifest and
checks each artifact it names is in the bucket — a `put` that returned `Ok` and an object that is
actually there are two different claims, and the moment to find out is while the local copy still
exists.

A backup is **not** a point-in-time snapshot across stores: it is taken store by store on a live
server. For a strict point-in-time image, quiesce writes or snapshot the volume. In practice the
skew is seconds and the manifest's counts tell you what was captured.

## Taking one

```bash
# Local, on whichever node answers
curl -fsS -X POST -H "Authorization: Bearer $KEY" localhost:8432/api/v1/admin/backup

# …and ship it to object storage, verifying the manifest landed
curl -fsS -X POST -H "Authorization: Bearer $KEY" 'localhost:8432/api/v1/admin/backup?target=s3'
```

```jsonc
{ "prefix": "backups/20260919T020003Z", "files": 6, "bytes": 412336128,
  "manifest_key": "backups/20260919T020003Z/manifest.json", "artifacts": 5 }
```

On Kubernetes, schedule it with the chart's CronJob rather than the in-process timer:

```yaml
config:
  storage:
    s3: { bucket: ecphoria-backups, region: eu-west-1, existingSecret: ecphoria-s3 }
backup:
  cronjob:
    enabled: true
    schedule: "0 2 * * *"
    apiKeySecret: ecphoria-admin
```

A CronJob has a result, a history and a failed object to alert on; an in-process timer has a log
line. The job fails loudly when the response carries no `manifest_key`, so a backup that uploaded
nothing cannot go green. `backup.autoEnabled` remains for clusters with no CronJob controller.

In a **sharded** deployment each shard backs up under its own prefix (`backups/shard-0/`, …): one
bucket, one directory per Raft group, so a restore cannot mix two shards' stores.

Alert on absence, not only on failure — a CronJob that stopped being scheduled produces no failed
job at all:

```promql
time() - max(kube_job_status_completion_time{job_name=~"ecphoria-backup.*"}) > 129600  # 36h
```

## Restoring

Restore is **destructive** and **node-local**: it replaces that node's stores.

```bash
# 1. Stop writes. In a cluster, scale to zero — a restore under a live Raft group races replication.
kubectl scale statefulset/ecphoria --replicas=0

# 2. Fetch the backup and check it against its own manifest BEFORE restoring anything.
aws s3 sync s3://ecphoria-backups/backups/20260919T020003Z /restore/
python3 - <<'EOF'
import hashlib, json, pathlib
root = pathlib.Path("/restore")
m = json.loads((root / "manifest.json").read_text())
def digest(p: pathlib.Path) -> str:
    h = hashlib.sha256()
    if p.is_dir():
        for f in sorted(x for x in p.rglob("*") if x.is_file()):
            h.update(str(f.relative_to(p)).encode()); h.update(b"\0"); h.update(f.read_bytes())
    else:
        h.update(p.read_bytes())
    return h.hexdigest()
bad = [a["path"] for a in m["artifacts"] if digest(root / a["path"]) != a["sha256"]]
print("MISMATCH:", bad) if bad else print("manifest ok —", m["counts"])
EOF

# 3. Restore, one node, then bring it up alone.
curl -fsS -X POST -H "Authorization: Bearer $KEY" \
  -H 'content-type: application/json' \
  -d '{"path": "/restore"}' localhost:8432/api/v1/admin/restore

# 4. Check the counts match the manifest, then scale back up. The other nodes come back empty and
#    are refilled by Raft from the restored leader — do not restore each node separately.
kubectl scale statefulset/ecphoria --replicas=3
```

Two things go wrong here often enough to name:

- **Restoring every node from the same backup.** They then all claim the same Raft log state with
  different term histories. Restore one, let replication do the rest.
- **Restoring into a running cluster.** The node is rewritten underneath the Raft log and will
  disagree with its peers about what was committed. Quiesce first.

## The drill

Do this on a schedule — quarterly is a reasonable default — and treat a failure as an incident,
because it is one you get to have on a Tuesday instead of during an outage.

1. Take a backup on production (`?target=s3`).
2. Start a **fresh, empty** Ecphoria (a container is enough) pointed at an empty data dir.
3. Restore the production backup into it.
4. Compare: `SELECT COUNT(*) FROM memories` against the manifest's `counts.memories`, then run three
   real `memory_search` queries and confirm the answers look like production's.
5. Write down how long steps 2–4 took. That number is your actual RTO; the one in the runbook before
   you measured it was a guess.

The fourth step is the one people skip, and it is the one that catches the failure that matters: a
restore that produces a *readable* store with an unusable vector index looks fine at step 3 and
answers nothing at step 4.
