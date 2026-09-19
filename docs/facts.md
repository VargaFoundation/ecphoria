# Typed facts

A memory store that accepts anything becomes a place where nothing can be found. Ecphoria does not
lock the store down — `metadata` stays yours, and an untyped memory is still a first-class memory —
but the handful of facts a team actually reasons about get a **shape** and a **key**.

| | |
| :-- | :-- |
| **Shape** | a JSON Schema per kind, in [`crates/ecphoria-core/schemas/facts/`](../crates/ecphoria-core/schemas/facts/), validated on the write path |
| **Key** | a `subject` grammar per kind, which is what makes supersession work |
| **Switch** | per tenant, off by default: `off` → `warn` → `strict` |

## Why a subject grammar

Supersession in Ecphoria is deterministic and keyed on `(scope, project, subject)`: a newer memory
with the same subject and different content replaces the older one, which keeps its history and
stays answerable as-of any past instant. That mechanism is only as good as the key.

Two tools recording the same outage as `"checkout outage"` and `"Checkout was down (Sept 14)"`
produce two active memories that contradict each other, and nobody finds out. The same two tools
recording `incident:checkout-api:2026-09-14` produce one memory with two versions.

The grammar is therefore not decoration. It is the part that makes a shared corpus converge.

## The vocabulary

| `kind` | `subject` | Required in `metadata` |
| :-- | :-- | :-- |
| `decision` | `decision:<area>:<slug>` | — |
| `convention` | `convention:<area>:<slug>` | — |
| `incident` | `incident:<service>:<yyyy-mm-dd>` | `service`, `occurred_at` |
| `ticket_summary` | `ticket:<tracker>:<key>` | `tracker`, `key` |
| `run_lesson` | `run_lesson:<workflow>:<slug>` | `workflow` |
| `flaky_test` | `flaky_test:<path>::<name>` | `test_path`, `test_name` |
| `hotspot` | `hotspot:<path>` | `path` |
| `finding` | `finding:<tool>:<rule>` | `tool`, `rule` |
| `other` | *(anything)* | — |

`other` is the escape hatch: the envelope still applies, nothing else is demanded, and no subject
grammar is imposed. A memory with no `kind` at all is untyped and only sees the envelope.

Subjects are matched **after** normalization — `normalize_subject` lowercases, trims and collapses
whitespace — so `Decision:API:Versioning` and `decision:api:versioning` are one key, and the
grammars are written in lowercase.

### The envelope

Checked for every fact, typed or not
([`_envelope.schema.json`](../crates/ecphoria-core/schemas/facts/_envelope.schema.json)):

```jsonc
{
  "kind": "incident",                       // one of the kinds above
  "paths": ["services/checkout/**"],        // code this fact is about; array of strings, unique
  "external_id": "PD-4412",                 // the writer's own id, for idempotent re-delivery
  "source": "pagerduty",
  "provenance": {                           // `source` is what makes a fact checkable later
    "source": "pagerduty",                  // required *within* provenance, if provenance is given
    "ref": "PD-4412",
    "run_id": "…", "work_item_key": "…", "author": "…",
    "ts": "2026-09-14T03:12:00Z"
  }
}
```

Anything else in `metadata` is yours and is never rejected: the schemas constrain the fields
Ecphoria itself reads, not your fields.

## An example

```bash
curl -X POST localhost:8432/api/v1/memories -H 'content-type: application/json' -d '{
  "tenant_id": "acme",
  "subject": "incident:checkout-api:2026-09-14",
  "content": "checkout returned 503 for 40 minutes after the 03:05 deploy",
  "metadata": {
    "kind": "incident",
    "service": "checkout-api",
    "occurred_at": "2026-09-14T03:12:00Z",
    "severity": "sev2",
    "paths": ["services/checkout/**"],
    "provenance": {"source": "pagerduty", "ref": "PD-4412"}
  }
}'
```

Get it wrong and the answer says everything that is wrong, at once:

```json
{
  "error": {
    "code": "VALIDATION_FAILED",
    "message": "metadata: missing required field `service`; metadata: missing required field `occurred_at`; subject: `the checkout outage` does not match the `incident` grammar `incident:<service>:<yyyy-mm-dd>`"
  }
}
```

**422**, not 500: a 500 tells a client to retry, a 422 tells it to fix the request.

## Turning it on

```toml
[memory.governance]
require_provenance = false     # default for every tenant
fact_validation = "off"        # "off" | "warn" | "strict"

[memory.governance.tenants.acme]
require_provenance = true
fact_validation = "strict"
```

Per tenant, because these are *editorial* rules rather than engine settings: one team runs a curated
corpus where every memory must be attributable, while the tenant beside it is still importing a
decade of unattributed notes. A single global switch would force the strictest tenant's policy on
the loosest, so the global value is only the default and each tenant may override it.

### The `warn` step matters

Switching a store that already holds untyped memories straight to `strict` means choosing between
breaking every writer and learning nothing. In `warn` the write still lands, the failure is logged
with the exact list, and `ecphoria_fact_validation_failures_total{mode="warn"}` counts it. Watch the
counter, fix the writers, then switch to `strict`.

| Mode | Write | Metric | Log |
| :-- | :-- | :-- | :-- |
| `off` | accepted, unchecked | — | — |
| `warn` | accepted | `…_failures_total{mode="warn"}` | `warn` with the list |
| `strict` | **422** | `…_failures_total{mode="strict"}` | — (the caller is told) |

### Provenance

`require_provenance` refuses a write whose origin is not recorded. Two things count, and nothing
else: `metadata.provenance.source`, or a non-empty `source_event_ids` pointing at episodic events
already in the store. An empty `provenance: {}` — what a client sends when it has nothing — does
not count, otherwise the requirement would be satisfied by the absence of an answer.

Ecphoria's own writes are attributable too, so a governed tenant keeps its features: document
sections carry `{"source": "document", "ref": "<path>"}`, webhook promotion carries the event it
came from, and consolidation carries `{"source": "ecphoria:consolidation"}`.

The refusal counts as `ecphoria_memory_writes_refused_total{reason="provenance"}`.

## Where it applies

Every write path, because a rule with one way around it is not a rule:

- `POST /api/v1/memories` and `?status=pending` (proposals)
- `POST /api/v1/memories/batch`
- `PUT /api/v1/memories/by-external-id`
- `POST /api/v1/documents`
- accepting a proposal (it runs the normal cognition path)
- the MCP `remember` tool and the gRPC and PG-wire surfaces, since all of them land in the same
  engine call

## `kind` is also a column

Migration 7 gives `memories` a `kind` column, derived from `metadata.kind` at write time and
backfilled for memories written before it existed. `metadata` remains the wire format; the column
exists for SQL:

```sql
SELECT service, COUNT(*)
FROM memories
WHERE kind = 'incident' AND valid_from > now() - INTERVAL 90 DAY
GROUP BY service ORDER BY 2 DESC;
```

An indexed predicate rather than a JSON extraction on every row — and an index cannot be built on
one. An untyped memory leaves the column `NULL` rather than inventing a kind.

## The validator

The schemas are validated by a small JSON Schema (2020-12 subset) validator in
`memory::schema`, not by a schema crate: the only documents it ever sees are the ones in this
repository.

The usual failure mode of a hand-rolled validator — a keyword nobody implemented silently passing
every document — is closed by construction: `Validator::compile` **rejects** a schema containing a
keyword it does not support, and a test compiles every embedded schema. Adding `oneOf` to a fact
schema breaks the build rather than quietly weakening validation.

Supported: `type`, `properties`, `required`, `additionalProperties`, `enum`, `const`, `pattern`,
`minLength`, `maxLength`, `minimum`, `maximum`, `minItems`, `maxItems`, `uniqueItems`, `items`,
`format` (`date-time`, `date`, `uri`, `uuid`).
