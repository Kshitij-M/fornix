# What works today

Status: current public-alpha capability map.

Fornix is a public alpha. It is a domain-neutral control plane with a
repository adapter and bounded reference connectors. The table below is the
shortest honest answer to “what can I use right now?”

| Use case | Status | Safe interpretation |
| --- | --- | --- |
| Offline fake-provider workflow | Supported alpha | Deterministic local evaluation with no model key or network requirement |
| Repository ingestion and read-only analysis | Alpha-qualified local adapter | Explicit local mounts, bounded indexing, evidence, artifacts, and replay; not qualification for unattended production changes |
| Approval-gated local repository changes | Experimental alpha | Use only on disposable or operator-controlled workspaces; review the change packet |
| HTTP/API read and list operations | Bounded reference adapter | Test-system experiments with explicit destination, response, retry, credential, and cost bounds |
| HTTP/API writes | Reservation/reconciliation foundation | Fornix records the effect boundary but does not provide generic live dispatch, verification, or compensation |
| Read-only SQL inspection | Bounded reference adapter | Prepared, read-only, allowlisted, row/byte/cost-bounded queries |
| Cloud operations | Not implemented or qualified | No production cloud executor is included |
| Ticketing and business systems | Not live-qualified | Use only a purpose-built adapter after its own conformance and recovery review |
| Multi-step workflows | Alpha foundation | Durable typed steps and waits exist; production worker/fairness qualification remains |
| Multi-agent execution graphs | Not implemented | Fornix does not currently provide a general multi-agent graph runtime |
| Hosted multi-tenant production service | Not qualified | No HA, PostgreSQL RLS, external secret manager, backup/restore SLO, or support contract |

## The safe default

Use the deterministic fake provider and a disposable local workspace first:

```sh
make build
./bin/fornix doctor
./bin/fornix start --repo .
./bin/fornix demo --repo .
```

The fake path is the canonical contribution and regression path. It does not
contact a model provider or external system.

## What the generic operation surface does today

Read-only and observation capabilities can be admitted, executed, persisted,
inspected, and replay-verified. An effectful operation must first be reserved
and later reconciled with a fenced, append-only state transition. Reservation
does not dispatch a remote request. The generic server intentionally rejects
effectful execution until a domain adapter owns dispatch, provider idempotency,
verification, and compensation semantics.

Fornix never claims exactly-once remote execution. If a process dies after a
provider may have accepted a request, the effect must be inspected and
reconciled explicitly rather than blindly retried.

Historical federation and router-learning compatibility routes are quarantined
by default because their original tables predate workspace isolation. They are
available only for explicit non-production compatibility qualification with
`FORNIX_ENABLE_LEGACY_GLOBAL_SURFACES=true`; production rejects that setting.

## Production warning

Do not use the current alpha for unattended production changes, sensitive
multi-tenant workloads, live cloud/database automation, or high-availability
operations. Read [`14-production-readiness-qualification.md`](14-production-readiness-qualification.md)
before evaluating a new domain.
