# Loop 15 completion — observability, cost accounting, and replay evaluation

Status: implemented and locally qualified on 2026-08-20.

## Delivered

- Added bounded typed contracts for observations, spans, cost entries, metric
  samples, evaluation datasets/cases/runs/results, and quality gates.
- Added migration `022_observability_evaluation.sql`. Existing databases and a
  newly-created Postgres database both applied it cleanly.
- Added append-only, workspace-scoped durable rows with idempotent accounting,
  conflict detection, redacted evidence, fixed metric dimensions, and a
  bounded authorized `GET /v1/observability/metrics` endpoint (plus `/v1/metrics`).
- Instrumented model completion, tool completion, retrieval compilation,
  agent transitions, approval request/decision, retry waits, scheduler claim/
  takeover/renew/release, and artifact metrics. Source transitions place
  observations/cost entries in the same Postgres transaction where the source
  mutation is authoritative.
- Added deterministic measured/estimated/unknown cost attribution for provider
  usage, tool duration, retrieval database work, artifact logical/physical
  bytes, and retry transitions. Model-call rows remain the usage authority.
- Added offline `eval.Runner` and `ReplayCase`. Replays read durable agent
  checkpoints/history only; they do not have provider or tool dependencies.
  Cases are sorted by ID, bounded by batch size, hashed canonically, and can be
  run dry without writing results. Oversized reports use ArtifactStore.
- Added `v0.24-observability-smokes.sh`, Make targets, CI integration tests,
  documentation, and redaction/cardinality/reconciliation tests.

## Qualification results

| Check | Result |
|---|---|
| Fresh Postgres migration 022 | Pass; new temporary database created, tested, and removed |
| Existing Postgres migration/checksum path | Pass |
| Focused observability/evaluation integration tests | Pass |
| Full `go test ./...` | Pass |
| Full `go test -race ./...` | Pass |
| Full `go vet ./...` | Pass |
| Production build | Pass |
| Python checks and shell syntax/diff checks | Pass |
| v0.24 store + HTTP smoke | Pass; unauthenticated metrics returned 401 and authorized response was workspace-scoped |

## Local measurements

- On the local Docker Postgres instance, 30 authorized empty-window metrics
  requests measured p50 **1.530 ms**, p95 **2.722 ms**, and max **7.279 ms**.
  This is an endpoint measurement on a small local dataset, not a capacity
  claim.
- A metrics snapshot performs five bounded aggregate SQL query groups (base
  observations, spans, samples, fixed operation groups, and cost groups), all
  constrained by workspace and a maximum 24-hour window. A normal terminal
  model/tool mutation adds one accounting insert and one idempotency lookup per
  accounting type inside its existing transaction.
- Relation sizes immediately after the qualification smoke on the local empty
  dataset were: observations 80 KiB, spans 32 KiB, costs 112 KiB, metric
  samples 32 KiB, datasets 80 KiB, runs 80 KiB, and results 64 KiB. These are
  PostgreSQL relation/index page allocations, not logical payload sizes.
- Twenty repetitions each of the replay and bounded evaluation integration
  cases executed 40 recorded cases in **0.473 s** of Go package runtime,
  approximately **84 case executions/s** excluding Docker startup and module
  download. A production throughput benchmark with large histories, concurrent
  writers, and populated indexes remains outstanding.
- Cost attribution is exact only when the provider supplies measured usage and
  configured pricing. Missing usage or pricing is recorded as estimated or
  unknown; it is never presented as measured billing data.

## Remaining limitations

- There is no Prometheus/OTel collector, metric retention compactor, HA metrics
  service, or administrative evaluation HTTP API. Postgres is the authority
  and the endpoint is a bounded snapshot surface.
- Gold evidence references are validated as canonical hashes, but a complete
  retrieval-gold scorer that resolves each reference against evidence/context
  history is deferred. Expected context, termination, abstention, cost, and
  checkpoint consistency are evaluated now.
- Evaluation replay verifies recorded agent state and history; it does not
  reconstruct a provider response or execute tools. External model/process
  boundaries therefore remain at-least-once.
- Observability rows are durable and append-only but do not yet have a
  scheduled retention/partitioning operation. Large-scale storage and
  backpressure require a measured workload benchmark and a later architecture
  decision.
