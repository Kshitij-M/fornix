# Loop 27 completion: durable multi-step workflow runtime

Status: alpha foundation implemented on `feat/issue-44-workflow-runtime` for
[Issue #44](https://github.com/Kshitij-M/fornix/issues/44). This loop expands
Fornix from a single agent-run path into a domain-neutral, durable workflow
runtime while keeping repository maintenance as only one adapter.

## What shipped

- `internal/contracts/workflow.go` defines bounded workflow runs, step states,
  waits, failures, budgets, checkpoints, terminal status, stable hashes, and
  deterministic runnable-step selection.
- `internal/store/migrations/038_workflow_runtime.sql` adds workspace-scoped
  current workflow state, step projections, append-only transition history,
  and step/run idempotency records.
- `internal/store/workflow.go` adds atomic create, operation-linked leases,
  fenced step start/completion, cancellation, recovery-required conversion,
  wait resume semantics, retry-budget enforcement, and replay from zero or a
  checkpoint.
- `internal/workflow/runtime.go` adds a generic executor seam. Independent
  read-only/observation siblings may execute concurrently; durable commits are
  applied in canonical plan order. Effectful siblings are serialized.
- The server composition now constructs the generic operation and workflow
  stores. Existing repository agent-loop behavior remains intact.
- `internal/store/operations.go` now exposes `CreateTx`, allowing operation
  identity, workflow projection, initial step rows, and the workflow-created
  event to commit together.

## Invariants verified

1. Postgres remains the only authority for workflow identity, checkpoint,
   transition, idempotency, and ownership.
2. Operation leases and task fences are validated inside the same transaction
   that changes workflow state. Stale owners fail closed.
3. Plan hashes and workspace references are immutable and validated before
   persistence.
4. A step start reserves the attempt before an executor can run. A crash after
   reservation is represented as `recovery_required`; the runtime never guesses
   whether an external effect happened.
5. Duplicate start, completion, cancellation, and create requests return the
   original durable outcome when their command hash matches.
6. Approval, human, callback, retry, and external waits are durable. Resume is
   a new idempotent command against the same step attempt.
7. Replay verifies transition version order, previous/state hashes, event
   presence, checkpoint contents, and the final current-state hash. It never
   invokes an executor or external system.
8. Read-only fan-out is bounded by `MaxParallelRead`; result commits remain
   ordinal-ordered. Effectful steps are not admitted into a parallel batch.

## Qualification performed

Against a clean PostgreSQL 17/pgvector database with migrations through 038:

- `go test ./internal/store -run TestWorkflow -count=1`
- `go test ./internal/workflow -count=1`
- `go test ./...` with `FORNIX_TEST_PG_DSN` set to the clean database
- crash rollback before workflow checkpoint commit;
- duplicate create/start/complete delivery;
- approval wait and resume;
- stale owner rejection;
- workspace-isolated reads;
- concurrent duplicate start with one transition;
- fake executor completion and replay;
- crash recovery of an in-flight step;
- bounded parallel read-only fan-out with concurrent executor calls and
  deterministic commit order.

The repository-wide clean-database run passed. Race and CI qualification are
still required before merge and are intentionally not represented as complete
until GitHub Actions verifies this branch.

## Local measurements

These are development observations, not capacity claims. On the local Docker
PostgreSQL instance used for this loop, the workflow integration package ran in
under one second after the database was warm, and the full Go suite completed
in roughly ten seconds with integration tests enabled. A one-step successful
workflow produces one initial event, two workflow transitions (start and
completion), three operation lifecycle transitions needed to move `created` to
`running`, and one terminal operation transition. Each step commit locks the
operation lease and workflow row, reads bounded step state, reserves one
idempotency key, appends one event, updates one run row and one step row, and
commits one transaction. Replay is read-only and scales with the bounded
transition history, not with model or connector work.

The migration deliberately stores hashes, references, and bounded JSON state;
raw model/tool/connector output remains in the existing artifact/evidence
authorities. Transition history grows approximately linearly with committed
step commands and is subject to the existing retention/operations policy work.

## Reuse and licensing

The implementation reuses Fornix operation contracts, operation leases and
fences, task-fence validation, event append, artifact/evidence references,
workspace actors, and Work Receipt links. Orloj execution/checkpoint seams,
DeepSeek Harness turn/tool boundaries, agentmemory leases/replay diagnostics,
ClawMem recorded action flow, and Temporal-style durable workflow concepts
informed the design. No reference source was copied. Kronaxis-fabric source was
not copied because its BSL 1.1 license is incompatible with this MIT project.

## Remaining limitations

- There is no distributed workflow scheduler or operator HTTP/MCP surface yet;
  the generic store/runtime seam is ready for the multi-domain workflow slice.
- Connector-specific authorization, verification, egress, compensation, and
  provider idempotency remain adapter responsibilities.
- The runtime treats an uncertain external boundary as recovery-required; it
  cannot infer or guarantee exactly-once execution outside Postgres.
- Wall-clock, SQL-work, and cost budgets are represented in the contract, but
  provider/connector accounting integration belongs to the universal reference
  workflow and production qualification slices.
- Work Receipt projection links for generic workflow transitions are additive
  follow-on work; the authoritative operation, event, artifact, and evidence
  histories are preserved now.
