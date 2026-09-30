# Loop 94 completion — multi-domain reference workflows and qualification

Status: implemented on `feat/issue-40-production-qualification`.

## Outcome

Fornix now exercises its generic operation/effect/replay contracts across four
non-repository scenario families:

- incident response;
- data pipeline operations;
- customer support;
- infrastructure maintenance.

Each scenario builds a workspace-scoped, actor-scoped operation plan with
observation, validation, and verification stages. Effectful variants add an
explicit approval boundary and an approval-required write step. The offline
replayer emits bounded synthetic hashes only; unresolved external effects
remain `recovery_required` until a recorded effect hash is supplied.

## Delivered files and surfaces

- `internal/contracts/reference_workflow.go`: bounded scenario intent,
  deterministic plan metadata, trace, effect-state, and replay contracts.
- `internal/contracts/reference_workflow_test.go`: identity, isolation,
  approval, and hash tests.
- `internal/workflows/reference/scenario.go`: generic plan builder and
  external-effect-safe offline replay runner.
- `internal/workflows/reference/scenario_test.go`: four-scenario plan,
  deterministic replay, unresolved-effect, and resolved-effect tests.
- `docs/213-multi-domain-reference-workflows-foundation.md`: invariants,
  scope, licensing, cost, and acceptance criteria.
- `Makefile`, CI, and documentation index: focused qualification command and
  public status handoff.

## Safety semantics

- No scenario accepts raw payloads, prompts, credentials, or arbitrary output
  text in its authority contract.
- Actor, target, connector, capability, input, and plan references are
  workspace-scoped and hash-bound.
- Effectful scenarios cannot be built without an explicit approval flag.
- A local replay never invokes a connector, model, network, filesystem,
  broker, or database.
- Unknown external outcomes do not become success by replaying locally.

## Verification

Verified without Docker, PostgreSQL, provider credentials, or external
systems:

- `make qualification-multidomain-reference` — passed.
- `go test ./internal/contracts ./internal/workflows/reference` — passed.
- Existing full tests, race tests, build, vet, documentation checks, and
  shell checks remained green before this slice; the complete suite should be
  rerun before commit/PR submission.

## Cost and storage impact

The fixture runner is in-memory and offline. It adds no migration, table,
artifact, event, provider call, container, or persistent cache. Test input is
four bounded plans with hash-only outputs. No Docker/Postgres storage was
created.

## Remaining limitations

This loop proves generic contract portability and deterministic replay, not
live system support. A production connector must still provide signed schema
admission, credential and egress authority, provider idempotency behavior,
verification, reconciliation, retention, and deployment evidence. The
universal roadmap remains open for deployment topology, backup/restore,
retention, load/soak, security, live connector, release, and support
qualification.
