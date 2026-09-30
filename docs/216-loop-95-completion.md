# Loop 95 completion — generic workflow service and multi-domain operator surface

Status: implemented on `feat/issue-40-production-qualification`.

## Outcome

Fornix now has a domain-neutral workflow application surface on top of the
existing Postgres-backed operation, workflow, connector, admission, effect,
evidence, artifact, and Work Receipt authorities. The implementation does not
introduce a second workflow database or a domain-specific queue.

The delivered path provides:

- typed create, advance, resume, approval, cancellation, replay, lease, and
  receipt contracts;
- explicit workspace-scoped workflow leases with fencing;
- deterministic offline read/observation execution;
- fail-closed effectful execution when the authority boundary is absent;
- server-composed admission and durable effect-dispatch wiring when configured;
- distinct data-pipeline and customer-support fake-domain capabilities;
- authenticated HTTP and CLI lifecycle surfaces; and
- operation-backed receipt finalization after terminal replay verification.

## Delivered implementation

- `internal/contracts/workflow_control.go` adds bounded workflow request,
  resume, cancel, and replay envelopes and rejects nested workspace/fence
  mismatches.
- `internal/workflows/generic/` owns the generic service, explicit lease
  lifecycle, deterministic executor, connector adapter, effect-dispatch
  boundary, bounded replay, and receipt finalization.
- `internal/adapters/fakedomains/` provides materially different,
  hash-addressed data-pipeline and customer-support capability schemas. They
  are offline qualification adapters, not claims of live provider support.
- `internal/server/workflows.go` exposes authenticated workspace-scoped HTTP
  routes. Lifecycle mutations require explicit operation fences; approval and
  receipt routes use their scoped permissions.
- `cmd/fornix/cli.go` exposes the same lifecycle through the `fornix workflow`
  command family with bounded JSON output and explicit fence headers.
- `internal/contracts/domain_effect_link.go` permits workflow-step links while
  retaining the common effect-link authority model.
- `Makefile` and CI include `qualification-generic-workflow`.

## Safety and authority decisions

1. Lease acquisition is explicit. `advance`, `approve`, `cancel`, and effect
   dispatch cannot silently claim or refresh ownership.
2. A stale, expired, released, cross-workspace, or task-fence-mismatched
   worker fails closed through the existing store and dispatcher authorities.
3. Read/observation steps may run through the bounded connector executor.
   Effectful steps require the persisted root operation capability, approval
   where required, live admission, an effect reservation, and the current
   workflow/task fence.
4. Acknowledged or uncertain external outcomes do not become success. They
   remain `awaiting_external` or `recovery_required` until a future verifier
   records an authoritative outcome.
5. Replay is read-only and bounded. It does not call a model, tool, connector,
   broker, network, or external system.
6. Receipt finalization does not fabricate evidence, artifacts, or external
   success. It requires a succeeded run and verified replay and uses the
   existing operation-backed receipt store.

## Verification

Completed without starting Docker, PostgreSQL, Ollama, OpenAI, Anthropic, or
any external provider:

- `go test ./...` — passed.
- `go test -race ./...` — passed.
- `make qualification-generic-workflow` — passed.
- `make check` — passed, including Go tests, `go vet`, Python compilation,
  documentation validation, shell syntax checks, and focused runtime tests.

Database-backed tests were not falsely reported as complete: they remain
DSN-gated because `FORNIX_TEST_PG_DSN` was not configured. No provider key was
requested, read, logged, or written.

## Cost and storage impact

This slice adds no migration, image, container, provider call, or persistent
cache. Existing workflow, operation, effect, link, evidence, artifact, and
receipt tables remain the authorities. Offline tests use bounded in-memory
fixtures and hash-only outputs.

The live Postgres figures that still need measurement are transaction count,
lock waits, WAL/row growth, effect-dispatch latency, receipt size, and replay
throughput under a disposable DSN. The offline test run has no provider token
or external execution cost.

## Critique and remaining limitations

This is a substantial universalization step, not completion of the universal
production roadmap. The main remaining gap is the external outcome boundary:

- no generic verifier/reconciliation endpoint yet resolves
  `awaiting_external` or `recovery_required` results;
- the fake domains do not qualify a real external system or provider;
- live Postgres transaction/lock/crash behavior remains unqualified in this
  environment;
- receipt evidence/artifact enrichment is still adapter-owned and must be
  attached before a domain-specific production receipt is meaningful; and
- HA, backup/restore, load/soak, deployment, security, retention, and live
  connector qualification remain open under Issue #40.

The next slice should build the generic verifier/reconciliation boundary and
qualify it against disposable Postgres before enabling any non-test external
write.

## Next task prompt

**Task 96 — Build Fornix’s generic effect verification, reconciliation, and
multi-domain end-to-end qualification boundary.**

Before coding, read the chats directory, `AGENTS.md`, the latest universal
roadmap/completion notes, `docs/215-generic-workflow-service-foundation.md`,
this completion note, and the existing operation, admission, effect-dispatch,
domain-effect-link, workflow, Work Receipt, connector, and authorization
implementations. Study the corresponding verification/reconciliation patterns
in the reference repositories without copying incompatible source.

Implement the smallest production-quality vertical slice:

- add typed verifier, reconciliation request/result, and recovery contracts;
- add an authenticated workspace-scoped resume/verify path that requires the
  exact effect identity, operation hash, workflow fence, and task fence;
- resolve acknowledged, verified, failed, and uncertain outcomes through the
  existing effect/link stores without overwriting history;
- add deterministic fake verifiers for data-pipeline publish and customer-
  support reply effects, including mismatch and uncertain outcomes;
- finalize receipts only after authoritative effect verification and replay;
- add duplicate, stale-token, approval, crash, retry, replay, and
  workspace-isolation tests against disposable Postgres when configured;
- add CLI/HTTP qualification commands, CI/Make/smoke coverage, measurements,
  and public architecture documentation; and
- preserve the rule that external execution is at-least-once and never claim
  exactly-once execution.
