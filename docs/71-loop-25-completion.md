# Universal admission and external-effect foundation completion

Status: implemented as an alpha foundation on the Issue [#46](https://github.com/Kshitij-M/fornix/issues/46) branch; not a claim that universal connector execution is production-qualified.

## Outcome

Fornix now has one durable, domain-neutral boundary for deciding whether a
typed operation may proceed and for recording what happened at an external
effect boundary. This is the control-plane layer shared by repository, API,
database, cloud, ticketing, and business-system adapters. No adapter is
allowed to reinterpret an unknown effect, bypass approval, or turn an
uncertain remote outcome into a silent retry.

The implementation is intentionally split into three authorities:

1. `internal/policy` evaluates normalized, secret-free facts without I/O.
2. `internal/store/AdmissionStore` persists decisions, approvals, and effect
   transitions transactionally in Postgres.
3. A future qualified connector supplies domain-specific execution, credential
   resolution, verification, and compensation; it does not become the source
   of truth for the control-plane history.

## Delivered

- `internal/contracts/admission.go` defines bounded policy, credential-state,
  admission, approval, and external-effect contracts.
- `internal/policy/admission.go` implements deterministic fail-closed
  admission for capability, connector, resource, actor, evidence, credential,
  task-fence, cost, quota, approval, and effect-class rules.
- Migration `036_operation_admission_effects.sql` adds workspace-scoped
  immutable admission decisions, approval state/history, current effect state,
  and append-only effect transitions.
- `internal/store/admission.go` adds transactional admit, read, approve, and
  fenced effect-update APIs with duplicate delivery and crash hooks.
- `OperationStore.ReserveEffect` initializes the durable effect state in the
  same transaction as the immutable effect reservation.
- Decision identity excludes transport request IDs and changing runtime facts;
  retries, quota-window changes, and worker takeover cannot create a second
  logical admission for one idempotency key.
- Quota reservations for one workspace/actor are serialized with a Postgres
  transaction advisory lock. A concurrent pair cannot both pass a one-slot
  quota.
- Events contain bounded hashes, references, and outcomes only. Credentials,
  prompts, headers, raw connector payloads, and approval reasons are not
  persisted in this slice.
- CI and Make smoke coverage exercise contracts, policy, store admission, and
  operation/fencing behavior.

## Validation evidence

The following checks passed locally after applying migration 036 to a fresh
Postgres database (`fornix_admission_fresh`):

| Check | Result |
| --- | --- |
| Fresh migration plus admission/operation integration tests | Passed; store package 1.286s |
| Full `go test ./...` without external database | Passed; all packages |
| Full `go test ./internal/store -count=1` against fresh Postgres | Passed; 4.511s |
| `go test -race ./...` | Passed |
| `go vet ./...` | Passed |
| `make fmt-check` | Passed |
| `make docs-check` | Passed; 74 Markdown files |
| `make build` | Passed; CLI, watcher, and evaluation binaries |
| `make hooks-check` and `make package-check` | Passed |
| `git diff --check` | Passed |

The targeted tests cover deterministic hashes, approval binding and
idempotency, concurrent quota reservations, workspace/credential failure,
stale operation and task fences, effect takeover, crash rollback, append-only
history, replay-safe duplicate delivery, and secret-like input rejection.

At the observed local schema scale, the five new tables occupied their
Postgres minimum relation pages (approximately 32–128 KiB each) after the
bounded test history. Each admission performs one operation lock, one
workspace/actor quota count, one decision insert, and one event insert.
Approval and effect transitions add one append-only history row and one event;
effect updates also update one small current-state row. The quota count is the
current O(window admissions) cost and is the first candidate for bounded
rollups/partitioning at larger scale.

## Recovery and delivery semantics

- Duplicate admission, approval, and effect commands return the original
  durable result when their idempotency identity matches.
- A conflicting reuse of an idempotency key fails closed.
- A crash before transaction commit leaves no decision, approval history,
  effect transition, or event.
- A remote call remains at-least-once. If the process crashes after dispatch,
  the durable state must be reconciled as acknowledged or
  `recovery_required`; Fornix does not claim exactly-once remote execution.
- Operation and task fencing is revalidated at effect mutation. A stale worker
  cannot advance effect state after takeover.
- Replay reads immutable decisions and transitions. It does not invoke
  connectors, models, tools, callbacks, networks, or compensation handlers.

## Remaining limitations

Issue #46 is not fully production-qualified yet. The following remain explicit
work rather than hidden assumptions:

- connector-specific authorization and durable connector registration;
- signed callbacks and provider request reconciliation;
- secret-manager-backed credential resolution and rotation integration;
- host-independent egress enforcement and network policy;
- operation lifecycle transitions that automatically consume admission outcomes;
- a public authenticated API/CLI surface for admission and effect recovery;
- durable multi-step workflow scheduling and compensation orchestration;
- quota rollups, partitioning, high availability, backup/restore, and load
  qualification under Issue #40.

The next dependency is Issue [#43](https://github.com/Kshitij-M/fornix/issues/43):
bounded HTTP/API and read-only SQL connectors that use this boundary without
creating a second authority.
