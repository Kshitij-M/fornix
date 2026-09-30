# Loop 96 completion — generic effect verification and fenced reconciliation

Status: implemented on the universal-transformation branch; live external
provider qualification remains deliberately separate.

## Outcome

Fornix now has a provider-neutral proof boundary for generic workflow steps
that have crossed an external effect boundary. The implementation closes the
gap between “an effect was admitted and may have been sent” and “the result is
authoritatively verified”. It does not add a second effect ledger, invoke a
model, or claim exactly-once external execution.

The delivered slice provides:

- typed effect-verification requests and redacted verified/failed/unknown
  outcomes;
- explicit capability-level verifier registration through the existing
  connector registry;
- deterministic fake-domain verifiers for data-pipeline publish and
  customer-support reply effects;
- a recovery lease validation path with monotonic fencing;
- atomic effect-state and domain-effect-link reconciliation in one Postgres
  transaction;
- a workflow `verify` API and CLI command with separate workflow/effect/task
  fences and expected versions;
- durable outcome reconstruction for the same verification idempotency key
  without invoking the verifier again; and
- contract, adapter, route-authorization, and CLI tests for workspace and
  redaction boundaries.

## Authority and crash semantics

The authorities remain intentionally separate:

```text
workflow lease + task fence
  → effect recovery lease + fence
  → registered verifier (hash-only facts)
  → operation_effect transition + domain-effect-link transition (one tx)
  → workflow checkpoint/resume (second fenced tx)
  → replay-verified receipt
```

The verifier runs before any local mutation. The reconciliation transaction
first records the pending state when necessary, then commits the final effect
state and matching link transition together. A crash before that commit leaves
both local authorities unchanged or explicitly pending. A crash after that
commit but before workflow resume leaves the workflow waiting; the same
idempotency key reconstructs the proof from Postgres and resumes without
calling the verifier again. An `unknown` result remains `recovery_required`,
not terminal: a later proof attempt may re-check it only with a new
idempotency key and current effect/link versions. A stale, expired, released,
or cross-workspace effect lease fails before even a duplicate terminal
observation can advance the workflow.

External execution remains at-least-once. A verifier proves an observed
outcome; it never retries or replays the external invocation.

## HTTP and CLI surface

Authenticated HTTP:

```text
POST /v1/workflows/{run_id}/verify?workspace_id=...
```

Required headers and body fields are:

- `X-Operation-Fence` for the workflow lease;
- `X-Effect-Fence` for the recovery lease;
- optional `X-Task-Fence` for task-bound runs;
- `step_id`, `expected_effect_version`, `expected_link_version`; and
- a stable `idempotency_key`.

The CLI equivalent is:

```text
fornix workflow verify --id RUN --step-id STEP \
  --fence WORKFLOW_FENCE --effect-fence EFFECT_FENCE \
  --expected-effect-version VERSION --expected-link-version VERSION
```

No raw provider payload, prompt, credential, or error string is accepted by
the contract or persisted by the reconciliation path.

When `--idempotency` is omitted, the CLI derives a bounded key from workspace,
workflow, step, and expected effect/link versions. Repeating the same proof
state is deduplicated; after an inconclusive result, an operator can inspect
the current versions and retry as a distinct proof attempt. Supplying an
explicit key remains supported and intentionally preserves that caller's
idempotency choice.

## Tests and verification

Offline checks added for this slice cover:

- operation/effect/link/hash/workspace binding;
- proof completeness and bounded failure codes;
- deterministic fake verified, mismatch, and uncertain outcomes;
- duplicate-safe CLI inputs and explicit fence requirements;
- authorization catalog coverage for the verify route;
- authorization replay protection: a repeated request identity cannot reuse an
  earlier allow after its effective role permissions change;
- an offline CLI command sequence for create, lease, advance, approve, verify,
  replay, and receipt finalization, validating workspace scope, fencing,
  expected proof versions, and idempotency headers; and
- default CLI verification idempotency that is stable for identical proof
  versions and changes when the expected effect/link versions advance.

The focused command is:

```sh
make qualification-generic-effect-verification
```

Postgres-backed stale-fence, expiry, takeover, atomicity, concurrent-winner,
crash-boundary, same-key verifier suppression, fresh-key retry, workflow
resume, and receipt tests are DSN-gated. They include the generic service-level
data-pipeline path and a customer-support path that builds and runs the Fornix
CLI against the complete authenticated HTTP router. The CLI path checks
workspace authorization, workflow/effect fences, duplicate verifier and
connector suppression, retry, replay, and receipt creation. In an explicitly
disposable environment, the required command is:

```sh
FORNIX_TEST_PG_DSN='postgres://.../fornix?sslmode=disable' \
  make qualification-generic-effect-verification-postgres
```

The authorization audit's concurrent idempotency, stale-principal role/key
revocation, changed-decision fingerprint, and HTTP middleware scenarios use a
separate target:

```sh
FORNIX_TEST_PG_DSN='postgres://.../fornix?sslmode=disable' \
  make qualification-authorization-audit-postgres
```

This environment did not have `FORNIX_TEST_PG_DSN`, so those tests were not
represented as passed. The host has only PostgreSQL 14.17 without pgvector,
and no local server is listening; Docker daemon access is unavailable. That is
not a compatible disposable target for the repository's CI migration
topology. No database, provider, or API key was started or accessed.

The full `go test ./... -count=1` command was attempted. Packages not requiring
loopback passed, but the run fails in pre-existing `httptest.NewServer` cases
because this execution sandbox rejects local socket binding (`operation not
permitted`). The affected packages include the watcher, HTTP connector,
connector/egress, credentials, model, and qualification suites. This is an
environment limitation, not evidence that the complete suite is green; run it
in CI or another environment that permits loopback listeners.

## Authorization decision freshness follow-up

The Task 97 review found that the HTTP layer authenticated a key and loaded
permissions, but the later store authorization call trusted that in-memory
permission slice. A role removal between authentication and authorization
could therefore leave a stale principal with a grant. `AuthStore.Authorize`
now reloads identity/key status, key expiry, and current unexpired role
bindings in its workspace transaction before deciding. Durable identity/key
and role rows are share-locked through that decision's commit; this gives
revocation a clear serialization point, but cannot retroactively cancel an
already-authorized in-flight effect. Effectful handlers still require their
own fences.

Migration `078_authorization_audit_decision_fingerprint.sql` makes audit
deduplication include the current decision hash. Identical decisions remain
idempotent; a changed allow/deny outcome or route is a new append-only audit
record, never a cached grant. Keyless principals are also denied by the store
unless they are the explicit development principal or the narrowly scoped
workspace-bootstrap principal. Authorization audit idempotency remains
separate from operation/effect idempotency. Because migration 078 changes the
audit conflict target, this schema is not compatible with older application
binaries; coordinated rollout is required until rolling-upgrade behavior is
qualified.

The new contract hash test and the full `contracts`, `store`, and `server`
package suites pass locally, including their race runs. All packages also
compile with tests disabled, `go vet` passes for the touched packages, and the
documentation checker passes. PostgreSQL-backed role/key revocation and audit
assertions are still DSN-gated and were not executed here; consequently no
authorization SQL latency or database/storage measurement is claimed.

Offline regression tests additionally prove that `recovery_required` is not
treated as a final verifier result, transition outcomes reconstruct
deterministically, and different workflow resume results cannot collide on
one idempotency key. A database-gated data-pipeline workflow test exercises
approval, fake connector dispatch, an injected crash after proof commit,
same-key resume without a second verifier call, a fresh-key successful retry,
full replay, and Work Receipt finalization. The customer-support integration
scenario builds the CLI into a temporary directory, supplies the test API key
through the child environment (not command arguments), and drives workflow
creation, approval, dispatch, effect-lease acquisition, unknown verification,
crash recovery, retry, replay, and receipt through the authenticated router.
The local sandbox still lacks the disposable PostgreSQL DSN needed to execute
either integration path.

## Cost, storage, and limitations

The Task 96 workflow-verification slice adds no migration or new
infrastructure. Its authorization-freshness follow-up adds migration 078 but
no new service. A successful reconciliation appends bounded effect/link
transition rows and a workflow checkpoint; a duplicate adds no final
transition. Fake verification has no provider token cost and stores only
hashes and bounded identifiers.

Still open for production qualification:

- live connector/provider proof adapters and their credential/egress controls;
- Postgres lock/WAL/latency measurements under concurrency;
- crash injection against a disposable database at each transaction boundary;
- receipt enrichment with domain-specific evidence and artifact references;
- backup/restore, HA/failover, load/soak, retention, and incident operations;
  and
- deployment-owned qualification evidence for every production adapter.

## Next task prompt

**Task 97 remains in progress — execute the generic verification qualification
against disposable Postgres/pgvector and run live CLI/HTTP coverage.** The
customer-support CLI-over-authenticated-HTTP integration test and offline CLI
command-sequence contract test are present, but the database-backed workflow
has not executed in this environment.

Before coding, read the chats directory, `AGENTS.md`, the universal roadmap,
`docs/215-generic-workflow-service-foundation.md`,
`docs/217-generic-effect-verification-foundation.md`, this completion note,
and the operation/effect/link/workflow/receipt authorization stores. Study the
reference repositories’ recovery, reconciliation, and evaluation patterns
without copying incompatible code.

Remaining qualification work:

- run the new DSN-gated effect lease expiry/takeover, stale-token,
  concurrent-reconciliation, atomic-commit, crash, same-key replay, and
  workflow-resume scenarios against disposable Postgres;
- add deterministic latency, lock-wait, SQL, WAL/storage, and replay metrics;
- exercise the complete data-pipeline and customer-support fake workflows
  through CLI and authenticated HTTP, including approval, effect reservation,
  verification, receipt finalization, and replay (the customer-support path
  now builds and runs the actual CLI against the authenticated router; both
  database-backed paths remain unexecuted here because no disposable DSN is
  available);
- run duplicate-suppression against authenticated HTTP and CLI integration
  paths and prove neither verifier nor connector is re-executed (the HTTP test
  asserts this, but still needs its PostgreSQL run);
- keep cross-workspace, RBAC, task-fence, redaction, and at-least-once tests;
- execute the existing CI/Make Postgres qualification target on a disposable
  database and record its measured results; and
- update the public readiness document with database measurements and the
  remaining live-provider limitations after that run.
