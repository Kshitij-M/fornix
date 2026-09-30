# Sandbox cleanup intent and recovery-redaction foundation

Status: durable cleanup-intent foundation implemented; runtime qualification is
incomplete. Typed contracts, migration 081, a workspace-scoped fenced Postgres
queue, append-only cleanup events, and transactional enqueue integration are
present. A cleanup worker/runtime adapter is not wired into `fornix start`, and
the database-backed and supported-host qualification gates have not passed in
this environment.

This slice is a prerequisite for a future OCI runner. It closes two authority
gaps before a container runtime is connected: cleanup must survive a control
process crash, and recovery must not return output that cannot be safely
redacted after restart. It does not add a Docker client, enable an OCI provider,
or claim process/container isolation.

## Invariants

1. PostgreSQL remains the authority. When the terminal tool result and
   verified/reconciled effect are already available together, the cleanup
   intent commits with that result transaction. If external verification
   happens later, the intent commits with the transaction that appends the
   matching reconciled-link transition; it re-reads and verifies the already
   committed result hash. A crash before the eligibility transaction commits
   leaves no cleanup request; a crash after commit leaves a durable retryable
   request.
2. A `recovery_required` tool run is never eligible for cleanup. The exact
   attempt remains available for reconciliation until the effect authority
   establishes a terminal outcome. Eligibility additionally requires the
   generic effect to be `verified` and the current domain-effect link
   transition to be `reconciled`; a terminal tool-run row alone is not enough.
   The verified effect response hash must equal the reconciled link result hash,
   and the tool result's canonical `ContentHash` must equal that same hash.
3. Cleanup is authorized by a durable, workspace-scoped record bound to the
   full normalized sandbox execution identity, tool request hash, and terminal
   result hash. A caller-supplied “result committed” claim or container name is
   not authority. The runner must compare the identity and request hash with
   immutable runtime labels before removing anything.
4. Cleanup is idempotent and bounded. “Already absent” is success only after
   the runtime confirms that the exact identity-bound object is absent. An
   unknown or mismatched object is retained and reported as a stable failure.
5. A worker claims cleanup with a workspace-scoped lease and monotonically
   increasing fencing token. Every renewal and state transition checks owner,
   fence, and lease validity in the same transaction. Expired work can be
   taken over; stale workers cannot complete or reschedule it.
6. Mutable queue state is a rebuildable scheduling projection. Append-only
   cleanup events preserve claim, retry, completion, and dead-letter history.
   Batches, retries, output, and event payloads are bounded.
7. The initial offline runner profile rejects all request-supplied environment
   entries and inherited host environment. It does not attempt to guess which
   arbitrary values are secrets. A credential-bearing profile remains disabled
   until a managed credential reference/lease and restart-safe output-redaction
   contract are implemented and qualified.

## Implemented durable control-plane slice

Typed `SandboxCleanupIntent`, `SandboxCleanupJob`, `SandboxCleanupLease`, and
`SandboxCleanupObservation` contracts carry only identifiers, stable hashes,
backend, and schema version—never argv, environment values, host paths,
credentials, or raw output.

Migration 081 adds a workspace-scoped cleanup-job table and append-only
cleanup-event table. The job identity is unique per workspace and exact tool
attempt. The normalized execution-identity JSON (IDs and hashes only), its
stable hash, tool request hash, and committed result hash let a future runner
verify the same authority after restart. Bounded status, owner/fence/lease
expiry, retry deadline/count, stable failure code, completion owner/fence, and
timestamps are stored. Duplicate completion is accepted only for the exact
owner and fence that committed it. Composite workspace foreign keys, explicit
RLS policies, deterministic claim indexes, append-only event enforcement, and
immutable intent identity are in place. Queue-state mutations and their
history append happen in one transaction.

The authoritative enqueue seam is shared by `ToolRunStore.finishToolResultTx`
and the domain-effect transition that first reconciles a link. A non-local
tool link cannot be reconciled until the generic effect is verified with the
same result hash, so verification-before-link is required and cannot race into
a missed enqueue. The first path covers a tool result finalized after its
effect; the second covers external verification that completes after the
tool-run row is terminal. Replaying the exact reconciliation transition also
rechecks eligibility and repairs a missing intent idempotently. Each path
rechecks the run, effect, and latest link transition in the same transaction.
A missing link supplies no cleanup authority, but does not roll back a known
terminal failure when the dispatcher never created an effect. Unknown external
outcomes must remain `recovery_required`. Cleanup work and exhausted-job
dead-lettering are both bounded to a maximum of 100 jobs per claim.
`recovery_required` never enqueues. Repeated finalization relies on the unique
identity and exact-hash comparison; a conflicting retry fails closed.

The store API claims a small bounded batch in deterministic order using
PostgreSQL time and `FOR UPDATE SKIP LOCKED`. Claim, heartbeat, retry, complete,
and dead-letter transitions are transactional and fenced. Retry delays use
bounded exponential backoff with a maximum attempt count. A cleanup result may
be marked complete only for an exact identity-matching “removed” or
“already absent” observation. Timeout, runner loss, identity mismatch, and
unknown status remain retryable or dead-lettered; none permits tool re-run.
The process that consumes these jobs and talks to an isolated runtime is still
future work; the queue foundation alone does not remove containers or prove
runtime isolation.

## Recovery redaction

`ToolRequest.RedactedEvidence` intentionally removes environment values, while
the current sandbox reconciliation contract is identity-only. Persisting raw
values to solve this would violate credential handling, and reconstructing
them from redacted evidence is impossible. Therefore the first runner profile
must reject non-empty request environments and inherited host environment
before dispatch. Recovery output must still pass bounded credential-pattern
redaction before persistence; no raw runner diagnostics are returned. Future
secret use requires explicit credential references, short-lived lease
materialization inside the trusted runner, and a restart-safe redaction design
that never persists secret bytes in labels, logs, evidence, checkpoints, or
cleanup records.

## Reuse and licensing

Reuse Fornix's existing tool-result transaction, domain-effect link, immutable
execution identity, fencing, event history, RLS conventions, and stable error
codes. Reference agentmemory's lease-expiry pattern for rechecking state under
lock and recording an audit transition; use Orloj's controller/reconcile
pattern for bounded retry and cleanup ownership. These references inform the
design only; no upstream source is copied. Both inspected reference projects
are Apache-2.0. Do not copy Kronaxis Fabric (BSL 1.1). No new dependency or
service is required for the durable queue.

## Cost and acceptance tests

The expected steady-state cost is one small job insert and one history insert
per finalized non-local tool attempt, plus bounded claim/transition SQL. Unique
indexes should keep claim work proportional to the due batch rather than total
history. Measure inserted bytes, SQL statements per attempt, claim latency,
retry throughput, and cleanup completion age with a disposable PostgreSQL
instance; do not infer runtime cleanup latency from unit tests.

Acceptance tests must cover fresh and upgrade migration paths; RLS and
cross-workspace denial; duplicate intent and conflicting hashes; atomic rollback
when result or event persistence fails; no intent for local, nonterminal, or
`recovery_required` runs; claim ordering and bounded batches; concurrent claims;
stale-fence rejection; lease expiry/takeover; retry/dead-letter bounds; crash
after result commit and before cleanup; repeated cleanup and exact
already-absent handling; runtime identity mismatch; unknown observations never
completing cleanup; environment and inherited-environment rejection; and
credential absence from rows, logs, errors, and events.

This slice is not complete until unit tests, race tests, disposable-Postgres
tests, fake-runner protocol tests, and live supported-host qualification have
all been run and recorded. Docker Engine/Desktop and PostgreSQL access are not
available in the current execution environment, so live qualification remains
an explicit external gate.
