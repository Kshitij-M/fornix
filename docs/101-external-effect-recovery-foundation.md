# External-effect recovery lease foundation

Status: implemented as an alpha recovery-ownership foundation; this note
records the invariants and qualification boundary.

## Problem

Fornix already records an external-effect reservation and append-only state
transitions. The reservation can survive a worker crash, provider timeout, or
uncertain network response. The remaining gap is ownership: an operation lease
is not a durable recovery lease, and an operation may become terminal while an
effect still needs verification or compensation.

## Invariants

- The effect reservation and transition history remain authoritative and
  append-only.
- Recovery is workspace-scoped and uses a separate monotonically increasing
  effect fencing token.
- A recovery worker must hold the current effect lease before it can append a
  reconciliation transition.
- An expired or released lease can be taken over; a stale token fails closed.
- Recovery discovery is bounded, deterministic, and read-only until a worker
  explicitly claims an effect.
- The recovery API exposes hashes, states, deadlines, and provider identifiers,
  never credentials or raw external payloads.
- Repeated active lease acquisition is reusable, reconciliation state
  submissions are idempotent, and conflicting request hashes fail closed.
  Renewal and release require the current fence and fail closed after expiry
  or release.
- Fornix records at-least-once/unknown external delivery. It never claims
  exactly-once execution or silently retries an uncertain effect.

## Schema and transaction design

Migrations 041 through 043 add one mutable operational lease projection keyed by
`(workspace_id, effect_id)`. Its append-only transition table remains the
source of truth for the effect state. The lease stores owner, fence, expiry,
and timestamps and has a workspace/effect foreign key. Migration 042 adds an
explicit `dispatching` state, transition authority (`operation` versus
`effect`), a unique effect-transition idempotency index, and bounded recovery
indexes. Migration 043 preserves the immutable verification-required bit so
effect identity comparisons remain lossless across duplicate delivery. Claim,
takeover, renewal, release, and state transition checks occur in one Postgres
transaction.

Recovery candidates are derived from committed effect state rows. The bounded
ordering is workspace, update time, effect ID. Terminal states are excluded.
No broker, process-local queue, or secondary authority is introduced.

## Crash semantics

- Crash before lease commit: no ownership is visible.
- Crash after lease commit but before reconciliation: the lease expires and a
  later worker can take over with a higher fence.
- The operation worker records `dispatching` before a provider call. If the
  process dies before a provider outcome is known, the effect is not treated
  as an unattempted reservation.
- Crash before a state-transition commit: no state change is visible.
- Crash after a state-transition commit: the idempotency key returns the same
  committed state and does not append a second transition.
- A provider response that cannot be classified remains `recovery_required`
  or another explicit non-terminal state until verified or compensated.

## Reuse and licensing

The implementation reuses Fornix's existing operation lease, append-only event,
effect-state, hash, workspace, and authentication conventions. No source is
copied from reference repositories. The repository remains MIT licensed.

## Cost and limits

Effect recovery adds one small indexed row per reserved effect and one indexed
lease read/write per recovery command. Candidate listing is bounded by a
server-side limit. It performs no remote calls and no model/tool work. The
latency and SQL count are reported in the completion note.

## Acceptance tests

- fresh and existing databases apply migrations 041 and 042 cleanly;
- only one active effect owner exists per workspace/effect;
- expiry creates a safe takeover with a higher fence;
- stale effect fences cannot mutate state;
- terminal operation status does not prevent valid effect recovery;
- duplicate recovery state commands append one transition;
- concurrent claims preserve one owner and workspace isolation;
- crash hooks leave the lease/state unchanged before commit;
- replay and effect history remain deterministic;
- recovery listings never disclose raw payloads, credentials, or other
  workspace data.
