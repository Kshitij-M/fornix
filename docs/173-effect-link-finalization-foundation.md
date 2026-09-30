# Task 74B — Generic effect and domain-link outcome finalization

Status: implemented repository-owned mutation-boundary slice; not a
production-readiness declaration.

## Problem

Fornix's durable effect dispatcher already records generic effect transitions
and binds a specialized domain-effect link before crossing an external
boundary. The link can still remain at `linked` after a verified dispatch, or
remain linked when an uncertain provider outcome is recorded as
`recovery_required`. That weakens operator receipts and makes the generic
relationship less useful as a universal audit trail.

## Scope and invariants

1. Every dispatcher-created domain link is bound to the exact generic effect
   identity, reservation hash, operation fence, workspace, and request hash.
2. A verified/no-verification-required outcome advances `linked` to
   `reconciled` with the stable result hash. A provider outcome that may have
   crossed the external boundary advances `linked` to `recovery_required`
   without claiming a result hash.
3. Link status changes are append-only, version-fenced, idempotent, and
   workspace-scoped. The immutable link identity is never overwritten.
4. Generic effect state and its domain-link transition are committed in one
   Postgres transaction for the terminal/recovery publication. The external
   call remains outside that transaction and explicitly at-least-once.
5. Duplicate dispatch never invokes the adapter again. It returns the durable
   effect state and current domain-link status.
6. Verification-pending effects remain linked until an explicit verifier or
   reconciler supplies proof; the dispatcher does not invent verification.
7. A stale operation/effect authority or workspace mismatch fails closed before
   the terminal transition.
8. When the caller requests an operation result, the result record, operation
   transition, authority link, terminal effect transition, and domain-link
   transition commit in the same transaction. A legacy terminal effect missing
   its requested result fails closed; duplicate delivery never repeats the
   external call to manufacture one.

## Reuse, licensing, and cost

This slice reuses `AdmissionStore.UpdateEffectTx`,
`DomainEffectLinkStore.TransitionTx`, the existing transition version fence,
and dispatcher reservation identity. It is informed by Orloj's explicit
provider outcome/recovery boundary and ClawMem's replay/abstention discipline;
no reference source is copied. The project remains MIT-licensed and excludes
Kronaxis BSL 1.1 source.

The change adds no tables or services. It uses one short transaction for the
terminal effect/link/result composition, with existing indexes and one
bounded transition row per authority. No provider call, payload, credential,
or duplicate artifact is introduced.

## Acceptance tests

- Successful effect dispatch produces one `reconciled` domain-link transition
  with a stable result hash.
- Uncertain dispatch produces `recovery_required` in both generic effect and
  domain link; the recovery path is idempotent.
- Duplicate dispatch returns terminal effect/link state and invokes the
  adapter zero additional times.
- Verification-required dispatch leaves the link linked and the effect
  verification-pending until a verifier acts.
- Concurrent/stale finalization cannot move a link backwards or create two
  transitions for one idempotency key.
- Cross-workspace links and effect updates fail closed.
- Existing unit, race, migration, smoke, and read-only qualification checks
  remain green.

## Explicit limitation

The dispatcher still reserves generic attempts/effects and binds the initial
link through separate transactions because the full cross-domain admission
composition requires a broader caller-owned transaction API. This slice makes
the terminal/recovery publication atomic and does not claim atomic creation of
every specialized ledger row with its generic reservation.
