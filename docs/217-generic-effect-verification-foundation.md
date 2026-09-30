# Generic effect verification and reconciliation foundation

Status: implemented as Task 96 on the current universal-transformation branch.

## Problem

Fornix can now reserve and dispatch a domain-neutral external effect, and a
generic workflow can wait at `awaiting_external` or `recovery_required`. The
remaining safety boundary is proving what happened after the external system
was contacted. A client must not be able to submit an arbitrary “success”
state, and a recovery worker must not race another worker into a different
outcome.

This slice adds a verifier seam and composes it with the existing effect lease,
append-only effect transition, domain-effect-link, workflow fence, and Work
Receipt authorities. It does not add a second effect ledger or claim exactly
once external execution.

## Non-goals

- No provider-specific database, queue, broker, object store, or new service.
- No raw provider response, credential, prompt, customer text, or payload in
  verification requests or durable transitions.
- No client-supplied success state without a registered verifier proof.
- No retry of an external invocation from the verifier path.
- No exactly-once claim: external execution remains at-least-once.
- No live provider qualification; fake-domain verifiers are deterministic test
  adapters only.

## Reuse and authority boundary

The verifier composes the existing authorities in this order:

```text
authenticated actor/workspace
  → workflow lease and task fence
  → effect recovery lease and fence
  → registered capability verifier
  → append-only operation_effect transition
  → append-only domain-effect-link transition
  → fenced workflow checkpoint/resume
  → replay-verified Work Receipt
```

The verifier receives only normalized operation/effect/link hashes and bounded
provider identifiers. The adapter owns provider-specific observation. The
effect dispatcher owns local state transitions and link composition. The
workflow store owns the final step checkpoint.

## Invariants

- Verification is workspace-scoped and binds the operation, step, effect,
  domain-link, operation hash, and expected effect/link versions.
- The caller must hold the current effect recovery lease. A stale, expired,
  released, or cross-workspace fence fails closed before local reconciliation.
- A workflow-bound verification also requires the current workflow operation
  fence and task fence before the step can resume.
- A verified result requires both a result hash and a verification hash. A
  mismatch is a deterministic `verification_failed` outcome; an unprovable
  outcome is `recovery_required`.
- The effect transition and corresponding domain-link transition commit in one
  Postgres transaction. The workflow checkpoint is a separate fenced
  transaction and may be safely retried after a crash.
- Repeating the same verification idempotency key returns the committed local
  outcome and never invokes the verifier twice after the durable final state is
  present.
- A verifier never mutates an immutable effect or link identity. It appends
  only bounded transitions and preserves all prior outcomes.
- Replay never invokes a verifier or any external system.

## State and crash semantics

The existing state machine is reused:

```text
acknowledged/recovery_required
  → verification_pending
  → verified | verification_failed | recovery_required
```

If a process crashes after the pending transition, a later fenced verifier
reuses the same request identity and completes the final transition. If it
crashes after effect/link commit but before workflow checkpoint commit, the
workflow still waits; retrying the verifier observes the durable outcome and
resumes the workflow once. If the verifier cannot prove the outcome, it never
promotes the effect to success.

## Fake-domain qualification behavior

The data-pipeline and customer-support fake capabilities share the verifier
interface but not schemas or capability names. They return deterministic
hash-only outcomes:

- ordinary bounded input → verified;
- a provider request identifier containing `mismatch` → failed;
- a provider request identifier containing `uncertain` → unknown/recovery;
- malformed or cross-workspace facts → fail closed.

These controls are test fixtures, not production provider behavior.

## Storage, latency, and cost budget

No migration is expected. Existing effect and link transition tables are the
authorities. A successful verification adds bounded transition rows and one
workflow checkpoint; a duplicate adds no new durable effect. The verifier
must not store raw external responses. Qualification should measure effect
lease acquisition, verification, local reconciliation transaction latency,
lock waits, WAL/row growth, duplicate suppression, and replay throughput on an
explicitly disposable PostgreSQL DSN.

There is no model-token, network, or provider cost in the offline fake path.
Live provider cost, latency, and availability remain adapter/deployment
qualification concerns.

## Licensing and reuse

This implementation reuses Fornix’s existing stores and interfaces and
independently applies patterns studied from Orloj, DeepSeek Harness, ClawMem,
agentmemory, and FornixDB. No reference source is copied. Kronaxis source is
not copied because its BSL 1.1 terms are incompatible with this MIT project.

## Acceptance tests

### Offline

- verification contracts reject missing identity, hashes, versions, and
  workspace mismatches;
- fake data-pipeline and customer-support verifiers produce stable verified,
  mismatch, and uncertain outcomes;
- repeated verifier inputs produce the same proof hashes;
- malformed outcomes cannot claim success; and
- verifier tests never contact a network, model, tool, broker, or database.

### PostgreSQL-backed

- only the holder of the current effect fence can reconcile;
- stale, expired, released, and cross-workspace verifiers fail closed;
- effect and domain-link transitions are atomic;
- duplicate verification produces one final effect/link transition;
- concurrent verifiers produce one winner;
- crash before final commit leaves both authorities unchanged;
- crash after final commit is safely replayable;
- verified outcomes resume a workflow once with no duplicate effect;
- mismatch and uncertain outcomes never produce a successful receipt; and
- replay from zero and checkpoint produce identical hashes.

### Qualification

- add bounded `Make`/CI coverage and an authenticated HTTP/CLI path;
- run all DB cases only with an explicitly supplied disposable DSN; and
- report latency, SQL work, WAL/storage growth, replay throughput, and the
  remaining live-adapter limitations.
