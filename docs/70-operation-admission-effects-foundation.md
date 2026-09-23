# Universal operation admission and external-effect foundation

Status: alpha implementation note for Issue [#46](https://github.com/Kshitij-M/fornix/issues/46); migration 036 and the pure/store test slices are implemented on the universal transformation branch.

This slice makes policy admission and external effects explicit at the generic
operation boundary. It does not turn policy into an executable programming
language, and it does not make remote execution exactly once. The decision is
deterministic, durable, workspace-scoped, and replayable; the connector still
owns domain validation and the external system still owns the final side
effect.

## Why this layer exists

The operation authority can now identify, fence, retry, and replay generic
work. Without a shared admission boundary, an adapter could still treat an
unknown capability as safe, bypass approval, use a revoked credential, or
leave an acknowledged remote effect indistinguishable from an unattempted
one. That would make the universal language only cosmetic.

The admission boundary answers, before execution:

- which actor and workspace requested the operation;
- which immutable policy revision and capability definition were used;
- which resource, effect class, credential references, evidence, and budget
  were evaluated;
- whether the operation is allowed, awaiting approval, denied, or abstained;
- which deterministic reason and input hash explain the decision.

The external-effect boundary answers, after reservation:

- whether delivery was reserved, dispatched, acknowledged, or left uncertain;
- whether the provider supplied an idempotency key or request identifier;
- whether the result was verified and whether compensation is available;
- whether a crash requires recovery rather than an unsafe retry.

## Invariants

1. **Unknown is unsafe.** Unknown effect classes, missing policy identity,
   unavailable connectors, missing evidence, invalid credentials, stale
   fences, contradictory scope, and exceeded quotas fail closed.
2. **Read defaults are narrow.** Read-only and observation capabilities may be
   automatic only when their resource, connector, actor, evidence, credential,
   health, and budget facts are valid. They cannot silently escalate to a
   write.
3. **Writes are explicit.** Reversible writes require the selected policy to
   allow automatic execution; irreversible writes and external communication
   require a durable approval. A caller cannot weaken a mandatory rule.
4. **Approval is exact.** An approval is bound to workspace, operation,
   operation hash, capability definition hash, target hash, input hash, policy
   hash, effect class, actor scope, and bounded budget. It cannot be moved to a
   different operation, revision, resource, or worker.
5. **Decisions are immutable.** An admission decision and policy snapshot are
   append-only facts. A policy update creates a new revision; it does not
   rewrite old decisions.
6. **Effects are at-least-once.** Fornix reserves an external boundary before
   the connector is called. Dispatch, acknowledgement, verification,
   compensation, and recovery-required outcomes are separate durable states.
   Fornix never claims exactly-once remote execution.
7. **Fencing is live.** An operation and task lease must be current at effect
   reservation and effect-state mutation. A stale worker cannot execute or
   finalize a remote effect.
8. **Secrets stay outside the decision.** Policy input contains credential
   references, status, and hashes only. Credential values, prompts, headers,
   tokens, and provider payloads never enter events, approvals, metrics,
   receipts, or this schema.
9. **Replay is inert.** Replay can recompute and verify decision hashes and
   effect history but never invokes a connector, model, tool, callback,
   network, or compensation handler.
10. **Quota accounting is conservative.** A pending or allowed decision
    reserves bounded admission usage in its workspace/actor window. A failed
    external call is not silently erased from accounting.

## Schema and transactional boundary

Migration `036_operation_admission_effects.sql` adds:

- immutable `operation_admission_decisions` with policy/capability/effect
  identity, redacted input hashes, decision status, reason, actor, and quota
  reservation;
- current approval state plus append-only approval history;
- append-only external-effect state transitions for reserved, dispatched,
  acknowledged, verification, compensation, and recovery outcomes;
- current effect state for bounded lookup and crash recovery.

The new store APIs normalize contracts, lock the operation and its live lease,
apply idempotency, enforce workspace/operation hashes, append the corresponding
event, and commit the decision or state transition in one Postgres transaction.
Duplicate commands return the original durable result. A conflict with the
same idempotency key fails closed.

The generic effect reservation in migration 035 remains the immutable boundary
record. Migration 036 adds state history around it rather than updating or
deleting authoritative effect rows.

## Reuse and licensing

The implementation reuses Fornix capability effect classes, credential
references, actor/workspace contracts, `EventStore.AppendTx`, operation leases,
task-fence validation, and the existing tool/change approval vocabulary. The
design was informed by Orloj governance and approval reconciliation, DeepSeek
Harness permission/approval seams, agentmemory leases and recovery records,
Goose permission decisions, and Temporal/River durable workflow boundaries.
These are independent implementations of patterns; no reference source is
copied. Kronaxis-fabric remains excluded because its BSL 1.1 license is not
compatible with the MIT-licensed Fornix distribution.

OPA, OpenFGA, OpenBao, and cloud policy/secret systems are future adapters,
not authorities in this slice. Postgres remains the authority for the durable
decision and effect record.

## Cost and storage budget

Each admission writes one bounded decision and one event. Approval writes one
current row plus one history row per decision. Each external state change
writes one append-only history row and updates one small current projection.
All policy, credential, target, and payload fields are bounded and hash-based;
raw requests and secrets are not stored. Admission is O(1) apart from the
bounded workspace/actor quota-window count. Effect updates are O(1). Replay is
bounded by a caller-supplied history limit and performs no external work.

The durable-write cost is intentional: a crash must distinguish “not
dispatched” from “possibly acknowledged.” Partitioning, archival, quota
compaction, high availability, and operational backpressure remain Issue #40
qualification work.

## Acceptance tests

- identical policy revision and input produce identical decision and audit
  hashes;
- read-only and observation operations admit automatically when all facts are
  valid;
- reversible, irreversible, and communication effects follow default approval
  rules;
- unknown effects, missing evidence, unavailable connectors, invalid or
  revoked credentials, stale fences, cross-workspace references, and quota
  overflow fail closed;
- duplicate admission and approval commands produce one durable effect;
- an approval cannot be reused for a different operation, input, policy,
  capability, resource, workspace, or actor;
- effect reservation is idempotent and stale workers cannot update effect
  state;
- crashes before commit leave no decision/approval/effect history;
- dispatch followed by a crash yields an explicit uncertain/recovery state and
  never silently retries;
- verification failure and compensation remain auditable;
- replay is deterministic and invokes no external system;
- all existing tests, race checks, documentation checks, builds, and smokes
  remain green.

## Remaining limitations

This slice does not provide a policy scripting language, organization-wide
policy distribution, OS-level egress enforcement, a secret manager, signed
third-party callbacks, a durable connector registry, or remote exactly-once
execution. Connector-specific authorization and credential-state resolution
must be supplied by the authoritative identity/registry boundaries until the
connector qualification and universal production issues are complete.
