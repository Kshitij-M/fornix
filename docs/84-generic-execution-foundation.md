# Universal durable connector execution foundation

Status: implementation note for the next Issue [#38](https://github.com/Kshitij-M/fornix/issues/38) and [#40](https://github.com/Kshitij-M/fornix/issues/40) qualification slice.

Fornix already has two important halves of a universal work control plane:
typed connector/capability registration and a Postgres operation authority.
Until now, they were connected only in tests. An operator could create an
operation, lease it, transition it, and replay it, but the public control
surface could not execute a registered capability and durably attach the
result.

This slice connects those halves for deterministic read-only and observation
capabilities. It intentionally does not make external writes safe by
assumption. A capability that declares an external effect must first use the
durable admission and effect-reservation path; the generic endpoint rejects
such execution until that reservation exists.

## Invariants

1. Execution is workspace-scoped. The authenticated principal, operation
   request, capability definition, target, plan, result, evidence references,
   and durable result row must all name the same workspace.
2. Execution requires an active Postgres operation lease. A task-bound
   operation also requires the live task owner and task fence; the operation's
   stored fence is only a snapshot.
3. Connector lookup is exact and trust-gated. Registration, connector version,
   capability identity, and definition hash must match the request. A runtime
   cannot execute an unknown, altered, disabled, or untrusted capability.
4. The authenticated actor is authoritative. The endpoint never accepts a
   replacement actor, owner, or workspace from the request body.
5. Plans are deterministic and bounded. The connector produces a normalized
   plan from the operation request; the plan is persisted before execution and
   its hash participates in the state history.
6. Results are hash/reference-only durable records. Raw connector output,
   credentials, prompts, and provider diagnostics do not enter the operation
   result table. Detailed evidence belongs in the existing evidence/artifact
   authorities.
7. Duplicate execution requests with the same operation and idempotency key
   return the committed result. A lost response cannot cause a second durable
   result effect.
8. A crash before the result transaction commits leaves the operation running
   without a committed result. Recovery marks the operation explicitly and
   never guesses that an external effect did or did not happen.
9. Only read-only and observation results are executable in this first slice.
   Capabilities that return or declare external effects fail closed unless a
   future effect reservation/execution API supplies the required proof.
10. Replay reads the recorded plan, result, transition chain, and hashes. It
    never invokes a connector, resolves credentials, performs filesystem work,
    or contacts a remote system.

## State and crash semantics

The lifecycle is:

```text
created → planned → admitted → running → succeeded
                                  └────→ failed / recovery_required
```

Plan persistence and the `created → planned` transition share one transaction.
Result insertion, per-step projection updates, and the terminal transition
share one transaction. If any failure occurs before commit, all of those
writes roll back. If the process fails after the connector returns but before
commit, the operation is recoverable but the external-effect boundary remains
explicitly unknown; this first slice avoids that ambiguity by refusing
effectful capabilities.

The operation lease is not released until the result transaction has committed.
Recovery workers must acquire a higher fencing token after expiry and must
re-read the operation state before retrying. A stale worker cannot attach a
result or move the state forward.

## Schema and API changes

Migration 040 adds one workspace-scoped, immutable `operation_results` row per
operation. It stores the normalized result contract and its stable hash. The
existing `operations.result_hash` remains the lifecycle projection and is
updated only in the same transaction as the result row and terminal event.

The authenticated HTTP surface adds:

```text
POST /v1/operations/{operation_id}/execute?workspace_id=...
```

The route requires `operation:execute`, uses the authenticated actor as the
lease owner, accepts only bounded execution options, and returns the durable
operation/result summary. It does not accept arbitrary connector payloads or
credentials. A future CLI command can call the same route without creating a
second execution semantic.

## Reuse and licensing

The implementation reuses the existing connector registry and trust policy,
connector executor/retry classifier, operation lease/fence store, typed
operation contracts, append-only event stream, and authorization middleware.
Orloj's execution engine and DeepSeek Harness's typed capability seams inform
the separation between plan, admission, execution, and result. agentmemory's
checkpoint/recovery patterns inform explicit recovery states. No reference
source is copied. Kronaxis Fabric remains excluded because its BSL 1.1 license
is incompatible with Fornix's MIT distribution.

## Cost and performance budget

The control-plane overhead is bounded by one operation read/lease transaction,
one plan transaction when a plan is missing, the connector's bounded execution
budget, and one result transaction. The result row is capped by the typed
contract and stores hashes/references rather than raw output. The endpoint must
report operation lease latency, connector latency, retry count, result storage
bytes, and SQL transaction time with bounded dimensions.

The local qualification target is no more than one extra Postgres round trip
for an already-planned execution and no unbounded result storage. This is a
regression target, not a production capacity claim.

## Acceptance tests

- a trusted fake read/observation capability executes through the authenticated
  HTTP boundary and produces one durable result;
- the operation plan is persisted and stable before execution;
- duplicate execution delivery returns the same result hash and does not add a
  second result row or terminal transition;
- missing, altered, disabled, untrusted, cross-workspace, or unauthorized
  capabilities fail closed;
- stale operation and task fences cannot attach a result;
- a crash before plan commit or result commit leaves no partial authoritative
  result;
- effectful capabilities are rejected without a durable effect reservation;
- result, evidence, and actor metadata are bounded and redacted;
- replay from zero and replay after a checkpoint remain deterministic and make
  no external calls;
- concurrent executors preserve one durable result and valid operation state;
- fresh and existing databases migrate cleanly; unit, race, smoke, CI, and
  documentation checks remain green.

## Remaining qualification gates

This slice is not a general background worker, multi-step connector scheduler,
external-effect executor, secret-manager implementation, central egress
proxy, backup/restore drill, HA deployment, or capacity qualification. Those
remain required before Fornix can claim production readiness for arbitrary
production systems.
