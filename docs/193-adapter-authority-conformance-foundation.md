# Task 84 — Adapter authority-conformance foundation

Status: implementation note for the universal work-control-plane roadmap. This
document defines a qualification boundary; it is not a claim that every
provider, deployment, or external system is production-qualified.

## Problem and scope

Fornix now has a durable operation authority, effect reservation, task and
operation fencing, credential references, signed trust/schema facts, and
deployment-admission consumption. Those controls are only useful if every
effectful adapter that the server composes actually reaches them. A registry
can be locally correct while a model gateway, tool executor, embedding path, or
future business-domain adapter bypasses the generic dispatcher.

Task 84 adds a bounded, deterministic conformance inventory for both seams:

1. registered connector capabilities; and
2. server-composed dynamic effect adapters.

The inventory is a process-local qualification artifact. It contains stable
identifiers and missing-control codes only. It does not store prompts,
credentials, provider payloads, DSNs, arbitrary errors, or external responses.
Postgres remains the authority for operations, leases, reservations, policy,
credentials, deployment admission, and durable history.

## Conformance invariants

Every effectful connector capability must:

- implement `AuthorityAwareCapability`;
- implement `EffectDescriber`;
- advertise provider idempotency and verification support;
- use a stable, workspace-scoped definition hash; and
- expose an explicit external-effect class and boundary.

Every dynamic effect adapter must enter through `effectdispatch.Runtime`. The
runtime must therefore compose, in order:

```text
typed request
  -> operation identity and workspace normalization
  -> durable operation admission
  -> fenced operation lease
  -> task fence validation when task-bound
  -> deployment-admission reference validation when strict mode is enabled
  -> durable attempt and external-effect reservation
  -> live authority validation
  -> adapter invocation
  -> fenced result or explicit recovery-required state
```

The inventory reports these controls as required composition facts. It does
not pretend that static metadata can prove a remote provider's behavior; the
dispatcher and deployment-owned integration tests remain the runtime proof.

Read-only and observation capabilities do not cross the external-effect
reservation boundary. They still require workspace, identity, schema, and
policy admission, but do not require the effectful adapter envelope.

## Fail-closed and crash semantics

- A missing required interface or composition fact makes the strict inventory
  fail; it never downgrades an effectful adapter to ordinary `Execute`.
- The inventory covers every registered workspace, not only the default
  workspace. A bounded workspace limit prevents an unbounded startup scan.
- A strict production server refuses to start when any effectful connector or
  dynamic adapter is not qualified, or when a required deployment-admission
  reference cannot be supplied.
- A process crash before reservation leaves no external dispatch attempt.
- A crash after reservation but before dispatch leaves a replayable durable
  reservation.
- A crash after dispatch with an uncertain outcome remains recovery-required;
  conformance does not authorize a blind retry.
- Duplicate requests use the existing workspace-scoped operation and effect
  identities and must not create a second local reservation.
- Stale operation, task, credential, trust, schema, or deployment fences fail
  closed before dispatch and again before finalization.

## Schema and API decision

No migration is required. The inventory is a bounded, hash-only process
qualification report and reuses existing operation, admission, effect,
credential, catalog, deployment-evidence, and domain-link authorities. A
future operator endpoint may persist the bounded report as an artifact; this
task does not introduce a second conformance table or mutable adapter registry.

The report has a schema version, workspace, strictness, sorted entries,
bounded missing-control codes, readiness, and a stable report hash. Timing is
diagnostic only and is excluded from the stable hash. Report errors are
normalized to codes and never echo provider or credential material.

## Reuse, licensing, and security

This slice reuses the existing connector registry, `AuthorityAwareCapability`,
`EffectDescriber`, effect dispatcher, operation store, deployment admission
reference, domain-effect links, and qualification-report conventions. It
learns from Orloj's explicit provider boundary, ClawMem's bounded replay and
abstention discipline, and agentmemory's diagnostic lifecycle without copying
source. Kronaxis Fabric source is not copied because its repository is BSL
1.1. Fornix remains MIT licensed.

The inventory never accepts credentials, secrets, raw prompts, headers,
provider payloads, arbitrary user text, or executable commands. It is not a
sandbox and does not grant authority.

## Cost and storage budget

The default check is in-memory and read-only. It adds no table, broker,
service, cache, provider call, or persistent payload. Registration inspection
is O(C log C) per workspace for C capabilities; dynamic adapter inventory is
O(A log A) for A declared compositions. Workspace enumeration is bounded by
the existing operator pagination limit and a configurable maximum. Deployment
admission and effect reservation retain their existing bounded Postgres work;
the conformance inventory does not add a transaction in the external-effect
path.

## Acceptance tests

- an effectful capability missing authority execution fails the inventory;
- an effectful capability missing an effect descriptor fails the inventory;
- missing idempotency, verification, workspace, definition, or effect facts
  fail closed;
- read-only and observation capabilities remain compatible;
- all registered workspaces are included in deterministic order;
- identical inventories have identical hashes despite map insertion order;
- report output contains no secret or arbitrary payload;
- dynamic adapter manifests cover model, embedding, tool, and change paths;
- strict runtime requests without a deployment-admission reference fail before
  reservation, while valid references are revalidated by Postgres;
- duplicate, stale-fence, crash, and cross-workspace dispatcher tests remain
  green; and
- the offline qualification command performs no provider, tool, broker, or
  database side effect.

The local dry-run report is available without a database or provider:

```sh
make qualification-effect-conformance
# or
fornix qualification adapter-conformance
```

It prints the sorted dynamic adapter inventory and its stable manifest hash.
The server publishes that hash in the authenticated authority/readiness
status surface so operators can detect composition drift without receiving
adapter payloads or credentials.

## Explicit limitations

Static conformance cannot prove that a remote provider honors idempotency,
that a filesystem or network sandbox is strong, that a deployment artifact is
actually running, or that a credential manager is highly available. Those
facts require deployment-owned integration, failover, load, and provider
qualification evidence. Fornix continues to expose at-least-once and
recovery-required outcomes rather than claiming exactly-once remote execution.
