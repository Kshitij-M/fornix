# Domain-neutral harness foundation

Status: implemented in the initial Issue #41 change and re-audited on
`feat/issue-45-connector-capability-registry`; this note is the design and
qualification record for issue #41.

## Why this boundary exists

Fornix started with repository maintenance as its first product-shaped
workflow. That remains an important adapter, but it is not the limit of the
control plane. Production systems also contain databases, ticketing systems,
cloud resources, deployment platforms, business records, and internal APIs.
They need the same properties that make repository work safe: an explicit
scope, a known capability, bounded execution, durable evidence, approval for
risky effects, recovery after interruption, and a replayable record of what
was attempted and verified.

This slice establishes the vocabulary for that broader surface without
pretending that a generic executor already exists. Repository ingestion,
retrieval, changes, and validation remain authoritative in their existing
tables and packages. They can be represented as a repository adapter through
the new contracts while future adapters add other systems without changing
the control-plane authority boundary.

## Scope and non-goals

This slice adds versioned Go contracts, normalization, validation, canonical
hashing, and compatibility links for generic operations. It does not add a
generic executor, a plugin loader, a new persistence schema, an external
connector, a broker, a model, or a sandbox. There is deliberately no migration
because these values are not yet durable first-class operation rows; a future
operation store must be designed only after an adapter proves the lifecycle.

## Contracts

The contracts are split by responsibility:

| Contract family | Purpose |
| --- | --- |
| `SystemRef`, `ResourceRef` | Identify an external system and a typed resource within it. |
| `ConnectorRef` | Identify the named, versioned adapter boundary. |
| `CapabilityRef`, `CapabilityDefinition` | Describe one registered operation and its input/output schema hashes, effect class, profile, and evidence requirements. |
| `EffectClass`, `ExternalEffect` | Make side-effect risk and at-least-once external execution explicit. |
| `ExecutionProfile` | Carry hard step, concurrency, timeout, byte, token, cost, disclosure, retry, and cancellation limits. |
| `EvidenceRequirement` | State the proof and integrity requirements for an operation. |
| `OperationRequest` | Bind authenticated intent to one capability and one target resource. |
| `OperationPlan`, `OperationStep` | Describe a bounded deterministic DAG without embedding executable code. |
| `OperationResult`, `OperationFailure` | Report bounded hashes, evidence links, effect status, and stable failure categories. |
| `OperationReference` | Link a receipt to a generic operation without copying its payload. |
| `OperationAdapter` | Validate typed intent and produce a bounded plan without executing or loading arbitrary code. |

The operation contracts carry hashes and typed references rather than arbitrary
prompt or parameter maps. A transport adapter may accept JSON at its edge, but
only validated, canonical input represented by an input hash and schema version
may cross this foundation toward execution. This keeps unvalidated JSON from
becoming an implicit executor interface.

The `OperationAdapter` interface is intentionally small. It exposes the
immutable capability definition, deterministic request validation, and plan
construction. `ValidateOperationRequest` verifies the definition hash, enabled
state, supported resource kind, and profile tightening before an adapter is
allowed to hand a plan to a future runtime. It does not run a connector or
resolve a credential, so adding this seam does not create a hidden executor.

## Invariants

1. Workspace scope is explicit on every resource-bearing reference and every
   operation. Nested system, resource, capability, task, session, actor,
   evidence, and effect references must either be in the request workspace or
   be rejected.
2. Actor attribution is explicit on operation requests and results. Actor
   references are identity-only and cannot carry credentials, prompts, or raw
   output. Static system, resource, connector, and capability definitions are
   reusable workspace identities; the authenticated actor is attached at the
   operation, result, event, and receipt boundary rather than duplicated into
   every immutable reference.
3. System, resource, connector, capability, contract, and schema versions are
   explicit and bounded. Unsupported schema versions fail closed.
4. Canonical hashes include logical scope, actor, capability, target, typed
   input hash, plan, and execution profile. They exclude request IDs,
   idempotency keys, causation/correlation delivery identity, generated
   database IDs, and wall-clock timestamps.
5. Unknown capability names, unknown effect classes, invalid resource kinds,
   and unsupported versions fail closed. A future registry must perform a
   deterministic lookup before admission.
6. Effects are never represented as exactly-once. External operations are
   explicitly at-least-once or unknown, with provider idempotency,
   verification, and compensation status recorded separately.
7. Plans are bounded, have unique step IDs and ordinals, and form an acyclic
   dependency graph. Step order is canonicalized before hashing.
8. Metadata is bounded and rejects keys or values that could smuggle prompts,
   credentials, tokens, authorization material, or arbitrary unbounded text.
9. Normalization is deterministic and does not perform I/O, model calls,
   connector calls, or database work.
10. Work Receipt compatibility is additive. When generic operation fields are
    absent, the old receipt logical and request payloads are byte-for-byte
    unchanged, so existing receipt hashes remain stable.

## Repository adapter mapping

The existing repository path remains the first adapter:

| Generic concept | Repository-first representation |
| --- | --- |
| System | A workspace-scoped `repository` system instance. |
| Resource | Repository, mounted source, file, chunk, symbol, or change packet with its existing source/content hash. |
| Connector | The versioned repository ingestion, retrieval, change, or validation adapter. |
| Capability | `repository.ingest`, `repository.retrieve`, `repository.propose_change`, `repository.validate`, or a later registered version. |
| Input | Existing typed request normalized by its package; the generic request carries only its canonical input hash and schema version. |
| Evidence | Existing evidence, provenance, artifact, validation, and Work Receipt references. |
| Effect | Read-only retrieval, observation, approval-gated reversible write, or the appropriate explicit external effect class. |

No existing repository table is renamed or replaced. Existing source manifest,
path, chunk, symbol, change, validation, artifact, evidence, and receipt
details remain available through their current APIs.

## Hash and normalization design

All new contracts expose stable hashes over normalized logical payloads. Hash
inputs are bounded before normalization and are serialized from Go structs with
fixed field names; maps are limited to safe string metadata and are serialized
by `encoding/json`'s deterministic map-key ordering. Slices whose order is
semantic retain that order. Sets such as tags, supported resource kinds,
dependencies, and evidence requirements are normalized and sorted using
stable keys.

Hashes are identity-independent where retries should compare the same logical
operation. `RequestID`, `IdempotencyKey`, causation/correlation IDs, operation
delivery IDs, timestamps, and database identity are therefore excluded. The
workspace, actor, capability, target, typed input hash, plan, and profile are
included so an operation cannot be replayed across a scope or under a
different authorization subject without changing its content hash.

## Effects, retries, and recovery

`EffectClassUnknown` and unknown external-effect statuses are admission-safe
failure values, not permissions. A caller must explicitly classify a
capability. `ExternalEffect` records the boundary, idempotency key, provider
support, verification state, and compensation state. It can state at-least-
once delivery; it cannot make an exactly-once claim. Durable execution,
provider idempotency, verification, and compensation remain adapter concerns.

The profile provides bounded retry and cancellation limits, but this slice does
not implement retry scheduling or cancellation. Future adapters must persist
their transitions through the existing event/checkpoint/task fencing
substrates and must fail closed for stale ownership.

## Reuse and licensing

The design was compared with the local Orloj resource/runtime contracts,
DeepSeek Harness provider/plugin seams, MCP capability/resource boundaries,
CloudEvents envelope conventions, ClawMem retrieval/graph gates,
agentmemory action/checkpoint/replay patterns, and FornixDB provenance and
disclosure tiers. We independently reimplement the applicable ideas as small
Fornix contracts; no third-party source is copied.

Orloj and agentmemory are Apache-2.0 references; ClawMem and FornixDB are
MIT references. Kronaxis Fabric is BSL 1.1 and is not a source-reuse input.
Fornix remains MIT-licensed. This contract-only slice adds no third-party
license obligation. Any future copied code must be reviewed separately and
retain its required notices.

## Cost and performance budget

This slice has no database work, network calls, model calls, connector calls,
or new storage. Contract normalization is O(n log n) for bounded lists and
O(n) for scalar fields and metadata. The maximum plan is 64 steps, with bounded
metadata, references, dependencies, and schema/hash strings. Expected cost is
one in-process normalization and SHA-256 pass per contract; actual latency is
qualified by package benchmarks and is not represented as a production SLO.

The new optional receipt fields are omitted when empty. This keeps legacy
receipt canonical JSON and hashes unchanged for receipts that do not use the
generic operation reference.

## Acceptance tests

The contract tests must prove:

- operation, resource, connector, and capability hashes are stable after
  normalization and are independent of delivery identity and timestamps;
- normalization trims and sorts bounded values deterministically;
- unknown capability, effect, resource, and schema values fail closed;
- nested cross-workspace references fail closed;
- repository and non-repository examples validate and round-trip through JSON;
- operation plans reject duplicate IDs, missing dependencies, cycles, and
  bounds violations;
- exactly-once claims are rejected and at-least-once semantics remain explicit;
- metadata rejects secret-like keys, raw prompts, credentials, and arbitrary
  unbounded values;
- unsupported schema versions fail closed;
- adding no generic fields to an existing Work Receipt preserves its previous
  stable and request hashes;
- adding a generic operation reference changes the receipt hash and remains
  linked to the same workspace and operation hash.

## Remaining limitations

The repository adapter is not yet migrated to emit generic operations
automatically, and no generic operation rows are durable. The contract-only
stage did not include a connector registry; the current branch adds that
process-local admission boundary in `docs/67-connector-capability-foundation.md`.
Generic execution, approval admission, external-effect verification, connector
credentials, and adapter-specific recovery are intentionally deferred to later
issues.

## Issue #41 audit and requalification

The contract boundary was reviewed again after the connector/capability work
was added. The audit found no missing top-level contract family, but it found
several fail-closed and hash-integrity gaps that are now corrected on the
current branch:

- task and session entity references now use the same bounded identifier and
  normalized-kind rules as operation identities;
- actor identity is required and normalized on Work Receipt finalization;
- metadata keys and values are canonicalized before hashing, and normalized
  key collisions are rejected rather than silently choosing one value;
- set-like string references reject empty or malformed entries instead of
  dropping them;
- input/output schema versions have an explicit upper bound;
- operation result steps are sorted by step identity before hashing;
- supplied Work Receipt canonical, request, and verification hashes are
  recomputed and must match rather than being accepted as arbitrary valid
  SHA-256 strings.

These are contract-tightening changes only. No migration or new dependency is
introduced, and valid previously persisted receipts remain compatible. Invalid
or ambiguous values that were previously accepted now fail closed before they
can become an operation or receipt authority. The implementation remains an
independent MIT-licensed reimplementation of the patterns documented in the
reference matrix; no source code was copied and no Kronaxis BSL-1.1 code was
used.

Local requalification on 2026-09-11 used Go 1.25.13 on an Apple M4 Pro:

- `go test ./...`, `go vet ./...`, and `go test -race ./...` passed;
- contract canonical hashing measured approximately 4.5 microseconds/op for
  both request and plan hashes in the local benchmark;
- the complete non-OpenAI Fornix smoke suite and Docker reference workflow
  passed;
- the contract slice performs no database, network, model, connector, or
  storage work. The remaining cost is bounded in-process normalization and
  SHA-256 hashing.

Issue #41 remains intentionally limited to typed contracts and adapter
boundaries. Durable generic operation identity, duplicate suppression,
authorization records, fencing, crash recovery, and external-effect
verification belong to the operation authority and policy issues that follow.
