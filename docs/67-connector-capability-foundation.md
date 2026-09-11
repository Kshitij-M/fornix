# Connector and capability registry foundation

Status: implemented on `feat/issue-45-connector-capability-registry`; this
note is the design and qualification record for Issue #45.

## Why this slice exists

The domain-neutral contracts in Issue #41 make it possible to describe work
against repositories, databases, APIs, cloud resources, and business systems
without putting domain-specific payloads into the universal control plane.
Those contracts are not executable by themselves. Fornix now needs a narrow,
deterministic admission boundary that answers:

1. Is this connector and capability explicitly registered?
2. Does the immutable capability definition match the request exactly?
3. Does the request target a supported, same-workspace resource?
4. Are the typed adapter checks, declared budgets, evidence obligations,
   credential references, approval requirements, and health state satisfied?
5. Can the capability produce a bounded plan and hash-only result without
   bypassing the future operation authority?

The registry is therefore a capability catalog and a fail-closed admission
seam. It is not a dynamic plugin loader, workflow engine, authorization
database, credential store, or claim of exactly-once external execution.

## Scope and non-goals

This slice adds:

- an explicit in-process connector registry;
- immutable, versioned capability discovery and exact definition lookup;
- deterministic admission and typed adapter validation;
- bounded execution through a narrow adapter-owned executor seam;
- connector health and unavailable-state reporting;
- a shared conformance suite for built-in adapters;
- the first repository inspection adapter, implemented as a read-only,
  hash-only capability.

It does not add a migration or durable connector catalog. PostgreSQL remains
the authority for operation state, events, evidence, artifacts, and future
idempotency records. Durable generic operation persistence is Issue #39.
Universal policy, approval, credential authorization, and external-effect
records are Issue #46. Bounded HTTP/API and SQL adapters are Issue #43.

## Invariants

### Registration

- Connector identity is `(workspace, name, version)` and capability identity
  is `(workspace, connector, capability name, capability version)`.
- Names and versions are normalized before comparison. Ambiguous duplicate
  identities are rejected; registration order never changes lookup results.
- A connector's capabilities must reference the exact connector identity and
  workspace that registered them.
- Capability definitions are normalized and their content hash is immutable.
- Registration is explicit process configuration. Models and operation input
  cannot create or replace capabilities at runtime.
- Lookup returns defensive definitions and requires the registered definition
  hash for execution.

### Admission and execution

- Admission validates the operation request, workspace, actor, resource kind,
  capability definition hash, input schema version/type, execution profile,
  declared effect, and adapter-specific typed input. Definitions carry bounded
  timeout, input/output byte and token, row, rate-per-minute, retry, and cost
  metadata; the request profile can tighten those limits.
- A disabled, unknown, unhealthy, unavailable, or schema-incompatible
  capability fails closed before execution.
- Required credentials are references only. The registry can require a caller
  to prove that a named reference is available, but it never receives or
  stores secret values.
- Approval is an explicit admission input when the capability declares it;
  the registry never treats model output as approval.
- The adapter owns domain semantics and returns only bounded, hash-based
  operation results and evidence references. Raw prompts, credentials,
  arbitrary parameter maps, and connector implementation details do not enter
  universal contracts.
- Execution is at-least-once at an external boundary. The registry does not
  claim exactly-once and does not provide durable duplicate suppression; the
  operation authority in Issue #39 will own that record.
- The process-local executor enforces the request timeout and retry ceiling.
  It cannot infer row, token, byte, rate, or monetary usage from a hash-only
  adapter result, so those measurements remain explicit inputs to the durable
  operation and accounting authorities rather than being guessed here.
- Context cancellation and capability timeout are enforced at the runtime
  boundary. An adapter must still honor the context cooperatively.

### Workspace and safety

- Connector, capability, resource, evidence, credential, actor, and result
  references must share the request workspace.
- Unknown effect classes, unsupported resources, missing evidence metadata,
  missing credential availability, stale definition hashes, and unavailable
  connectors fail closed.
- Read-only repository inspection produces no filesystem mutation. It does
  not turn the registry into a shell, URL fetcher, or arbitrary code runner.

## Contracts and package boundaries

`internal/connector` owns the registry, admission options, bounded execution
wrapper, and conformance cases. `internal/contracts` remains the universal
wire/hash boundary. `internal/adapters/repository` owns repository-specific
resource validation and planning. The registry depends on the adapter
interface; adapters do not import the registry, preventing a capability from
silently acquiring control-plane authority.

The adapter receives a normalized `OperationRequest` whose input is already
represented by a bounded hash and schema identity. A future typed adapter may
validate an input document before constructing that request; the universal
plane never accepts `map[string]any` as executable input.

## Reference scan and reuse decisions

Repositories/files searched:

- Orloj `runtime/model_provider_registry.go`, `runtime/tool_contract.go`,
  `runtime/tool_runtime_conformance_test.go`, `resources/resource_types.go`,
  and governed tool runtime tests;
- DeepSeek Harness capability-seam and plugin architecture notes under
  `.agents/notes/implemented/architecture/`, plus structured tool snapshots;
- agentmemory `src/mcp/tools-registry.ts`, action/lease/checkpoint tests, and
  diagnostic hooks;
- ClawMem retrieval-gate and evaluation conformance tests;
- FornixDB bounded disclosure, integrity, and concurrency tests;
- existing Fornix `internal/tool`, `internal/model`, `internal/validation`,
  and Issue #41 domain-neutral contracts.

Closest implementations:

- Orloj provides the strongest explicit registry, alias-collision, contract,
  timeout, retry, and conformance patterns.
- DeepSeek Harness provides the useful separation between capability ownership
  and client presentation, but its plugin surface is broader than this
  control-plane slice.
- agentmemory contributes lifecycle diagnostics and duplicate/lease test
  discipline, not an authority model suitable for Fornix's Postgres plane.
- ClawMem and FornixDB reinforce fail-closed evidence gates, bounded output,
  and deterministic evaluation rather than registry code.

Behavior reimplemented rather than copied:

- stable registration and collision detection;
- exact versioned lookup;
- adapter-owned validation and execution;
- bounded cancellation/timeout;
- reusable conformance cases.

No source code was copied. No Kronaxis source was used because that project is
BSL 1.1. Orloj and agentmemory are Apache-2.0; ClawMem and FornixDB are MIT.
Fornix's implementation is independent and does not introduce a third-party
runtime dependency or notice obligation. The repository remains MIT-licensed.

## Schema and migration decision

No migration is included. Registry entries are process configuration until
Issue #39 defines durable generic operation and connector authority. This
avoids making an in-memory registry appear authoritative or creating a second
source of truth. Existing repository, tool, model, event, evidence, artifact,
and validation tables remain unchanged and compatible.

## Cost and performance budget

Registry lookup and admission are in-process, read-only operations. The
targeted budget is:

- O(1) exact lookup under a read lock;
- stable listing sorted by identity, with no network or database work;
- one adapter validation and one plan construction per admitted execution;
- no model calls, embeddings, broker traffic, or credential resolution;
- bounded plan size using the existing execution profile and operation-plan
  limits.

The qualification record will report package test time and benchmark lookup,
admission, and plan construction. Database/storage impact is zero in this
slice. External connector latency, retries, and durable idempotency are
measured only after their owning issues are implemented.

## Acceptance tests

The registry and repository adapter must prove:

- registration and exact lookup are deterministic;
- duplicate connector/capability identities and connector mismatches fail;
- list order and definition hashes are stable across registration order;
- unknown, disabled, unsupported-resource, cross-workspace, stale-definition,
  invalid-schema, missing-credential, missing-approval, and unavailable-health
  requests fail before execution;
- execution honors cancellation and capability timeout;
- capability results are normalized, bounded, workspace-scoped, and stable;
- repository inspection is read-only and emits only hash/evidence references;
- concurrent registration and lookup preserve integrity;
- conformance cases cover success, duplicate delivery semantics, failure
  classification, cancellation, timeout, bounds, redaction, evidence, and
  replay/no-external-effect behavior;
- existing unit tests, race tests, CI, and all Fornix smokes remain green.

## Remaining limitations

The registry is intentionally process-local and does not survive a restart.
It cannot by itself prove durable operation idempotency, task fencing, RBAC,
approval storage, connector signing, secret-manager access, or external
effect verification. Those guarantees must be added by Issues #39, #40, #43,
and #46 before third-party connectors are described as production-ready.

## Qualification results

The Issue #45 branch was qualified locally on 2026-09-11 with Go 1.25.13 on an
Apple M4 Pro:

- `make check` passed, including the full unit suite, vet, Python helpers,
  documentation validation, and package checks.
- `make test-race` passed across every Go package.
- `go test ./internal/connector -run '^$' -bench 'BenchmarkRegistry'
  -benchmem -count=1` measured exact lookup at approximately 620 ns/op with
  3 allocations and admission plus plan construction at approximately 133
  microseconds/op with 241 allocations. These are local measurements, not
  service-level SLOs.
- The connector conformance suite covers exact and identity lookup, health,
  workspace and definition fences, redacted contracts, cancellation,
  retryability, duplicate read replay, output schema integrity, and evidence
  boundaries.
- Database writes, migrations, storage growth, network calls, and model calls
  are zero for this slice. The registry's runtime memory is bounded by the
  explicitly registered connector and capability definitions; durable catalog
  and operation storage are intentionally deferred to Issue #39.
