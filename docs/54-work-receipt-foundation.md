# Work Receipt and Verified Change Packet Foundation

Status: implemented on `feat/work-receipts`; awaiting CI and maintainer review
Audience: Fornix contributors, operators, reviewers, and adopters

## Why this exists

Fornix is intended to make long-running repository work safe to authorize,
resume, inspect, and verify. The current control plane records the ingredients
of a run—tasks, events, retrieval surfaces, model calls, tool runs, evidence,
artifacts, costs, and replay checkpoints—but an operator still has to assemble
those records manually to answer a basic question:

> What exactly did this run change or decide, under which policy and budget,
> using which evidence, and can another operator verify the result without
> re-running an external model or tool?

This feature adds the first-class, machine-verifiable answer: a **Work
Receipt**. A future user-facing **Verified Change Packet** will present the
same receipt with human-oriented summaries, evidence disclosure, and changed
file details. The receipt is the durable contract; the packet is a projection
of that contract.

The receipt is deliberately a derived verification record. It does not become
a second task, event, artifact, evidence, or cost authority, and it never
rewrites those records.

## Scope of this vertical slice

The implementation will provide:

- typed contracts for receipts, steps, evidence and artifact references, and
  verification results;
- migration `028_work_receipts.sql` with an immutable receipt row and bounded,
  queryable step/reference links;
- one transaction that validates authoritative references, writes the receipt,
  and writes all links;
- deterministic canonicalization and SHA-256 content hashes;
- workspace-scoped idempotent finalization;
- authenticated gist/detail/raw disclosure with hard byte, item, and token
  budgets;
- integration with the existing reference workflow, HTTP API, CLI, and MCP
  surface;
- replay and integrity tests that never call a remote model or external tool.

Cryptographic signatures, external notarization, object storage, a change
patch generator, general multi-agent graphs, and exactly-once remote execution
are intentionally out of scope for this slice.

## Invariants

### Authority and immutability

1. Existing task, event, agent-run, retrieval-surface, model-call, tool-run,
   evidence, artifact, validation, observation, cost, and replay records remain
   authoritative.
2. A receipt can only reference records that exist, are complete enough for
   the requested work kind, and belong to the same workspace.
3. A committed receipt, its canonical payload, steps, and reference links are
   append-only. Corrections are represented by a new receipt or a later
   verification record; history is never overwritten.
4. A receipt is not considered verified merely because it was requested. The
   verification result records resolved references, integrity checks, replay
   status, and any failure reason.

### Identity and idempotency

1. The natural receipt identity is `(workspace_id, work_kind, work_id)`.
2. A caller-provided idempotency key is scoped to the workspace. Repeating the
   same request returns the original receipt and produces no second effect.
3. Reusing either identity with a different canonical request hash fails closed
   with a conflict; it never silently changes the receipt.
4. The canonical receipt hash is derived from stable work identity, normalized
   steps, normalized references, verification inputs, and bounded accounting
   fields. Database IDs, insertion order, wall-clock timestamps, credentials,
   raw prompts, and arbitrary unbounded user text are excluded.
5. All collections are sorted by stable semantic keys before hashing. Hashes
   therefore remain stable across equivalent request ordering and replay.

### Fences and terminal state

1. A task-bound finalization request may include the task execution fence and
   owner identity. The store checks the current monotonic fence and the
   terminal task/run history before committing.
2. An expired or stale worker cannot finalize a receipt with an older fence.
3. A successful agent run or completed task is required for a completed receipt;
   awaiting, cancelled, failed, or dead-letter work can be recorded only by a
   future explicitly different receipt status.
4. The validation and receipt/link writes happen in the same Postgres
   transaction. A crash before commit leaves no receipt or partial link set.
5. A crash after commit is safe: a retry resolves the existing idempotent
   receipt and replay reads the committed snapshot without external effects.

### Evidence and disclosure

1. Every step that claims evidence or an artifact carries a source identity and
   content hash. Hash mismatches, missing records, stale records, contradictory
   workspace IDs, and malformed references fail closed.
2. Receipt summaries contain bounded metadata and hashes, not credentials,
   raw prompts, secret environment values, or unlimited user text.
3. Gist, detail, and raw disclosure are views over the same immutable canonical
   receipt. The returned receipt hash never changes between disclosure levels.
4. Disclosure enforces item, byte, and token budgets before returning data.
   Truncation is explicit and deterministic; an operator can request a larger
   bounded view only with authorization.
5. Workspace authorization is checked on every read and disclosure. A receipt
   ID alone is never sufficient to cross a workspace boundary.

### External effects and replay

Fornix records remote model calls and external tool execution as at-least-once
operations. Provider idempotency keys and tool-run idempotency reduce duplicate
work, but the receipt never claims exactly-once external execution. The
verification contract distinguishes measured effects, estimated usage, retry
work, duplicate work, and external at-least-once boundaries. Replay consumes
recorded inputs and outputs only; it never invokes a model, tool, broker, or
other external system.

## Contract design

The typed contracts live under `internal/contracts`:

- `WorkReceipt`: immutable workspace/work-item identity, schema version,
  canonical hash, bounded summary, steps, references, accounting, and
  verification.
- `WorkReceiptStep`: deterministic phase, status, source reference, timing and
  budget measurements, retries/duplicate-work markers, and external-effect
  boundary.
- `WorkReceiptEvidence` and `WorkReceiptArtifact`: typed source ID, workspace,
  content hash, role, and disclosure-safe summary metadata.
- `WorkReceiptVerification`: reference-resolution status, integrity checks,
  replay/hash checks, at-least-once disclosures, and deterministic failure
  codes.
- `WorkReceiptFinalizeRequest` and `WorkReceiptDisclosureRequest`: bounded,
  authenticated write/read inputs with explicit idempotency and fence fields.

The contracts expose normalization and stable hashing helpers. The store never
hashes arbitrary request JSON directly: it hashes the normalized typed form so
field ordering and harmless caller formatting cannot change identity.

## Schema and transaction design

Migration `028_work_receipts.sql` adds three workspace-scoped tables:

1. `work_receipts` stores the immutable receipt identity, request hash,
   canonical receipt hash, bounded canonical payload, actor/task/session
   references, source manifest/replay hashes, status, and verification JSON.
2. `work_receipt_steps` stores bounded, ordered step snapshots for efficient
   inspection without parsing the canonical payload.
3. `work_receipt_references` stores typed, ordered links to authoritative
   records with their expected content hashes and semantic roles.

The tables use the existing `workspaces` authority and composite workspace
foreign keys, uniqueness constraints for natural identity and idempotency,
length/size/count checks, and immutable-row triggers. The store writes the
three table sets in one transaction. Link validation happens before any insert;
the transaction is still the final protection against a crash or concurrent
commit.

The migration is additive and compatible with existing migrations and rows.
No existing authoritative table is altered, and no receipt is required for
older work. Receipts are opt-in until the reference workflow integration is
enabled.

## Reuse and licensing decisions

Fornix reuses its own existing interfaces and data authority:

- EventStore for append-only history and replay;
- TaskStore and AgentRunStore for terminal-state and fence validation;
- RetrievalSurfaceStore, EvidenceStore, and ArtifactStore for source/hash
  resolution and bounded disclosure;
- ModelCallStore, ToolRunStore, ObservabilityStore, EvaluationStore, and
  artifact references for measured/estimated accounting;
- existing RBAC middleware for workspace and actor authorization;
- existing CLI/API/MCP conventions for operator access and redaction.

The design is informed by the public architecture patterns in Orloj
(Apache-2.0), agentmemory (Apache-2.0), ClawMem (MIT), and FornixDB (MIT):
structured trace history, replay-safe diagnostics, evidence/gold resolution,
and progressive disclosure. The implementation is independent and does not
copy source code. Kronaxis-Fabric is not a source because its BSL 1.1 license
is incompatible with copying into this MIT-licensed project. Fornix remains
MIT-licensed; no third-party source is vendored by this feature.

## Cost and performance budget

The target transaction is bounded by the request limits:

- at most 64 receipt steps;
- at most 128 typed references;
- at most 1 MiB canonical receipt payload;
- at most 128 disclosure items and the caller’s byte/token budgets.

Finalization performs one Postgres transaction with bounded reference
validation, one receipt insert, and bounded step/reference inserts. It performs
no model, embedding, tool, network, or filesystem work. Reads perform one
receipt lookup plus bounded step/reference lookups and, for detail/raw views,
bounded source/hash verification. The expected storage is O(receipt payload +
steps + references), with no duplication of raw artifact or evidence bytes.

The implementation measures finalization/disclosure behavior through the
focused Postgres suite and the end-to-end reference smoke. The focused suite
completed in approximately 0.83 seconds on a warm local Docker/Postgres setup;
the full verification and smoke runs also completed successfully. These are
operational baselines, not performance guarantees; production capacity still
depends on Postgres sizing, indexes, retention, and workload shape.

## Acceptance tests

The vertical slice is complete only when the following are covered:

- fresh and existing databases apply migration 028 cleanly;
- duplicate finalization returns one receipt and one durable effect;
- conflicting idempotency or natural identity requests fail closed;
- identical fake-provider runs produce identical receipt and verification
  hashes;
- missing, stale, contradictory, and cross-workspace references are rejected;
- stale task/run fences cannot finalize a receipt;
- a crash before commit leaves no receipt, step, or reference rows;
- a crash after commit is idempotently replayable;
- gist/detail/raw disclosures preserve the receipt hash and enforce hard
  budgets;
- credentials, raw prompts, and unbounded arbitrary text do not appear in
  receipts, logs, events, evidence, or reports;
- reconciliation identifies model/tool/retrieval/artifact/retry/duplicate
  work without claiming exactly-once external execution;
- replay reads recorded history and never calls remote providers or tools;
- CLI, HTTP, and MCP use equivalent workspace-scoped semantics;
- concurrent finalizers preserve one identity and immutable links;
- unit, integration, race, build, CI, Docker, and existing smoke suites remain
  green.

## Known limitations after this slice

This foundation does not yet produce a semantic source-code diff, cryptographic
signatures, or an independently notarized change packet. It does not provide
object storage, database partitioning, automated receipt retention, SSO/KMS,
or exactly-once external execution. Those capabilities require separate
decisions and must build on this receipt contract rather than creating a
parallel authority.
