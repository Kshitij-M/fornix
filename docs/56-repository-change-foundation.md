# Repository Change Artifacts and Verified Change Packet Foundation

Status: design and implementation note for Task 21
Audience: Fornix contributors, operators, reviewers, and adopters

## Product problem

Fornix is intended to make long-running repository work safe to delegate,
resume, inspect, and verify. The current alpha can ingest a repository, build a
bounded context pack, run a deterministic model/tool workflow, and produce a
Work Receipt. It does not yet have a controlled write boundary. A user can
therefore verify what a run observed, but cannot yet ask Fornix to produce a
reviewable repository change through the same durable authority model.

The missing boundary is deliberately narrower than “let an agent run shell
commands.” Fornix needs to represent a proposed change as typed, content-
addressed operations against a known source snapshot; obtain an explicit,
durable decision; apply only those approved operations inside a configured
workspace mount; and verify the resulting tree. The result must be inspectable
without trusting a mutable summary or re-running a model or tool.

This task establishes that boundary as the foundation of a future **Verified
Change Packet**:

```text
source snapshot
      -> deterministic change proposal
      -> policy admission
      -> durable approval
      -> fenced application
      -> post-state verification
      -> Work Receipt
```

The proposal, approval, application, and receipt are different facts:

- **Propose** records intended structured operations and the source hashes they
  require. It must not mutate repository files.
- **Approve** records which authenticated actor accepted the exact proposal
  hash, under which policy and until when. Approval cannot expand the packet.
- **Apply** revalidates source and policy conditions, performs only approved
  structured file operations, and records the external filesystem boundary.
- **Verify** reads the resulting files and compares content/tree hashes with
  the expected post-state. It turns an observed effect into a durable result;
  it does not overwrite the source snapshot.
- **Work Receipt** binds the complete evidence chain—source manifest, operation
  hashes, diff/result artifacts, actor, task/run/fence, approval, validation,
  and replay identities—into the existing immutable verification contract.

Unrestricted shell execution is not an acceptable change mechanism. Shell
parsing makes quoting, environment inheritance, redirection, command
substitution, working-directory scope, and side effects difficult to admit or
replay deterministically. A registered read-only tool may continue to use the
existing structured-argv runtime. This change path uses typed file operations
and direct Go filesystem APIs only; it never accepts a shell string or starts a
shell interpreter.

## Scope of this vertical slice

This task adds the smallest production-shaped write-capable workflow:

- typed source snapshot, change proposal, change packet, operation, approval,
  application, conflict, recovery, and disclosure contracts;
- deterministic canonicalization and SHA-256 hashes for proposals,
  operations, diffs, and resulting trees;
- migration `029_repository_changes.sql` with workspace-scoped immutable
  proposal/application history, approvals, operations, conflicts, artifacts,
  and idempotency identities;
- a transactional Postgres store that owns proposal/approval/application
  metadata, validates workspace and task-fence boundaries, and links artifacts;
- a deterministic planner for create, replace, delete, and rename operations;
- a local structured-file executor constrained to configured workspace mounts;
- proposal-only by default and explicit approval/policy admission for writes;
- read-back verification, conflict detection, and crash/recovery states;
- artifact-backed diff, source, result, conflict, and recovery disclosures;
- integration with the existing Work Receipt store;
- authenticated HTTP, CLI, and MCP access through one service/store path;
- unit, Postgres, concurrency, crash-injection, race, and end-to-end smoke
  coverage.

The first slice does not automatically re-index a changed repository. A
successful application produces a new observed tree identity and audit links;
an explicit ingestion job remains responsible for creating a new indexed
source snapshot. This prevents a file mutation from silently replacing the
authoritative history of the previous ingest.

## Explicit non-goals

This task does not implement:

- arbitrary shell or command execution;
- implicit `sh -c`, `bash -c`, PowerShell, command interpolation, or redirection;
- unrestricted filesystem access or an implicit repository root;
- automatic merge-conflict resolution;
- remote git pushes, commits, branches, or pull-request creation;
- autonomous deployment or production infrastructure mutation;
- an LLM-generated patch as a required dependency;
- kernel-level sandbox guarantees on every operating system;
- exactly-once external filesystem execution;
- cryptographic signatures or external notarization;
- a second Work Receipt or artifact authority;
- automatic re-ingestion after a successful write.

## Authority boundaries and invariants

### Postgres authority

Postgres is authoritative for change identity, source preconditions,
operations, approval decisions, state transitions, idempotency, audit actors,
artifact links, conflicts, recovery states, and Work Receipt references. Change
history is append-only. A current status is an operational projection over that
history; it is not permission to erase prior states.

The local filesystem is an external side-effect boundary. Postgres cannot make
a filesystem rename and a database commit one physical transaction. The
implementation therefore records an explicit `applying`/`recovery_required`
boundary, verifies expected post-state hashes, and treats ambiguous external
effects as at-least-once and inspectable. It never claims exactly-once file
mutation.

### Workspace and identity

1. Every proposal, source file, operation, approval, application, artifact
   link, and disclosure belongs to exactly one workspace.
2. Every store query includes the authenticated workspace predicate and
   composite workspace references where the schema permits them.
3. Body, query, path, and header workspace values cannot override the
   authenticated principal.
4. Actor, task, session, agent-run, request, idempotency, causation, and
   correlation identities are preserved without storing credentials.
5. Cross-workspace source, artifact, task, approval, or receipt references fail
   closed.

### Proposal identity and deterministic hashing

1. A proposal has a stable workspace-scoped identity and a canonical request
   hash.
2. An idempotency key can be used once for one canonical request. Repeating an
   equivalent request returns the original durable identity; reusing the key
   for a different request fails closed.
3. The change packet hash includes normalized source identity, ordered
   operations, expected preconditions, expected resulting file hashes, policy
   inputs, and schema version.
4. Operation ordering is canonicalized by explicit operation ordinal and then
   stable path/operation keys. Caller map or slice order cannot change a hash.
5. Delivery IDs, database-generated IDs, wall-clock timestamps, credentials,
   raw prompts, and arbitrary unbounded text are excluded from canonical
   hashes.
6. A diff/result hash is a hash of bounded, redacted, content-addressed
   representations and never replaces the raw source or resulting files.

### Source preconditions

Every proposal records the source snapshot it was planned against:

- workspace and repository/source identity;
- configured mount identity and normalized relative root;
- source manifest hash;
- normalized paths, modes, sizes, and content hashes for affected files;
- actor and provenance metadata;
- capture/request identity.

The executor re-reads affected paths immediately before mutation. A missing,
unexpected, changed, or newly conflicting path produces a durable `conflicted`
result. The executor does not overwrite an unexpected state and does not
silently reinterpret a stale proposal. A new source snapshot requires a new
proposal.

### Structured operations

The canonical operation vocabulary is intentionally small:

- `create_file`: path must be absent; new bytes are supplied by an immutable
  ArtifactStore reference;
- `replace_file`: path must exist with the expected prior hash; new bytes come
  from an artifact reference;
- `delete_file`: path must exist with the expected prior hash;
- `rename_file`: source must exist with the expected prior hash and destination
  must be absent unless a future explicit overwrite policy is introduced;
- `chmod_file`: only an explicitly supported, bounded mode change.

Each operation includes a stable operation ID, ordinal, normalized relative
path(s), expected prior state, new content hash/reference where applicable,
resulting hash, byte size, mode, provenance, and request identity. No operation
contains a shell string. If a future API accepts a unified diff, it must first
compile it into these typed operations; the executor never passes diff text to
a shell.

### Filesystem safety

The source root must be inside an explicitly configured workspace mount. The
change service rejects:

- absolute paths, `..` traversal, null bytes, control characters, and path
  escapes;
- unknown or unconfigured mounts;
- symlink components or symlink escapes unless a future explicitly qualified
  provider replaces this fail-closed behavior;
- paths outside the repository root;
- credential/private-key paths and protected policy paths by default;
- oversized files, operation counts, argument/content bytes, or total change
  bytes.

Path validation is performed on normalized lexical paths and again against
the filesystem before each external mutation. Temporary files are created in
the destination directory and renamed atomically where the host supports it.
The implementation documents that atomic rename and directory durability
semantics are operating-system dependent.

### State machine and transitions

The durable lifecycle is:

```text
proposed -> awaiting_approval -> approved -> applying -> applied
    |            |                 |          |
    |            |                 |          +-> recovery_required
    |            |                 +-> conflicted / failed
    |            +-> rejected / expired / cancelled
    +-> rejected / cancelled / conflicted
```

Allowed transitions are explicit and monotonic. Every transition records the
actor, request identity, reason, expected prior state, resulting state, and
relevant hashes. Terminal states do not accept ordinary mutation. A duplicate
request returns the committed outcome. A conflicting request or illegal
transition fails closed.

`recovery_required` means the filesystem may have changed but the authoritative
application result is not safely known. Recovery must read expected and actual
hashes; it may finalize an already-observed expected post-state, or record a
new failure/conflict. It must never overwrite an unexpected state merely to
make the database look successful.

### Approval and policy

Proposal-only is the default. Application requires:

- authenticated workspace principal;
- `change:apply` permission and any task/run execution permission;
- an explicit policy admission for the requested mount, paths, operation
  types, and budgets;
- a durable approval tied to the exact proposal/change-packet hash, unless an
  explicit automatic mode is configured;
- valid source preconditions;
- valid current task owner/fencing token for task-bound work;
- valid budget and non-cancelled state.

Approval is one-shot, request-bound, expiry-bound, and auditable. It cannot
expand paths, operations, bytes, or policy. The default policy denies protected
paths and requires interactive approval for writes. Self-approval behavior is
explicitly represented in policy; this slice defaults to requiring the
`change:approve` permission and records the actor rather than silently
accepting an unlogged local prompt.

### Task fencing

For task-bound proposals and applications, the store validates the current
task owner and monotonic fence before durable mutation and immediately before
filesystem mutation. The same fence is required for application finalization,
artifact linking, and Work Receipt finalization. A stale worker cannot advance
the change state, even if its filesystem call was already transmitted. The
external at-least-once boundary is surfaced as a recovery/conflict result.

### Crash semantics

- Crash before database commit: no proposal/application/approval/reference
  mutation is authoritative.
- Crash before filesystem mutation: retry sees the unchanged source state and
  can safely resume the approved application.
- Crash during a temporary write: incomplete temporary state is ignored or
  cleaned by bounded recovery; the target path remains governed by its prior
  hash.
- Crash after filesystem mutation but before database commit: retry reads the
  expected post-state. Matching state may be recorded as applied; unexpected
  state becomes `recovery_required` and is never overwritten.
- Crash after database commit: the durable result is replayable and duplicate
  application returns it without reapplying.

### Artifacts, provenance, and receipts

Large proposed content, source manifests, diffs, results, conflicts, and
recovery reports use the existing content-addressed ArtifactStore. Artifacts
are linked transactionally to the change record. Raw bytes remain immutable;
inline fields contain only bounded compatibility summaries and hashes.

The Work Receipt remains the verification authority. A completed application
can link source manifest hash, packet hash, operation hashes, diff/result
artifact hashes, approval/application IDs, resulting tree hash, validation,
task/run/fence, and replay identities. The change store must not invent a
parallel receipt or mutate source/evidence/artifact history.

## Schema and transaction design

Migration `029_repository_changes.sql` is additive and follows migration 028.
It adds workspace-scoped tables for:

- immutable proposal identity and request hash;
- source snapshots and affected source files;
- normalized change operations;
- approval decisions;
- application attempts and state transitions;
- conflicts and recovery-required observations;
- artifact links and Work Receipt references;
- bounded audit/idempotency metadata.

Composite workspace foreign keys, uniqueness constraints, bounded JSON checks,
indexes, and append-only triggers protect the authority. The schema must work
against fresh and already-migrated databases without changing prior migration
checksums.

Proposal/approval persistence, source validation, artifact linking, and
authoritative state transitions are transactional. Filesystem effects are
outside that database transaction and are recorded with the explicit
recovery semantics above.

## Reuse and licensing decisions

The implementation reuses existing Fornix authorities and patterns:

- `RepositorySource` mount and normalized-path policy from ingestion;
- `ArtifactStore` for immutable content and bounded disclosure;
- `TaskStore`/task leases for owner/fence validation;
- identity/RBAC middleware for workspace authorization;
- typed events, evidence, observations, and Work Receipts for audit/proof;
- existing CLI, HTTP, MCP, redaction, migration, and smoke conventions.

The design is informed by:

- DeepSeek Harness filesystem/sandbox roots, fail-closed provider boundary,
  approval outcomes, atomic writes, and explicit execution pipeline;
- Orloj tool policy, task approval, task lifecycle, execution checkpoint, and
  artifact/evidence patterns;
- agentmemory action leases, checkpoints, cleanup, diagnostics, and provenance;
- FornixDB immutable raw/detail/gist, retention, lineage, and disclosure
  patterns;
- ClawMem provenance and bounded evidence/replay gates.

No reference source code is copied. DeepSeek Harness, ClawMem, and FornixDB
are MIT; Orloj and agentmemory are Apache-2.0. Kronaxis Fabric is BSL 1.1 and
is not a source for this MIT-licensed project. Fornix remains MIT-licensed.

## Cost and performance budget

The normal proposal path performs deterministic validation, hashing, bounded
source reads, one Postgres transaction, and artifact writes only for content
that is actually new. It performs no model or embedding call.

Initial hard limits are deliberately conservative and configurable only
downward per request:

- 128 operations per proposal;
- 64 MiB per file content artifact;
- 256 MiB total proposed content;
- 100 MiB total expected filesystem change bytes;
- 4,096-byte maximum normalized path length;
- bounded diff/result/conflict reports of 1 MiB inline before artifact storage;
- one approval and one active application attempt per proposal identity;
- bounded list/disclosure pages of 100 records.

The qualification run must measure proposal, approval, application, and
verification latency; Postgres statement/row work; filesystem read/write
throughput; artifact bytes and deduplication; conflict detection; crash
recovery; and replay throughput. These are local workload measurements, not
production SLOs.

The expected database cost is O(proposal + source-file rows + operations +
transitions + artifact references). Raw file content is not duplicated when
the same workspace artifact already exists. The principal remaining cost is
content hashing and safe source revalidation; no broker, cache, model, or
additional database is introduced.

## Acceptance tests

Contracts and planner:

- normalization, canonical hash, stable operation ordering, clone isolation,
  redaction, bounds, workspace checks, and disclosure budgets;
- create/replace/delete/rename/mode operations;
- duplicate/conflicting paths and stale source hashes;
- expected file/tree hash calculation;
- oversized file/operation/byte/path rejection;
- shell-shaped input and protected-path rejection.

Filesystem safety:

- absolute/traversal/null/control path rejection;
- mount escape rejection;
- symlink source and destination rejection;
- read-only/protected path rejection;
- atomic replacement and post-state verification;
- no implicit shell execution.

Store and authorization:

- fresh/existing migration compatibility;
- duplicate and conflicting proposal/approval/application identities;
- workspace isolation and RBAC fail-closed behavior;
- approval expiry/rejection/mismatch;
- stale task worker before application and before finalization;
- append-only transition and audit history.

Crash and concurrency:

- concurrent proposals produce one identity;
- concurrent applications produce one durable effect;
- crash before commit rolls back authoritative state;
- crash before filesystem mutation is resumable;
- crash after filesystem mutation is safely reconciled;
- unexpected state becomes recovery-required without overwrite;
- stale fence cannot finalize an already-running external effect.

Integration:

- transactional artifact and Work Receipt links;
- bounded diff/result/conflict disclosure;
- CLI, HTTP, and MCP semantic equivalence;
- dry-run is side-effect free;
- replay from sequence zero and checkpoint produces identical hashes;
- credentials, raw prompts, and arbitrary unbounded text never enter logs,
  events, artifacts, receipts, metrics, or errors;
- v0.31 local fixture smoke and the complete existing smoke matrix remain
  green.

## Known limitations after this slice

This foundation will not provide cryptographic signing, remote git hosting,
automatic merge resolution, kernel-level sandboxing on every host, object
storage, background change scheduling, or exactly-once filesystem effects.
Recovery-required states can require operator inspection. The source tree is
an external authority for bytes; Postgres is the authority for what Fornix
admitted, approved, attempted, observed, and verified. A future change packet
reviewer can build on this contract without reinterpreting mutable summaries.
