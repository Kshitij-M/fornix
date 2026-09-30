# Generic operation authority

Status: implementation note for Issue [#39](https://github.com/Kshitij-M/fornix/issues/39).

This note defines the first durable authority shared by every future Fornix
adapter. It is deliberately separate from the repository adapter: a repository
operation, an API request, a read-only SQL query, and a business-system action
must have the same control-plane guarantees before their domain code runs.

## Why this layer exists

The domain-neutral contracts in `internal/contracts` describe intent, plans,
resources, capabilities, results, evidence references, and external effects.
Those contracts are not sufficient on their own. A process can still lose an
operation between admission and execution, accept two deliveries, let a stale
worker finish after takeover, or make an external call without leaving a
durable recovery boundary.

Migration `035_operations.sql` and `internal/store/operations.go` add the
missing Postgres authority. The operation row is a current projection for
queries; transition, attempt, effect, callback, and link records preserve the
append-only history and boundary evidence needed for recovery and replay.

## Invariants

1. **One scoped identity.** An operation is identified by `(workspace_id,
   idempotency_key)` and carries a canonical request hash. A repeated request
   returns the original operation; a different request using the same key is
   rejected.
2. **No cross-workspace references.** The request, actor, task/session
   references, capability, target, plan, resources, links, attempts, effects,
   callbacks, events, and reads all carry or derive the same workspace.
3. **Fail-closed lifecycle.** The exported transition graph accepts only
   explicit, bounded transitions. Terminal operations cannot be advanced.
4. **Fenced mutation.** A worker must hold the current, unexpired operation
   lease. Takeover increments a monotonically increasing fence; an old owner
   cannot renew, transition, reserve an attempt, or reserve an effect.
5. **Atomic authority.** The operation projection, transition record,
   idempotency record, and typed event are committed in one transaction.
   Injected failures before commit roll back all of them.
6. **External effects are not exactly once.** `ReserveEffect` records the
   boundary before an adapter is called and makes delivery, provider
   idempotency, verification, and compensation explicit. It never calls an
   external system and never claims exactly-once execution.
7. **Replay is read-only.** Replay verifies ordered state versions and state
   hashes. It cannot invoke a connector, model, tool, callback, filesystem,
   network, or other external effect.
8. **Raw domain data stays with its authority.** The generic store keeps
   typed inputs, bounded metadata, IDs, hashes, and references. Payloads,
   prompts, credentials, and connector-specific raw records belong in the
   existing evidence/artifact authorities.

## Durable schema

Migration 035 creates the following workspace-scoped tables:

| Table | Responsibility | Mutation rule |
| --- | --- | --- |
| `operations` | Current operation projection and bounded request/plan identity | Updated only through the store’s transaction boundary |
| `operation_idempotency` | Create/transition command deduplication and outcome links | Append reservation, then outcome completion |
| `operation_transitions` | Ordered lifecycle history and state hashes | Append-only trigger |
| `operation_resources` | Hash-only domain-resource links | Append-only trigger |
| `operation_steps` | Bounded normalized plan nodes | Inserted with the operation; history is not overwritten |
| `operation_attempts` | Fenced step-attempt reservations | Append-only trigger |
| `operation_effects` | External boundary and verification ledger | Append-only trigger |
| `operation_callbacks` | At-least-once external completion callbacks | Append-only trigger |
| `operation_leases` | Current owner, expiry, and monotonic fence | Transactionally acquired/renewed/released |
| `operation_links` | Compatibility links to existing task/run/tool/receipt/artifact/evidence authorities | Append-only trigger |

The generic row does not replace the existing specialized authorities. A link
is a compatibility reference, not permission to bypass the specialized store.
The next admission slice will add source-existence and authorization checks
for each link kind.

## Reuse and licensing decisions

The implementation reuses Fornix’s existing `EventStore.AppendTx`, migration
runner, workspace-scoped contracts, and Postgres transaction conventions. The
design was informed by Orloj’s transactional claims/checkpoints, DeepSeek
Harness’s typed provider and permission seams, agentmemory’s leases and
replay/checkpoint lifecycle, ClawMem’s bounded evidence-oriented retrieval,
FornixDB’s immutable disclosure tiers, and Temporal/River-style durable
workflow concepts.

These are independent implementations of patterns, not copied source. No
Kronaxis-fabric source is used; its BSL 1.1 license is incompatible with the
MIT-licensed Fornix distribution. If code is copied from a reference project
in a later adapter, the contribution must first record the exact license,
notice, and file-level provenance in `docs/13-reference-reuse-matrix.md`.

## Cost and storage budget

The authority writes one operation row, one idempotency reservation, one
typed event, and one transition row per committed lifecycle change. Plans and
failure metadata are bounded by database checks; raw outputs are hashes or
references. Expected database work is O(1) for create, lease, and transition
commands plus O(number of plan steps) for plan insertion. Replay is bounded by
the caller’s transition limit and performs no external work.

The trade-off is deliberate: every state change costs a durable write so a
large operation can be recovered and audited. High-volume telemetry belongs
in the observability path, not in unbounded operation metadata. Partitioning,
archival, and high-availability PostgreSQL qualification remain open Issue
[#40](https://github.com/Kshitij-M/fornix/issues/40) work.

## Acceptance tests

The store test suite must qualify, with a fresh and an already-migrated
database:

- duplicate and conflicting create requests;
- concurrent create requests with one durable effect;
- legal and illegal lifecycle transitions;
- lease acquisition, renewal, expiry, takeover, release, and stale-fence rejection;
- concurrent fenced transitions with one monotonic state version;
- crash injection before commit with no partial operation history;
- replay from zero and from a checkpoint with a stable replay hash;
- duplicate attempt, effect, and callback delivery;
- no external invocation from replay or effect reservation;
- cross-workspace read and mutation rejection; and
- schema checks for append-only history and bounded hashes/JSON.

The repository-wide qualification remains `go test ./...`, `go vet ./...`,
`go test -race ./...`, `make docs-check`, `make build`, and the Postgres-backed
smokes. A missing `FORNIX_TEST_PG_DSN` skips integration tests locally but is a
qualification gap, not a passing production claim.

## Known limits after this slice

This slice does not yet implement universal policy evaluation, durable
authorization of every connector link, a host-independent sandbox, remote
egress/SSRF enforcement, a durable connector registry, a multi-step workflow
runtime, database RLS, backup/restore evidence, or HA operations. Those are
explicitly sequenced next rather than hidden behind the generic operation
table. The repository adapter remains the only fully qualified domain adapter
until the universal qualification issue is complete.

See the [universal control-plane overview](68-universal-work-control-plane.md),
the [domain-neutral contracts note](66-domain-neutral-harness-foundation.md),
and the [production qualification](14-production-readiness-qualification.md)
for the top-down product boundary.
