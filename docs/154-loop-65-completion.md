# Loop 65 completion: workspace-scoped coordination and router authority

Status: implemented and locally qualified on the Task 65 branch.

## Delivered

- Added migration 061 with two additive, workspace-scoped authorities:
  `fornix.workspace_coordination_messages` and
  `fornix.workspace_router_observations`.
- Added typed coordination and router contracts with bounded fields,
  canonical request hashes, actor/workspace validation, and deterministic
  recommendation ordering.
- Added transactional stores that append the authoritative row and typed
  control event in one Postgres transaction.
- Added workspace/idempotency uniqueness. Duplicate delivery returns the
  original row without a second event; a different request hash fails closed.
- Added read-after-sequence coordination pagination with bounded limits and
  strict cursor validation.
- Moved ordinary `/v1/coord`, `/v1/coord/recent`, and `/v1/router/*` traffic
  onto the workspace authorities. Historical global federation routes remain
  quarantined behind the explicit compatibility flag and
  `legacy:global_admin` capability.
- Added runtime-role RLS qualification for coordination and router HTTP paths,
  including authenticated duplicate delivery and invalid-cursor behavior.
- Added failure hooks and tests for transaction rollback, concurrent duplicate
  writers, ordering, replay identity, recommendation determinism, and
  workspace isolation.
- Added `make smoke-universal-coordination` and included it in the aggregate
  smoke target.

## Verification

The following checks passed against a disposable PostgreSQL + pgvector
container using tmpfs storage:

- `go test ./internal/contracts ./internal/store -count=1`
- `go test ./internal/server ./internal/contracts ./internal/store -count=1`
- full `go test ./... -count=1` against a fresh database
- role-separated qualification with a non-superuser, `NOBYPASSRLS` runtime
  role, scoped API-key authentication, credential leases, retrieval, and the
  new coordination/router routes
- invalid HTTP cursor rejection and duplicate coordination/router delivery
- existing-database migration compatibility through the same qualification
  database

The disposable container used a tmpfs data directory and was not backed by a
persistent Docker volume. No persistent Docker volume, unrelated image, or
host credential was modified.

## Authority and security interpretation

Postgres remains the only authority. Application workspace predicates and
transaction-local RLS context are both required. The new tables do not inherit
ownership from historical global rows, and no historical row was copied or
assigned to a workspace by inference.

The coordination body is bounded and preserved as authoritative data. Router
recommendations expose aggregates only. Actor, request, causation, and
correlation metadata are durable; credentials and raw bearer tokens are not.
External federation remains unavailable in safe mode because its historical
peer records still contain an unsafe raw-token and unscoped egress design.

## Efficiency and storage impact

- A successful coordination or router write performs one bounded transaction,
  one workspace/idempotency unique-key arbitration, and one append-only event
  insert.
- Coordination reads use `(workspace_id, sequence)` and recipient indexes with
  a hard page limit of 500 rows.
- Router recommendation reads aggregate one workspace/category over a bounded
  recent window and sort in memory using stable score and model-ID tie-breaks.
- Accepted writes add one authoritative domain row and one event; duplicate
  requests add no domain row and no event.
- No broker, cache, embedding call, model call, or remote network request was
  introduced.
- The temporary qualification database used tmpfs, so its storage impact was
  limited to the test run and was discarded with the container.

Exact production latency and storage numbers remain deployment-specific and
must be measured with representative message sizes, event retention, and
workspace cardinality before a production SLO is declared.

## Remaining limitations

Task 65 does not complete the universal transformation by itself. The next
slice must:

1. replace federation peer rows with workspace-scoped records containing
   managed credential references rather than bearer tokens;
2. validate lease/fence state immediately before controlled outbound egress;
3. define provider idempotency and unknown-outcome reconciliation for remote
   coordination;
4. prove migration or permanent quarantine semantics for historical global
   federation rows; and
5. qualify a second first-party effectful adapter against the same authority,
   verification, compensation, and replay contract.

These limitations are reflected in the public roadmap rather than hidden
behind the completion status of this slice.
