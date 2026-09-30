# Loop 63 completion — workspace transaction boundary and pool hygiene

Status: qualification slice implemented and verified; universal production
qualification remains open

Task 63 closed a concrete deployment blocker in the universal transformation:
workspace-scoped compatibility HTTP handlers were not consistently installing
the transaction-local PostgreSQL workspace context required by the RLS
policies. The fix keeps tenant scope in Postgres and makes the boundary
reusable instead of adding process-global state.

## Delivered

- Added `store.WithWorkspaceTx`, a callback-owned transaction helper that sets
  workspace context before application SQL and commits or rolls back exactly
  once.
- Added pool-hygiene coverage proving commit, rollback, and callback failure
  clear `fornix.workspace_id` before a single pooled connection is reused.
- Migrated legacy memo read/update/delete/search paths to scoped transactions.
- Migrated RAG preflight and final ranking queries to scoped transactions.
- Migrated chunk and symbol compatibility boundaries, symbol graph edges,
  neighbors, reindex, and embedding backfill reads to scoped transactions.
- Migrated session registration, heartbeat, and listing to scoped
  transactions.
- Added a role-separated HTTP retrieval test that runs as a non-owner,
  `NOBYPASSRLS` runtime role and proves authentication plus same-workspace
  memo search through the real middleware and route.
- Extended the role-separated qualification script to run that HTTP test with
  the scoped retrieval permission.
- Kept legacy global coordination/federation/router tables explicitly outside
  this workspace-scoped boundary; they remain a separate containment task.

## Qualification evidence

Passed on the feature branch:

- Offline `go test ./internal/server ./internal/store -count=1`.
- Pool-hygiene and retrieval integration tests against disposable
  PostgreSQL/pgvector.
- Role-separated qualification with a runtime `NOSUPERUSER NOBYPASSRLS`
  role, migration-owned tables, least-privilege grants, scoped API-key
  authentication, credential lease tests, and the real HTTP retrieval route.
- Full `go test ./... -count=1` against the disposable database.

The test database used tmpfs only. No persistent development volume was
modified, and no provider or external system was contacted.

## Measured impact

- Schema/storage: no migration, durable table, index, vector, or artifact
  change.
- Per scoped transaction: one bounded `set_workspace_context` SQL call,
  already required by the RLS design.
- Pool safety: context is transaction-local and cleared by both commit and
  rollback; the single-connection test makes cross-request leakage observable.
- The full disposable database suite remained green after the boundary
  migration. Production latency, pool saturation, lock waits, and connection
  reset behavior still require target-topology load/soak measurement.

## Critic and remaining limitations

This slice does not make every current route production-qualified. The audit
still identifies legacy global coordination/federation/router surfaces that do
not carry workspace scope and therefore must remain explicitly contained or
be replaced by universal workspace-scoped adapters. It also does not provide
managed secret-manager deployment, signer rotation ceremony, live connector
conformance, backup/restore, HA, or production load evidence. Those are
tracked as separate gates; this note does not claim Issue #38 is complete.

## Next task prompt

```text
Task 64 — Contain legacy global surfaces and qualify the first complete live
effectful adapter under the universal authority envelope.

Read the chats directory, AGENTS.md, docs/00-fornix-foundation.md,
docs/14-production-readiness-qualification.md,
docs/90-qualification-runbook.md,
docs/111-universal-production-roadmap-status.md,
docs/149-workspace-transaction-boundary-foundation.md, and
docs/150-loop-63-completion.md.

Audit the remaining direct global coordination, federation, router-learning,
and other compatibility routes. For each one, either migrate it to an
explicit workspace-scoped universal adapter or contain it behind a documented
development-only/administrative boundary with deny-by-default authorization.
Do not allow an unscoped legacy route to act as a tenant-neutral shortcut.

Then choose one effectful adapter (HTTP/API is the preferred first target) and
qualify it end to end as a non-owner NOBYPASSRLS runtime role: signed
connector/schema admission, exact managed-credential source-version lease,
controlled egress, operation/task fence revalidation, provider idempotency,
verification, duplicate delivery, stale worker, timeout, crash-before/after
dispatch, uncertain outcome, and recovery replay. Add a deterministic
local/fake adapter test and a clearly separated opt-in live conformance test.

Keep Postgres as the authority. Do not add Redis, brokers, NATS, an object
store, or an LLM orchestration framework. Preserve append-only history, redact
credentials and provider payloads, add runbook/metrics evidence, and keep the
universal roadmap honest about what remains unqualified.
```
