# Loop 119 — Agent-run fencing at generic effect dispatch

Status: contract and control-plane validation implemented; PostgreSQL-backed
stale-takeover test added but not executed in this environment.

## Delivered

- Extended the optional generic `EffectAuthority` envelope with the agent-run
  ID, owner, and monotonic fence. Partial tuples and fences outside PostgreSQL's
  signed integer range are rejected. The optional JSON fields remain omitted
  for standalone effects, preserving their serialized shape and stable hash.
- Propagated the existing lease tuple from model and tool requests through
  their production domain adapters and child-effect runtime.
- Added `OperationStore.BeginEffectDispatch`, which validates operation,
  task, agent-run, credential, and effect authority and commits the unique
  `dispatching` transition in the same workspace transaction. This closes the
  former check-then-intent gap: a takeover that wins before the transaction
  rejects dispatch, and the effect remains `reserved` for a current owner.
- Kept the external invocation outside Postgres. The dispatch-intent commit is
  the authorization linearization point; a later lease takeover does not
  revoke a one-shot effect already authorized for delivery. Duplicate delivery
  cannot create a second permit, and uncertain external outcomes still require
  reconciliation.
- Strengthened the PostgreSQL integration test to force takeover after effect
  reservation but before the atomic authority/intent transaction, assert no
  invocation and a still-reserved state, then resume the same reservation under
  the new owner.

No migration or new infrastructure was needed. The check reuses the existing
agent-run lease table and validator. Agent-run-bound dispatch adds one indexed
lease-row lock/read to the final authority transaction; standalone effects
have no added agent-run query.

## Database work and cost

The dispatch path now combines live-authority validation and the durable
dispatching transition in one commit, eliminating the former separate
post-intent validation transaction. The transaction still performs the
existing operation/task/effect checks plus one agent-run lease-row lock for
run-bound effects. No latency or contention number is claimed: the required
Postgres DSN was unavailable here, so database-backed timing and lock
contention must be collected by the disposable-Postgres CI qualification.

## Verification

- `GOPROXY=off go test ./...` — passed across all packages.
- `GOPROXY=off go test -race ./...` — passed across all packages.
- `GOPROXY=off go vet ./...` — passed.
- All three binaries built to a temporary directory; `make docs-check` passed
  for 270 Markdown files; `git diff --check` passed.
- Go build caches and temporary binaries were removed after verification.
- The stale-takeover integration case is gated by `FORNIX_TEST_PG_DSN`. This
  environment did not provide a disposable PostgreSQL test database, so the
  successful package and full-suite results do not count as execution of that
  database-backed assertion. The explicit
  `make qualification-agent-run-effect-dispatch-postgres` target is wired into
  the Postgres-backed CI job so CI executes it with a disposable database.

## Remaining limitations

- Run the database integration test with the repository's disposable pgvector
  test database and run the full CI suite before treating this slice as
  qualified.
- The durable intent authorizes one external delivery but cannot recall it if
  ownership changes after commit. Generic external process/provider execution
  remains at-least-once and uncertain; exactly-once or instantly revocable
  external execution is not claimed.
- This does not add OCI/container isolation. A native Moby lifecycle adapter
  and live host/runtime qualification remain open, and OCI continues to fail
  closed.
