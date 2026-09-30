# Loop 104 completion — agent tool resume and path-containment hardening

Status: offline implementation and focused qualification complete. Issue #40
and the database/deployment production gates remain open.

## Outcome

Read-only tool path arguments now fail closed if symlink resolution fails or
resolves outside the canonical authorized root. A regression test covers a
symlink to an outside directory followed by a missing leaf; the executor
rejects the request before the registered process can create that outside
file. This intentionally requires path-restricted read-only arguments to
resolve to existing paths.

Before each model dispatch, the agent loop re-resolves every persisted tool
name against the current registry, recomputes its canonical model-visible
description/schema/fingerprint, and checks current workspace/actor/task/session
policy admission. Revocation or definition drift deterministically fails the
run before provider egress. Existing execution-time argument checks, approval,
fencing, and budgets remain authoritative.

OpenAI-compatible response parsing rejects duplicate normalized tool-call IDs
before they can collide with durable tool-run idempotency keys. Unique calls
continue to preserve their IDs and registered tool mapping.

An independent security review surfaced the path escape as P1 and the two
resume/idempotency issues as P2. All three findings are addressed by this
slice; the review did not identify a P0 issue.

## Schema, storage, and cost

No database migration, public contract change, dependency, or infrastructure
was added. The loop reuses its registered catalog binder and existing
authorization seam. Model dispatch now performs one bounded registry lookup
and policy admission per declared tool. Path resolution and duplicate-ID
checking are bounded by existing argument and provider-call limits. No new
durable bytes or SQL work are introduced. No throughput benchmark is claimed.

## Verification

Passed locally:

- `make qualification-agent-tool-schema-authority`
- `go test ./internal/tool ./internal/agentloop -count=1`
- Focused provider parser/projection tests in `internal/model`
- `go test -race ./internal/agentloop ./internal/tool -count=1`
- `go vet ./internal/agentloop ./internal/tool ./internal/model`
- `make fmt-check docs-check`
- `git diff --check`

The focused model tests avoid network calls. The complete model-provider HTTP
tests use loopback listeners, which this sandbox does not permit. PostgreSQL
qualification is not claimed: `FORNIX_TEST_PG_DSN` is unset, the installed
local PostgreSQL is version 14 without pgvector, and the Docker daemon is
inaccessible. CI was not run for this local, uncommitted worktree.

## Remaining limits and critique

- This closes the reviewed path-resolution bypass; it does not make local
  process execution a kernel-enforced filesystem or network sandbox.
- Revalidation is performed immediately before model egress and cannot make
  policy mutation and a remote provider request one atomic transaction. A
  deployment requiring stronger revocation timing needs a durable admission
  generation/fence bound to the external request.
- Existing catalog persistence, RLS, worker recovery, and migration behavior
  still require disposable-Postgres qualification. Issue #40 also retains
  deployment-specific secret-manager, egress, backup/restore, HA, load/soak,
  connector-conformance, and adversarial-security gates.

The next highest-priority step remains running the existing retry-deadline,
agent-catalog persistence, workspace RLS, and recovery qualifications against
the supported disposable PostgreSQL/pgvector topology and preserving that CI
evidence against this exact branch snapshot.
