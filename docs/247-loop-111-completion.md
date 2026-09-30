# Loop 111 — Capability-aware local HTTP test preflight

Status: implemented and locally verified in the current working tree. This
improves test portability; it does not qualify PostgreSQL or network behavior.

## Outcome

Several integration-style unit tests use `httptest.NewServer`, which needs a
local loopback listener. This execution sandbox denies loopback binds. The
tests previously panicked before reaching their assertions, making the suite
result dependent on an environmental capability unrelated to the behavior
under test.

`internal/testutil.RequireLocalHTTP` now probes an ephemeral IPv4 loopback
listener and then IPv6 if needed. Tests that require `httptest.NewServer` call
the helper first. If either family works, the original real HTTP test runs
unchanged. Only when both are denied does Go mark that test skipped with the
reason. This helper is test-only; production code does not depend on it.

## Verification

- `go test -v -p 2 ./... -count=1` — exit code 0. This is a successful suite
  run with explicit skips, not evidence that skipped tests passed.
- Tests requiring local HTTP listeners were skipped when neither loopback
  family could bind in this environment.
- PostgreSQL-backed tests were skipped because `FORNIX_TEST_PG_DSN` is unset;
  database migrations, concurrency, RLS, rollback, and recovery behavior were
  therefore not qualified by this run.
- `make fmt-check docs-check` — passed.
- `git diff --check` — passed before this documentation update.
- The task-specific Go build cache was cleaned after testing.

## Remaining limits

Run the skipped HTTP tests in CI or a host that permits loopback listeners.
Run the PostgreSQL integration and qualification suites against the supported
PostgreSQL/pgvector topology. An exit code of zero with skipped tests is not a
substitute for either qualification. No Docker image or external service was
started for this loop.

No commit or pull request was created. Existing dirty worktree changes were
preserved.
