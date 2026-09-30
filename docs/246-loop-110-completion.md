# Loop 110 — Root-anchored repository change I/O

Status: implemented and locally verified in the current working tree. This
does not qualify Fornix for hostile or unattended filesystem operations.

## Outcome

The repository change executor previously checked a path using the configured
absolute path and later used that path for mutation. A parent symlink swapped
after validation could redirect a write outside the repository. The executor
now opens one checked `os.Root`, verifies the opened handle refers to the
checked directory, and uses that handle through precondition validation,
mutation, and post-state hashing.

Create now publishes a same-directory temporary file using a no-overwrite hard
link, so a file that appears after the precondition check is not replaced.
Rename uses a no-overwrite link followed by unlink rather than a replacing
rename. That protects an unexpected destination, but it is not atomic: a crash
between the calls can leave both names. Chmod is applied through an opened file
handle. Post-state hashing now omits absent paths, fixing a pre-existing
verification mismatch for delete and rename operations.

See [the feature note](245-root-anchored-change-io-foundation.md), the
[repository change foundation](56-repository-change-foundation.md), and the
[production qualification status](14-production-readiness-qualification.md).

## Verification

- `go test -p 2 ./internal/change -count=1` — passed.
- `go test -race -p 2 ./internal/change -count=1` — passed.
- `go test -p 2 ./... -run '^$' -count=1` — every Go package compiled.
- `go vet -p 2 ./...` — passed.
- `make fmt-check` — passed.
- `make docs-check` — passed for 251 Markdown files.
- `git diff --check` — passed.
- Added deterministic tests for an outside-symlink swap after precondition
  checking, pre-existing symlink path/root rejection, a concurrently created
  file not being overwritten, and successful create/replace/delete/rename/chmod
  with stable resulting-tree hashes.

The initial `go test -p 2 ./... -count=1` attempt did not pass in this
execution environment: `httptest.NewServer` could not bind the sandbox's
loopback listener in six packages. Loop 111 added an explicit capability
preflight for those tests. A later full suite exited successfully, with
loopback-dependent tests skipped when neither loopback family was available.
That exit status does not mean those skipped tests passed; run them in CI or a
network-enabled local environment. PostgreSQL-backed tests were also skipped
because `FORNIX_TEST_PG_DSN` was unset. See
[`247-loop-111-completion.md`](247-loop-111-completion.md).

The suite does not establish that a deployment mount cannot be concurrently
rewired by its administrator, that in-root mount traversal is prevented, or
that arbitrary code is sandboxed. The relevant Postgres-backed change tests
were not run because `FORNIX_TEST_PG_DSN` is unset; local Postgres 14 is
installed but pgvector is absent. No Docker image was pulled or started.

## Cost and remaining limits

No migration, dependency, database query, or external service was added. Each
apply/observation holds one root handle and uses rooted filesystem operations.
No latency or throughput benchmark was collected. Multi-operation application
is still not crash-atomic; an interrupted rename may leave both names, and
filesystem durability remains platform-dependent. `os.Root` is a path
containment primitive, not a complete OS sandbox; mount traversal and
independent concurrent writers remain deployment concerns.

No commit or pull request was created. Existing dirty worktree changes were
preserved.
