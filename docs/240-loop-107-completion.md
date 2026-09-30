# Loop 107 — Sandbox capability contract and fail-closed selection

Status: partial delivery for Issue #28. This record does not mark the tiered
sandbox workstream complete or claim that untrusted code is isolated.

## Delivered

- Added typed names for local-process, OCI, gVisor, and microVM backends plus
  a deterministic capability report that separates enforced controls from
  controls a backend does not enforce.
- Added an explicit provider registry. Selection is exact; an unavailable
  backend or missing required capability returns a stable failure and never
  falls back to local execution.
- Registered only the current local-process provider in the default server
  configuration. The capability report calls out same-host execution and
  missing filesystem, network, CPU, memory, PID, and scratch isolation.
- Added validated immutable image digests and bounded CPU, memory, PID, and
  scratch fields for future runtime-backed profiles. Non-local profiles cannot
  be run until a provider is explicitly registered.
- Bound new run evidence to hashes of the normalized effective tool definition
  and sandbox profile. Duplicate requests with changed execution identity fail
  closed while legacy request hashes remain unchanged; historical terminal
  rows replay without re-execution, and historical nonterminal rows without
  execution identity cannot start a new effect.
- Tightened request budgets for argument count/size and full `KEY=value`
  environment bytes. Process limits intersect by minimum across definition,
  policy, and request.
- On Unix, local-process cancellation terminates the process group and sets a
  bounded wait delay for inherited output pipes. This is not a hostile-child
  boundary: a process that escapes its group is not contained, and no CPU,
  memory, PID, filesystem, or network isolation is provided.
- Local backend availability is false on platforms that cannot provide the
  required process-group cancellation; admission fails closed rather than
  silently running without that baseline.
- Local workdir narrowing resolves both roots through symlinks before
  containment comparison. Runtime-backed profiles require distinct process-
  tree, filesystem, network, read-only mount/root, and configured resource
  capabilities.
- The default development and packaged Compose manifests are regression-tested
  to keep the Docker daemon socket out of the control server. Runtime access
  must be placed behind a narrow runner interface, not granted to the API.
- Updated the production-readiness and tool-runtime docs to make the current
  local execution boundary explicit.

No migration was added. This slice creates no external runtime object and no
durable sandbox attempt. Migration `081` is a prerequisite for OCI/gVisor/VM
execution so a runtime object can be bound to a fenced tool run and reconciled
after a crash. A tool-run row alone cannot prove whether a detached container
is still executing.

## Verification

The following checks passed using a task-scoped Go cache:

```text
go test ./internal/contracts ./internal/tool ./internal/runtime ./internal/server -count=1
go test -race -p 3 ./internal/contracts ./internal/tool ./internal/runtime ./internal/server -count=1
go vet -p 4 ./...
go test ./... -run '^$' -count=1
GOOS=linux GOARCH=amd64 go test -exec /usr/bin/true ./internal/tool ./internal/runtime -run '^$' -count=1
GOOS=windows GOARCH=amd64 go test -exec /usr/bin/true ./internal/tool ./internal/runtime -run '^$' -count=1
make fmt-check docs-check
git diff --check
```

The full command `go test -p 4 ./... -count=1` was attempted and exited
nonzero because this execution environment denies loopback binds (`listen
tcp6 [::1]:0: operation not permitted`) used by existing `httptest` cases in
`cmd/fornix-watcher`, `internal/adapters/httpapi`, `internal/connector`,
`internal/credentials`, `internal/model`, and `internal/qualification`. The
affected package tests need to run in CI or another environment that permits
local listeners. This environment has no configured `FORNIX_TEST_PG_DSN`, so
PostgreSQL integration tests and runtime-backed sandbox integration tests
were not run. No Docker image was pulled or started, no database was changed,
and CI was not run. The task-scoped Go cache was cleared before verification
and will be cleared again afterward.

A fresh independent reviewer could not be spawned because the multi-agent
concurrency limit was full; the existing sandbox reviewer was reused. That
review surfaced and this loop fixed local-root symlink escape, missing
non-local isolation-capability admission, platform-dependent local-backend
availability reporting, and the profile-identity test gap. Parent-cancellation
and output-overflow cleanup coverage were also added. Abrupt Fornix process or
host failure can still leave a local child alive; that limitation remains
explicit and requires a supervised runtime with durable attempt reconciliation.
This review is not maintainer approval.

## Critical review and remaining work

The packaged architecture adds a constraint to the next sandbox step: the
Fornix control server runs in a container without the Docker daemon socket, and
the default manifests should keep it that way. Docker's security guidance
warns that controlling a rootful daemon can grant host-filesystem authority
through container mounts ([Docker Engine security](https://docs.docker.com/engine/security/)).
The next implementation must therefore define a narrow host-runner or
deployment-worker boundary and an authorized workspace-transfer identity;
mounting the daemon socket into the API/control container is not an acceptable
shortcut.

The most important limitation is unchanged: `local-process` still runs with
the Fornix user's host authority. Path checks are not a filesystem sandbox,
`AllowNetwork=false` does not block network access, and the local executor does
not enforce CPU, memory, PID, or scratch limits. Unix process-group termination
does not contain a hostile child that escapes its group, and abrupt Fornix
process or host failure can bypass local cleanup. A malicious executable or
repository must not be treated as safe because it has structured argv, a
timeout, or bounded captured output.

Issue #28 remains open. The next implementation slice must add the OCI runtime
adapter and migration `081` for durable sandbox attempts, then qualify mounts,
network denial, resource limits, complete process/container cleanup, stale
fence rejection, and crash reconciliation with an opt-in runtime test matrix.
gVisor and microVM remain unavailable until their distinct host prerequisites
and security boundaries are independently qualified. No benchmark, CI result,
or isolation guarantee is claimed by this loop.
