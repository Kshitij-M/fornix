# Loop 108 — Fenced sandbox attempt identity bridge

Status: delivered as a control-plane contract and provider-admission slice for
Issue #28. It does not provide an OCI/gVisor/microVM runtime or qualify hostile
code execution.

## Delivered

- Added `SandboxExecutionIdentity`, a normalized, hash-bound, secret-free
  contract connecting workspace and tool-run identity to operation owner and
  fence, attempt/effect IDs, both the specialized tool request hash and the
  parent operation request hash, reservation hash, task fence, registered tool
  definition hash, effective sandbox profile hash, and agent-run owner/fence
  when a tool is agent-run-bound.
- Added a deterministic opaque runtime name derived from the full identity
  hash. It contains no workspace, tool, command, host path, prompt, or
  credential text.
- Passed the exact `EffectAuthority` from the durable effect dispatcher through
  the tool effect callback. The non-local invocation path validates matching
  workspace/task scope and uses the attempt-aware provider method.
- Added a typed reconciliation observation that binds its state/result back
  to the exact attempt identity. Unknown state cannot claim a result.
- Made non-local providers implement both attempt-aware execution and
  reconciliation and advertise the corresponding capability requirements.
  Exact-backend selection and no-fallback behavior remain in place. The
  default registry still contains only the local-process provider.
- Reused migration 036's append-only effect journal and the existing tool-run
  lifecycle rather than introducing another overlapping effect ledger. No
  migration, runtime service, dependency, image pull, or persistent storage was
  added.
- Updated the sandbox foundation and documentation map with the migration
  decision and the remaining runtime boundary.

## Tests and checks

Passed:

```text
go test -p 3 ./internal/contracts ./internal/tool ./internal/runtime ./internal/server -count=1
go test -race -p 2 ./internal/contracts ./internal/tool ./internal/runtime ./internal/server -count=1
go test -p 3 ./... -run '^$' -count=1
go vet -p 3 ./...
make fmt-check docs-check
git diff --check
```

The test suite covers incomplete and cross-workspace identities, identity
hash/runtime-name stability, result/identity mismatch, rejection of a
non-local provider that lacks attempt-aware methods, successful propagation of
separate tool/operation hashes plus operation/task/agent-run fences, bounded
reconciliation results, rejection without a durable dispatcher, and rejection
before provider invocation when authority does not match. The existing local
provider tests remain green.

The package compile check builds every test package but intentionally runs no
tests; the complete `go test ./...` suite was not rerun for this slice. A
PostgreSQL DSN and usable pgvector test
database are not configured in this environment, so DB-backed effect-journal
tests were not run here. The previous full test-suite attempt recorded in
[`240-loop-107-completion.md`](240-loop-107-completion.md) remains the
environmental baseline: some `httptest` cases cannot bind loopback in this
restricted environment. No full test-suite pass is claimed for this loop.

## Cost and performance

This slice adds no SQL statements, rows, artifacts, runtime startup, or
external services. It computes two existing canonical hashes (tool definition
and effective sandbox profile) when preparing a non-local attempt identity,
then hashes the compact identity for an opaque runtime name. The local default
does not construct or use this identity. No standalone latency benchmark was
run because there is no runtime adapter to measure; runtime startup, image
storage, and reconciliation throughput remain unmeasured.

## Critical review and remaining work

This is a safe seam, not a completed sandbox. The reconciliation interface is
required at provider registration but no scheduler/operator path invokes it
yet. A provider can implement the interface incorrectly; capability claims
remain declarations until independently tested against a real runtime. The
existing effect journal stores operation/effect identity and recovery state,
but this slice does not persist a runtime ID or process output, terminate an
orphaned runtime, or recover a completed result after a Fornix crash.

Before any non-local backend is enabled, the next work must implement a narrow
runner boundary that has no Docker socket in the API container; deterministic
runtime creation and lookup; bounded mount, network, CPU, memory, PID, output,
and scratch enforcement; runtime-wide cancellation; restart/takeover
reconciliation; exact result/artifact linking; and an opt-in integration test
matrix on a supported host. Decide on a new migration only if those runtime
facts cannot be reconstructed safely from the existing operation/effect and
tool authorities. gVisor and microVM remain distinct, optional tiers requiring
their own host prerequisites and qualification.

Issue #28, the universal transformation umbrella #38, and production
qualification #40 remain open. This loop does not declare Fornix production
ready.
