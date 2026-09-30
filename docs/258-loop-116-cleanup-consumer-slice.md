# Loop 116 — Authenticated sandbox cleanup transport and fenced worker slice

Status: partial implementation; the Moby/OCI runtime and full Task 116 are not
complete.

## What changed

This slice closes part of the durable cleanup path described in
[`257-moby-runner-implementation-note.md`](257-moby-runner-implementation-note.md):

- Added a versioned, bounded cleanup command to the private runner protocol.
  The command binds the durable intent hash, workspace, attempt identity,
  worker owner, and fencing token. The response is checked against that exact
  command before it can leave the runner boundary.
- Added a cleanup worker that claims bounded workspace batches, renews the
  durable lease while the runner request is active, validates the observation,
  and records completion or a stable redacted retry through the Postgres store.
  Lease loss cancels the request and prevents a stale worker from writing.
- Added optional server composition for the worker when an authenticated
  `SandboxCleanupRunner` is explicitly injected. The ordinary local runtime
  does not inject a runner, and therefore does not consume cleanup jobs.
- Added protocol, transport, redaction, renewal, mismatched-response, and
  stale-lease tests.

## Verification

The following focused suites passed locally after the slice:

```text
go test ./internal/contracts ./internal/sandboxrunner \
  ./internal/operationworker ./internal/server -count=1
```

These tests use fakes and in-memory transports. PostgreSQL integration tests
were not run because `FORNIX_TEST_PG_DSN` is not configured and the available
environment lacks a usable PostgreSQL/pgvector service. Docker Engine tests
were not run because Docker is unavailable. The test result is not evidence of
database fencing or operating-system isolation.

## Still required for Task 116

- Integrate and pin the official Moby client/API modules. A download attempt
  failed because this environment could not resolve `proxy.golang.org`; no
  Engine implementation was substituted.
- Implement and adversarially test exact create/start/inspect/reconcile/log
  capture/remove semantics against an injectable Engine interface.
- Build a native host-runner lifecycle and safely pass its private socket to
  the local application without exposing the Docker socket to the control
  server.
- Wire the runtime into `fornix start`, with OCI disabled unless all required
  trust and profile qualification evidence is present.
- Run fresh/existing migration, RLS, concurrency, crash, Linux Engine, and
  macOS Docker Desktop qualification, then measure latency, API work, image
  transfer, scratch/output storage, and cleanup backlog.

Fornix is not yet ready for unattended OCI-backed production work. This note
records one code slice, not completion of Issue #40 or the universal
production-readiness roadmap.
