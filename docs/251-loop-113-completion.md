# Loop 113 — Path-free sandbox-runner protocol contracts

Status: implemented and unit-tested. No IPC listener, host runner, Docker
client, OCI provider, or qualified isolation tier is shipped.

## Outcome

Added versioned sandbox-runner request/response contracts and bounded
normalization in `internal/contracts/sandbox_runner.go`.

The request carries an opaque workspace-mount reference, the existing fenced
`SandboxExecutionIdentity`, tool ID and definition hash, structured arguments,
explicit environment, a workspace-relative working directory, and a
path-free runner profile. It does not carry an executable path, host path,
`AllowedWorkdirRoot`, Docker/OCI options, devices, ports, namespace selectors,
or an arbitrary mount list. A future runner must resolve the executable and
allowed environment keys from its own trusted catalog.

The path-free runner profile has its own canonical hash; the full profile hash
remains referenced from the execution identity without disclosing a profile path.
The contract only checks that the caller-supplied full-profile hash agrees with
that identity; the future runner must derive the effective profile from its
trusted tool/workspace policy and independently enforce the signed
qualification envelope. The
response is bounded, carries stable failure codes rather than raw diagnostics,
and echoes both the execution hash and normalized request hash. Mapping a
validated response to `ToolResult` preserves existing timeout retry semantics
and computes the established result hash.

## Independent review and corrections

An independent read-only review found that the first draft could expose an
absolute workdir root, implied a stronger request/identity binding than the
contract actually provided, and did not preserve timeout retryability. The
wire profile now excludes caller paths, the response hash is checked against
the exact supplied request, timeout mapping is retryable, and the documentation
now states the remaining trust requirements explicitly. The protocol hash does
not by itself prove a changed request is consistent with the durable
`ToolRequestHash`; the future provider must verify that binding before IPC, and
the runner must pin the first request hash to the deterministic runtime object
before starting it. The runner must also enforce its trusted definition/env
catalog, mount mapping, and signed qualification limits. These requirements
are not claimed as implemented here.

## Persistence, cost, and reuse

- No migration, new service, image, external dependency, or third-party source
  reuse was added. There is no persistent storage growth or SQL work.
- The deterministic request is capped at 1 MiB; output is capped by the
  requested stdout/stderr budgets, with protocol response JSON capped at
  16 MiB. These limits bound the future IPC payload, not runtime disk use.
- No execution latency or throughput claim is made. Contract tests complete
  in under a second on this host; that is not sandbox startup or tool latency.
- Docker documents that rootful daemon control can mount and modify the host
  filesystem and that resource enforcement depends on host capabilities. For
  that reason, Docker authority remains outside the Fornix server. A future
  Docker implementation belongs in a separate runner and requires exact target
  qualification. See [Docker Engine security](https://docs.docker.com/engine/security/),
  [Engine SDK guidance](https://docs.docker.com/reference/api/engine/sdk/),
  and [resource constraints](https://docs.docker.com/engine/containers/resource_constraints).

## Verification

Executed in this working tree:

```text
GOCACHE=/private/tmp/fornix-loop113-go-cache go test -p 2 ./internal/contracts -count=1
PASS
GOCACHE=/private/tmp/fornix-loop113-go-cache go test -race -p 2 ./internal/contracts -count=1
PASS
GOCACHE=/private/tmp/fornix-loop113-go-cache go test -p 2 ./... -count=1
PASS
```

Tests cover deterministic canonical hashing, workspace and execution scope,
path-free serialization, request and response size/output/time limits,
unsupported protocol/profile forms, aggregate argument limits, response
identity mismatch, exit/failure mapping, redacted failure messages, and
timeout retryability. Full Go package tests passed. PostgreSQL-backed
qualification was not available because `FORNIX_TEST_PG_DSN` is unset; live
Docker runtime tests were not possible because the daemon socket is inaccessible
in this environment. No OCI isolation, resource enforcement, cleanup, or crash
recovery claim follows from these tests.

`go vet ./...` and `make fmt-check docs-check` passed (256 Markdown files).
The full service smoke target was not run: its required local service/DB
prerequisites are not configured, and Docker daemon access is denied here. No
runtime smoke is relevant to this protocol-only slice. No commit or PR was
created, and existing working-tree changes were preserved.

## Remaining critical work

1. Implement and manage a separate, owner-authenticated runner process; keep
   Docker daemon authority out of the control server.
2. Bind the provider's complete invocation to `ToolRequestHash`; resolve the
   tool ID/hash and environment allowlist from the runner's trusted catalog;
   map opaque workspace references using symlink-safe host operations.
3. Implement deterministic create/start/inspect/wait/log/reconcile/cleanup
   through a typed Engine API client. Pin the request hash before start and
   never relaunch during reconciliation.
4. Add fake Engine API tests, then live qualification on explicitly disposable
   supported Docker Engine/Desktop and rootless/Linux targets. Do not enable
   the backend before the signed target-specific evidence is accepted.
