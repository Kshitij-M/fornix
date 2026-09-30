# Task 116 — Moby attempt runner and durable cleanup consumer

Status: partial control-plane implementation; OCI runtime is not implemented
or qualified.

## Goal and scope

Turn the existing OCI plan, authenticated host-runner protocol, recovery
identity, and Postgres cleanup queue into an executable host-side runtime.
The runtime must use the official Moby Go client, execute one exact attempt,
inspect that same attempt after uncertain delivery, and remove only a
Postgres-authorized completed attempt. The existing `fornix start` flow must
not enable OCI until the native runner, secure IPC handoff, trusted catalogs,
and supported-host qualification are all present.

This task does not weaken the default local-process policy, enable OCI in
production, or claim kernel isolation from fake-engine tests. If local runtime
startup cannot safely supply the native runner and socket, the adapter remains
unavailable and the gap must stay visible in readiness output.

## Invariants

1. PostgreSQL remains authoritative for operation/effect/task fences, tool
   results, and cleanup leases. The Moby daemon is an execution mechanism, not
   a second durable work ledger.
2. A host-local Engine endpoint is explicitly configured and validated. Do
   not inherit Moby SDK defaults or silently select remote Docker contexts.
   Host bind mounts require a daemon-local path on the same machine.
3. Every create/start/inspect/log/stop/remove call is derived from a validated
   `OCIContainerPlan` and immutable execution identity. Caller-provided image,
   mount, device, namespace, network, daemon, or security options are rejected.
4. Container name and labels bind the complete execution/request/plan hashes.
   A name collision with different labels or security configuration is a
   conflict; never adopt, start, or remove it.
5. Create is idempotent by deterministic name. A retry inspects the exact
   container; it never creates a second container. Start occurs only after
   exact pre-start policy inspection and is not repeated for running/exited
   attempts.
6. stdout/stderr capture is bounded while streaming, not after buffering all
   daemon logs. Timeout, cancellation, nonzero exit, output overflow, and
   daemon disconnect have stable result/recovery classifications.
7. Cleanup commands carry the complete immutable cleanup intent plus the
   current workspace/job/owner/fence. The runner verifies exact labels and
   configuration, removes no volumes, and returns a bounded observation. Only
   the fenced Postgres store can complete or retry the job.
8. Unknown, mismatched, or unverifiable Engine state fails closed and retains
   the object. No transport error is interpreted as proof of absence.
9. No host paths, argv, raw output, credentials, or Engine endpoint details
   enter logs, events, labels, or public responses.
10. OCI remains excluded from the default tool registry until signed runtime
    qualification is accepted for the exact Engine, host, and profile.

## Runtime and recovery semantics

The Moby implementation exposes an injectable narrow Engine interface so
deterministic tests can cover SDK mapping and crash boundaries without a live
daemon. The implementation maps `OCIContainerPlan` to create options, inspects
the object before start, starts once, waits with a deadline, streams bounded
logs, and re-inspects before returning a result. Reconciliation only inspects
the deterministic object and never starts it. Cleanup inspects and removes
only the exact identity-bound object with `Force=false` and
`RemoveVolumes=false`; repeated cleanup of that same absent object returns
`already_absent`.

The private IPC protocol gains a versioned, bounded cleanup command and
observation. The durable cleanup worker claims bounded workspace batches,
renews fences while the runner call is in flight, and records only a
normalized complete/retry transition. A lost lease cancels future control
plane writes; an external removal may still have happened and is safely
reconciled as an idempotent exact-object cleanup.

The cleanup command/observation protocol and client call now exist. A bounded
worker renews the cleanup fence during the call, validates the returned
identity, converts adapter errors into stable retry evidence, and commits only
through the Postgres cleanup store. The server can compose this worker only
when a deployment explicitly injects an authenticated runner client. The
ordinary `fornix start` path supplies none, so no cleanup queue is consumed by
default. Focused contract, transport, worker, and server tests pass locally;
Postgres-backed behavior remains DSN-gated.

## Reuse and licensing

Reuse Fornix's `OCIContainerPlan`, `ToolCatalog`, `MountCatalog`, authenticated
Unix-socket client/handler, `SandboxExecutionIdentity`, cleanup intent/job
contracts, and `SandboxCleanupStore`. Use the independently versioned official
`github.com/moby/moby/client` and `github.com/moby/moby/api` modules; do not
invoke the Docker CLI and do not hand-roll Engine HTTP. Pin the reviewed
client/API versions and preserve their upstream Apache-2.0 notices through
normal dependency/SBOM reporting. Do not copy Kronaxis source. No broker,
new database, object store, or orchestration framework is introduced.

## Cost and operational impact

One attempt uses a bounded create/inspect/start/wait/log stream/final inspect;
cleanup uses a bounded claim/inspect/remove/complete flow. SDK connection and
worker concurrency are capped. Image transfer is separate from steady-state
latency and is never implicit during execution. Scratch is tmpfs and removed
with the exact container; no named or anonymous volumes are created. Record
container-create/start/wait/capture/cleanup latency, Engine API calls, image
pull bytes, captured output bytes, cleanup backlog age, and retry counts on
each supported platform before publishing performance claims.

## Acceptance tests

- SDK configuration uses only the explicit local endpoint and rejects remote
  or ambiguous endpoints.
- Exact plan-to-Engine mapping covers immutable image, argv, user, mounts,
  read-only root/workspace, no network, dropped capabilities,
  no-new-privileges, cgroups, tmpfs, labels, and no volumes.
- Existing exact attempt is reused by inspection; hash/config collisions fail
  closed; retries never create or start a second attempt.
- Pre-start mismatch prevents execution; reconciliation never executes;
  running/exited/unknown states map deterministically.
- Output is capped while streaming; cancellation, timeout, daemon loss, and
  overflow do not leak unbounded bytes or diagnostics.
- Cleanup verifies identity, never removes volumes, supports exact
  already-absent replay, and rejects name/label/config mismatch.
- Cleanup worker tests bounded claims, renewal, expiry/takeover, stale fences,
  retry/dead-letter, cancellation, duplicate delivery, and crash after remove
  before Postgres completion.
- Workspace, actor, task, operation, effect, attempt, request, and result
  hashes survive the full tool→runtime→result→cleanup path.
- All migrations, PostgreSQL RLS/rollback/concurrency tests, race tests,
  offline smokes, and existing suites pass. Live Linux Engine and macOS Docker
  Desktop qualification remain separate required gates.

## Current implementation limits

The official Moby modules are not currently in `go.mod` or the local module
cache. The current development Compose topology also does not start a native
host runner or pass its private socket into the application container. Those
are dependencies of the end-to-end user path, not details to paper over with a
fake provider or direct Docker-socket mount into the control server.

Loop 121 now supplies the engine-independent attempt lifecycle coordinator
behind an injected `OCIEngine`; it provides no Moby method mapping or host
runtime behavior. Follow its qualification limits in
[`269-oci-attempt-lifecycle-foundation.md`](269-oci-attempt-lifecycle-foundation.md)
and [`270-loop-121-completion.md`](270-loop-121-completion.md). Moby currently
documents `github.com/moby/moby/client` and `github.com/moby/moby/api` as its
supported client/API modules and marks `github.com/docker/docker` deprecated;
the integration should pin the independently released modules rather than
the Moby engine-source module. See the
[upstream module guidance](https://github.com/moby/moby#go-modules).

The cleanup protocol and bounded worker have since been implemented. The
server composes that worker only when a deployment injects an authenticated
runner client; normal `fornix start` supplies none, so cleanup is not consumed
by default. An attempt to fetch the official Moby modules failed because this
environment could not resolve `proxy.golang.org`, so no Engine adapter was
implemented or compiled. OCI execution, native runner lifecycle, default
cleanup consumption, PostgreSQL-backed qualification, and supported-host
Docker smoke tests remain open.
