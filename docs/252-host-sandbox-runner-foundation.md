# Host sandbox runner and OCI provider

Status: implementation in progress. The code slices now include an
operator-bound mount catalog, authenticated Unix-socket protocol, bounded
request lifecycle, socket-path ownership checks, exact-hash tool catalog, and
fixed OCI container-plan builder. They are not wired into `fornix start` and do
not claim that an Engine-backed runner or OCI isolation is shipped.

## Problem and user outcome

Fornix currently has durable admission, effect identity, fences, and recovery
contracts, but its only selectable tool runtime is `local-process`. That
backend executes with the Fornix process identity and is not a filesystem,
network, CPU, memory, or process sandbox. A production-system control plane
cannot treat those limits as isolation.

The local product needs to run approved tools in an actual constrained
container without granting the main Fornix server control of the Docker
daemon. The user should still use `fornix start` and the existing operation
workflow; runner lifecycle and IPC are implementation details managed by the
local runtime.

## Non-negotiable invariants

1. Postgres remains authoritative for operation/effect reservation, task and
   agent fencing, request identity, approvals, final results, and replay.
2. The Docker Engine socket is accessible only to a separately managed native
   runner process. It is never mounted into the Fornix control-server
   container.
3. The control server reaches the runner through a dedicated private IPC
   socket, not a host-facing TCP listener. IPC authentication is mandatory;
   socket reachability alone is not authorization.
4. A missing runner, failed handshake, invalid qualification, unsupported
   runtime control, or corrupt identity fails closed. There is no fallback to
   `local-process`.
5. The runner resolves tool IDs and definition hashes, environment keys, and
   workspace mount references from its own operator-controlled catalogs. It
   does not accept executable paths, daemon options, or host mount paths from
   an API request.
6. Workspace mounts are explicitly registered on the host, workspace-bound,
   read-only by default, and checked for containment before every execution.
   Cross-workspace overlapping mount roots are rejected.
7. Runtime objects are deterministic per durable attempt. A duplicate with a
   different request hash is a conflict; reconciliation inspects the exact
   prior runtime object and never launches a replacement execution.
8. Containers are not auto-removed. Output must be recovered and committed to
   the authoritative tool/effect records before runtime cleanup. Unknown
   outcomes remain uncertain rather than being retried as if no effect ran.
9. OCI settings are constructed from a fixed allowlist: immutable image digest,
   structured argv, explicit environment, bounded resources/time/output,
   read-only root/workspace, no privileged mode, no host namespaces/devices,
   and no network unless a separately qualified profile authorizes it.
10. Qualification records the exact Engine/runtime version, image digest,
    profile, host capabilities, and test evidence. A successful fake-engine
    test is not evidence of kernel isolation.

## Runner and IPC shape

The local runtime manager will start a native `fornix runner` before starting
the application container. The runner alone uses the configured Docker Engine
connection. The Engine adapter must resolve the same endpoint selected by the
local runtime (including an explicitly selected Docker context); it must not
assume that the Moby SDK's environment defaults equal the Docker CLI context.
The app container receives only the runner IPC socket and a narrowly scoped
IPC credential; it receives no Docker socket or Engine API endpoint.
The HTTP client is pinned to the private socket, never follows redirects, and
returns only stable transport errors; listener diagnostics are not written to
the process-wide default logger.

The first transport is a bind-mounted Unix-domain socket. Linux Engine uses
the host filesystem socket directly. Docker Desktop must pass an explicit
startup handshake proving bidirectional AF_UNIX sharing; Docker Desktop 4.86.0
documents the VirtioFS fix for host/guest traffic in both directions. Require
that release or later for Docker Desktop and verify the active file-sharing
backend at startup. If the handshake fails, the OCI backend remains
unavailable. A runner-initiated authenticated connection to the already
loopback-published control endpoint is a later portability alternative, not an
implicit fallback.

The listener requires its immediate directory to be owned by the current
user and private (0700 or stricter). Every ancestor must be root- or
current-user-owned and not writable by group/other; only a root-owned sticky
temporary directory is allowed as a shared ancestor. Existing permissions are
never repaired in place. The standard library checks Unix ownership and mode
bits but does not enumerate macOS extended ACL entries, so this is not yet a
multi-user-host isolation claim. Deployments that rely on adversarial local
accounts still require platform-specific ACL qualification before enabling
the OCI provider.

Shutdown first refuses new requests, cancels active request contexts, closes
the listener, and waits for a bounded drain. If a handler remains active after
the graceful shutdown and post-close drain intervals, `Listen` returns
`ErrRunnerUnavailable` instead of hanging or claiming a clean stop. The runtime
may still have an uncertain external attempt; it is never retried here, and
the normal durable reconciliation path must inspect its exact identity before
any follow-up action.

The host-path model relies on the logged-in local operator and host
administrator being trusted. Docker's Engine API accepts a bind source as a
path string, not a pinned directory handle; holding an `os.Root` and checking
`SameFile` cannot make Docker atomically mount that inode. The runner rechecks
the canonical root immediately before Engine use, rejects symlinked roots and
cross-workspace physical/case aliases, and must reject remote Engine contexts
for local workspace mounts. This prevents API callers and sandboxed processes
from selecting or redirecting host paths, but it is not protection against a
host process with the same trusted identity or a daemon administrator changing
the path at the instant of container creation. Production multi-user runners
must protect the mount's parent chain and ACLs from principals outside the
trusted operator set. This also is not a point-in-time snapshot: trusted host
processes can change workspace contents between tool calls. If those stronger
guarantees are required, Fornix needs a qualified immutable snapshot or a
content-addressed staged copy, with its storage and indexing cost measured.

The runner accepts the existing versioned `SandboxRunnerRequest` only after
IPC authentication, canonical validation, and trusted-catalog resolution. It
uses the existing `AttemptAwareSandboxProvider` contract:

```text
Postgres effect authority
        │ exact identity + fence
        ▼
Fornix server ── private authenticated UDS ──► host runner
        │                                      │ trusted catalogs
        │                                      ▼
        │                                  Docker Engine
        │                                      │ deterministic container
        ◄──────── bounded result/reconcile ────┘
        ▼
atomic authoritative finalization, then cleanup
```

The server computes and checks the canonical durable tool request hash before
IPC. The runner binds the complete normalized wire-request hash to a
deterministic runtime name and labels. It must reject mismatched existing
labels, keep output out of labels/logs, and inspect rather than re-execute on
recovery. Cleanup is a separate operation after the database records the
result. A crash at any earlier boundary leaves the attempt available for
inspection or marks it uncertain.

## Workspace mount catalog

The host catalog maps an opaque `WorkspaceMountRef` to an explicitly selected,
canonical directory. It never learns a mount from an absolute path in the
container request. Registration validates the root and its workspace scope;
resolution rechecks that the root still identifies the same directory. A
relative workdir is resolved beneath that root using root-confined filesystem
operations. Path traversal, symlink escape, stale mount identity, and
cross-workspace overlapping roots fail closed, including physical aliases and
case variants on macOS. `ResolvedMount` serializes use and close of its open
root handle, while the Engine adapter still must recheck the host path
immediately before create. Docker includes nested bind submounts by default;
the Engine adapter must disable recursive mounts where supported and verify
the actual mount is read-only. Unsupported mount semantics make the provider
unavailable rather than weakening the profile. The catalog contains host
paths, so it is runner-local state and must never be logged or serialized into
events, tool evidence, or API responses.

## Trusted tool catalog foundation

The host runner needs an independent tool-definition check; IPC authentication
alone does not make a request's executable or argument policy trustworthy.
Before connecting an Engine, the runner will accept only operator-registered,
normalized OCI tool definitions keyed by the exact tool ID and definition
hash. Registration is append-only for the process lifetime: an identical
snapshot is idempotent, while a changed definition has a different hash and
must be registered as a separate snapshot. No request can register or mutate
catalog entries.

Resolution invariants are: the request's tool and sandbox-profile hashes must
match one registered snapshot; the path-free runner profile must exactly
equal that snapshot's normalized OCI policy; the executable and argv prefix
come only from the catalog; and request arguments remain structured argv.
The workspace mount reference must resolve to the same workspace, relative
working directories and declared path arguments must remain beneath its
root, and request-supplied environment remains denied. Invalid or unknown
requests fail closed with stable errors that do not include argv, host paths,
or environment values. This feature changes no database schema: Postgres
continues to own operation identity, authorization, and effect state.

The in-process `ToolCatalog` foundation implements these checks and returns a
host-path-free `ResolvedInvocation`; no Engine runtime consumes it yet.
Snapshots use exact definition and sandbox-profile hashes. If an admitted
effective profile changes, its exact snapshot must be registered with the
runner before a request using it can resolve. This intentionally favors a
fail-closed catalog miss over accepting a policy that was not installed in
the runner.

Reuse the existing `ToolDefinition.Normalize`/`Hash`,
`SandboxRunnerRequest.Normalize`, and `MountCatalog` rather than introducing a
second tool schema. This is original Fornix glue; no upstream source is copied
and there is no new dependency or license obligation. The steady-state cost is
one bounded in-memory lookup plus bounded argument/path checks per invocation;
catalog memory grows with explicitly configured definition snapshots, not
requests. Acceptance tests cover changed/unknown hashes, profile mismatch,
shell/path substitution, argv-prefix mismatch, budget enforcement, path
traversal and symlink escape, workspace mismatch, idempotent registration,
concurrent resolution, and absence of sensitive values in errors.

## OCI container-plan feature note

Before the Moby adapter is connected, the host runner needs one deterministic
translation from an authorized invocation to the only container settings it
may create. `BuildOCIContainerPlan` accepts a normalized request, a trusted
tool catalog, and the matching resolved workspace mount; callers do not supply
image, daemon options, mounts, environment, capabilities, network, or
namespace settings. The plan fixes the immutable image digest, argv
entrypoint, host user's non-root numeric UID/GID, no network, dropped
capabilities, no-new-privileges, read-only root and workspace, non-recursive
bind semantics, and bounded CPU/memory/PID/tmpfs budgets. Its name is derived
from the execution identity; labels contain hashes and schema metadata only.
Host paths remain runner-local and must not be serialized or logged.

The plan builder does not create a container or prove that an Engine enforces
the plan. An eventual adapter must validate the plan immediately before each
Engine mutation, inspect the resulting container's security/resource/mount
configuration, and fail closed if the selected Engine does not support an
option. This is in-memory runner policy only: no migration or authority
changes. The returned plan is opaque outside the runner package; the Engine
adapter must obtain a validated, deep-copied snapshot immediately before
building its SDK request. Root UID or GID is rejected. Reuse the existing
normalized runner profile, tool catalog, mount handle, and execution identity.
No Docker SDK source is copied; Moby remains
the intended Apache-2.0 client dependency, which is not available in this
offline environment. Unit tests cover deterministic identity, non-root
selection, fixed-deny controls, budget mapping, no secret/argv labels, and
tamper rejection. Docker's primary references for [container creation and
host configuration](https://docs.docker.com/reference/api/engine/) and
[tmpfs isolation and memory accounting](https://docs.docker.com/engine/storage/tmpfs)
inform the plan; passing these unit tests is not a runtime qualification.

## Recovery redaction and cleanup gates

Keep the OCI provider and automatic runtime recovery/cleanup disabled until
both gates below are implemented end to end and qualified:

1. `ToolRequest.RedactedEvidence` intentionally replaces environment values,
   while the current runner reconciliation call carries only the execution
   identity. A restarted runner therefore cannot assume it can reconstruct
   every output-redaction token from Postgres. The path-free OCI request
   contract now rejects all request-supplied environment entries, and its
   profile rejects inherited host environment. The actual provider is not yet
   wired, so this is a contract-level guard rather than live runtime evidence.
   A later credential-bearing profile needs an explicit
   credential-reference/lease and restart-safe redaction design before it is
   enabled.
2. Removing the runtime object immediately after `ToolRunStore.Finish` leaves
   a crash window between the database commit and the Engine removal. Migration
   081 now supplies a durable Postgres intent tied to the finalized
   tool/effect identity, with fenced bounded retries and append-only status
   history. No process consumes that queue yet. The runner must not accept a
   cleanup request based only on a bearer token or caller assertion that the
   result was committed. Until a qualified consumer verifies the exact durable
   identity, retain the runtime object for recovery and keep automatic cleanup
   unavailable.

These are implementation prerequisites, not optional hardening. The Engine
adapter must pin and verify the complete canonical request hash and execution
identity at create, start, reconciliation, and cleanup; a matching deterministic
container name alone is insufficient evidence. The durable queue is cleanup
authority bookkeeping, not proof that a runner removed anything.

## Schema and recovery

Migration 081 adds workspace-scoped cleanup jobs and append-only cleanup
events. Existing Postgres tool runs, generic effect links, attempt identity,
fencing, and recovery records remain authoritative for execution. Runner-local
transport credentials and mount registrations live under the private Fornix
profile with restrictive permissions; they are configuration, not operation
history. Runtime labels contain only versioned identity hashes and stable IDs,
never command arguments, environment values, prompts, credentials, or raw
output.

If a later multi-host/remote runner needs durable registration leases or a
server-side dispatch queue, that is a separate schema change. It must not
promote an in-memory queue or runner journal over the existing Postgres
authority.

## Reuse and licensing

- Reuse Fornix's `AttemptAwareSandboxProvider`, signed sandbox qualification,
  `SandboxRunnerRequest`/`Response`, effect reservation, fencing, and recovery
  finalization. The immediate code gap is the host mount catalog and transport.
- Orloj's governed runtime resolver and fail-closed unavailable-backend
  behavior inform provider selection; no source code is copied.
- OpenSandbox's secure-runtime proposal and isolated-execution API inform the
  distinction between sandbox lifecycle, per-execution identity, capabilities,
  and explicit fallback. No source code is copied; its reference checkout is
  Apache-2.0, but the planned implementation does not need to vendor it.
- For the Docker Engine adapter, use the official Moby Go client
  (`github.com/moby/moby/client`) instead of invoking a shell or reimplementing
  the REST protocol. The current `client/v0.6.0` module declares Go 1.24,
  compatible with this module's Go 1.25 baseline; Moby is Apache-2.0. The
  dependency was not added because this environment cannot resolve
  `proxy.golang.org`; therefore its transitive size and binary impact have not
  been measured. Resolve the module and record its license/size impact before
  implementing the Engine adapter. Docker documents automatic API-version
  negotiation; use it rather than an ambient `DOCKER_API_VERSION` override.
- Do not copy Kronaxis Fabric source (BSL 1.1). No third-party implementation
  code is copied in this slice.

## Cost and storage budget

The IPC request/response limits remain those in the typed contract. The
control server adds no container-engine dependency or daemon connection. The
native runner adds one bounded process and an Engine SDK dependency; record
binary-size and dependency-graph deltas before release. Keep one retained
container only while its authoritative attempt is unfinished; cleanup is
idempotent after finalization. Avoid duplicate image layers by pinning one
qualified base image per profile. Report cold-start and warm-start latency,
runner RSS, temporary container storage, result recovery, and cleanup rate
from real host measurements; no performance target is claimed here.

## Acceptance and qualification matrix

### Unit/fake-engine tests

- Host catalog rejects invalid refs, symlinked roots, root changes, traversal,
  cross-workspace roots, and nested cross-workspace overlaps.
- Physical same-directory aliases and case-variant aliases fail closed;
  live qualification covers Linux bind-mount aliases and macOS's active
  filesystem behavior.
- Every request is authenticated, bounded, workspace-bound, profile-bound,
  and matched to the exact execution identity and durable request hash.
- Tool and environment catalogs reject unknown IDs, changed definitions,
  unapproved environment keys, and arguments outside the definition.
- Engine creation uses only the fixed security/resource options and a digest
  reference; no shell, host namespace, device, privileged mode, or implicit
  inherited environment is possible.
- Duplicate create is idempotent only for the same identity and request hash.
  Recovery inspects; it never executes a second command.
- Output, timeout, cancellation, runner restart, stale fences, request
  conflicts, cleanup ordering, and redaction are deterministic.
- Startup reports unavailable when the private socket handshake or Engine
  preflight fails. No fallback provider is selected.

### Live qualification (required before enabling the provider)

- Run the same matrix on supported macOS Docker Desktop with VirtioFS and
  supported Linux Docker Engine, recording exact versions and architecture.
- Verify request/result round-trip across the mounted UDS; reject an ordinary
  HTTP client without the IPC credential.
- Reject remote Docker contexts, unshared Desktop paths, unsupported
  non-recursive/read-only mount behavior, and nested submount escape attempts.
- Prove read-only workspace/root behavior, no network in the offline profile,
  CPU/memory/PID/scratch/output/time limits, and cancellation of descendants.
- Inject crashes before create, after create, during start, while running,
  after exit/before result commit, after result commit/before cleanup, and
  during runner restart. Verify no duplicate execution and no premature
  container deletion.
- Run disposable-Postgres tests for migration compatibility, RLS,
  cross-workspace access, fencing, atomic finalization, and recovery.
- Sign qualification evidence for each exact runtime/build/profile. Until the
  required target evidence is imported and verified, the provider is not
  available in the default registry.

## Current limitations

The typed UDS HTTP transport, host mount catalog, trusted tool-catalog
foundation, and deterministic fixed-policy OCI plan are implemented, but no
Engine consumes them and the host runtime is not wired into the local runtime
manager. The Engine client, invocation/response lifecycle integration,
container lifecycle, cleanup consumer, and live qualification remain open.
Listener setup creates missing path components one at a time only below a
validated owner-controlled or root-owned sticky ancestor, verifies ownership
and replaceability of the complete chain, and refuses unsafe existing
permissions without changing them. Symlink components are rejected before
creating descendants, so a rejected alias cannot mutate its target. This
avoids silently tightening permissions on an operator-selected path.
The current execution sandbox denies Unix-domain listener creation, and this
host has no usable Docker daemon or configured disposable PostgreSQL DSN.
Handler/mount unit tests and static checks can run here; UDS end-to-end,
host-isolation, and database-backed qualification cannot be claimed. The
transport tests explicitly report the denied listener as skipped rather than
passing it as runtime evidence. The latest attempted Engine SDK resolution
failed before modifying `go.mod` because `proxy.golang.org` could not be
resolved. See [Loop 113](251-loop-113-completion.md) for the earlier
protocol-only status.

Latest local verification: `go test ./...`, `go test -race ./...`,
`go vet ./...`, `make package-check`, `make fmt-check`, `make docs-check`, and
`git diff --check` pass. The race run explicitly skipped listener-backed UDS
round trips because `bind(2)` returns `operation not permitted`; the in-memory
HTTP lifecycle, handler, and mount-catalog tests did execute. Database tests
guarded by `FORNIX_TEST_PG_DSN` and live Engine/isolation checks are not
qualified by this local run.

## References

- [Docker Engine security](https://docs.docker.com/engine/security/)
- [Docker Engine Go SDK](https://docs.docker.com/reference/api/engine/sdk/)
- [Moby Engine client v0.6.0 module metadata](https://github.com/moby/moby/blob/client/v0.6.0/client/go.mod)
- [Moby license](https://github.com/moby/moby/blob/master/LICENSE)
- [Docker Engine API and version negotiation](https://docs.docker.com/reference/api/engine/)
- [Docker Desktop release notes](https://docs.docker.com/desktop/release-notes/)
- [Docker bind mounts](https://docs.docker.com/engine/storage/bind-mounts/)
- [OpenSandbox secure container runtime proposal](https://github.com/alibaba/OpenSandbox/blob/82143b6c2d65698718d63e53cbf5aec3d40c4208/oseps/0004-secure-container-runtime.md)
- [OpenSandbox isolated execution API proposal](https://github.com/alibaba/OpenSandbox/blob/82143b6c2d65698718d63e53cbf5aec3d40c4208/oseps/0013-isolated-execution-api.md)
- [Orloj governed tool runtime](https://github.com/OrlojHQ/orloj/blob/e6b723b3df582cc262d6876715d5877b4b61226e/runtime/tool_runtime_governed.go)
- [Orloj container runtime](https://github.com/OrlojHQ/orloj/blob/e6b723b3df582cc262d6876715d5877b4b61226e/runtime/tool_runtime_container.go)
