# Narrow host sandbox-runner protocol foundation

Status: design and implementation note for the first typed boundary between
Fornix's control plane and a separately trusted runtime runner. This slice
does **not** ship an OCI executor, runner daemon, Docker integration, or a
qualified isolation tier.

## Problem and decision

Fornix's server runs inside the managed application container. It does not
have, and must not receive, Docker daemon authority. A server-container path
such as `/workspace/repository` is not a host path that may be passed to a
runtime daemon. The next sandbox step therefore begins with a strict,
versioned execution protocol whose authority can later be implemented by a
separate local host runner or deployment-managed isolated worker.

The runner receives only a typed capability execution request. A workspace is
referenced by an opaque mount identifier registered out of band with the
runner; the protocol never carries a host path, executable path, Docker option,
runtime socket, or arbitrary container-creation object. The runner resolves
the tool ID and exact tool-definition hash in its own trusted catalog, which
selects the executable and allowed environment keys. Workspace-relative
working paths are validated lexically here and must later be resolved under
the runner's pre-authorized mount with symlink-safe filesystem operations.
Runtime implementation and authenticated transport are separate next steps
and remain unavailable until independently tested.

Docker warns that controlling a rootful daemon can mount and modify the host
filesystem, and says a service provisioning containers must carefully validate
all parameters. The protocol is intentionally narrower than the Docker Engine
API. If Docker is selected for a later runner implementation, it should use a
typed API client in the runner process, never in the Fornix server; resource
limits must be verified against the actual daemon/host, since Docker documents
that enforcement depends on host kernel support. OCI and Docker containers are
not represented as VM-equivalent isolation. See [Docker Engine security](https://docs.docker.com/engine/security/),
[Docker Engine SDK guidance](https://docs.docker.com/reference/api/engine/sdk/),
[Docker resource constraints](https://docs.docker.com/engine/containers/resource_constraints),
and the [OCI Linux runtime configuration](https://github.com/opencontainers/runtime-spec/blob/main/config-linux.md).

## Invariants

1. **No daemon authority in the control plane.** Neither development nor
   packaged server configuration may mount or expose the Docker socket. A
   future runner is a separate process/security principal and must be
   explicitly enabled and qualified.
2. **No caller-selected host paths.** Requests identify an opaque,
   pre-authorized workspace mount. The runner's own mount catalog resolves it
   to a path; neither a request nor a profile can add mounts.
3. **No generic runtime API.** The request contains a registered tool ID and
   definition hash, structured argv, environment, a workspace-relative
   working path, a digest-pinned image, and a path-free normalized runner
   profile. The runner resolves the executable and allowed environment keys
   from its own catalog. The protocol has no arbitrary Docker/OCI JSON, host
   namespace, device, port, host path, or volume field.
4. **Bind to existing durable authority.** The request carries the exact
   `SandboxExecutionIdentity`, including workspace, tool attempt, effect,
   current operation/task/agent fences, definition/profile hashes, and signed
   qualification hash. It cannot reserve, start, or complete an effect on its
   own.
5. **Exact-attempt recovery only.** Later transport/provider code may reconcile
   only the deterministic attempt already identified by the durable effect.
   It must never start a replacement during reconciliation, adopt a mismatched
   runtime object, or fall back to local process execution.
6. **Bound and redact data.** Request and response bytes, argv, environment,
   output, and execution time are bounded. The protocol is sensitive because
   argv/environment can contain user data; implementations must not log or
   persist them in runner diagnostics. The future runner must enforce its
   catalog's environment-key allowlist and reject credential material; this
   contract's syntax check is not that allowlist.
7. **Honest qualification.** Contract tests prove canonicalization and
   rejection of unsafe shapes only. They do not prove runtime isolation,
   cleanup, network denial, resource enforcement, or crash recovery.

## Protocol and persistence impact

Add versioned typed request/response contracts and deterministic validation in
`internal/contracts`. The runner request binds:

- request/workspace identity and an opaque workspace-mount reference;
- the exact fenced `SandboxExecutionIdentity`;
- a registered tool ID and content hash, with the executable resolved only
  from the runner's trusted catalog;
- immutable image digest, structured argv, explicit environment, and
  workspace-relative working directory;
- the effective normalized, path-free runner profile and its separate hash.
  The full effective profile hash remains bound to the durable execution
  identity; it is not sent as a filesystem path.

The response is bounded and carries the same execution identity/hash, the
hash of the complete normalized request, status, exit code, timestamps, and
bounded stdout/stderr. Response validation rejects a response paired with a
different supplied request. That hash alone does **not** prove that a changed
request is consistent with the durable `ToolRequestHash`: before sending,
the future provider must recompute the original `ToolRequest.RequestHash()`
and compare it with the execution identity; the host runner must atomically
bind the first request hash to the deterministic runtime object before start
and fail closed on any mismatch during retry or recovery. Likewise, the runner
must resolve the exact tool definition and enforce its environment-key policy
from its own trusted catalog, and enforce the qualification's tested maximum
limits even when a request supplies a smaller budget. The path-free wire
profile hash binds the serialized controls but does not independently prove
that they were derived from the original full profile. This is a protocol
contract, not an authorization proof. It carries no host path, daemon
identifier, credential, or unbounded diagnostic string. No database migration
is justified by this protocol-only slice: current tool/effect
records already preserve the authoritative request, attempt, fence, and
result. A later runtime adapter must demonstrate whether an opaque runtime ID
or recovery cursor is genuinely required before adding schema.

## Transport, runner lifecycle, and future runtime

This slice deliberately does not decide the final transport or launch a
privileged process. The follow-on runner must select a platform-appropriate
private authenticated IPC mechanism, enforce owner-only credentials and
permissions, reject replay/stale fences, atomically pin a request hash to each
runtime attempt, rate/size-limit requests, and map mount references only from
its local trusted registry. The provider must verify the incoming structured
invocation against the durable tool request and registered definition before
the IPC call. The primary candidate
for Docker is a small separate host-side process using the official typed Go
Engine client, with API-version negotiation; the client dependency belongs in
that runner package only. A Docker CLI prototype is not equivalent evidence,
and `runc` would make Fornix own image-rootfs and low-level lifecycle details.

For any Docker implementation, the fixed request policy must use a verified
preloaded digest (no tool-time pull), no shell, non-root user, read-only root
and workspace, no network/devices/host namespaces/ports, dropped capabilities,
no-new-privileges, and explicit CPU/memory/PID/scratch/output/time limits.
Unsupported hard limits fail closed. Containers remain kernel-sharing and
their guarantees are target-specific. Rootless mode is preferable where
supported, but its actual resource controllers still require detection and
qualification. Retain bounded logs only until the authoritative result is
committed; reconcile exact identity before cleanup, and never broadly prune.

## Reuse, licensing, and cost

Reuse Fornix's `SandboxExecutionIdentity`, signed qualification gate,
`AttemptAwareSandboxProvider`, effect dispatcher, and current workspace/tool
authorization. Do not copy source from reference repositories. This contract
adds no third-party dependency and no database/storage growth. The later
Docker runner may use Docker's official Go SDK; its exact module version,
transitive graph, license, supported Go version, binary size, and release
impact must be reviewed before adoption. Pulling or retaining large images is
operator-controlled, outside tool execution, and is a material disk-cost item
that must be measured before enabling a runtime by default.

## Acceptance tests for this slice

- Canonical requests validate deterministically and hash identically.
- Request execution workspace, backend, qualification, tool definition, and
  supplied full-profile hashes agree with the nested durable execution
  identity; the path-free runner profile has its own stable hash. The actual
  provider must derive and verify the full profile from trusted policy.
- Empty, malformed, or cross-workspace mount references fail closed.
- Absolute or traversal working paths, arbitrary runtime options, unpinned
  images, unbounded environment/output, and unsupported protocol versions are
  rejected. The runner's trusted catalog—not request fields—must reject shell
  executables and definition drift.
- Request/response serialization remains within the protocol byte cap.
- Result identity or request-hash mismatches cannot be accepted as a response
  to the exact request object supplied for validation. Durable invocation and
  runtime-object binding remain acceptance tests for the actual runner.
- Existing local-process behavior and default registry remain unchanged; no
  stronger provider becomes available by adding the protocol types.
- Existing tests, formatting, docs checks, and smoke paths remain green.

## Deferred evidence

The host's Docker executable is present, but its daemon socket is inaccessible
in this development environment. Therefore this work cannot run a real
container or qualify filesystem/network/resource/process-tree isolation. A
later slice needs an explicitly provisioned disposable daemon/VM, a supported
OS/runtime matrix, adversarial conformance tests, bounded runtime-crash tests,
and a deployment-owned signer/trust key before registry admission can be
enabled.
