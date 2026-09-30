# Tiered sandbox execution foundation

Status: feature note and partial implementation record for Issue #28. The
capability contract and fail-closed provider selection are implemented; no
runtime-backed isolation tier has been implemented or qualified.

## Problem and current behavior

Fornix already has useful control-plane protections: tool definitions are
registered, policy denies by default, argv is structured, the environment is
not inherited, output and wall time are bounded, approvals are durable, and
task-bound execution is fenced. These controls decide *whether* a capability may
run and constrain its request. They do not isolate the child process from the
host.

`internal/tool.LocalExecutor` invokes the registered absolute executable as a
same-identity host process. `ReadOnlyWorkdir` performs path checks for declared
path arguments; it does not mount the directory read-only. `AllowNetwork=false`
is not a network namespace or an egress block. On Unix, timeout and output-limit
cancellation kills the child process group and bounds inherited-pipe cleanup;
a child that deliberately escapes the group is not contained. An abrupt Fornix
process or host crash bypasses in-process cleanup and can leave descendants
running; durable tool recovery does not prove that a host process disappeared.
A child can still use host filesystem, network, process, and resource authority
available to the Fornix user. The local tier does not impose CPU, memory, PID,
or storage limits. It is therefore suitable only for trusted, low-risk built-ins
on a trusted machine—not for hostile generated code or untrusted multi-tenant
execution. On platforms where process-group termination is unavailable, this
provider reports unavailable and default tool admission fails closed.

Built-in post-change validators currently execute trusted Go callbacks in the
Fornix process; they do not spawn subprocesses. Their filesystem roots and
budgets are application-level controls, not a sandbox for third-party validator
plugins. Repository change application is a separate host-filesystem mutation
boundary and retains its own path check/use and crash-atomicity qualification
work; this feature does not claim to solve that boundary by wrapping validators.

## Runtime placement constraint

Fornix Local's control server runs inside a container. Both the development
Compose file and the embedded local-runtime manifest intentionally omit the
Docker daemon socket. The server therefore cannot safely implement an OCI
backend by issuing host Docker commands, and a path such as
`/workspace/repository` inside the Fornix container is not automatically a
valid host path for a nested runtime mount.

Do not solve this by mounting `/var/run/docker.sock` into the control server.
Docker documents that control of a rootful daemon can create a container with
the host filesystem mounted and modified, so daemon access expands the trust
boundary to host-root authority. A future execution tier must put runtime
authority behind a deliberately narrow runner boundary: a local host runner
with a private authenticated IPC contract for single-user installs, or a
deployment-managed isolated worker for production. The control plane must send
typed capability requests and opaque, pre-authorized workspace mount/artifact
references—not arbitrary Docker options or host paths. The runner must itself
be treated as a privileged security component and qualified separately.

Until that placement and workspace-transfer contract exists, OCI, gVisor, and
microVM remain unavailable. No Docker daemon socket is part of the default
Fornix Compose surface.

## Research and reuse decisions

The implementation should reuse Fornix's existing `ProcessExecutor` seam,
workspace/task/agent-run fencing, policy admission, durable tool-run lifecycle,
artifact output path, and deterministic request hashes. It should not copy
source from another repository or introduce a general container-orchestration
framework.

The upstream references support a layered design rather than calling all
containers “sandboxed”:

- [Dagger](https://github.com/dagger/dagger) demonstrates typed, repeatable,
  content-addressed container operations; its runtime still depends on a
  container runtime.
- [OpenSandbox lifecycle spec](https://github.com/opensandbox-group/opensandbox/blob/main/specs/sandbox-lifecycle.yml)
  separates creation, execution, pause/resume, and disposal. Fornix should keep
  lifecycle and cleanup explicit without adopting a new service in the default
  installation.
- [gVisor security model](https://gvisor.dev/docs/architecture_guide/security/)
  describes its userspace application kernel as a distinct isolation layer,
  not as an ordinary container or VM. [gVisor installation guidance](https://gvisor.dev/docs/user_guide/install/)
  requires an explicitly installed OCI runtime; it is Linux-specific.
- [Firecracker production setup](https://github.com/firecracker-microvm/firecracker/blob/main/docs/prod-host-setup.md)
  requires KVM plus the jailer or equally restrictive host controls, with
  cgroups, namespaces, privilege dropping, and resource limits. It is an
  optional Linux deployment tier, not a portable default.
- The [OCI runtime configuration](https://github.com/opencontainers/runtime-spec/blob/main/config.md)
  provides the portable vocabulary for process identity, mounts, read-only
  root filesystems, namespaces, cgroups, and limits. Actual enforcement remains
  runtime- and host-dependent.

No upstream source is copied. There is no new Go dependency in the initial
design, and no change to Fornix's MIT license. Any future adapter that vendors
or copies third-party implementation code must be separately reviewed for
license compatibility and attribution.

## Invariants

1. **An isolation request never silently degrades.** The selected backend must
   match the requested backend and prove every required capability before a
   process starts. An unavailable gVisor runtime or microVM provider fails
   closed; it never falls back to an ordinary container or host process.
2. **The host tier is named honestly.** `local-process` is same-user host
   execution with bounded argv, environment, time, and output only. It cannot
   assert host-filesystem isolation, network isolation, a read-only mount, or
   hard CPU/memory/PID limits. Admission may use it only for explicitly
   trusted local capabilities whose policy does not require those controls.
3. **Every stronger tier is explicit and content-bound.** OCI images must be
   specified by immutable digest. The execution record binds the tool
   definition, normalized request, sandbox profile, image/runtime digest,
   mounted input manifest, and result hash. Mutable tags are not execution
   identities.
4. **Mounts are minimal and typed.** The initial container profile exposes
   only the registered workspace root at a fixed in-container path, read-only
   unless a separately authorized write capability explicitly requests a
   writable mount. Host home directories, credential files, the Docker socket,
   arbitrary host paths, and implicit volumes are never mounted.
5. **Network and credentials are deny-by-default.** Container execution uses a
   network-disabled mode unless a separately qualified egress proxy capability
   is selected. No host environment is inherited. Only explicitly allowed
   request values are supplied; secret values are not recorded in profile,
   event, evidence, metrics, or logs.
6. **Budgets are intersections, not overrides.** Effective argument count/size,
   environment count/bytes, wall time, stdout/stderr, CPU, memory, PID count,
   and scratch storage are the strictest non-zero limits from the registered
   definition, workspace policy, caller request, and backend ceiling. A backend
   that cannot enforce a requested hard limit rejects the run.
7. **Authority remains in Fornix.** Sandbox creation and completion remain
   workspace-scoped and bound to current task/agent-run fences. Stale workers
   cannot publish results, links, or artifacts. The runtime is never the
   authority for task state.
8. **Uncertain execution is not retried blindly.** A crash after runtime
   creation but before result commit is reconciled using a deterministic
   sandbox-run identity and runtime label. If the backend cannot prove the
   prior execution's state/output, the tool run becomes recovery-required; the
   system does not launch a duplicate process or claim exactly-once execution.
9. **The default path remains offline and dependency-free.** Existing default
   smokes use the explicitly limited host tier with trusted fixtures. Stronger
   tiers are opt-in and do not pull images, install runtimes, or add persistent
   services during ordinary startup.

## Proposed tier matrix

| Tier | Guarantee | Prerequisites | Status after the first implementation slice |
|---|---|---|---|
| `local-process` | Structured argv, explicit environment, timeout/output budgets; same host identity | None | Available only as an explicitly limited trusted-host profile |
| `oci-container` | Namespaced container, fixed read-only root, explicit workspace mount, no network by default, dropped capabilities, no-new-privileges, CPU/memory/PID/scratch limits | Supported container runtime and digest-pinned image | Not implemented; planned first stronger runtime tier, not equivalent to a VM boundary |
| `gvisor` | OCI controls plus the configured `runsc` application-kernel boundary | Linux, Docker/containerd configured with `runsc`, supported architecture | Not implemented; must be detected and qualified without runtime fallback |
| `microvm` | Separate guest-kernel/KVM boundary with jailer, cgroups, namespaces, privilege drop, and explicit guest mounts/network | Linux/KVM, Firecracker/jailer, deployment-owned images and networking | Not implemented; optional provider only after deployment-specific qualification |

The issue's expected migration `034` is stale: the repository currently has
migrations through `080`. A dedicated sandbox migration is not automatically
required: the existing effect ledger can identify an attempt if the backend
can discover the runtime deterministically from that exact effect identity and
recover its result. Add the next monotonic migration only if qualification
shows runtime IDs, status transitions, or recovery cursors cannot be safely
derived from the existing tool/effect authorities. Never add a parallel effect
ledger merely to repeat operation/effect state. Raw prompts, credentials,
arbitrary output, and host environment values do not belong in such a journal.

## Implemented capability slice (Loop 107)

The code now defines a closed backend vocabulary, typed independently
enforceable capabilities, explicit CPU/memory/PID/scratch budget fields, and
digest-pinned image identity for non-local profiles. The process-local
`SandboxRegistry` performs exact backend lookup and checks required
capabilities. It never substitutes a different provider. The default registry
contains only `local-process`; OCI, gVisor, and microVM report unavailable.
Local execution advertises its argument, environment, time, output, and path
preflight bounds while explicitly reporting that it does not isolate the host
filesystem or network and does not enforce CPU, memory, PID, or scratch caps.
New tool-run evidence records the normalized tool-definition hash and the
effective sandbox-profile hash. They are excluded from the legacy logical
request hash, then compared on duplicate delivery so a changed executable,
image, or effective profile cannot reuse a new-format idempotency key. Old
terminal history remains replayable; an old nonterminal record without these
identities cannot resume into a new process execution.

Loop 108 adds a secret-free `SandboxExecutionIdentity`, passes the exact
dispatcher authority through the tool effect callback, and requires non-local
providers to implement attempt-aware execution and reconciliation. That
identity separates the tool request hash from the parent operation request
hash and binds both alongside the reserved effect, operation/task fences,
definition, and effective profile. A bounded opaque runtime name is derived
from the identity hash. No migration is added because this slice creates no
external runtime object and the dispatcher already persists reservation,
attempt, effect, and recovery-required state. This does not by itself prove
runtime discoverability or persist an opaque runtime ID; a future adapter must
demonstrate deterministic lookup and result recovery, and add durable fields
only if those facts cannot safely be derived. Non-local providers remain
unavailable in the default registry. Before selection, they must implement
attempt execution/reconciliation and advertise process-tree termination,
filesystem/network isolation, read-only root/workspace mounts, durable attempt
identity, crash reconciliation, and every requested resource bound.

## Crash and cleanup protocol

For a runtime-backed process, reserve its deterministic sandbox identity before
creating the runtime object. Persist the runtime's opaque ID and backend
identity before start when the runtime API allows a create/start split. On
cancellation or timeout, terminate the complete runtime/process tree, wait only
within a bounded cleanup deadline, then commit a terminal or recovery-required
record. On startup/takeover, reconcile only objects bearing the exact Fornix
workspace/run labels and current expected fence. Never remove unlabelled or
foreign runtime objects. If cleanup cannot be proven, preserve the tool run as
recovery-required and surface an operator action; do not create a replacement
container under the same logical run.

The sandbox journal and final tool/artifact links must commit in the same local
transaction where possible. External runtime creation cannot participate in a
Postgres transaction, so failures between reserve/create/attach/execute/finalize
must be explicit, retry-safe states with an idempotent compensating cleanup.

## Schema, cost, and operations

The current contracts live in `internal/contracts/sandbox.go` and
`internal/contracts/sandbox_execution.go`; provider selection and the
attempt-aware extension point live in `internal/tool/sandbox.go`, with the
effect-authority handoff integrated into `internal/tool/executor.go` and
`internal/server/effect_adapters.go`. No sandbox lifecycle migration exists.
If a runtime adapter requires additional persisted lifecycle facts, the next
monotonic migration must be justified by that adapter's crash protocol.
Existing `SandboxProfile` fields in `contracts/tool.go` must remain compatible
with old request hashes and persisted rows; retain compatibility defaults and
version canonicalization when new fields would change identity.

Storage is bounded to one lifecycle row and a small number of append-only
transitions per tool run; large outputs continue through `ArtifactStore`. OCI
and gVisor add image storage and runtime startup overhead. Images are operator
provisioned and digest-pinned; Fornix does not auto-pull them. Benchmark cold
and warm startup, execution latency, CPU/memory ceilings, output capture,
scratch bytes, deduplication/cache behavior, cleanup time, and recovery after
worker/runtime interruption. No performance or isolation guarantee may be
inferred from a fake runtime test.

## Acceptance tests

- Backend capability matrix reports guarantees and non-guarantees; every
  unsupported or missing backend fails closed without invoking another backend.
- Request, definition, policy, and provider budgets compose by minimum for all
  enforced fields; environment byte limits are applied before process start.
- Image references require immutable digests; changes in executable/image,
  profile, workspace input manifest, or argv change the stable execution hash.
- Unit tests verify deterministic OCI argument construction, no shell use,
  no inherited environment, fixed/no-network settings, explicit mounts only,
  resource limits, bounded output, and redaction.
- Runtime integration tests read an explicitly mounted file, reject a host-only
  marker, reject writes to a read-only workspace, fail network access in the
  offline profile, enforce CPU/memory/PID/time/output/scratch limits, and prove
  cancellation leaves no live descendant/container.
- gVisor selection runs only when `runsc` is configured and verified; missing
  or broken gVisor never downgrades to OCI/host. MicroVM is reported unavailable
  until a qualified adapter exists.
- Concurrency, duplicate delivery, stale task/agent fences, process/runtime
  crash between each lifecycle boundary, takeover, output recovery, cleanup,
  workspace isolation, and artifact-link atomicity tests pass.
- Default `make smoke` and offline reference workflow continue to run without
  Docker, gVisor, Firecracker, or network access.
- Default development and packaged Compose manifests do not mount the Docker
  daemon socket into the control server; stronger runtime access is unavailable
  until an explicit, separately authorized runner boundary is implemented.
- CI records which runtime integration matrix executed and which deployment
  gates remain environment-specific; it does not mark skipped runtime tests as
  passed.

## Loop 113 protocol boundary

[`250-sandbox-runner-protocol-foundation.md`](250-sandbox-runner-protocol-foundation.md)
adds the first typed boundary for a future separately trusted runner. Its
request has an opaque workspace mount reference, a fenced execution identity,
a tool ID/definition hash, structured arguments, and a path-free runner
profile. It does not carry an executable path, `AllowedWorkdirRoot`, Docker
options, or a host mount path. The runner must resolve the tool and environment
allowlist from its own trusted catalog and resolve the workspace reference
with symlink-safe operations. The response is bound to both the execution
identity and the exact canonical request hash.

This is protocol groundwork only: there is no IPC transport, host runner,
Docker Engine client, OCI provider, or migration, and the default registry
still exposes only `local-process`. The contract tests validate shape,
identity, byte/time bounds, and fail-closed response mapping; they do not test
runtime isolation or process cleanup.
