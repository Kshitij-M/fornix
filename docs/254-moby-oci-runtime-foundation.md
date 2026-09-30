# Moby-backed OCI execution runtime foundation

Status: implementation plan and acceptance contract. The runner transport,
workspace/tool catalogs, sealed OCI plan, and durable cleanup intents exist;
there is not yet a Moby Engine adapter, executable OCI provider, or cleanup
consumer. This note is not runtime qualification.

## Problem and intended outcome

Fornix can currently describe a bounded OCI execution plan and persist the
authority needed to identify and later clean up a tool attempt, but it cannot
execute that plan. The missing production slice is the boundary that turns a
trusted request into one exact Engine object, captures its result without
exceeding budgets, inspects the same object after a crash, and removes it only
after durable result verification.

The target flow is:

```text
Postgres reserves/fences exact attempt
  → authenticated host-runner request
  → trusted catalog resolves command and local workspace mount
  → sealed plan maps to one Moby container configuration
  → inspect policy before start; execute once; capture bounded result
  → Postgres atomically finalizes result/effect/link and enqueues cleanup
  → fenced cleanup worker asks runner to remove the exact verified object
```

Postgres remains the authority for admission, identity, effect state,
finalization, and cleanup leases. Moby is an execution mechanism, not a second
work ledger. OCI remains opt-in and unavailable in the default registry until
the Engine integration and target-runtime qualification pass.

## Invariants

1. The runtime uses the official `github.com/moby/moby/client` module, pinned
   to a reviewed release, not Docker CLI subprocesses or hand-written Engine
   HTTP calls. The current research target is `client/v0.6.0`, whose module
   declares Go 1.24 and depends on the separate `github.com/moby/moby/api`
   module. Recheck API compatibility and advisories when implementation lands.
2. The runner connects only to an explicitly resolved **local** Engine. A
   remote TCP, SSH, or unrecognized Docker context is rejected because bind
   sources must identify paths on the daemon host. The endpoint used by Moby
   must be the exact endpoint selected by Fornix's local runtime manager; it
   must not silently substitute SDK environment defaults for a user's Docker
   context.
3. Callers never provide Engine options, image names, host paths, namespace
   settings, devices, port mappings, or mounts. `BuildOCIContainerPlan` and
   operator-controlled catalogs remain the only source of those values.
   The current `SandboxProfile.ImageDigest` field is defined for this Docker
   backend as the local image ID/config digest returned by `ImageInspect.ID`,
   not a registry manifest or multi-platform index digest. Docker's API models
   those separately (`ID` versus `RepoDigests`/manifest descriptors). The
   adapter must resolve by the exact local ID, inspect and compare that ID,
   validate the image OS/architecture/variant against trusted runtime qualification,
   and fail if it is absent. It must not call an image-pull API. This local
   identity pins the configuration and its referenced layer digests; it does
   not claim registry provenance or manifest-signature verification. If
   manifest-level supply-chain verification becomes a requirement, add a
   separately named, versioned repository+manifest+platform contract instead
   of changing the meaning of persisted `ImageDigest` values.
4. Every container name and immutable label set is bound to the complete
   normalized execution identity and canonical request hash. Existing objects
   with any identity, plan, or security mismatch fail closed and are never
   started, adopted, or removed as though they belonged to this attempt.
5. The offline profile is non-root, network-disabled, capability-dropped,
   no-new-privileges, read-only-root, read-only-workspace, non-recursive bind,
   bounded CPU/memory/process/tmpfs/time/output, and has no inherited or
   request-supplied environment. Unsupported or uninspectable controls make
   the backend unavailable; there is no local-process fallback.
6. Policy is inspected from the Engine's created object before start and
   verified again during reconciliation and cleanup. A mismatch never permits
   execution. Engine-reported configuration is evidence of what the daemon
   reports, not proof against a malicious daemon or host administrator.
7. A transport error after create/start is an uncertain external outcome.
   Retrying `RunAttempt` or reconciliation must never create a second
   container or start an already-running/completed attempt. Recovery inspects
   the exact object and returns an identity-bound observation; it does not
   claim exactly-once execution.
8. Cleanup is authorized only by a durable cleanup intent created in the same
   transaction as verified result/effect/link finalization. Cleanup checks the
   immutable identity and labels, is idempotent, never removes volumes, and
   rejects unknown or mismatched objects. A stale Postgres fence cannot mark
   cleanup complete or schedule a retry. Removing the exact already-authorized
   object after lease expiry is safe/idempotent; the stale worker still cannot
   commit the outcome.
9. All captured output is bounded before it enters Postgres or artifacts.
   The implementation must not rely on an unbounded post-exit log read or
   unbounded daemon-side log retention. It must define the over-budget result
   and cancellation semantics and prove them against the supported Engine.
10. Host paths, argv, raw output, credentials, and Docker endpoint details do
    not enter logs, labels, events, or public API responses. Only bounded IDs,
    stable hashes, safe error codes, and redacted evidence cross those
    boundaries.

## Lifecycle and crash semantics

The Moby adapter must expose a small injectable Engine interface for unit
tests; the production implementation wraps the official SDK. The runtime
state machine must distinguish at least: absent, created-but-not-started,
running, exited-with-result-available, stopped/failed, mismatched, and
unknown. Existing protocol states may be extended only with versioned
contracts and explicit compatibility behavior.

The principal crash boundaries are:

- **Before create:** no Engine effect; retry is allowed only when durable
  authority still admits the same request.
- **Create acknowledged, start not acknowledged:** inspect the deterministic
  object and policy. Never create a second object. Do not claim that the tool
  ran. Resuming a created-but-not-started object requires a durable, fenced
  continuation transition; absent that transition, leave the attempt in
  recovery-required for explicit resolution.
- **Start acknowledged, result not committed:** inspect the same object; do
  not start a replacement. If it is still running, keep the effect uncertain.
  If exited, recover bounded logs only when the configured logging/capture
  mechanism proves they belong to this exact object and stay within budget.
- **Result/effect/link commit succeeded, cleanup not committed:** the
  transactionally enqueued cleanup intent is authoritative. The cleanup worker
  claims with a durable workspace fence and the runner verifies/removes only
  that exact object. A crash after removal but before queue completion is
  retried as `already_absent` for the same identity.
- **Cleanup identity mismatch or unknown Engine state:** retain the object,
  record a stable failure, retry only within the bounded policy, then
  dead-letter for operator review. Never delete by name alone.

The first implementation must not paper over the created-but-not-started
boundary. If durable continuation is not included, document that boundary as
manual recovery rather than claiming automatic recovery at every crash point.

## Cleanup authority

The worker reads a claimed cleanup job from `SandboxCleanupStore`; it does not
accept cleanup requests from public API callers. The runner must receive the
complete immutable cleanup identity and current lease facts through the
authenticated private transport. The runtime then inspects labels/config and
removes only the matching container with `RemoveVolumes=false` and
`Force=false`. It returns an observation bound to the job/intent/result hashes
and cleanup fence. `SandboxCleanupStore.Complete` or `Retry` is the final
authority and rechecks workspace, owner, fence, lease expiry, identity, and
observation in Postgres.

The observation is not self-authenticating proof: the orchestrator trusts the
runner only across the private authenticated channel, and the runner's Moby
inspection/removal result is the execution evidence. The implementation must
keep the private channel inaccessible to untrusted API clients and must not
add a public endpoint that lets a caller assert that the result committed.

## Reuse and licensing

- Reuse the official Moby client and API type modules; do not copy SDK code.
- Reuse Fornix's runner protocol, trusted catalogs, path-confined mount
  resolution, sealed container plan, attempt identity, effect recovery, and
  migration 081 cleanup queue.
- No source from Kronaxis is used. No new broker, database, object store,
  orchestration framework, or privileged helper is introduced.
- The Moby client module is Apache-2.0. Preserve its upstream license through
  normal dependency/SBOM reporting; do not vend or alter its source. The
  Fornix project license remains unchanged.

## Cost and operational budget

There is no additional always-on service: the host runner uses the selected
local Docker Engine. Per attempt, cost is one exact create, pre-start inspect,
start, wait/result capture, and final inspect/reconcile as needed; cleanup is
one durable claim plus one exact inspect/remove and one Postgres completion.
SDK connection pools are bounded and closed on shutdown. Engine logs, image
pulls, scratch, container count, and cleanup backlog need explicit limits.
Containers are never auto-removed before result commit. No reliable latency,
disk, or throughput claim exists until measurements are collected on Linux
Engine and supported Docker Desktop. Qualification must report image pull
bytes separately from steady-state execution and log/scratch storage.

## Acceptance and qualification

### Deterministic/unit and race tests

- Exact SDK mapping of sealed plan to image, argv, UID:GID, no-network,
  read-only mounts/root, capabilities, security options, CPU/memory/PID/tmpfs,
  and immutable labels; prove no caller field can loosen them.
- Local endpoint/context resolution; reject remote, malformed, ambiguous, and
  unsupported Engine endpoints before connecting.
- Exact local image-ID/config-digest availability, OS/architecture mismatch,
  and trust failure fail closed; no mutable tag or implicit image pull is
  accepted. Tests must distinguish the local image ID from a registry
  manifest/index digest.
- Duplicate same identity/request inspects/reuses the exact attempt without
  creating or starting a duplicate; conflicting request hash is rejected.
- Pre-start inspection mismatch prevents start; running/exited reconciliation
  never restarts; wrong labels, configuration, or runtime ID are rejected.
- Strict stdout/stderr byte limits, timeout, cancellation, process stop, and
  Engine error redaction; no unbounded response/log buffers.
- Cleanup queue claim/renew/takeover, stale-fence rejection, exact-object
  inspection, no-volume removal, duplicate cleanup, crash after remove/before
  commit, retry/dead-letter bounds, and workspace isolation.
- Shutdown rejects new work, cancels active requests, waits boundedly, and
  reports unavailable if runtime calls do not drain.

### Disposable Postgres and live Engine gates

- Fresh and existing database migration, RLS/runtime-role isolation,
  concurrent cleanup claims, verified-result eligibility, atomic result plus
  cleanup intent, and crash recovery.
- Linux Engine and macOS Docker Desktop are qualified separately: record exact
  Engine/Desktop version, API version, host OS, image digest, profile hash,
  file-sharing backend, and signed test evidence.
- Verify actual network isolation, mount read-only/non-recursive behavior,
  user mapping, cgroup/resource limits, process termination, output limits,
  and cleanup after injected crashes. Fake-engine tests are not kernel or
  Engine isolation evidence.
- Keep OCI excluded from the default registry until every required target
  qualification passes and the signed evidence is accepted by the existing
  sandbox trust gate.

## References

- [Moby client module at `client/v0.6.0`](https://github.com/moby/moby/tree/client/v0.6.0/client)
- [Moby API module at `api/v1.56.0`](https://github.com/moby/moby/tree/api/v1.56.0/api)
- [Moby client module requirements](https://github.com/moby/moby/blob/client/v0.6.0/client/go.mod)
- [Moby LICENSE](https://github.com/moby/moby/blob/client/v0.6.0/LICENSE)
- [Docker Engine API](https://docs.docker.com/reference/api/engine/)
- [Docker bind-mount behavior](https://docs.docker.com/engine/storage/bind-mounts/)
- [Docker tmpfs and memory accounting](https://docs.docker.com/engine/storage/tmpfs/)
- [`docs/252-host-sandbox-runner-foundation.md`](252-host-sandbox-runner-foundation.md)
- [`docs/253-sandbox-cleanup-intent-foundation.md`](253-sandbox-cleanup-intent-foundation.md)
