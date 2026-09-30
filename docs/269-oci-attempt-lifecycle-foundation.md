# OCI attempt lifecycle coordinator foundation

Status: implementation scope for an engine-independent lifecycle coordinator.
This is not a Moby adapter, a shipped OCI provider, or isolation qualification.

## Problem and intended outcome

Fornix already has a path-free runner request, an operator-owned tool and
workspace catalog, a sealed OCI plan, durable effect identity, and a fenced
cleanup queue. The missing boundary is the deterministic lifecycle that
decides whether an exact attempt may be created, started, inspected, recovered,
or removed. That logic must be independently testable before it is mapped to
the official Moby SDK; otherwise SDK plumbing and crash semantics become one
untestable unit.

This slice adds an engine-independent coordinator behind the existing runner
protocol. A future Moby adapter will implement its narrow Engine interface.
The fake Engine used by unit tests proves Fornix's decisions and request
mapping only; it does not prove the Docker daemon or kernel enforces the
profile.

## Invariants

1. The sealed `OCIContainerPlan` is the only source of image, argv, labels,
   mounts, namespaces, network, user, security options, and resource limits.
   The coordinator never accepts daemon options from a request.
2. Image resolution is inspection-only. A missing local image fails closed;
   the lifecycle has no pull, build, tag-resolution, or mutable-name method.
3. The canonical execution name and exact identity labels select one object.
   A name collision, mismatched image, label, or inspected policy never gets
   started, adopted as the attempt, or removed.
4. A newly created object is inspected and matched to the plan before its
   single `Start` call. An object found on entry is never started. A verified
   `created` object is removed without force or volume deletion and requires a
   later retry; a running object remains uncertain. An exited object can be
   disclosed only when bounded output capture proves a complete result for the
   exact execution identity.
5. A create/start transport error is ambiguous. The coordinator inspects the
   deterministic object and never blindly repeats a start. If the exact
   object is still `created`, the coordinator removes that never-started
   object without force or volume deletion; the current call remains unknown
   and a caller may retry. An uninspectable state remains unknown and is not
   success.
6. The Engine streams ordered stdout/stderr chunks serially to a coordinator-
   owned sink. The sink retains at most the configured bytes per stream and
   stops on the first over-budget chunk; overflow produces a bounded failed
   result, never success. Incomplete capture, daemon disconnect, unknown
   state, or failed policy inspection cannot become success. The adapter must
   not pre-buffer the complete output; its transport and demultiplexing
   allocations remain an adapter qualification requirement. Timeout/cancellation
   stops only the exact verified object using a bounded cleanup context.
7. Reconciliation is inspect-only and cannot start, stop, or remove an object.
   It reports `absent`, `running`, `stopped`, or `unknown`; it reports
   `completed` only if a complete, bounded, identity-bound result is actually
   recoverable. The current identity-only protocol may not contain enough
   metadata to reconstruct a completed `ToolResult`; in that case it must
   report `unknown` rather than invent request fields.
8. Cleanup is separately authorized by the durable cleanup intent. It inspects
   the canonical object and its execution-identity labels, removes only
   `created` or `exited` objects, never forces removal, never removes volumes,
   and treats an already-absent exact object as idempotent success. Unknown or
   mismatched objects are retained.
9. Postgres remains authoritative for execution admission, operation/task/run
   fences, effect/result finalization, and cleanup-job state. The runner does
   not infer current Postgres authority from IPC authentication.
10. No raw argv, host path, output, credential, Engine endpoint, or SDK error
    is logged or copied to public diagnostics.

## Interfaces and state transitions

The coordinator depends on a narrow local Engine interface for image
inspection, exact object lookup/create/start/wait/stop, ordered output
streaming into a bounded sink, and non-forced object removal. Inspection
returns typed configuration and state facts needed to compare the live object
with the sealed plan; an opaque
`plan_hash` returned by the same adapter is not accepted as policy proof.

```text
absent --create--> created --inspect exact policy--> --start once--> running
  |                       |                                      |
  | create uncertainty    | pre-existing: remove safely           | wait/stop
  v                       v                                      v
unknown               absent + retry-required               exited + bounded capture
```

The coordinator holds only a fixed-size set of in-process lock stripes to
serialize duplicate requests for the same execution identity. This prevents
two goroutines in one runner from racing a start against cleanup; it is not a
cross-process lease or substitute for Postgres fencing. Durable attempt
identity and container identity let a new process inspect the same object;
uncertainty is preserved, not hidden in an in-memory retry. A successful
container is not removed by the execution call; the separate cleanup worker
consumes the Postgres-authorized intent after result/effect finalization.

## Schema, API, and compatibility

- No database migration or new durable table is introduced.
- The existing private runner protocol and tool/provider contracts remain the
  authority boundary. Any future protocol change needed to reconstruct a
  completed result from identity-only reconciliation requires a separate
  explicit versioned contract; this slice must not silently change its shape.
- OCI remains absent from the default registry. An injected test Engine does
  not qualify a provider or permit production registration.

## Reuse and licensing

- Reuse Fornix's `Runtime`, `ToolCatalog`, `MountCatalog`,
  `BuildOCIContainerPlan`, local-image verifier, execution identity, response,
  cleanup command, and durable cleanup worker.
- Orloj's container tool implementation informs bounded process/output handling
  but invokes a container CLI and has no equivalent durable effect identity;
  no source is copied.
- Dagger's container backend seam informs the narrow injected Engine interface;
  no source is copied.
- No Kronaxis Fabric source is used. This slice adds no dependency and creates
  no new licensing obligation. A later official Moby SDK integration must
  preserve its Apache-2.0 notices and dependency/SBOM metadata.
- The upstream Moby project currently identifies `github.com/moby/moby/client`
  as its supported Docker Engine Go client and `github.com/moby/moby/api` as
  the separately versioned shared API module; it marks the old
  `github.com/docker/docker` module deprecated. See the
  [Moby module guidance](https://github.com/moby/moby#go-modules) and the
  [official client package documentation](https://pkg.go.dev/github.com/moby/moby/client).
  The modules are not present in this branch's dependency graph or local
  module cache, so no client adapter is claimed here.

## Cost and operational budget

There is no new database work, persistent storage, background service, image
pull, model call, or dependency. A first attempt is bounded to image inspect,
exact name lookup, create, pre-start inspect, one start, one wait, bounded
output capture, and final inspect. Recovery uses exact-object inspection only;
cleanup uses exact-object inspection and one non-forced removal. Engine calls
inherit caller deadlines, and the runtime caps concurrent requests through
the existing runner handler. No latency or disk claim is made without a real
Engine measurement.

## Acceptance tests

- Exact-plan construction and live-inspection comparison reject every policy
  relaxation, extra label, wrong image, wrong identity, mount drift, or
  unsupported control before start.
- No pull method exists; a missing local image never creates a container.
- Identical concurrent delivery within one runner instance creates one
  canonical object and starts it at most once; changed execution identities do
  not adopt another attempt's object.
- A transport-only request ID change with the same execution identity reuses
  the same recovered effect; the identity label set does not require a
  redundant full-request hash unavailable to the durable cleanup command.
- Create acknowledgement loss, start acknowledgement loss, daemon errors,
  crashes in created/running/exited states, and repeated reconciliation never
  repeat an ambiguous start. A verified never-started object is safely removed
  before a subsequent retry.
- Existing `created` and `running` objects are never started by retries. A
  verified `created` object can be removed safely and retried later; running
  objects remain unknown.
- Exited attempts recover only with a complete bounded output capture; output
  overflow, truncation, disconnect, malformed timestamps, and incomplete logs
  fail closed.
- Timeout and cancellation stop only the exact verified container; unknown or
  mismatched objects are never stopped or removed. A natural exit after the
  deadline is classified as timed out, not completed.
- Cleanup is idempotent, non-forced, volume-preserving, execution-identity
  bound, and rejects stale/mismatched identities.
- Error values, logs, and responses do not disclose host paths, argv,
  credentials, raw Engine errors, or raw output beyond the bounded result.
- Existing unit, race, connector, runner, and smoke checks remain green.
- Live Docker Engine, Postgres fencing, and Linux/macOS isolation checks are
  recorded as unverified until the official SDK adapter and supported-host
  qualification run.
