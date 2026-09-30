# Loop 121 completion: OCI attempt lifecycle coordinator

Status: coordinator slice implemented and locally qualified with fake-Engine
unit, race, and vet checks. This is not a shipped OCI runtime or host-isolation
qualification.

## Outcome

The sandbox runner now has an engine-independent lifecycle coordinator that
derives an exact container plan from the trusted runner request, inspects the
local image and runtime object, and creates, starts, observes, recovers, or
cleans up only that attempt. The injected `OCIEngine` has no image pull, build,
tag-resolution, or caller-defined daemon-policy operation.

The coordinator preserves uncertainty at ambiguous create/start/wait
boundaries. It never blindly repeats a start, adopts a mismatched object, or
reports an incomplete output capture as success. A verified never-started
object can be removed without force or volume deletion before a later retry.
Cancellation and deadlines stop only an inspected matching object. A natural
exit observed after the deadline is recorded as timeout, not completion.

Output crosses the Engine boundary as ordered chunks rather than an already
buffered pair of byte slices. The coordinator retains no more than the
configured per-stream result budget, returns a bounded failure prefix on
overflow, and rejects incomplete stream recovery. A future Engine adapter
must honor this streaming contract and qualify its transport and
demultiplexing buffers; the fake Engine does not establish that property.

Fixed-size in-process lock stripes serialize duplicate lifecycle operations
inside one runtime instance. They do not replace durable Postgres fencing and
do not coordinate separate runner processes.

## Delivered and reviewed

- Added a narrow typed Engine interface and lifecycle coordinator for
  `RunAttempt`, inspect-only reconciliation, and cleanup.
- Compare inspected identity and effective policy against the sealed plan
  before start, stop, output disclosure, or removal.
- Reject absent local images; the interface cannot pull or build images.
- Preserve ambiguous outcomes and safely remove only verified `created`
  objects or `exited` objects authorized by cleanup intent.
- Serialize duplicate operations in one runtime process with bounded lock
  stripes; honor cancellation while waiting for a stripe.
- Replace whole-output return values with a serial chunk sink; bound retained
  bytes per stream and stop the Engine at the first over-budget chunk.
- Add fake-Engine tests for plan mismatch, create/start acknowledgement loss,
  late deadline exits, output truncation, safe cleanup, recovery, duplicate
  requests, concurrency, and redaction.
- Independent review findings were addressed for late completion after a
  deadline, unstartable `created` objects after ambiguous create, duplicate
  in-process deliveries, and cleanup identity compatibility.

No migration, new dependency, model call, image pull, persistent record, or
new infrastructure is introduced.

## Verification

Fresh targeted checks passed:

```text
GOPROXY=off go test ./internal/sandboxrunner ./internal/adapters/sqlreadonly -count=1
GOPROXY=off go test -race ./internal/sandboxrunner ./internal/adapters/sqlreadonly -count=1
GOPROXY=off go vet ./internal/sandboxrunner ./internal/adapters/sqlreadonly
GOPROXY=off go test ./... -count=1
GOPROXY=off go test -race ./... -count=1
GOPROXY=off go vet ./...
make fmt-check
make docs-check                 # passed for 274 Markdown files, including this note
make smoke-reference-connectors PROJECTION_PG_DSN=
git diff --check
```

The reference-connector smoke passed, but its HTTP listener cases were skipped
because this sandbox prohibits loopback binding. PostgreSQL integration cases
were skipped because `FORNIX_TEST_PG_DSN` was unset. A temporary local
PostgreSQL initialization attempt also failed because the sandbox denies the
shared-memory operation required by the server; no host configuration was
changed. The full offline repository test, race, and vet suites passed. The
repository's `make check` also passed with an isolated temporary Go build
cache. Live PostgreSQL and container-runtime qualification remain unverified.

## Remaining qualification gaps

- No official Moby SDK implementation is included or registered in the
  default runner or server. There is no real Docker Engine lifecycle test.
- The test Engine is not evidence that a daemon or kernel enforces seccomp,
  namespaces, mount behavior, capabilities, network isolation, cgroups, or
  resource limits on Linux or macOS.
- The coordinator bounds retained output before it enters the returned result,
  but this slice has no real adapter proving transport and demultiplexing
  allocations stay bounded while reading output from the daemon.
- Mount verification and the later host bind operation still need an
  adapter-level race-resistant design and qualification.
- Lock stripes only serialize within one process. Durable admission, fencing,
  effect finalization, and cleanup ownership remain external Postgres
  responsibilities; a one-shot external start retains its documented
  at-least-once uncertainty window.
- Live PostgreSQL fencing, container-runtime cleanup, crash-at-every-boundary,
  and supported-host qualification remain open.

See [the lifecycle foundation note](269-oci-attempt-lifecycle-foundation.md)
and the [universal qualification roadmap](111-universal-production-roadmap-status.md).
