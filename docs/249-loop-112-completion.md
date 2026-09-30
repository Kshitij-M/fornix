# Loop 112 — Signed qualification for non-local sandbox backends

Status: implemented and locally unit-tested; no non-local runtime is shipped.

## Outcome

Non-local sandbox providers can no longer become selectable merely by
declaring that they are available and listing controls. Registration and
resolution now require a signed, current qualification proof tied to a
deployment-selected Ed25519 key and target. The proof binds provider/runtime/
configuration hashes, immutable image digest, normalized controls, expiry,
and the conformance-suite identity. It also carries a tested numeric budget
envelope; a requested profile cannot exceed its signed limits.

The accepted qualification hash is included in durable tool request evidence
and the fenced sandbox attempt identity. Before effect dispatch, the executor
rechecks that the exact qualification remains current. Recovery requires the
original proof hash and asks only the recorded backend to reconcile the exact
attempt. No invalid, expired, stale, or unavailable non-local backend falls
back to local process execution.

## Decisions and boundaries

- Reused Fornix's existing signed qualification bundle and trusted-key
  verification. No new signature format, migration, dependency, or service
  was added.
- Qualification trust is deployment-wide unless the operator pins a workspace
  in the trust policy. A pinned policy is enforced at provider resolution;
  qualification is not an authorization grant and does not replace RBAC.
- Numeric envelope values cover wall time, output bytes, argv count/bytes,
  environment entries/bytes, CPU, memory, process count, and scratch storage.
  Profiles must fit within the limits represented by the proof.
- Runtime providers must revalidate their active identity atomically
  immediately before runtime creation. Registry preflight cannot prove
  provider-internal behavior; each real provider still requires code review
  and deployment-owned conformance testing.
- A legacy uncertain non-local attempt without a qualification hash remains
  recovery-required. It is not upgraded, relaunched, or assigned a guessed
  identity; operators must resolve it through deployment-owned inspection.
- Only the limited `local-process` implementation is available by default.
  OCI, gVisor, and microVM remain unavailable until an actual provider is
  implemented and qualified against its supported host/runtime.

## Verification

Executed on this branch:

```text
go test -p 2 ./internal/contracts ./internal/qualification ./internal/tool -count=1
PASS
go test -race -p 2 ./internal/contracts ./internal/qualification ./internal/tool -count=1
PASS
go test -p 2 ./... -count=1
PASS (environment-gated cases remain skipped without their services)
go vet ./...
PASS
make fmt-check docs-check
PASS (254 Markdown files)
```

Tests cover evidence validity and expiry, signer/target/workspace selection,
signed case and manifest binding, tampering, provider/runtime/capability drift,
measured numeric envelope enforcement, durable request identity, exact
recovery identity, and rejection of pre-qualification uncertain attempts.
Existing package tests also cover the local-process default and no-fallback
behavior.

The full suite did not exercise PostgreSQL because `FORNIX_TEST_PG_DSN` is not
configured, and environment-gated loopback/service tests may skip when their
prerequisites are unavailable. It did not execute a real OCI, gVisor, or
microVM runtime. A separate `go build -o /dev/null` check could not finish
because the sandbox denied a write to the Go module download cache; the full
`go test ./...` command nevertheless compiled and tested all Go packages.

## Cost and remaining work

There is no additional database query or table. Existing tool-run request JSON
gains one optional 64-character qualification hash; the sandbox attempt
identity gains the same-sized hash. The signed proof remains bounded and
process-local. Registration and selection add bounded hashing, normalization,
and Ed25519 verification; runtime latency cannot be measured until a real
provider exists. No storage-growth or throughput claim is made.

Production use still requires a real provider, independently operated
qualification signing/custody, provider-internal identity pinning at launch,
runtime crash-reconciliation evidence, host/kernel isolation tests, adversarial
network/filesystem tests, and fresh database/upgrade testing for the broader
repository. The proof is scoped to the named target and does not certify a
host or future configuration.
