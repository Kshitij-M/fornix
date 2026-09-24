# Loop 31 completion: shared destination and egress policy

Status: implemented on the Issue #40 production-qualification branch.

This loop adds the first shared outbound boundary for the universal connector
surface. It prevents adapters from silently inventing different scheme, host,
path, and redirect semantics while preserving the HTTP adapter's stronger
transport-specific protections.

## Delivered

- Added `connector.DestinationPolicy` with bounded scheme, host, path-prefix,
  private-network, redirect, and redirect-budget fields.
- Added normalization, fail-closed validation, deterministic allowlist
  ordering, URL authorization, and a secret-free stable policy hash.
- Integrated the policy into HTTP request URL construction and redirect
  handling.
- Kept the existing HTTP exact-host/port, relative-path, private-network DNS
  resolution, timeout, request-byte, response-byte, and page budgets active.
- Added unit tests for successful admission, scheme/host/path/credential
  rejection, malformed policy rejection, and order-independent hashing.
- Added a public feature note explaining authority, limits, cost, licensing,
  and the qualification boundary.

## Verification

- `gofmt` and `git diff --check`
- `go test ./internal/connector ./internal/adapters/httpapi`
- The HTTP connector continues to use typed relative paths and cannot accept
  an absolute URL or URL credentials from an operation payload.

The broader repository checks and universal smoke targets are the release
qualification for this slice and are recorded after they complete.

## Measured local cost

The shared policy adds bounded in-process normalization and URL checks. It
adds no SQL query, network request, migration, container, or persistent row.
The HTTP adapter's existing DNS and transport checks remain the dominant
outbound admission cost. Production deployment must measure policy admission,
DNS resolution, redirect rejection, and outbound latency separately.

## Remaining Issue #40 gates

The implementation does not yet provide a generic executor, durable policy
version records, signed connector/policy catalogs, a central egress proxy,
external secret-manager integration, tenant RLS/equivalent isolation,
quotas/backpressure, backup/restore, HA/failover, adversarial confused-deputy
qualification, or load/soak evidence. Fornix remains an alpha universal
control-plane foundation until those gates are completed and measured.
