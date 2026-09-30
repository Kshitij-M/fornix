# Loop 90 completion — readiness freshness policy and incident review

Status: implemented as a repository-owned advisory qualification slice.

## Delivered

- Added immutable, versioned `ReadinessFreshnessPolicy` contracts with bounded
  maximum age, explicit review-only `require_ready` behavior, stable hashes,
  actor provenance, and idempotency.
- Added migration `074` for policy history, append-only publication events,
  bounded indexes, and workspace RLS.
- Added transactional policy publication with monotonic revisions, duplicate
  policy deduplication, conflicting idempotency rejection, dry-run behavior,
  and serialized workspace/deployment scope locking.
- Added read-only snapshot comparison against the current policy. Reviews use
  explicit `as_of` time, detect future/stale observations, compare readiness
  and gate hashes, report evidence and blocked-reason drift, and return a
  deterministic review hash without writing a review row.
- Added authenticated HTTP and CLI surfaces for policy publication,
  inspection, pagination, and readiness review.
- Added RBAC tests: policy mutation requires qualification-admin; policy reads
  and reviews require qualification-read.
- Added contract and PostgreSQL-backed tests for stable hashes, idempotency,
  monotonic concurrent publication, stale/future behavior, deterministic
  comparisons, workspace isolation, and read-only review semantics.

## Verification

The complete repository gates include:

```text
make check
go test -race ./...
make qualification-effect-conformance qualification-external-boundary
go build ./...
git diff --check
```

PostgreSQL migration, RLS, concurrency, and recovery fixtures execute when
`FORNIX_TEST_PG_DSN` is configured. No PostgreSQL DSN is configured in this
environment, so those integration tests skip explicitly.

## Cost and remaining limitations

Policy publication adds one bounded policy row and event. Review performs two
indexed snapshot reads and one current-policy read and creates no durable
review artifact or event. Policies do not attest deployment truth, change
release admission, extend evidence expiry, or qualify provider behavior.
Deployment-owned external-boundary, HA/PITR/failover, and
load/soak evidence remain open production gates.

## Follow-on status

Task 91 is implemented in
[`208-loop-91-completion.md`](208-loop-91-completion.md). The next repository
handoff is Task 92: deployment-backed qualification execution and evidence
refresh. The repository still cannot prove deployment-owned HA, PITR, failover,
secret-manager, mTLS, DNS, workload-identity, or remote exactly-once behavior
without signed observations from those authorities.
