# Loop 86 completion — deployment-owned boundary and provider evidence

Status: implemented as a repository-owned evidence-consumption slice.

## Outcome

Fornix can now accept a signed, bounded, hash-only deployment observation for
the external facts that the repository cannot prove locally. Those facts are
bound to the exact external-boundary policy and release-admission decision
before a strict external effect is reserved.

This does not make Fornix a secret manager, proxy, DNS resolver, certificate
authority, provider reconciler, or deployment executor. A deployment-owned
publisher must still collect the observations and sign the redacted bundle.

## Delivered

- Added `BoundaryQualificationEvidence` with a closed vocabulary for:
  credential resolution, workload identity, mTLS, DNS/rebinding,
  proxy/firewall enforcement, provider idempotency, and external recovery.
- Required passed observations to be measured, hash-only, target-scoped, and
  time-bounded with a future expiry.
- Included boundary observations in report, observation, evidence-set, and
  signed-subject hashes.
- Added migration `071` for boundary hashes and expiry on deployment evidence
  links and release verifications, while retaining historical compatibility.
- Derived boundary facts from the signed import inside the existing Postgres
  transaction; callers cannot inject a different boundary hash or evidence
  set.
- Propagated the exact boundary hash, evidence-set hash, and expiry through
  release admission, operation references, and durable lifecycle metadata.
- Rejected missing, partial, mismatched, expired, ambiguous, cross-target, and
  structurally invalid external-boundary proof before effect reservation.
- Extended the offline qualification CLI with the supported observation
  vocabulary and explicit signed-observation requirements.
- Kept raw credentials, URLs, certificates, prompts, headers, provider
  payloads, and deployment output out of contracts, database rows, events,
  hashes, and tests.

## Verification

The following checks are required for this loop and were run locally after the
implementation was formatted:

```text
go test ./...
go test -race ./...
go vet ./...
make fmt-check
make package-check
make docs-check
make check
make qualification-effect-conformance
make qualification-external-boundary
go build ./...
git diff --check
```

The PostgreSQL-backed migration, RLS, concurrency, and crash tests run when
`FORNIX_TEST_PG_DSN` points to a disposable PostgreSQL instance. No local DSN
was configured during this loop, so those integration tests remain explicitly
skipped rather than being represented as verified.

## Cost and storage impact

The new durable fields are two SHA-256 identities and one timestamp on each
deployment evidence/release-verification row. Evidence linking performs one
bounded signed-bundle validation inside the existing transaction. No raw
deployment material is duplicated and no new service, broker, cache, or
container is introduced. Local unit/CLI verification is offline; production
latency, WAL growth, index size, and hosted boundary-check cost still require
deployment-specific measurement.

## Remaining limitations

- Fornix does not collect or independently attest secret-manager/KMS,
  workload-identity/mTLS, DNS, proxy/firewall, provider-idempotency, or
  recovery observations.
- External evidence freshness is bounded by the signed expiry, but revocation
  and incident response remain deployment-owned operations.
- At-least-once remote execution remains the honest boundary; provider
  idempotency and reconciliation require deployment/provider evidence.
- HA, PITR, failover, partition-maintenance, topology load/soak, and live
  connector/provider qualification remain open production gates.
- PostgreSQL migration and concurrency qualification still require a
  disposable configured database in CI or a deployment environment.

## Next handoff

Task 87 should build the deployment-owned observation/publishing adapter and
runbook that produces these signed, expiring bundles. It must remain outside
Fornix's authority path and preserve the same offline, redacted, replayable
contract.
