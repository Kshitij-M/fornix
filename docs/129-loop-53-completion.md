# Task 53 completion — effectful adapter authority and exact credential-fence linkage

Status: implemented and locally qualified on the current feature branch; this
is not a production-readiness declaration.

Task 53 closes the authority gap identified by the signed schema-catalog and
managed-credential slices. An effectful adapter must not treat a process-local
admission result as permission to dispatch. It must carry the exact durable
authority facts that were admitted, reserve the external effect through
Postgres, and leave enough immutable evidence for recovery and review.

## Delivered

- Added typed `EffectAuthority` and `OperationAuthorityLink` facts for schema
  catalog hash/revision and managed credential source version/expiry. The
  authority envelope contains identifiers and fences only; it never contains a
  secret, prompt, provider payload, or raw schema document.
- Added migration `052_effect_authority_facts.sql`. Existing rows receive
  empty compatibility values, so historical authority-link hashes are not
  rewritten. New schema and credential facts have bounded checks and indexes.
- Made durable effect reservation depend on an existing operation admission.
  A reservation derives its authority facts from the admission link and
  rejects caller-supplied facts that do not match the durable record.
- Persisted the same authority facts on `operation_effects`, so effect
  recovery can inspect the exact schema and credential-source identity that was
  admitted without consulting a mutable process cache.
- Made result authority links inherit the committed admission/effect facts in
  the same transaction. Work Receipt links inherit those facts as well and use
  historical validation so an audit remains readable after a lease or signer
  expires.
- Added exact existing-lease resolution. Credential-bearing effect adapters can
  materialize the already-admitted lease and source version without acquiring
  a replacement fence. Source version, source expiry, lease fence, revocation
  epoch, reference, and credential-reference version are compared before the
  secret is returned to the adapter.
- Added an authority-aware connector execution seam. Strict effectful
  capabilities fail closed unless the adapter accepts the typed authority
  envelope. The HTTP adapter is the first implementation and binds its
  credential lookup to the exact admitted lease.
- Preserved at-least-once external semantics. A provider may receive a request
  before a local crash is recorded; provider idempotency, verification, and
  compensation remain adapter responsibilities. Fornix does not claim
  exactly-once remote execution.
- Added coverage for secret-free authority hashes, schema-catalog binding,
  managed credential source inheritance, exact-lease resolution, stale source
  rejection, durable effect admission, duplicate delivery, HTTP reconciliation,
  rollback, and workspace scoping.
- Added `make smoke-universal-effect-authority`, aggregate smoke coverage, a CI
  qualification step, and the qualification runbook entry.

## Authority and transaction model

```text
signed catalog + managed credential lease
                 │
                 ▼
        durable operation admission
                 │ exact facts
                 ▼
       fenced effect reservation
                 │ same facts
                 ▼
   adapter dispatch with exact lease
                 │ provider may be at-least-once
                 ▼
     result / receipt / recovery links
```

The control plane owns admission identity, workspace scope, schema identity,
operation and task fences, managed credential lease identity, and append-only
effect state. The adapter owns provider request construction, destination
policy, provider idempotency, response verification, compensation, and
redacted output handling. A receipt can therefore explain which authority was
used without retaining the authority's secret material.

## Qualification evidence

The following checks passed against the named disposable database
`fornix_task53_20260924`; the persistent development database was not dropped
or used as a cleanup target:

| Check | Result |
| --- | --- |
| `go test ./... -count=1` | Passed offline; database tests skip when no DSN is supplied |
| `FORNIX_TEST_PG_DSN=... go test ./... -count=1` | Passed against PostgreSQL after migrations reached `052_effect_authority_facts` |
| Focused store, server, and HTTP adapter tests | Passed, including exact lease, authority inheritance, effect reservation, duplicate delivery, and reconciliation |
| `git diff --check` | Passed |
| Fresh migration path | Passed as part of the disposable database qualification |
| Existing-schema migration path | Covered by the migration runner's numbered upgrade path and full integration suite; deployment rollback remains open |

The command added for repeatable focused qualification is:

```sh
FORNIX_TEST_PG_DSN='postgres://USER:PASSWORD@HOST:PORT/DISPOSABLE_DATABASE?sslmode=disable' \
  make smoke-universal-effect-authority
```

This smoke performs no provider call and does not dispatch an external effect.
It creates only bounded test rows in the supplied disposable database.

## Cost and storage impact

The normal effect path adds one durable admission lookup/validation and one
effect-row write containing bounded hashes, versions, timestamps, and fences.
Result and receipt linkage reuses the same transaction rather than adding a
second authority service. Exact credential resolution performs one controlled
secret-source lookup followed by a locked Postgres comparison; the secret is
never inserted into an event, effect, authority link, log, or evidence record.

Migration 052 adds nullable-in-practice compatibility columns, bounded checks,
and partial indexes to existing operation relations. It stores no raw schema,
prompt, request body, or credential bytes. Production sizing still needs
deployment-topology measurements for operation volume, effect-recovery rows,
catalog rotation, WAL, lock waits, and retention.

On the disposable PostgreSQL 17 instance after the qualification suite, the
three affected relations measured approximately 168 KiB for
`operation_authority_links`, 56 KiB for `operation_effects`, and 80 KiB for
`credential_leases`. These are small isolated-test observations, not a
production capacity estimate; append-only link/effect history will grow with
operation volume and must be included in retention and backup sizing.

## Reuse and licensing

The implementation reuses Fornix's existing operation admission, effect
reservation, credential lease, signed schema catalog, connector registry,
controlled HTTP egress, Work Receipt, and append-only event boundaries. The
design was compared with Orloj's execution fences, DeepSeek Harness's
provider/capability seams, and agentmemory's lease/checkpoint patterns. No
source was copied from Kronaxis Fabric; its BSL 1.1 license remains outside
the implementation. New Fornix code is covered by the repository's MIT
license.

## Remaining limitations

- HTTP is the first authority-aware effectful adapter. SQL, model-provider
  mutation paths, MCP, cloud, ticketing, deployment, and future adapters need
  conformance tests and explicit authority-aware implementations.
- Process startup does not yet automatically reload and verify every durable
  schema catalog into every adapter registry. Deployment-wide catalog rollout,
  lag, signer rotation, and rollback ceremonies remain open.
- The local credential resolver is a development/test authority. A production
  deployment must supply workload identity or mTLS, KMS/secret-manager
  integration, zeroization, rotation-lag monitoring, and failure drills.
- External providers remain at-least-once. A provider-specific idempotency key
  and verification protocol are required before an effect can be considered
  safe for unattended use.
- Role-separated RLS, backup/restore, HA/failover, retention, load/soak,
  adversarial security, and live connector qualification remain Issue #40
  gates.

## Next handoff

The next slice is Task 54: qualify the authority envelope across all effectful
adapters and add startup catalog/lease authority reload with deployment-wide
lag and signer-rotation evidence. It must preserve this slice's invariant:
no effectful dispatch without one exact, durable, workspace-scoped authority
envelope.
