# Loop 26 completion: bounded HTTP/API and read-only SQL connectors

Status: alpha reference-connector foundation implemented on the universal
transformation branch for [Issue #43](https://github.com/Kshitij-M/fornix/issues/43).

## Outcome

Fornix now demonstrates the universal adapter boundary against two systems
that are not repositories:

- `httpapi@1` exposes `read`, `list`, and approval-gated
  `submit_idempotent` capabilities.
- `sqlreadonly@1` exposes `describe`, `query_readonly`, and
  `explain_readonly` capabilities.

Both adapters accept only typed, hashed input payloads, enforce workspace and
target identity, return bounded hash/evidence references, and remain separate
from the Postgres control-plane authority. The repository adapter remains
unchanged and is still the first end-user workflow.

## Delivered components

### Contracts and persistence

- Added `ConnectorBinding` and `ConnectorBindingRequest` contracts with
  canonical configuration, configuration hashes, credential references,
  workspace checks, immutable status, and stable request hashes.
- Added migration `037_connector_bindings.sql` with immutable workspace-scoped
  binding rows, idempotency records, bounded JSON checks, indexes, and an
  append-only mutation trigger.
- Added a transactional `ConnectorBindingStore` with create/get, duplicate
  replay, conflict detection, event emission, and injected rollback testing.
- Kept credentials as reference identities only; binding configuration rejects
  secret-like fields and stores no DSN, password, token, or authorization
  value.

### HTTP/API adapter

- Builds requests only from a configured base URL, exact host/port allowlist,
  normalized path prefixes, and relative typed payloads.
- Rejects absolute paths, traversal, unsafe headers, unbounded query/body
  input, private or metadata destinations by default, and redirects that leave
  the configured target policy.
- Bounds request/response bytes, timeout, retries, redirect hops, and list
  page size. A continuation token is returned as a hash rather than raw
  response content.
- Resolves credentials only at the outbound boundary, clears the secret
  buffer, and never puts it in an operation result or error.
- Marks `submit_idempotent` as an external at-least-once effect with the
  provider request ID, idempotency support, and verification state. It never
  claims exactly-once remote execution.

### Read-only SQL adapter

- Requires configured schema/table allowlists and schema-qualified references.
- Accepts one `SELECT`/`WITH` statement, validates parameter placeholders,
  rejects comments, multiple statements, mutation keywords, transaction
  control, `COPY`, `ANALYZE`, and unapproved tables before database access.
- Uses prepared statements inside a PostgreSQL read-only transaction with a
  statement timeout. Result rows and bytes are bounded while being decoded.
- Enforces a deterministic result-cost estimate: one base unit plus one unit
  per returned row and per started KiB of encoded values. This is explicitly a
  safety budget, not a database planner-cost or provider billing measurement.
- Includes only column/result hashes, counts, truncation, and evidence
  references in the operation result. Raw rows never become Fornix authority.
- Includes a `FixtureDatabase` for offline deterministic replay and a live
  `PGDatabase` test that proves the read-only transaction rejects mutation.

### Shared qualification and integration

- Added a common conformance test for the HTTP read and SQL read capabilities.
- Added adapter tests for allowlists, SSRF/private-network policy, redirects,
  response limits, pagination, credential redaction, retry classification,
  SQL injection/multi-statement/write rejection, parameter binding, cost
  limits, workspace isolation, and deterministic hashes.
- Added migration/store coverage to the connector smoke target and CI.
- The application composition now owns the durable connector-binding store;
  adapter instantiation remains explicit and does not hydrate executable code
  from a database row.

## Validation performed

The following checks were run on the branch:

```text
go test ./internal/connector ./internal/adapters/httpapi \
  ./internal/adapters/sqlreadonly ./internal/contracts ./internal/server -count=1
go test ./internal/store -run 'Test(ConnectorBinding|Operation|Admission)' \
  -count=1
make fmt-check
```

All passed. A clean Postgres database was created, migrated through migration
037, and used for the binding-store and live read-only transaction tests. The
fresh database reported 37 applied migrations. With three test binding rows,
the measured relation sizes were 64 KiB for `connector_bindings` and 32 KiB
for `connector_binding_idempotency`; this is relation allocation, not a
capacity claim for production data.

The focused Go package run completed in approximately 2.3 seconds locally;
the focused store run completed in approximately 1.2 seconds. These are
developer-machine measurements, not throughput guarantees. The adapter tests
use fake/loopback systems and do not qualify public-network latency, remote
provider behavior, or database planner cost.

## Reuse and licensing

The implementation reuses Fornix's typed operation, capability, admission,
effect, evidence, credential-reference, event, and Postgres-store seams. It
independently applies bounded-adapter ideas studied in Orloj, DeepSeek
Harness, Dagger, and DBOS. No reference source was copied. Kronaxis-fabric
was not copied because its BSL 1.1 license is incompatible with Fornix's MIT
distribution.

## Remaining limitations

This loop is not a production qualification of arbitrary HTTP or database
access. The following remain intentionally open:

- no public connector-binding API or automatic runtime hydration from binding
  rows; callers must construct and register an adapter explicitly;
- no external secret-manager integration, OAuth flow, signed webhook
  verification, or host-independent egress enforcement;
- no remote exactly-once guarantee, provider-specific post-effect
  reconciliation, or generic verification worker;
- SQL cost units estimate returned result work and do not replace a database
  planner budget; large scans still require deployment-level query policy;
- HTTP pagination is one bounded page per operation and requires the caller to
  submit a continuation request; `MaxPages` is a binding ceiling for future
  workflow composition, not an automatic multi-page loop;
- raw external responses are not automatically persisted as artifacts by the
  adapter; a higher-level operation/evidence writer must opt into bounded
  artifact disclosure;
- the multi-step workflow runtime, multi-domain reference workflow, and
  security/scale/recovery qualification remain Issues #44, #42, and #40.

These limitations are deliberate. The slice proves that Fornix's universal
control-plane contracts can govern non-repository capabilities without
turning the product into an arbitrary URL fetcher or SQL console.
