# Loop 120 completion: structured read-only SQL boundary

Status: implemented and verified offline. PostgreSQL-backed cases are present
but were skipped locally because `FORNIX_TEST_PG_DSN` is unset.

## Outcome

The SQL reference connector no longer accepts caller-authored SQL. `sqlreadonly`
is version 2 and accepts a bounded table/column/filter/order/limit request. It
compiles the request to deterministic parameterized SQL; raw SQL fields from
the prior v1 contract fail strict decoding. This closes the function/expression
gap that keyword and relation-reference regular expressions could not safely
constrain.

The connector remains an alpha reference adapter. This work does not qualify a
production database, a deployment's permissions/RLS, arbitrary SQL dialects,
or database execution cost.

## Delivered

- Added typed filters with fixed comparison operators, scalar JSON values,
  bounded `IN` lists, explicit column selection, and bounded sort terms.
- Canonicalized identifiers, commutative filters, and `IN` values; generated
  SQL contains only quoted identifiers and fixed syntax. All data values are
  driver parameters. Generated reads use PostgreSQL `FROM ONLY` so an
  allowlisted inheritance parent cannot return rows from unlisted children.
- Bumped the connector and capability identity to v2. Strict JSON decoding
  rejects unknown fields, including the legacy `statement` and `parameters`
  fields. Existing v1 payloads are not silently reinterpreted; callers must
  register a v2 connector binding and submit a v2 input hash.
- Bound `PGDatabase` to the exact connector allowlist. Before query execution,
  one catalog round trip validates the named relation, ordinary persistent
  table kind, and every referenced column's built-in scalar type. The
  transaction is repeatable-read and read-only, uses a request deadline and
  server statement timeout, and pins `search_path` to `pg_catalog`.
- Applied row and serialized-result byte limits after the driver returns,
  checked result row shape and byte accounting, and recomputed the minimum
  deterministic cost proxy so a custom `Database` implementation cannot
  under-report it. This does not bound the wire size or peak allocation of a
  single oversized database cell; do not treat it as a hard memory ceiling.
- Removed a fixed adapter-owned prepared-statement name. Different generated
  SQL shapes now execute by SQL text, avoiding name collisions when one pool
  connection is reused. A DSN-gated regression test covers sequential query,
  describe, and explain calls on a single connection.
- Preserved SQL `NULL` separately from a literal `"<nil>"` in the canonical
  result digest.
- Added deterministic compiler and injection-as-data tests, legacy-field and
  expression rejection, allowlist and workspace checks, result-budget tests,
  and PostgreSQL-gated qualification for base tables, views, temporary tables,
  custom types, and read-only transactions.

## Verification

At initial Loop 120 implementation, the following checks passed:

```text
GOPROXY=off go test ./...
GOPROXY=off go test -race ./...
GOPROXY=off go vet ./...
make fmt-check docs-check
make smoke-reference-connectors PROJECTION_PG_DSN=
git diff --check
```

All Go packages passed, including the SQL adapter, connector conformance, and
qualification matrix. Loopback HTTP cases skipped because this sandbox does
not permit binding local listeners. PostgreSQL-backed cases skipped because
`FORNIX_TEST_PG_DSN` was unset; live PostgreSQL success is not claimed here.
The documentation check then passed for 272 Markdown files. After the
independent SQL review, targeted regression fixes were applied and verified
with:

```text
GOPROXY=off go test ./internal/adapters/sqlreadonly
make docs-check
git diff --check
```

The focused Go package test passed; its live PostgreSQL cases remained skipped
because `FORNIX_TEST_PG_DSN` is unset. The updated documentation check passed
for 273 Markdown files. In a later qualification pass, after the Loop 121
coordinator follow-ups, the full repository Go test, race, and vet suites,
`make check`, and documentation checks passed. PostgreSQL cases remained
skipped because `FORNIX_TEST_PG_DSN` was unset; loopback-dependent smoke cases
also skipped under the sandbox. See [Loop 121](270-loop-121-completion.md)
for the exact later verification boundary.

## Cost and storage

- No migration, persistent table, Go dependency, model call, service, or
  infrastructure was added. The request remains in the existing hash-resolved
  input path; connector operation history continues to persist bounded hashes
  and evidence references, not raw SQL or result rows.
- A query/explain adds one catalog metadata round trip before the query. A
  describe performs the same relation validation plus its bounded metadata
  read. No catalog cache is used, so there is no stale-authorization window.
- No live latency or external database planner-cost measurement was available.
  The existing cost units measure returned rows/bytes and are not a query-plan
  work limit. Statement timeout, table grants, RLS, and deployment-level
  database resource controls remain necessary.

## Remaining limitations

- The structured contract intentionally supports a narrow SQL subset. It does
  not expose joins, aggregates, expressions, functions, casts, CTEs, or
  caller-authored plans.
- Filter value compatibility depends on PostgreSQL's inferred parameter type;
  a value incompatible with its selected column returns a bounded failure.
- The response byte budget is checked after pgx materializes the row. A single
  oversized database cell can exceed that budget in wire bytes and peak
  allocation; this is not a hard memory ceiling.
- Limited queries without a stable total ordering can return different
  snapshots or row orders. Result hashes identify the returned snapshot and
  are not replay-stable in that case.
- The adapter is not registered by default server startup; SQL is not yet
  reachable through the bundled public API without explicit registry wiring.
- A read-only transaction is not a replacement for least-privilege database
  grants and RLS. Trusted server objects such as RLS policy functions remain
  part of the configured database security boundary.
- The disposable PostgreSQL integration tests must still be run in CI or a
  qualified local database before this SQL path is considered live-qualified.
- The SQL cost proxy bounds disclosed result work, not full scan/planner work.

See [the SQL query-contract feature note](267-sql-read-only-query-contract-foundation.md)
and [the reference connector architecture](72-reference-connectors-foundation.md).
