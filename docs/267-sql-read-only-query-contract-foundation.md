# Safe structured reads for the SQL connector

Status: implemented as SQL connector v2. Offline verification passed; the
PostgreSQL-gated cases are documented in [Loop 120](268-loop-120-completion.md)
and still require a disposable PostgreSQL qualification run.

## Problem and threat model

The current `sqlreadonly` payload accepts caller-supplied SQL and tries to
recognize a safe subset with keyword and table-reference regular expressions.
That is not a SQL parser. In particular, a statement such as
`SELECT pg_read_file(...)` contains no table reference or mutation keyword,
yet invokes a database function. PostgreSQL's read-only transaction mode is
not a function allowlist: SQL functions can perform side effects, and
privileged file functions are available to roles with the corresponding
server-file capability. PostgreSQL also executes `SECURITY DEFINER` functions
with their owner's privileges. See the official documentation for
[function side effects and volatility](https://www.postgresql.org/docs/current/sql-createfunction.html),
[predefined server-file roles](https://www.postgresql.org/docs/current/predefined-roles.html),
and [function execution privileges](https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY).

This slice removes arbitrary SQL text from the executable connector input. It
does not attempt to make a database administrator's chosen binding safe: the
binding must still point at the intended database, use a least-privilege
credential, and allow only intended base tables.

## Invariants

- Query intent is a typed table/column/filter/order/limit specification. The
  caller cannot submit SQL syntax, function names, casts, joins, CTEs, or
  expressions.
- Relation and column identifiers are normalized, bounded, and quoted by the
  connector. Generated `FROM ONLY` clauses prevent an allowlisted inheritance
  parent from returning rows stored in unlisted child tables. Values are
  always driver parameters. Decimal and large-integer JSON values retain
  exact base-10 semantics; the adapter does not round them through `float64`.
- Every requested relation must exactly match the workspace binding's schema
  and table allowlists. Every selected, filtered, and ordered column must be
  declared by that relation and use a built-in PostgreSQL type.
- The PostgreSQL driver accepts only a persistent ordinary base table; it
  rejects views, foreign tables, temporary relations, and custom column
  types. `search_path` is pinned to `pg_catalog` in the transaction.
- The driver opens a repeatable-read, read-only transaction so catalog
  validation and query execution share a snapshot; it applies both a request
  context deadline and server statement timeout. Row and serialized-result
  byte budgets are checked after the driver materializes each row. They bound
  the returned result, but are **not** a hard network or peak-memory bound for
  one oversized database value; that remains a qualification gap. These
  controls supplement, but do not replace, a least-privilege external database
  role and RLS.
- SQL payload hashes bind the normalized structured query. Raw SQL, raw result
  rows, credentials, and host connection details do not enter generic
  operation history.
- Unsupported schema versions, injected legacy statement fields, unbound
  values, unknown identifiers/operators, and cross-workspace targets fail
  closed before the database adapter is called.

The connector's result digest identifies the rows returned by one query
snapshot; it does not promise byte-for-byte stable results across executions
when the request omits ordering, ordering keys tie, or source data changes.
Callers that need stable replay comparisons must use a stable total ordering
and compare against the same source snapshot.

## Contract and compatibility decision

Replace `Payload.Statement` and positional `Payload.Parameters` with explicit
`Schema`, `Table`, `Columns`, bounded `Filters`, `OrderBy`, `Limit`, and
`MaxBytes` fields. Supported filters are equality/inequality and ordered
comparisons, `LIKE`/`ILIKE`, `IN`, `IS NULL`, and `IS NOT NULL`; all values are
parameterized. This is intentionally a read/query contract, not a general SQL
console.

Bump the SQL connector/capability schema identity to v2. Do not reinterpret
already-persisted v1 payload hashes or silently convert raw SQL. Existing
v1 bindings must be explicitly replaced by a v2 binding and new typed request.
No migration is needed because raw query payloads are resolved from their
content hash and are not copied into the connector-binding authority tables.

## Store and execution design

`sqlreadonly` compiles a normalized query specification into one deterministic
parameterized `SELECT` using quoted identifiers from the exact table binding.
The compiler emits no free-form SQL fragments and uses `FROM ONLY`. `EXPLAIN`
wraps only this generated query. The Postgres driver verifies base-table and column metadata
inside a repeatable-read, read-only transaction before executing the generated statement.
Schema changes therefore fail closed instead of widening access.

No query result becomes durable raw authority. Existing bounded row/byte
collection, deterministic cost estimation, result/evidence hashes, task and
workspace controls, generic operation admission, and at-least-once semantics
remain in place.

## Cost, storage, and licensing

No service, model call, migration, or new Go dependency is planned. The
structured payload is bounded by fixed limits on columns, filters, sort terms,
parameter bytes, rows, response bytes, and time. The live PostgreSQL driver
adds one catalog validation round trip per operation before the bounded query; no
catalog cache is introduced because a stale schema cache could authorize a
removed or changed relation. Measure the extra round trip and relation/row
growth in disposable-Postgres CI; no latency or planner-cost claim is made in
advance.

This independently tightens Fornix's adapter contract and uses the existing
pgx driver. No reference source is copied and no license changes.

## Acceptance tests

- Deterministic generated SQL and parameter order for identical normalized
  query specs; no caller value is interpolated into SQL.
- Injection strings in legacy statement fields, identifiers, filter values,
  table names, sort terms, and operators fail closed or remain data parameters.
- Function calls, casts, joins, subqueries, comments, semicolons, and
  schema-qualified non-allowlisted relations cannot be expressed by the
  contract and never reach the `Database` interface.
- Unknown and cross-workspace relations, columns, operators, and out-of-budget
  query specs fail before a database call.
- Existing result bytes, row, timeout, and cost budgets remain enforced.
- PostgreSQL qualification proves ordinary base-table reads work, views,
  foreign/temp relations and custom-type columns are rejected, the transaction
  is read-only, and prepared values cannot alter query structure.
- Existing HTTP connector, operation, replay, workspace-isolation, race,
  smoke, and documentation checks remain green.

## Remaining limitations

This prevents request-supplied SQL expressions and limits relation kinds, but
the configured database role, table grants, RLS policies, trusted database
extensions, and database server remain part of the security boundary. It does
not qualify arbitrary database vendors or production credentials. Live
PostgreSQL checks require the disposable connector qualification database.

The adapter is currently exercised through explicit registry construction;
default server startup does not yet provision workspace SQL bindings or
register a database connector. This slice alone does not make SQL available
through the bundled public API. Result hashes represent the returned snapshot,
not a replay-stable answer when ordering is absent or non-unique, or when
source rows change.
