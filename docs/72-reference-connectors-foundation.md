# Bounded HTTP/API and read-only SQL reference connectors

Status: alpha implementation note for Issue [#43](https://github.com/Kshitij-M/fornix/issues/43). The bounded HTTP/API and read-only SQL reference adapters, migration 037, binding store, shared conformance tests, and CI/smoke coverage are implemented on the universal transformation branch. Production qualification and broader adapter integration remain open.

The current SQL connector contract is `sqlreadonly@2`. The original v1
statement-shape checks were replaced by the structured-query boundary recorded
in [the SQL query-contract feature note](267-sql-read-only-query-contract-foundation.md)
and [Loop 120 completion](268-loop-120-completion.md). The historical Loop 26
record describes the original v1 implementation, not the current wire format.

## Purpose

Repositories are the first Fornix adapter, not the product boundary. This
slice proves that the same typed operation, admission, evidence, budget, and
recovery rules can govern two materially different production systems:

- an HTTP/API resource with bounded reads, lists, and explicitly idempotent
  submissions;
- a database resource with describe, read-only query, and read-only explain.

The connectors are deliberately narrow. They are not arbitrary URL fetchers,
general SQL consoles, credential stores, or a new orchestration service.

## Invariants

1. **Typed input only.** Connector input is a versioned, canonical payload
   resolved by its request `InputHash`. Raw input is not placed in the durable
   operation, event, admission, metric, or error records. The resolver verifies
   the SHA-256 before the adapter can use the payload.
2. **Configured targets only.** HTTP requests are derived from a configured
   base URL, exact host/port policy, normalized path prefixes, and an explicit
   private-network setting. A caller cannot supply an arbitrary absolute URL,
   redirect target, proxy, or DNS destination.
3. **Read is the default.** `http.read`, `http.list`, `sql.describe`,
   `sql.query_readonly`, and `sql.explain_readonly` are read-only or
   observation capabilities. `http.submit_idempotent` is an explicit external
   effect, requires an idempotency key, admission/approval as selected by the
   policy, and verification when the binding requires it.
4. **SQL has no caller-authored syntax.** The v2 input is a structured
   schema/table/column/filter/order/limit specification. The connector compiles
   it into one parameterized query; callers cannot supply functions, casts,
   joins, CTEs, expressions, comments, or statement fragments. PostgreSQL
  independently enforces the exact table binding, persistent ordinary-table
  kind, built-in scalar column types, repeatable-read/read-only transaction,
  pinned `search_path`, request deadline, statement timeout, and result budgets. Generated reads use
  PostgreSQL `ONLY`, preventing an allowlisted inheritance parent from
  implicitly returning child-table rows.
5. **Credentials are references.** A capability declares credential reference
   identities. The resolver is the only code allowed to obtain a short-lived
   secret; the value is injected into the outbound request or database
   connection and is never returned, hashed into evidence, logged, or persisted.
6. **Bounded disclosure.** Request bytes, response bytes, rows, columns,
   pagination, redirects, retries, and timeout are capped by the capability
   definition and binding. Truncation is explicit and hashable; limits are
   checked before allocation and while streaming.
7. **Evidence is redacted and linked.** Every successful connector result has
   a schema hash, output hash, and bounded evidence reference. Raw external
   payloads may be handed to the existing evidence/artifact authority by a
   higher layer, but this connector package never stores them as authority.
8. **External delivery is explicit.** HTTP submission is at-least-once unless
   the remote provider's idempotency behavior is recorded. A crash after
   dispatch is uncertain and must be reconciled; the connector never claims
   exactly-once execution.
9. **Workspace isolation is structural.** Binding, capability, target,
   credential reference, evidence, operation, and result must share one
   workspace. Cross-workspace lookups, bindings, and evidence fail closed.
10. **Replay is inert.** Replay consumes recorded connector responses and
    hashes only. It never performs a live HTTP call, opens a live SQL
    connection, repeats a submission, or resolves a credential.

## Typed payload boundary

`contracts.OperationRequest` intentionally carries only the input schema and
content hash. The connector receives a `PayloadResolver` and resolves the
bounded typed JSON envelope by `(workspace_id, input_hash)`:

```text
OperationRequest.InputHash
        │
        ├─ verify resolver bytes are <= capability input budget
        ├─ verify SHA-256(bytes) == InputHash
        ├─ decode one connector-specific versioned envelope
        └─ validate allowlists, budgets, and target binding
```

This keeps prompts, SQL text, request bodies, and credentials out of the
control-plane rows while still allowing a real adapter to execute typed input.
The first implementation supplies an in-memory resolver for deterministic
offline tests; production wiring may resolve the same hash from the existing
artifact/evidence authority.

## HTTP contract

The HTTP connector exposes exactly:

| Capability | Effect | Input | Safety boundary |
| --- | --- | --- | --- |
| `http.read` | read-only | method `GET`/`HEAD`, relative path, bounded headers/query | configured host/path, no body, no redirects outside binding |
| `http.list` | read-only | relative path, bounded page size/token | bounded pages, response bytes, stable item hashes |
| `http.submit_idempotent` | reversible/external write | method/path, body hash, idempotency key | admission/approval, credential ref, provider key, verification contract |

The default transport rejects IP literals and private, loopback, link-local,
multicast, unspecified, and metadata-service destinations unless a binding
explicitly enables private targets. DNS resolution is checked before dialing;
redirects are disabled by default and revalidated against the same policy when
enabled. The test transport uses an `httptest` server and an injectable dial
policy so no public network is required.

## SQL contract

The SQL connector exposes:

| Capability | Effect | Input | Safety boundary |
| --- | --- | --- | --- |
| `sql.describe` | observation | v2 allowlisted schema/table identity | catalog-only, bounded columns |
| `sql.query_readonly` | read-only | v2 explicit columns, fixed filter operators, ordering, and limit | internally compiled query, parameterized values, exact table/column checks, read-only transaction, row/byte/time budgets |
| `sql.explain_readonly` | observation | the same v2 structured query | `EXPLAIN` over connector-generated SQL only; never `ANALYZE`; bounded plan output |

The binding supplies a workspace-specific database target, credential
reference, allowed schemas/tables, maximum rows/bytes, deterministic result
cost units, and statement timeout.
The connector uses `pgx` with a separate pool or transaction configuration;
Fornix Postgres remains the control-plane authority and is never queried
through this adapter. The model cannot choose a DSN, database, schema, table,
or transaction mode. Generated reads use `ONLY` to avoid implicitly querying
unlisted inheritance children. The request cannot choose SQL syntax: identifiers
are validated and quoted, values are parameters, and only fixed operators and
ordering directions are accepted. PostgreSQL rejects views, foreign tables,
temporary tables, and non-built-in/non-scalar query columns. Each request uses
a repeatable-read, read-only snapshot plus a client deadline and server
statement timeout. External database roles still require least-privilege
grants and appropriate RLS policies.

## Binding and persistence plan

Migration `037_connector_bindings.sql` persists immutable workspace-scoped
binding identity and configuration hashes, redacted allowlists, credential
references, budgets, and lifecycle status. Secret values and raw connection
strings are excluded. A binding update creates a new version/hash; old
versions remain auditable and cannot silently change an admitted operation.

The binding store provides bounded create/get operations with idempotent
requests and workspace-scoped reads. Adapter construction remains
explicit and process-local in this slice; a future runtime may hydrate a
validated binding into a connector registry, but no database row will execute
code by itself.

## Reuse and licensing decisions

- Reuse Fornix `CapabilityDefinition`, `OperationRequest`, `OperationResult`,
  `ExternalEffect`, `OperationEvidenceRef`, connector registry/conformance,
  credential references, event store, operation authority, and admission/effect
  boundary.
- Reuse the security ideas of Orloj's bounded HTTP/tool runtime and authz
  seams, DeepSeek Harness's explicit provider/plugin boundary, Dagger's
  transport/secret separation, and DBOS's durable idempotency/retry discipline.
  Implementations are independent; no reference source is copied.
- Do not copy Kronaxis-fabric source because its BSL 1.1 license is not
  compatible with the MIT-licensed Fornix distribution.
- No new broker, Redis, NATS, workflow service, object store, or policy
  engine is introduced. Postgres remains the control-plane authority.

## Cost and stability budget

- HTTP request bytes, response bytes, redirects, pages, and retries are
  bounded before execution. Expected connector work is O(pages) and O(response
  bytes), with no unbounded buffering.
- SQL work is one prepared read-only statement or bounded catalog query. Row,
  byte, statement-time, and deterministic result-cost budgets are enforced in
  the adapter; cost units are a conservative result-size estimate, not a
  provider billing or database planner-cost claim. Query plans and outputs are
  hash-only in Fornix authority.
- Each successful connector invocation creates one bounded operation result and
  evidence link. External-effect transitions remain one append-only row plus
  one event per state change.
- Fake transport/database tests must measure p50/p95 latency, rows, bytes,
  retries, SQL statements, and duplicate work. Values reported in the
  completion note are measured locally, not extrapolated capacity claims.

## Acceptance tests

- registry lookup and shared conformance pass for all six capabilities;
- HTTP host/path allowlists reject arbitrary URLs, redirects, private-network
  targets, DNS rebinding candidates, oversized requests/responses, and
  unsupported methods;
- HTTP pagination is bounded and deterministic; retries are classified; a
  duplicate idempotency key creates one recorded effect;
- HTTP credentials are resolved by reference and absent from logs, errors,
  evidence, hashes, and results;
- SQL rejects v1 and arbitrary SQL-text fields, malformed identifiers,
  unsupported operators, unallowlisted relations, and oversized query terms
  before database access;
- SQL values remain bound parameters even when they contain SQL-looking text;
  generated query text is deterministic and identical structured inputs have
  stable query/evidence hashes;
- PostgreSQL rejects views and other non-persistent/non-ordinary relations,
  validates all referenced columns, and executes generated reads in a
  read-only transaction with timeout, row, byte, and cost bounds;
- both connectors reject cross-workspace targets/bindings/evidence;
- cancellation and timeout stop work without corrupting Postgres authority;
- crash before acknowledgement leaves no committed connector effect; crash
  after dispatch is represented as recovery-required and is safely replayable;
- replay uses recorded fake responses and never calls the fake transport or
  database a second time;
- identical typed inputs and fake responses produce identical plans, output
  hashes, evidence references, and metrics;
- existing repository behavior, race tests, CI, smokes, and documentation
  checks remain green.

## Deliberate limitations

This slice does not implement provider-specific catalogs, arbitrary API
discovery, write-capable SQL, generic OAuth flows, a full network sandbox,
signed webhook verification, database failover, or remote exactly-once
semantics. Those require separate qualification and deployment decisions.
