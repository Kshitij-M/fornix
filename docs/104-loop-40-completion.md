# Loop 40 completion: Postgres workspace-isolation foundation

Status: implemented as a generic-authority defense-in-depth foundation;
production role separation and complete legacy-table policy coverage remain
deployment gates.

Delivered:

- migration 044 with a transaction-local workspace-context function and
  idempotent row-level-security policies for the 17 generic operation,
  admission, approval, effect, lease, callback, result, and replay tables;
- store transaction boundaries that set workspace context before protected
  reads and writes, while retaining explicit workspace predicates;
- an opt-in non-owner/NOBYPASSRLS qualification test that proves unset-context
  fail-closed reads, same-workspace writes, foreign-read invisibility, and
  foreign-write rejection without persisting fixtures;
- `make qualification-workspace-isolation` and a redacted smoke entrypoint;
- public deployment, cost, failure, acceptance, and limitation documentation.

Measured local qualification:

- the complete Postgres-backed store/server/workflow/scheduler/change/eval/
  retrieval integration suite passed against a database migrated through 044;
- the dedicated non-owner qualification passed against a cloned database with
  an explicit `NOSUPERUSER NOBYPASSRLS` runtime role;
- migration 044 is idempotent and adds one bounded transaction-local context
  statement per protected generic transaction plus a simple indexed equality
  policy predicate;
- no model, tool, broker, or external network call is made by this slice.

The result is intentionally narrower than a universal tenancy certification.
The local development role still owns the tables, production must transfer
ownership to a migration role and grant least privilege to the runtime role,
and identity, retrieval, artifacts, tasks, and older compatibility surfaces
still require staged policy coverage and adversarial qualification.
