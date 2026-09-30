# Loop 75 completion — portable qualification runner and evidence bundle

Status: implemented repository-owned qualification slice; not a
production-readiness declaration.

## Delivered

- Added bounded qualification check and manifest contracts with stable hashes,
  target/workspace binding, commit metadata, and environment-variable names
  without environment values.
- Added a sequential deterministic runner with explicit check registration,
  stable ordering, no retries, offline-only admission, cancellation/deadline
  handling, report-byte limits, and redacted error classification.
- Added built-in side-effect-free contract, determinism, and redaction checks.
- Added deterministic validated bundle merging for deployment-produced evidence;
  duplicate bundle hashes are idempotent and conflicting cases fail closed.
- Added native CLI commands:
  - 'fornix qualification run --file PATH'
  - 'fornix qualification merge --file PATH --inputs A,B'
  - 'fornix qualification validate --file PATH'
  - 'fornix qualification hash --file PATH'
- Added atomic bundle writes with bounded input/output and restrictive
  permissions.
- Added a Make target and shell wrapper used by CI. The default path uses
  ephemeral storage and performs no database, provider, model, tool, broker,
  network, or deployment operation.

## Verification

The runner has unit coverage for ordering/timing hash stability, duplicate
registration, offline blocking, timeout bounds, error redaction, deterministic
merge, workspace isolation, and environment-value rejection. The portable
qualification smoke passed locally and emitted matching report/manifest hashes
for generation and validation.

## Cost, storage, and limits

No migration, service, database row, provider call, or external side effect was
added. The default profile permits at most 64 checks, five minutes, and 128 KiB
of report/bundle data. CI writes to ephemeral temporary storage and removes the
bundle after validation unless an operator explicitly supplies an output path.
A check that ignores cancellation can continue in its own goroutine; portable
checks are required to be side-effect-free and context-aware.

The runner validates deployment evidence but cannot create genuine proof of
deployment-owned HA/PITR, secret-manager, live-provider, retention-scale,
sandbox, or load/soak behavior.

## Next task prompt

Task 76 — add authority-aware dispatcher-backed portable effect qualification.

Read the chats directory, AGENTS.md, docs 14, 90, 111, 132, 134, 136, 138,
171, 173, 175, and this completion note. Extend the portable runner with a
read-only-by-default fake authority probe that proves generic reservation,
domain-link binding, successful reconciliation, uncertain recovery,
duplicate suppression, stale-fence rejection, and Work Receipt hash linkage.
Keep the probe entirely in-memory or against an explicitly disposable
PostgreSQL DSN; never report deployment qualification from a local fake.
Make the evidence bounded, redacted, deterministic, and importable. Add
concurrency, crash, replay, workspace-isolation, CLI, CI, race, and public
documentation coverage.
