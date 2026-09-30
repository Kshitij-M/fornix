# Loop 74B completion — generic effect and domain-link finalization

Status: implemented repository-owned mutation-boundary slice; not a
production-readiness declaration.

## Delivered

- Added a caller-owned workspace transaction seam to compose generic effect
  transitions with append-only domain-link transitions.
- Added `linked → reconciled` and `linked → recovery_required` transition
  support with existing version and idempotency fences.
- Updated the generic effect dispatcher so verified effects reconcile their
  domain link with the stable response hash, while uncertain or missing
  effects mark both authorities as recovery-required without claiming success.
- Duplicate dispatches return the current workspace-scoped link state and do
  not invoke the external adapter again.
- Kept verification-required effects linked until an explicit verifier supplies
  proof; the dispatcher does not invent verification.
- Added integration assertions for successful reconciliation, uncertain
  recovery, duplicate delivery, and stable link state.

## Verification

The PostgreSQL-backed integration tests are covered by the existing CI database
job and are skipped locally when `FORNIX_TEST_PG_DSN` is absent. The complete
offline suite, race checks, package checks, hooks checks, documentation checks,
and diff checks are run in the task handoff. No Docker image, database volume,
provider, credential, broker, or additional storage is created by this slice.

## Cost, storage, and limits

The slice adds no table and no service. A terminal or recovery publication uses
one bounded Postgres transaction and one append-only domain-link transition row.
The remote call remains outside the transaction and therefore remains
at-least-once. Initial generic reservation, specialized-ledger mutation, and
link binding are not universally composed in one transaction; adapters still
need explicit authority-aware integration tests.

## Next task prompt

Task 75 — build the portable qualification runner and bounded evidence bundle.

Read the chats directory, `AGENTS.md`, docs 14, 90, 111, 159, 163, 165, 167,
169, 171, 173, and this completion note. Add a deterministic offline runner
that registers checks in stable order, enforces check/runtime/report budgets,
redacts secrets, emits one canonical `QualificationReport` manifest, and can
merge revalidated deployment evidence without contacting deployment systems.
Keep the default matrix read-only. Add explicit dispatcher-backed fake-effect
checks for successful reconciliation, uncertain recovery, stale-fence failure,
duplicate suppression, and receipt/link hash integrity. Do not claim live
provider, secret-manager, HA/PITR, retention-scale, sandbox, or load/soak
qualification from offline checks. Add CLI, Make, CI, smoke, race, and public
documentation coverage.
