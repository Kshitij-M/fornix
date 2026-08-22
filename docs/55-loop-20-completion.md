# Loop 20 completion — Work Receipts and Verified Change Packet foundation

Status: implemented on `feat/work-receipts`; awaiting CI and maintainer review
before merge.

## Delivered

- Added typed Work Receipt contracts in
  `internal/contracts/work_receipt.go`: immutable work identity, bounded
  steps, typed evidence/artifact links, normalized authoritative references,
  measured-versus-estimated cost fields, replay/fence verification, stable
  hashes, and bounded gist/detail/raw disclosure requests/results.
- Added migration `028_work_receipts.sql` with workspace-scoped immutable
  receipt, step, and normalized reference tables. The tables use uniqueness
  constraints for natural work identity and idempotency, bounded payload
  checks, composite workspace keys, indexes, and append-only mutation
  triggers.
- Added `WorkReceiptStore` with one Postgres transaction for terminal-state and
  fence validation, authoritative reference resolution, typed evidence/artifact
  integrity checks, receipt insertion, step/link insertion, and commit. A
  deterministic failure hook proves rollback before commit.
- Receipt hashes exclude delivery IDs and wall-clock fields while preserving
  workspace, work, actor, fence, step, source-hash, accounting, and replay
  semantics. Receipt raw disclosure is canonical redacted metadata, not a
  second copy of prompt/tool output.
- Added authenticated HTTP routes:
  `POST /v1/work-receipts`, `GET /v1/work-receipts/{id}`, and
  `POST /v1/work-receipts/disclose`. Added explicit `receipt:read` and
  `receipt:write` RBAC capabilities.
- Extended the deterministic CLI with `receipt get` and `receipt disclose`,
  and extended the MCP shim with equivalent receipt inspection tools.
- Integrated receipt finalization into the reference workflow after task
  completion and replay. The workflow links task, agent-run, artifact,
  evidence, source manifest, context, and replay identities.
- Updated the public README, HTTP reference, documentation index, and
  production qualification so the Work Receipt is described as the durable
  foundation behind the Verified Change Packet, without claiming a complete
  patch-application or exactly-once system.
- Added the dedicated `scripts/test/v0.30-work-receipt-smokes.sh` and
  `make smoke-work-receipts` target. It executes the full reference workflow,
  reads the receipt through the authenticated HTTP API, and verifies bounded
  gist disclosure preserves the canonical receipt hash.

## Qualification tests

The following focused tests passed against the local Docker PostgreSQL 17
instance:

- contract normalization, deterministic hash, redaction, ordinal ordering,
  unsafe metadata rejection, workspace checks, and disclosure-budget tests;
- concurrent receipt finalization with one created row and one step/reference
  set;
- conflicting idempotency/natural-identity requests and stale task-fence
  rejection;
- crash injection before commit with no receipt or partial links, followed by
  a successful retry;
- typed evidence and content-addressed artifact reference/hash validation;
- hash-preserving gist/detail/raw disclosure with bounded item/byte/token
  budgets;
- cross-workspace reads and append-only update/delete rejection;
- migration application on the existing development database.

The focused Postgres Work Receipt suite completed in approximately 0.83 seconds
on a warm local Docker/Postgres setup. This is a test-suite wall-clock
measurement, not a production SLO. Finalization performs one bounded
transaction, validates at most 128 references plus 128 typed evidence/artifact
links, inserts one receipt, up to 64 steps, and up to 128 normalized links.
It performs no model, tool, broker, embedding, or filesystem work. Storage is
the canonical payload plus O(steps + links); raw evidence and artifact bytes
remain in their existing authorities and are not duplicated by the receipt.

The final pre-PR gates were also green: `make check`, `make verify`,
`make hooks-check`, the complete `make smoke` matrix, and the dedicated v0.30
receipt smoke. `make verify` includes the full race suite and all three binary
builds; the smoke matrix covers the existing v0.10–v0.29 surfaces plus v0.30.

## Remaining limitations

- This slice creates a verified envelope, not a semantic source-code diff or a
  cryptographically signed/notarized change packet.
- The reference workflow is still read-only and fake-provider by default. It
  does not apply patches, run a general validation suite, or provide a
  reviewer approval UI.
- Validation and cost references are bounded; the current repository has no
  separate durable validation authority, so validation references are stable
  hashes until that authority is introduced.
- Postgres remains the only storage authority. There is no object-storage cold
  tier, receipt retention compactor, RLS policy, SSO/KMS integration, or HA
  backup/restore qualification.
- Remote model and external process effects remain at-least-once. A Work
  Receipt records that boundary and never asserts exactly-once execution.

## Review gate

This branch must be pushed as a draft PR. CI must pass, and an additional
maintainer review is required before the protected `main` branch can accept
the change. The receipt feature should be merged only after the reviewer has
checked migration compatibility, workspace/fence fail-closed behavior,
redaction, and the end-to-end reference smoke.
