# Loop 14 completion — artifact-backed output integration and retention operations

Status: complete for the bounded Postgres vertical slice.

## Delivered

- Added migration `021_artifact_output_links.sql` with workspace-scoped links
  from tool stdout/stderr/result, evidence raw payloads, and agent output/history
  to immutable artifacts. It also adds append-only artifact lifecycle history,
  integrity indexes, composite workspace foreign keys, and the one-way evidence
  backfill guard.
- Integrated `ArtifactStore` transactionally into tool completion, evidence
  creation, and agent checkpoint commits. Oversized values are redacted where
  appropriate, stored by SHA-256 identity, and replaced in hot rows with
  bounded markers; small existing values stay inline unchanged.
- Preserved actor, workspace, causation, correlation, source, role, and
  idempotency metadata on artifact references. Task-bound tool and agent writes
  validate the current task fence before artifact creation.
- Added bounded, deterministic, source-specific backfill for tool, evidence,
  and agent rows. It supports stable cursors, batch caps, dry runs, and
  idempotent re-entry.
- Added retention sweeps with explicit archive-then-delete phases, dry runs,
  retention deadlines, authoritative-reference blocking, tombstones, and
  append-only lifecycle events.
- Added bounded integrity verification/corruption reporting and storage,
  reference, chunk, logical-byte, deduplication, and status metrics.
- Added crash rollback, duplicate, disclosure, retention, integrity,
  workspace, and smoke/CI coverage. Added v0.23 development and HTTP smoke
  commands.

## Qualification results

- Fresh database startup applied all migrations through 021, including the
  artifact-output schema. Existing database startup revalidated the immutable
  migration checksum:
  `ffeae7588ac6eaf0316efb3e178ec74adf98a7d8273ba12485f6596405061449`.
- Host-independent Go tests: `make test` passed with the configured local
  Postgres DSN; `make vet`, `make build`, and `make python-check` passed.
- `go test -race ./...` passed. The complete smoke chain v0.10 through v0.23
  passed against a current-code HTTP instance, including v0.22 artifact
  storage and v0.23 output operations.
- Duplicate oversized tool delivery returned one durable source effect and
  stable artifact identities. Duplicate oversized evidence returned the same
  raw artifact and disclosure hash. Agent history reloaded with the same
  state hash and artifact identity.
- Injected failures after artifact-reference insertion rolled back both tool
  and agent source mutations; no artifact or reference remained. Dry-run
  backfill and retention produced no writes. Missing artifact chunks were
  detected by both batch and single-artifact verification.

## Local measurements

Measurements are local Postgres samples, not capacity guarantees:

- Oversized tool output: approximately 66 KiB input, `34 ms` end-to-end in
  the current v0.23 smoke run. The focused runs observed `28–268 ms` across
  warm/cold container conditions.
- Oversized evidence: approximately 4.20 MiB raw input, `147 ms` in the
  current v0.23 smoke run; focused runs observed `63–164 ms`. The raw bytes
  use the existing 256 KiB artifact chunk size and are read only on explicit
  disclosure.
- Oversized agent history: approximately 4 MiB serialized history, `290 ms`
  in the current v0.23 smoke run. The checkpoint and artifact link remain one
  Postgres transaction.
- A one-row backfill linked one oversized tool output at approximately
  `94 rows/s` in the current smoke run; the focused sample ranged from `88–188
  rows/s`. The default operator batch is 100 and is capped at 1,000.
- A representative metrics sample reported one artifact, 13 artifact/chunk
  bytes, one reference, and a `1.00` unique/logical ratio. Task 13’s broader
  artifact sample measured approximately `2.66 MiB` of artifact-related
  Postgres relation storage for 20 small test artifacts; actual production
  storage is dominated by raw payload size, chunk/index overhead, and the
  retention window.
- A new oversized write performs one source-row lock/update, one canonical
  artifact identity lookup/insert, bounded chunk inserts, one reference
  insert, and the existing lifecycle event/checkpoint update. A duplicate
  content write reuses the canonical artifact and performs reference
  idempotency work only. Backfill, retention, and verification are bounded
  ordered operations and are not run as unbounded request scans.

## Reuse, license, and limitations

The implementation reuses Fornix’s existing artifact, event, evidence,
identity, task-fence, and checkpoint primitives. FornixDB’s non-destructive
tiering/disclosure ideas, agentmemory’s dry-run retention and diagnostics,
Orloj’s transactional cleanup/checkpoint discipline, and DeepSeek Harness’s
lossless output boundary informed the design. No reference source was copied;
the repository remains MIT-licensed, and Kronaxis Fabric’s BSL 1.1 code was
not reused.

The current slice intentionally remains Postgres-only. There is no object
store, streaming verifier, resumable upload, background retention scheduler,
partitioning, cold tier, or backup/restore drill. Backfill is producer-specific
and operator-triggered; it does not migrate every historical prompt or inline
payload. External process/model effects remain at-least-once even though the
durable source mutation and artifact reference are idempotent.
