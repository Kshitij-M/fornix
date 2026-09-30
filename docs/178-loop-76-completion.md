# Loop 76 completion — authority-aware effect qualification observation

Status: implemented repository-owned qualification slice; not a
production-readiness declaration.

## Delivered

- Added a bounded `EffectAuthorityObservation` contract containing only
  workspace-scoped identities, hashes, terminal-state facts, proof flags, and
  an external invocation count.
- Required successful observations to include generic reservation identity,
  domain-link identity, result and Work Receipt hashes, an explicit receipt-link
  hash and verification flag, duplicate suppression, stale-fence rejection,
  workspace-isolation proof, and replay stability.
- Prevented uncertain recovery observations from claiming a result, receipt,
  or receipt-link hash.
- Added `RunEffectAuthorityProbe` and `NewEffectAuthorityCheck` adapters. A
  probe is authority-owned and explicit; it cannot discover credentials or
  deployment state implicitly.
- Extended the qualification manifest with an `offline` or `external`
  execution mode. The runner blocks external checks by default and executes
  them only when `AllowExternalChecks` is explicitly enabled.
- Added deterministic unit coverage for complete success proof, incomplete
  receipt linkage, uncertain recovery, stable redacted hashes, and the
  default-blocked/explicitly-enabled external check boundary.
- Added `make qualification-effect-observation` and a CI check for the seam.

## Verification

The observation contract and runner tests pass locally. The check is fully
offline and does not contact PostgreSQL, a provider, a tool, a broker, or a
deployment. Existing dispatcher and domain-link PostgreSQL tests remain the
authority-owned source of live behavior; they are intentionally not converted
into a default CI deployment claim.

## Cost, storage, and limits

This slice adds no migration, service, persistent row, network call, or
container. The new evidence is bounded to hashes, booleans, labels, and one
small measurement. External invocation count is capped at two by the contract,
while a successful or uncertain proof requires exactly one invocation. The
runner still cannot prove provider idempotency, live credential rotation,
PostgreSQL HA/PITR, sandbox strength, or deployment topology.

## Next task prompt

Task 77 — build one explicit disposable-PostgreSQL authority qualification
probe over the existing generic effect dispatcher and domain-effect link
stores. Read the chats directory, AGENTS.md, docs 14, 90, 111, 132, 134, 136,
171, 173, 175, 177, and this completion note. The probe must create a bounded
workspace and fake provider, exercise reservation, one successful dispatch,
duplicate delivery, stale-fence rejection, cross-workspace rejection, and
replay, then emit only `EffectAuthorityObservation` hashes and facts. It must
require an explicitly supplied disposable DSN, never use the persistent
development database, never print credentials or provider payloads, clean up
its temporary rows, and remain opt-in from CI. Add crash/rollback assertions,
concurrency coverage, CLI or Make documentation, and measured SQL/latency
limits. Do not call it production certification.
