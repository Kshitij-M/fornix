# Loop 77 completion — disposable PostgreSQL effect-authority probe

Status: implemented repository-owned qualification slice; not a
production-readiness declaration.

## Delivered

- Added `RunPostgresEffectAuthorityProbe` with an explicit DSN, workspace,
  run identifier, and bounded timeout. It never discovers or emits DSN values.
- Composed the real operation lease, admission, effect reservation,
  domain-effect link, Work Receipt, and operation authority-link stores around
  a deterministic in-process fake invoker.
- Proved one successful dispatch, one duplicate replay without a second
  invocation, Work Receipt finalization and idempotent replay, receipt-link hash
  verification, receipt rollback before commit, stale-fence rejection before
  invocation, and cross-workspace link isolation.
- Returned only `EffectAuthorityObservation` hashes, booleans, identities, and
  the bounded invocation count. Driver errors and raw payloads are classified
  into stable phase errors.
- Added the opt-in integration test, explicit disposable-database confirmation,
  `make qualification-effect-authority-probe`, and the executable shell
  wrapper. The default suite skips the database probe and remains offline.
- Preserved append-only authority history. The probe never disables triggers,
  truncates shared tables, or deletes rows; the caller discards the confirmed
  disposable database after the run.

## Verification

The qualification and contract packages pass with no DSN. The opt-in test
correctly skips unless both `FORNIX_RUN_EFFECT_AUTHORITY_PROBE=1` and
`FORNIX_DISPOSABLE_DATABASE_CONFIRM=1` are set. The shell wrapper refuses to
run without an explicit DSN and confirmation. A live result requires a
disposable PostgreSQL/pgvector database; none was started implicitly.

## Cost, storage, and limits

The normal test and CI paths add zero database work. The opt-in profile uses a
single bounded pool, two operations, one fake external invocation, one receipt,
and a 30-second default/2-minute maximum deadline with no retries. It adds no
migration, persistent qualification table, provider call, artifact, broker,
or container. It intentionally leaves the small append-only evidence rows in
the disposable database so the authority history is not weakened; database
disposal is the cleanup boundary.

This still does not prove hosted PostgreSQL HA/PITR, live provider
idempotency, credential rotation, sandbox strength, network partitions,
deployment topology, or load/soak behavior.

## Next task prompt

Task 78 — build the deployment-owned qualification importer and signed evidence
bundle. Read the chats directory, AGENTS.md, docs 14, 90, 111, 159, 161, 163,
165, 175, 177, 179, and this completion note. Add an explicit signature and
key-reference contract for imported authority observations; bind the signature
to workspace, target, runner version, commit, environment-name set, and
observation hash; reject stale, cross-workspace, unsigned, duplicate-conflict,
or over-budget bundles; keep private keys outside Fornix and never persist
secret material; support offline validation and deterministic merge; and add
operator documentation. Do not represent a locally generated signature as
hosted production proof.
