# Task 80 — Loop completion: qualification trust distribution

Status: implemented on the feature branch; local verification complete. This
is not a production-readiness declaration.

## Delivered

- Added typed, bounded, public-key-only trust snapshot contracts with stable
  entry ordering, content hashes, Ed25519 signing, validity windows, explicit
  status, publisher identity, and signer-entry authorization.
- Added migration 067 for workspace/deployment-scoped snapshot rows,
  append-only publication/revocation events, bounded signed bytes, RLS,
  monotonic revision/hash/idempotency identities, and nullable snapshot
  revision/hash columns on qualification imports.
- Added transactional snapshot publication, duplicate replay, monotonic
  revision checks, current-load verification against the Task 79 signer
  catalog, revocation, bounded listing, and explicit raw disclosure.
- Bound every new authorized import to the exact current snapshot revision and
  hash inside the import transaction. Historical Task 79 imports remain
  readable and idempotent without being rewritten.
- Added authenticated snapshot HTTP routes and separate RBAC behavior for
  listing/disclosure versus publication/revocation.
- Added CLI commands for local signing and authenticated publish/list/get/
  revoke operations. Private keys are read only from an explicitly named local
  file and are never sent in requests or printed.
- Added optional startup/readiness conformance through
  `FORNIX_REQUIRE_QUALIFICATION_TRUST` and
  `FORNIX_QUALIFICATION_DEPLOYMENT_ID`. The default development startup remains
  compatible, while new durable authorized imports still require a current
  snapshot.
- Updated the API reference, qualification runbook, documentation map, public
  roadmap, Make targets, and CI integration qualification.

## Qualification evidence

The following commands pass locally with caches cleared before execution:

```text
go test ./internal/contracts ./internal/store ./internal/server ./internal/config ./cmd/fornix
```

The focused PostgreSQL tests are present and skip when
`FORNIX_TEST_PG_DSN` is unset. CI runs them against its disposable PostgreSQL
service through `make qualification-trust-distribution`. The local machine
did not have a reachable PostgreSQL endpoint, so migration execution,
transactional snapshot publication, RLS, concurrent import binding, and
rollback behavior require the CI qualification result.

The focused tests cover deterministic entry ordering and signatures, validity
boundaries, wrong publisher material, private-key non-disclosure, snapshot
publication/replay/revocation, import snapshot binding, excluded signer
rejection, historical metadata compatibility, and route permissions.

## Measured cost and limits

- Snapshot publication is one bounded Postgres transaction plus one indexed
  workspace/deployment lookup and one Ed25519 verification over a fixed-size
  hash subject.
- A snapshot is capped at 64 signer entries and 128 KiB of signed JSON. Only
  public keys, hashes, timestamps, identifiers, and bounded audit metadata are
  retained.
- A new import adds one `BIGINT` revision and one 64-character hash; it does
  not duplicate the signed evidence body.
- Current-load uses the highest active, unexpired revision and rechecks the
  publisher catalog on every load/import boundary. The process cache contains
  only revision/hash metadata.

No live latency, WAL, storage-growth, or concurrent transaction measurement is
claimed in this local run because PostgreSQL was unavailable. CI and a
deployment-owned topology qualification must report those values.

## Remaining limitations

This slice does not provide HSM/KMS custody, workload identity, mTLS, a trust
distribution transport, key-ceremony evidence, rollback/lag policy, hosted
release signature verification, backup/restore proof, or live external-effect
reconciliation. Snapshot publication is an authority record, not proof that a
deployment operator selected truthful signer membership. Production operators
must qualify those boundaries and retain their evidence separately.

## Next task prompt

### Task 81 — Build Fornix’s deployment release, recovery, and live effect evidence chain

Before coding:

1. Read the chats directory end to end.
2. Read `AGENTS.md`, `docs/00-fornix-foundation.md`,
   `docs/14-production-readiness-qualification.md`,
   `docs/111-universal-production-roadmap-status.md`,
   `docs/159-deployment-credential-retention-load-qualification-foundation.md`,
   `docs/163-live-adapter-postgres-topology-qualification-foundation.md`,
   `docs/165-live-provider-recovery-qualification-foundation.md`,
   `docs/181-signed-qualification-import-foundation.md`,
   `docs/183-qualification-trust-catalog-foundation.md`,
   `docs/185-qualification-trust-distribution-foundation.md`, and this
   completion note.
3. Study the existing release verification scripts, migration runner,
   qualification runner, trust catalog/snapshot stores, operation/effect
   authority links, credential lease boundary, controlled egress client,
   backup/restore probe, topology probe, and live-provider qualification
   seams. Reuse patterns; do not copy Kronaxis BSL-licensed source.
4. Write a feature note before implementation covering release identity,
   artifact/signature verification, migration forward/rollback evidence,
   backup/PITR/restore semantics, live provider/effect reconciliation,
   snapshot lag and rollback, workspace isolation, redaction, cost/storage,
   deployment ownership, and acceptance tests.

Implement the smallest production-quality vertical slice:

- Add typed deployment release, migration evidence, recovery drill,
  provider/effect observation, and qualification-gate contracts.
- Add durable, workspace-scoped release/evidence references without storing
  credentials, private keys, prompts, or unrestricted provider payloads.
- Bind release identity and loaded qualification snapshot revision/hash to
  readiness and operation/effect qualification records.
- Add deterministic forward-migration, rollback-policy, backup/restore,
  topology, provider, and external-effect evidence ingestion with dry-run,
  duplicate, stale-target, and cross-workspace rejection.
- Keep remote provider/tool/effect execution outside replay; record unknown
  outcomes as recovery-required and never claim exactly-once external work.
- Add bounded retention and disclosure, CI/Make/smoke coverage, API/CLI
  inspection, architecture documentation, measured latency/SQL/storage
  impact, and explicit deployment-owned limitations.

Acceptance criteria:

- A release cannot report production-ready without a current authorized trust
  snapshot and bounded deployment evidence for every configured workspace.
- Duplicate evidence is idempotent; conflicting target/release identities fail
  closed; historical evidence remains auditable.
- Restore/replay hashes are stable and no replay performs a remote effect.
- Provider/effect outcomes distinguish measured, estimated, unknown, and
  reconciled states.
- Credentials and private signing material never enter durable output.
- Existing tests, race checks, migration checks, CI, smokes, and docs checks
  remain green.
