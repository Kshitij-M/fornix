# Qualification runbook

Status: current alpha qualification reference.

## Task 54 — authority startup and adapter composition

Task 54 adds the first mandatory universal composition guard. The server loads
the current signed trust policy and schema catalog for each durable workspace,
verifies active signers and validity windows, and exposes the readiness state
through `/readyz` and `/v1/health`. In development, both signed catalogs may
be absent and the explicit unsigned built-in compatibility path remains
available. A partial, malformed, expired, revoked, or mismatched signed
generation fails closed.

The connector registry now provides a side-effect-free
`ValidateEffectAuthorityConformance` check and the server enables
`RequireEffectAuthority(true)`, so effectful capabilities must use the typed
authority-aware seam. The operation store revalidates the live operation and
task fences immediately before an authority-bound dispatch. This is not yet a
shared durable effect dispatcher: incident workflow, repository-change,
side-effectful tool, and provider-specific migration plus process-level crash
and restart tests remain open.

Use a named disposable database for qualification:

```sh
FORNIX_REQUIRE_SIGNED_AUTHORITY=true \
FORNIX_TEST_PG_DSN='postgres://USER:PASSWORD@HOST:PORT/DISPOSABLE_DATABASE?sslmode=disable' \
  go test ./internal/connector ./internal/server ./internal/store -count=1
```

This slice never claims exactly-once remote execution. Provider idempotency,
verification, compensation, and unknown-outcome recovery remain explicit
at-least-once boundaries.

Qualification is evidence, not a blanket production certification. Run the
smallest class that matches the change and record the command, commit, target
database version, duration, and result.

| Class | Commands | Prerequisites | External effects | Mutates durable state |
| --- | --- | --- | --- | --- |
| Offline unit/contract | `make test`, `make vet`, `make fmt-check` | Go toolchain | No | No external system; tests may use in-memory fixtures |
| Postgres integration | `FORNIX_TEST_PG_DSN=... make check` | Disposable PostgreSQL/pgvector; migrations applied by tests | No remote provider | Yes, in the test database; use a disposable workspace/database |
| Universal effect slice | `PROJECTION_PG_DSN=... make smoke-universal-effects` | Disposable Postgres | No provider call | Yes, test operation/effect rows |
| PostgreSQL workspace isolation | `FORNIX_RLS_TEST_DSN=... make qualification-workspace-isolation` | Disposable database cloned from a migrated authority; dedicated non-owner `NOBYPASSRLS` role | No provider call | Rolled-back qualification transaction only |
| Role-separated PostgreSQL deployment | `FORNIX_RLS_ADMIN_DSN=... FORNIX_RLS_TEST_DSN=... FORNIX_RLS_APP_ROLE=... FORNIX_RLS_MIGRATION_ROLE=... make qualification-role-separated-postgres` | Pre-provisioned disposable database, runtime role, migration role, and administrator DSN | No provider call | Synthetic authentication fixture is deleted by the administrator; RLS fixture is rolled back |
| Generic operation capacity | `FORNIX_CAPACITY_PG_DSN=... make qualification-capacity` | Dedicated disposable Postgres database | No provider call | Own workspace rows plus retained append-only events |
| Federation capacity/retention | `FORNIX_FEDERATION_CAPACITY_PG_DSN=... make qualification-federation-capacity` | Dedicated disposable PostgreSQL/pgvector database | No provider call; no remote peer | Scoped peer, lease, poll, event, and retention tombstone rows; cleanup is database-scoped |
| Live credential authority | `FORNIX_LIVE_AUTHORITY_URL=... make qualification-credential-authority` | Deployment-owned authority endpoint, token environment variable, and explicit workspace/reference metadata | One bounded authority read; no external mutation | No durable probe payload; only operator output |
| PostgreSQL topology and pool hygiene | `FORNIX_TOPOLOGY_PG_DSN=... make qualification-postgres-topology` | Explicit deployment-owned writable PostgreSQL database; optional archive requirement | No provider call; no failover or PITR mutation | Transaction-local qualification reads only; no authoritative rows are created |
| Redacted recovery evidence | `FORNIX_QUALIFICATION_REPORT_FILE=... make qualification-recovery-evidence` | Deployment-produced bounded JSON report with hashes and explicit drill outcomes | No provider or database call | Read-only local validation |
| Deployment release/evidence gate | `FORNIX_TEST_PG_DSN=... make qualification-deployment-evidence` | Disposable PostgreSQL; accepted signed imports and trust snapshot fixtures | No deployment, provider, backup, or external effect | Release/evidence references and append-only gate history in the disposable database |
| Generic operation worker | `FORNIX_TEST_PG_DSN=... make test-operation-worker` | Dedicated disposable Postgres database | No provider call; adapter handler is test-owned | Scoped operation rows and append-only events |
| Server-composed operation worker | `FORNIX_TEST_PG_DSN=... go test ./internal/server -run '^TestServerOperationWorker' -count=1 -v` | Dedicated disposable Postgres database; `FORNIX_WORKER_ENABLED=true` for runtime composition | Read/observation adapters only; effectful plans are excluded at claim time | Scoped operation rows, transitions, results, and events |
| Operation fairness/resource coordination | `make test-operation-supervisor` plus the Postgres queue tests | Unit tests plus dedicated disposable Postgres database | No provider call | Resource lease/current-history rows and bounded operation leases |
| Managed credential boundary | `make smoke-universal-credentials` | Go tests; use a disposable Postgres database for lease tests; HTTP manager tests use a local controlled test server | No real secret manager or provider call | Scoped lease/source-version rows in the disposable database |
| Workspace federation authority | `FORNIX_TEST_PG_DSN=... make smoke-universal-federation` | Disposable Postgres and local controlled HTTP server | No production peer or raw credential; injected fake lease only | Scoped peer, lease, poll, reconciliation, quarantine, coordination, and event rows |
| Signed schema catalog | `make smoke-universal-schema` | Go tests; use a disposable Postgres database for catalog publication tests | No provider call | Scoped signer and append-only schema-catalog rows in the disposable database |
| Effectful adapter authority linkage | `FORNIX_TEST_PG_DSN=... make smoke-universal-effect-authority` | Disposable Postgres; registered fake capability and local credential resolver | No provider or external effect call | Admission, exact lease, effect, and authority-link rows in the disposable database |
| HTTP service smoke | `make smoke-universal-operation`, `make smoke-reference-connectors` | Running Fornix HTTP server and Postgres | Reference HTTP/SQL adapters only when configured by the smoke | Yes, scoped test workspace |
| Managed Docker runtime | `make smoke-local-runtime` | Docker Desktop/Engine and Compose v2 | Fake provider by default | Yes, local runtime volume |
| Full repository smoke | `make smoke` | Running service, Postgres, Python helpers, Docker for local runtime | Fake-first; optional adapters are explicit | Yes, disposable smoke workspaces |
| Optional OpenAI | `make smoke-reference-openai` | `FORNIX_OPENAI_API_KEY` in the environment only | OpenAI call; bounded by smoke config | Model-call metadata only; never store the key |
| Package/release | `make package-check`, `make release-check`, `make smoke-package` | GoReleaser/package tools | No | Temporary archive/install directories only |
| Backup/restore drill | `make qualification-backup-restore` with separate DSNs and explicit confirmation | PostgreSQL client tools and a clean restore database | Destructive only on the explicitly named restore target | Backup file plus restored database |
| Built-in adapter matrix | `make qualification-adapter-matrix` | Go toolchain; fixture/recorded adapters only | None | No durable mutation |
| Multi-domain reference workflow contracts | `make qualification-multidomain-reference` | Go toolchain only | No model, provider, connector, network, filesystem, or database call | No durable mutation; hash-only in-memory traces |
| Workflow retry runtime and queue deadline | `make qualification-workflow-retry-deadline`; then `FORNIX_TEST_PG_DSN=... make qualification-workflow-retry-deadline-postgres` | Offline Go checks; disposable PostgreSQL/pgvector for the second target | No provider or external effect | Workflow/operation fixtures and append-only retry events in the disposable database |
| Local support-bundle privacy | `make qualification-support-bundle` | Go toolchain only | No network or database call | Temporary test files only |
| Agent-loop trust boundaries | `make qualification-agent-trust-boundary`; `FORNIX_TEST_PG_DSN=... make qualification-agent-trust-boundary-postgres` | Offline Go checks; disposable PostgreSQL for catalog reload/mutation checks | No model provider or external tool | Agent-run fixtures/events in the disposable database |
| Registered tool-schema authority | `make qualification-agent-tool-schema-authority`; `FORNIX_TEST_PG_DSN=... make qualification-agent-trust-boundary-postgres` | Go toolchain; use the PostgreSQL target with an explicitly disposable DSN to verify persisted fingerprints | No model provider or external tool | Bounded per-run catalog metadata in the disposable database |
| Agent tool resume and path containment | Included in `make qualification-agent-tool-schema-authority` and CI | Go toolchain; the focused suite uses temporary directories and fake providers | No provider or external tool | Temporary test files only |

## Environment rules

Do not put provider keys in `.env`, Compose files, request JSON, logs, test
fixtures, or repository files. The fake provider is the default. OpenAI is
opt-in and must be supplied only through the process environment. Never paste
a key into a terminal transcript or chat.

For Postgres tests, use a disposable database and a unique workspace. The
tests apply embedded numbered migrations and preserve append-only history;
cleanup is not a substitute for backup or retention policy.

## PostgreSQL topology qualification

Task 70 adds a bounded, opt-in probe for deployment-owned PostgreSQL. It
requires `FORNIX_TOPOLOGY_PG_DSN` and applies the embedded migrations to the
explicit database before checking that the endpoint is writable, not in
recovery, and has the expected WAL/archive settings. It then verifies
transaction-local workspace context on commit and rollback, concurrent pool
use, context clearing on connection return, and bounded acquire latency. The
probe accepts `FORNIX_TOPOLOGY_OPERATIONS` (default 32, maximum 512),
`FORNIX_TOPOLOGY_WORKERS` (default 4, maximum 16),
`FORNIX_TOPOLOGY_POOL_MAX` (default workers, maximum 32), and the optional
`FORNIX_TOPOLOGY_REQUIRE_ARCHIVE=true` gate. It prints only bounded topology
facts and latency samples; it never prints the DSN or credentials.

This qualification does not execute a primary/standby failover, WAL replay,
PITR restore, partition maintenance job, or certificate rotation. Those are
deployment-owned drills and must be recorded separately with measured RPO,
RTO, recovery identity, and the exact topology under test.

## Validate deployment recovery evidence

After a deployment owner runs an isolated failover, PITR, backup/restore,
partition, or identity-rotation drill, the owner may assemble the typed
`QualificationReport` using the Fornix contracts and validate the redacted JSON
without contacting the live system:

```sh
FORNIX_QUALIFICATION_REPORT_FILE=/protected/evidence/fornix-qualification.json \
  make qualification-recovery-evidence
```

The native `fornix qualification validate` command rejects unknown fields,
oversized input, invalid hashes, duplicate cases, missing passed-drill
evidence, and unmeasured recovery values. It prints only the run identity,
outcome, counts, target hash, and report hash. Keep the mapping from target
hash to topology, DSN, operator, and raw provider evidence in the deployment's
protected evidence system; never put that material in the public report.

## Run the built-in adapter qualification matrix

Task 72 adds a deterministic, side-effect-free matrix over the built-in
read-only adapter seams. It uses a local HTTP fixture, a SQL fixture database,
a deterministic repository inspector, and the fake incident read capability:

```sh
make qualification-adapter-matrix
```

Entries are sorted by name and aggregated into the common redacted
`QualificationReport`; duplicate names, cross-workspace requests, malformed
requests, and effectful capabilities fail closed. The matrix does not provide
live provider, credential, database-topology, or external-effect evidence.
Use deployment-owned configuration and the existing offline report validator
for those qualifications. The command performs no durable mutation and does
not require Docker, Postgres, model credentials, or an LLM.

## Portable qualification bundle

The native CLI can generate one bounded, redacted offline evidence bundle.
It runs only the built-in contract, determinism, and redaction checks; it does
not contact Postgres, providers, tools, brokers, or deployment systems.

    make qualification-offline

For an operator-retained bundle, provide an explicit output path:

    FORNIX_QUALIFICATION_BUNDLE_FILE=/protected/evidence/fornix-portable.json \
      make qualification-offline

The bundle contains a stable report hash and manifest hash. It records
environment variable names only, never their values. Validate or inspect it
without external access:

    fornix qualification validate --file /protected/evidence/fornix-portable.json
    fornix qualification hash --file /protected/evidence/fornix-portable.json

Deployment-produced bundles can be merged only when their workspace and target
hash match. The merge is deterministic, bounded, and fail-closed on conflicting
case or check identities:

    fornix qualification merge --file merged.json --inputs local.json,deploy.json

This bundle is an evidence envelope, not a deployment authority. It cannot
prove HA/PITR, secret-manager behavior, live provider idempotency, sandbox
strength, retention scale, or load/soak behavior unless those properties are
qualified in the deployment and imported as redacted evidence.

## Signed deployment-owned qualification evidence

Task 78 adds a bounded offline signature envelope for deployment-owned
qualification facts. Generate a portable bundle, sign it with a private key
held outside the repository, and validate it against the deployment's trusted
key reference and target hash:

    fornix qualification sign --file portable.json \
      --output signed.json --key-file /protected/qualification-key.hex \
      --key-id deployment-key-v1
    fornix qualification validate-signed --file signed.json \
      --workspace WORKSPACE_ID --target-hash TARGET_HASH \
      --key-id deployment-key-v1 --public-key-file /protected/key.pub
    fornix qualification import --file signed.json \
      --workspace WORKSPACE_ID --target-hash TARGET_HASH \
      --key-id deployment-key-v1 --public-key-file /protected/key.pub

`import` is deliberately an offline validation boundary in this repository
slice; it does not write an implicit database row. A deployment-owned system
must authorize the key ID, apply rotation/revocation policy, and retain the
verified bundle in its own evidence system. The embedded public key proves
only cryptographic integrity and is not authorization. Private keys never
enter Fornix contracts, logs, events, reports, or artifacts.

Task 79 adds the durable authorized-import path. Register a deployment-owned
public key through the authenticated operator API or CLI, then import through
the workspace/deployment trust catalog:

    fornix qualification signer-register \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --key-id deployment-key-v1 --public-key HEX \
      --valid-from 2026-09-27T00:00:00Z --valid-until 2026-10-27T00:00:00Z
    fornix qualification import-authorized \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --file signed.json --source deployment-run
    fornix qualification imports-list \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID

`qualification import-authorized` is the only path in this slice that creates
a durable import row. It verifies the signed subject against the current
catalog signer, retains the exact bounded submitted bytes and source hash,
and returns a stable duplicate result on replay. Use `--dry-run true` to run
the same trust, scope, size, and conflict checks without writing a row.
Signer rotation is explicit with `qualification signer-rotate --supersedes`;
revoked and superseded keys remain auditable but cannot authorize new imports.
The API and CLI never accept private keys, secret-manager values, or bearer
tokens as qualification evidence.

## Distribute the qualification trust snapshot

Task 80 adds a signed, revisioned trust snapshot. The Task 79 signer catalog
authorizes the snapshot publisher; the snapshot carries the bounded signer set
used for new authorized imports. Keep the snapshot signing key outside the
repository and never send its private bytes to Fornix.

Create an unsigned snapshot with public signer entries, then sign it locally:

    fornix qualification snapshot-sign --file snapshot.json \
      --output snapshot-signed.json --key-file /protected/key.hex \
      --key-id deployment-key-v1

Publish it through the authenticated workspace-scoped API:

    fornix qualification snapshot-publish \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --file snapshot-signed.json --source deployment-trust
    fornix qualification snapshot-list \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID
    fornix qualification snapshot-get \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --id SNAPSHOT_ID

Publishing a lower revision, changing an existing revision, using an expired
publisher, or publishing a snapshot signed by a revoked catalog key fails
closed. Snapshot revocation is explicit and preserves the signed bytes:

    fornix qualification snapshot-revoke \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --id SNAPSHOT_ID

For production startup conformance, set
`FORNIX_REQUIRE_QUALIFICATION_TRUST=true` and
`FORNIX_QUALIFICATION_DEPLOYMENT_ID=DEPLOYMENT_ID`. The service remains
unready when any workspace lacks a current, non-revoked snapshot. The
development-compatible startup default does not load snapshots, but new
durable `import-authorized` submissions still require a current snapshot so
that accepted evidence is always bound to an explicit trust revision.

## Register a release and evaluate its evidence gate

Task 81 provides a durable release/evidence index so an operator can answer
which exact release is backed by which accepted qualification imports. Register
the immutable release after publishing the current trust snapshot:

    fornix qualification release-register \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --release-hash RELEASE_HASH --target-hash TARGET_HASH \
      --version RELEASE_VERSION --commit-hash COMMIT_HASH

Link accepted imports by their durable import IDs. The link stores hashes and
provenance only; the raw signed bytes remain owned by the qualification import
record:

    fornix qualification evidence-link \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --release-id RELEASE_ID --kind provider --import-id IMPORT_ID

Inspect the release and its bounded links, then evaluate the read-only gate:

    fornix qualification release-get \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --id RELEASE_ID
    fornix qualification evidence-list \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --release-id RELEASE_ID
    fornix qualification release-gate \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --release-id RELEASE_ID \
      --required-kinds provider

Without `--required-kinds`, the gate requires migration, backup/restore,
topology, provider, and external-effect links. A gate is ready only when all
required evidence is passed, unresolved recovery is absent, and every link is
bound to the release's current trust snapshot. Snapshot rotation does not
silently rewrite history: existing releases remain bound to their original
snapshot and newly linked evidence must match it. This surface does not run a
deployment, contact a provider, perform a backup/restore, or claim HA/PITR.

Capture a durable, hash-only operator observation when you need to preserve
exactly which active evidence made the gate ready or blocked:

    fornix qualification readiness-capture \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --release-id RELEASE_ID
    fornix qualification readiness-list \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --release-id RELEASE_ID
    fornix qualification readiness-get \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --id SNAPSHOT_ID
    fornix qualification incident-annotate \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --release-id RELEASE_ID \
      --snapshot-id SNAPSHOT_ID --code provider-timeout \
      --disposition acknowledged --reference-hash EVIDENCE_HASH
    fornix qualification incident-list \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --release-id RELEASE_ID \
      --snapshot-id SNAPSHOT_ID

Snapshots are advisory and append-only. These commands do not mutate signed
qualification imports, change release admission, call a provider, or accept
raw deployment logs. `--dry-run true` returns a bounded projection without
creating a snapshot or annotation.

Set an explicit review freshness policy and compare two immutable snapshots:

    fornix qualification freshness-policy-set \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --max-age-seconds 86400 --require-ready true
    fornix qualification freshness-policy-get \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID
    fornix qualification readiness-review \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --release-id RELEASE_ID --left-snapshot-id LEFT_ID \
      --right-snapshot-id RIGHT_ID --as-of 2026-09-27T12:00:00Z

The review is diagnostic only. Future-dated or over-age snapshots are marked
stale, but the review cannot make a release admissible and does not alter the
underlying gate.

## Plan qualification retention and inspect recovery integrity

Task 91 adds a separate retention overlay for long-lived qualification
history. It is deliberately conservative: Fornix never purges readiness
snapshots, incident annotations, or freshness policies through this API. The
policy and plan provide hash-only facts that an external archive controller can
use while preserving a verifiable provenance manifest.

Publish a bounded, immutable policy (the CLI accepts either seconds or days):

    fornix qualification retention-policy-set \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --snapshot-retention-days 90 --incident-retention-days 365 \
      --policy-retention-days 365 --keep-latest-snapshots 10 \
      --keep-latest-incidents 10 --protect-incidents true

If migration 075 was applied after existing readiness rows were created, fill
only missing hash-only metadata in bounded, resumable pages:

    fornix qualification retention-sync \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --batch 100
    fornix qualification retention-sync \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --cursor incident_annotation:INCIDENT_ID --dry-run true

Generate a deterministic external-archival plan at a fixed time and inspect
the source/metadata integrity report:

    fornix qualification retention-plan \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --as-of 2026-09-27T12:00:00Z --batch 100
    fornix qualification retention-recovery \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --as-of 2026-09-27T12:00:00Z

Plans classify rows as eligible for external archival, protected because they
are recent, protected incidents, not due, or missing metadata. Recovery reports
detect missing/dangling metadata, hash mismatches, dangling annotation
references, and event-reference mismatches. They contain only IDs, hashes,
counts, and closed issue codes: no prompts, deployment payloads, logs, or
credentials. Plan and recovery requests require `qualification:read`; policy
publication and metadata sync require `qualification:admin`.

## Refresh deployment qualification evidence

Task 92 adds an operator-owned refresh transaction for an immutable release.
The deployment scheduler or verifier first produces a new signed qualification
bundle and imports it through the existing trust boundary. Fornix then links
that accepted import to the release without contacting the deployment system:

```sh
cat > refresh-items.json <<'JSON'
{
  "items": [
    {"kind": "provider", "import_id": "qualification-import-provider-2", "supersedes_link_id": "deployment-evidence-provider-1"}
  ]
}
JSON

fornix qualification refresh-plan \
  --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
  --release-id RELEASE_ID --file refresh-items.json \
  --as-of 2026-09-27T12:00:00Z

fornix qualification refresh \
  --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
  --release-id RELEASE_ID --file refresh-items.json \
  --as-of 2026-09-27T12:00:00Z \
  --idempotency qualification-refresh-provider-2

fornix qualification refresh-list \
  --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID --release-id RELEASE_ID
fornix qualification refresh-get \
  --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
  --release-id RELEASE_ID --id REFRESH_ID
```

The item file is bounded and reference-only: it cannot contain signed bytes,
credentials, URLs, prompts, or arbitrary deployment output. Refresh uses one
workspace/deployment transaction and a fixed `as_of`; a crash before commit
leaves the predecessor active, while a retry after commit returns the original
report. Omitted kinds remain unchanged and revocation remains an explicit
operation. An expired, revoked, superseded, cross-deployment, target-mismatch,
or stale-snapshot import fails closed. This workflow does not prove HA, PITR,
failover, DNS, mTLS, workload identity, provider idempotency, or remote
exactly-once execution; those facts must continue to come from deployment-owned
observations.

## Bind a verified artifact to release admission

Task 82 adds a deployment-owned, hash-only verification record. After a
Task 81 gate is ready, the deployment verifier can bind the exact release
artifact and attestation identity to that gate:

    fornix qualification release-verify \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --release-id RELEASE_ID --release-hash RELEASE_HASH \
      --target-hash TARGET_HASH --artifact-kind image \
      --artifact-hash ARTIFACT_HASH --attestation-hash ATTESTATION_HASH \
      --gate-hash GATE_HASH --expires-at 2026-09-28T00:00:00Z \
      --source deployment-verifier

Inspect or revoke the bounded verification, and evaluate the read-only
admission decision:

    fornix qualification verification-get \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --release-id RELEASE_ID --artifact-kind image
    fornix qualification release-admission \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --release-id RELEASE_ID --artifact-kind image \
      --artifact-hash ARTIFACT_HASH
    fornix qualification verification-revoke \
      --workspace WORKSPACE_ID --deployment DEPLOYMENT_ID \
      --release-id RELEASE_ID --artifact-kind image

The verification row retains hashes and bounded provenance only. It does not
prove that an image was pulled or that a deployment target ran it. Admission
fails closed for a missing/failed/revoked/expired verification, a changed
gate or trust snapshot, or an artifact-hash mismatch. To require this at
startup, set `FORNIX_REQUIRE_QUALIFICATION_RELEASE=true` and
`FORNIX_QUALIFICATION_RELEASE_ID=RELEASE_ID`; production enables the policy
automatically and requires the deployment and release IDs explicitly.

## Consume release admission from a generic effect

Task 83 lets any domain-neutral operation bind its external-effect reservation
to the read-only decision above. Copy the decision's release, artifact, gate,
trust-snapshot, and `decision_hash` fields into the request's
`deployment_admission` reference. The operation request is then identity-bound
to those hashes; the operation store re-evaluates them transactionally before
inserting `operation_effects`.

Use `FORNIX_REQUIRE_RELEASE_ADMISSION_FOR_EFFECTS=true` (automatic in
production) to reject new generic effect reservations that omit the reference.
The check never stores raw manifests, signatures, image layers, credentials,
or provider payloads, and it never executes a deployment. Duplicate
reservations remain idempotent; revocation, expiry, gate drift, snapshot drift,
artifact mismatch, and transaction rollback fail closed without rewriting
historical operation or qualification records.

Signed merges verify every source before deterministic composition. Duplicate
signed inputs are idempotent; conflicting evidence for the same case/check
identity fails closed. The aggregate must be signed explicitly by the
deployment-owned key:

    fornix qualification merge-signed --file aggregate.json \
      --inputs signed-a.json,signed-b.json \
      --key-file /protected/qualification-key.hex \
      --key-id deployment-key-v1

An offline signature is not hosted production proof. It cannot establish that
the signer is trusted, that the deployment facts are truthful, or that
HA/PITR, failover, credential rotation, provider idempotency, sandbox, or
load/soak behavior occurred. Those claims require deployment-owned drills and
separate evidence.

## Disposable PostgreSQL effect-authority probe

Task 77 provides an opt-in bridge from the existing PostgreSQL dispatcher and
Work Receipt authorities to the hash-only qualification observation. It uses a
deterministic in-process fake invoker, not a model, provider, tool, or broker.
Never point it at a development or production database. The command requires
both an explicit DSN and an operator confirmation that the database is
disposable:

    FORNIX_TEST_PG_DSN='postgres://...' \
    FORNIX_DISPOSABLE_DATABASE_CONFIRM=1 \
      make qualification-effect-authority-probe

The probe has a hard deadline, no retries, creates only two bounded operations,
and emits hashes and state facts without printing the DSN or provider payloads.
Because Fornix's authority and receipt histories are append-only, the probe
does not disable triggers or delete those rows. Discard the confirmed
disposable database after the run. The default CI and offline qualification
commands never invoke this probe.

## Evidence to retain

For a qualification run, record:

- Git commit and Go/Python/Docker/Postgres versions;
- command and non-secret environment names (not values);
- migration version and database image digest/tag;
- pass/fail, duration, and bounded latency samples;
- test/smoke output after redaction;
- replay, state, context, artifact, and report hashes where applicable;
- storage and row-count deltas for database-backed tests.

For the backup/restore drill, also retain the backup checksum, byte size,
backup duration, restore duration, source/restore fingerprint, measured
deployment RPO, measured deployment RTO, and the emitted replay identity hash.
Never retain DSNs or secret values in the qualification record.

## Recover an uncertain external effect

The recovery surface is a durable ownership boundary, not a provider
dispatcher. Discover bounded candidates and claim one with a separate effect
fence:

```sh
fornix operation effect-recovery --limit 32
fornix operation effect-lease --id OPERATION_ID --effect-id EFFECT_ID
fornix operation effect-state --id OPERATION_ID --effect-id EFFECT_ID \
  --state recovery_required --idempotency recovery-1 --effect-fence EFFECT_FENCE
fornix operation effect-release --id OPERATION_ID --effect-id EFFECT_ID \
  --effect-fence EFFECT_FENCE
```

Use the effect fence for verification or compensation after a parent operation
has become terminal. Never send raw provider payloads through this API. A
domain adapter remains responsible for its own bounded dispatch, provider
idempotency, verification, compensation, credentials, and egress policy.

## Database workspace-isolation qualification

The normal development database user is a superuser/table owner for
compatibility, so it cannot prove row-level-security enforcement. Workspace-
scoped HTTP compatibility routes now use the same transaction-local context
helper as the stores, and the role-separated command exercises memo retrieval
through the real middleware. Before a deployment claims database-enforced
tenant isolation, create a disposable
database from the migrated schema, run the service role as a non-owner with
`NOBYPASSRLS`, grant only the runtime privileges, and run:

```sh
FORNIX_RLS_TEST_DSN='postgres://APP_ROLE:APP_PASSWORD@HOST:PORT/RLS_DATABASE?sslmode=disable' \
  make qualification-workspace-isolation
```

The smoke verifies that unset context exposes no protected rows, same-workspace
writes succeed, foreign reads are invisible, and foreign writes fail. It uses
an explicit rollback and creates no durable qualification fixture. The
role-separated helper additionally transfers current table ownership to the
migration role, grants the runtime role only the service privileges, checks
that every workspace table has RLS, and exercises the scoped API-key lookup.
It also seeds two synthetic federation rows and runs
`TestRuntimeRoleWorkspaceRLSPoolReuseFailsClosed` as the non-owner application
role. That test uses one acquired pooled connection to check unscoped denial,
workspace A/B isolation, and transaction-local context clearing after both
commit and rollback. A shell `EXIT` cleanup removes those rows even if the Go
qualification fails.
Role creation, passwords, secret rotation, and the final deployment topology
remain deployment-specific and must be reviewed by the database owner.

## Generic operation capacity qualification

Run the bounded capacity harness only against a disposable database:

```sh
FORNIX_CAPACITY_PG_DSN='postgres://USER:PASSWORD@HOST:PORT/DISPOSABLE_DATABASE?sslmode=disable' \
FORNIX_CAPACITY_OPERATIONS=128 FORNIX_CAPACITY_WORKERS=4 \
  make qualification-capacity
```

The harness clamps operations to 2,048 and workers to 32. It reports create
and lease p50/p95/max latency, duplicate-hit and authoritative row counts,
Postgres transaction/buffer deltas, and relation-size growth. Set
`FORNIX_CAPACITY_MAX_P95_MS` to make the run fail when either measured p95
exceeds an explicitly chosen local budget. The reported counters are local
observations and must not be promoted to a service SLO without repeating the
run on the target Postgres topology, network, storage class, pool size, and
retention policy.

## Federation credential, retention, and capacity qualification

The certificate and authority tests are offline and use only public test
material plus in-memory secret bytes. The test authority supports explicit
rotation and revocation so an operator can verify source-version changes,
revocation failure, and bounded redacted observations without a real manager:

```sh
make smoke-universal-credentials
```

For a disposable database, run the bounded federation authority workload:

```sh
FORNIX_FEDERATION_CAPACITY_PG_DSN='postgres://USER:PASSWORD@HOST:PORT/DISPOSABLE_DATABASE?sslmode=disable' \
FORNIX_FEDERATION_CAPACITY_OPERATIONS=64 \
FORNIX_FEDERATION_CAPACITY_WORKERS=4 \
  make qualification-federation-capacity
```

The harness reports lease/poll p50, p95, and p99 latency, transaction and
buffer deltas, relation growth, waiting locks, and a bounded operation count.
The retention store expires only terminal operational poll attempts and legacy
quarantine rows whose explicit deadlines have passed. Dry-runs select and
report candidates without mutation; committed sweeps insert hash-only,
workspace-scoped tombstones before deleting the selected rows. Peer-command
history, control events, recovery-required attempts, and actively leased
attempts are protected. Tombstones are stored in a range-partitioned parent
with a default partition so deployments can add time partitions without
changing source identity.

### Fenced retention owner

Retention ownership is disabled by default. A deployment that explicitly sets
`FORNIX_FEDERATION_RETENTION_ENABLED=true` gets a bounded server loop with
these controls:

```text
FORNIX_FEDERATION_RETENTION_INTERVAL_SECONDS=3600
FORNIX_FEDERATION_RETENTION_BATCH_SIZE=100
FORNIX_FEDERATION_RETENTION_WORKSPACE_LIMIT=100
```

Each pass pages workspaces deterministically, acquires the Postgres consumer
lease `federation.retention`, and supplies the exact owner/fence to the
transactional retention store. A takeover makes the old owner fail closed;
the loop never reuses a lease or crosses a workspace. The owner is an
operational scheduler, not a replacement for database backup or partition
maintenance. Run the same store tests and role-separated qualification before
enabling it in a deployment.

### Durable qualification refresh handoff

Task 93 is an explicit deployment-owned scheduler contract. Register a bounded
schedule, claim it with `fornix qualification schedule-claim`, import the new
signed observations with the Task 79 trust boundary, run the Task 92 refresh
with the returned `schedule_authorization`, and complete the attempt with the
refresh ID/hash. A worker crash after refresh commit is recovered by replaying
the same refresh idempotency key; a stale owner cannot start a new refresh or
advance the schedule. Use `schedule-renew`, `schedule-release`, and the
fenced `schedule-pause`/`schedule-cancel` commands only with the current
owner/fence. `schedule-resume` is an authenticated administrative state
transition.

The schedule stores no deployment credentials, raw reports, URLs, logs, or
backup bytes. It verifies required recovery drills from the accepted signed
imports referenced by the refresh report. This is a durable handoff and audit
surface, not a cron service or deployment executor.

### Live deployment-authority qualification

The live authority check is opt-in and reads only process environment metadata:

```sh
FORNIX_LIVE_AUTHORITY_URL='https://authority.example/v1/resolve' \\
FORNIX_LIVE_AUTHORITY_WORKSPACE='workspace-a' \\
FORNIX_LIVE_AUTHORITY_PROVIDER='openai' \\
FORNIX_LIVE_AUTHORITY_REFERENCE='provider/openai' \\
FORNIX_LIVE_AUTHORITY_PURPOSE='model:openai' \\
FORNIX_LIVE_AUTHORITY_TOKEN_ENV='FORNIX_CREDENTIAL_MANAGER_TOKEN' \\
make qualification-credential-authority
```

The token value must already be injected into the named environment variable;
it is never an argument, file, log field, or report field. The qualification
validates workspace/provider/purpose/reference shape, bounded response and
timeout behavior, source version, expiry, redaction, and controlled egress.
It does not claim exactly-once remote reads or certify the authority's own
rotation, revocation, HA, or audit implementation. Those facts require a
deployment-owned runbook and retained evidence.

## Current production gates still open

## Repository-owned agent-run effect qualification

The model-call and tool-run ledgers now persist the agent-run owner and fence
for leased loop effects. Run the bounded local qualification with:

```bash
make test-agent-run-effects
```

Set `FORNIX_TEST_PG_DSN` to a disposable PostgreSQL database to execute the
stale-worker takeover cases. The command never creates Docker images or
volumes. It proves durable stale-mutation rejection and request propagation;
it does not prove exactly-once remote execution, live provider behavior, or
deployment recovery.

Fornix is not production-ready for unattended, high-impact operations. The
remaining gates include deployment-specific application-context and pool
hygiene evidence on top of the role-separated database qualification,
deployment-specific external secret management, signed capability/policy catalogs, generic background
dispatch and verification workers, backup/restore drills, HA/failover,
quota/backpressure/fairness qualification, adversarial confused-deputy and
egress testing, load/soak/failure-injection evidence, and operational support
runbooks.
