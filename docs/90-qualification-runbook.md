# Qualification runbook

Status: current alpha qualification reference.

Qualification is evidence, not a blanket production certification. Run the
smallest class that matches the change and record the command, commit, target
database version, duration, and result.

| Class | Commands | Prerequisites | External effects | Mutates durable state |
| --- | --- | --- | --- | --- |
| Offline unit/contract | `make test`, `make vet`, `make fmt-check` | Go toolchain | No | No external system; tests may use in-memory fixtures |
| Postgres integration | `FORNIX_TEST_PG_DSN=... make check` | Disposable PostgreSQL/pgvector; migrations applied by tests | No remote provider | Yes, in the test database; use a disposable workspace/database |
| Universal effect slice | `PROJECTION_PG_DSN=... make smoke-universal-effects` | Disposable Postgres | No provider call | Yes, test operation/effect rows |
| PostgreSQL workspace isolation | `FORNIX_RLS_TEST_DSN=... make qualification-workspace-isolation` | Disposable database cloned from a migrated authority; dedicated non-owner `NOBYPASSRLS` role | No provider call | Rolled-back qualification transaction only |
| Generic operation capacity | `FORNIX_CAPACITY_PG_DSN=... make qualification-capacity` | Dedicated disposable Postgres database | No provider call | Own workspace rows plus retained append-only events |
| Generic operation worker | `FORNIX_TEST_PG_DSN=... make test-operation-worker` | Dedicated disposable Postgres database | No provider call; adapter handler is test-owned | Scoped operation rows and append-only events |
| HTTP service smoke | `make smoke-universal-operation`, `make smoke-reference-connectors` | Running Fornix HTTP server and Postgres | Reference HTTP/SQL adapters only when configured by the smoke | Yes, scoped test workspace |
| Managed Docker runtime | `make smoke-local-runtime` | Docker Desktop/Engine and Compose v2 | Fake provider by default | Yes, local runtime volume |
| Full repository smoke | `make smoke` | Running service, Postgres, Python helpers, Docker for local runtime | Fake-first; optional adapters are explicit | Yes, disposable smoke workspaces |
| Optional OpenAI | `make smoke-reference-openai` | `FORNIX_OPENAI_API_KEY` in the environment only | OpenAI call; bounded by smoke config | Model-call metadata only; never store the key |
| Package/release | `make package-check`, `make release-check`, `make smoke-package` | GoReleaser/package tools | No | Temporary archive/install directories only |
| Backup/restore drill | `make qualification-backup-restore` with separate DSNs and explicit confirmation | PostgreSQL client tools and a clean restore database | Destructive only on the explicitly named restore target | Backup file plus restored database |

## Environment rules

Do not put provider keys in `.env`, Compose files, request JSON, logs, test
fixtures, or repository files. The fake provider is the default. OpenAI is
opt-in and must be supplied only through the process environment. Never paste
a key into a terminal transcript or chat.

For Postgres tests, use a disposable database and a unique workspace. The
tests apply embedded numbered migrations and preserve append-only history;
cleanup is not a substitute for backup or retention policy.

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
deployment RPO, and measured deployment RTO. Never retain DSNs or secret
values in the qualification record.

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
compatibility, so it cannot prove row-level-security enforcement. Before a
deployment claims database-enforced tenant isolation, create a disposable
database from the migrated schema, run the service role as a non-owner with
`NOBYPASSRLS`, grant only the runtime privileges, and run:

```sh
FORNIX_RLS_TEST_DSN='postgres://APP_ROLE:APP_PASSWORD@HOST:PORT/RLS_DATABASE?sslmode=disable' \
  make qualification-workspace-isolation
```

The smoke verifies that unset context exposes no protected rows, same-workspace
writes succeed, foreign reads are invisible, and foreign writes fail. It uses
an explicit rollback and creates no durable qualification fixture. The
production role-transfer and privilege-grant procedure remains deployment
specific and must be reviewed by the database owner.

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

## Current production gates still open

Fornix is not production-ready for unattended, high-impact operations. The
remaining gates include production role separation and complete coverage of
all legacy tables for database-enforced tenant defense in depth, external
secret management, signed capability/policy catalogs, generic background
dispatch and verification workers, backup/restore drills, HA/failover,
quota/backpressure/fairness qualification, adversarial confused-deputy and
egress testing, load/soak/failure-injection evidence, and operational support
runbooks.
