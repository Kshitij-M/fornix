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
| HTTP service smoke | `make smoke-universal-operation`, `make smoke-reference-connectors` | Running Fornix HTTP server and Postgres | Reference HTTP/SQL adapters only when configured by the smoke | Yes, scoped test workspace |
| Managed Docker runtime | `make smoke-local-runtime` | Docker Desktop/Engine and Compose v2 | Fake provider by default | Yes, local runtime volume |
| Full repository smoke | `make smoke` | Running service, Postgres, Python helpers, Docker for local runtime | Fake-first; optional adapters are explicit | Yes, disposable smoke workspaces |
| Optional OpenAI | `make smoke-reference-openai` | `FORNIX_OPENAI_API_KEY` in the environment only | OpenAI call; bounded by smoke config | Model-call metadata only; never store the key |
| Package/release | `make package-check`, `make release-check`, `make smoke-package` | GoReleaser/package tools | No | Temporary archive/install directories only |

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

## Current production gates still open

Fornix is not production-ready for unattended, high-impact operations. The
remaining gates include database-enforced tenant defense in depth, external
secret management, signed capability/policy catalogs, generic background
dispatch and verification workers, backup/restore drills, HA/failover,
quota/backpressure/fairness qualification, adversarial confused-deputy and
egress testing, load/soak/failure-injection evidence, and operational support
runbooks.
