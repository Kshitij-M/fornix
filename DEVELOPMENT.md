# Fornix development environment

The local environment is a Go/Postgres service with optional pgvector and
Ollama support. Docker provides the pinned toolchain when Go is not installed
on the host.

## First run

```sh
cp .env.example .env
make dev-up
make build
make test
make dev-run
curl http://localhost:8201/readyz
```

The application applies numbered, checksum-validated migrations at startup.
Postgres uses host port `55433` by default.

## Commands

```sh
make fmt
make test
make vet
make build
make python-install
make python-check
make smoke
make smoke-events
make smoke-projection
make smoke-leases
make dev-up
make dev-run
make dev-up-watcher
make dev-logs
make dev-down
```

The projection runtime is an internal Postgres-backed pull API. It does not
start a background worker; callers explicitly run bounded batches or a
rebuild. The v0.12 smoke exercises replay, duplicate delivery, rollback,
concurrency, and workspace isolation. The v0.13 smoke adds durable
workspace-scoped consumer leases, fencing, expiry/takeover, stale-owner
rejection, and lease transaction rollback:

```sh
make smoke-projection
make smoke-leases
```

To run the database integration tests directly:

```sh
docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN='postgres://fornix:fornix-dev-only@host.docker.internal:55433/fornix?sslmode=disable' \
  -v "$PWD:/workspace" -w /workspace golang:1.25.13 \
  go test ./internal/store ./internal/projection -count=1 -v
```

## Repository rules

Never commit `.env`, database volumes, model files, raw transcripts, or
generated binaries. Use synthetic fixtures for tests. Read `AGENTS.md` and
the architecture notes before implementing new functionality.
