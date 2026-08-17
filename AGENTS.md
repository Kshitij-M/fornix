# Fornix development rules

Fornix is an efficiency-first AI harness. Read `docs/00-fornix-foundation.md`
before changing the system.

## Research gate

Before implementing a feature:

1. Read the relevant architecture notes and the complete project research in
   `/Users/kshitijmohan/Developer/omaveda/HARNESS/chats`.
2. Search the relevant repositories in
   `/Users/kshitijmohan/Developer/omaveda/HARNESS/reference_repos` with `rg`.
3. Write a feature note describing invariants, API/schema changes, cost
   budget, risks, reuse decisions, and acceptance tests.
4. Prefer independent reimplementation of proven patterns over speculative
   infrastructure or model calls.
5. Preserve evidence, provenance, and applicable third-party notices.

## Architecture rules

- PostgreSQL is the authority for control state, event history, checkpoints,
  and derived-state rebuild inputs.
- Preserve append-only raw evidence. Projections, summaries, indexes, graphs,
  and caches must remain rebuildable.
- Agents exchange typed state changes and evidence references by default.
- Use deterministic SQL, exact lookup, task/dependency routing, and bounded
  replay before lexical, vector, or learned retrieval.
- Every workspace boundary is explicit and enforced in contracts and queries.
- Every task claim is concurrency-safe, idempotent, lease/fence protected, and
  observable before it is considered production-ready.
- External boundaries validate input, use bounded timeouts, and return stable
  errors. Fail closed for authorization and evidence integrity.
- Do not add an LLM, broker, Redis, NATS, or another database unless a
  measured bottleneck justifies it.

## Required checks

```sh
make fmt
make test
make vet
make build
make python-check
make smoke
```

For Postgres-backed integration tests:

```sh
FORNIX_TEST_PG_DSN='postgres://fornix:fornix-dev-only@localhost:55433/fornix?sslmode=disable' \
  go test ./internal/store ./internal/projection -count=1 -v
```

Keep changes small and update code, migrations, tests, documentation, CI, and
smoke coverage together.
