# Fornix development rules

Fornix is an efficiency-first AI harness. Read `docs/00-fornix-foundation.md`
before changing the system.

## Research gate

Before implementing a feature:

1. Read the relevant architecture notes and, when available in the surrounding
   development workspace, the complete project research in `chats/`.
2. Search the relevant repositories in the surrounding `reference_repos/`
   directory with `rg` when a reference implementation is needed. Public
   contributors may use equivalent upstream checkouts; the repository must not
   depend on a developer-specific absolute path.
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
make docs-check
make smoke
```

For Postgres-backed integration tests:

```sh
FORNIX_TEST_PG_DSN='postgres://fornix:fornix-dev-only@localhost:55433/fornix?sslmode=disable' \
  go test ./internal/store ./internal/projection -count=1 -v
```

Keep changes small and update code, migrations, tests, documentation, CI, and
smoke coverage together.

## Documentation contract

Fornix is a public repository. Documentation is part of the product surface,
not a release-time afterthought. Every user-visible behavior and every
security, durability, cost, or licensing decision must be explainable to a
reader who has not seen the implementation history.

When changing behavior:

1. Update the relevant public guide, API reference, architecture note, or
   completion record in the same change.
2. State the audience, status, scope, authority, failure semantics, and
   remaining limitations. Distinguish measured results from design targets.
3. Document the safe/default path first. Put optional providers, destructive
   operations, development shortcuts, and at-least-once boundaries next to the
   command or endpoint that exposes them.
4. Keep examples copyable: show required environment variables, bounded
   budgets, workspace scope, authentication, and expected result shape. Never
   place real credentials, tokens, private paths, or unbounded output in docs.
5. Link concepts instead of repeating them. The documentation map and style
   rules live in `docs/52-documentation-guide.md`; the HTTP surface is mapped
   in `docs/53-http-api-reference.md`.

For Go code, package comments and exported identifier comments must explain
what a component owns, what it does not own, and which invariants callers must
preserve. Contracts should document units, workspace scope, mutability, and
whether a value is authoritative, derived, estimated, or redacted. Comments
must describe behavior rather than narrate obvious syntax.

Before opening a change for review, check links, command examples, formatting,
and terminology. A documentation-only change still runs the documentation
checks and the normal repository quality gates when code or generated output
is touched.
