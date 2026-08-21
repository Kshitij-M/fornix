# Contributing to Fornix

Thank you for helping improve Fornix. The project is developing public,
verifiable AI work infrastructure for long-running repository operations. The
goal is to let teams delegate serious repository work to AI without losing
control of scope, cost, evidence, approval, or recovery.

A good contribution moves that goal forward. It makes behavior safer, more
deterministic, easier to inspect, easier to recover, or less expensive—and
explains the user problem and trade-off clearly. The harness is an engineering
means; the outcome is safe autonomous work that produces a result people can
understand and trust.

## Before you start

Read these documents in order:

1. [`README.md`](README.md) for the product boundary and alpha status.
2. [`docs/01-product-vision.md`](docs/01-product-vision.md) for the problem,
   target users, flagship workflow, and product language.
3. [`docs/00-fornix-foundation.md`](docs/00-fornix-foundation.md) for
   authority and determinism rules.
4. [`docs/52-documentation-guide.md`](docs/52-documentation-guide.md) for
   terminology, claims, examples, and review expectations.
5. [`docs/14-production-readiness-qualification.md`](docs/14-production-readiness-qualification.md)
   for verified behavior and known gaps.
6. The relevant foundation/completion pair in the
   [documentation index](docs/README.md).

For security-sensitive changes, read [`SECURITY.md`](SECURITY.md) first. Do
not include credentials, private repository contents, raw prompts, or exploit
details in an issue, pull request, test fixture, or documentation example.

## What contributions are useful

Documentation, tests, bug fixes, performance measurements, API ergonomics,
operator workflows, and carefully scoped features are all welcome. Product
work should make the flagship repository workflow more useful; infrastructure
work should preserve the guarantees that make a Verified Change Packet
credible. Small changes are easier to review and make it possible to preserve
the project’s append-only and replay guarantees.

Before implementing a new subsystem, search the existing code, migrations,
tests, architecture notes, and the reference reuse matrix. If a reference
repository supplies a useful pattern, reimplement the behavior independently
unless the licensing and attribution requirements for copied code have been
explicitly reviewed. Do not copy Kronaxis Fabric source; its BSL 1.1 license is
not the project’s MIT license.

## Local setup

The supported development path uses Docker for PostgreSQL and, when Go is not
installed locally, for the pinned Go toolchain:

```sh
cp .env.example .env
make dev-up
make hooks-install
make build
make test
```

Start the application with `make dev-run` when you need HTTP or smoke tests.
The default local path uses the development authentication mode and fake model
provider. Use workspace authentication and explicit workspace API keys when
testing production-shaped authorization behavior.

## Development workflow

1. Create a focused branch from the current default branch.
2. Write or update the feature note before implementing a non-trivial change.
3. Define invariants, authority, workspace scope, idempotency, crash behavior,
   budgets, and acceptance tests before choosing an abstraction.
4. Reuse existing typed contracts, stores, migrations, and test helpers where
   they already express the required behavior.
5. Keep external effects explicit. Do not add a broker, Redis, NATS, another
   database, or an LLM dependency without measured evidence and an architecture
   decision.
6. Update the relevant public documentation in the same change. Explain what
   users can rely on and what remains unqualified.
7. Run the appropriate tests and record the environment and measurements in a
   completion note when the change is a new loop.

## Quality checks

The repository-local hooks are recommended for every checkout:

```sh
make hooks-install
make hooks-check
```

The pre-commit hook checks staged whitespace, Go formatting, Python syntax,
and shell syntax. The pre-push hook runs the deterministic verification gate.
Run the checks explicitly before opening a pull request:

```sh
make fmt-check
make test
make test-race
make vet
make python-check
make docs-check
make build
```

For the full Postgres-backed qualification path, start the development
database and application, then run:

```sh
make smoke
```

The smoke suite uses the fake provider by default. The OpenAI smoke is
explicitly optional and reads its key only from `FORNIX_OPENAI_API_KEY`; it is
not needed to validate the deterministic path. See [`DEVELOPMENT.md`](DEVELOPMENT.md)
for the individual smoke commands and database integration-test commands.

## Documentation expectations

Public documentation should be understandable without reading the original
design conversation. For a user-visible behavior, document:

- the problem it solves and the safe/default path;
- the authoritative record and which outputs are derived;
- workspace, actor, task, session, and credential boundaries;
- duplicate, retry, timeout, crash, cancellation, and stale-owner behavior;
- hard item, byte, token, time, SQL, storage, and cost limits;
- evidence/provenance and how a result can be inspected or replayed;
- measured results versus targets, followed by remaining limitations.

Prefer links to existing notes over repeating the same contract in multiple
places. Keep examples bounded and copyable. Use placeholders for generated
IDs and never use real credentials or private paths.

## Pull requests

A pull request should use the repository template and state:

- what changed and why;
- the user-visible and operational behavior;
- schema, API, migration, or compatibility impact;
- security, licensing, and cost implications;
- tests and smokes run, including the environment;
- known limitations and follow-up work.

GitHub Actions runs the deterministic quality, integration, dependency, and
security checks for every pull request. A maintainer should not merge until
the required checks and code-owner review are complete. Fork contributions must
remain safe to run without repository secrets; do not ask contributors to use
`pull_request_target` or to paste provider credentials into a workflow.

If the change is documentation-only, say so explicitly and run
`make docs-check`; also check links, commands, terminology, and sensitive-data
handling. If a change modifies an
authority or failure boundary, include the relevant architecture note and
acceptance tests rather than relying on an implementation description alone.

## License

By contributing, you agree that your contribution is provided under the
repository’s [MIT License](LICENSE). Please preserve existing copyright and
third-party attribution notices. The [reference reuse matrix](docs/13-reference-reuse-matrix.md)
describes the license boundaries for the projects that informed Fornix.

## Community standards

Participation is governed by [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md). For
security concerns, follow [`SECURITY.md`](SECURITY.md) rather than opening a
public issue with sensitive details.
