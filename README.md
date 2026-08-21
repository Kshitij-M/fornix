# Fornix

Fornix is **verifiable AI work infrastructure for long-running repository
operations**.

Teams can already ask AI to suggest code. The harder problem is allowing AI to
perform important work—dependency upgrades, security remediation, migrations,
large refactors, CI repair, and repository maintenance—without losing control
of scope, cost, evidence, approval, or recovery.

Fornix is being built to close that gap:

> **Delegate serious repository work to AI without losing the ability to bound,
> understand, recover, and replay it.**

The technical form is an efficiency-first AI harness. The product outcome is
safe autonomous work. Fornix uses exact state, deterministic routing, and
bounded retrieval first, spending model tokens only when remaining ambiguity
justifies them.

Fornix is open source and currently alpha. The durable control and retrieval
substrate is usable and tested, but the complete unattended repository
maintenance product is still being built.

Read the [product vision](docs/01-product-vision.md) for the problem, target
user, flagship workflow, and the distinction between the current alpha and the
longer-term product.

## The problem Fornix solves

Long-running AI work fails in expensive and difficult-to-audit ways. A worker
can lose its place, repeat a tool call, use the wrong project context, exceed a
budget, or produce an answer that cannot be traced back to source evidence.

When that happens, teams cannot confidently answer:

- What exactly did the agent do?
- Which source version and evidence did it use?
- What did the work cost?
- Can it resume after a crash without duplicating work?
- Can a reviewer verify the result without reading an entire transcript?

Fornix makes the conditions and history of AI work durable and inspectable:

- **State and recovery:** tasks, agent runs, leases, fencing tokens,
  checkpoints, retries, cancellation, and replay history.
- **Context and evidence:** deterministic staged retrieval, hard context
  budgets, immutable evidence, provenance edges, disclosure levels, and
  content-addressed artifacts.
- **External work:** explicit model providers, structured-argv tools,
  deny-by-default policy, approvals, idempotency records, and clear
  at-least-once boundaries.
- **Efficiency:** cost-aware routing metadata, token/byte/time limits,
  retrieval traces, fixed-dimension metrics, and offline evaluation.

The intended product output is a **Verified Change Packet**: a result linked
to its source snapshot, evidence, validation, cost, and recovery history. Its
machine-verifiable foundation is a future first-class **Work Receipt**. The
current alpha already stores most of the underlying control-plane facts; the
flagship repository-maintenance workflow will make that value visible as one
user-facing result.

## What Fornix is—and is not

Fornix is:

- a Postgres-backed authority for control-plane state and append-only history;
- a deterministic-first retrieval and context compiler;
- a bounded model, tool, and agent-run execution substrate;
- a workspace-scoped operator API, CLI, and MCP compatibility surface;
- a repository ingestion path for explicitly mounted local repositories.

In the product direction, these capabilities combine into a safe,
workspace-scoped runtime for repository maintenance. Fornix should integrate
with existing agent clients and runtimes where possible rather than requiring
every team to replace its preferred model or chat interface.

Fornix is not currently:

- a hosted model service, universal agent framework, or multi-agent graph
  executor;
- a kernel-level sandbox for arbitrary processes;
- a promise of exactly-once execution at a remote provider or process boundary;
- a replacement for backups, high availability, OAuth/SSO, an external secret
  manager, object storage, or a full production operations platform.

These boundaries are intentional. The current qualification and gap list are
maintained in [`docs/14-production-readiness-qualification.md`](docs/14-production-readiness-qualification.md).

## How the architecture works

At a high level, a request follows this shape:

```text
workspace + authenticated actor
  → task and ownership/fencing
  → deterministic retrieval and bounded context
  → optional model step
  → policy-controlled structured tool step
  → durable checkpoint, evidence, and artifact
  → projection, metrics, inspection, and replay
```

Postgres is the initial authority for control state, event history,
checkpoints, task ownership, identity metadata, evidence, artifacts, and
evaluation records. Projections, indexes, graphs, embeddings, metrics, and
reports are derived data; they must not silently replace authoritative history.

Every workspace-scoped operation is expected to preserve its actor, request,
idempotency, causation, and correlation references where that boundary
supports them. Stale workers fail closed through fencing rather than being
allowed to overwrite newer work.

## Current alpha capabilities

The current implementation includes the following tested slices:

- typed events, state deltas, idempotency, replay, projections, checkpoints,
  and consumer leases;
- workspace-scoped task claims, dependencies, retries, cancellation,
  dead-letter transitions, and fenced recovery;
- deterministic structured/lexical retrieval, bounded provenance expansion,
  gated vector retrieval, context hashes, and hard item/byte/token budgets;
- immutable evidence, provenance, supersession/contradiction metadata, and
  gist/detail/raw disclosure;
- fake and Ollama provider seams plus opt-in OpenAI-compatible chat, with
  bounded retries, budgets, redaction, and durable model-call metadata;
- deny-by-default structured-argv tools, approvals, bounded local execution,
  task fencing, and durable tool-run records;
- bounded agent runs and a Postgres-backed single-node scheduler;
- workspace identities, RBAC, hashed/expirable/revocable/rotatable API keys,
  credential references, and authorization audit records;
- content-addressed Postgres artifacts, output links, retention/integrity
  operations, observations, cost accounting, offline evaluation, and
  retrieval-surface capture;
- operator workspace/bootstrap, inspection, evaluation, disclosure, and
  durable repository-ingestion commands.

The [HTTP API reference](docs/53-http-api-reference.md) maps the current
routes. The [production qualification](docs/14-production-readiness-qualification.md)
records what has been verified and what remains outside the current slice.

## Quickstart: safe local path

The default development path uses a fake provider and local Docker services;
it does not require an OpenAI key or Ollama.

```sh
cp .env.example .env
make dev-up
```

In a second terminal, start the application:

```sh
make dev-run
```

Then check readiness:

```sh
curl http://localhost:8201/readyz
```

For the complete command list, local authentication modes, database-backed
tests, smoke suites, CLI usage, and the reference workflow, read
[`DEVELOPMENT.md`](DEVELOPMENT.md).

## First complete workflow

The reference workflow is the first executable showcase of the product
direction. It demonstrates the control-plane foundation behind a future
Verified Change Packet: bootstrap a workspace, ingest a source snapshot, create
and claim a task, retrieve bounded context, run a bounded agent loop, capture a
report artifact and evidence, complete the task, and verify replay hashes.

The current fixture workflow is intentionally read-only and uses a fake
provider. It proves durable admission, retrieval, execution, evidence, and
replay; it does not yet claim to be the finished unattended repository-change
experience.

With the service running:

```sh
make smoke-reference-workflow
```

OpenAI-compatible chat is disabled by default. The optional smoke reads
`FORNIX_OPENAI_API_KEY` from the process environment only; never put a key in
this repository, a test fixture, a command transcript, or an issue. Remote
provider calls remain at-least-once if the process dies after transmission and
before the local durable acknowledgement.

## Documentation

Start at the [documentation index](docs/README.md). It explains which
document answers which question and pairs each numbered foundation note with
its completion record.

The most useful entry points are:

- [Product vision](docs/01-product-vision.md) — the problem Fornix exists to
  solve, who it serves, and the Verified Change Packet direction.
- [Development guide](DEVELOPMENT.md) — setup, commands, tests, smoke suites,
  CLI usage, and local quality gates.
- [Documentation guide](docs/52-documentation-guide.md) — terminology,
  claim discipline, examples, and review checklist.
- [GitHub maintainer setup](GITHUB_SETUP.md) — repository rules, security
  features, project operations, and release automation.
- [Release guide](RELEASING.md) — tag-driven binaries, attestations, and GHCR
  images.
- [HTTP API reference](docs/53-http-api-reference.md) — routes, auth,
  workspace scope, idempotency, and external-effect semantics.
- [Fornix foundation](docs/00-fornix-foundation.md) — authority boundaries,
  design principles, development order, and success metrics.
- [Production qualification](docs/14-production-readiness-qualification.md)
  — verified capabilities and explicit production gaps.
- [Reference reuse matrix](docs/13-reference-reuse-matrix.md) — research
  sources, independent reimplementation decisions, and license boundaries.
- [Contributing](CONTRIBUTING.md) and [Security](SECURITY.md) — how to help
  safely and how to report security concerns.

## Repository layout

```text
cmd/fornix/                 service and operator CLI entrypoint
cmd/fornix-watcher/         filesystem watcher and indexing loop
cmd/fornix-eval/            offline retrieval-evaluation CLI
internal/contracts/         typed control-plane contracts
internal/model/             provider registry, gateway, and adapters
internal/tool/              tool registry, policy, approvals, and executor
internal/scheduler/         durable agent-run scheduling and recovery
internal/projection/        deterministic subscribers and derived views
internal/server/            HTTP handlers and auth boundaries
internal/store/             Postgres authority and migrations
scripts/                    import, indexing, MCP, and smoke helpers
docs/                       public architecture, API, and qualification notes
fixtures/                   small deterministic development fixtures
```

## Status and roadmap boundary

Fornix is intentionally being developed as a sequence of small, testable
control-plane slices that lead toward safe autonomous repository work. The
current alpha still lacks the complete change-producing workflow, OAuth/SSO,
external KMS or secret-manager integration, PostgreSQL row-level security,
general background evaluation and ingestion scheduling, multi-agent execution
graphs, a general sandbox provider, external artifact storage, backup/restore
drills, capacity benchmarks, and high-availability operations.

The roadmap is expressed as architecture and completion records rather than
an implied promise that every planned feature is production-ready. If you are
evaluating Fornix for a real workload, start with the qualification note and
review the relevant foundation/completion pair before relying on a boundary.

## License and research provenance

Fornix is released under the [MIT License](LICENSE). The design was informed
by the open-source repositories listed in the [reference reuse matrix](docs/13-reference-reuse-matrix.md),
but Fornix does not treat a reference repository as an authorization to copy
code. License compatibility and required notices are reviewed before reuse;
Kronaxis Fabric's BSL 1.1 code is not copied into Fornix.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution expectations and
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community standards.
