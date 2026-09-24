# Fornix

Fornix is a **control plane for AI work against systems that matter**.

It is verifiable AI work infrastructure for long-running production-system
operations: bounded, durable, evidence-backed work with explicit scope,
policy, approval, recovery, cost controls, and replay.

Teams can already ask AI to suggest changes. The harder problem is allowing AI
to perform important work against production systems—repositories, APIs,
databases, cloud resources, ticketing systems, and internal operations—without
losing control of scope, cost, evidence, approval, or recovery. Repository
maintenance is Fornix's first concrete adapter, not its ceiling.

Fornix is being built to close that gap:

> **Delegate serious production-system work to AI without losing the ability to
> bound, understand, recover, and replay it.**

The technical form is an efficiency-first AI harness. The product outcome is
safe autonomous work. Fornix uses exact state, deterministic routing, and
bounded retrieval first, spending model tokens only when remaining ambiguity
justifies them.

Fornix is open source and currently alpha. The durable control and retrieval
substrate is usable and tested, but the complete autonomous production-system
operations product is still being built.

Read the [product vision](docs/01-product-vision.md) for the problem, target
user, flagship workflow, and the distinction between the current alpha and the
longer-term product.

The canonical top-down explanation of the universal direction is the
[Universal AI work control plane](docs/68-universal-work-control-plane.md).
It describes the domain-neutral lifecycle first and the repository adapter
second.

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
machine-verifiable foundation is now the first-class **Work Receipt**. The
current alpha already stores most of the underlying control-plane facts; the
reference workflow assembles them into one immutable, hash-stable receipt
without claiming that remote work was exactly-once.

## The universal control-plane model

Fornix is designed to govern work against repositories, APIs, databases, cloud
resources, ticketing systems, and internal business systems through explicit
typed adapters. The universal lifecycle is:

```text
intent
  → scope and identity
  → capability admission and policy
  → approval and budgets
  → durable fenced execution
  → evidence, artifacts, and external-effect records
  → validation and verification
  → Work Receipt
  → replay and evaluation
```

The repository adapter is the first concrete adapter and qualification path.
Bounded HTTP/API and read-only SQL adapters also exist as alpha reference
implementations, but they are not live-production-qualified. Cloud, ticketing,
and generic effectful execution remain adapter and recovery work. See
[`docs/02-what-works-today.md`](docs/02-what-works-today.md) for the concise
capability map, the [universal work control plane overview](docs/68-universal-work-control-plane.md)
for the architecture, and the [production qualification](docs/14-production-readiness-qualification.md)
for verified boundaries.

### What Fornix is relative to adjacent tools

Fornix is not trying to replace every model SDK, agent framework, workflow
engine, task queue, or connector. It provides the durable control plane around
those layers:

| Layer | Primary responsibility | Fornix’s relationship |
| --- | --- | --- |
| Model SDK | Call a model provider | Govern the bounded model step, usage, cost, evidence, and failure boundary |
| Agent framework | Compose prompts, turns, and tools | Add durable scope, admission, recovery, provenance, and replay |
| Workflow engine | Coordinate process execution | Add AI-specific context, approval, cost, and external-effect semantics |
| Task queue | Dispatch work | Add authoritative operation identity, fencing, verification, and receipts |
| Connector system | Access one external system | Standardize capability admission, policy, evidence, and lifecycle |

Fornix can integrate with existing agent clients and runtimes. A team should
not need to replace its preferred model or chat interface to gain bounded,
inspectable work.

## What Fornix is—and is not

Fornix is:

- a Postgres-backed authority for control-plane state and append-only history;
- a deterministic-first retrieval and context compiler;
- a bounded model, tool, and agent-run execution substrate;
- a workspace-scoped operator API, CLI, and MCP compatibility surface;
- a domain-neutral operation vocabulary for typed systems, resources,
  connectors, capabilities, effects, evidence, plans, and results;
- a repository ingestion path for explicitly mounted local repositories.

In the product direction, these capabilities combine into a safe,
workspace-scoped runtime for production-system operations. Repository
maintenance is the first adapter and qualification path. Fornix should
integrate with existing agent clients and runtimes where possible rather than
requiring every team to replace its preferred model or chat interface.

Fornix is not currently:

- a hosted model service, universal agent framework, or multi-agent graph
  executor;
- a kernel-level sandbox for arbitrary processes;
- a promise of exactly-once execution at a remote provider or process boundary;
- a replacement for backups, high availability, OAuth/SSO, an external secret
  manager, object storage, or a full production operations platform.

These boundaries are intentional. The current qualification and gap list are
maintained in [`docs/14-production-readiness-qualification.md`](docs/14-production-readiness-qualification.md).

For a quick answer about supported domains, read
[`docs/02-what-works-today.md`](docs/02-what-works-today.md). For the meaning
of workspace, operation, capability, lease, fence, effect, evidence, artifact,
and replay, read [`docs/03-concepts.md`](docs/03-concepts.md).

## How the architecture works

At a high level, a domain-neutral request follows this shape:

```text
intent + authenticated actor + workspace
  → resource and capability admission
  → policy, approval, and budgets
  → durable ownership/fencing and checkpointed steps
  → connector/model/tool execution
  → evidence, artifacts, validation, and cost
  → Work Receipt, inspection, and replay
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
- immutable workspace-scoped Work Receipts linking task/run, retrieval,
  model/tool, evidence, artifact, cost, and replay identities with bounded
  gist/detail/raw disclosure;
- approval-gated repository change packets with source preconditions,
  content-addressed operation artifacts, workspace-mount and symlink safety,
  durable approval decisions, structured filesystem application, post-state
  verification, recovery-required classification, and Verified Change Packet
  receipts;
- immutable workspace-scoped validation policy packs with deterministic
  validator resolution, tightening-only budgets, approval/re-index controls,
  lifecycle audit, exact policy pinning, and fail-closed verified change
  admission;
- operator workspace/bootstrap, inspection, evaluation, disclosure, and
  durable repository-ingestion commands.
- a domain-neutral durable workflow runtime with typed model/tool/connector,
  approval/human/callback, validation, compensation, wait, retry, checkpoint,
  fencing, and inert replay seams; repository maintenance is only one adapter.
- a generic Postgres operation queue and adapter-owned worker boundary with
  deterministic claims, heartbeats, cancellation, expiry takeover, and
  fail-closed fencing; it does not dispatch providers or claim exactly-once
  remote execution.
- an explicit-workspace supervisor with deterministic round-robin turns,
  bounded concurrency, workspace active-lease quotas, and declared resource
  serialization; this is a scheduling foundation, not autoscaling or a
  universal provider dispatcher.

The [HTTP API reference](docs/53-http-api-reference.md) maps the current
routes. The [production qualification](docs/14-production-readiness-qualification.md)
records what has been verified and what remains outside the current slice.

## Universal transformation roadmap

The historical implementation loops built the deterministic control and
retrieval substrate. The next transformation makes generic operations,
connectors, effects, policy, and workflows first-class:

| Issue | Scope | Status |
| --- | --- | --- |
| [#38](https://github.com/Kshitij-M/fornix/issues/38) | Universal transformation umbrella | Active roadmap |
| [#41](https://github.com/Kshitij-M/fornix/issues/41) | Domain-neutral contracts and adapter boundary | Implemented alpha slice |
| [#45](https://github.com/Kshitij-M/fornix/issues/45) | Connector and capability registry | Implemented alpha slice; process-local |
| [#39](https://github.com/Kshitij-M/fornix/issues/39) | Durable generic operation authority | Alpha foundation implemented; final qualification and adapter integration remain |
| [#46](https://github.com/Kshitij-M/fornix/issues/46) | Universal policy, approvals, and external effects | Alpha Postgres admission/effect foundation; connector qualification remains |
| [#43](https://github.com/Kshitij-M/fornix/issues/43) | HTTP/API and read-only SQL connectors | Alpha bounded reference adapters implemented; production qualification remains |
| [#44](https://github.com/Kshitij-M/fornix/issues/44) | Durable multi-step workflow runtime | Alpha foundation implemented; qualification remains |
| [#42](https://github.com/Kshitij-M/fornix/issues/42) | Multi-domain reference workflow | Fake-first alpha workflow implemented; production qualification remains |
| [#40](https://github.com/Kshitij-M/fornix/issues/40) | Universal production qualification | In progress: authority, RLS, capacity, queue-claim, and adapter-worker qualification slices; production security, fairness, HA, recovery, backup/restore, and operational evidence remain |

This roadmap is not a claim that the complete universal production platform
already exists. It is the sequence for extending the implemented control-plane
substrate beyond the repository adapter.

## Quickstart: Fornix Local

The product path is install, start, run, and inspect. Docker Desktop on macOS
or Docker Engine plus Compose v2 on Linux is the one host prerequisite; Fornix
manages the database, migrations, workspace bootstrap, and runtime lifecycle
after Docker is available.

The checksum-verified release installer is the intended distribution path. For a source
checkout, or when evaluating unreleased changes, build the same CLI locally:

```sh
make build
./bin/fornix doctor
./bin/fornix start --repo .
./bin/fornix run --repo . "Explain the architecture of this repository"
```

The default fake provider is deterministic and offline. To exercise the
complete local reference workflow, including replay and Work Receipt checks:

```sh
./bin/fornix demo --repo .
```

The verified public alpha clean-install path is:

```sh
curl -fsSL https://raw.githubusercontent.com/Kshitij-M/fornix/main/scripts/install.sh | sh
cd my-repository
fornix start
fornix run --repo . "Review this repository and identify the highest-risk issues"
```

The current public alpha is [v0.11.0-alpha.3](https://github.com/Kshitij-M/fornix/releases/tag/v0.11.0-alpha.3).
The raw GitHub installer URL is the explicit fallback until the planned
`get.fornix.dev` alias has verified DNS and hosting.

`https://get.fornix.dev/install.sh` is a planned short alias for the same
versioned installer and should only be advertised after its DNS and hosting
are verified. Release archives include checksums, an SBOM, and third-party
notices; the release workflow attests the checksum manifest. Docker Desktop
on macOS or Docker Engine plus Compose v2 on Linux remains the one host
prerequisite.

Useful lifecycle commands are `fornix status`, `fornix logs`, `fornix doctor`,
`fornix stop`, `fornix restart`, `fornix upgrade`, and `fornix uninstall`.
Read the [Fornix Local operations guide](docs/63-fornix-local-operations.md)
for profile layout, provider opt-in, budgets, recovery, security boundaries,
and troubleshooting.

## Quickstart: development services

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
report artifact and evidence, complete the task, verify replay hashes, and
finalize one Work Receipt over the authoritative records.

The current fixture workflow is intentionally read-only and uses a fake
provider. It proves durable admission, retrieval, execution, evidence, and
replay. The separate `fornix change` workflow now demonstrates the approval-
gated write boundary for explicitly configured local mounts; it does not yet
automatically turn every agent response into a proposed patch or provide a
host-independent sandbox.

The first non-repository reference workflow is also available as a bounded,
fake-first incident investigation flow. It accepts a typed monitoring event,
reads a runbook through a connector seam, builds evidence-backed diagnostics,
pauses for approval, records an idempotent remediation attempt, verifies the
recorded external state, and produces a replayable result. It is a showcase
of the universal control-plane contract—not a live monitoring or remediation
integration.

With the service running, exercise it through the shared HTTP/CLI/MCP surface:

```sh
bin/fornix incident start --workspace reference-local \
  --source monitor --external-id payment-1 \
  --payload '{"service":"payments","status":"degraded"}'
bin/fornix incident approve --workspace reference-local \
  --run-id <run-id> --decision approve
bin/fornix incident replay --workspace reference-local --run-id <run-id>
```

The default incident adapter is deterministic and offline. No live external
effect is enabled by this workflow, and rejection, duplicate delivery,
workspace isolation, crash recovery, and inert replay are tested explicitly.

With the service running:

```sh
make smoke-reference-workflow
```

Inspect a receipt without replaying external work:

```sh
bin/fornix receipt disclose --id <receipt-id> --level detail
```

Post-change validation closes the repository-change proof loop. After an
approved packet is applied, Fornix verifies the observed result tree and
configured mount with a deterministic bounded validator plan, preserves
hash-only evidence, creates a new ingestion handoff, and records a verified
Work Receipt. The default path is offline and read-only at the repository
boundary:

```sh
make smoke-validation
```

Validation replay reads only the durable Postgres result/event history and
cannot execute a model, tool, or ingestion job. See the
[validation foundation](docs/58-validation-foundation.md),
[Task 22 completion record](docs/59-loop-22-completion.md), and
[HTTP API reference](docs/53-http-api-reference.md) for the exact lifecycle,
budgets, workspace authorization, crash semantics, and operator commands.

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
- [Connector and capability foundation](docs/67-connector-capability-foundation.md)
  — explicit adapter registration, fail-closed admission, and conformance.
- [Contributing](CONTRIBUTING.md) and [Security](SECURITY.md) — how to help
  safely and how to report security concerns.

## Repository layout

```text
cmd/fornix/                 service and operator CLI entrypoint
cmd/fornix-watcher/         filesystem watcher and indexing loop
cmd/fornix-eval/            offline retrieval-evaluation CLI
internal/contracts/         typed control-plane contracts
internal/connector/         connector registry, admission, execution, conformance
internal/adapters/          domain-specific connector implementations
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
control-plane slices that lead toward safe autonomous production-system work.
Repository maintenance is the first adapter and qualification path. The
current alpha still lacks a fully automated agent-to-change workflow, OAuth/SSO,
external KMS or secret-manager integration, PostgreSQL row-level security,
general background evaluation and ingestion scheduling, multi-agent execution
graphs, a general sandbox provider, external artifact storage, deployment-
specific backup/PITR/HA operations, and capacity benchmarks. A destructive
logical backup/restore qualification drill exists, but it is not a production
RPO/RTO or failover guarantee.

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
