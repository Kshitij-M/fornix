# Fornix documentation guide

Status: active public documentation contract.

This guide explains how the Fornix documentation is organized, what each
document promises, and how contributors should describe new behavior. It is
intended for users, operators, maintainers, reviewers, and future contributors.

## Start with the right document

| Question | Start here | What it covers |
| --- | --- | --- |
| What is Fornix and why does it exist? | [`README.md`](../README.md) | Product purpose, current capabilities, quickstart, and honest alpha status |
| What problem should Fornix own? | [`01-product-vision.md`](01-product-vision.md) | Verifiable AI work, repository-maintenance wedge, Work Receipts, and product boundaries |
| How do I run or test it locally? | [`DEVELOPMENT.md`](../DEVELOPMENT.md) | Docker environment, commands, smoke suites, CLI workflow, and quality gates |
| What rules govern implementation? | [`AGENTS.md`](../AGENTS.md) | Research gate, architecture invariants, documentation contract, and required checks |
| How is the system designed? | [`00-fornix-foundation.md`](00-fornix-foundation.md) | Authority boundaries, deterministic-first design, development order, and success metrics |
| What has been implemented? | The relevant `*-foundation.md` note | Contracts, schema, behavior, tests, cost, and limitations for one subsystem |
| What evidence qualifies a loop? | The relevant `*-completion.md` note | Delivered changes, validation evidence, measurements, and remaining limitations |
| Is it production-ready? | [`14-production-readiness-qualification.md`](14-production-readiness-qualification.md) | Verified capabilities, explicit gaps, and qualification commands |
| Which HTTP routes exist? | [`53-http-api-reference.md`](53-http-api-reference.md) | Route families, authentication, request conventions, and mutation semantics |
| Where did a design pattern come from? | [`13-reference-reuse-matrix.md`](13-reference-reuse-matrix.md) | Reference repositories, independent-reimplementation decisions, and license boundaries |

The numbered architecture notes are intentionally chronological. A foundation
note describes a design slice; the completion note with the matching loop
number records what was actually delivered. When a completion note and a
foundation note disagree, the completion note describes the current
implementation, while the foundation note remains the historical design
record. The production qualification note is the current cross-cutting
summary.

## The public explanation of Fornix

Every subsystem should be explainable in this order. The repository as a whole
should follow the same order: start with the repository-work problem, then
show the user-visible result, then explain the machinery that makes it
trustworthy.

1. **User problem.** What expensive, unsafe, or ambiguous workflow does this
   solve?
2. **Default path.** What happens when the user supplies no optional provider
   or advanced setting?
3. **Authority.** Which Postgres record is authoritative? Which indexes,
   projections, summaries, embeddings, or reports are rebuildable?
4. **Boundaries.** What is workspace-scoped? Which actor, task, session,
   request, idempotency, causation, and correlation references are preserved?
5. **Failure behavior.** What happens on duplicate delivery, timeout, crash,
   stale fencing token, cancellation, provider failure, or partial external
   execution?
6. **Cost and limits.** Which item, byte, token, time, SQL, storage, retry,
   or provider budgets are enforced? Are measurements exact, estimated, or
   unknown?
7. **Evidence.** How can a user inspect the result, provenance, artifact,
   event, checkpoint, replay hash, or audit record?
8. **Limitations.** What is intentionally not implemented yet?

This order makes the docs useful to both a first-time user and an engineer
reviewing a production decision. Avoid describing a subsystem only as a list
of types or endpoints.

## Terminology and claims

Use these terms consistently:

- **Authority:** the durable Postgres row or append-only history from which
  derived state can be rebuilt.
- **Derived:** a projection, index, graph, embedding, metric, report, or
  summary that can be recomputed and must not replace the authority.
- **Deterministic:** identical scoped inputs and recorded dependencies produce
  identical ordering, hashes, state transitions, or decisions.
- **Bounded:** a hard limit is enforced before work or disclosure exceeds the
  configured item, byte, token, time, SQL, retry, or cost budget.
- **Idempotent:** repeating the same scoped request produces the same durable
  effect rather than another effect.
- **At-least-once:** an external provider or process may receive work before
  the local durable acknowledgment is committed. Do not call this exactly-once
  even when an idempotency key is sent.
- **Fail closed:** uncertainty, missing authority, stale ownership, invalid
  scope, or failed integrity checks reject the operation rather than widening
  access or guessing.

Do not describe an alpha capability as “production-ready” merely because its
unit tests pass. Say what was tested, in which environment, with which
dataset, and what remains unqualified. Measurements must include units and
scope; for example, “warm local HTTP p95 over ten two-file fixture requests,”
not “fast.”

## Writing code documentation

Go documentation follows the standard Go convention:

- Add a package comment for each non-trivial package.
- Begin exported type, interface, function, method, and constant comments
  with the identifier name.
- Document units (`bytes`, `tokens`, `milliseconds`, or `SHA-256`), ownership,
  mutability, and normalization rules where they are not obvious.
- Explain security-sensitive behavior at the boundary: authentication,
  workspace checks, fencing, shell avoidance, redaction, and credential
  handling.
- Explain why an operation is transactional or append-only when that is part
  of its correctness contract.
- Do not add comments that merely restate a function name or every field in a
  trivial private helper.

The source is the detailed contract for maintainers; the Markdown docs are
the task-oriented explanation for users. Keep the two aligned, but do not
copy entire source files or SQL migrations into prose.

## Examples and sensitive data

Examples must use:

- `localhost` or an explicitly documented Docker host alias;
- the development-only credentials already named in `.env.example`, clearly
  labeled as local-only;
- fake providers and fixture repositories by default;
- explicit workspace IDs and bounded limits;
- placeholders such as `<workspace-id>` and `<run-id>` for generated values.

Never include a real API key, an OpenAI credential, a private filesystem path,
raw prompts, raw user content, or an unbounded log dump. Explain how to supply
secrets through the environment without asking users to paste them into an
issue, chat, commit, or test fixture.

## Review checklist

Before merging documentation or a feature that changes docs, verify:

- the relevant document is linked from the documentation map;
- status and alpha limitations are explicit;
- commands and route names match the current code;
- every write example includes workspace and authentication context;
- idempotency, replay, crash, and at-least-once semantics are not hidden;
- security and credential handling are stated near the relevant operation;
- measured results identify environment, sample, and units;
- links resolve without relying on a local absolute path;
- no secrets or arbitrary user content appear in examples;
- Markdown is readable on GitHub without requiring earlier chat history.
