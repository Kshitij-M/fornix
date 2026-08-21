# Operator control and reference workflow foundation

Status: active implementation note for Task 18.

## Objective

Task 18 adds the first usable operator surface around the existing Postgres
control plane and proves one complete deterministic workflow. The CLI is an
authenticated HTTP client of the same application routes used by operators
and the MCP compatibility shim; it does not contain a second task, model,
tool, retrieval, or artifact implementation.

The reference path is:

```text
workspace/bootstrap → actor/API key → repository chunks/symbols → task
  → fenced claim → retrieval/context → fake model → read-only tool
  → agent checkpoint → report artifact/evidence → task completion
  → events/projections/metrics → CLI/API/MCP inspection → replay
```

## Research and reuse decisions

- DeepSeek Harness contributes the operator ergonomics: explicit commands,
  machine-readable output, bounded diagnostics, and explicit credential
  configuration.
- Orloj contributes durable operator lifecycle ideas: authenticated API
  admission, deterministic inspection, controller-style health reporting, and
  evaluation lifecycle separation.
- agentmemory contributes cleanup/diagnostic discipline and replay-oriented
  operational commands.
- ClawMem and FornixDB contribute offline evaluation, bounded reports, and
  MCP/CLI parity patterns.
- Existing Fornix `AuthStore`, `TaskStore`, `AgentRunStore`, retrieval store,
  artifact/evidence stores, observability store, and HTTP routes remain the
  authority. No reference source is copied.
- DeepSeek Harness and FornixDB/ClawMem are MIT; Orloj and agentmemory are
  Apache-2.0 and would require notices if source were copied. Kronaxis Fabric
  is BSL 1.1 and is not copied. Fornix remains MIT.

## Invariants

1. Every operator request has one authenticated actor and one workspace.
2. A non-bootstrap API key cannot read or mutate another workspace, even when
   the body, header, query, and path disagree.
3. Bootstrap is explicit, bounded, idempotent, and never stores or returns a
   credential after its one-time creation response.
4. API keys are hashed at rest; OpenAI credentials are environment-only and
   never enter a request, event, artifact, metric, error, or test output.
5. Workspace bootstrap never downgrades an existing role or replaces an
   existing active API key. A repeated bootstrap returns metadata with no
   secret token.
6. Lists have deterministic ordering and a bounded opaque cursor. Operator
   output never contains raw prompts, credentials, or unbounded repository
   content.
7. Repository ingestion stores only bounded indexed content and a manifest
   identity. It is idempotent by workspace/repository/manifest and can be
   retried without duplicate authoritative rows.
8. The fake provider is the default. Its reference-workflow tool call is
   derived only from stable request metadata and the declared tool catalog.
9. Read-only repository execution uses structured argv, a workspace-scoped
   workdir policy, no shell, no network, and bounded output.
10. Task-bound agent and tool effects require the task owner/fence. A stale
    worker fails closed.
11. Report artifacts and evidence are content-addressed and linked by the
    same workspace/content hash. Raw history remains authoritative.
12. CLI, HTTP, and MCP task semantics are equivalent because all write paths
    call the same HTTP application routes.
13. Replay consumes recorded history and never invokes a provider or tool.

## Schema changes

Migration `025_operator_reference_workflow.sql` adds:

- workspace metadata with default-provider and read-only tool-root settings;
- workspace/repository ingest records with manifest and idempotency identity;
- append-only operator workflow audit records where the existing authorization
  audit is insufficient for bootstrap lifecycle detail.

The migration inserts the logical `default` workspace for existing databases
without changing existing task, event, or evidence history.

## Command and API contract

The Go `fornix` binary retains its server mode and adds subcommands. The CLI
uses `FORNIX_URL`, `FORNIX_KEY`, and `FORNIX_WORKSPACE_ID` by default and emits
stable JSON. Administrative operations are exposed under `/v1/operator/*`;
existing task/run/retrieval/artifact/evidence/metrics routes remain shared.

MCP extends the current stdio shim with the equivalent task/run/retrieval,
artifact, evidence, and evaluation inspection/call tools. It does not expose
plaintext API-key rotation output or OpenAI credentials.

## Bootstrap and secret policy

Workspace mode supports an explicitly configured one-time `FORNIX_BOOTSTRAP_KEY`
for the bootstrap endpoint. Development mode may use the existing development
key. The bootstrap key is compared in constant time and is never persisted.

A generated Fornix API key is returned only on the successful create/rotate
response that generated it. Stored API-key rows contain only a hash and prefix.
Normal list/get/replay/metrics output is always redacted.

## Reference workflow and crash semantics

The documented Docker/Make command uses a small fixture repository and the
fake provider. It records task ownership, retrieval/context hashes, model/tool
ledger identities, artifact/evidence hashes, and terminal task/run state.

The workflow is safe to retry after crashes at bootstrap, ingest, task claim,
retrieval capture, model persistence, tool execution, artifact/evidence write,
task completion, projection advancement, and replay. Postgres transactions and
existing idempotency/fencing boundaries determine whether the retry is a
no-op, a deterministic resume, or a fail-closed stale-worker result.

Remote OpenAI execution remains explicitly at-least-once. The opt-in smoke is
bounded by configured token, time, retry, and cost budgets and is skipped when
`FORNIX_OPENAI_API_KEY` is absent.

## Cost and storage budget

- Operator list/read endpoints: at most 100 rows per request and one bounded
  indexed query per resource.
- Ingest: bounded text files/chunks, no implicit embeddings required, and no
  raw repository archive upload.
- Reference workflow: fake model, one bounded read-only tool call, one report
  artifact, one evidence record, and no external model/tool calls.
- CLI and MCP never print full artifact bytes unless an explicit bounded raw
  disclosure command requests them.
- Workflow metrics report latency, SQL work where available, token usage,
  artifact bytes, deduplication, and replay throughput.

## Acceptance tests

- workspace bootstrap creates one workspace, identity, role, and API key;
  repeated bootstrap creates no duplicate and does not reveal the old key;
- API-key hash, expiry, revocation, rotation, and cross-workspace checks fail
  closed;
- operator lists are deterministic, bounded, cursor-paginated, and redacted;
- CLI, HTTP, and MCP task creation produce equivalent semantics;
- repository ingest is deterministic, bounded, resumable, and idempotent;
- fake-provider runs repeat with the same state/context/artifact/replay hashes;
- read-only repository tool execution is fenced, policy-scoped, shell-free,
  and output-bounded;
- report artifact and evidence share the expected content hash and provenance;
- duplicate task/run/artifact/evidence requests produce one durable effect;
- crash injection at every documented boundary is recoverable;
- replay never calls a remote provider or external tool;
- opt-in OpenAI smoke is skipped without a key and redacts the key when enabled;
- fresh/existing migrations, unit tests, race tests, builds, CI, and all smokes
  remain green.
