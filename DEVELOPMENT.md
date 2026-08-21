# Fornix development environment

The local environment is a Go/Postgres service with optional pgvector and
Ollama support. Docker provides the pinned toolchain when Go is not installed
on the host.

## What this repository demonstrates

Fornix is being built as **verifiable AI work infrastructure for long-running
repository operations**. The development workflow is therefore organized
around the product path:

```text
admit a scoped task
  → execute with bounded context, model, and tools
  → preserve evidence, artifacts, cost, and recovery
  → inspect and replay the result
```

The current alpha demonstrates this path with a deterministic, read-only
reference workflow. It is a control-plane showcase, not yet a finished
unattended repository-change product. Use the [product vision](docs/01-product-vision.md)
to understand why each local command exists and the
[qualification note](docs/14-production-readiness-qualification.md) before
interpreting a passing smoke as production readiness.

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
make fmt-check
make test
make test-race
make vet
make build
make python-install
make python-check
make docs-check
make check
make verify
make hooks-install
make hooks-check
make smoke
make smoke-events
make smoke-projection
make smoke-leases
make smoke-tasks
make smoke-retrieval
make smoke-provenance
make smoke-model
make smoke-tools
make smoke-agent
make smoke-scheduler
make smoke-identity
make smoke-artifacts
make smoke-artifact-output
make smoke-observability
make smoke-retrieval-quality
make smoke-retrieval-evaluation
make smoke-reference-workflow
make smoke-reference-openai
make smoke-ingestion
make operator-reference
make dev-up
make dev-run
make dev-up-watcher
make dev-logs
make dev-down
```

## Local quality gates

Install the repository-local hooks once per checkout:

```sh
make hooks-install
```

The pre-commit hook runs staged whitespace checks, Go formatting, Python
syntax checks, and shell syntax checks. The pre-push hook runs `make verify`,
which includes formatting, unit tests, race tests, vet, Python checks,
documentation link/status checks, and binary builds. Docker is used
automatically for the pinned Go toolchain when Go is not installed locally.
Hooks fail closed when a required toolchain is unavailable.

`make docs-check` validates that Markdown files have a readable top-level
heading, architecture notes declare their status, local links resolve, and
developer-specific absolute paths do not leak into public documentation.

To remove the repository-local hook override:

```sh
make hooks-uninstall
```

CI runs the same Make targets and keeps the Postgres/HTTP qualification job
separate. Its integration job has a bounded timeout, cancels superseded runs,
and supplies the runner-to-container database address explicitly to the
database-backed smoke scripts.

The projection runtime is an internal Postgres-backed pull API. It does not
start a background worker; callers explicitly run bounded batches or a
rebuild. The v0.12 smoke exercises replay, duplicate delivery, rollback,
concurrency, and workspace isolation. The v0.13 smoke adds durable
workspace-scoped consumer leases, fencing, expiry/takeover, stale-owner
rejection, and lease transaction rollback:

```sh
make smoke-projection
make smoke-leases
make smoke-tasks
make smoke-retrieval
make smoke-provenance
```

Task 17 adds append-only retrieval-surface capture and an authenticated
operator evaluation surface. `POST /v1/retrieve` records only request/plan/
context hashes, bounded stage traces, evidence references, budgets, and
measurements; it never records the query or rendered context. Operators can
register datasets, list recorded surfaces, run bounded durable or dry-run
evaluations, compare a baseline run, and read metrics/gates/reports:

```text
POST /v1/evaluations/datasets
GET|POST /v1/evaluations/retrieval/surfaces
POST /v1/evaluations/retrieval/runs
GET /v1/evaluations/runs/{id}
```

All endpoints are workspace-authorized and require the evaluation read,
evaluation run, or evaluation write capability. Dry runs perform only bounded
Postgres reads and create no evaluation rows or artifacts. The local
`fornix-eval` command consumes a redacted recorded-surface bundle and never
calls a model, tool, broker, or external system:

```sh
make build
bin/fornix-eval -input ./recorded-retrieval-bundle.json
make smoke-retrieval-evaluation
```

The v0.26 smoke checks automatic capture, operator authentication,
idempotent surface/dataset/run submission, dry-run non-persistence,
artifact-free redacted reports, and byte-identical offline CLI replay.

## Operator CLI and reference workflow

Bootstrap a workspace with the explicit bootstrap credential, then use the
returned workspace API key for all subsequent operations:

```sh
export FORNIX_URL=http://localhost:8201
export FORNIX_BOOTSTRAP_KEY=local-bootstrap-secret
bin/fornix --url "$FORNIX_URL" workspace bootstrap --workspace reference-local --tool-root /workspace/fixtures/reference-repo
bin/fornix --workspace reference-local workspace list
bin/fornix --workspace reference-local task list
```

The complete deterministic path indexes a local fixture, claims a fenced task,
compiles bounded retrieval context, invokes the fake provider, reads a file
through the registered structured-argv read-only tool, writes a report artifact
and linked evidence, completes the task, and replays the run from sequence
zero. This is the durable foundation behind the future Verified Change Packet;
it deliberately does not modify the fixture:

```sh
bin/fornix reference-workflow --workspace reference-local --fixture fixtures/reference-repo --workdir /workspace/fixtures/reference-repo
```

The CLI/API/MCP surfaces are backed by the same HTTP semantics. List,
disclosure, metrics, and evaluation operations are bounded and workspace
authorized. One-time API-key tokens are returned only by explicit bootstrap or
key-create/rotate commands; normal workflow output is redacted.

Repository ingestion is a durable, operator-driven path. A workspace must be
bootstrapped with an explicit `tool_root`; submitted `source_root` values must
remain inside that configured mount. Discovery is deterministic and rejects
symlinks, traversal, oversized/binary files, and unstable paths. The default
path is offline and does not call Ollama or an LLM:

```sh
bin/fornix --workspace reference-local ingest dry-run --source-root /workspace/fixtures/reference-repo
bin/fornix --workspace reference-local ingest submit --source-root /workspace/fixtures/reference-repo --repository reference-repo
bin/fornix --workspace reference-local ingest status --id <job-id>
bin/fornix --workspace reference-local ingest resume --id <job-id> --batch-size 32
bin/fornix --workspace reference-local ingest cancel --id <job-id>
make smoke-ingestion
```

Each resume advances one bounded Postgres transaction containing compatibility
chunks/symbols, immutable ingest lineage, the checkpoint, and the lifecycle
event. Repeating submit/resume is idempotent; changed or removed source paths
create auditable supersession/removal metadata. Embeddings are explicit opt-in
work and skipped when the provider or configured budget is unavailable.

The model gateway is exposed at `POST /v1/model/complete`. It always routes
through an explicitly registered provider and persists one workspace-scoped
model-call record per idempotency key. The deterministic `fake` provider is
available for offline development and smoke tests. Ollama remains the local
embedding provider. OpenAI-compatible chat is disabled by default; enable it
only with `FORNIX_OPENAI_ENABLED=true` and provide the credential through
`FORNIX_OPENAI_API_KEY` (or a configured credential reference). The request
contract accepts a provider/model, prompt or messages, workspace and actor
references, and hard budget/retry settings. Remote execution is at-least-once
if a process dies after transmission; the gateway sends `Idempotency-Key` but
cannot promise exactly-once behavior from an arbitrary upstream.

Run the v0.17 model smoke after rebuilding the service:

```sh
make smoke-model
```

The tool boundary is exposed at `POST /v1/tools/execute`. Only explicitly
registered structured argv definitions can run; the default policy allows the
safe `fornix.echo` capability only in the `default` workspace. Requests carry
an idempotency key and receive one durable tool run. Use
`POST /v1/tools/approvals/{approval_id}/decide` for a durable interactive
decision when a later policy rule enables `interactive` or `pre_approved`
mode. The local process executor does not invoke a shell, inherits no
environment, caps output/arguments/environment/time, and does not claim
kernel-level network or filesystem isolation. A process crash after spawn is
therefore explicitly at-least-once.

Run the v0.18 tool smoke after rebuilding the service:

```sh
make smoke-tools
```

The bounded agent loop is exposed at `POST /v1/agent/run`. It compiles the
workspace-scoped context pack once, then advances deterministic model/tool
phases until completion or an explicit waiting state. Duplicate submissions
reuse the same run; `GET /v1/agent/run/{id}` reads the checkpoint and
`POST /v1/agent/run/{id}/replay` returns only that run's typed events.
Integrations can durably pause at `/external/wait` and resume once at
`/external/complete`; cancellation remains terminal. The
offline `fake` provider is the recommended development path. A remote model
call or local process can still be observed at-least-once if the process dies
after the external side effect and before the durable checkpoint; provider and
tool idempotency keys reduce, but cannot eliminate, that boundary.

Run the v0.19 agent-loop smoke after rebuilding the service:

```sh
make smoke-agent
```

Fornix also starts a Postgres-backed agent-run pull worker with the HTTP
service. It claims due `pending`, expired `running`, due `awaiting_retry`, and
approved `awaiting_approval` runs using a workspace/run lease and monotonic
fence. Heartbeats and checkpoint commits are lease-protected; a process crash
leaves the run reclaimable after expiry. `awaiting_external` is resumed only
through its explicit idempotent external-completion boundary, never by
speculative polling. The v0.20 smoke checks the worker-compatible durable run
and duplicate path; the Postgres scheduler, takeover, stale-worker, crash, and
workspace tests run with:

```sh
FORNIX_TEST_PG_DSN='postgres://fornix:fornix-dev-only@host.docker.internal:55433/fornix?sslmode=disable' \
  docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN -v "$PWD:/workspace" -w /workspace golang:1.25.13 \
  go test ./internal/store ./internal/scheduler -run 'TestAgentRunScheduler|TestWorker' -count=1 -v
```

Authentication has two explicit modes. Local smokes use
`FORNIX_AUTH_MODE=development` and the legacy `FORNIX_KEY` compatibility key.
Production must use `FORNIX_AUTH_MODE=workspace`; bearer tokens are then
workspace-bound API keys created through `internal/store.AuthStore`. Keys are
hashed, expirable, revocable, rotatable, and never appear in events or logs.
Authorization decisions are audited in Postgres and the authenticated actor
overrides spoofable body/header actor fields. The v0.21 smoke checks the
unauthenticated, authorized, and wrong-key HTTP paths; store and server tests
cover workspace isolation, RBAC, audit idempotency, rotation, expiry, and
revocation.

When running package integration tests against the same local database as a
live service, set `FORNIX_WORKER_ENABLED=false` for the service or stop it
temporarily; otherwise its legitimate pull worker can claim the scheduler
fixture concurrently. CI sets this flag explicitly while its standalone
worker tests run.

To run the database integration tests directly:

```sh
docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN='postgres://fornix:fornix-dev-only@host.docker.internal:55433/fornix?sslmode=disable' \
  -v "$PWD:/workspace" -w /workspace golang:1.25.13 \
  go test ./internal/store ./internal/projection -count=1 -v
```

Task execution is a Postgres-only state machine. Claim responses include the
current task fence; send that fence with the session ID to renew, complete,
fail, or cancel a claimed task. A retryable failure returns a task to the
deterministic dependency-aware queue until its bounded attempt budget is
exhausted, then dead-letters it. Expired ownership is recovered by takeover;
the old fence cannot mutate the task.

Retrieval is a read-only Postgres snapshot and is exposed at `POST
/v1/retrieve`. The planner performs structured and lexical work before bounded
graph expansion, and requires a caller-supplied embedding before vector work.
Every response includes stage trace counters, source/evidence references, a
stable context hash, and hard item/byte/token totals. Run the v0.15 smoke after
rebuilding the service:

```sh
make smoke-retrieval
```

Evidence and provenance are exposed at `POST /v1/evidence`,
`POST /v1/evidence/edge`, `POST /v1/evidence/disclose`, and
`POST /v1/evidence/provenance`. Evidence writes compute the raw SHA-256 in
Fornix, preserve raw bytes, and reject updates/deletes. Disclosure defaults to
gist-first, can request detail or complete raw, and reports truncation when a
hard byte/token budget cannot fit the complete raw payload. Supersession and
contradiction records remain auditable; all routes require the existing bearer
key and workspace scope.

Run the v0.16 smoke after rebuilding the service:

```sh
make smoke-provenance
```

Artifacts are immutable, workspace-scoped SHA-256 content records backed by
Postgres chunks. Oversized tool output, evidence raw payloads, and agent
output/history are linked transactionally while inline compatibility fields
remain bounded markers. Operators can use the typed HTTP/store APIs for
bounded dry-run or resumable backfill, archive-then-delete retention sweeps,
integrity verification, and storage metrics. The v0.22 smoke covers artifact
creation/disclosure/isolation; the v0.23 smoke covers output integration,
rollback, backfill, retention, integrity, and metrics:

```sh
make smoke-artifacts
make smoke-artifact-output
make smoke-observability
```

The output path remains at-least-once at external tool/process boundaries.
Postgres commits provide one durable source effect and one idempotent artifact
reference; they cannot make a remote process or already-started child process
exactly-once.

Task 15 adds durable workspace-scoped observations, cost ledger entries, fixed
dimension metrics, and offline evaluation. `GET /v1/observability/metrics`
(also available as `/v1/metrics`) returns a bounded authorized snapshot for at
most 24 hours. Replay uses recorded agent checkpoints only and never invokes a
remote model or external tool. Evaluation reports contain hashes and bounded
gate summaries; oversized reports use the Postgres artifact plane:

```sh
make smoke-observability
```

## Repository rules

Never commit `.env`, database volumes, model files, raw transcripts, or
generated binaries. Use synthetic fixtures for tests. Read `AGENTS.md` and
the architecture notes before implementing new functionality.
