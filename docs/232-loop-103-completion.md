# Loop 103 completion — registry-authoritative model tool schemas

Status: offline implementation and focused qualification complete. The
PostgreSQL persistence qualification remains pending because this local run has
no disposable PostgreSQL DSN.

## Outcome

Agent-run tool descriptions and JSON schemas now come from the registered tool
definition. A request can select a registered tool, but cannot replace its
description, argument schema, or authority fingerprint. Every selected tool
must pass a side-effect-free workspace/actor/task policy admission before run
reservation; policy is evaluated again against the actual model-produced
arguments before execution.

The model controls only dynamic argv values. The loop inserts the registered
executable and fixed argv prefix immediately before tool dispatch, so trusted
command structure and private execution paths are not placed in the provider
schema. Schema item/character limits are derived from the normalized executor
limits, and path positions are described relative to the dynamic arguments.
Commands fully described by their registered executable and fixed prefix can
use an empty dynamic argument list.
JSON Schema remains advisory: strict JSON decoding, byte-count checks, current
policy, workdir checks, approvals, and fencing remain authoritative.

An independent review caught and closed a critical clone-boundary defect:
registry copies now preserve `PathArgvIndexes`. Regression coverage confirms
the same path restrictions survive model-schema generation and actual
registered executor lookup, and that absolute paths outside the authorized
working directory are rejected.

Each catalog item carries a deterministic SHA-256 binding of the normalized
registration, model-visible name/description, and derived schema. The persisted
run hash includes the request hash and catalog. Resume recomputes the binding;
a changed registration fails before the external tool can run. The fingerprint
is internal metadata and is omitted from provider wire representations.

The OpenAI-compatible adapter maps internal names to deterministic,
reversible provider-safe aliases when necessary, remaps historical calls and
responses, and rejects provider responses for names outside the registered
catalog. The Ollama adapter now sends native tool definitions and preserves
assistant tool calls plus named tool-result history. Both adapters reject
unsupported streaming tool flows before provider egress rather than silently
dropping the catalog.

## References and license boundary

The design reuses the registered-schema compilation seam from [DeepSeek
Harness](https://github.com/deepseek-ai/DeepSeek-Harness) and the governed
capability lookup seam from [Orloj](https://github.com/omaveda/orloj); no
reference source code or dependency was copied. The inspected repositories use
MIT and Apache-2.0 licenses respectively. Kronaxis BSL-licensed source was not
used.

Provider serialization follows the current [OpenAI function-tool
reference](https://platform.openai.com/docs/api-reference/evals/deleteRun?lang=python)
for function-name character/length constraints, and the [Ollama native tool
calling guide](https://github.com/ollama/ollama/blob/main/docs/capabilities/tool-calling.mdx)
for `tools`, assistant `tool_calls`, and `tool_name` tool-result messages.

## Cost and storage

- No migration, table, index, service, or dependency was added.
- No new SQL read is required by the schema compiler; it uses the already
  registered tool definition and bounded catalog.
- The existing per-run JSON request snapshot stores the derived schema and one
  64-character SHA-256 hex fingerprint per selected tool. There is no extra
  authoritative record.
- Create-time authorization and schema derivation are bounded by the existing
  maximum catalog size. They reject unavailable or unauthorized tools before
  model spend.
- Fixed argv prefixes are no longer repeated in model output; their trusted
  bytes are inserted only in the fenced tool request.
- No representative throughput or production latency benchmark was run. This
  slice adds bounded in-memory work and no SQL query path; no throughput claim
  is made.

## Verification

Passed locally:

- `make qualification-agent-tool-schema-authority`
- `make qualification-agent-trust-boundary qualification-support-bundle qualification-workflow-retry-deadline`
- Full unit tests for `internal/agentloop`, `internal/contracts`, and
  `internal/tool`
- Focused pure adapter tests for Ollama tool request/history projection,
  OpenAI provider-safe name mapping, and provider-standard wire projection
- `go test ./... -run '^$' -count=1` (all Go test packages compile)
- A broad `go test ./... -count=1` run with the known local-listener test
  cases excluded: all remaining package tests pass. Those excluded tests
  create local HTTP listeners; this environment rejects `httptest.NewServer`
  binds.
- Race tests for the full `internal/agentloop`, `internal/contracts`, and
  `internal/tool` packages, plus focused pure model-provider tests
- Focused `go vet`, `make fmt-check`, and `make docs-check`

An unfiltered `go test ./... -count=1` was attempted and fails in tests that
create `httptest.NewServer` because this sandbox cannot bind loopback. The new
Ollama tests therefore exercise request projection and response normalization
without opening a socket. No live Ollama endpoint or OpenAI key was used. CI
must run the full provider HTTP tests in an environment that permits local
test listeners.

The PostgreSQL mutation test is wired into
`make qualification-agent-trust-boundary-postgres` and CI. It was not run
locally because `FORNIX_TEST_PG_DSN` is unset. CI itself was not run from this
local environment. A local PostgreSQL server is unavailable, and although the
Docker CLI exists, access to the Docker daemon socket is denied in this
execution environment. No image was pulled or database container created.
`git diff --check` is part of the final local handoff check and is not a
substitute for CI.

## Remaining limits and critique

- A registered description can still be misleading or hostile text; it is
  model-visible content, not policy. A trusted catalog administrator remains
  part of the security boundary.
- JSON Schema cannot prove the executor's UTF-8 byte limits or filesystem
  containment. The parser and executor remain authoritative and reject
  violations.
- Fixed argv prefixes are trusted registration data. Their insertion avoids
  asking a model to reproduce them but does not authenticate executable bytes
  at a mutable path; package integrity is a separate deployment concern.
- Provider function-name aliasing is deterministic and reversible but is not a
  provider idempotency guarantee. Remote model execution remains at-least-once
  where the provider does not honor idempotency keys.
- PostgreSQL immutability, workspace RLS, and crash-recovery claims still
  require the disposable-database qualification target.
- Streaming tool calls are deliberately unsupported by these adapters in this
  slice. Callers receive a fail-closed error before network activity.

The next production gate is to run the database-backed catalog immutability,
RLS, and recovery scenarios against a disposable PostgreSQL topology, then
qualify full OpenAI/Ollama provider calls in a network-enabled CI environment.
