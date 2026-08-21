# Loop 8 completion — model gateway and provider execution

Status: complete.

## Delivered

- Typed model request/response/stream, endpoint, provider, usage, cost,
  failure, retry, identity, workspace, actor, task, and session contracts.
- Explicit registry with canonical names, collision-safe aliases, and stable
  lookup/listing.
- Ollama chat/embedding behavior behind the provider interface, deterministic
  fake provider, and opt-in OpenAI-compatible chat completion with SSE.
- Bounded input/output bytes, estimated tokens, total tokens, timeout, retry,
  response evidence, and configured cost budgets.
- Stable authentication, quota, rate-limit, context-window, transport,
  timeout, provider, invalid-request, cancellation, and budget failures.
- Retry only when both the provider failure and request policy mark the failure
  retryable. Fallback is permitted only before visible stream content.
- Workspace-scoped durable model-call ledger with request hashes, idempotency,
  metadata, timing, usage, cost, provider request IDs, terminal outcomes, and
  bounded redacted evidence.
- Migration 011 creates the ledger. Migrations 012 and 013 add timing,
  metadata, and first-class request identity columns additively so historical
  migration checksums remain stable for existing databases.
- Authenticated `POST /v1/model/complete`, fake-provider smoke, CI coverage,
  local configuration, and operational documentation.

## Verification

Executed successfully:

```text
make test
FORNIX_TEST_PG_DSN=... make test
make vet
make build
make python-check
go test -race ./...
make smoke-model
```

The Postgres suite applied migrations against both the already-initialized
development database and the model-call integration tests. Concurrent starts
produced one durable row; terminal duplicates replayed the stored response;
same-workspace hash conflicts failed closed; different workspaces remained
isolated.

The model unit suite covered deterministic fake replay, retryable and
non-retryable failures, timeout/output budgets, stable provider lookup,
OpenAI-compatible serialization and streaming, Retry-After parsing, quota
classification, credential redaction, Ollama embedding compatibility,
pre-content fallback, and no fallback after partial content.

## Measured cost and performance

Measurements were taken on the local Docker-backed development stack on 19
August 2026:

- Provider-neutral fake gateway benchmark: 0.95–0.97 µs/op, 776 B/op, and 11
  allocations/op over five benchmark runs. This excludes HTTP, Postgres, and
  provider network time.
- Fifty sequential authenticated fake-provider HTTP calls, including the
  durable ledger: p50 2.254 ms, p95 8.152 ms, maximum 21.854 ms. This is a
  local development measurement, not a capacity guarantee.
- Each simple ledger row occupied approximately 2.3 KiB with bounded request
  and response evidence. The relation plus indexes occupied 408 KiB for the
  50-call measurement set; index pages were 16 KiB each at this small scale.
- A normal successful call performs one transactional reservation/read, one
  attempt update, and one terminal update. A durable duplicate performs one
  conflict/read transaction and does not call the provider. Retries remain in
  the same ledger row and increase attempt count rather than multiplying
  durable records.

## Remaining limitations

- There is no durable agent loop, tool/sandbox runtime, approval gate, or
  public model-run orchestration API yet.
- A process crash after an upstream request is transmitted and before terminal
  persistence can cause an upstream duplicate. Provider idempotency keys are
  sent where supported, but arbitrary providers do not provide exactly-once
  semantics. An in-flight ledger row currently fails closed until an operator
  or a later recovery policy resolves it.
- Credentials are configuration-scoped and OpenAI is opt-in; tenant-aware
  identity, scoped credentials, rotation, and revocation remain future work.
- Pricing comes from endpoint configuration and is not a provider invoice.
- Evidence is bounded Postgres JSONB. Large raw prompts, tool outputs, and
  artifacts still need the later content-addressed artifact layer.
