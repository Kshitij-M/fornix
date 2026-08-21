# Model gateway foundation

Status: implemented for Loop 8.

## Scope

This slice adds a provider-neutral model execution boundary without starting
an agent loop. It makes the existing Ollama embedding path a registered
provider capability, adds an opt-in OpenAI-compatible chat provider, and adds a
deterministic fake provider for tests and offline development. Model execution
is still an explicit caller action; retrieval remains model-free by default.

## Invariants

- Every model request has an explicit workspace, provider, model, request
  identity, idempotency key, and bounded execution budget.
- Provider lookup is explicit, case-normalized, alias-safe, and deterministic.
- A provider may not receive a credential through the request contract. The
  provider resolves its credential from configuration at call time.
- Raw provider request and response evidence is redacted and bounded before it
  is persisted. Authorization headers, API keys, bearer tokens, and secret
  fields never enter logs, events, or the database.
- A duplicate idempotency key cannot start a second durable model call. A
  completed duplicate returns the stored response; an in-flight duplicate
  fails closed instead of issuing a second external request.
- Remote model execution is at-least-once at the network boundary when a
  process dies after transmission and before persistence. Provider idempotency
  keys are sent where supported, and this limitation is explicit rather than
  being described as exactly-once execution.
- Retry decisions use stable failure codes, not human-readable error text.
  Only explicitly retryable failures are retried, with bounded deterministic
  backoff.
- Provider fallback is allowed only before visible content has been emitted.
  A partial response is never silently replayed through another provider.
- Usage, cost, latency, attempt count, provider request ID, terminal outcome,
  and evidence references are durable and workspace-scoped.
- The event store and evidence store remain authoritative. Model-call rows are
  an attributable execution ledger and are not a replacement for raw events.

## Schema changes

Migration `011_model_calls.sql` adds `fornix.model_calls`; additive migrations
`012_model_call_timing_metadata.sql` and `013_model_call_identity.sql` add
metadata/timing and first-class schema/causation/correlation identity without
changing the checksum of the original ledger migration. Together they provide:

- workspace, request, idempotency, and request-hash identities;
- provider/model and actor/task/session references;
- lifecycle status, attempt count, content-emitted flag, and provider request ID;
- bounded redacted request/response JSON evidence;
- usage, cost, failure, timing, and timestamps.

The unique workspace/idempotency key prevents duplicate durable model-call
records. A request hash mismatch on key reuse fails closed. Model-call rows are
mutable execution metadata, not authoritative event history; terminal records
retain the evidence needed to audit the attempt.

## Reference scan and reuse decisions

Repositories/files searched:

- DeepSeek Harness `packages/llm/llm`, `llm-retry`, `token-meter`,
  `credentials`, `llm-deepseek`, and session persistence;
- Orloj `resources/model_endpoint.go`, `runtime/model_provider_registry.go`,
  `runtime/model_gateway_router.go`, OpenAI/Ollama gateways, and model errors;
- agentmemory provider registry, OpenAI adapter, fallback chain, resilient
  wrapper, and circuit breaker;
- current Fornix Ollama embedding, router observations, event store, evidence
  store, configuration, migrations, and HTTP server.

Closest behaviors:

- DeepSeek's provider-neutral vocabulary, stable failure codes, per-operation
  credential resolution, token usage projections, and retry events;
- Orloj's explicit provider registry, endpoint normalization, provider
  routing, OpenAI-compatible wire format, and no-fallback-after-content rule;
- agentmemory's provider fallback and circuit-breaker boundaries.

Adaptation:

- Reimplement the contracts in Go under `internal/contracts` and the runtime
  under `internal/model`.
- Keep Postgres as the authority instead of adding a broker or cache.
- Keep retry attempts inside one durable model-call record; future agent-loop
  events can reference it without changing this boundary.
- Use a deterministic backoff by default. Jitter is deliberately deferred
  until there is a durable randomness/replay policy.

License/provenance:

- DeepSeek Harness is MIT; ClawMem and FornixDB are MIT; agentmemory and Orloj
  are Apache-2.0. This slice independently reimplements behavior and copies no
  source.
- Kronaxis Fabric is BSL-1.1 and is not used as a source-code dependency.
- Fornix remains MIT. No third-party source notice is required for this
  independent implementation; any future copied source must retain its own
  notice.

## Cost and efficiency budget

- No model call is introduced on retrieval's deterministic hot path.
- Gateway overhead target is under 5 ms p95 excluding provider/network time.
- Request and response evidence is capped at 1 MiB per call and is redacted
  before persistence.
- Default request timeout is 30 seconds, input is bounded at 1,048,576
  estimated tokens, output at 8,192 tokens, and total estimated request/
  response bytes at 4 MiB. Callers can set stricter limits but cannot exceed
  the global caps.
- Retry defaults to one initial attempt plus two retries for rate-limit,
  timeout, transport, and server failures, with a maximum 10-second delay.
- Cost is calculated from usage and endpoint pricing when configured; unknown
  pricing remains explicitly unknown instead of being fabricated.
- The database adds one bounded ledger row per idempotent model request plus
  bounded redacted evidence; provider response bodies are never duplicated in
  control-event payloads by this slice.

## Acceptance tests

- Contract normalization, request hashing, stable failure classification, and
  redaction tests.
- Provider registration, alias collision, deterministic lookup, and listing
  tests.
- Fake-provider deterministic response and replay tests.
- OpenAI-compatible JSON request/response, usage, auth-header, and streaming
  tests using `httptest.Server`.
- Ollama embedding compatibility tests using `httptest.Server`.
- Retry, non-retryable failure, bounded timeout, and fallback tests.
- Partial-content fallback prohibition tests.
- Concurrent duplicate model-call submissions producing one row and one fake
  provider effect.
- Request-hash conflict and credential-leak tests.
- Fresh/existing migration tests and all existing Go, smoke, and CI checks.

## Remaining limitations after this slice

- There is no durable agent loop, tool runtime, approval system, or public
  model-run API yet; those belong to later loops.
- A process crash after an external request is transmitted but before the
  ledger is finalized can produce an upstream duplicate unless the provider
  honors the idempotency key.
- Pricing is configuration data, not a billing guarantee. Provider invoices
  remain authoritative.
- Credentials are still configuration-scoped rather than tenant/RBAC-scoped;
  the existing shared bearer-key limitation remains until the identity loop.
