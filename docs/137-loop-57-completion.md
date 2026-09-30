# Loop 57 completion — universal domain-effect dispatch

Status: implemented as an additive, qualified vertical slice on the current
feature branch. This closes the first server-composition pass for model calls,
structured tools, and repository-change application. It does not close the
universal production roadmap or claim that every adapter and every agent path
is production-qualified.

## What changed

- Added `effectdispatch.Runtime` and `ChildRequest` as the deterministic
  composition seam for dynamic domain effects. Each child operation derives a
  stable workspace-scoped identity, creates a one-step plan, acquires a
  monotonic operation lease, attaches the task fence when present, and enters
  the common dispatcher before invoking a provider, process, or filesystem
  callback.
- Made `Dispatcher.Dispatch` fail closed unless a typed
  `DomainEffectLink` is supplied. Generic operation/effect history and the
  specialized domain ledger therefore cannot silently diverge at the
  composition boundary.
- Wired the server model gateway, tool executor, and repository-change service
  to the child runtime. Existing specialized stores remain authoritative for
  model usage, tool output/approval, and verified change facts; generic
  operation results contain bounded hashes and references only.
- Propagated task owner/fence facts into model requests and child dispatches.
  Stale operation or task owners fail before the external callback.
- Generic workflow effect approval is plan-bound: only succeeded approval
  ancestors can authorize a write, the requester cannot self-approve those
  gates, and the durable workflow decision is mirrored idempotently into the
  generic admission record before dispatch. Adapter-supplied approval flags
  are not treated as authority.
- Preserved streaming sink behavior and the existing no-retry-after-content
  rule. Provider fallback remains disabled once content has been emitted.
- Added migration 055. Model calls and tool runs can now record
  `recovery_required` when a provider/process outcome may have crossed its
  boundary. A duplicate recovery request fails closed and is not treated as a
  safe retry. The state is intentionally non-terminal until a future
  domain-specific reconciler resolves it.
- Added unit coverage for generic child-effect replay, uncertain model/tool
  outcomes, and no-second-invocation behavior. Existing dispatcher, model,
  tool, change, server, agent-loop, and store suites remain green.

## Authority and failure semantics

```text
specialized request
  → durable child operation and plan
  → fenced operation lease
  → admission and generic effect reservation
  → typed domain-effect link
  → live authority revalidation
  → provider/process/filesystem callback
  → specialized ledger + generic hash result
```

The external callback is at-least-once. A crash or transport failure after the
boundary may leave the generic effect and specialized ledger in
`recovery_required`; the runtime must not invent success or issue a blind
duplicate. Provider idempotency is recorded as a fact where known, not claimed
universally.

## Qualification evidence

All disposable PostgreSQL instances were isolated from the persistent
`fornix-dev-db-1` database and removed after each run. No new service or
broker was introduced.

| Check | Result |
| --- | --- |
| Offline full Go suite | Passed: `go test ./... -count=1` |
| Recovery-state unit tests | Passed: model and tool uncertain outcomes persist and replay fail closed |
| Fresh PostgreSQL migrations through 055 | Passed |
| Existing-schema upgrade from pre-055 status constraints | Passed |
| Full PostgreSQL-backed Go suite | Passed |
| Child runtime duplicate/replay qualification | Passed |
| Dispatcher mandatory-link qualification | Passed |
| Model/tool/change/server/store focused suites | Passed |
| Repository race suite on disposable PostgreSQL | Passed |

The slice adds one forward-only migration and bounded child operation,
attempt, effect, and link rows per effectful domain invocation. It adds indexed
Postgres reads and writes before and after the external callback but no network
hop. Exact p50/p95/p99 production latency, WAL, pool saturation, and storage
growth remain deployment measurements rather than local SLO claims.

## Critic and remaining limitations

1. The generic operation/link and specialized domain reservation are not one
   SQL transaction for every domain. The current stores expose separate
   transaction seams; the dispatcher link is therefore a mandatory pre-dispatch
   reference and reconciliation remains explicit. A future composition API must
   add atomic `BindTx` integration without dynamic SQL or guessed source rows.
2. Embedding calls still use the provider `Embed` interface directly and need
   the same effect-authority boundary before production claims extend to vector
   generation.
3. `recovery_required` is now explicit for model/tool ledgers, but the bounded
   provider/process-specific reconciliation adapters and atomic
   specialized-ledger/link transaction seam are still open. Recovery remains
   an operator-visible stop, not an automatic retry.
4. Embedding calls still use the provider `Embed` interface directly and need
   a scoped durable embedding-call ledger before production claims extend to
   vector generation.
5. Local-process execution remains bounded argv execution, not a kernel
   sandbox. Repository changes can be partially applied across a multi-file
   packet and require verification; filesystem atomicity is not claimed.
6. No OpenAI or other live-provider key was used in qualification. The fake
   provider is the deterministic default; live-provider idempotency, billing,
   outage, and response verification require separate opt-in qualification.

## Next task prompt

```text
Task 59 — Build Fornix's scoped embedding-call authority and provider boundary.

Read AGENTS.md, docs/00-fornix-foundation.md,
docs/13-reference-reuse-matrix.md, docs/14-production-readiness-qualification.md,
docs/24-retrieval-context-foundation.md, docs/28-model-gateway-foundation.md,
docs/136-universal-domain-effect-dispatch-foundation.md,
docs/138-agent-run-recovery-authority-foundation.md, and
docs/139-loop-58-completion.md. Study every current embedding caller,
internal/model/provider.go, the Ollama provider, model-call storage,
effectdispatch.Runtime, AdmissionStore effect leases, ingestion checkpoints,
retrieval budgets, and workspace/RBAC boundaries.

Write a feature note first. Add typed scoped embedding contracts and a
workspace-scoped durable embedding-call ledger. Put Ollama embedding behind a
deterministic registry/gateway and route calls through the same fenced
child-operation dispatcher and typed domain-effect link before any provider
callback. Propagate actor, workspace, causation/correlation, task owner/fence,
and agent-run scope through ingestion, retrieval, memo writes, and backfills.
Make duplicate requests replay one call record, retry only explicit transient
failures, and never blindly repeat an uncertain external call. Gate embedding
work by provider availability, measured need, and explicit budgets so offline
ingestion remains usable. Add fresh/upgrade migrations, crash/concurrency/
stale-fence/duplicate/workspace-isolation/replay tests, CI, smokes, and
measured latency/SQL/storage/recovery-backlog reports. Keep Postgres as the
authority, preserve at-least-once semantics, and add no broker, Redis, NATS,
object store, LLM framework, or exactly-once claim.
```
