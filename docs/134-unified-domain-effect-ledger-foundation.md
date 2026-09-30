# Fornix unified domain-effect ledger foundation

Status: Task 56 feature note. This note defines the smallest safe bridge from
Fornix's specialized domain ledgers to the shared durable effect dispatcher.
It is a design and implementation contract, not a claim that remote systems
provide exactly-once execution.

## Problem and decision

Fornix already has durable detail ledgers for repository-change applications,
tool runs, model calls, agent-run transitions, and the HTTP/MCP operator
surface. Task 55 added the generic effect dispatcher, but only the generic HTTP
connector and incident remediation path use it. The remaining paths could still
reach a filesystem, process, or provider before a common operation/effect
identity was visible to Postgres.

Task 56 introduces one bounded `EffectLink` contract and one append-only
workspace-scoped link table. The generic `operation_effects` row remains the
pre-dispatch authority. Each specialized ledger remains the authoritative
domain detail record. The link is a verifiable cross-reference, not a second
effect ledger and not a replacement for model usage, tool output, change
verification, or agent checkpoints.

The required ordering for effectful domain work becomes:

```text
authenticated domain intent
  -> generic operation/admission/effect reservation
  -> domain-ledger reservation/link
  -> live operation/task/credential/catalog validation
  -> provider, process, or filesystem boundary
  -> domain-ledger result/evidence/artifact
  -> effect reconciliation and link finalization
```

MCP remains a compatibility transport. It does not gain a separate effect
authority: mutating MCP tools must continue to reach the same authenticated
HTTP/domain path and therefore the same dispatcher and link records.

## Invariants

1. Every link is scoped by workspace, generic operation ID, effect ID, domain
   kind, and specialized record identity. Cross-workspace links fail closed.
2. A link references an existing generic effect and an existing specialized
   record. It stores hashes and bounded metadata only; it never copies prompts,
   credentials, tool arguments, filesystem bytes, or provider payloads.
3. Link identity is immutable. Repeating the same delivery returns the same
   link; reusing its idempotency key with a different effect, domain record, or
   hash fails closed.
4. The generic effect reservation is the common before-call gate. A specialized
   ledger must not be treated as permission to bypass it in production
   composition.
5. Domain details remain authoritative for their own lifecycle: model calls
   own measured/estimated usage, tool runs own bounded output, change
   applications own filesystem verification, and agent runs own checkpoints.
6. Every link carries the exact operation fence, task fence, credential lease
   facts, provider idempotency support, and delivery guarantee recorded by the
   generic effect. A stale fence cannot create or finalize a link.
7. A process crash after a provider or filesystem boundary may leave a generic
   effect in `dispatching` or `recovery_required`, a domain record in its own
   terminal/recovery state, or both without a link. A bounded reconciler links
   matching hashes; it never invents success and never blind-retries an
   uncertain external effect.
8. Replay reads the generic effect, link, specialized record, evidence, and
   artifact hashes. Replay never calls a provider, process, filesystem
   mutation, or credential resolver.
9. At-least-once external delivery remains explicit. Provider idempotency keys
   and provider-specific reconciliation are required where supported; Fornix
   never claims exactly-once remote execution.

## Domain mapping

| Domain | Specialized authority | Link identity | External boundary |
| --- | --- | --- | --- |
| Repository change | `change_applications` | application ID + packet/result hash | configured filesystem mount |
| Tool | `tool_runs` | tool-run ID + result hash | structured process execution |
| Model | `model_calls` | model-call request ID + response/failure hash | model provider |
| Agent loop | `agent_runs` and transition/checkpoint history | run ID + step/request hash | delegated model/tool step |
| MCP | HTTP/domain record reached by shim | underlying domain identity | none in the shim itself |

The agent-run record is the orchestration authority. Individual model/tool
effects are linked to their own domain records and retain the agent run as a
causation/correlation reference; the run checkpoint is never replaced by the
link table.

## Schema and migration

Migration `054_domain_effect_links.sql` adds
`fornix.domain_effect_links` with:

- workspace, link ID, operation ID, effect ID, and idempotency key;
- domain kind, record ID, record hash, request hash, and result hash;
- operation/task owner and fence facts;
- credential lease/source facts and provider request identity;
- delivery/verification status, provenance references, and bounded metadata;
- created/reconciled timestamps and append-only transition history.

The table uses foreign keys to the generic operation/effect rows where the
current schema permits them, workspace-local unique identities, bounded JSON
metadata, and append-only transitions. Existing specialized rows are not
rewritten. Backfill is explicit, bounded, dry-run capable, and refuses to
guess missing effect identity.

The migration runner must preserve checksum immutability. A database created
from an earlier incompatible operation migration must be upgraded through a
forward-only repair migration or explicitly reseeded after backup; no checksum
is silently rewritten.

## Reuse and licensing

The implementation reuses the existing operation/effect stores, authority
facts, artifact/evidence hashes, model/tool/change/agent ledgers, and replay
contracts. It follows Orloj's execution/checkpoint discipline, DeepSeek
Harness's prepared-call and no-retry-after-content boundary, agentmemory's
lease/recovery patterns, and FornixDB's immutable hash/provenance model as
architectural references only. No Kronaxis-fabric source is copied because it
is BSL 1.1. Fornix remains MIT-licensed; future dependencies require a
license review before adoption.

## Cost and performance budget

The link adds one bounded indexed row and append-only transition rows per
effectful domain invocation. The dispatcher adds no network hop or service.
Expected overhead is one or two Postgres writes/reads per domain effect plus
bounded reconciliation queries. The link must not store raw outputs or
credentials. Qualification must measure p50/p95 dispatch-plus-link latency,
SQL statements, row growth, duplicate-hit rate, reconciliation backlog, and
storage per effect. These are measurements to collect, not SLO claims.

## Acceptance tests

- fresh databases apply migration 054 and existing compatible databases migrate
  without rewriting history;
- duplicate link submissions produce one link and one append-only outcome;
- conflicting domain/effect/request hashes fail closed;
- stale operation, task, credential, or workspace facts cannot create or
  reconcile a link;
- generic effect reservation precedes every production model, tool, change,
  and agent-bound effect callback;
- specialized ledger records and generic effects remain linked after success,
  failure, partial filesystem application, and uncertain provider outcome;
- crash before link commit is retryable without a second external dispatch;
- crash after domain completion but before link commit is reconciled by hash,
  not re-executed;
- replay from sequence zero and from a checkpoint produces stable hashes;
- model usage/cost, tool artifacts, change receipts, and agent checkpoints
  remain authoritative and workspace isolated;
- MCP mutation calls have the same link and authorization semantics as HTTP;
- no credentials, raw prompts, argv secrets, or arbitrary provider payloads
  appear in links, events, errors, metrics, or reports;
- unit, integration, race, smoke, CI, redaction, and migration checks remain
  green.

## Explicit scope boundary

This loop does not make a local process a kernel sandbox, make a filesystem
atomic, or make a remote API exactly-once. It creates the common durable
identity and recovery boundary needed to qualify those domain-specific
semantics and makes any uncertain outcome visible to operators.
