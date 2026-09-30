# Loop 56 completion — unified domain-effect ledger bindings

Status: implemented as an additive, qualified vertical slice on the current
feature branch. This closes the shared relationship and generic-path binding
slice; it does not yet claim that every model, tool, repository-change, or
agent-loop invocation is dispatcher-backed.

## What changed

- Added the typed `contracts.DomainEffectLink` and
  `contracts.DomainEffectLinkTransition` contracts. They carry only bounded
  identities, hashes, workspace/actor/correlation metadata, fence snapshots,
  provider-idempotency facts, and credential/catalog references. Secret bytes,
  prompts, commands, and provider payloads are excluded.
- Added migration 054 with workspace-scoped immutable
  `fornix.domain_effect_links` and append-only transition history. Foreign
  keys bind every link to an existing generic operation and effect. Unique
  source/attempt/effect identities make duplicate delivery deterministic while
  permitting a new generic attempt to be recorded for a retry.
- Added `DomainEffectLinkStore.Bind`, `BindTx`, `Get`, `GetByDomain`, and
  bounded `List` APIs. Binding verifies the operation hash, effect reservation
  hash, request hash, effect class/boundary/delivery facts, catalog and
  credential-source facts, task fence, and live operation lease in Postgres.
- Added append-only link transitions so later reconciliation does not mutate
  the original relationship row.
- Extended the durable effect dispatcher with an optional domain-link binding
  hook. The dispatcher derives generic identity and authority facts from the
  authoritative Postgres effect; callers can supply only domain kind, domain
  ID/hash, role, and link idempotency.
- Integrated the generic HTTP operation path and the fake incident remediation
  path with the link hook. The incident path continues to verify its durable
  domain approval before dispatch. MCP remains a transport shim and inherits
  these server semantics.
- Added contract, Postgres binding, duplicate, conflict, workspace-isolation,
  dispatcher ordering, and replay tests.

## Qualification evidence

The disposable pgvector-backed Postgres instance used for this qualification
was `fornix-task56-db` on port 55456 and is not a development database. It was
removed after the run; no persistent development container or migration
history was modified.

| Check | Result |
| --- | --- |
| Offline contract/store/effect-dispatch compilation | Passed |
| Link normalization and stable hash tests | Passed |
| Fresh migration through 054 | Passed |
| Link binding to existing operation/effect | Passed |
| Duplicate link delivery | Passed; original link identity returned |
| Conflicting domain/effect identity | Passed; rejected |
| Cross-workspace link inspection | Passed; no data returned |
| New-table RLS installation | Passed; both 054 tables have fail-closed workspace policies |
| Dispatcher binds before external invocation | Passed |
| Dispatcher duplicate delivery | Passed; no second external invocation |
| Incident workflow regression | Passed |

The migration adds one relationship row and one transition row per bound
domain effect, plus three bounded indexes. The binding path adds one short
Postgres transaction before the dispatch-intent transaction. Exact p95
latency and relation-growth numbers still require the repository's repeatable
capacity qualification against the intended deployment topology.

## Critic and remaining limitations

1. Model calls, local tools, repository-change filesystem application, and
   agent-loop model/tool steps still have specialized execution paths that are
   not universally forced through a generic operation/effect reservation.
   They remain protected by their existing idempotency, approval, and fence
   semantics, but this is the next production-critical slice.
2. `DomainEffectLinkStore` cannot validate a polymorphic source row by table
   name. Domain adapters must create the specialized row and call `BindTx` in
   their transaction; the generic store intentionally does not execute
   dynamic SQL or accept an arbitrary table identifier.
3. Binding and generic effect reconciliation are still separate from an
   external provider/process/filesystem call. Unknown outcomes remain
   at-least-once and require provider-specific reconciliation; exactly-once
   remote execution is not claimed.
4. Link transition APIs are prepared by the schema and contract, but the
   complete provider-result reconciliation service and bounded backfill are
   still open.
5. The persistent `fornix-dev-db-1` database may fail closed on an older
   migration-035 checksum. That is an existing migration-history incompatibility;
   migration checksums were not rewritten. Operators must use a forward-only
   repair/reseed procedure with backup evidence.

## Next task prompt

```text
Task 57 — Make model, tool, repository-change, and agent-loop effects
dispatcher-backed end to end.

Read AGENTS.md, docs/00-fornix-foundation.md,
docs/128-effectful-adapter-authority-foundation.md,
docs/132-durable-effect-dispatcher-foundation.md,
docs/134-unified-domain-effect-ledger-foundation.md, and this completion note.
Study the current ModelCallStore/Gateway, ToolRunStore/Executor,
RepositoryChangeStore/Service, AgentLoop/Scheduler, operation worker, and MCP
transport. Re-check Orloj execution safety, DeepSeek Harness prepared-call and
no-retry-after-content rules, agentmemory lease/checkpoint recovery, and
FornixDB immutable evidence/artifact patterns without copying incompatible or
non-MIT source.

Write a feature note first. Add deterministic child operations for dynamic
agent model/tool steps and make the generic effect reservation plus
DomainEffectLink the mandatory pre-dispatch gate for model calls, structured
tools, repository-change filesystem application, and agent-owned steps.
Preserve specialized ledgers as authoritative detail and use BindTx where
local reservation and link creation can commit atomically. Revalidate the
operation lease, task fence, agent-run lease, credential lease, schema catalog,
and controlled egress immediately before and after every external boundary.

Add explicit uncertain/recovery states and bounded reconciliation; never blind
retry an effect that may have crossed a provider/process/filesystem boundary.
Route HTTP, CLI, agent, and MCP mutations through the same composition layer.
Add duplicate/concurrency/stale-fence/expiry/crash/replay/workspace tests and
provider-idempotency tests. Run fresh and upgrade migrations, offline tests,
race tests, CI, all relevant smokes, and measured latency/SQL/storage runs.
Keep Postgres as the only authority, preserve at-least-once disclosures, and
do not add a broker, Redis, NATS, object store, LLM framework, or exactly-once
claim.
```
