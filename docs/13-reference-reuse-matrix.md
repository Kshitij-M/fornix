# Reference reuse matrix

Status: active research and licensing reference.

This is the mandatory pre-implementation map. Read source before adopting a
pattern; do not copy code solely because a README names a feature.

| Fornix concern | Reference family | Reuse candidates | Fornix adaptation |
|---|---|---|---|
| Control-plane substrate | Coordination baseline | Postgres schema, pgx pool, capability dispatch, heartbeats, watcher backoff | Split seams, add workspaces, identities, dependencies, leases, and migrations |
| Typed event/state history | Orloj | Event vocabulary, ordered reads, idempotency, transactional checkpoints | Reimplemented in `internal/contracts` and `internal/store`; Postgres remains authority |
| Checkpointed projections | Orloj + agentmemory | Checkpoint commits, replay verification, bounded replay, keyed serialization | Reimplemented in `internal/projection` with derived task state and no broker |
| Consumer ownership and fencing | Orloj + agentmemory | Transactional row locking, explicit owner/fence validation, expiry/takeover, bounded TTLs | Reimplemented in `internal/store` and integrated with projections; Postgres remains authority |
| Memory lifecycle | agentmemory | Observation capture, memory types, budgets, audit trail, worker boundaries | Use Postgres events and typed provenance while retaining private/shared scope |
| Hybrid retrieval | agentmemory + ClawMem | BM25/vector/graph fusion, intent routing, traversal, score explanations | Deterministic escalation first with stage telemetry and hard budgets |
| Reversible disclosure | FornixDB | Gist/detail/source references, supersession, decay, retention tiers | Keep evidence content-addressed and indexes derived |
| Graph/provenance | ClawMem + agentmemory | Typed relations, causal/temporal traversal, stale-edge handling | Bound graph expansion to high-value relations |
| Evidence disclosure | FornixDB + agentmemory | Gist/detail/raw drill-down, source citations, supersession, integrity checks | Immutable Postgres evidence with hard disclosure budgets |
| Task runtime | Orloj | Desired/current state, leases, retries, idempotency, dead letters, replay | Implement as focused Postgres control-plane packages |
| Governance | Orloj | Roles, operation allowlists, approval gates, fail-closed policy | Define workspace and identity contracts before broadening access |
| Validation admission | Orloj + DeepSeek Harness + ClawMem + agentmemory + FornixDB | Explicit capability admission, declarative approval seams, fail-closed evidence gates, lifecycle/audit discipline, bounded budgets | Independently reimplemented as immutable workspace policy packs; registered validators only; exact hashes and Postgres audit/history |
| Observability | agentmemory + Orloj | Structured logs, traces, task history, queue metrics, Prometheus | Instrument cost and stage boundaries before tuning retrieval |
| Code graph | Coordination baseline | Tree-sitter indexing, symbol edges, watcher debounce/backoff | Add repository/version/artifact provenance |

## Source files to study

- Orloj resource contracts, session stores, checkpoint stores, and event buses.
- agentmemory hybrid search, graph retrieval, checkpoint, queue, and replay
  modules.
- ClawMem retrieval gates, scoring, graph traversal, and lifecycle code.
- FornixDB storage tiers, source disclosure, consolidation, and concurrency
  tests.
- The local coordination baseline only for behavior that the current HTTP API
  must preserve; reimplement its seams in Fornix packages.

## License and provenance gate

Reference projects are not license-equivalent. Orloj and agentmemory use
Apache-2.0; ClawMem and FornixDB use MIT. Architecture patterns may be
reimplemented independently. Any source reuse must preserve the applicable
notice and attribution requirements. Fornix itself is released under MIT with
copyright permission confirmed by the project owner.

## Reuse decision template

Every feature note should include:

```text
Reference scan:
- repositories/files searched:
- closest implementation:
- behavior copied or intentionally not copied:
- license/provenance action:
- deterministic fallback:
- benchmark and acceptance threshold:
```
