# Task 58 — agent-run ownership and recovery authority

Status: implemented and qualified as an additive Task 58 vertical slice. The
scope is intentionally limited to direct agent-run ownership, scheduler
workspace enumeration, and the append-only domain-link recovery seam. It does
not close the universal production roadmap.

Task 57 made the common child-operation dispatcher mandatory for the server's
model, tool, and repository-change adapters. The next risk is the orchestration
boundary itself: scheduler-owned agent runs have a fenced lease, but direct
HTTP mutations can currently enter the same run state machine without owning
that lease. A second risk is recovery visibility: the generic effect and
specialized ledger can both say that an external outcome is uncertain without
one bounded, authenticated operator path to inspect and reconcile that state.

This slice tightens agent-run ownership and creates the reconciliation seam.
Embedding calls remain a separate provider capability because the current
embedding consumers do not carry a complete actor/task/session scope; they are
explicitly tracked as the next provider-boundary slice rather than silently
treated as protected.

## Invariants

1. Every direct agent-run mutation that can advance, cancel, wait, complete,
   or execute a model/tool step must hold the current workspace-scoped
   `AgentRunLease` and monotonic fence. Read-only inspection does not require a
   lease.
2. A direct API owner uses the same Postgres lease authority as the scheduler;
   it cannot manufacture or reuse a fence. Concurrent direct and scheduled
   execution has one winner, and the stale owner fails closed before the next
   checkpoint or external effect.
3. Lease renewal and release are bounded and cancellation-aware. A heartbeat
   failure cancels the operation context; the handler does not report success
   after losing ownership.
4. `recovery_required` is not a retry invitation. A recovery read returns
   hashes, workspace/domain identity, provider/process boundary facts, and
   current state only. A future reconcile command must be authenticated,
   workspace-scoped, idempotent, fenced, and append-only.
5. Reconciliation never fabricates provider success. It may record a verified
   outcome, an explicit failure, or leave the effect unresolved. It cannot
   execute a provider, process, filesystem mutation, or embedding call during
   a read-only inspection.
6. Specialized ledgers remain authoritative for model usage, tool output,
   change verification, and agent checkpoints. Generic effect state remains the
   cross-domain recovery authority; links carry hashes and references only.
7. All lease, recovery, and reconciliation records preserve workspace, actor,
   causation, correlation, request, and idempotency identity without prompts,
   credentials, argv, environment values, or raw provider payloads.

## Delivered scope

- Direct HTTP create, advance, cancel, wait, and external-completion paths now
  acquire and release the same workspace-scoped fenced agent-run lease used by
  the scheduler. Renewal failure cancels the callback and prevents a false
  success response.
- The scheduler refreshes a bounded active-workspace inventory and runs one
  explicit workspace-scoped claim transaction per workspace. An empty
  workspace no longer reaches the RLS-sensitive claim query.
- `AgentRunStore.AcquireAgentRunLease` supports idempotent same-owner reuse,
  expiry takeover, monotonic fences, and stale-owner rejection.
- Agent-loop execution backed by the production owned store fails closed when
  a worker lease is absent, including before budget-failure mutation.
- `DomainEffectLinkStore.Transition` and `TransitionTx` append versioned,
  idempotent, hash-only `linked → recovery_required → reconciled` history.
  Stale expected versions and conflicting duplicate identities fail closed;
  the immutable link row is never overwritten.

## Schema and API strategy

The first ownership slice is additive and reuses the existing
`agent_run_worker_leases` table. It adds an explicit acquire-by-run API for
direct operators instead of overloading queue selection. No broker or worker
service is introduced.

Recovery inspection and transition APIs reuse migration 054's typed
`DomainEffectLink` and existing external-effect state history. A forward-only
migration was not required because the existing bounded transition schema
already carries the required version, proof hashes, failure code, actor, and
idempotency identity. Existing generic effects and specialized rows are never
rewritten or guessed into existence.

## Crash and concurrency semantics

- Crash before lease acquisition: no run transition and no external effect.
- Crash after acquisition but before a checkpoint: the lease expires and a
  scheduler takeover receives a higher fence; the old owner cannot commit.
- Crash after a provider/process/filesystem boundary: the model/tool/change
  ledger and generic effect remain `recovery_required` where the outcome is
  unknown; no direct API retries it.
- Concurrent direct requests: one lease row/fence wins; the other receives a
  held/stale error without invoking the loop.
- Duplicate request identity: the authoritative run/event result is replayed;
  lease acquisition does not create a second run or checkpoint.

## Reuse and licensing

The design reuses the existing `AgentRunStore` lease state machine,
`agentloop.WithWorkerLease`, scheduler heartbeat pattern,
`effectdispatch.Dispatch`, `DomainEffectLinkStore`, and Postgres RLS. The
ownership shape follows Orloj's fenced controller ownership; the checkpoint
and recovery boundary follows agentmemory's lease/checkpoint discipline; the
hash-only evidence boundary follows FornixDB's immutable disclosure model.
These are architectural references, not copied source. Kronaxis-fabric source
is not copied because it is BSL 1.1. Fornix remains MIT-licensed.

## Cost and operational budget

Direct mutation adds one short lease transaction, heartbeat writes only while
an operation is active, and one release update. Recovery inspection is bounded
by workspace, cursor, and row limits and performs no external work. The target
is constant bounded SQL per lease command; exact p95 latency, pool wait, lock
time, and recovery backlog must be measured against the deployment topology.

## Qualification evidence

- Offline focused suites for contracts, store, agent loop, scheduler, and
  server passed.
- Fresh disposable pgvector/Postgres migrations and the Postgres-backed store,
  scheduler, and server suites passed.
- The direct lease test covers same-owner reuse, competing-owner rejection,
  expiry takeover, monotonic fencing, and stale validation.
- The domain-link test covers append-only recovery/reconciliation transitions,
  duplicate replay, stale version rejection, projected current status, and
  workspace isolation.
- No persistent development database was touched, and no remote provider was
  contacted during qualification.

## Acceptance tests

- direct create/advance/cancel/wait/complete operations acquire and release a
  durable run lease;
- scheduler and direct API contention has one owner and monotonic takeover;
- stale and expired fences fail before checkpoint or external dispatch;
- heartbeat loss cancels work and prevents a false success response;
- duplicate direct requests replay one run transition;
- recovery inspection is workspace-scoped, bounded, redacted, and read-only;
- recovery transitions are idempotent, fenced, append-only, and never invoke
  a remote model, tool, filesystem mutation, or broker;
- replay from zero remains deterministic after takeover or recovery;
- fresh and existing databases migrate cleanly if a forward-only migration is
  needed;
- unit, integration, race, smoke, CI, documentation, and storage/latency
  checks remain green.

## Explicit non-goals for this slice

Embedding provider calls still require a scope-carrying embedding contract and
durable replay record before they can claim the same authority. The stronger
atomic specialized-ledger/domain-link transaction seam and provider-specific
reconciliation implementations remain follow-up work; this slice provides
the safe ownership and fail-closed control boundary needed to implement them.
