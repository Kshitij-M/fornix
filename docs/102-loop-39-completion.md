# Loop 39 completion: independent external-effect recovery ownership

Status: implemented as a universal alpha recovery foundation; live provider
dispatch, verification, compensation, and deployment qualification remain
adapter-owned work.

Delivered:

- migrations 041 through 043 for workspace-scoped external-effect recovery
  leases, effect-transition integrity, and lossless effect identity;
- explicit `dispatching` state before a provider call, so an uncertain outcome
  is not confused with an unattempted reservation;
- monotonic effect fencing tokens independent from operation leases;
- bounded recovery-candidate listing ordered by committed state time and ID;
- transactional acquire, takeover, renewal, and release;
- effect-state reconciliation using the effect fence, including after a parent
  operation reaches a terminal state;
- unique effect-transition idempotency enforcement and recorded lease
  authority (`operation` or `effect`);
- stale-owner, concurrent-owner, release, workspace, duplicate, and replay
  coverage in store and HTTP tests;
- CLI commands for recovery discovery and effect lease lifecycle;
- public architecture and runbook updates.

The slice makes the uncertain-effect recovery boundary durable. It does not
dispatch a provider request, verify an external system, compensate an effect,
or claim exactly-once execution. Those actions require a domain adapter with
its own provider contract, idempotency, egress, credential, and qualification
evidence.

Measured local qualification:

- migration and store/server tests pass against the local Postgres authority;
- effect recovery adds one indexed lease row per active effect, one recovery
  queue index, and one transaction per lease command;
- candidate reads are bounded by the requested limit and return hashes and
  references only;
- no model, tool, broker, or external network call is made by this slice.
