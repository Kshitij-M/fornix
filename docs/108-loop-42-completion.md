# Loop 42 completion: generic operation queue and worker claims

Status: implemented as a durable queue-claim foundation; background worker
loops, provider dispatch, external-effect verification, HA, and global
fairness qualification remain open.

Delivered:

- bounded deterministic `OperationStore.ClaimReady` selection using PostgreSQL
  `FOR UPDATE SKIP LOCKED`;
- workspace-scoped selection ordered by due time, creation time, and operation
  ID, with active-lease exclusion and expiry takeover;
- reuse of monotonic operation lease fencing in the same transaction as queue
  selection;
- explicit exclusion of `awaiting_external` from generic claiming so uncertain
  provider effects cannot be silently redispatched;
- authenticated `POST /v1/operations/claims` and `fornix operation claim`
  surfaces;
- concurrent-owner, expiry/takeover, bounded, duplicate, and workspace-scoped
  integration coverage;
- architecture and API documentation with the exact execution boundary.

Local qualification passed against the migrated Postgres authority for the
store and HTTP claim paths, including concurrent claimers where one operation
produced one active owner and takeover advanced the fence. No model, tool,
connector, broker, provider, or external effect is invoked by a claim.

This loop makes work discoverable and fenced; it does not itself execute work.
An adapter worker must still lease, plan, execute, persist results, and use the
independent effect-reconciliation path for at-least-once external effects.
