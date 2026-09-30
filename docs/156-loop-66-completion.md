# Loop 66 completion: workspace federation authority and effect identity

Status: implemented and qualified on the Task 66 branch; this is a
qualification slice, not a production-readiness declaration.

## Delivered

- Added typed workspace-scoped federation contracts for peer configuration,
  append-only commands, peer leases, poll requests, and poll attempts.
- Added migration 062 with current peer state, append-only peer commands,
  monotonic peer leases, bounded poll attempts, indexes, and fail-closed RLS.
- Added transactional `FederationStore` operations for peer registration,
  idempotency/conflict detection, lease acquisition/renewal/release/validation,
  poll reservation, dispatch marking, fenced completion, and recovery-required
  transitions.
- Retained released lease rows at their latest fence so a later acquisition
  cannot restart at fence 1 or reuse an authority event identity.
- Added the bounded federation poller. It resolves a logical credential
  reference through the injected lease authority, validates its scope and
  expiry, uses the shared controlled egress client, caps the remote response,
  hashes provider output, and imports messages through the workspace
  coordination authority.
- Added deterministic peer/remote-sequence idempotency keys. A crash or
  duplicate response cannot create a second local coordination event.
- Added explicit `recovery_required` handling for unknown remote outcomes;
  remote response bodies and raw credential bytes are not persisted or
  returned in errors.
- Replaced ordinary peer registration/listing routes with workspace-scoped
  API handlers. Strict JSON decoding rejects the historical inline
  `bearer_token` field. Historical global federation routes remain
  quarantined.
- Qualified the independently packaged fake-incident effectful adapter to
  preserve the generic dispatcher's reserved effect ID rather than invent a
  second local identity.

## Verification

Against a disposable PostgreSQL + pgvector container using a tmpfs data
directory, the following passed:

- migration application and re-application through the normal embedded
  migration runner;
- typed contract tests for config-hash stability, unsafe endpoint/credential
  rejection, and poll request identity;
- store tests for peer idempotency, append-only event behavior, workspace
  isolation, concurrent lease ownership, takeover fencing, crash rollback,
  duplicate poll completion, and duplicate imported messages;
- poller integration tests using a local bounded HTTP server for authorization,
  URL/path policy, response hashing, secret redaction, deterministic import,
  replay, and recovery-required remote failure;
- server authorization and API tests for workspace-scoped peer routes,
  strict bearer-token rejection, duplicate registration, and cross-workspace
  denial;
- disposable non-owner `NOBYPASSRLS` role qualification showing a scoped peer
  row is visible only inside its transaction-local workspace context;
- fake-incident and connector authority-conformance tests;
- the new `make smoke-universal-federation` target, once supplied an explicit
  disposable `FORNIX_TEST_PG_DSN`.

The test data was held in tmpfs and the disposable container was removed after
qualification. No persistent database volume or host credential was changed.

## Efficiency and storage impact

- Peer writes perform one bounded workspace transaction with one current
  projection row, one append-only command row, and one typed control event.
- Lease operations use one workspace transaction and one primary-key lease
  row. Release expires the row instead of deleting it so fencing remains
  monotonic.
- A poll performs one indexed attempt reservation, one managed credential
  lease, one bounded HTTP read, and one transaction that imports zero or more
  coordination messages and completes the attempt. Duplicate terminal polls
  perform no remote request and no second import.
- Stored remote payload cost is limited to response hashes, provider request
  IDs, imported bounded coordination fields, and lifecycle metadata. Raw
  response bodies and credentials are not stored.
- This slice does not claim production SLOs. Deployments must measure remote
  latency, SQL statements/lock waits per poll, recovery rate, imported-message
  rate, retained-attempt growth, and event storage before setting limits.

## Remaining limitations

- `server.New` does not invent a production credential authority. The poller
  is available as an injected runtime component; without that resolver the
  poll API fails closed with service-unavailable.
- Federation currently implements a bounded remote read. Remote writes still
  require the generic effect dispatcher, provider idempotency, verification,
  and compensation contract.
- Historical global peer rows and global coordination rows are quarantined;
  Task 66 deliberately does not infer workspace ownership or copy raw bearer
  tokens. Operators need an explicit export/re-registration decision.
- Provider-specific remote authentication, certificate/pinning policy,
  multi-region failover, retention/partitioning, and role-separated live
  qualification remain deployment work.
