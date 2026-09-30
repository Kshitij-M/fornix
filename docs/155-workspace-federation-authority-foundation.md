# Workspace-scoped federation authority foundation

Status: implemented feature note; see [`156-loop-66-completion.md`](156-loop-66-completion.md)
for qualification evidence and remaining deployment limitations.

## Problem and boundary

Fornix's historical federation path predates workspace authority. It stores
peer URLs and raw bearer tokens in a global table, polls peers with direct SQL
and `http.Client` calls, and imports into the global coordination table. That
path cannot be made safe by adding an ordinary workspace permission: ownership
is absent from the rows, credentials are not managed references, and a lost
remote response has no durable recovery identity.

Task 66 introduces the smallest production-quality replacement:

- workspace-scoped peer registrations containing a logical credential
  reference, never secret bytes;
- append-only, idempotent peer commands and typed events;
- a per-peer durable poll lease with monotonic fencing;
- a durable poll-attempt identity and explicit recovery-required state;
- controlled egress and exact credential-lease use immediately before the
  remote read;
- bounded import into the workspace coordination authority with idempotent
  remote-message keys; and
- strict authority-identity qualification for the independently packaged
  fake-incident effectful adapter.

The old global federation tables remain historical/quarantined. Task 66 does
not infer ownership for existing rows, copy raw tokens, or silently migrate
them into a workspace.

## Invariants

1. Every peer has one workspace, one remote workspace identity, one bounded
   HTTPS/HTTP endpoint, and one parsed credential reference.
2. A peer URL is an allowlisted destination, not caller-controlled arbitrary
   egress. Redirects, private networks, request bytes, response bytes, and
   wall time are bounded by `connector.NewEgressClient`.
3. Secret material is resolved only through `credentials.LeaseResolver` for
   the immediate request. It never enters a peer row, event, error, log,
   checkpoint, artifact, or response.
4. Only one poll owner may hold a peer lease. Takeover increments a monotonic
   fence; stale owner/fence pairs fail closed before import or finalization.
5. Poll attempts are idempotent by workspace, peer, and starting sequence.
   Duplicate delivery returns the durable attempt and never performs a second
   import effect after completion.
6. A remote response that cannot be classified as received is
   `recovery_required`; the system does not claim success or blindly classify
   an unknown external boundary as a transport failure.
7. A received response is bounded, hash-recorded, and imported through the
   workspace coordination store. Remote messages use a deterministic
   peer/remote-sequence idempotency key, so a crash after import but before
   attempt finalization is replay-safe.
8. Imported messages and poll attempts carry actor, request, causation,
   correlation, peer, response-hash, and provenance metadata without raw
   response bodies.
9. Peer configuration changes are append-only commands over a current
   workspace projection. A new configuration revision never overwrites
   historical command/event evidence.
10. The fake-incident adapter must preserve the generic dispatcher's reserved
    effect ID when executing with authority, just as the HTTP adapter does.
    No adapter may create a second local effect identity.

## Schema and migration

Migration 062 adds workspace-scoped tables for:

- current peer configuration and bounded revision metadata;
- append-only peer commands with request-hash/idempotency uniqueness;
- per-peer ownership leases and monotonic fences; and
- append-only poll attempts with state, response hash, provider request
  identity, credential-lease facts, imported count, and recovery metadata.

All tables live under `fornix`, include `workspace_id`, have fail-closed RLS,
and are indexed by workspace/peer/idempotency and lease state. No historical
`fabric.federation_peers` or `public.coord_messages` row is updated.

## Poll and crash semantics

The poll sequence is:

```text
workspace authorization
  → peer row + lease/fence acquisition
  → idempotent poll-attempt reservation
  → exact managed credential lease resolution
  → controlled bounded remote GET
  → response hash and bounded decode
  → fenced workspace coordination import
  → fenced attempt completion and lease release
```

If the process crashes before the remote request, the reserved attempt has no
external effect and is safely retryable. If the process loses the response,
the attempt is `recovery_required`; no success is written. If the response was
received and local import committed but finalization crashed, deterministic
message idempotency prevents a second local effect on replay. If a peer lease
expires, a takeover receives a higher fence and stale workers cannot import or
complete the old attempt.

## Reuse and licensing

The implementation reuses Fornix's typed workspace contracts, `EventStore`,
`WorkspaceCoordinationStore`, credential lease interfaces, `CredentialLeaseStore`,
destination policy, controlled egress client, and universal effect authority.
It copies no source. Reference repositories inform architecture only; in
particular, Kronaxis Fabric source is not copied because its BSL 1.1 license
is incompatible with direct reuse. New code remains under Fornix's MIT
license.

## Efficiency and cost budget

- Peer registration is one bounded workspace transaction and one command/event
  row per accepted command.
- Poll selection locks one peer row and one lease row; no global table scan is
  permitted.
- Each poll has a hard response byte limit, message limit, timeout, and
  import limit. The default path performs no model or embedding call.
- Duplicate poll delivery performs indexed idempotency reads and zero remote
  work after a terminal attempt is known.
- The durable cost is one current peer row, append-only command history, one
  lease row, and one bounded attempt row per poll identity. Raw remote bodies
  are not stored.
- Production deployments must measure remote latency, SQL statements per poll,
  imported-message rate, recovery rate, and storage per retained attempt
  before setting SLOs.

## Acceptance tests

- fresh and existing databases apply migration 062 cleanly;
- peer registration is workspace isolated, idempotent, conflict-safe, and
  redacted;
- raw bearer-token fields are rejected and never persisted;
- lease acquisition, renewal, expiry, takeover, release, and stale-fence
  rejection are transactional;
- controlled egress rejects an unallowlisted host, redirect escape, private
  address, oversized response, and timeout;
- exact credential lease scope/fence is required before remote dispatch and is
  released afterward;
- duplicate poll requests and duplicate remote messages produce one local
  coordination effect;
- crashes before request, after response, during import, and before attempt
  finalization are recoverable and replayable;
- uncertain remote outcomes remain `recovery_required` until explicitly
  reconciled;
- cross-workspace peer, lease, attempt, and import access fails under RLS;
- fake-incident effectful execution preserves the reserved generic effect ID;
- full offline, race, disposable-Postgres, role-separated, docs, package,
  and smoke checks remain green.

## Explicit non-goals

Task 66 does not infer historical peer ownership, provide exactly-once remote
execution, or add a broker, Redis, NATS, object store, or new service. A
remote GET is safe to repeat, but any future remote write must use the generic
effect dispatcher and provider idempotency contract rather than this poll
path.
