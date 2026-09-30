# Task 85 — managed-credential and controlled-egress conformance

Status: implementation note for the universal production-qualification slice.

Fornix already has the right authorities for credentials and external effects:
workspace-scoped credential references, fenced leases, opaque source versions,
revocation epochs, controlled HTTP egress, deployment admission, and durable
at-least-once effect reservations. The remaining composition gap is that the
generic operation/effect reservation does not bind the exact egress policy and
network-boundary facts used by the adapter. A process can therefore prove that
it held a credential lease while leaving the destination and boundary decision
implicit in adapter-local configuration.

This slice closes that gap without creating a secret store, proxy, broker, or
second authority. It adds a provider-neutral, hash-only external-boundary
envelope and requires it at the shared effect boundary when strict production
mode is enabled. The envelope is evidence of the policy inputs admitted by
Fornix; it is not a claim that a host, proxy, registry, remote provider, or
network actually behaved as configured.

## Scope and non-goals

In scope:

- a typed, redacted external-boundary authority contract;
- exact egress-policy, destination-policy, and network-boundary hashes on the
  durable effect reservation and authority links;
- fail-closed structural validation and same-transaction consistency checks;
- a deterministic conformance report for effectful adapter composition;
- built-in model, embedding, connector, federation-poll, tool, and change
  paths carrying the boundary envelope where their adapter can prove it;
- explicit strict-production configuration and offline development behavior;
- tests for missing, mismatched, stale-shaped, cross-workspace, and duplicate
  authority facts.

Out of scope:

- storing or resolving secret bytes in Postgres;
- installing or operating a network proxy, firewall, service mesh, or secret
  manager;
- proving DNS, routing, mTLS, workload identity, provider idempotency, or
  remote-side execution from a local hash;
- silently converting an at-least-once external call into exactly-once;
- requiring Ollama, OpenAI, Anthropic, or any other live provider in tests.

## Invariants

1. External-boundary facts are references, not secrets. Only canonical hashes,
   bounded mode names, lease identities, fences, source versions, and expiry
   metadata may cross the control-plane boundary. API keys, bearer values,
   cookies, response bodies, URLs with credentials, and arbitrary provider
   error text are rejected or redacted.
2. A boundary envelope is all-or-nothing. An effect that claims an egress
   policy must carry the matching destination-policy hash, supported network
   boundary mode, and network-boundary hash. Partial envelopes fail closed.
3. Credential facts remain exact. A lease ID is valid only with its current
   fence, revocation epoch, source version, source expiry, workspace, and live
   lease status. A new lease may not be substituted for the one admitted by
   the operation.
4. The effect reservation, authority link, task/operation fences, deployment
   admission, credential facts, and boundary envelope are checked in the same
   Postgres transaction. Process-local caches are hints only.
5. A duplicate reservation must match the original request hash and every
   authority fact. A duplicate with a different boundary, lease, or source
   snapshot is a conflict, never a second external attempt.
6. Read-only and observation paths remain offline and do not need an external
   boundary envelope. Fake providers remain deterministic and do not require
   a credential lease or network access.
7. A conformance inventory proves that a registered effectful adapter exposes
   the required seams. It does not prove hosted network isolation or remote
   provider behavior; those remain deployment-owned qualification gates.
8. Historical authority links remain auditable after a lease expires or is
   revoked. Live dispatch must fail closed, while receipt and replay reads may
   validate the historical row without requiring current liveness.

## Boundary contract

The typed envelope contains:

- `egress_policy_hash`: the normalized hash of the full bounded egress policy,
  including request/response limits, timeout, destination policy, redirects,
  private-network behavior, and proxy allowance;
- `destination_policy_hash`: the normalized hash of the allowed scheme, host,
  path, redirect, and private-network destination policy;
- `network_boundary`: a bounded mode such as `controlled_transport` or
  `deployment_attested`; unknown modes fail closed;
- `network_boundary_hash`: a deployment/process-owned hash of the boundary
  facts used by the adapter. It contains no secret or arbitrary payload.

Credential lease ID, fence, revocation epoch, source version, and source
expiry remain the existing exact lease fields in `EffectAuthority`. The
credential reference itself remains in the durable credential authority and in
capability/provider configuration; the effect envelope does not duplicate or
serialize secret material.

The egress policy hash is derived from `connector.EgressPolicy.StableHash()`.
The destination hash is derived from `DestinationPolicy.StableHash()`. For the
in-process controlled transport, the network-boundary hash is derived from a
bounded mode and the two policy hashes. A deployment may supply a stronger
attestation hash through the same field, but Fornix must not infer that a
process-local hash proves a hosted firewall, proxy, or network namespace.

## Schema and migration

Migration `070_external_boundary_authority.sql` is additive. It adds the
four boundary columns to `fornix.operation_effects`,
`fornix.operation_authority_links`, `fornix.domain_effect_links`, and
`fornix.workspace_federation_poll_attempts`, with empty defaults for
historical rows. It adds pair/completeness and hash-shape constraints plus
narrow lookup indexes. Existing migrations and historical effect hashes remain
immutable.

No raw policy JSON, URL query values, credentials, provider responses, or
network diagnostics are persisted in these columns. The authoritative policy
configuration remains in the connector/provider deployment boundary; these
columns bind its exact redacted identity to the effect history.

## Runtime behavior

- In development, missing boundary facts preserve the existing fake/read-only
  and explicitly configured compatibility paths. A live effect without a
  boundary envelope is marked unavailable by strict conformance and is not
  dispatched when strict external-boundary admission is enabled.
- In strict production, the operation store rejects an external effect before
  the external invocation unless the envelope is complete and the exact
  credential lease/source facts are valid when the capability requires them.
- The dispatcher copies the normalized envelope into the authority and effect
  reservation, then revalidates the live facts immediately before invocation
  and after invocation before finalization.
- A stale lease, source version, expiry, boundary mismatch, deployment
  admission mismatch, task fence, or operation fence fails closed. An uncertain
  external outcome remains recovery-required and is never blindly retried.
- Federation poll attempts persist the same envelope when their controlled
  peer transport is constructed. Strict production mode rejects a federation
  dispatch that has no boundary facts; local lifecycle fixtures may opt out of
  that strict requirement.

## Reuse and licensing

This slice reuses Fornix's existing credential lease validator and resolver,
`EgressPolicy`/`DestinationPolicy` hashes, operation/effect reservation,
authority-link stores, deployment-admission reference, connector conformance
registry, and redaction rules. It adds no external dependency and copies no
Kronaxis Fabric source. Kronaxis remains BSL 1.1 and outside Fornix's MIT
distribution boundary.

## Cost, latency, and storage budget

- Contract normalization and conformance inventory are in-memory and bounded
  by the number of registered capabilities.
- Reservation work adds four small text fields, one consistency check, and no
  network call. The existing credential lease validation remains one indexed
  Postgres lookup in the reservation transaction.
- The migration adds at most four bounded hash/mode values per external effect
  and authority link. It does not duplicate policy payloads or secret bytes.
- A deployment should measure effect-reservation p50/p95/p99, rejected missing
  envelope counts, lease validation latency, boundary-hash drift, and storage
  growth by effect class. Local verification cannot claim Postgres latency
  because no `FORNIX_TEST_PG_DSN` is configured in this workspace.

## Acceptance tests

- Complete boundary envelopes normalize, hash, serialize, and replay
  deterministically without raw URLs, credentials, or arbitrary text.
- Missing, partial, malformed, unsupported-mode, or cross-workspace envelopes
  fail closed before reservation or invocation.
- Credential lease fence, revocation epoch, source-version, source-expiry,
  workspace, and current-status mismatches fail closed.
- Duplicate effect delivery with the same envelope produces one reservation;
  a duplicate with changed boundary or lease facts produces a conflict.
- Authority links and effect rows preserve identical boundary hashes and
  credential facts in one transaction.
- Historical links remain readable after expiry/revocation, while new live
  dispatch is rejected.
- Fake/read-only paths remain deterministic and network-free.
- All built-in effectful adapter conformance entries are complete; a missing
  boundary provider is rejected by the strict manifest.
- Workspace isolation, migration-upgrade, transaction-crash, race, redaction,
  replay, and existing smoke/CI checks remain green.
