# Task 67 — production federation injection, reconciliation, and quarantine

Status: implementation feature note. This document defines the next
qualification slice; it is not a production-readiness declaration.

## Problem and scope

The workspace-scoped federation authority from Task 66 has the correct
durable peer, lease, poll-attempt, credential-reference, and controlled-egress
contracts, but the normal server composition deliberately leaves the poller
unconfigured. That is safe, but it means a deployment cannot use the new path
without an explicit dependency boundary. The same slice needs a durable,
operator-visible response to uncertain remote outcomes and a safe disposition
for historical federation rows whose ownership cannot be inferred.

Task 67 closes those gaps without copying the historical global federation
model into the workspace authority and without pretending that a remote call
can be exactly-once.

## Invariants

1. A production server can enable federation polling only when it receives an
   explicit credential-authority dependency. Missing authority is a startup or
   readiness configuration error, never an inline-token fallback.
2. The server dependency boundary accepts either a provider-neutral secret
   manager (which is wrapped by Fornix's managed resolver and Postgres lease
   store) or a test/deployment-supplied fenced lease resolver. The resolver
   owns secret material; Postgres owns workspace scope, lease identity, fence,
   revocation epoch, source version, and audit history.
3. A credential manager endpoint is a bounded, controlled destination. Its
   authentication token is supplied through an injected token source or an
   explicitly named process environment variable; the token value is never
   placed in `config.Config`, logs, events, errors, artifacts, or database
   rows.
4. Federation polling is opt-in through configuration. Enabling it without a
   usable resolver fails closed. Disabling it leaves the route unavailable and
   does not start a background poller.
5. A poll attempt is successful only after the bounded response hash, imported
   message identities, and attempt state are committed under the current peer
   fence. A lost or ambiguous remote result remains `recovery_required`.
6. Reconciliation can use a previously recorded response hash and bounded
   message set, but cannot invent a success for an attempt with no received
   response, cross a workspace, bypass a peer fence, or execute a remote call.
7. Historical global federation rows are never assigned to a workspace by
   inference. Quarantine/export records contain only bounded, redacted
   metadata and a deterministic hash; raw bearer tokens and raw response
   bodies are never copied or exported.
8. Repeated configuration, reconciliation, quarantine, and startup requests
   are idempotent. Stale workers fail closed and replay has no network or
   secret-manager side effects.

## Configuration and composition

`config.Config` carries only non-secret deployment settings:

- whether the workspace federation poll surface is enabled;
- a bounded managed-credential authority URL;
- whether private manager destinations are allowed; and
- the name of an environment variable used by the optional process token
  source.

`server.New` remains source-compatible and safe by default. A new
`server.NewWithDependencies` accepts an explicit `ServerDependencies` value.
When a managed secret manager is supplied, the server constructs
`ManagedSecretResolver` and `CredentialLeaseStore` after migrations and wires
the poller with the exact fenced validator. A deployment may instead inject a
strict `LeaseResolver`/`LeaseValidator` pair for mTLS, workload identity, or a
test authority. No constructor silently reads an arbitrary provider key.

The command binary may construct the bounded HTTP manager from the explicit
manager URL and token-variable name. Hosted deployments should use the
dependency constructor directly when they have workload identity or mTLS.

## Remote outcome reconciliation

The remote boundary is at-least-once. A response that was received but whose
local transaction did not finish may be reconciled from a bounded,
operator-provided response envelope. The reconciliation transaction:

1. locks the attempt and validates workspace, peer, owner, and fence;
2. requires `recovery_required` (or the equivalent dispatching recovery
   state) and a non-empty recorded response hash;
3. normalizes and sorts remote messages with the same deterministic identity
   rules as the live poller;
4. verifies the supplied response hash and provider request identity against
   the recorded facts; and
5. imports idempotent message identities and commits attempt completion in one
   transaction.

The operation does not resolve credentials, contact the peer, or downgrade an
unknown response to success. A remote read may be retried because it is
semantically repeatable, but Fornix claims exactly-once only for the local
durable message identity and not for the external request.

## Historical quarantine and export

Migration `063` adds a workspace-scoped audit/quarantine table. An operator
must choose an audit workspace explicitly; that scope is not ownership of the
historical peer. The operation records source table/row identity
(`fornix.federation_peers` after the legacy schema rename), bounded URL
metadata or URL hash, a row fingerprint, disposition, reason, actor, and
timestamps. It never selects or returns the legacy bearer token. Export is a
redacted, paginated report and is safe to replay; it does not mutate the
historical global tables.

## Reuse and licensing

- Orloj's injected secret-resolver and controller/reconcile separation inform
  the composition boundary.
- agentmemory's lease, heartbeat, and checkpoint behavior informs explicit
  stale-owner failure and recovery state.
- ClawMem's replay runner informs the rule that reconciliation and replay do
  not call remote systems.
- Fornix reuses its own managed resolver, controlled egress client, credential
  lease store, federation poller, coordination store, and RLS conventions.
- No reference source is copied. Kronaxis Fabric remains excluded because its
  BSL 1.1 license is incompatible with direct reuse. New code remains under
  Fornix's MIT license.

## Cost and storage budget

- Manager resolution is one bounded external request plus one short Postgres
  lease transaction; it never holds a database lock during network I/O.
- Polling performs indexed peer/attempt/lease work and stores hashes and
  counters, not raw remote bodies or credentials.
- Reconciliation performs no external call and is bounded by the peer message
  and byte limits. Quarantine exports are paginated and bounded.
- Production qualification must measure manager latency, lease failures,
  poll recovery rate, SQL statements per poll/reconcile, WAL, retained
  metadata bytes, and remote retry rate before setting deployment SLOs.

## Acceptance tests

- production composition requires explicit managed authority when polling is
  enabled;
- injected fake and managed authorities preserve workspace, purpose, source
  version, expiry, and fencing semantics;
- manager configuration rejects unsafe endpoints and oversized/invalid
  responses, and manager tokens never appear in diagnostics;
- duplicate polling creates one local import effect;
- stale peer and credential fences reject completion and reconciliation;
- a lost response remains recovery-required;
- a received response can be reconciled exactly once without network or
  secret-manager calls;
- contradictory, cross-workspace, over-budget, and hash-mismatched envelopes
  fail closed;
- historical quarantine never copies raw tokens and does not infer peer
  ownership;
- redacted quarantine export is paginated, idempotent, and workspace scoped;
- fresh and existing migrations, RLS role separation, race tests, smokes,
  full tests, and documentation checks remain green.

## Explicit non-goals

This slice does not add a vendor-specific secret-manager SDK, a broker, a
global federation rewrite, exactly-once external execution, certificate pinning
for every provider, HA PostgreSQL, or a production SLO claim. Those remain
deployment qualification work tracked by Issue #40.
