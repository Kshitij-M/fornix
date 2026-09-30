# Task 77 — Disposable PostgreSQL effect-authority qualification probe

Status: implemented repository-owned qualification slice; not a
production-readiness declaration.

## Problem

Task 76 added a portable, hash-only observation contract for generic effect
authority. The repository already has PostgreSQL integration tests for
reservation, fencing, duplicate delivery, uncertain recovery, domain-link
transitions, and Work Receipt linkage, but those tests do not emit one
composable qualification observation. The next slice connects those existing
authorities without turning a local test into a production certification claim.

## Scope and invariants

The probe is explicit, bounded, and disposable:

1. The caller must provide a DSN, a run identifier, and a workspace identifier.
   The Make/script entry point additionally requires an explicit opt-in flag.
   No DSN is discovered from files, Docker, the shell profile, or Fornix
   configuration.
2. The probe creates one successful operation and one short-lived stale-fence
   operation under a unique caller-supplied workspace. It never deletes
   append-only authority rows, disables append-only triggers, truncates shared
   tables, or touches rows outside that exact workspace. The caller disposes
   the explicitly confirmed test database after the run.
3. The successful path must prove one reservation, one external invoker call,
   one reconciled domain link, one duplicate replay with no second invocation,
   one immutable Work Receipt, and one receipt authority-link hash.
4. The stale path must reject the expired owner after takeover before the
   invoker is reached. Cross-workspace reads must fail closed.
5. Replay is a read-only recomputation over returned durable hashes and the
   duplicate dispatch result. The probe never replays a provider or external
   system.
6. Raw provider payloads, SQL text, DSNs, credentials, prompts, and driver
   error strings never enter the observation or qualification report.
7. Any missing authority row, hash mismatch, unexpected duplicate, stale
   acceptance, workspace leak, or cleanup failure fails closed.

## Crash and concurrency semantics

The existing dispatcher and receipt failure hooks remain the authority-owned
crash tests. This probe adds a bounded rollback assertion by using the receipt
store's failure hook: an injected failure after receipt child links are staged
must leave no receipt or receipt-link row. The successful retry must create one
receipt. Duplicate dispatch is exercised sequentially because the existing
dispatcher package owns the higher-cost concurrent race qualification; the
probe must not inflate database load or produce nondeterministic evidence.

## Reuse and licensing

Reuse is intentional: `effectdispatch.Dispatcher`, `OperationStore`,
`AdmissionStore`, `DomainEffectLinkStore`, `WorkReceiptStore`, and the existing
migration/test cleanup conventions remain authoritative. The probe adds only
composition and redaction. It copies no source from Kronaxis (BSL 1.1); the
implementation remains original and MIT-compatible. Orloj's fenced execution
boundary, ClawMem's replay/abstention discipline, agentmemory's bounded
diagnostics, and FornixDB's immutable disclosure model inform the design but
are not copied.

## Cost and storage budget

The default path performs zero database work. The opt-in probe uses one
explicit disposable PostgreSQL pool, two small operations, one effectful
invocation, one receipt, and a hard context deadline with no retries. The
observation is in-memory and hash-only; it adds no schema, artifact, broker,
provider, or persistent qualification table. The probe deliberately preserves
the small append-only rows it creates; the operator must discard the confirmed
disposable database rather than bypass history protections to reclaim them.

## Acceptance tests

- Missing opt-in or DSN skips/fails closed without contacting a database.
- Success produces a valid `EffectAuthorityObservation` with receipt-link
  proof and a stable observation hash.
- Duplicate delivery returns the committed effect/link and invokes exactly
  once.
- Work Receipt finalization is idempotent and its authority link carries the
  same receipt hash.
- Failure before receipt commit leaves no receipt or receipt authority link.
- Stale worker takeover rejects the old fence before invocation.
- A foreign workspace cannot read the successful domain link.
- Recomputed replay hash and duplicate replay hash are identical.
- The probe never mutates or deletes append-only authority history; disposal of
  the confirmed ephemeral database is the cleanup boundary.
- The script is not part of default CI and never prints secrets or raw
  provider/database errors.

## Explicit limitation

This is repository-owned disposable-authority evidence. It does not prove
hosted PostgreSQL HA/PITR, live provider idempotency, production credential
rotation, sandbox strength, network partitions, deployment topology, or
load/soak behavior. Those remain deployment-owned qualifications.
