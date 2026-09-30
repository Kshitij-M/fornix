# Task 50: Transactional authority linkage

Status: implementation in progress.

## Why this exists

Fornix already has separate durable authorities for capability trust, policy
admission, credential leases, operation ownership, external-effect
reservations, evidence, artifacts, and Work Receipts. Separate authorities
are useful for independent recovery, but a production control plane also needs
an auditable answer to one question:

> Which exact authority snapshots permitted this operation, and which exact
> result did those snapshots produce?

This slice adds an append-only authority-link record. It is a hash-and-reference
join across those authorities. It does not replace any source record and it
never stores credentials, prompts, connector payloads, or raw evidence.

## Invariants

1. Every link is workspace-scoped and is rejected if its operation hash does
   not match the authoritative operation row.
2. A link binds the admission decision, capability definition, policy snapshot,
   trust-policy snapshot, credential lease fence/epoch, operation fence, task
   fence, effect reservations, result hashes, and evidence/artifact references
   that were known at that boundary.
3. Links are append-only. A duplicate idempotency key is accepted only when
   the complete normalized link hash is identical; conflicting reuse fails
   closed.
4. Admission links are recorded in the same transaction as the admission
   decision and event. Result links are recorded in the same transaction as
   the immutable operation result and lifecycle transition. Receipt links are
   recorded in the same transaction as the immutable Work Receipt.
   The generic HTTP read/observation execution path now uses the durable
   admission store before adapter execution, so its result link is connected
   to the persisted admission decision rather than only to process-local
   registry state.
5. A non-empty trust-policy hash must resolve to an unexpired policy signed by
   an active durable signer. A credential lease reference must resolve to the
   exact active lease, fence, and revocation epoch. The compatibility path may
   omit those facts only when the caller is not claiming them.
6. Replay reads links and hashes; it never reuses a stale lease, trust policy,
   connector definition, or external effect reservation for new work.
7. Inspection is bounded, workspace-authorized, and reference-only. Raw
   payloads remain in their existing evidence/artifact authorities.

## Schema and API

Migration `049_authority_links.sql` adds an append-only
`fornix.operation_authority_links` table with explicit columns for authority
hashes, fences, effect IDs, evidence/artifact references, result/receipt
references, actor, causation, correlation, and idempotency. JSON columns are
bounded and RLS is enabled. The Go contract is
`contracts.OperationAuthorityLink`; `OperationStore` exposes bounded
workspace-scoped inspection while admission, result, and receipt stores use a
shared transactional append helper.

## Failure and crash semantics

All three integration points are one Postgres transaction. A failure before
commit rolls back the source mutation and its link together. A committed
source mutation always has its link. A duplicate delivery reads the original
immutable link. A stale operation or task fence is rejected before either
source mutation or link can commit.

External calls remain at-least-once. The link records provider/effect identity
and reservation hashes, but it does not claim exactly-once execution.

## Reuse and licensing

The implementation reuses Fornix's existing operation leases, admission
decisions, credential lease authority, trust catalog, evidence/artifact
references, and Work Receipt validation. It introduces no broker, cache,
object store, or LLM framework. Reference-repository ideas are architectural
only; no Kronaxis source is copied. Fornix-owned additions remain under the
repository MIT license.

## Cost and storage budget

One link is a bounded metadata row. The target is less than 8 KiB of encoded
metadata per lifecycle boundary, with at most 128 references/effect IDs and
bounded JSON checks in Postgres. Admission, result, and receipt writes add one
indexed insert and one bounded validation read in their existing transaction.
Inspection is paginated and never loads raw payloads.

## Acceptance tests

- fresh and existing databases apply migration 049 cleanly;
- admission, result, and receipt links are atomic with their source writes;
- duplicate exact deliveries return one link and conflicting deliveries fail;
- stale operation/task/credential/trust authority fails closed;
- cross-workspace reads and writes fail closed under RLS and explicit checks;
- evidence/artifact/effect hashes survive replay unchanged;
- bounded inspection is deterministic and contains no secret or raw payload;
- injected failures leave no partial authority link or source mutation; and
- full tests, race checks, vet, build, docs, and smoke qualification remain
  green.
