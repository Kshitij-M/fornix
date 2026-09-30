# Loop 50 completion: transactional cross-authority linkage

Status: implemented on `feat/issue-40-production-qualification` and locally
qualified; not yet committed or merged.

## Outcome

Fornix now records a bounded, append-only `OperationAuthorityLink` for the
three important generic boundaries:

```text
admission decision ─┐
                    ├─ operation_authority_links ─ replay/inspection
fenced result     ──┤
Work Receipt       ─┘
```

The link is a hash-and-reference join. Existing operation, admission,
credential, trust, effect, evidence, artifact, result, and receipt rows remain
the authorities for their own state. The link records which snapshot and
fences were used without copying raw payloads or secret material.

## Delivered

- Added `internal/contracts/authority.go` with bounded, canonical
  `OperationAuthorityLink` and reference contracts.
- Added migration `049_authority_links.sql` with append-only triggers, hash and
  fence constraints, bounded JSON arrays, operation foreign-key integrity, and
  workspace RLS.
- Added shared transactional validation for operation hashes, live operation
  leases, credential lease fences/epochs, unexpired signed trust policies,
  admission decisions, operation results, and Work Receipts.
- Integrated admission links into `AdmissionStore.Admit`.
- Routed generic HTTP read/observation execution through the durable admission
  store before adapter execution, preserving the process-local trust check as
  an additional boundary.
- Integrated result links into `OperationStore.RecordResult`, including
  output/evidence/effect hashes and current operation fence.
- Integrated receipt links into `WorkReceiptStore.finalizeTx` when a receipt
  references a generic operation.
- Added deterministic bounded `OperationStore.AuthorityLinks` inspection and
  `GET /v1/operations/{id}/authority-links`.
- Added contract, concurrency/idempotency, stale-fence, workspace-isolation,
  receipt-link, and crash-rollback tests.
- Documented the route and the authority boundary in the HTTP API reference.

## Correctness and recovery semantics

Admission, result, or receipt linkage fails closed when the operation hash,
trust snapshot, credential fence, operation fence, task fence, or source hash
does not match. The source mutation and link share one Postgres transaction:

- before commit: neither the source mutation nor link is durable;
- after commit: the source mutation and link are both inspectable; and
- duplicate delivery: the original immutable link is returned when the
  normalized link hash is identical, otherwise the request conflicts.

Remote providers and external effects remain at-least-once. A link proves the
reserved/effect identity and its hashes; it does not claim exactly-once remote
execution.

## Qualification evidence

Against a fresh disposable PostgreSQL database with all embedded migrations:

```text
FORNIX_TEST_PG_DSN=... go test ./internal/store -count=1
FORNIX_TEST_PG_DSN=... go test ./... -count=1
go test ./internal/contracts ./internal/store ./internal/server
```

The focused authority tests and the full fresh-database repository suite passed
locally. The full non-database suite and later repository qualification still
need to be repeated after any merge conflict resolution. No OpenAI key,
remote model, broker, or external tool was used.

## Cost and storage observation

The link stores bounded metadata only: at most 128 effect IDs and 128 evidence
and artifact references per boundary. The migration constrains encoded arrays
and actor metadata; inspection is paginated and never loads raw payloads. A
fresh local qualification database measured 22 generated links at 120 KiB of
relation storage; the average encoded row was approximately 1,015 bytes and
the largest was 1,824 bytes. These are local PostgreSQL measurements, not a
production capacity claim. A deployment must measure relation growth under
its operation rate and include authority-link rows in retention/backup
capacity planning.

## Remaining limitations

- Existing historical operations may have no link until a bounded backfill or
  replay-safe inspection repair is designed; no authoritative history is
  rewritten by this loop.
- Some legacy callers omit trust-policy and credential-lease facts. They remain
  compatible, but effectful production adapters must be composed to supply
  those references before execution.
- Session, cost-ledger, artifact-reference, and live external-effect adapter
  coverage still needs qualification across every connector family.
- Production readiness still requires managed secret-manager integration,
  role-separated deployment evidence, sandbox qualification, backup/restore,
  load/soak, live connector, and release qualification.
