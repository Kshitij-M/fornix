# Loop 54 completion — universal effect authority conformance and startup

Status: implemented and qualified on the current feature branch. This is a
bounded safety slice, not a production-readiness declaration.

## Delivered

- Extended `contracts.EffectAuthority` with the operation owner identity so a
  fence cannot be presented without the worker that owns it.
- Added `OperationStore.ValidateEffectAuthority`. It locks and validates the
  live operation lease, terminal state, operation owner/fence, and task owner/
  fence in Postgres immediately before an authority-bound dispatch. It never
  acquires or renews a lease.
- Added `connector.AuthorityValidator` and wired it at both admission and the
  final executor dispatch attempt. A retry cannot continue after a live fence
  becomes stale.
- Added `Registry.RequireEffectAuthority` and
  `Registry.ValidateEffectAuthorityConformance`. Server composition enables
  the requirement, and every registered write-like capability must implement
  `AuthorityAwareCapability`.
- Migrated the deterministic fake incident remediation capability to the
  authority-aware seam. Direct low-level unit registries remain compatible for
  isolated contract tests; the server registry is fail-closed.
- Added durable signed trust/schema catalog loading for every paginated
  workspace during server startup and before generic operation admission.
  Missing catalogs are tolerated only in development compatibility mode; a
  partial, invalid, expired, revoked, mismatched, or downgraded generation
  fails closed.
- Added readiness and health authority status with required/ready/workspace/
  loaded-at/error fields plus bounded per-workspace policy/schema revision,
  hash, signer, and expiry generation records. Signed authority is automatically required when
  `FORNIX_ENV=production`, or can be rehearsed with
  `FORNIX_REQUIRE_SIGNED_AUTHORITY=true`.
- Fixed idempotent signed trust reload so reloading the same signed revision
  still enables signed verification instead of silently retaining an unsigned
  process snapshot.
- Added startup generation, direct effect rejection, pre-dispatch validator,
  live operation-fence takeover, migration, and full database integration
  coverage.
- Added `make smoke-universal-authority-conformance`, aggregate smoke and CI
  coverage, `.env.example` documentation, this feature note, and this report.

## Authority flow

```text
Postgres signed policy + schema
             │ verify, page, refresh
             ▼
  immutable process registry generation
             │ exact EffectAuthority
             ▼
 admission → live operation/task fence validation → adapter seam
             │
             └── stale/missing/revoked/expired authority fails closed
```

The authority envelope contains identifiers, hashes, revisions, leases, and
fences only. It never contains credential values, raw prompts, provider
payloads, or secrets.

## Qualification evidence

The checks below used the explicitly named disposable database
`fornix_task54_20260924`. The persistent development database was not used for
cleanup and was not dropped.

| Check | Result |
| --- | --- |
| `go test ./... -count=1` without Postgres | Passed |
| `FORNIX_TEST_PG_DSN=... go test ./... -count=1` | Passed; migrations reached `052_effect_authority_facts` |
| Startup reload integration test | Passed; signed workspace policy and schema were loaded after server composition |
| Live operation-fence integration test | Passed; original owner rejected after takeover, current owner accepted |
| Effect authority/conformance tests | Passed; direct effect admission rejected and validator ran before dispatch |
| `make smoke-universal-effect-authority` | Passed against disposable Postgres |
| `make smoke-universal-authority-conformance` | Passed |
| `git diff --check` | Passed |
| Focused connector/server/store run | Passed in approximately 1.24 seconds wall time in the local run |

Measured disposable relation sizes after the qualification run:

| Relation | Total bytes |
| --- | ---: |
| `operation_authority_links` | 163,840 |
| `operation_effects` | 57,344 |
| `operation_leases` | 65,536 |
| `trust_policies` | 114,688 |
| `trust_schema_catalogs` | 122,880 |
| `trust_signers` | 65,536 |

These are fixture-size observations, not capacity or production SLO claims.
The reload path intentionally adds bounded Postgres reads per workspace at
startup and before generic operation admission. The next qualification must
measure pool pressure, p95 latency, and catalog refresh cost under realistic
workspace counts.

## Remaining limitations

Task 54 does not yet make the entire universal harness production-ready:

1. The shared durable effect dispatcher that atomically requires admission and
   `ReserveEffect` before every external mutation is still the next task.
2. Incident workflow remediation still has a domain-specific execution path
   and must be migrated to the shared dispatcher.
3. Repository change application, side-effectful tools, and provider-backed
   model calls need the same authority/reservation integration. Model calls
   retain their separate billable at-least-once boundary.
4. Process-level crash after provider dispatch, verification, compensation,
   and restart recovery remain to be qualified with controlled adapter fakes.
5. Signer rotation ceremony, deployment-wide rollout lag metrics, catalog
   rollback operations, and external provider live conformance remain open.
6. Existing low-level capability interfaces still expose `Execute`; production
   server composition rejects unsafe effectful admission, but a future public
   adapter SDK must remove or explicitly mark that bypass.
7. No exactly-once remote execution guarantee is made or implied.

## Next task prompt

```text
Task 55 — Build Fornix’s shared durable effect dispatcher and migrate every
effectful execution path.

Read the chats directory, AGENTS.md, docs/00-fornix-foundation.md,
docs/14-production-readiness-qualification.md,
docs/128-effectful-adapter-authority-foundation.md,
docs/130-universal-effect-authority-conformance-foundation.md, and
docs/131-loop-54-completion.md. Study the current operation admission/effect
stores, credential lease resolver, signed catalog reload, connector executor,
HTTP adapter, fake incident workflow, repository-change service, tool
executor, model gateway, and MCP bridge. Re-read Orloj execution-engine and
provider patterns, DeepSeek Harness prepared-call/atomic-reload patterns,
agentmemory retry/checkpoint patterns, and OpenBao lease/reload patterns
without copying incompatible source.

Write docs/132-durable-effect-dispatcher-foundation.md before coding.

Implement one domain-neutral dispatcher that transactionally requires durable
operation admission, effect reservation, exact schema/catalog facts, exact
credential lease/source facts, operation/task fences, egress authority, and
provider idempotency metadata before calling any effectful adapter. Validate
the live authority again immediately before dispatch and before finalization.
Migrate HTTP submit, fake incident remediation, repository changes, and
side-effectful tools first. Define an explicit model-call authority seam for
billable provider calls without incorrectly treating inference as a generic
filesystem mutation. Make MCP route through the same protected path.

Record provider request IDs, verification state, compensation state, unknown
outcomes, redacted evidence, actor, causation, correlation, workspace, task,
session, and idempotency metadata. Retry only before content/effect emission.
Never acquire a replacement credential lease after admission. Never claim
exactly-once remote execution.

Add integration tests for reservation-before-dispatch, duplicate delivery,
stale operation/task/credential fences, lease expiry/takeover, signer/catalog
rotation, workspace isolation, crash before reservation, crash after possible
provider dispatch, verification, compensation, replay, and MCP parity. Add
process restart tests using disposable Postgres and controlled fake adapters.
Update CI, Make/smokes, runbooks, API/CLI/MCP docs, measured SQL/latency/
storage impact, and remaining limitations. Preserve all existing dirty work
and do not introduce brokers, Redis, NATS, or new infrastructure.
```
