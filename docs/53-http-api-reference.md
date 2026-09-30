# Fornix HTTP API reference

Status: active reference for the current alpha API.

The Go server is the source of truth for route behavior. This page is a
human-readable map of the public surface and its cross-cutting rules; it is
not a promise that every compatibility endpoint will remain unchanged during
the alpha period.

## Common request rules

All workspace-scoped operations must identify one workspace. Prefer the
`workspace_id` field in a JSON body or query parameter. The authenticated
workspace-bound API key is authoritative; caller-supplied actor fields do not
override the authenticated actor. The development compatibility mode is
explicitly enabled with `FORNIX_AUTH_MODE=development`; production mode is
`FORNIX_AUTH_MODE=workspace`.

For mutating requests, send:

```http
Authorization: Bearer <workspace-api-key>
Idempotency-Key: <stable-request-key>
Content-Type: application/json
```

`Idempotency-Key` is scoped by workspace and operation. Reusing it with a
different payload is rejected. A successful duplicate returns the previously
recorded durable effect. A remote model provider or local process can still be
at-least-once if the process dies after the external effect and before the
local commit; Fornix does not claim exactly-once external execution.

Responses are JSON. Errors are bounded and do not include credentials, raw
prompts, or arbitrary unredacted provider output. Request IDs are returned or
logged through the server's normal request tracing path.

## Health and service discovery

| Method | Route | Purpose | Authentication |
| --- | --- | --- | --- |
| `GET` | `/healthz` | Process liveness | None |
| `GET` | `/readyz` | Database/migration readiness | None |
| `GET` | `/v1/health` | Bounded service health response | None |

Use `/readyz` before running a smoke or operator workflow. A live process is
not necessarily ready to accept workspace-scoped writes.

## Workspace and identity administration

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/v1/operator/workspaces/bootstrap` | Create or select a workspace through the explicit bootstrap credential |
| `GET` | `/v1/operator/workspaces` | List authorized workspaces with bounded pagination |
| `GET` | `/v1/operator/workspaces/{id}` | Read one authorized workspace |
| `GET` / `POST` | `/v1/operator/identities` | List or create workspace identities |
| `POST` | `/v1/operator/identities/{id}/disable` | Disable an identity |
| `GET` / `POST` | `/v1/operator/roles` | List or bind a role and its permissions |
| `POST` | `/v1/operator/roles/{identity}/{role}/unbind` | Remove a role binding |
| `GET` / `POST` | `/v1/operator/api-keys` | List or create workspace API keys |
| `POST` | `/v1/operator/api-keys/{id}/rotate` | Rotate a key; the token is returned only by this operation |
| `POST` | `/v1/operator/api-keys/{id}/revoke` | Revoke a key |

Bootstrap credentials are compared in memory and are not stored as API keys.
API keys are hashed, can expire, can be revoked, and are never written to
events, evidence, metrics, or logs. Treat a newly returned token as a secret;
Fornix does not provide it again after the create/rotate response.

## Repository ingestion

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/v1/operator/ingest/dry-run` | Discover a mounted repository and return a bounded report without mutation |
| `GET` | `/v1/operator/ingest/jobs` | List authorized ingest jobs |
| `POST` | `/v1/operator/ingest/jobs` | Submit an idempotent durable ingest job |
| `GET` | `/v1/operator/ingest/jobs/{id}` | Read job status and bounded report |
| `POST` | `/v1/operator/ingest/jobs/{id}/resume` | Process one bounded transactional batch |
| `POST` | `/v1/operator/ingest/jobs/{id}/cancel` | Cancel a job durably |

Source roots must be inside the workspace's explicitly configured mount.
Discovery normalizes paths, applies ignore rules, enforces file limits, and
rejects traversal and symlink escapes. The default path is offline and does
not require Ollama or an LLM. Changed and removed files remain auditable; old
authoritative history is not overwritten.

## Tasks, sessions, and coordination

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/v1/session` | Create or heartbeat a session |
| `GET` | `/v1/sessions` | List authorized sessions |
| `POST` | `/v1/session/{id}/heartbeat` | Renew a session lease/heartbeat |
| `POST` | `/v1/task` | Create a task |
| `GET` | `/v1/tasks` or `/v1/task/{id}` | List or read tasks |
| `POST` | `/v1/task/claim` | Claim a dependency-ready task with a lease/fence |
| `POST` | `/v1/task/{id}/renew` | Renew task ownership |
| `POST` | `/v1/task/{id}/complete` | Complete a task using the current fence |
| `POST` | `/v1/task/{id}/fail` | Record a classified failure/retry or dead-letter transition |
| `POST` | `/v1/task/{id}/cancel` | Cancel a task |
| `POST` | `/v1/coord` | Append a coordination message |
| `GET` | `/v1/coord/recent` | Read bounded recent coordination messages |

Task claims are workspace-scoped and return a fencing value. Renew, complete,
fail, and cancel requests must carry the current ownership identity and fence;
stale workers fail closed. Postgres is the authority for task state and
append-only lifecycle events.

Coordination messages are also workspace-scoped. `/v1/coord` accepts an
`Idempotency-Key` and returns the durable workspace sequence; repeating the
same request returns the original message without a second event, while a
different request under the same key fails with `409`. `/v1/coord/recent`
supports bounded `after_sequence`, `recipient`, and `limit` query parameters.
Malformed or negative sequence cursors fail with `400`. The historical global
coordination table is not read by these ordinary workspace routes.

## Generic operations

Generic operations are the domain-neutral control-plane surface. They identify
typed capabilities and resources by workspace-scoped references and hashes;
they do not accept executable code, shell fragments, credentials, prompts, or
connector payloads. A connector adapter is responsible for validating its
input and producing the input/schema hashes before admission.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `POST` | `/v1/operations` | Register one typed, idempotent operation intent | `operation:create` |
| `POST` | `/v1/operations/claims` | Claim a bounded deterministic batch of due non-effect-dispatch work | `operation:execute` |
| `GET` | `/v1/operations/{id}` | Read the operation projection and hashes | `operation:read` |
| `POST` | `/v1/operations/{id}/lease` | Acquire or take over the operation lease | `operation:execute` |
| `POST` | `/v1/operations/{id}/renew` | Renew the current operation lease | `operation:execute` |
| `POST` | `/v1/operations/{id}/release` | Release the current operation lease | `operation:execute` |
| `POST` | `/v1/operations/{id}/transition` | Advance one legal state transition | `operation:execute` |
| `POST` | `/v1/operations/{id}/execute` | Execute one trusted read-only or observation capability and persist its result | `operation:execute` |
| `POST` | `/v1/operations/{id}/replay` | Verify the durable transition/event hash chain | `operation:read` |
| `GET` | `/v1/operations/{id}/authority-links` | Read bounded trust, policy, lease, fence, effect, evidence, artifact, result, and receipt linkage | `operation:read` |
| `POST` | `/v1/operations/{id}/effects/reserve` | Reserve an external effect before connector dispatch | `operation:execute` |
| `POST` | `/v1/operations/{id}/effects/{effect_id}` | Reserve an external effect with a caller-selected identity | `operation:execute` |
| `GET` | `/v1/operations/{id}/effects/{effect_id}` | Read the current effect recovery state | `operation:read` |
| `POST` | `/v1/operations/{id}/effects/{effect_id}/lease` | Acquire or take over the workspace-scoped recovery lease | `operation:execute` |
| `POST` | `/v1/operations/{id}/effects/{effect_id}/renew` | Renew the current recovery lease | `operation:execute` |
| `POST` | `/v1/operations/{id}/effects/{effect_id}/release` | Release the current recovery lease | `operation:execute` |
| `POST` | `/v1/operations/{id}/effects/{effect_id}/state` | Append a fenced dispatch, acknowledgement, verification, compensation, or recovery transition | `operation:execute` |
| `GET` | `/v1/operation-effects/recovery` | List bounded, hash-only non-terminal recovery candidates | `operation:read` |

The create body contains a `request` object and may contain a normalized
`plan`, bounded resource references, and provenance links. The authenticated
principal supplies the actor; caller-supplied actor fields are overwritten.
Task-bound requests must include the current task fence and are subsequently
checked against the live task lease inside the same Postgres transaction.

For deployments that require release admission for generic effects, the
request may include a hash-only `deployment_admission` object:

```json
{
  "request": {
    "workspace_id": "workspace-a",
    "idempotency_key": "operation-1",
    "deployment_admission": {
      "workspace_id": "workspace-a",
      "deployment_id": "deployment-a",
      "release_id": "release-a",
      "release_hash": "<sha256>",
      "artifact_kind": "image",
      "artifact_hash": "<sha256>",
      "trust_snapshot_revision": 3,
      "trust_snapshot_hash": "<sha256>",
      "gate_hash": "<sha256>",
      "decision_hash": "<sha256>"
    }
  }
}
```

The reference is re-evaluated against the deployment release/evidence
authority in the same transaction that reserves an external effect. Missing,
expired, revoked, stale, mismatched, or cross-workspace references fail
closed. Set `FORNIX_REQUIRE_RELEASE_ADMISSION_FOR_EFFECTS=true` to require
the reference for new generic effect reservations; production enables this
policy by default. This does not execute a deployment or claim exactly-once
provider behavior.

`authority-links` is a bounded, reference-only inspection view. It connects
the operation to the admission decision, capability/policy/trust hashes,
credential lease fence, operation/task fences, effect reservations, evidence,
artifacts, result, and Work Receipt when those authorities were supplied. It
does not disclose payloads or secrets. Admission, result, and receipt links
are appended in the same transaction as their source records; replay can
verify the chain without reusing any authority for new work.

Lease acquisition returns a positive monotonic `fence`. Transition requests
must send it as `X-Operation-Fence`; missing, expired, released, or stale
fences fail closed. The operation lease is separate from the task lease, so a
task-bound worker must hold both authorities. Reusing a command idempotency
key with the same canonical request returns the existing result; changing the
logical request or plan under that key is a conflict.

The execute route uses the authenticated actor as the operation owner, acquires
or validates the operation fence, persists a deterministic connector plan when
needed, and records a bounded hash-only result in the same transaction as the
terminal state transition. It currently admits only read-only and observation
capabilities. A capability that declares an external effect is rejected until
the durable admission and effect-reservation path is used; Fornix never
silently turns a connector call into an untracked side effect. Duplicate
execution delivery returns the committed result without requiring a second
connector call. Raw output, credentials, and provider diagnostics remain in
their domain authorities and are not included in the operation result.

Workers can claim due generic work in one workspace-scoped transaction:

```sh
fornix operation claim --limit 16 --ttl-ms 90000
```

The equivalent HTTP call is `POST /v1/operations/claims?workspace_id=...`
with `{ "limit": 16, "ttl_ms": 90000 }`. The authenticated principal becomes
the lease owner. Selection is bounded and ordered by due time, creation time,
and operation ID; active leases are skipped and expired leases are taken over
with a higher fence. `awaiting_external` is intentionally excluded: uncertain
provider outcomes require the separate effect-recovery lease. Claiming work
does not execute a connector or external effect.

The CLI maps this surface without adding a second authority:

```sh
fornix operation create --request-file operation-request.json [--plan-file operation-plan.json]
fornix operation claim --limit 16 --ttl-ms 90000
fornix operation get --id op-123
fornix operation lease --id op-123
fornix operation execute --id op-123
fornix operation transition --id op-123 --fence 1 --to-status planned
fornix operation replay --id op-123
fornix operation effect-reserve --id op-123 --effect-file effect.json --step-id step-1 --attempt-id attempt-1 --request-hash HASH --fence 1
fornix operation effect-get --id op-123 --effect-id effect-123
fornix operation effect-state --id op-123 --effect-id effect-123 --state recovery_required --idempotency recovery-1 --fence 1
fornix operation effect-recovery --limit 64
fornix operation effect-lease --id op-123 --effect-id effect-123
fornix operation effect-state --id op-123 --effect-id effect-123 --state verification_pending --idempotency verify-1 --effect-fence 1
fornix operation effect-renew --id op-123 --effect-id effect-123 --effect-fence 1
fornix operation effect-release --id op-123 --effect-id effect-123 --effect-fence 1
```

Replay is read-only. It validates the initial state anchor, contiguous
versions, previous-state hashes, legal status transitions, linked event rows,
and the current projection hash. It never calls a model, tool, connector, or
external system. A committed external effect remains explicitly at-least-once
and is represented as uncertain when delivery cannot be reconciled.

### External-effect reconciliation

Reservation is the admission boundary, not execution. The reservation body
contains `step_id`, `attempt_id`, `request_hash`, and a typed `effect` with a
workspace, boundary, effect class, delivery guarantee, provider idempotency
metadata, and verification/compensation status. It does not contain a provider
payload, secret, credential, header, or arbitrary diagnostic text. The caller
must hold the current `X-Operation-Fence` returned by the operation lease.

The state route accepts only typed provider request identifiers, SHA-256
response/verification/compensation references, and a bounded failure code. The
operation lease can record dispatch intent and the `dispatching` state before a
provider call. A recovery worker must first acquire the separate effect lease;
it may reconcile an already-dispatching or later state, but cannot turn an
untouched `reserved` effect into a dispatch. The effect fence is sent as
`X-Effect-Fence` and is independent from `X-Operation-Fence`.

Recovery lease acquisition, takeover, renewal, and release are transactional
and workspace-scoped. Only one owner can hold an active lease; takeover
increments the fence, and a stale or expired fence fails closed. Recovery
listing is bounded and hash-only. Valid transitions are enforced by
`AdmissionStore`; repeated idempotency keys return the committed state, while
a different command under the same key is a conflict. A provider timeout or
process crash must be reconciled explicitly. Fornix never silently repeats an
external call and never claims exactly-once remote execution.

## Retrieval, evidence, and artifacts

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/v1/retrieve` | Build a deterministic, budgeted context pack |
| `POST` | `/v1/rag` | Legacy compatibility retrieval surface |
| `POST` | `/v1/evidence` | Create immutable evidence |
| `POST` | `/v1/evidence/edge` | Append a typed provenance edge |
| `POST` | `/v1/evidence/disclose` | Disclose gist, detail, or bounded raw evidence |
| `POST` | `/v1/evidence/provenance` | Traverse bounded provenance |
| `POST` | `/v1/artifacts` | Create or deduplicate a content-addressed artifact |
| `POST` | `/v1/artifacts/disclose` | Disclose a bounded artifact representation |
| `POST` | `/v1/artifacts/provenance` | Read artifact provenance |
| `GET` | `/v1/artifacts/metrics` | Read bounded artifact metrics |
| `POST` | `/v1/artifacts/backfill` | Run a bounded or dry-run output backfill |
| `POST` | `/v1/artifacts/retention` | Run a bounded retention sweep |
| `POST` | `/v1/artifacts/integrity` | Verify bounded artifact integrity |

Retrieval is deterministic and read-only over authoritative records. It tries
structured and lexical work before bounded graph/provenance expansion and only
uses vector work when the request supplies an embedding and the plan justifies
the cost. Context items carry source/evidence references and hard item, byte,
and token budgets. Evidence and artifact disclosure preserves content hashes;
raw bytes are not overwritten.

## Models, tools, and agent runs

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/v1/model/complete` | Execute a registered model provider under hard budgets |
| `POST` | `/v1/tools/execute` | Execute a registered structured-argv tool under policy |
| `POST` | `/v1/tools/recovery` | Reconcile one durable sandbox attempt without re-executing it |
| `POST` | `/v1/tools/approvals/{id}/decide` | Record an approval decision |
| `POST` | `/v1/agent/run` | Create or resume a bounded agent run |
| `GET` | `/v1/agent/runs` | List bounded workspace-scoped run summaries |
| `GET` | `/v1/agent/run/{id}` | Read a run checkpoint and status |
| `POST` | `/v1/agent/run/{id}/advance` | Advance one deterministic run step |
| `POST` | `/v1/agent/run/{id}/cancel` | Cancel a run durably |
| `POST` | `/v1/agent/run/{id}/external/wait` | Put a run at an explicit external wait boundary |
| `POST` | `/v1/agent/run/{id}/external/complete` | Complete an external wait idempotently |
| `POST` | `/v1/agent/run/{id}/replay` | Replay recorded run history without external effects |

The fake provider is the default offline path. OpenAI-compatible chat is
explicitly opt-in and receives credentials only through environment/configured
credential references. Tools use structured argv and do not invoke an
implicit shell. Policy is deny-by-default, approvals are durable, output and
timeout budgets are hard, and task-bound execution requires the current task
fence. A crash after a model/process side effect but before checkpoint commit
is recoverable but remains an at-least-once boundary.

`POST /v1/tools/recovery` requires workspace-scoped `operation:execute`
authorization and accepts `workspace_id`, `tool_run_id`, the expected effect
and domain-link versions, an optional idempotency key, and a bounded lease TTL.
It asks only the exact registered attempt-aware sandbox backend to inspect the
recorded attempt. A completed observation is finalized transactionally with
the tool result, output artifacts, effect/link transitions, and terminal event.
Absent, running, stopped, and unknown observations return a nonterminal
recovery response; none permits a retry or backend fallback. The default
installation has no such non-local runtime, so this route cannot recover an
OCI/gVisor/microVM attempt until a qualified provider is installed.

Tool backend selection is bound to the registered definition and matching
workspace policy; a caller cannot lower that backend. The request may tighten
budgets and require additional capabilities. For new tool runs, Fornix records
the normalized tool-definition and effective-profile hashes in redacted run
evidence. These hashes preserve the pre-existing logical request hash format
for replay compatibility, while duplicate requests with a changed definition
or effective profile fail closed. Historical terminal tool runs without those
hashes may be replayed without execution; historical nonterminal runs without
them cannot be resumed into a new process effect. Only the local-process
backend is currently available, and it is not a hostile-code sandbox.

## Evaluation, observability, and compatibility surfaces

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/v1/observability/metrics` or `/v1/metrics` | Read an authorized bounded metrics snapshot |
| `POST` | `/v1/evaluations/datasets` | Register a deterministic evaluation dataset |
| `GET` / `POST` | `/v1/evaluations/retrieval/surfaces` | List or capture redacted retrieval surfaces |
| `POST` | `/v1/evaluations/retrieval/runs` | Run a bounded durable or dry-run evaluation |
| `GET` | `/v1/evaluations/runs/{id}` | Read evaluation status, metrics, gates, and report references |
| `POST` | `/v1/router/observation` | Record workspace-scoped router telemetry |
| `GET` | `/v1/router/recommend` | Read a workspace-scoped cost-aware provider recommendation |
| `POST` | `/v1/federation/peer` | Register or revise a workspace-scoped peer using a logical credential reference |
| `GET` | `/v1/federation/peers` | List authorized workspace-scoped peers |
| `POST` | `/v1/federation/peer/poll` | Run one bounded fenced peer poll when a credential authority is injected |
| `POST` | `/v1/federation/poll/reconcile` | Reconcile a bounded recorded response without network or credential-manager access |
| `POST` / `GET` | `/v1/federation/legacy-quarantine` | Record or page redacted dispositions for historical global federation rows |
| `POST` | `/v1/federation/coord/import` | Import a bounded coordination compatibility record |
| `GET` | `/v1/federation/coord/since/{sequence}` | Read bounded coordination history |

Observability and evaluation never store credentials or raw prompts in metric
dimensions or reports. Replay consumes recorded model/tool/retrieval history;
it does not call remote providers or execute external tools.

Router observations are append-only, workspace-scoped, idempotent, and
canonicalized before persistence. Recommendations are bounded aggregates over
the requesting workspace's recent observations, sorted by deterministic
success-per-cost, success-rate, and model-ID tie-breakers. Peer registration
and listing use ordinary workspace permissions and never accept inline bearer
tokens. Polling requires an injected managed credential lease authority; if it
is not configured, the route fails closed. Peer leases are fenced, remote
responses are bounded and hash-recorded, and uncertain outcomes remain
`recovery_required`. The historical `/v1/federation/coord/*` routes remain an
explicitly quarantined compatibility surface and require the non-production
legacy flag plus `legacy:global_admin`.

Reconciliation accepts only a bounded response envelope whose in-memory hash
and provider request identity match the recorded attempt. It performs no
remote or credential-manager call. Quarantine records are scoped to an
explicit audit workspace, contain hashes and disposition metadata only, and
do not infer ownership or copy legacy bearer tokens.

## Work Receipts

Work Receipts are immutable verification envelopes over completed task or
agent-run work. They are derived from existing Postgres authorities; they do
not replace task, event, model, tool, retrieval, evidence, artifact, cost, or
replay records.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `POST` | `/v1/work-receipts` | Finalize one idempotent, fenced receipt and its typed links | `receipt:write` |
| `GET` | `/v1/work-receipts/{id}` | Read one workspace-scoped immutable receipt | `receipt:read` |
| `POST` | `/v1/work-receipts/disclose` | Read bounded gist/detail/raw canonical receipt JSON | `receipt:read` |

Finalization validates terminal task/run state, current task fences, source
identity, workspace ownership, and evidence/artifact hashes before committing
the receipt, steps, and normalized links in one transaction. Reusing the
natural work identity or idempotency key with the same logical request returns
one receipt; changing the request fails with a conflict. A crash before commit
leaves no receipt or partial link set.

The canonical receipt hash excludes delivery IDs and wall-clock fields while
including stable work identity, actor, fences, steps, source hashes, cost
classification, and replay verification. Gist/detail/raw views preserve that
canonical hash and enforce byte, token, and item budgets. Raw receipt JSON is
still redacted receipt metadata; it is not a disclosure of prompts,
credentials, or unbounded tool output. Remote providers and local processes
remain at-least-once external boundaries.

The CLI exposes `fornix receipt get` and `fornix receipt disclose`; the MCP
shim exposes equivalent `fornix__receipt_get` and
`fornix__receipt_disclose` tools. All three surfaces call the same HTTP
authority and preserve workspace authorization.

## Approval-gated repository changes

Repository changes use a separate external-effect boundary. A proposal is a
typed, bounded packet against a captured source snapshot; approval is a
durable decision over the packet hash; application is admitted only after the
packet and configured workspace mount are rechecked. Raw proposed bytes are
content-addressed artifacts and are never copied into change rows or event
payloads.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `POST` | `/v1/changes/dry-run` | Plan and verify a change without durable mutation or filesystem writes | `change:propose` |
| `POST` | `/v1/changes` | Capture a source snapshot and create an idempotent proposal | `change:propose` |
| `GET` | `/v1/changes/{id}` | Read a workspace-scoped proposal and packet references | `change:read` |
| `POST` | `/v1/changes/{id}/approve` | Approve or reject the exact packet hash | `change:approve` |
| `POST` | `/v1/changes/{id}/apply` | Apply an approved packet through structured filesystem APIs | `change:apply` |
| `POST` | `/v1/changes/disclose` | Read a bounded hash-preserving proposal/application view | `change:disclose` |

The source root must be inside the workspace's explicitly configured
`tool_root`; absolute-path traversal, symlink components, non-regular files,
source hash drift, duplicate operations, and budget violations fail closed.
Application uses temporary-file writes with content-hash verification and
checks the affected post-state. A multi-operation crash can leave an external
partial effect; the durable application is then classified
`recovery_required`, and Fornix does not claim exactly-once filesystem
execution. Successful applications produce a derived Verified Change Packet
Work Receipt with source, artifact, packet, result-tree, and provenance hashes.

The CLI exposes `fornix change dry-run`, `propose`, `get`, `approve`, `apply`,
and `disclose`. The MCP shim exposes equivalent
`fornix__change_*` tools. The fake provider is not involved in planning,
approval, or application.

## Post-change validation and re-index handoff

Validation is the read-only proof boundary after an approved change. It checks
the applied packet and configured mount, persists bounded result/evidence
history, creates a new ingestion handoff, and emits a verified validation Work
Receipt. It never overwrites the applied change or invokes a model, tool,
broker, or external CI service.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `POST` | `/v1/validations` | Run or dry-run the registered deterministic validators | `change:validate` |
| `GET` | `/v1/validations/{id}` | Read one workspace-scoped validation run | `change:read` |
| `GET` | `/v1/validations/{id}/results` | Read bounded validator results in ordinal order | `change:read` |
| `GET` | `/v1/validations/{id}/replay` | Reconstruct recorded results/events without live effects | `change:read` |
| `POST` | `/v1/validations/{id}/resume` | Resume a pending run from its durable identity | `change:validate` |
| `POST` | `/v1/validations/{id}/cancel` | Durably cancel a non-terminal run | `change:validate` |
| `POST` | `/v1/validations/disclose` | Read a bounded gist/detail/raw report view | `change:read` |
| `GET` | `/v1/reindex-handoffs/{id}` | Read a durable handoff | `change:read` |
| `POST` | `/v1/reindex-handoffs/{id}/submit` | Idempotently submit the handoff to ingestion | `change:validate` |

The initial validator registry covers source preconditions, changed-file and
byte limits, path safety, result-tree equality, and re-index discovery. Plans
are explicit and sorted by validator ID/version. Results are hash-preserving,
workspace-scoped, bounded, and append-only. A crash before the final Postgres
commit leaves the run pending with no result/evidence/handoff effect; a crash
after commit is safely replayable. The local filesystem observation and the
subsequent ingestion job are separate external boundaries, so handoff
submission is at-least-once and remains retryable.

The CLI exposes `fornix validation dry-run`, `run`, `status`, `results`,
`replay`, `disclose`, `resume`, `cancel`, and `handoff`. The MCP shim exposes
the corresponding validation and handoff tools. See
[`58-validation-foundation.md`](58-validation-foundation.md) for invariants
and [`59-loop-22-completion.md`](59-loop-22-completion.md) for qualification
evidence and remaining limitations.

## Deployment qualification trust and authorized imports

Signed qualification bundles are not authorized by their embedded public key.
An operator must first register a deployment/workspace-scoped public signer,
then use the authorized import route. Private keys and secret-manager values
never enter these requests.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `GET` | `/v1/qualification/signers` | List bounded signer metadata | `qualification:read` |
| `POST` | `/v1/qualification/signers` | Register a public signer | `qualification:admin` |
| `POST` | `/v1/qualification/signers/{key_id}/revoke` | Revoke a signer | `qualification:admin` |
| `POST` | `/v1/qualification/import` | Verify and durably import a signed bundle, or dry-run | `qualification:import` |
| `GET` | `/v1/qualification/imports` | List bounded accepted import metadata | `qualification:read` |
| `GET` | `/v1/qualification/imports/{id}` | Disclose one bounded signed envelope | `qualification:read` |
| `GET` | `/v1/qualification/snapshots` | List bounded deployment trust snapshots | `qualification:read` |
| `POST` | `/v1/qualification/snapshots` | Publish or dry-run a signed trust snapshot | `qualification:admin` |
| `GET` | `/v1/qualification/snapshots/{id}` | Disclose one signed snapshot | `qualification:read` |
| `POST` | `/v1/qualification/snapshots/{id}/revoke` | Revoke a snapshot without deleting history | `qualification:admin` |

Signer rotation is registration with `supersedes_key_id`; the predecessor
remains auditable but cannot authorize new imports. Import identity is scoped
to `(workspace_id, deployment_id, signed_hash)`. Replaying the same signed
bytes with the same idempotency key returns the original record; different
bytes or a different idempotency identity fails closed. `dry_run: true`
performs trust, signature, scope, size, and conflict checks without creating
signer, import, or audit rows. `raw=true` on explicit disclosure returns the
bounded exact submitted bytes as base64 in JSON; ordinary list responses
contain hashes and redacted metadata only.

The equivalent CLI commands are `fornix qualification signer-list`,
`signer-register`, `signer-rotate`, `signer-revoke`,
`import-authorized`, `imports-list`, and `import-get`. The existing
`fornix qualification import` command remains offline validation and does not
write PostgreSQL state.

Task 81 adds a hash-only release/evidence index. It references accepted
authorized imports; it does not copy their signed bytes and it never executes
the deployment operation represented by an evidence kind.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `GET` | `/v1/qualification/releases` | List bounded workspace/deployment release identities | `qualification:read` |
| `POST` | `/v1/qualification/releases` | Register one immutable release against the current trust snapshot | `qualification:admin` |
| `GET` | `/v1/qualification/releases/{id}` | Read one release identity | `qualification:read` |
| `GET` | `/v1/qualification/releases/{id}/evidence` | List hash-only evidence links | `qualification:read` |
| `POST` | `/v1/qualification/releases/{id}/evidence` | Link one accepted import to a release | `qualification:admin` |
| `GET` | `/v1/qualification/releases/{id}/gate` | Evaluate a deterministic read-only qualification gate | `qualification:read` |

Readiness observations are advisory snapshots of the gate, not admission
tokens. They contain only release/trust/evidence identities, hashes, bounded
diagnostics, and evaluation metadata. Incident annotations use structured
codes and dispositions; raw deployment logs and signed bundles are not
accepted by these routes.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `GET` | `/v1/qualification/readiness/snapshots` | List bounded readiness snapshots for a release | `qualification:read` |
| `POST` | `/v1/qualification/readiness/snapshots` | Capture or dry-run one current readiness snapshot | `qualification:admin` |
| `GET` | `/v1/qualification/readiness/snapshots/{id}` | Disclose one hash-only readiness snapshot | `qualification:read` |
| `GET` | `/v1/qualification/readiness/snapshots/{id}/annotations` | List bounded incident annotations | `qualification:read` |
| `POST` | `/v1/qualification/readiness/snapshots/{id}/annotations` | Add or dry-run one structured incident annotation | `qualification:admin` |

Freshness policies and reviews remain advisory. A policy applies only to
operator review age; it cannot extend signed evidence expiry or grant effect
admission. Reviews are read-only and compare two immutable snapshots at an
explicit `as_of` time.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `GET` | `/v1/qualification/readiness/policies` | List immutable freshness-policy revisions | `qualification:read` |
| `POST` | `/v1/qualification/readiness/policies` | Publish or dry-run a freshness policy | `qualification:admin` |
| `GET` | `/v1/qualification/readiness/policies/current` | Read the current freshness policy | `qualification:read` |
| `POST` | `/v1/qualification/readiness/review` | Compare two snapshots without mutation | `qualification:read` |

Qualification retention is an operational overlay for external archival
planning. It never deletes or rewrites snapshots, incident annotations, or
freshness policies, and it cannot change release admission. Policy revisions
are immutable. Metadata synchronization inserts only missing hash-only rows;
planning and recovery are read-only and accept an explicit `as_of` for
replayable results.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `GET` | `/v1/qualification/readiness/retention/policies` | List immutable retention-policy revisions | `qualification:read` |
| `POST` | `/v1/qualification/readiness/retention/policies` | Publish or dry-run a retention policy | `qualification:admin` |
| `GET` | `/v1/qualification/readiness/retention/policies/current` | Read the current retention policy | `qualification:read` |
| `POST` | `/v1/qualification/readiness/retention/metadata/sync` | Register missing retention metadata in bounded pages | `qualification:admin` |
| `POST` | `/v1/qualification/readiness/retention/plan` | Produce a deterministic, hash-only archival plan | `qualification:read` |
| `POST` | `/v1/qualification/readiness/retention/recovery` | Produce a read-only integrity/recovery report | `qualification:read` |

Use `Idempotency-Key` for capture and annotation writes. The equivalent CLI
commands are `fornix qualification readiness-capture`, `readiness-list`,
`readiness-get`, `incident-annotate`, `incident-list`,
`freshness-policy-set`, `freshness-policy-get`, `freshness-policy-list`, and
`readiness-review`. Retention operators use `retention-policy-set`,
`retention-policy-get`, `retention-policy-list`, `retention-sync`,
`retention-plan`, and `retention-recovery`.

Release registration captures the exact current trust-snapshot revision and
hash. New evidence links must reference an accepted import with the same
deployment, target, signed report, manifest, source, and trust-snapshot facts;
links from a later snapshot fail closed as stale. Replaying the same
idempotency key returns the original release or link. `required_kinds` on the
gate route is an optional comma-separated bounded list; when omitted, the
default gate requires migration, backup/restore, topology, provider, and
external-effect evidence. The gate is ready only when every required link is
passed, resolved or not applicable, and bound to the current snapshot. It
returns a stable `gate_hash` and never performs a live check.

The equivalent CLI commands are `fornix qualification release-register`,
`release-list`, `release-get`, `evidence-link`, `evidence-list`, and
`release-gate`. This is a release/evidence index, not a substitute for
deployment-owned backup, HA, provider, or external-effect authorities.

Task 92 adds a bounded deployment-owned evidence refresh. A refresh accepts
only references to already accepted signed imports and atomically supersedes
the selected active links. The deployment remains responsible for producing
the observation; Fornix does not contact it or extend an evidence expiry.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `POST` | `/v1/qualification/releases/{id}/refresh` | Validate and atomically refresh selected evidence links | `qualification:admin` |
| `POST` | `/v1/qualification/releases/{id}/refresh/plan` | Perform the same validation as a forced dry-run | `qualification:read` |
| `GET` | `/v1/qualification/releases/{id}/refreshes` | List bounded refresh reports | `qualification:read` |
| `GET` | `/v1/qualification/releases/{id}/refreshes/{refresh_id}` | Read one refresh report | `qualification:read` |

The request requires `workspace_id`, `deployment_id`, an explicit RFC3339
`as_of`, an idempotency key, and one to six unique `{kind, import_id}` items.
`kind` is limited to `release`, `migration`, `backup_restore`, `topology`,
`provider`, and `external_effect`. When an active link exists, its exact
`supersedes_link_id` is required. The fixed `as_of` is used for expiry checks,
so the same request is replay-comparable. A refresh report contains only
release/link IDs, outcomes, stable hashes, predecessor IDs, boundary expiry,
and bounded reasons. It never contains signed bytes, credentials, prompts,
URLs, or deployment payloads.

`dry_run: true` and `/refresh/plan` roll back after validation and create no
refresh, link, or lifecycle rows. A committed refresh stores one append-only
run, bounded item rows, and one event in migration 076. Replaying the same
idempotency key returns the original report; conflicting reuse fails closed.
The equivalent CLI commands are `fornix qualification refresh`,
`refresh-plan`, `refresh-list`, and `refresh-get`. The refresh item file is a
bounded JSON object containing only an `items` array of typed references.

Task 82 adds the hash-only release verification and admission surface. A
deployment-owned verifier supplies the artifact hash, attestation hash, and
the exact Task 81 gate hash; Fornix binds those facts to the release and
current trust snapshot transactionally. It does not accept raw manifests,
signatures, image layers, tokens, or credentials.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `POST` | `/v1/qualification/releases/{id}/verification` | Register or dry-run one release/artifact verification binding | `qualification:admin` |
| `GET` | `/v1/qualification/releases/{id}/verification` | Read one bounded verification by `artifact_kind` | `qualification:read` |
| `POST` | `/v1/qualification/releases/{id}/verification/revoke` | Revoke a verification without deleting history | `qualification:admin` |
| `GET` | `/v1/qualification/releases/{id}/admission` | Evaluate the read-only startup/admission decision | `qualification:read` |

`artifact_kind` is one of `release`, `image`, `binary`, or `manifest`.
`release-admission` returns a stable `decision_hash`; it is ready only when
the qualification gate is ready, the verification is current and unrevoked,
the optional requested artifact hash matches, and all trust/gate bindings
match. Expiry is evaluated without mutating the verification row. Production
can require this decision before readiness with
`FORNIX_REQUIRE_QUALIFICATION_RELEASE=true` (enabled automatically for
`FORNIX_ENV=production`) and an explicit
`FORNIX_QUALIFICATION_RELEASE_ID`. The API is an admission projection, not a
deployment executor.

The equivalent CLI commands are `fornix qualification release-verify`,
`verification-get`, `verification-revoke`, and `release-admission`.

Task 80 adds a bounded deployment trust snapshot between the signer catalog
and new authorized imports. A snapshot is scoped to one workspace and
deployment, has a monotonically increasing revision, carries only public
signer material, and is signed by a currently trusted Task 79 catalog signer.
The exact signed bytes and source hash are retained. New imports resolve the
current snapshot in the same transaction and retain its revision and hash;
historical Task 79 imports remain readable with empty snapshot fields and are
never rewritten. Revoking a snapshot or its publisher blocks future imports
but preserves disclosure and audit history. Snapshot publication is bounded
and idempotent by `(workspace_id, deployment_id, snapshot_hash)` and the
idempotency key. The equivalent CLI commands are `snapshot-sign`,
`snapshot-publish`, `snapshot-list`, `snapshot-get`, and `snapshot-revoke`.
When `FORNIX_REQUIRE_QUALIFICATION_TRUST=true` (automatic in production),
the configured `FORNIX_QUALIFICATION_DEPLOYMENT_ID` must have a current
snapshot for each workspace loaded at startup before readiness is reported.

## Validation policy packs

Validation policies are workspace-scoped, declarative, content-addressed
admission snapshots. Their rules reference only validators registered in the
running binary; policies cannot contain executable code, shell commands,
credentials, prompts, or arbitrary callbacks. A version is immutable after
creation. Only active versions admit new work, while retirement preserves all
historical references. Rollback is activation of an earlier immutable version.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `GET` | `/v1/policies` | List authorized policy versions with bounded pagination | `policy:read` |
| `POST` | `/v1/policies` | Create one immutable policy version idempotently | `policy:create` |
| `GET` | `/v1/policies/{policy_id}/{version}` | Read an exact workspace policy version | `policy:read` |
| `POST` | `/v1/policies/{policy_id}/{version}/activate` | Activate an exact version | `policy:activate` |
| `POST` | `/v1/policies/{policy_id}/{version}/default` | Bind the workspace default to an active version | `policy:activate` |
| `POST` | `/v1/policies/{policy_id}/{version}/retire` | Retire a version from new admission | `policy:retire` |
| `POST` | `/v1/policies/resolve` | Resolve the active/default policy for admission | `policy:resolve` |
| `POST` | `/v1/policies/dry-run-resolve` | Resolve without writing lifecycle or audit state | `policy:resolve` |
| `POST` | `/v1/policies/compare` | Compare two exact versions in one workspace | `policy:compare` |
| `GET` | `/v1/policies/audit` | Read bounded lifecycle audit history | `policy:read` |

Use `Idempotency-Key` on policy creation and lifecycle mutations. Reusing a
key with the same normalized payload returns the original durable result;
changing the payload fails closed. A request can tighten a policy's budgets or
approval mode, but cannot widen limits or weaken the mandatory
`change.preconditions`, `change.files`, `change.safety`, and `change.tree`
validators. Workspace isolation, actor propagation, task fencing, evidence
integrity, append-only history, and replay safety are non-disableable floors.

Change proposals and validation runs pin the exact policy ID, version, and
hash. Those fields continue through approvals, applications, validation
handoffs, Work Receipts, events, observations, and cost records. Replay uses
the recorded policy snapshot and never consults a newer default or invokes
external work. Policy selection is optional for compatibility; when omitted,
the historical deterministic validator behavior remains in force.

The CLI exposes `fornix policy list`, `create`, `get`, `activate`, `default`,
`retire`, `resolve`, `dry-run-resolve`, `compare`, and `audit`. The MCP shim
exposes matching `fornix__policy_*` tools. All three surfaces use the same
workspace authorization and redaction rules.

## Deployment-owned qualification refresh schedules

Task 93 adds a durable handoff for recurring freshness without making Fornix
the deployment executor. The schedule API stores only bounded identities,
hashes, lease metadata, and references to Task 92 refresh reports.

| Method | Route | Permission | Purpose |
| --- | --- | --- | --- |
| `POST` / `GET` | `/v1/qualification/refresh-schedules` | `qualification:admin` / `qualification:read` | Register or list schedules |
| `GET` | `/v1/qualification/refresh-schedules/plan` | `qualification:read` | Inspect bounded due candidates |
| `POST` | `/v1/qualification/refresh-schedules/claim` | `qualification:admin` | Claim one due schedule with a fence |
| `GET` | `/v1/qualification/refresh-schedules/{id}` | `qualification:read` | Read current projection |
| `POST` | `/v1/qualification/refresh-schedules/{id}/renew` | `qualification:admin` | Renew exact owner/fence |
| `POST` | `/v1/qualification/refresh-schedules/{id}/release` | `qualification:admin` | Release exact owner/fence |
| `POST` | `/v1/qualification/refresh-schedules/{id}/complete` | `qualification:admin` | Commit a fenced attempt |
| `POST` | `/v1/qualification/refresh-schedules/{id}/pause`, `/resume`, `/cancel` | `qualification:admin` | Change durable schedule state |
| `GET` | `/v1/qualification/refresh-schedules/{id}/attempts` | `qualification:read` | Read append-only attempts |

Completion accepts only an attempt/fence/plan identity, bounded outcome, and
the ID/hash of an existing refresh report. A successful completion is accepted
only when the report is same-scope, fresh at the fixed `as_of`, passed, covers
the configured evidence kinds, and contains the required passed recovery drills
in its accepted signed imports. The schedule does not call a provider,
deployment, backup, failover, DNS, or secret system. Claim, renewal, evidence
mutation (when using `schedule_authorization`), and completion fail closed on
stale fences. The scheduler is at-least-once; replay the same idempotency key
after a crash rather than assuming remote exactly-once behavior.

## Generic workflow and external-effect verification

Generic workflows are workspace-scoped, durable orchestration over the common
operation, connector, policy, effect, evidence, artifact, and receipt
authorities. They are not repository-specific. Lifecycle mutations require an
explicit workflow lease fence; external-effect verification additionally
requires the current effect recovery fence and exact observed versions.

| Method | Route | Purpose | Required capability |
| --- | --- | --- | --- |
| `POST` | `/v1/workflows` | Create an idempotent typed workflow from an operation and plan | `operation:create` |
| `GET` | `/v1/workflows/{id}` | Read the bounded workflow projection | `operation:read` |
| `POST` | `/v1/workflows/{id}/lease` | Acquire or take over the workspace-scoped workflow lease | `operation:execute` |
| `POST` | `/v1/workflows/{id}/renew` | Renew the exact workflow lease | `operation:execute` |
| `POST` | `/v1/workflows/{id}/release` | Release the exact workflow lease | `operation:execute` |
| `POST` | `/v1/workflows/{id}/advance` | Execute the next bounded deterministic steps | `operation:execute` |
| `POST` | `/v1/workflows/{id}/approve` | Resolve an explicit approval wait | `tool:approve` |
| `POST` | `/v1/workflows/{id}/verify` | Verify and reconcile one reserved external effect | `operation:execute` |
| `POST` | `/v1/workflows/{id}/cancel` | Record a durable cancellation | `operation:execute` |
| `POST` | `/v1/workflows/{id}/replay` | Replay recorded workflow transitions without effects | `operation:read` |
| `GET` / `POST` | `/v1/workflows/{id}/receipt` | Read or finalize a replay-verified Work Receipt | `receipt:read` / `receipt:write` |

The verify request must include `X-Operation-Fence`, `X-Effect-Fence`,
`step_id`, `expected_effect_version`, `expected_link_version`, and a stable
`idempotency_key`; task-bound workflows also require `X-Task-Fence`. The
registered connector verifier receives normalized hashes and bounded provider
identifiers, never raw payloads or credentials. The generic effect transition
and domain-effect-link transition commit atomically in Postgres. A duplicate
after that commit reconstructs the proof without invoking the verifier again,
but still requires the current recovery lease. External execution remains
at-least-once and is never described as exactly-once.

For dispatcher paths that request a generic operation result, terminal effect
state, domain-effect link, operation result, operation transition, and result
authority link share one Postgres commit. A duplicate replays the immutable
result. If a historical terminal effect has no requested result, Fornix
returns a recovery-required error and does not invoke the external adapter
again. An acknowledged but unfinalized effect also remains non-dispatchable
until domain-specific reconciliation resolves it.

The CLI's `fornix workflow verify` command derives a bounded default
idempotency key from the workspace, workflow, step, and expected effect/link
versions. Repeating a request for the same proof state replays the same
attempt. After an inconclusive outcome, inspect the current versions and use
those values for a new attempt; the derived key will change. An explicit
`--idempotency` value overrides this behavior and remains the caller's
responsibility to manage.

The equivalent CLI commands are `fornix workflow create`, `lease`, `renew`,
`release`, `advance`, `approve`, `verify`, `cancel`, `replay`, and `receipt`.

## Compatibility data-plane routes

These routes support the original memo, chunk, symbol, coordination, and
federation integrations. New workflows should prefer the durable ingestion,
retrieval, evidence, artifact, task, and agent-run surfaces above when their
stronger lineage and replay semantics are required.

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/v1/memo` | Create a workspace-scoped memo |
| `GET` / `PUT` / `DELETE` | `/v1/memo/{id}` | Read or mutate a compatibility memo record |
| `POST` | `/v1/memo/search` | Search memo records |
| `POST` | `/v1/memo/backfill` | Run bounded embedding backfill |
| `POST` | `/v1/chunks` | Upsert a compatibility chunk/index record |
| `POST` | `/v1/symbol` | Upsert a compatibility symbol record |
| `POST` | `/v1/symbol/search` | Search symbols |
| `POST` | `/v1/symbol/edge` | Add a symbol graph edge |
| `POST` | `/v1/symbol/reindex` | Rebuild the compatibility symbol index |
| `GET` | `/v1/symbol/{id}/callers` or `/callees` | Read bounded symbol neighbors |
| `POST` | `/v1/federation/peer` | Register or revise a workspace-scoped peer |
| `GET` | `/v1/federation/peers` | List workspace-scoped peers |
| `POST` | `/v1/federation/peer/poll` | Poll a peer through managed credentials and controlled egress |

Compatibility writes remain workspace-authorized. Historical federation
coordination imports remain quarantined and must not be used as a substitute
for the new peer/poll authority. Peer configuration is append-only in
Postgres; raw tokens, response bodies, and credentials are never durable.

## Multi-domain incident reference workflow

The incident surface is a fake-first qualification workflow for demonstrating
the universal control-plane contract outside repository maintenance. It accepts
a bounded typed event, creates an idempotent incident identity, executes
read-only investigation steps, pauses before remediation, records an approval
or rejection, and exposes a replay-only result. The default connector never
contacts a monitoring, deployment, cloud, database, or ticketing system.

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/v1/incident/workflows` | Ingest a bounded incident event and advance a workflow until approval or terminal state |
| `GET` | `/v1/incident/workflows/{run_id}` | Read the authorized incident, workflow, evidence, and receipt summary |
| `POST` | `/v1/incident/workflows/{run_id}/approve` | Record an approval or rejection bound to the exact waiting step |
| `POST` | `/v1/incident/workflows/{run_id}/replay` | Verify recorded transitions from a checkpoint without external effects |

Use a stable `Idempotency-Key` for event delivery and approval. A repeated
delivery with the same workspace/source/external identity and payload hash is
read-only; a changed payload is rejected as a conflict. Approval decisions
are durable and auditable. A rejection is terminal and cannot unlock the
remediation step. All routes require workspace authorization and preserve
actor, request, causation, correlation, evidence, and operation references.

The equivalent CLI commands are `fornix incident start`, `get`, `approve`,
and `replay`; the MCP shim exposes `fornix__incident_start`, `get`, `approve`,
and `replay`. These surfaces intentionally share the same HTTP semantics.

## CLI and MCP equivalence

The `fornix` CLI and MCP compatibility shim call the same workspace-scoped
HTTP semantics. Start with the deterministic reference workflow:

```sh
make build
bin/fornix reference-workflow \
  --workspace reference-local \
  --fixture fixtures/reference-repo \
  --workdir /workspace/fixtures/reference-repo
```

Use [`DEVELOPMENT.md`](../DEVELOPMENT.md) for bootstrap, key lifecycle,
ingestion, task, evaluation, and smoke commands. Generated IDs and one-time
API-key tokens are intentionally not stable documentation values.

## Compatibility and versioning

The current API is an alpha surface. Adding fields should be backward
compatible where possible; changing workspace scope, idempotency semantics,
authority, or disclosure behavior requires an architecture note and tests.
Keep legacy routes documented as compatibility routes until they are removed,
and never silently replace an authoritative record with a projection or
summary.
