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
| `POST` | `/v1/tools/approvals/{id}/decide` | Record an approval decision |
| `POST` | `/v1/agent/run` | Create or resume a bounded agent run |
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

## Evaluation, observability, and compatibility surfaces

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/v1/observability/metrics` or `/v1/metrics` | Read an authorized bounded metrics snapshot |
| `POST` | `/v1/evaluations/datasets` | Register a deterministic evaluation dataset |
| `GET` / `POST` | `/v1/evaluations/retrieval/surfaces` | List or capture redacted retrieval surfaces |
| `POST` | `/v1/evaluations/retrieval/runs` | Run a bounded durable or dry-run evaluation |
| `GET` | `/v1/evaluations/runs/{id}` | Read evaluation status, metrics, gates, and report references |
| `POST` | `/v1/router/observation` | Record router telemetry |
| `POST` | `/v1/router/recommend` | Read a cost-aware provider recommendation |
| `POST` | `/v1/federation/coord/import` | Import a bounded coordination compatibility record |
| `GET` | `/v1/federation/coord/since/{sequence}` | Read bounded coordination history |

Observability and evaluation never store credentials or raw prompts in metric
dimensions or reports. Replay consumes recorded model/tool/retrieval history;
it does not call remote providers or execute external tools.

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
| `POST` | `/v1/federation/peer` | Register a coordination compatibility peer |
| `GET` | `/v1/federation/peers` | List registered peers |

Compatibility writes remain workspace-authorized. Their rows are not a reason
to bypass the append-only event, evidence, artifact, or ingestion authorities
when a durable workflow depends on replay or provenance.

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
