# Loop 28 completion: multi-domain incident reference workflow

Status: implemented on the Issue [#42](https://github.com/Kshitij-M/fornix/issues/42) feature branch; fake-first alpha qualification complete, live production connector qualification pending.

## Outcome

Fornix now has a complete non-repository reference workflow that demonstrates
the universal control-plane contract from typed ingress to replayable proof:

```text
typed incident event
  → workspace-scoped idempotent ingestion
  → read-only incident and runbook context
  → bounded model investigation and diagnostics
  → report evidence/artifact
  → exact approval or rejection
  → fenced remediation record
  → verification
  → Work Receipt
  → inert replay
```

The workflow is intentionally deterministic and offline. Its fake incident and
remediation connectors prove the authority, admission, approval, fencing,
evidence, receipt, and replay seams without pretending that a fake external
system qualifies a live monitoring or remediation integration.

## Implemented surface

- Typed `IncidentEvent`, `Incident`, `IncidentApproval`, and
  `IncidentWorkflowResult` contracts with bounded payloads, normalized
  severity/delivery modes, content hashes, actor/workspace references, and
  explicit delivery identity.
- Migration `039_incident_workflow.sql` for the current incident projection,
  append-only delivery history, request idempotency, and append-only approval
  decisions.
- Transactional `IncidentStore` ingestion with evidence-backed raw payloads,
  duplicate delivery handling, natural-identity conflict rejection, durable
  `incident.received` events, workspace checks, and approval records bound to
  run/step/operation/plan hashes.
- Deterministic `fakeincident` and `fakeremediation` adapters registered
  through the existing capability registry. They are bounded, workspace
  scoped, and do not access live network or host state.
- `incident.Service` that composes the existing operation authority, workflow
  runtime, connector admission, model gateway, evidence, artifacts, and Work
  Receipts. It owns no second scheduler, lease table, or replay authority.
- Approval-gated progression. Read-only/observation recovery can be retried
  after a crash; an uncertain effectful step remains recovery-required and
  fails closed until a future reconciliation policy resolves it.
- HTTP routes, CLI commands, and MCP tools with equivalent request semantics:
  `start`, `get`, `approve`, and `replay`.
- A focused smoke script and CI/Make target covering the live server, HTTP,
  CLI, MCP, duplicate delivery, approval, replay, and workspace isolation.

## Safety and correctness decisions

The natural incident identity is `(workspace, source system, external event
ID)`. An identical repeat is a read-only duplicate. A different payload for
that identity is a conflict and cannot overwrite the authoritative incident.
Raw event payloads are preserved as immutable evidence; projections and
workflow rows carry references and hashes rather than a second raw copy.

Approval is not a boolean on the incident row. A decision is an append-only,
hash-bound record containing the workspace, run, waiting step, operation hash,
plan hash, actor, idempotency key, decision, and evidence reference. Approval
can resume the exact waiting step. Rejection is terminal and cannot unlock
the remediation step.

The remediation connector records an explicit at-least-once external-effect
boundary and provider idempotency key. Fornix does not claim exactly-once
remote execution. Replay reads committed transitions, evidence, artifacts,
and receipt references only; it never calls a connector, model, tool, network,
or approval callback.

Workspace and actor scope are checked at ingress, lookup, connector admission,
workflow execution, approval, evidence, artifact, and receipt boundaries.
Credentials, raw prompts, and arbitrary unredacted provider/monitoring output
are not placed in events, logs, metrics, or error messages.

## Reuse and licensing

The implementation reuses Fornix's existing Postgres authorities and typed
seams: operation identity and leases, workflow checkpoints, connector
registration/admission, model gateway, evidence, artifacts, receipts,
identity middleware, and workspace authorization. The design incorporates
patterns studied in Orloj, DeepSeek Harness, agentmemory, ClawMem, and
FornixDB without copying their source. Kronaxis-fabric's BSL 1.1 code was not
copied. Fornix remains MIT licensed and adds no dependency or infrastructure.

## Qualification evidence

The following checks were run while completing the slice:

```sh
go test ./...
FORNIX_TEST_PG_DSN="$TEST_DSN" go test ./internal/store ./internal/workflows/incident ./internal/connector ./internal/adapters/fakeincident -count=1
FORNIX_URL=http://127.0.0.1:18201 FORNIX_KEY=fornix-incident-smoke-key \
  scripts/test/v0.38-multidomain-smokes.sh
python3 -m py_compile scripts/fornix-mcp.py
sh -n scripts/test/v0.38-multidomain-smokes.sh
```

The clean-database integration run applied all migrations, including 039, and
passed duplicate, conflict, approval, rejection, workspace-isolation,
crash-before-checkpoint, replay, and receipt assertions. The live smoke passed
HTTP, CLI, and MCP paths on a local server at `127.0.0.1:18201` using the
deterministic provider. No OpenAI, Ollama, monitoring, deployment, cloud,
database, or ticketing credential was used.

Observed local timings were approximately 1.4 seconds for the complete
incident package on a warm local Postgres database, 3.8 seconds for the cached
repository unit suite, and 6.2 seconds for the focused store/incident
integration suite. These are development observations, not capacity or SLO
claims.

## Database, storage, and recovery profile

Ingest performs one bounded transaction for evidence deduplication, incident
identity/delivery handling, idempotency, and the received event. Each workflow
step uses the existing fenced workflow transition/checkpoint transactions.
Approval adds one bounded approval evidence record and one durable approval
record before resuming the exact waiting step. Completion adds immutable
workflow transitions, evidence/artifacts, and one idempotent Work Receipt.

Storage grows with immutable deliveries, events, evidence, artifacts,
workflow transitions, approvals, and receipt references. Duplicate payloads
reuse content-addressed evidence within a workspace. The migration preserves
history and does not introduce a second raw payload store. Rebuild/replay
reads bounded durable history and produces no external effects.

## Remaining limitations

Issue #42 is a reference workflow, not the end of universal qualification. The
remaining production work includes:

- signed ingress verification rather than accepting only pre-verified signed
  metadata;
- secret-manager-backed connector credentials and audited credential rotation;
- real monitoring, deployment, cloud, database, ticketing, and business-system
  connectors with domain-specific authorization and reconciliation;
- distributed worker scheduling, HA, backup/restore, load, quota, and SLO
  qualification;
- alert-storm control, durable external-effect reconciliation, and explicit
  compensation policies;
- broader multi-agent orchestration and operator UX;
- the Issue #40 universal execution-plane qualification evidence.

The correct product statement is therefore: Fornix now proves the universal
control-plane shape across a repository adapter and a non-repository incident
workflow, while remaining an alpha single-node platform whose live external
adapters require separate qualification.
