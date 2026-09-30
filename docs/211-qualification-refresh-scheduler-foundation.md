# Qualification refresh scheduler and recovery handoff foundation

Status: implemented as a bounded foundation (Task 93)

## Purpose

Task 92 made one deployment-owned evidence refresh atomic and replayable. A
production control plane also needs a durable handoff for recurring freshness:
an operator must be able to declare when a release should be requalified,
claim one due handoff with a fenced worker, submit the resulting Task 92
refresh report, and recover safely if the worker or deployment publisher
crashes.

This feature adds that handoff only. It does not execute a deployment, call a
provider, run a backup/PITR/failover drill, resolve a secret, or operate a
production scheduler outside Fornix. The deployment scheduler remains the
authority for producing and signing observations; Fornix stores and gates the
accepted evidence.

## Invariants

- A schedule is scoped to one workspace, deployment, and release and has one
  immutable cadence/freshness/retry configuration.
- Expected qualification kinds and recovery-drill kinds are closed,
  normalized, bounded, and hash-stable. They are requirements, not evidence.
- A due schedule can have only one active owner. Claim, renewal, takeover,
  completion, pause, resume, and cancellation are transactional.
- Fences are monotonic. A stale owner cannot renew, complete, cancel, append
  an attempt, or advance `next_due_at`.
- Each attempt is append-only and idempotent by its request identity. A crash
  after the Task 92 refresh commits but before schedule completion is safe:
  the same refresh idempotency key is replayed, then the schedule completion
  is accepted with the still-valid fence or after a deterministic takeover.
- A successful schedule completion advances the next due time by the fixed
  cadence. A retryable failure uses bounded exponential backoff. Exceeding
  the consecutive retry budget moves the schedule to a paused/dead-lettered
  state rather than retrying forever; a successful cadence resets that retry
  budget while preserving the monotonic attempt sequence.
- A schedule completion must reference an existing refresh report in the same
  workspace, deployment, and release. It cannot invent a report hash.
- Required recovery drills are checked against the signed imports represented
  by that refresh report. A missing, stale, failed, or cross-scope drill fails
  closed; no recovery success is inferred from health or schedule completion.

## Crash and external-boundary semantics

The scheduler is at-least-once. A claim is an internal durable lease, not an
external execution guarantee. Fornix does not retry an uncertain provider,
backup, failover, or deployment operation. The deployment-owned publisher must
produce a new signed import or reuse the exact refresh idempotency key. The
only external boundary is the signed evidence handoff; remote exactly-once is
not claimed.

The schedule projection and attempt event commit together. A crash before
commit leaves the previous lease/projection unchanged. A crash after commit
leaves an auditable attempt and a deterministic next due time. Expired leases
can be taken over with a higher fence.

## Schema strategy

Migration 077 adds a workspace/deployment/release schedule projection, a
bounded append-only attempt table, and append-only schedule events. The
projection is mutable only through fenced store methods; its history remains
in events and attempts. RLS, scope checks, hash constraints, idempotency
uniqueness, and JSON-size bounds follow migrations 066–076.

## API and CLI

The authenticated surface supports schedule registration, due-plan claim,
renewal, completion, pause/resume/cancel, and bounded inspection. Mutation
routes require `qualification:admin`; inspection and due-plan reads require
`qualification:read`. Schedule completion accepts only the refresh report ID
and hash plus bounded outcome metadata; it accepts no raw deployment result,
credential, endpoint, log, or provider payload.

The CLI mirrors the API and uses explicit owner/fence values so operators can
see the lease boundary. A deployment-owned scheduler can use the same API or
the typed store seam; no daemon is silently started by this feature.

## Reuse and licensing

The implementation reuses Task 92 refresh/store contracts, signed import
verification, recovery-drill contracts, workspace transactions, qualification
RBAC, and the existing agent-run/operation lease patterns. No reference source
is copied. Kronaxis Fabric remains excluded because its BSL 1.1 license is not
compatible with the MIT distribution.

## Cost and storage budget

Each schedule adds one small projection row. Each claim/completion adds one
bounded attempt and event; raw signed bytes remain in the existing import
authority and are not duplicated. Claims perform one locked Postgres scope
transaction. Recovery validation reads only the bounded imports referenced by
the refresh report. No model, tool, provider, broker, Redis, NATS, object
store, or new infrastructure is introduced.

## Acceptance tests

- Contracts reject unbounded/unknown kinds, invalid cadence/retry windows,
  cross-workspace actors, and unsafe recovery requirements.
- Fresh and existing databases apply migration 077 cleanly.
- Registration is idempotent; conflicting immutable configuration fails.
- Due ordering and claim ownership are deterministic.
- Only one owner exists; renewals and takeovers increment fences.
- Stale, expired, released, and cross-workspace tokens fail closed.
- Completion references an existing same-scope refresh report and validates
  required signed recovery drills.
- Success, retryable failure, bounded backoff, and retry exhaustion produce
  deterministic next states.
- Duplicate completion creates one attempt/event and one schedule effect.
- Crash/rollback leaves no partial attempt or projection advancement.
- Pause, resume, and cancel prevent future claims as specified.
- HTTP/CLI authorization, redaction, pagination, and workspace isolation pass.
- Existing tests, race checks, builds, smokes, and replay remain green.

## Explicit limitations

This is a durable control-plane handoff, not a deployment executor, hosted
cron service, backup manager, HA controller, provider reconciler, secret
manager, or remote transaction coordinator. Production qualification still
requires deployment-owned live drills and measured SLO evidence.

## Delivered surface

The authenticated API is exposed under `/v1/qualification/refresh-schedules`:

- `POST` and `GET` the workspace/deployment/release schedule projection;
- `GET /plan` for bounded due-work inspection;
- `POST /claim`, `/{id}/renew`, `/{id}/release`, and `/{id}/complete` for
  fenced ownership and completion;
- `POST /{id}/pause`, `/{id}/resume`, and `/{id}/cancel` for durable state;
- `GET /{id}/attempts` for append-only attempt history.

Inspection requires `qualification:read`; mutations require
`qualification:admin`. The CLI mirrors these operations as
`fornix qualification schedule-*` commands. Schedule registration files use
strict bounded JSON and contain no endpoint, credential, raw report, or log
fields.

The Task 92 refresh request accepts an optional `schedule_authorization`
envelope. For a new refresh, the store locks the schedule and verifies the
exact owner, monotonic fence, attempt identity, plan hash, scope, and live
lease before any evidence link can change. An exact idempotency replay of an
already committed refresh remains readable after a worker crash, which gives
the deployment-owned publisher a safe at-least-once recovery path.
