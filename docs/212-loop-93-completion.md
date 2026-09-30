# Loop 93 completion — qualification refresh scheduling and recovery handoff

Status: implemented on `feat/issue-40-production-qualification`

## Outcome

Fornix now has a durable workspace-scoped handoff for recurring deployment
qualification refreshes. A deployment-owned scheduler can register one bounded
schedule for a deployment release, claim due work with an expiring monotonic
fence, renew or release it, submit a Task 92 refresh reference, and complete
the attempt transactionally. Attempts and lifecycle events are append-only;
the schedule is the only mutable projection.

The implementation deliberately does not execute deployments, providers,
backups, PITR, failover, DNS, mTLS, workload identity, secret resolution, or
external cron. It accepts only already-authorized signed evidence and report
references. The external boundary remains at-least-once and is explicit.

## Delivered files and surfaces

- `internal/contracts/qualification_schedule.go`: bounded schedule, claim,
  completion, attempt, page, lease, backoff, and plan-hash contracts.
- `internal/contracts/qualification_refresh.go`: optional schedule
  authorization envelope for stale-worker rejection before evidence mutation.
- `internal/store/migrations/077_qualification_refresh_scheduler.sql`:
  schedule projection, append-only attempts/events, constraints, indexes, and
  workspace RLS.
- `internal/store/qualification_schedule.go`: registration, deterministic due
  claim, renewal, release, completion, state transitions, pagination, report
  and signed recovery-drill validation.
- `internal/server/qualification_schedule.go`: authenticated HTTP API.
- `cmd/fornix/qualification_cli.go`: strict bounded schedule file and
  `schedule-*` operator commands.
- `Makefile` and `.github/workflows/ci.yml`: focused qualification target and
  CI integration job.

## Safety and recovery semantics

1. Claim locks one due schedule and increments its fence in one transaction.
2. A live owner is unique per schedule; expiry permits takeover with a higher
   fence. Stale renewals, releases, completions, and refresh mutations fail
   closed.
3. A successful completion requires a same-workspace, same-deployment,
   same-release Task 92 report with the claimed `as_of`, matching hash, fresh
   evidence, required evidence kinds, and required passed recovery drills from
   the signed imports referenced by that report.
4. Success schedules the next cadence and resets the consecutive retry budget
   without reusing an attempt identity. Retryable failures use capped
   exponential backoff; exhaustion becomes `dead_letter`. Non-retryable
   failures pause the schedule.
5. Schedule projection, attempt row, and lifecycle event commit together. An
   uncertain caller replays the same idempotency key; no remote exactly-once
   guarantee is claimed.

## Verification

Verified without starting Docker, PostgreSQL, a model provider, or any
external system:

- `go test ./...` — passed.
- Focused contract, store, server, and CLI packages — passed.
- Contract tests cover stable configuration hashes, unknown requirement
  rejection, fence-bound plan hashes, backoff caps, and completion validation.
- PostgreSQL integration tests cover registration replay, concurrent claim
  ownership, expiry takeover, stale fencing, schedule-authorized refresh
  rejection, pause/resume, and state integrity. They are safely skipped when
  `FORNIX_TEST_PG_DSN` is unset.

Still requiring disposable PostgreSQL execution in CI or locally:

- migration 077 on fresh and existing catalogs;
- RLS and role-separated cross-workspace checks;
- concurrent claim/renew/complete behavior against real locks;
- rollback/crash injection around claim and completion;
- recovery-drill success and rejection fixtures with signed imports;
- measured latency, SQL work, and relation growth.

The focused command is:

```sh
FORNIX_TEST_PG_DSN='postgres://...' make qualification-deployment-refresh-scheduler
```

## Cost and storage impact

Each schedule stores one small projection row. Each claim/completion adds one
bounded attempt and one lifecycle event. Raw signed imports are referenced,
not copied. Claims and completions use one locked Postgres transaction; no
broker, Redis, NATS, object store, model, or provider is added.

## Remaining limitations

Task 93 is a durable control-plane handoff, not a hosted scheduler or a proof
that a deployment actually performed a recovery drill. Production readiness
still needs deployment-owned live evidence, backup/restore and HA drills,
secret/identity and egress qualification, load/soak measurements, operational
alerting, and an explicit scheduler composition outside Fornix.
