# Loop 43 completion: adapter-owned generic operation worker

Status: implemented and locally qualified on the current production-
qualification branch.

Delivered:

- a bounded `internal/operationworker` package that consumes explicit
  workspace-scoped `OperationStore.ClaimReady` batches;
- stable owner identity, bounded claim batch, lease TTL, heartbeat, polling,
  and cancellation defaults;
- handler delivery of the original operation request and exact operation fence;
- heartbeat cancellation and fail-closed reporting when a lease is lost;
- successful-handler lease release and failure behavior that leaves the claim
  to expiry for takeover instead of hot-looping;
- concurrent-worker, heartbeat, failed-handler, expiry/takeover, and
  duplicate-claim integration coverage;
- operator CLI smoke coverage for the queue claim path alongside the existing
  fenced lease, stale-owner, transition, and replay checks;
- feature and production-readiness documentation that states the adapter and
  external-effect boundaries explicitly.

Qualification run:

```text
go test ./internal/operationworker ./internal/store -run 'TestWorker|TestOperationQueue' -count=1
```

The tests passed against the local migrated Postgres authority. The worker
does not call a model, tool, connector, broker, or remote system by itself.
The callback owns adapter admission, plan persistence, result/transition
commit, and the separate external-effect reservation/reconciliation path.

This loop closes the claim-consumption boundary without over-claiming a
universal dispatcher. Global fairness, resource-level serialization, provider
qualification, autoscaling, HA/failover, and external secret-manager
integration remain production gates.
