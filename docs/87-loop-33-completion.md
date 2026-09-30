# Loop 33 completion: external-effect reconciliation

Status: implemented on the universal production-qualification branch.

## Delivered

- Composed `AdmissionStore` into the authenticated server.
- Added workspace-scoped HTTP reservation at
  `POST /v1/operations/{id}/effects/reserve` and the explicit-ID form at
  `POST /v1/operations/{id}/effects/{effect_id}`.
- Added read-only state inspection at
  `GET /v1/operations/{id}/effects/{effect_id}`.
- Added fenced reconciliation at
  `POST /v1/operations/{id}/effects/{effect_id}/state`.
- Added CLI commands `effect-reserve`, `effect-get`, and `effect-state`.
- Kept all state-machine, lease, task-fence, idempotency, and append-only
  behavior in the existing Postgres stores.
- Added HTTP coverage for reservation, state reads, duplicate transitions,
  same-key conflicts, stale operation fences, and workspace isolation.
- Added a focused Make/CI smoke target and public API documentation.

## Safety behavior

The server overwrites workspace, operation, effect, and owner identity with
authenticated route values. It accepts only bounded effect metadata and hashes;
it never receives or stores provider payloads, credentials, headers, or raw
failure text. A connector remains responsible for making the remote call and
for deciding whether a `dispatched`, `acknowledged`, `recovery_required`,
`verified`, or `compensated` transition is justified.

Duplicate transitions return the existing committed state. Conflicting reuse
of an idempotency key is rejected. A stale operation fence is rejected before
the append-only transition or current-state projection can change.

## Verification

The focused qualification command is:

```sh
FORNIX_TEST_PG_DSN="$PROJECTION_PG_DSN" make smoke-universal-effects
```

The new Postgres-backed tests passed locally against the qualification
database, including the direct store recovery test and the authenticated HTTP
vertical slice. Full repository race tests, vet, documentation checks, and the
remaining smoke suite are required before merging this branch.

## Cost and latency

This slice adds no infrastructure or remote calls. Reservation and each state
transition are one bounded indexed Postgres transaction; state reads use the
workspace/effect primary key. The repository does not yet publish a stable
capacity number for this path, so local test latency is qualification evidence
only and not a production SLO.

## Remaining limitations

- No generic worker dispatches live effectful connectors yet.
- Provider callback ingestion, verification, and compensation remain adapter
  responsibilities.
- Remote delivery is explicitly at-least-once or unknown; exactly-once is not
  claimed.
- Tenant defense-in-depth, backup/restore drills, HA/failover, load/soak
  evidence, and operational alerting remain open production gates.
