# Universal operator runbook

Status: current alpha operator reference.

Important: this runbook demonstrates durable reservation and reconciliation
records only. Fornix does not dispatch generic provider effects; the current
execute route supports trusted read-only and observation capabilities.

This runbook demonstrates the domain-neutral operation lifecycle. It uses
hashes and references rather than raw provider payloads. Replace the example
IDs and hashes with values returned by the preceding command.

## Start a local, fake-first runtime

```sh
make build
./bin/fornix doctor
./bin/fornix start --repo .
```

Use `./bin/fornix demo --repo .` for the complete offline repository adapter
workflow. The steps below show the universal API boundary directly.

## Register, lease, and execute a read-only operation

Create a bounded request file for the adapter. The request contains typed
capability/resource references and hashes, not executable code or credentials.

```sh
fornix operation create --request-file operation-request.json
fornix operation lease --id OPERATION_ID
fornix operation execute --id OPERATION_ID
fornix operation get --id OPERATION_ID
fornix operation replay --id OPERATION_ID
```

The lease response contains the operation fence. Commands that mutate the
operation or an effect must send that fence. A duplicate execution returns
the durable result rather than invoking the connector a second time.

## Reserve an external effect

An effect file is reference-only:

```json
{
  "workspace_id": "local",
  "boundary": "example.ticketing",
  "class": "reversible_write",
  "delivery_guarantee": "at_least_once",
  "idempotency_key": "provider-command-123",
  "provider_idempotency_supported": true,
  "verification_required": true,
  "verification_status": "pending",
  "compensation_status": "available"
}
```

Reserve before the connector dispatches:

```sh
fornix operation effect-reserve \
  --id OPERATION_ID \
  --effect-file effect.json \
  --step-id STEP_ID \
  --attempt-id ATTEMPT_ID \
  --request-hash REQUEST_SHA256 \
  --fence FENCE
```

This command writes a durable reservation. It does not send a request to the
provider. The connector must retain the returned `effect_id` and provider
idempotency key while it performs its own bounded dispatch.

## Reconcile without claiming exactly-once

After a provider response, record only bounded identifiers and hashes:

```sh
fornix operation effect-state --id OPERATION_ID --effect-id EFFECT_ID \
  --state dispatching --idempotency dispatch-intent-1 --fence FENCE

fornix operation effect-state --id OPERATION_ID --effect-id EFFECT_ID \
  --state dispatched --provider-request-id PROVIDER_REQUEST_ID \
  --idempotency dispatch-1 --fence FENCE

fornix operation effect-state --id OPERATION_ID --effect-id EFFECT_ID \
  --state acknowledged --response-hash RESPONSE_SHA256 \
  --idempotency acknowledge-1 --fence FENCE

fornix operation effect-state --id OPERATION_ID --effect-id EFFECT_ID \
  --state verification_pending --idempotency verify-1 --fence FENCE

fornix operation effect-state --id OPERATION_ID --effect-id EFFECT_ID \
  --state verified --verification-hash VERIFICATION_SHA256 \
  --idempotency verified-1 --fence FENCE
```

If the worker or provider response is uncertain, use `recovery_required` and
investigate the provider by its idempotency key. Never treat a lost response
as proof that the request was not accepted. A stale worker cannot append a
transition after lease takeover. Repeating a committed idempotency command is
read-only; changing its logical state is a conflict.

For recovery after the original operation lease expires, discover and claim
the effect with the separate effect fence:

```sh
fornix operation effect-recovery --limit 32
fornix operation effect-lease --id OPERATION_ID --effect-id EFFECT_ID
fornix operation effect-state --id OPERATION_ID --effect-id EFFECT_ID \
  --state recovery_required --idempotency recovery-1 --effect-fence EFFECT_FENCE
```

## What this runbook does not do

The current generic execute route performs only trusted read-only and
observation capabilities. The effect surface is a control-plane authority,
not a universal dispatcher. It does not provide generic cloud changes,
ticketing writes, verification, compensation, OAuth/SSO, a kernel sandbox,
HA, or exactly-once provider execution.
