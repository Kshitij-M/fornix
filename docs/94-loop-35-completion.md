# Loop 35 completion: terminal operation admission hardening

Status: implemented and locally verified; replay scalability and full recovery qualification remain open.

## Delivered

- Added a durable `ErrOperationTerminal` admission error.
- Rejected new operation leases after terminality.
- Rejected new attempt, external-effect, and callback reservations after
  terminality while preserving duplicate reads.
- Rejected generic connector execution before adapter work when a terminal
  operation has no result.
- Kept already-reserved external effects reconcilable after parent terminality.
- Added an integration test covering terminal lease and admission rejection.
- Added public error mapping and architecture documentation.

## Verification

The terminal-admission integration test and existing operation/effect HTTP
tests pass against the disposable qualification database. The full repository
quality gate remains the release check for this slice.

## Remaining limitations

This does not yet provide cursor-based snapshot replay, process-kill
qualification around uncertain provider acceptance, or a generic background
dispatch/verification worker. Those are separate production gates and must
not be inferred from terminal-state enforcement.
