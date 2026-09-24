# Loop 34 completion: legacy global surface containment

Status: implemented and locally verified; follow-up isolation qualification remains open.

## Delivered

- Added `FORNIX_ENABLE_LEGACY_GLOBAL_SURFACES`, disabled by default.
- Rejected the flag in production configuration.
- Quarantined legacy federation and router routes before authorization and
  handler execution when the flag is disabled.
- Changed unknown route authorization from an implicit workspace-read grant to
  fail-closed route unavailability.
- Disabled the federation background poller in the safe default mode.
- Kept compatibility smokes explicit and opt-in.
- Added configuration and middleware tests for default deny, opt-in behavior,
  and production rejection.

## Verification

The focused configuration and server authorization tests pass. Shell scripts
were updated so the historical compatibility smoke path is skipped with a
clear message unless the operator explicitly enables the legacy flag. The
normal production runtime manifest keeps the flag false.

## Security interpretation

This loop reduces exposure; it does not certify the legacy schemas as
workspace-safe. No historical global data was reassigned. The next required
slice is a durable, workspace-scoped replacement and migration/quarantine
proof before these compatibility routes can be considered for re-enablement.

## Measured impact

Safe-mode request cost is an in-process boolean/path check. Safe mode avoids
the legacy polling goroutine and its database/network work. There is no new
durable storage. Compatibility mode retains its prior operational cost and
must be treated as a migration/test mode.
