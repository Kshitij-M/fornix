#!/usr/bin/env bash
set -euo pipefail

dsn="${FORNIX_TEST_PG_DSN:-${PROJECTION_PG_DSN:-}}"
if [[ -z "${dsn}" ]]; then
  echo "universal effect smoke: FORNIX_TEST_PG_DSN or PROJECTION_PG_DSN is required" >&2
  exit 1
fi

FORNIX_TEST_PG_DSN="${dsn}" go test ./internal/store ./internal/server \
  -run 'Test(AdmissionStore(EffectRecoveryIsFencedAndReplayable|IndependentEffectLeaseRecoversAfterTerminalOperation)|GenericOperationHTTPReservesAndReconcilesExternalEffect)' \
  -count=1 -v
echo "universal effect smoke: dispatch intent, reservation, recovery fencing, duplicate reconciliation, and workspace isolation passed"
