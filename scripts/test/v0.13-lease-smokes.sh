#!/usr/bin/env bash
# Loop 4 consumer-lease smoke — ownership, fencing, takeover, and stale-owner rejection.
# Requires: Docker and a reachable Postgres database.
set -euo pipefail

PG_DSN="${FORNIX_LEASE_PG_DSN:?FORNIX_LEASE_PG_DSN is required}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/store ./internal/projection \
  -run 'TestConsumerLease|TestRunnerStaleLease|TestRunnerConcurrentConsumers' \
  -count=1 -v

echo "all v0.13 consumer lease smokes passed."
