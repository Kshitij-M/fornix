#!/usr/bin/env bash
# Loop 3 projection smoke — checkpointed batches, replay, rollback, and isolation.
# Requires: Docker and a reachable Postgres database.
set -euo pipefail

PG_DSN="${FORNIX_PROJECTION_PG_DSN:?FORNIX_PROJECTION_PG_DSN is required}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/projection -run TestRunner -count=1 -v

echo "all v0.12 projection smokes passed."
