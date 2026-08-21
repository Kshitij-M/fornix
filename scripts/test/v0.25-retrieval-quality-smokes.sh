#!/usr/bin/env bash
# Loop 16 deterministic retrieval quality, gold resolution, and regression smoke.
set -euo pipefail

PG_DSN="${FORNIX_EVAL_PG_DSN:?FORNIX_EVAL_PG_DSN is required}"
TEST_PG_DSN="${PG_DSN/localhost/host.docker.internal}"
TEST_PG_DSN="${TEST_PG_DSN/127.0.0.1/host.docker.internal}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${TEST_PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/eval -run 'Test(EvaluateRetrievalDataset|ScoreRankedEvidence|RetrievalGates)' -count=1 -v

echo "all v0.25 retrieval quality smokes passed"
