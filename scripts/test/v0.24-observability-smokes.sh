#!/usr/bin/env bash
# Loop 15 durable observability, cost, and offline evaluation smoke.
set -euo pipefail

PG_DSN="${FORNIX_OBSERVABILITY_PG_DSN:?FORNIX_OBSERVABILITY_PG_DSN is required}"
TEST_PG_DSN="${PG_DSN/localhost/host.docker.internal}"
TEST_PG_DSN="${TEST_PG_DSN/127.0.0.1/host.docker.internal}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${TEST_PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/store ./internal/eval -run 'Test(Observability|ModelUsage|Evaluation|Replay)' -count=1 -v

if [[ -n "${FORNIX_URL:-}" && -n "${FORNIX_KEY:-}" ]]; then
  unauth_status=$(curl -sS -o /dev/null -w '%{http_code}' "${FORNIX_URL}/v1/observability/metrics?workspace_id=default")
  [[ "${unauth_status}" == "401" ]] || { echo "metrics endpoint did not reject unauthenticated request: ${unauth_status}" >&2; exit 1; }
  metrics=$(curl -fsS -H "Authorization: Bearer ${FORNIX_KEY}" "${FORNIX_URL}/v1/observability/metrics?workspace_id=default")
  [[ "$(echo "${metrics}" | jq -r '.workspace_id')" == "default" ]] || { echo "metrics workspace mismatch" >&2; exit 1; }
  [[ "$(echo "${metrics}" | jq -r '.schema_version')" == "1" ]] || { echo "metrics schema mismatch" >&2; exit 1; }
  echo "all v0.24 observability smokes passed"
else
  echo "all v0.24 observability store smokes passed (HTTP portion skipped: FORNIX_URL/FORNIX_KEY not set)"
fi
