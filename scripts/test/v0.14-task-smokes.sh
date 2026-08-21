#!/usr/bin/env bash
# Loop 5 task-execution smoke — transactional leases, dependencies, retries,
# dead-letter, cancellation, duplicate delivery, and HTTP compatibility.
set -euo pipefail

PG_DSN="${FORNIX_TASK_PG_DSN:?FORNIX_TASK_PG_DSN is required}"
TEST_PG_DSN="${PG_DSN/localhost/host.docker.internal}"
TEST_PG_DSN="${TEST_PG_DSN/127.0.0.1/host.docker.internal}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${TEST_PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/store -run '^TestTask' -count=1 -v

if [[ -n "${FORNIX_URL:-}" && -n "${FORNIX_KEY:-}" ]]; then
  H=(-H "Authorization: Bearer ${FORNIX_KEY}" -H "Content-Type: application/json")
  stamp=$(date +%s%N)
  workspace="task-smoke-${stamp}"
  session="task-worker-${stamp}"

  curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/session" \
    -d "{\"workspace_id\":\"${workspace}\",\"id\":\"${session}\",\"host\":\"task-smoke\",\"capabilities\":[\"root\"]}" >/dev/null
  root=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task" \
    -d "{\"workspace_id\":\"${workspace}\",\"title\":\"task-root-${stamp}\",\"brief\":\"root\",\"created_by\":\"task-smoke\",\"required_capabilities\":[\"root\"]}" | jq -r .id)
  child=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task" \
    -d "{\"workspace_id\":\"${workspace}\",\"title\":\"task-child-${stamp}\",\"brief\":\"child\",\"created_by\":\"task-smoke\",\"max_attempts\":2,\"depends_on\":[${root}]}" | jq -r .id)

  claimed=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/claim" \
    -d "{\"workspace_id\":\"${workspace}\",\"session_id\":\"${session}\"}")
  fence=$(echo "${claimed}" | jq -r .fence)
  [[ "${claimed}" =~ "\"id\":${root}" ]] || { echo "root was not claimed" >&2; exit 1; }
  curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/${root}/renew" \
    -d "{\"workspace_id\":\"${workspace}\",\"session_id\":\"${session}\",\"fence\":${fence}}" >/dev/null
  completed=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/${root}/complete" \
    -d "{\"workspace_id\":\"${workspace}\",\"session_id\":\"${session}\",\"fence\":${fence},\"status\":\"done\",\"result\":\"root complete\",\"idempotency_key\":\"root-${stamp}\"}")
  duplicate=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/${root}/complete" \
    -d "{\"workspace_id\":\"${workspace}\",\"session_id\":\"${session}\",\"fence\":${fence},\"status\":\"done\",\"result\":\"root complete\",\"idempotency_key\":\"root-${stamp}\"}")
  [[ "$(echo "${completed}" | jq -r .deduped)" == false && "$(echo "${duplicate}" | jq -r .deduped)" == true ]] || { echo "completion was not idempotent" >&2; exit 1; }

  claimed=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/claim" \
    -d "{\"workspace_id\":\"${workspace}\",\"session_id\":\"${session}\"}")
  child_fence=$(echo "${claimed}" | jq -r .fence)
  [[ "${claimed}" =~ "\"id\":${child}" ]] || { echo "child was not claimable after root" >&2; exit 1; }
  curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/${child}/fail" \
    -d "{\"workspace_id\":\"${workspace}\",\"session_id\":\"${session}\",\"fence\":${child_fence},\"error\":\"temporary\",\"failure_class\":\"transient\",\"retryable\":true,\"retry_after_ms\":0,\"idempotency_key\":\"retry-${stamp}\"}" >/dev/null
  claimed=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/claim" \
    -d "{\"workspace_id\":\"${workspace}\",\"session_id\":\"${session}\"}")
  child_fence=$(echo "${claimed}" | jq -r .fence)
  curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/${child}/fail" \
    -d "{\"workspace_id\":\"${workspace}\",\"session_id\":\"${session}\",\"fence\":${child_fence},\"error\":\"permanent after retry\",\"failure_class\":\"transient\",\"retryable\":true}" >/dev/null
  status=$(curl -fsS "${H[@]}" "${FORNIX_URL}/v1/tasks?workspace_id=${workspace}" | jq -r ".tasks[] | select(.id==${child}) | .status")
  [[ "${status}" == "deadletter" ]] || { echo "child status was ${status}, expected deadletter" >&2; exit 1; }

  cancelled=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task" \
    -d "{\"workspace_id\":\"${workspace}\",\"title\":\"task-cancel-${stamp}\",\"brief\":\"cancel\",\"created_by\":\"task-smoke\"}" | jq -r .id)
  curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/${cancelled}/cancel" \
    -d "{\"workspace_id\":\"${workspace}\",\"reason\":\"smoke\",\"idempotency_key\":\"cancel-${stamp}\"}" >/dev/null
  status=$(curl -fsS "${H[@]}" "${FORNIX_URL}/v1/tasks?workspace_id=${workspace}" | jq -r ".tasks[] | select(.id==${cancelled}) | .status")
  [[ "${status}" == "cancelled" ]] || { echo "cancelled task status was ${status}" >&2; exit 1; }
  echo "all v0.14 task execution smokes passed (root=${root}, child=${child})"
else
  echo "all v0.14 task store smokes passed (HTTP portion skipped: FORNIX_URL/FORNIX_KEY not set)"
fi
