#!/usr/bin/env bash
# Loop 9 tool registry/policy/approval/execution smoke.
set -euo pipefail

url="${FORNIX_URL:-http://localhost:8201}"
key="${FORNIX_KEY:-}"
pg_dsn="${FORNIX_TOOL_PG_DSN:-${FORNIX_TEST_PG_DSN:-}}"
if [[ -z "${key}" ]]; then echo "FORNIX_KEY is required" >&2; exit 1; fi

if [[ -n "${pg_dsn}" ]]; then
  go_image="${GO_IMAGE:-golang:1.25.13}"
  repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
  test_pg_dsn="${pg_dsn/localhost/host.docker.internal}"
  test_pg_dsn="${test_pg_dsn/127.0.0.1/host.docker.internal}"
  docker run --rm --add-host=host.docker.internal:host-gateway \
    -e FORNIX_TEST_PG_DSN="${test_pg_dsn}" \
    -v "${repo_root}:/workspace" -w /workspace "${go_image}" \
    go test ./internal/store -run '^TestToolRunStore' -count=1 -v
fi

stamp=$(date +%s%N)
key_id="tool-smoke-${stamp}"
body="{\"workspace_id\":\"default\",\"request_id\":\"${key_id}\",\"idempotency_key\":\"${key_id}\",\"tool_id\":\"fornix.echo\",\"argv\":[\"/bin/echo\",\"fornix-tool-smoke\"]}"
headers=(-H "Authorization: Bearer ${key}" -H 'Content-Type: application/json' -H "Idempotency-Key: ${key_id}")
first=$(curl --fail --silent --show-error "${headers[@]}" -X POST "${url}/v1/tools/execute" -d "${body}")
second=$(curl --fail --silent --show-error "${headers[@]}" -X POST "${url}/v1/tools/execute" -d "${body}")
[[ "$(echo "${first}" | jq -r '.result.stdout')" == "fornix-tool-smoke" ]] || { echo "tool output mismatch: ${first}" >&2; exit 1; }
[[ "$(echo "${second}" | jq -r '.deduplicated')" == "true" ]] || { echo "duplicate was not replayed: ${second}" >&2; exit 1; }

bad_body="{\"workspace_id\":\"default\",\"request_id\":\"bad-${stamp}\",\"idempotency_key\":\"bad-${stamp}\",\"tool_id\":\"unregistered.tool\",\"argv\":[\"/bin/echo\",\"must-deny\"]}"
status=$(curl --silent --show-error "${headers[@]}" -o /tmp/fornix-tool-deny.json -w '%{http_code}' -X POST "${url}/v1/tools/execute" -d "${bad_body}")
[[ "${status}" == "502" ]] || { echo "unregistered tool returned HTTP ${status}" >&2; exit 1; }

echo "all v0.18 tool registry/policy/execution smokes passed (run=${key_id})"
