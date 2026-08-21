#!/usr/bin/env bash
# Loop 11 durable agent-run scheduler smoke.
set -euo pipefail

url="${FORNIX_URL:-http://localhost:8201}"
key="${FORNIX_KEY:-}"
if [[ -z "${key}" ]]; then echo "FORNIX_KEY is required" >&2; exit 1; fi

stamp=$(date +%s%N)
run_key="scheduler-smoke-${stamp}"
body=$(jq -cn --arg key "${run_key}" '{workspace_id:"default",request_id:($key+"-request"),idempotency_key:$key,goal:"Return one scheduler smoke response",provider:{provider:"fake",model:"fake-model"},budget:{max_turns:2,max_model_steps:2,max_tool_calls:2,max_context_bytes:65536,max_output_tokens:128,max_wall_time_ms:60000,max_cost_usd:1,max_tool_attempts:1}}')
headers=(-H "Authorization: Bearer ${key}" -H 'Content-Type: application/json' -H "Idempotency-Key: ${run_key}")
first=$(curl --fail --silent --show-error "${headers[@]}" -X POST "${url}/v1/agent/run" -d "${body}")
run_id=$(echo "${first}" | jq -r '.run.id')
[[ -n "${run_id}" && "${run_id}" != "null" ]] || { echo "scheduler smoke run id missing: ${first}" >&2; exit 1; }
[[ "$(echo "${first}" | jq -r '.run.state')" == "succeeded" ]] || { echo "scheduler smoke run did not settle: ${first}" >&2; exit 1; }

second=$(curl --fail --silent --show-error "${headers[@]}" -X POST "${url}/v1/agent/run" -d "${body}")
[[ "$(echo "${second}" | jq -r '.deduplicated')" == "true" ]] || { echo "scheduler smoke duplicate was not durable: ${second}" >&2; exit 1; }

status=$(curl --fail --silent --show-error -H "Authorization: Bearer ${key}" "${url}/v1/agent/run/${run_id}?workspace_id=default")
[[ "$(echo "${status}" | jq -r '.state')" == "succeeded" ]] || { echo "scheduler smoke status changed: ${status}" >&2; exit 1; }

echo "all v0.20 durable agent-run scheduler smokes passed (run=${run_id})"
