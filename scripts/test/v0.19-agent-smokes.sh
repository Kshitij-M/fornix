#!/usr/bin/env bash
# Loop 10 deterministic bounded agent-loop smoke.
set -euo pipefail

url="${FORNIX_URL:-http://localhost:8201}"
key="${FORNIX_KEY:-}"
if [[ -z "${key}" ]]; then echo "FORNIX_KEY is required" >&2; exit 1; fi

stamp=$(date +%s%N)
run_key="agent-smoke-${stamp}"
body=$(jq -cn --arg key "${run_key}" '{workspace_id:"default",request_id:($key+"-request"),idempotency_key:$key,goal:"Return a deterministic smoke response",provider:{provider:"fake",model:"fake-model"},budget:{max_turns:2,max_model_steps:2,max_tool_calls:2,max_context_bytes:65536,max_output_tokens:128,max_wall_time_ms:60000,max_cost_usd:1,max_tool_attempts:1}}')
headers=(-H "Authorization: Bearer ${key}" -H 'Content-Type: application/json' -H "Idempotency-Key: ${run_key}")
first=$(curl --fail --silent --show-error "${headers[@]}" -X POST "${url}/v1/agent/run" -d "${body}")
run_id=$(echo "${first}" | jq -r '.run.id')
[[ "${run_id}" != "null" && -n "${run_id}" ]] || { echo "agent run id missing: ${first}" >&2; exit 1; }
[[ "$(echo "${first}" | jq -r '.run.state')" == "succeeded" ]] || { echo "agent run did not succeed: ${first}" >&2; exit 1; }
[[ "$(echo "${first}" | jq -r '.run.context_hash')" != "" ]] || { echo "context was not durably compiled: ${first}" >&2; exit 1; }

second=$(curl --fail --silent --show-error "${headers[@]}" -X POST "${url}/v1/agent/run" -d "${body}")
[[ "$(echo "${second}" | jq -r '.deduplicated')" == "true" ]] || { echo "duplicate agent run was not replayed: ${second}" >&2; exit 1; }
[[ "$(echo "${second}" | jq -r '.run.id')" == "${run_id}" ]] || { echo "duplicate returned a different run: ${second}" >&2; exit 1; }

replay=$(curl --fail --silent --show-error -H "Authorization: Bearer ${key}" -X POST "${url}/v1/agent/run/${run_id}/replay?workspace_id=default")
count=$(echo "${replay}" | jq -r '.count')
[[ "${count}" -ge 3 ]] || { echo "agent replay is incomplete: ${replay}" >&2; exit 1; }
[[ "$(echo "${replay}" | jq -r '.checkpoint.state_hash')" != "null" ]] || { echo "agent replay checkpoint hash missing: ${replay}" >&2; exit 1; }

echo "all v0.19 deterministic agent-loop smokes passed (run=${run_id}, events=${count})"
