#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
url=${FORNIX_URL:-http://localhost:8201}
key=${FORNIX_KEY:-}
workspace="incident-smoke-$$"
auth="Authorization: Bearer $key"

request() {
  method=$1
  path=$2
  body=${3-}
  curl --fail --silent --show-error -H "$auth" -H "X-Workspace-ID: $workspace" -H "Content-Type: application/json" -X "$method" "$url$path" -d "$body"
}

# Keep smoke failures actionable without ever printing request payloads,
# credentials, or arbitrary response text into CI logs.
assert_json() {
  label=$1
  document=$2
  predicate=$3
  if ! printf '%s' "$document" | jq -e "$predicate" >/dev/null; then
    printf 'multi-domain incident smoke: %s assertion failed\n' "$label" >&2
    printf '%s' "$document" | jq -c '{workflow_id: .workflow.id, workflow_status: .workflow.status, workflow_failure_code: (.workflow.failure.code // null), failed_steps: [(.workflow.steps // [])[] | select(.status == "failed" or .status == "recovery_required") | {ordinal,kind,status,failure_code:(.failure.code // null)}], incident_status: .incident.status, workspace_id: .incident.workspace_id, duplicate, replay_verified, replay_hash_present: (.replay_hash != null and .replay_hash != "")}' >&2 || true
    exit 1
  fi
}

event=$(printf '%s' "{\"workspace_id\":\"$workspace\",\"source_system\":\"smoke-monitor\",\"external_id\":\"incident-1\",\"severity\":\"warning\",\"summary\":\"bounded smoke incident\",\"delivery_mode\":\"fake\",\"idempotency_key\":\"incident-smoke-$workspace\",\"payload\":{\"service\":\"payments\",\"status\":\"degraded\"}}")
body=$(printf '%s' "{\"workspace_id\":\"$workspace\",\"idempotency_key\":\"incident-smoke-$workspace\",\"event\":$event}")
started=$(request POST /v1/incident/workflows "$body")
assert_json 'start pauses for approval in the selected workspace' "$started" '.workflow.status == "awaiting_approval" and .incident.workspace_id == "'"$workspace"'"'
run_id=$(printf '%s' "$started" | jq -r '.workflow.id')

duplicate=$(request POST /v1/incident/workflows "$body")
assert_json 'duplicate delivery returns the original run' "$duplicate" '.duplicate == true and .workflow.id == "'"$run_id"'"'

approved=$(request POST "/v1/incident/workflows/$run_id/approve" "{\"workspace_id\":\"$workspace\",\"decision\":\"approve\",\"idempotency_key\":\"approval-smoke-$workspace\"}")
assert_json 'approval completes the workflow with receipt and replay proof' "$approved" '.workflow.status == "succeeded" and .incident.status == "resolved" and .receipt != null and .replay_verified == true'

replayed=$(request POST "/v1/incident/workflows/$run_id/replay?workspace_id=$workspace" '{}')
assert_json 'replay verifies the durable workflow history' "$replayed" '.replay_verified == true and .replay_hash != ""'

if curl --silent --output /dev/null --write-out '%{http_code}' -H "$auth" -H 'X-Workspace-ID: other-workspace' "$url/v1/incident/workflows/$run_id?workspace_id=other-workspace" | grep -q '^2'; then
  echo "incident smoke: cross-workspace read unexpectedly succeeded" >&2
  exit 1
fi

if [ -x "$repo_root/bin/fornix" ]; then
  cli_workspace="incident-cli-smoke-$$"
  cli_started=$(FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$cli_workspace" "$repo_root/bin/fornix" incident start --external-id cli-incident --json)
  cli_run=$(printf '%s' "$cli_started" | jq -r '.workflow.id')
  FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$cli_workspace" "$repo_root/bin/fornix" incident approve --id "$cli_run" --decision approve --json | jq -e '.workflow.status == "succeeded"' >/dev/null
fi

mcp_output=$(printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"fornix__incident_start","arguments":{"source_system":"mcp-monitor","external_id":"mcp-incident","payload":{"service":"search","status":"degraded"}}}}' \
  | FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="incident-mcp-smoke-$$" python3 "$repo_root/scripts/fornix-mcp.py")
if ! printf '%s\n' "$mcp_output" | jq -e 'select(.id == 1) | .result.tools | map(.name) | index("fornix__incident_start") != null' >/dev/null; then
  echo 'multi-domain incident smoke: MCP tool discovery assertion failed' >&2
  exit 1
fi
if ! printf '%s\n' "$mcp_output" | jq -e 'select(.id == 2) | .result.isError != true' >/dev/null; then
  echo 'multi-domain incident smoke: MCP start returned an error' >&2
  exit 1
fi
mcp_run=$(printf '%s\n' "$mcp_output" | jq -r 'select(.id == 2) | .result.content[0].text | fromjson | .workflow.id')
mcp_followup=$(printf '%s\n' \
  "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"fornix__incident_approve\",\"arguments\":{\"run_id\":\"$mcp_run\",\"decision\":\"approve\"}}}" \
  "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{\"name\":\"fornix__incident_replay\",\"arguments\":{\"run_id\":\"$mcp_run\"}}}" \
  | FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="incident-mcp-smoke-$$" python3 "$repo_root/scripts/fornix-mcp.py")
if ! printf '%s\n' "$mcp_followup" | jq -e 'select(.id == 3) | .result.content[0].text | fromjson | .workflow.status == "succeeded"' >/dev/null; then
  echo 'multi-domain incident smoke: MCP approval did not complete the workflow' >&2
  exit 1
fi
if ! printf '%s\n' "$mcp_followup" | jq -e 'select(.id == 4) | .result.content[0].text | fromjson | .replay_verified == true' >/dev/null; then
  echo 'multi-domain incident smoke: MCP replay did not verify workflow history' >&2
  exit 1
fi

printf '%s\n' "multi-domain incident smoke: duplicate delivery, approval, replay, isolation, CLI, and MCP passed ($workspace)"
