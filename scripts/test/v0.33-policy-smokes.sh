#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
url=${FORNIX_URL:-http://localhost:8201}
key=${FORNIX_KEY:-}
workspace="policy-smoke-$$"
trap 'rm -f "/tmp/fornix-policy-smoke-$workspace"*' EXIT INT TERM

request() {
  method=$1
  path=$2
  body=${3-}
  if [ -n "$body" ]; then
    curl --fail --silent --show-error -H "Authorization: Bearer $key" -H "X-Workspace-ID: $workspace" -H "Content-Type: application/json" -X "$method" "$url$path" -d "$body"
  else
    curl --fail --silent --show-error -H "Authorization: Bearer $key" -H "X-Workspace-ID: $workspace" -H "Content-Type: application/json" -X "$method" "$url$path"
  fi
}

create_body=$(printf '%s' "{\"workspace_id\":\"$workspace\",\"idempotency_key\":\"policy-create-$workspace\",\"pack\":{\"workspace_id\":\"$workspace\",\"policy_id\":\"repository-safety\",\"version\":\"1\",\"rules\":[],\"approval\":{\"mode\":\"required\"}}}")
created=$(request POST /v1/policies "$create_body")
printf '%s' "$created" | jq -e '.created == true and .deduped == false and .policy.policy_hash != ""' >/dev/null
policy_hash=$(printf '%s' "$created" | jq -r '.policy.policy_hash')

duplicate=$(request POST /v1/policies "$create_body")
printf '%s' "$duplicate" | jq -e '.created == false and .deduped == true and .policy.policy_hash == "'"$policy_hash"'"' >/dev/null

lifecycle_body=$(printf '%s' "{\"workspace_id\":\"$workspace\",\"policy_hash\":\"$policy_hash\",\"idempotency_key\":\"activate-$workspace\"}")
activated=$(request POST /v1/policies/repository-safety/1/activate "$lifecycle_body")
printf '%s' "$activated" | jq -e '.policy.status == "active" and .deduped == false' >/dev/null

activated_duplicate=$(request POST /v1/policies/repository-safety/1/activate "$lifecycle_body")
printf '%s' "$activated_duplicate" | jq -e '.policy.status == "active" and .created == false and .deduped == true' >/dev/null

default_body=$(printf '%s' "{\"workspace_id\":\"$workspace\",\"policy_hash\":\"$policy_hash\",\"idempotency_key\":\"default-$workspace\"}")
defaulted=$(request POST /v1/policies/repository-safety/1/default "$default_body")
printf '%s' "$defaulted" | jq -e '.policy.status == "active"' >/dev/null

resolved=$(request POST /v1/policies/resolve "{\"workspace_id\":\"$workspace\"}")
printf '%s' "$resolved" | jq -e '.resolution.selected == true and .resolution.ref.policy_hash == "'"$policy_hash"'" and (.resolution.validators | length) == 4' >/dev/null

dry_run=$(request POST /v1/policies/dry-run-resolve "{\"workspace_id\":\"$workspace\"}")
printf '%s' "$dry_run" | jq -e '.dry_run == true and .resolution.resolution_hash != ""' >/dev/null

comparison=$(request POST /v1/policies/compare "{\"workspace_id\":\"$workspace\",\"left\":{\"workspace_id\":\"$workspace\",\"policy_id\":\"repository-safety\",\"version\":\"1\",\"policy_hash\":\"$policy_hash\"},\"right\":{\"workspace_id\":\"$workspace\",\"policy_id\":\"repository-safety\",\"version\":\"1\",\"policy_hash\":\"$policy_hash\"}}")
printf '%s' "$comparison" | jq -e '.same == true and .hash != ""' >/dev/null

listed=$(request GET "/v1/policies?workspace_id=$workspace&limit=10")
printf '%s' "$listed" | jq -e '.policies | length == 1' >/dev/null
audited=$(request GET "/v1/policies/audit?workspace_id=$workspace&limit=10")
printf '%s' "$audited" | jq -e '.audit | length >= 3' >/dev/null

if [ -x "$repo_root/bin/fornix" ]; then
  FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$repo_root/bin/fornix" policy get --id repository-safety --version 1 >/tmp/fornix-policy-smoke-$workspace-cli
  jq -e '.policy_hash == "'"$policy_hash"'"' /tmp/fornix-policy-smoke-$workspace-cli >/dev/null
fi

printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" python3 "$repo_root/scripts/fornix-mcp.py" >/tmp/fornix-policy-smoke-$workspace-mcp
jq -e '.result.tools | map(.name) | index("fornix__policy_resolve") != null' /tmp/fornix-policy-smoke-$workspace-mcp >/dev/null

printf '%s\n' "policy smoke: lifecycle, resolution, CLI, and MCP surfaces passed ($workspace)"
