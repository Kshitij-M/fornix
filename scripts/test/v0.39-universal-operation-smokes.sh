#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
binary=${FORNIX_BINARY:-$repo_root/bin/fornix}
url=${FORNIX_URL:-http://localhost:8201}
key=${FORNIX_KEY:-}
workspace=${FORNIX_WORKSPACE_ID:-universal-operation-smoke-$$}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/fornix-operation-smoke.XXXXXX")
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM

[ -x "$binary" ] || { echo "universal operation smoke: missing executable $binary" >&2; exit 1; }

hash=$(printf '%064d' 0 | tr '0' 'a')
request_file="$tmp/request.json"
cat >"$request_file" <<EOF
{
  "id": "cli-operation-$workspace",
  "request_id": "cli-request-$workspace",
  "idempotency_key": "cli-create-$workspace",
  "workspace_id": "$workspace",
  "capability": {
    "workspace_id": "$workspace",
    "connector": {"workspace_id": "$workspace", "name": "smoke-connector", "version": "v1"},
    "name": "read-resource",
    "version": "v1",
    "definition_hash": "$hash"
  },
  "target": {
    "workspace_id": "$workspace",
    "system": {"workspace_id": "$workspace", "type": "smoke-system", "id": "system-1", "version": "v1"},
    "kind": "record",
    "id": "record-1",
    "version": "v1",
    "content_hash": "$hash"
  },
  "input_type": "smoke.input",
  "input_schema_version": 1,
  "input_schema_hash": "$hash",
  "input_hash": "$hash",
  "metadata": {"test_case": "universal-operation"}
}
EOF

created=$(env FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$binary" operation create --request-file "$request_file")
operation_id=$(printf '%s' "$created" | jq -r '.operation.id')
[ -n "$operation_id" ] && [ "$operation_id" != "null" ]

duplicate=$(env FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$binary" operation create --request-file "$request_file")
printf '%s' "$duplicate" | jq -e --arg id "$operation_id" '.duplicate == true and .operation.id == $id' >/dev/null

claim=$(env FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$binary" operation claim --limit 1 --ttl-ms 30000)
printf '%s' "$claim" | jq -e --arg id "$operation_id" '.count == 1 and .claims[0].operation.id == $id' >/dev/null
fence=$(printf '%s' "$claim" | jq -r '.claims[0].lease.fence')
[ "$fence" -gt 0 ]

env FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$binary" operation renew --id "$operation_id" --fence "$fence" >/dev/null
transition=$(env FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$binary" operation transition --id "$operation_id" --fence "$fence" --to-status planned)
printf '%s' "$transition" | jq -e '.operation.status == "planned" and .duplicate != true' >/dev/null

env FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$binary" operation release --id "$operation_id" --fence "$fence" >/dev/null
new_lease=$(env FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$binary" operation lease --id "$operation_id")
new_fence=$(printf '%s' "$new_lease" | jq -r '.lease.fence')
[ "$new_fence" -gt "$fence" ]

if env FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$binary" operation transition --id "$operation_id" --fence "$fence" --to-status admitted >/dev/null 2>"$tmp/stale.err"; then
  echo 'universal operation smoke: stale fence unexpectedly succeeded' >&2
  exit 1
fi
grep -F 'operation lease fence is stale' "$tmp/stale.err" >/dev/null

replay=$(env FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$binary" operation replay --id "$operation_id")
printf '%s' "$replay" | jq -e '.verified == true and .replay_hash != ""' >/dev/null

printf '%s\n' "universal operation smoke: CLI create, duplicate, queue claim, fenced lease lifecycle, stale rejection, transition, and replay passed ($operation_id)"
