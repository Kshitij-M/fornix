#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
. "$repo_root/scripts/test/smoke_helpers.sh"
url=${FORNIX_URL:-http://localhost:8201}
key=${FORNIX_KEY:-}
bootstrap_key=${FORNIX_BOOTSTRAP_KEY:-}
workspace="work-receipt-smoke-$$"
workdir=${FORNIX_REFERENCE_WORKDIR:-/workspace/fixtures/reference-repo}

if [ -x "$repo_root/bin/fornix" ]; then
  out=$(FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_BOOTSTRAP_KEY="$bootstrap_key" \
    "$repo_root/bin/fornix" reference-workflow --workspace "$workspace" \
    --fixture fixtures/reference-repo --workdir "$workdir")
else
  container_url=$(fornix_smoke_container_url "$url")
  container_workdir=$(fornix_smoke_container_path "$workdir" "$repo_root")
  out=$(docker run --rm --add-host=host.docker.internal:host-gateway \
    -e FORNIX_URL="$container_url" -e FORNIX_KEY="$key" -e FORNIX_BOOTSTRAP_KEY="$bootstrap_key" \
    -v "$repo_root:/workspace" -w /workspace golang:1.25.13 \
    go run ./cmd/fornix reference-workflow --workspace "$workspace" \
    --fixture fixtures/reference-repo --workdir "$container_workdir")
fi

receipt_id=$(python3 -c 'import json,sys
v=json.loads(sys.stdin.read())
r=v["receipt"].get("receipt", v["receipt"])
assert r["verification"]["status"] == "verified", r
assert r["canonical_hash"], r
print(r["id"])' <<EOF
$out
EOF
)

receipt=$(curl --fail --silent --show-error \
  -H "Authorization: Bearer $key" \
  -H "X-Workspace-ID: $workspace" \
  "$url/v1/work-receipts/$receipt_id?workspace_id=$workspace")

gist=$(curl --fail --silent --show-error \
  -X POST \
  -H "Authorization: Bearer $key" \
  -H "X-Workspace-ID: $workspace" \
  -H "Content-Type: application/json" \
  --data "{\"workspace_id\":\"$workspace\",\"receipt_id\":\"$receipt_id\",\"level\":\"gist\",\"max_bytes\":8192,\"max_tokens\":1024,\"max_items\":16}" \
  "$url/v1/work-receipts/disclose")

python3 -c 'import json,sys
receipt=json.loads(sys.argv[1])["receipt"]
gist=json.loads(sys.argv[2])
assert receipt["canonical_hash"] == gist["canonical_hash"], (receipt, gist)
assert gist["level"] == "gist", gist
assert gist["content_view_hash"], gist
print("work receipt smoke: ok")' "$receipt" "$gist"
