#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
. "$repo_root/scripts/test/smoke_helpers.sh"
url=${FORNIX_URL:-http://localhost:8201}
workspace="reference-smoke-$$"
key=${FORNIX_KEY:-}
bootstrap_key=${FORNIX_BOOTSTRAP_KEY:-}
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

python3 -c 'import json,sys
v=json.loads(sys.stdin.read())
assert v["workspace"]
assert v["replay_verified"] is True
assert v["artifact"]
assert v["evidence"]
assert v["completion"]
assert v["receipt"]
receipt=v["receipt"].get("receipt", v["receipt"])
assert receipt.get("canonical_hash"), receipt
assert receipt.get("verification", {}).get("status") == "verified", receipt
run=v["run"].get("run", v["run"])
assert run.get("state") == "succeeded", run
print("reference workflow smoke: ok")' <<EOF
$out
EOF
