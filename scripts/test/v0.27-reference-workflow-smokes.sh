#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
url=${FORNIX_URL:-http://localhost:8201}
workspace="reference-smoke-$$"
key=${FORNIX_KEY:-}
bootstrap_key=${FORNIX_BOOTSTRAP_KEY:-}
workdir=${FORNIX_REFERENCE_WORKDIR:-/workspace/fixtures/reference-repo}

out=$(docker run --rm --network host \
  -e FORNIX_URL="$url" -e FORNIX_KEY="$key" -e FORNIX_BOOTSTRAP_KEY="$bootstrap_key" \
  -v "$repo_root:/workspace" -w /workspace golang:1.25.13 \
  go run ./cmd/fornix reference-workflow --workspace "$workspace" --fixture fixtures/reference-repo --workdir "$workdir")

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
