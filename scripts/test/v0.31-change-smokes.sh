#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
url=${FORNIX_URL:-http://localhost:8201}
key=${FORNIX_KEY:-}
workspace="change-smoke-$$"
workdir=${FORNIX_REFERENCE_WORKDIR:-"$repo_root/fixtures/reference-repo"}
path=".fornix-change-smoke-$workspace.txt"
cleanup() {
  rm -f "$workdir/$path"
}
trap cleanup EXIT INT TERM

cli() {
  if [ -x "$repo_root/bin/fornix" ]; then
    FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_WORKSPACE_ID="$workspace" "$repo_root/bin/fornix" "$@"
  else
    docker run --rm --network host \
      -e FORNIX_URL="$url" -e FORNIX_KEY="$key" -e FORNIX_WORKSPACE_ID="$workspace" \
      -v "$repo_root:/workspace" -w /workspace golang:1.25.13 \
      go run ./cmd/fornix "$@"
  fi
}

cli workspace bootstrap --workspace "$workspace" --tool-root "$workdir" >/dev/null
proposal=$(cli change propose --repository reference-repo --source-root "$workdir" --type create_file --path "$path" --content "Fornix verified change packet\n" --idempotency "change-smoke:$workspace")
proposal_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["proposal"]["id"])' <<EOF
$proposal
EOF
)
packet_hash=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["packet_hash"])' <<EOF
$proposal
EOF
)

duplicate=$(cli change propose --repository reference-repo --source-root "$workdir" --type create_file --path "$path" --content "Fornix verified change packet\n" --idempotency "change-smoke:$workspace")
python3 -c 'import json,sys
v=json.load(sys.stdin); assert v.get("duplicate") is True, v; print("change proposal idempotency: ok")' <<EOF
$duplicate
EOF

approval=$(cli change approve --id "$proposal_id" --packet-hash "$packet_hash" --idempotency "change-approval-smoke:$workspace")
python3 -c 'import json,sys
v=json.load(sys.stdin); assert v["proposal"]["status"] == "approved", v
print("change approval: ok")' <<EOF
$approval
EOF

application=$(cli change apply --id "$proposal_id" --packet-hash "$packet_hash" --idempotency "change-apply-smoke:$workspace")
application_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["application"]["id"])' <<EOF
$application
EOF
)
python3 -c 'import json,sys
v=json.load(sys.stdin); app=v["application"]; assert app["status"] == "applied", v; assert app.get("receipt", {}).get("verification", {}).get("status") == "verified", v
print("change application and receipt: ok")' <<EOF
$application
EOF

disclosure=$(cli change disclose --id "$proposal_id" --level detail)
python3 -c 'import json,sys
v=json.load(sys.stdin); assert v["packet_hash"]; assert v["content_view_hash"]; assert v["proposal"]["operations"]; print("change disclosure: ok")' <<EOF
$disclosure
EOF

duplicate_application=$(cli change apply --id "$proposal_id" --packet-hash "$packet_hash" --idempotency "change-apply-smoke:$workspace")
python3 -c 'import json,sys
v=json.load(sys.stdin); assert v["application"]["id"] == sys.argv[1], v; assert v["application"]["status"] == "applied", v
print("change application idempotency: ok")' "$application_id" <<EOF
$duplicate_application
EOF
