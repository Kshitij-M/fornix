#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
. "$repo_root/scripts/test/smoke_helpers.sh"
url=${FORNIX_URL:-http://localhost:8201}
key=${FORNIX_KEY:-}
bootstrap_key=${FORNIX_BOOTSTRAP_KEY:-}
workspace="validation-smoke-$$"
workdir=${FORNIX_REFERENCE_WORKDIR:-"$repo_root/fixtures/reference-repo"}
container_url=$(fornix_smoke_container_url "$url")
if [ -x "$repo_root/bin/fornix" ]; then
  cli_workdir=$workdir
else
  cli_workdir=$(fornix_smoke_container_path "$workdir" "$repo_root")
fi
path=".fornix-validation-smoke-$workspace.txt"
cleanup_root=$workdir
case "$cleanup_root" in
  /workspace/*) cleanup_root="$repo_root/${cleanup_root#/workspace/}" ;;
esac
trap 'rm -f "$cleanup_root/$path"' EXIT INT TERM

cli() {
  if [ -x "$repo_root/bin/fornix" ]; then
    FORNIX_URL="$url" FORNIX_KEY="$key" FORNIX_BOOTSTRAP_KEY="$bootstrap_key" FORNIX_WORKSPACE_ID="$workspace" "$repo_root/bin/fornix" "$@"
  else
    docker run --rm --add-host=host.docker.internal:host-gateway \
      -e FORNIX_URL="$container_url" -e FORNIX_KEY="$key" -e FORNIX_BOOTSTRAP_KEY="$bootstrap_key" -e FORNIX_WORKSPACE_ID="$workspace" \
      -v "$repo_root:/workspace" -w /workspace golang:1.25.13 \
      go run ./cmd/fornix "$@"
  fi
}

cli workspace bootstrap --workspace "$workspace" --tool-root "$cli_workdir" >/dev/null
proposal=$(cli change propose --repository reference-repo --source-root "$cli_workdir" --approval-mode automatic --type create_file --path "$path" --content "Fornix post-change validation\n" --idempotency "validation-change:$workspace")
proposal_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["proposal"]["id"])' <<EOF
$proposal
EOF
)
packet_hash=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["packet_hash"])' <<EOF
$proposal
EOF
)
application=$(cli change apply --id "$proposal_id" --packet-hash "$packet_hash" --idempotency "validation-application:$workspace")
application_id=$(python3 -c 'import json,sys; v=json.load(sys.stdin); assert v["application"]["status"] == "applied", v; print(v["application"]["id"])' <<EOF
$application
EOF
)
expected_tree_hash=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["application"]["expected_tree_hash"])' <<EOF
$application
EOF
)

validation=$(cli validation run --repository reference-repo --source-root "$cli_workdir" --application-id "$application_id" --proposal-id "$proposal_id" --packet-hash "$packet_hash" --expected-tree-hash "$expected_tree_hash" --idempotency "validation-run:$workspace")
run_id=$(python3 -c 'import json,sys
v=json.load(sys.stdin); r=v["run"]
assert r["status"] == "passed", v
assert r["report"]["result_count"] == 5, v
assert v.get("handoff", {}).get("status") in ("submitted", "pending"), v
print(r["id"])' <<EOF
$validation
EOF
)

duplicate=$(cli validation run --repository reference-repo --source-root "$cli_workdir" --application-id "$application_id" --proposal-id "$proposal_id" --packet-hash "$packet_hash" --expected-tree-hash "$expected_tree_hash" --idempotency "validation-run:$workspace")
python3 -c 'import json,sys
v=json.load(sys.stdin); assert v["run"]["id"] == sys.argv[1], v; assert v["run"]["replay_hash"], v
print("validation idempotency: ok")' "$run_id" <<EOF
$duplicate
EOF

results=$(cli validation results --id "$run_id" --limit 16)
python3 -c 'import json,sys
v=json.load(sys.stdin); assert len(v["results"]) == 5, v; assert [r["ordinal"] for r in v["results"]] == list(range(5)), v
print("validation results: ok")' <<EOF
$results
EOF

replay=$(cli validation replay --id "$run_id")
python3 -c 'import json,sys
v=json.load(sys.stdin); assert v["replay_hash"] == v["run"]["replay_hash"], v; assert len(v["events"]) == 2, v
print("validation replay: ok")' <<EOF
$replay
EOF

disclosure=$(cli validation disclose --id "$run_id" --level detail --max-bytes 32768 --max-items 16)
python3 -c 'import json,sys
v=json.load(sys.stdin); assert v["report_hash"]; assert v["content_view_hash"]; assert v["total_bytes"] <= 32768, v
print("validation disclosure: ok")' <<EOF
$disclosure
EOF

handoff=$(cli validation handoff --id "reindex-$run_id")
python3 -c 'import json,sys
v=json.load(sys.stdin); assert v["validation_run_id"] == sys.argv[1], v; assert v["status"] == "submitted" and v.get("ingest_job_id"), v
print("re-index handoff: ok")' "$run_id" <<EOF
$handoff
EOF
