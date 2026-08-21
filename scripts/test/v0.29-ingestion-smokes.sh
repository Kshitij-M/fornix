#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
url=${FORNIX_URL:-http://localhost:8201}
key=${FORNIX_KEY:-}
bootstrap_key=${FORNIX_BOOTSTRAP_KEY:-}
workspace="ingest-smoke-$$"
workdir=${FORNIX_REFERENCE_WORKDIR:-/workspace/fixtures/reference-repo}

cli() {
  if [ -x "$repo_root/bin/fornix" ]; then
    docker run --rm --network host \
      -e FORNIX_URL="$url" -e FORNIX_KEY="$key" -e FORNIX_BOOTSTRAP_KEY="$bootstrap_key" \
      -v "$repo_root:/workspace" -w /workspace golang:1.25.13 \
      /workspace/bin/fornix --workspace "$workspace" "$@"
  else
    docker run --rm --network host \
      -e FORNIX_URL="$url" -e FORNIX_KEY="$key" -e FORNIX_BOOTSTRAP_KEY="$bootstrap_key" \
      -v "$repo_root:/workspace" -w /workspace golang:1.25.13 \
      go run ./cmd/fornix --workspace "$workspace" "$@"
  fi
}

cli workspace bootstrap --workspace "$workspace" --tool-root "$workdir" >/dev/null
before=$(cli ingest list)
dry_run=$(cli ingest dry-run --source-root "$workdir" --repository reference-repo)
python3 -c 'import json,sys
v=json.loads(sys.stdin.read()); r=v["report"]
assert r["dry_run"] is True
assert r["manifest_hash"] and r["report_hash"]
print("ingestion dry-run: ok")' <<EOF
$dry_run
EOF

after=$(cli ingest list)
python3 -c 'import json,sys
before=json.loads(sys.argv[1]); after=json.loads(sys.argv[2])
assert len(before.get("jobs", [])) == len(after.get("jobs", []))
print("ingestion dry-run mutation check: ok")' "$before" "$after"

submitted=$(cli ingest submit --source-root "$workdir" --repository reference-repo --idempotency "smoke-ingest:$workspace")
job_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["job"]["id"])' <<EOF
$submitted
EOF
)

for _ in $(seq 1 128); do
  status=$(cli ingest status --id "$job_id")
  state=$(python3 -c 'import json,sys
v=json.load(sys.stdin); print(v.get("status", v.get("job", {}).get("status", "")))' <<EOF
$status
EOF
)
  [ "$state" = succeeded ] && break
  [ "$state" = failed ] && { echo "$status" >&2; exit 1; }
  [ "$state" = cancelled ] && { echo "$status" >&2; exit 1; }
  cli ingest resume --id "$job_id" --batch-size 2 >/dev/null
done

final=$(cli ingest status --id "$job_id")
duplicate=$(cli ingest submit --source-root "$workdir" --repository reference-repo --idempotency "smoke-ingest:$workspace")
python3 -c 'import json,sys
final=json.loads(sys.argv[1]); dup=json.loads(sys.argv[2])
assert final.get("status", final.get("job", {}).get("status")) == "succeeded", final
job=final.get("job", final)
assert job["manifest_hash"] and job["report"]["report_hash"]
assert dup.get("created") is False and dup.get("deduped") is True
raw=json.dumps(final)
assert "FORNIX_OPENAI_API_KEY" not in raw and "api_key" not in raw.lower()
print("durable ingestion smoke: ok")' "$final" "$duplicate"
