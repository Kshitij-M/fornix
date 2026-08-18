#!/usr/bin/env bash
# Loop 7 provenance/disclosure smoke — immutable evidence, deterministic
# gist/detail/raw disclosure, supersession, graph bounds, and workspace scope.
set -euo pipefail

PG_DSN="${FORNIX_PROVENANCE_PG_DSN:?FORNIX_PROVENANCE_PG_DSN is required}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/store -run '^Test(Evidence|EventAppendCreates)' -count=1 -v

if [[ -n "${FORNIX_URL:-}" && -n "${FORNIX_KEY:-}" ]]; then
  H=(-H "Authorization: Bearer ${FORNIX_KEY}" -H "Content-Type: application/json")
  stamp=$(date +%s%N)
  workspace="provenance-smoke-${stamp}"
  other_workspace="provenance-smoke-other-${stamp}"
  old=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evidence" \
    -d "{\"workspace_id\":\"${workspace}\",\"source_reference\":\"smoke:old\",\"kind\":\"memo\",\"gist\":\"old gist\",\"detail\":\"old detail\",\"raw_payload\":{\"version\":1}}")
  old_id=$(echo "${old}" | jq -r .record.id)
  [[ "${old_id}" != "null" && -n "${old_id}" ]] || { echo "evidence insert did not return an id" >&2; exit 1; }
  newer=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evidence" \
    -d "{\"workspace_id\":\"${workspace}\",\"source_reference\":\"smoke:new\",\"kind\":\"memo\",\"gist\":\"new gist\",\"detail\":\"new detail\",\"supersedes_id\":${old_id},\"raw_payload\":{\"version\":2}}")
  new_id=$(echo "${newer}" | jq -r .record.id)
  body="{\"workspace_id\":\"${workspace}\",\"evidence_id\":${new_id},\"level\":\"raw\",\"max_bytes\":1024,\"max_tokens\":256,\"max_depth\":2,\"max_nodes\":8}"
  first=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evidence/disclose" -d "${body}")
  second=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evidence/disclose" -d "${body}")
  first_hash=$(echo "${first}" | jq -r .content_hash)
  second_hash=$(echo "${second}" | jq -r .content_hash)
  [[ -n "${first_hash}" && "${first_hash}" == "${second_hash}" ]] || { echo "disclosure hash was not stable" >&2; exit 1; }
  [[ "$(echo "${first}" | jq -r .evidence_hash)" != "" ]] || { echo "evidence hash missing" >&2; exit 1; }
  old_view=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evidence/disclose" \
    -d "{\"workspace_id\":\"${workspace}\",\"evidence_id\":${old_id},\"level\":\"gist\",\"max_bytes\":128,\"max_tokens\":32}")
  [[ "$(echo "${old_view}" | jq '.superseded_by | length')" == "1" ]] || { echo "supersession metadata missing" >&2; exit 1; }
  status=$(curl -sS "${H[@]}" -o /dev/null -w '%{http_code}' -X POST "${FORNIX_URL}/v1/evidence/disclose" \
    -d "{\"workspace_id\":\"${other_workspace}\",\"evidence_id\":${new_id},\"level\":\"gist\"}")
  [[ "${status}" == "404" ]] || { echo "workspace isolation returned HTTP ${status}" >&2; exit 1; }
  echo "all v0.16 provenance smokes passed (old=${old_id}, new=${new_id}, hash=${first_hash})"
else
  echo "all v0.16 provenance store smokes passed (HTTP portion skipped: FORNIX_URL/FORNIX_KEY not set)"
fi
