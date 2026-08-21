#!/usr/bin/env bash
# Loop 13 content-addressed artifacts, disclosure, integrity, and retention smoke.
set -euo pipefail

PG_DSN="${FORNIX_ARTIFACT_PG_DSN:?FORNIX_ARTIFACT_PG_DSN is required}"
TEST_PG_DSN="${PG_DSN/localhost/host.docker.internal}"
TEST_PG_DSN="${TEST_PG_DSN/127.0.0.1/host.docker.internal}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${TEST_PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/store -run '^TestArtifactStore|^TestModelCallStoreTerminal' -count=1 -v

if [[ -n "${FORNIX_URL:-}" && -n "${FORNIX_KEY:-}" ]]; then
  H=(-H "Authorization: Bearer ${FORNIX_KEY}" -H 'Content-Type: application/json')
  stamp=$(date +%s%N)
  workspace="artifact-smoke-${stamp}"
  raw_b64=$(printf 'fornix artifact smoke payload\n' | base64 | tr -d '\n')
  body=$(jq -cn --arg workspace "${workspace}" --arg raw "${raw_b64}" \
    '{workspace_id:$workspace,kind:"smoke",media_type:"text/plain",raw:$raw,manifest:{gist:"smoke artifact"},source_kind:"smoke",source_id:"artifact-1",role:"output",idempotency_key:"artifact-smoke-once"}')
  created=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/artifacts" -d "${body}")
  artifact_id=$(echo "${created}" | jq -r '.artifact.id')
  content_hash=$(echo "${created}" | jq -r '.artifact.content_hash')
  [[ "${artifact_id}" != "null" && -n "${artifact_id}" && -n "${content_hash}" ]] || { echo "artifact create missing identity" >&2; exit 1; }
  disclosure_body=$(jq -cn --arg workspace "${workspace}" --argjson id "${artifact_id}" \
    '{workspace_id:$workspace,artifact_id:$id,level:"raw",max_bytes:4096,max_tokens:1024,max_items:8}')
  disclosure=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/artifacts/disclose" -d "${disclosure_body}")
  [[ "$(echo "${disclosure}" | jq -r '.content_hash')" == "${content_hash}" ]] || { echo "artifact hash changed during disclosure" >&2; exit 1; }
  foreign_body=$(jq -cn --arg workspace "${workspace}-foreign" --argjson id "${artifact_id}" \
    '{workspace_id:$workspace,artifact_id:$id,level:"gist"}')
  foreign_status=$(curl -sS "${H[@]}" -o /tmp/fornix-v022-foreign.json -w '%{http_code}' -X POST "${FORNIX_URL}/v1/artifacts/disclose" -d "${foreign_body}")
  [[ "${foreign_status}" == "404" ]] || { echo "artifact workspace isolation returned ${foreign_status}" >&2; exit 1; }
  echo "all v0.22 artifact smokes passed (artifact=${artifact_id}, hash=${content_hash})"
else
  echo "all v0.22 artifact store smokes passed (HTTP portion skipped: FORNIX_URL/FORNIX_KEY not set)"
fi
