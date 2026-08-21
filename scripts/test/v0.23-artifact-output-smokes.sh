#!/usr/bin/env bash
# Loop 14 artifact-backed output, backfill, retention, and integrity smoke.
set -euo pipefail

PG_DSN="${FORNIX_ARTIFACT_OUTPUT_PG_DSN:?FORNIX_ARTIFACT_OUTPUT_PG_DSN is required}"
TEST_PG_DSN="${PG_DSN/localhost/host.docker.internal}"
TEST_PG_DSN="${TEST_PG_DSN/127.0.0.1/host.docker.internal}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${TEST_PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/store -run 'Test(ArtifactBacked|Oversized|ArtifactBackfill|EvidenceBackfill|AgentBackfill|ArtifactRetentionSweep)' -count=1 -v

if [[ -n "${FORNIX_URL:-}" && -n "${FORNIX_KEY:-}" ]]; then
  H=(-H "Authorization: Bearer ${FORNIX_KEY}" -H 'Content-Type: application/json')
  stamp=$(date +%s%N)
  workspace="artifact-output-smoke-${stamp}"

  metrics=$(curl -fsS "${H[@]}" "${FORNIX_URL}/v1/artifacts/metrics?workspace_id=${workspace}")
  [[ "$(echo "${metrics}" | jq -r '.workspace_id')" == "${workspace}" ]] || { echo "artifact metrics workspace mismatch" >&2; exit 1; }

  backfill=$(jq -cn --arg workspace "${workspace}" \
    '{workspace_id:$workspace,source_kind:"tool_run",batch_size:10,dry_run:true}')
  backfill_result=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/artifacts/backfill" -d "${backfill}")
  [[ "$(echo "${backfill_result}" | jq -r '.dry_run')" == "true" ]] || { echo "artifact backfill was not dry-run" >&2; exit 1; }

  retention=$(jq -cn --arg workspace "${workspace}" \
    '{workspace_id:$workspace,batch_size:10,dry_run:true}')
  retention_result=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/artifacts/retention" -d "${retention}")
  [[ "$(echo "${retention_result}" | jq -r '.dry_run')" == "true" ]] || { echo "artifact retention was not dry-run" >&2; exit 1; }

  integrity=$(jq -cn --arg workspace "${workspace}" \
    '{workspace_id:$workspace,batch_size:10,dry_run:true}')
  integrity_result=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/artifacts/integrity" -d "${integrity}")
  [[ "$(echo "${integrity_result}" | jq -r '.dry_run')" == "true" ]] || { echo "artifact integrity was not dry-run" >&2; exit 1; }
  echo "all v0.23 artifact output smokes passed (workspace=${workspace})"
else
  echo "all v0.23 artifact output store smokes passed (HTTP portion skipped: FORNIX_URL/FORNIX_KEY not set)"
fi
