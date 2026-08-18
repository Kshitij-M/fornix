#!/usr/bin/env bash
# Loop 6 retrieval smoke — deterministic planning, hard budgets, workspace
# isolation, duplicate collapse, and stable context hashes.
set -euo pipefail

PG_DSN="${FORNIX_RETRIEVAL_PG_DSN:?FORNIX_RETRIEVAL_PG_DSN is required}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/retrieval -run '^Test(Plan|Compiler|Retrieve)' -count=1 -v

if [[ -n "${FORNIX_URL:-}" && -n "${FORNIX_KEY:-}" ]]; then
  H=(-H "Authorization: Bearer ${FORNIX_KEY}" -H "Content-Type: application/json")
  stamp=$(date +%s%N)
  workspace="retrieval-smoke-${stamp}"
  other_workspace="retrieval-smoke-other-${stamp}"
  first=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/memo" \
    -d "{\"workspace_id\":\"${workspace}\",\"title\":\"retrieval smoke\",\"content\":\"deterministic alpha evidence workspace A\",\"type\":\"general\"}")
  curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/memo" \
    -d "{\"workspace_id\":\"${other_workspace}\",\"title\":\"retrieval smoke\",\"content\":\"deterministic alpha evidence workspace B\",\"type\":\"general\"}" >/dev/null

  body="{\"workspace_id\":\"${workspace}\",\"query\":\"alpha\",\"max_items\":1,\"max_bytes\":256,\"max_tokens\":64}"
  first_pack=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/retrieve" -d "${body}")
  second_pack=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/retrieve" -d "${body}")
  first_hash=$(echo "${first_pack}" | jq -r .pack.content_hash)
  second_hash=$(echo "${second_pack}" | jq -r .pack.content_hash)
  [[ -n "${first_hash}" && "${first_hash}" == "${second_hash}" ]] || { echo "context hash was not stable" >&2; exit 1; }
  [[ "$(echo "${first_pack}" | jq '.pack.items | length')" == "1" ]] || { echo "retrieval returned an unexpected item count" >&2; exit 1; }
  [[ "$(echo "${first_pack}" | jq -r '.pack.items[0].workspace_id')" == "${workspace}" ]] || { echo "workspace isolation failed" >&2; exit 1; }
  [[ "$(echo "${first_pack}" | jq -r '.trace.stages[3].reason')" == "query_embedding_not_supplied" ]] || { echo "vector gate was not observable" >&2; exit 1; }
  echo "all v0.15 retrieval smokes passed (memo=$(echo "${first}" | jq -r .id), hash=${first_hash})"
else
  echo "all v0.15 retrieval store smokes passed (HTTP portion skipped: FORNIX_URL/FORNIX_KEY not set)"
fi
