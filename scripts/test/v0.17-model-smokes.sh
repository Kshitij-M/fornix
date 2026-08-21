#!/usr/bin/env bash
set -euo pipefail

url="${FORNIX_URL:-http://localhost:8201}"
key="${FORNIX_KEY:-}"
if [[ -z "${key}" ]]; then
  echo "FORNIX_KEY is required" >&2
  exit 1
fi

stamp="$(date +%s%N)"
workspace="model-smoke-${stamp}"
idempotency="model-smoke-idempotency-${stamp}"
body="{\"workspace_id\":\"${workspace}\",\"request_id\":\"model-smoke-request-${stamp}\",\"idempotency_key\":\"${idempotency}\",\"provider\":{\"provider\":\"fake\",\"model\":\"fake-model\"},\"prompt\":\"return a deterministic smoke response\",\"budget\":{\"max_output_tokens\":128}}"

health="$(curl --fail --silent --show-error -H "Authorization: Bearer ${key}" "${url}/v1/health")"
echo "${health}" | jq -e '.model_providers | index("fake") != null' >/dev/null

first="$(curl --fail --silent --show-error \
  -H "Authorization: Bearer ${key}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${idempotency}" \
  -d "${body}" "${url}/v1/model/complete")"
second="$(curl --fail --silent --show-error \
  -H "Authorization: Bearer ${key}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${idempotency}" \
  -d "${body}" "${url}/v1/model/complete")"

first_hash="$(echo "${first}" | jq -r '.content_hash')"
second_hash="$(echo "${second}" | jq -r '.content_hash')"
[[ -n "${first_hash}" && "${first_hash}" == "${second_hash}" ]]
[[ "$(echo "${first}" | jq -r '.content')" == "$(echo "${second}" | jq -r '.content')" ]]

echo "all v0.17 model gateway smokes passed (provider=fake, content_hash=${first_hash})"
