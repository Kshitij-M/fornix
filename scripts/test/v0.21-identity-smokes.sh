#!/usr/bin/env bash
# Loop 12 workspace identity and authorization smoke.
set -euo pipefail

url="${FORNIX_URL:-http://localhost:8201}"
key="${FORNIX_KEY:-}"
if [[ -z "${key}" ]]; then
  echo "FORNIX_KEY is required" >&2
  exit 1
fi

unauthorized_code=$(curl -sS -o /tmp/fornix-v021-unauthorized.json -w "%{http_code}" \
  -H 'Content-Type: application/json' -X POST "${url}/v1/model/complete" \
  -d '{"workspace_id":"identity-smoke","provider":{"provider":"fake"},"prompt":"ignored"}')
[[ "${unauthorized_code}" == "401" ]] || { echo "unauthorized request returned ${unauthorized_code}" >&2; exit 1; }

body='{"workspace_id":"identity-smoke","request_id":"identity-smoke-request","idempotency_key":"identity-smoke-idempotency","provider":{"provider":"fake","model":"fake-model"},"prompt":"identity smoke"}'
authorized_code=$(curl -sS -o /tmp/fornix-v021-authorized.json -w "%{http_code}" \
  -H "Authorization: Bearer ${key}" -H 'Content-Type: application/json' \
  -X POST "${url}/v1/model/complete" -d "${body}")
[[ "${authorized_code}" == "200" ]] || { echo "authorized request returned ${authorized_code}" >&2; cat /tmp/fornix-v021-authorized.json >&2; exit 1; }

wrong_code=$(curl -sS -o /tmp/fornix-v021-wrong-key.json -w "%{http_code}" \
  -H 'Authorization: Bearer definitely-not-the-fornix-key' \
  -H 'Content-Type: application/json' -X POST "${url}/v1/model/complete" -d "${body}")
[[ "${wrong_code}" == "401" ]] || { echo "wrong key returned ${wrong_code}" >&2; exit 1; }

echo "all v0.21 identity and authorization smokes passed"
