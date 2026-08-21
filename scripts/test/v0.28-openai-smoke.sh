#!/bin/sh
set -eu

if [ -z "${FORNIX_OPENAI_API_KEY:-}" ]; then
  echo "openai smoke: skipped (FORNIX_OPENAI_API_KEY is not set)"
  exit 0
fi

url=${FORNIX_URL:-http://localhost:8201}
key=${FORNIX_KEY:-}
status=$(curl -sS -o /tmp/fornix-openai-smoke-response -w '%{http_code}' \
  -X POST "$url/v1/model/complete" \
  -H "Authorization: Bearer $key" -H 'Content-Type: application/json' \
  -H 'X-Workspace-ID: default' \
  --data '{"workspace_id":"default","request_id":"openai-smoke","idempotency_key":"openai-smoke","provider":{"provider":"openai"},"prompt":"Reply with the word bounded.","budget":{"max_output_tokens":16,"max_input_bytes":4096,"max_cost_usd":0.05,"timeout_ms":10000}}')
case "$status" in
  2*) echo "openai smoke: ok" ;;
  *) echo "openai smoke: provider returned HTTP $status" >&2; exit 1 ;;
esac
