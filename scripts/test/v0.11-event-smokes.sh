#!/usr/bin/env bash
# Loop 2 control-plane smoke — task completion, durable event identity, and idempotency.
# Requires: Fornix running, curl, and jq.
set -euo pipefail

FORNIX_URL="${FORNIX_URL:-http://localhost:8201}"
FORNIX_KEY="${FORNIX_KEY:?FORNIX_KEY env var required}"
H=(-H "Authorization: Bearer ${FORNIX_KEY}" -H "Content-Type: application/json")

pass() { printf "  \033[32mPASS\033[0m %s\n" "$1"; }
fail() { printf "  \033[31mFAIL\033[0m %s\n" "$1"; exit 1; }

stamp=$(date +%s%N)
task_id=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task" \
  -d "{\"title\":\"event-smoke-${stamp}\",\"brief\":\"verify durable completion event\",\"created_by\":\"event-smoke\"}" \
  | jq -r .id)
[[ -n "${task_id}" && "${task_id}" != "null" ]] || fail "task create returned no id"

key="event-smoke-${stamp}"
body="{\"status\":\"done\",\"result\":\"event smoke complete\",\"idempotency_key\":\"${key}\"}"
first=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/${task_id}/complete" -d "${body}")
second=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/${task_id}/complete" -d "${body}")

first_id=$(echo "${first}" | jq -r .event_id)
second_id=$(echo "${second}" | jq -r .event_id)
first_sequence=$(echo "${first}" | jq -r .event_sequence)
second_sequence=$(echo "${second}" | jq -r .event_sequence)
[[ "${first_id}" == "${second_id}" ]] || fail "duplicate returned a different event id"
[[ "${first_sequence}" == "${second_sequence}" ]] || fail "duplicate returned a different event sequence"
[[ "$(echo "${first}" | jq -r .deduped)" == "false" ]] || fail "first completion was marked deduped"
[[ "$(echo "${second}" | jq -r .deduped)" == "true" ]] || fail "second completion was not marked deduped"
pass "duplicate task completion returns one event (${first_id}, sequence ${first_sequence})"

status=$(curl -fsS "${H[@]}" "${FORNIX_URL}/v1/tasks?status=done" \
  | jq -r ".tasks[] | select(.id==${task_id}) | .status" | head -n 1)
[[ "${status}" == "done" ]] || fail "task status was ${status}, expected done"
pass "task state remains done after replayed delivery"

http_code=$(curl -sS -o /tmp/fornix-v011-conflict.json -w "%{http_code}" "${H[@]}" \
  -X POST "${FORNIX_URL}/v1/task/${task_id}/complete" \
  -d "{\"status\":\"failed\",\"result\":\"conflicting retry\",\"idempotency_key\":\"${key}\"}")
[[ "${http_code}" == "409" ]] || fail "conflicting idempotency key returned HTTP ${http_code}, expected 409"
pass "conflicting idempotency reuse fails closed"

echo "all v0.11 event smokes passed."
