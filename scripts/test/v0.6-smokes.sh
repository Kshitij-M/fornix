#!/usr/bin/env bash
# v0.6 orchestrator smoke tests — sessions + tasks.
# Requires: Fornix running, FORNIX_KEY exported, jq.

set -euo pipefail
FORNIX_URL="${FORNIX_URL:-http://localhost:8201}"
KEY=${FORNIX_KEY:?FORNIX_KEY env var required}
H=(-H "Authorization: Bearer ${KEY}" -H "Content-Type: application/json")

pass() { printf "  \033[32mPASS\033[0m %s\n" "$1"; }
fail() { printf "  \033[31mFAIL\033[0m %s\n" "$1"; exit 1; }

stamp=$(date +%s)
session_a="smoke-a-${stamp}"
session_b="smoke-b-${stamp}"

echo "== v0.6 smoke test 1: capability dispatch =="
curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/session" \
  -d "{\"id\":\"${session_a}\",\"host\":\"hostA\",\"capabilities\":[\"go-build\"]}" >/dev/null
curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/session" \
  -d "{\"id\":\"${session_b}\",\"host\":\"hostB\",\"capabilities\":[\"python\",\"gpu-3090\"]}" >/dev/null
task_id=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task" \
  -d "{\"title\":\"smoke-go-build-${stamp}\",\"brief\":\"compile something\",\"required_capabilities\":[\"go-build\"],\"created_by\":\"smoke\"}" | jq -r .id)
[[ "${task_id}" != "null" && -n "${task_id}" ]] || fail "task create returned no id"

# Session B should NOT be able to claim (lacks go-build).
http_b=$(curl -s -o /tmp/v06-b.json -w "%{http_code}" "${H[@]}" -X POST "${FORNIX_URL}/v1/task/claim" -d "{\"session_id\":\"${session_b}\"}")
[[ "${http_b}" == "204" ]] || fail "session B claim should be 204, got ${http_b}"
pass "session B (no go-build) gets 204"

# Session A should successfully claim.
claim_a=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/claim" -d "{\"session_id\":\"${session_a}\"}")
claimed_id=$(echo "${claim_a}" | jq -r .id)
claim_fence=$(echo "${claim_a}" | jq -r .fence)
[[ "${claimed_id}" == "${task_id}" ]] || fail "session A claimed wrong task: expected ${task_id} got ${claimed_id}"
[[ "${claim_fence}" != "null" && -n "${claim_fence}" ]] || fail "claim returned no execution fence"
pass "session A claimed task ${task_id}"

echo "== v0.6 smoke test 2: complete + session returns to idle =="
curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/task/${task_id}/complete" \
  -d "{\"result\":\"smoke complete\",\"status\":\"done\",\"session_id\":\"${session_a}\",\"fence\":${claim_fence}}" >/dev/null
status=$(curl -fsS "${H[@]}" "${FORNIX_URL}/v1/tasks?status=done" | jq -r ".tasks[] | select(.id==${task_id}) | .status")
[[ "${status}" == "done" ]] || fail "task not marked done: ${status}"
sess_a_status=$(curl -fsS "${H[@]}" "${FORNIX_URL}/v1/sessions" | jq -r ".sessions[] | select(.id==\"${session_a}\") | .status")
[[ "${sess_a_status}" == "idle" ]] || fail "session A status ${sess_a_status}, expected idle"
pass "complete returns session A to idle"

echo "== v0.6 smoke test 3: capability filter on session list =="
listed=$(curl -fsS "${H[@]}" "${FORNIX_URL}/v1/sessions?capability=gpu-3090" | jq -r ".sessions[].id")
echo "${listed}" | grep -q "${session_b}" || fail "session B not in gpu-3090 capability list"
echo "${listed}" | grep -qv "${session_a}" || true
pass "capability filter returns session B for gpu-3090"

echo "all v0.6 smokes passed."
