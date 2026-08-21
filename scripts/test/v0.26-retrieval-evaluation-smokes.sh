#!/usr/bin/env bash
# Loop 17 retrieval-surface capture, operator API, offline CLI, and redaction smoke.
set -euo pipefail

PG_DSN="${FORNIX_RETRIEVAL_EVAL_PG_DSN:?FORNIX_RETRIEVAL_EVAL_PG_DSN is required}"
TEST_PG_DSN="${PG_DSN/localhost/host.docker.internal}"
TEST_PG_DSN="${TEST_PG_DSN/127.0.0.1/host.docker.internal}"
GO_IMAGE="${GO_IMAGE:-golang:1.25.13}"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

docker run --rm --add-host=host.docker.internal:host-gateway \
  -e FORNIX_TEST_PG_DSN="${TEST_PG_DSN}" \
  -v "${REPO_ROOT}:/workspace" -w /workspace "${GO_IMAGE}" \
  go test ./internal/contracts ./internal/store -run 'TestRetrievalSurface' -count=1 -v

if [[ -z "${FORNIX_URL:-}" || -z "${FORNIX_KEY:-}" ]]; then
  echo "all v0.26 retrieval surface store smokes passed (HTTP/CLI portion skipped)"
  exit 0
fi

H=(-H "Authorization: Bearer ${FORNIX_KEY}" -H "Content-Type: application/json")
stamp=$(date +%s%N)
workspace="retrieval-eval-smoke-${stamp}"

unauthenticated=$(curl -sS -o /dev/null -w '%{http_code}' -X GET "${FORNIX_URL}/v1/evaluations/retrieval/surfaces")
[[ "${unauthenticated}" == "401" ]] || { echo "unauthenticated evaluation read returned ${unauthenticated}" >&2; exit 1; }

# A normal retrieval call records a redacted surface automatically.
retrieve_body=$(jq -nc --arg workspace "${workspace}" '{workspace_id:$workspace,query:"surface-capture-smoke",max_items:1,max_bytes:256,max_tokens:64}')
curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/retrieve" -d "${retrieve_body}" >/dev/null
surface_page=$(curl -fsS "${H[@]}" -X GET "${FORNIX_URL}/v1/evaluations/retrieval/surfaces?workspace_id=${workspace}&limit=1")
[[ "$(echo "${surface_page}" | jq '.items | length')" -ge 1 ]] || { echo "automatic retrieval surface capture missing" >&2; exit 1; }
[[ "$(echo "${surface_page}" | jq -r '.items[0].trace.stages[0].name')" != "null" ]] || { echo "captured trace missing" >&2; exit 1; }

# Register authoritative evidence and one redacted surface so the operator
# evaluator resolves gold without replaying retrieval or invoking a provider.
evidence_body=$(jq -nc --arg workspace "${workspace}" '{workspace_id:$workspace,source_reference:"memo:surface-eval",kind:"memo",gist:"bounded evaluation evidence",raw_payload:{text:"recorded evidence"}}')
evidence_response=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evidence" -d "${evidence_body}")
evidence_hash=$(echo "${evidence_response}" | jq -r '.record.evidence_hash')
evidence_id=$(echo "${evidence_response}" | jq -r '.record.id')
[[ "${evidence_hash}" =~ ^[0-9a-f]{64}$ ]] || { echo "evidence hash missing" >&2; exit 1; }

surface_id="surface-${stamp}"
request_hash=$(printf '%064d' 1)
plan_hash=$(printf '%064d' 2)
context_hash=$(printf '%064d' 3)
surface_body=$(jq -nc --arg workspace "${workspace}" --arg id "${surface_id}" \
  --arg request_hash "${request_hash}" --arg plan_hash "${plan_hash}" --arg context_hash "${context_hash}" \
  --arg evidence_hash "${evidence_hash}" '{
    id:$id,workspace_id:$workspace,request_id:("request-"+$id),idempotency_key:("capture-"+$id),
    request_hash:$request_hash,plan_hash:$plan_hash,context_hash:$context_hash,
    budget:{max_items:1,max_bytes:256,max_tokens:64},
    trace:{plan_hash:$plan_hash,stages:[{name:"structured",status:"completed",queries:1,accepted:1}],compiled_items:1,compiled_bytes:32,compiled_tokens:8},
    references:[{source_reference:("evidence:"+$evidence_hash),kind:"memo",evidence_hash:$evidence_hash,score:1,stage:"structured",representation:"detail"}],
    duration_ms:2,sql_queries:1,cost_usd:0,cost_known:false,cost_estimated:true
  }')
surface_response=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evaluations/retrieval/surfaces" -d "${surface_body}")
[[ "$(echo "${surface_response}" | jq -r '.created')" == "true" ]] || { echo "surface registration did not create" >&2; exit 1; }
duplicate_surface=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evaluations/retrieval/surfaces" -d "${surface_body}")
[[ "$(echo "${duplicate_surface}" | jq -r '.created')" == "false" ]] || { echo "duplicate surface registration was not idempotent" >&2; exit 1; }

dataset_body=$(jq -nc --arg workspace "${workspace}" --arg surface_id "${surface_id}" \
  --arg evidence_hash "${evidence_hash}" --arg context_hash "${context_hash}" '{
    workspace_id:$workspace,name:"retrieval-surface-smoke",version:1,
    cases:[{id:"case-1",retrieval_surface_id:$surface_id,input_hash:("a"*64),gold_evidence:[$evidence_hash],expected_context_hash:$context_hash,retrieval_k:1}]
  }')
dataset_response=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evaluations/datasets" -d "${dataset_body}")
dataset_id=$(echo "${dataset_response}" | jq -r '.dataset.id')
[[ -n "${dataset_id}" && "${dataset_id}" != "null" ]] || { echo "dataset registration failed" >&2; exit 1; }

dry_key="dry-${stamp}"
dry_body=$(jq -nc --arg workspace "${workspace}" --arg dataset_id "${dataset_id}" --arg key "${dry_key}" \
  '{workspace_id:$workspace,dataset_id:$dataset_id,idempotency_key:$key,dry_run:true,batch_limit:1}')
dry_response=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evaluations/retrieval/runs" -d "${dry_body}")
dry_run_id=$(echo "${dry_response}" | jq -r '.run.id')
[[ "$(echo "${dry_response}" | jq -r '.dry_run')" == "true" && "$(echo "${dry_response}" | jq -r '.run.status')" == "succeeded" ]] || { echo "dry-run evaluation failed" >&2; exit 1; }
dry_status=$(curl -sS -o /dev/null -w '%{http_code}' "${H[@]}" -X GET "${FORNIX_URL}/v1/evaluations/runs/${dry_run_id}?workspace_id=${workspace}")
[[ "${dry_status}" == "404" ]] || { echo "dry-run created durable run (${dry_status})" >&2; exit 1; }

run_key="run-${stamp}"
run_body=$(jq -nc --arg workspace "${workspace}" --arg dataset_id "${dataset_id}" --arg key "${run_key}" \
  '{workspace_id:$workspace,dataset_id:$dataset_id,idempotency_key:$key,batch_limit:1}')
run_response=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evaluations/retrieval/runs" -d "${run_body}")
run_id=$(echo "${run_response}" | jq -r '.run.id')
[[ "$(echo "${run_response}" | jq -r '.run.status')" == "succeeded" ]] || { echo "durable evaluation failed: ${run_response}" >&2; exit 1; }
duplicate_run=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evaluations/retrieval/runs" -d "${run_body}")
[[ "$(echo "${duplicate_run}" | jq -r '.run.id')" == "${run_id}" ]] || { echo "duplicate evaluation changed run identity" >&2; exit 1; }
run_get=$(curl -fsS "${H[@]}" -X GET "${FORNIX_URL}/v1/evaluations/runs/${run_id}?workspace_id=${workspace}")
[[ "$(echo "${run_get}" | jq -r '.retrieval_quality.cases')" == "1" ]] || { echo "evaluation status/metrics missing" >&2; exit 1; }

baseline_key="baseline-${stamp}"
baseline_body=$(jq -nc --arg workspace "${workspace}" --arg dataset_id "${dataset_id}" --arg key "${baseline_key}" --arg baseline "${run_id}" \
  '{workspace_id:$workspace,dataset_id:$dataset_id,idempotency_key:$key,baseline_eval_run_id:$baseline,batch_limit:1}')
baseline_response=$(curl -fsS "${H[@]}" -X POST "${FORNIX_URL}/v1/evaluations/retrieval/runs" -d "${baseline_body}")
[[ "$(echo "${baseline_response}" | jq '.run.regressions | length')" -ge 7 ]] || { echo "baseline comparison was not recorded" >&2; exit 1; }

# The local CLI consumes only the redacted surface and evidence hash. Running
# it twice must produce byte-identical output.
bundle=$(mktemp)
out_one=$(mktemp)
out_two=$(mktemp)
bundle_name=$(basename "${bundle}")
bundle_dir=$(dirname "${bundle}")
trap 'rm -f "${bundle}" "${out_one}" "${out_two}"' EXIT
surface_json=$(echo "${surface_response}" | jq -c '.surface')
dataset_json=$(echo "${dataset_response}" | jq -c '.dataset')
jq -n --arg workspace "${workspace}" --argjson dataset "${dataset_json}" --argjson surface "${surface_json}" \
  '{workspace_id:$workspace,dataset:$dataset,surfaces:{($surface.id):$surface}}' >"${bundle}"
docker run --rm --add-host=host.docker.internal:host-gateway -v "${REPO_ROOT}:/workspace" -v "${bundle_dir}:/input:ro" -w /workspace "${GO_IMAGE}" \
  go run ./cmd/fornix-eval -input "/input/${bundle_name}" >"${out_one}"
docker run --rm --add-host=host.docker.internal:host-gateway -v "${REPO_ROOT}:/workspace" -v "${bundle_dir}:/input:ro" -w /workspace "${GO_IMAGE}" \
  go run ./cmd/fornix-eval -input "/input/${bundle_name}" >"${out_two}"
cmp -s "${out_one}" "${out_two}" || { echo "offline CLI output was not deterministic" >&2; exit 1; }
[[ "$(jq -r .passed "${out_one}")" == "true" ]] || { echo "offline CLI quality gate failed" >&2; exit 1; }

echo "all v0.26 retrieval evaluation smokes passed (workspace=${workspace}, evidence_id=${evidence_id}, run=${run_id})"
