#!/usr/bin/env bash
# Baseline smokes — exercises the watcher/indexer contract, MCP shim, validation, and grader.
# Requires: curl and jq. Python dependencies are installed by `make python-install`.
set -euo pipefail

FORNIX_URL="${FORNIX_URL:-http://localhost:8201}"
FORNIX_KEY="${FORNIX_KEY:-test-key-1}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
export FORNIX_URL FORNIX_KEY
H=(-H "Authorization: Bearer $FORNIX_KEY" -H "Content-Type: application/json")
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PASS=0; FAIL=0
ok()  { echo "  PASS — $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL — $1"; FAIL=$((FAIL+1)); }

echo "== version =="
ver=$(curl -sS "$FORNIX_URL/v1/health" | "$PYTHON_BIN" -c 'import json,sys; print(json.load(sys.stdin)["version"])')
[ "$ver" = "0.10.1" ] && ok "fornix reports v$ver" || bad "expected v0.10.1, got v$ver"

# ---------- D3: real-data validation ----------
echo "== D3: index + query — fornix =="
FORNIX_URL="$FORNIX_URL" FORNIX_KEY="$FORNIX_KEY" "$PYTHON_BIN" "$ROOT_DIR/scripts/fornix-indexer.py" --repo fornix --root "$ROOT_DIR" > /tmp/idx-fornix.log
grep -q "upserted" /tmp/idx-fornix.log && ok "indexer ran cleanly" || bad "indexer failed (see /tmp/idx-fornix.log)"

count=$(curl -sS "${H[@]}" -X POST "$FORNIX_URL/v1/symbol/search" -d '{"query":"handleSearch","top_k":1,"repo":"fornix"}' \
        | "$PYTHON_BIN" -c 'import json,sys; r=json.load(sys.stdin)["results"]; print(len(r))')
[ "$count" -ge 1 ] && ok "handleSearch found in Fornix" || bad "handleSearch missing from Fornix index"

for sym in handleSearch handleSymbolUpsert handleRouterObservation pollPeersOnce gradeOutcome; do
  top=$(curl -sS "${H[@]}" -X POST "$FORNIX_URL/v1/symbol/search" \
        -d "{\"query\":\"$sym\",\"top_k\":1,\"repo\":\"fornix\"}" \
        | "$PYTHON_BIN" -c "import json,sys; r=json.load(sys.stdin)['results']; print(r[0]['symbol_name'] if r else '')")
  [ "$top" = "$sym" ] && ok "top hit = $sym" || bad "top hit for $sym was '$top'"
done

t0=$("$PYTHON_BIN" -c 'import time; print(time.monotonic_ns())')
curl -sS "${H[@]}" -X POST "$FORNIX_URL/v1/symbol/search" -d '{"query":"handleSearch","top_k":10}' > /dev/null
t1=$("$PYTHON_BIN" -c 'import time; print(time.monotonic_ns())')
dt=$(( (t1-t0) / 1000000 ))
[ "$dt" -lt 200 ] && ok "symbol_search p1 = ${dt}ms (<200ms)" || bad "symbol_search slow: ${dt}ms"

# ---------- D2: MCP shim ----------
echo "== D2: MCP shim — tools/list + symbol_search =="
( printf '%s\n' \
    '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
    '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
    '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fornix__symbol_search","arguments":{"query":"handleSearch","top_k":1}}}' \
    '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"fornix__symbol_context","arguments":{"query":"handleSearch","top_k":1}}}' \
) | FORNIX_URL="$FORNIX_URL" FORNIX_KEY="$FORNIX_KEY" "$PYTHON_BIN" "$ROOT_DIR/scripts/fornix-mcp.py" > /tmp/mcp-out.jsonl

grep -q '"fornix__symbol_search"'   /tmp/mcp-out.jsonl && ok "tools/list exposes fornix__symbol_search"   || bad "fornix__symbol_search missing from tools/list"
grep -q '"fornix__symbol_callers"'  /tmp/mcp-out.jsonl && ok "tools/list exposes fornix__symbol_callers"  || bad "fornix__symbol_callers missing from tools/list"
grep -q '"fornix__symbol_callees"'  /tmp/mcp-out.jsonl && ok "tools/list exposes fornix__symbol_callees"  || bad "fornix__symbol_callees missing from tools/list"
grep -q '"fornix__symbol_context"'  /tmp/mcp-out.jsonl && ok "tools/list exposes fornix__symbol_context"  || bad "fornix__symbol_context missing from tools/list"
grep -q 'handleSearch'              /tmp/mcp-out.jsonl && ok "symbol_search round-trips through MCP"      || bad "symbol_search round-trip failed"

# ---------- D4: grader ----------
echo "== D4: quality grader =="
graded_ok=$(curl -sS "${H[@]}" -X POST "$FORNIX_URL/v1/router/observation" \
            -d '{"request_hash":"v09-smoke-1","task_category":"smoke","model_id":"sonnet-4-6","cost_usd":0.01,"latency_ms":1200,"outcome":"success"}' \
            | "$PYTHON_BIN" -c 'import json,sys; d=json.load(sys.stdin); print(d.get("outcome_score") or 0)')
"$PYTHON_BIN" -c "import sys; v=float('$graded_ok'); sys.exit(0 if v >= 0.8 else 1)" && ok "success auto-graded >=0.8 ($graded_ok)" || bad "success grade too low ($graded_ok)"

graded_fail=$(curl -sS "${H[@]}" -X POST "$FORNIX_URL/v1/router/observation" \
              -d '{"request_hash":"v09-smoke-2","task_category":"smoke","model_id":"sonnet-4-6","cost_usd":0.01,"latency_ms":3200,"outcome":"failed"}' \
              | "$PYTHON_BIN" -c 'import json,sys; d=json.load(sys.stdin); print(d.get("outcome_score") or 0)')
"$PYTHON_BIN" -c "import sys; v=float('$graded_fail'); sys.exit(0 if v <= 0.3 else 1)" && ok "failure auto-graded <=0.3 ($graded_fail)" || bad "failure grade too high ($graded_fail)"

echo
echo "== summary: pass=$PASS fail=$FAIL =="
exit $((FAIL>0?1:0))
