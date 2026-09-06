#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
binary=${FORNIX_BINARY:-$repo_root/bin/fornix}
home=$(mktemp -d "${TMPDIR:-/tmp}/fornix-local-smoke.XXXXXX")
cleanup() { rm -rf "$home"; }
trap cleanup EXIT HUP INT TERM

[ -x "$binary" ] || { echo "local CLI smoke: missing executable $binary" >&2; exit 1; }

version=$(FORNIX_HOME="$home" "$binary" version --json)
VERSION_JSON="$version" python3 -c 'import json,os
v=json.loads(os.environ["VERSION_JSON"])
assert v["name"] == "fornix"
assert v["schema_version"] == 1
assert "commit" in v and "build_date" in v
print("local CLI smoke: version ok")'

help=$(FORNIX_HOME="$home" "$binary" --help)
printf '%s\n' "$help" | grep -F 'fornix start' >/dev/null
printf '%s\n' "$help" | grep -F 'fornix run --repo' >/dev/null
echo 'local CLI smoke: help ok'

doctor=$(FORNIX_HOME="$home" "$binary" doctor --json)
DOCTOR_JSON="$doctor" python3 -c 'import json,os
v=json.loads(os.environ["DOCTOR_JSON"])
assert v["checks"]["profile"]["status"] == "warning"
print("local CLI smoke: redacted first-run doctor ok")'

error_output="$home/invalid-budget.err"
if FORNIX_HOME="$home" "$binary" run --max-turns 0 'invalid budget' >"$home/invalid-budget.out" 2>"$error_output"; then
  echo 'local CLI smoke: invalid budget unexpectedly succeeded' >&2
  exit 1
fi
grep -F 'positive integer' "$error_output" >/dev/null
echo 'local CLI smoke: budget validation ok'
