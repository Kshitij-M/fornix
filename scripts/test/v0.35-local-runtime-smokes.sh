#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
binary=${FORNIX_BINARY:-$repo_root/bin/fornix}
image=${FORNIX_LOCAL_IMAGE:-fornix-local:smoke}
port=${FORNIX_LOCAL_PORT:-18281}
project=${FORNIX_RUNTIME_PROJECT:-fornix-local-smoke}
home=$(mktemp -d "${TMPDIR:-/tmp}/fornix-local-runtime.XXXXXX")

docker_bin=${FORNIX_DOCKER_PATH:-}
if [ -z "$docker_bin" ]; then
  docker_bin=$(command -v docker 2>/dev/null || true)
fi
if [ -z "$docker_bin" ] && [ -x /usr/local/bin/docker ]; then
  docker_bin=/usr/local/bin/docker
fi

cleanup() {
  FORNIX_HOME="$home" FORNIX_RUNTIME_PROJECT="$project" FORNIX_IMAGE="$image" FORNIX_PORT="$port" FORNIX_DOCKER_PATH="$docker_bin" "$binary" uninstall --purge-data --yes --json >/dev/null 2>&1 || true
  rm -rf "$home"
}
trap cleanup EXIT HUP INT TERM

[ -x "$binary" ] || { echo "local runtime smoke: missing executable $binary" >&2; exit 1; }
[ -n "$docker_bin" ] || { echo "local runtime smoke: Docker executable was not found" >&2; exit 1; }
"$docker_bin" image inspect "$image" >/dev/null 2>&1 || { echo "local runtime smoke: missing image $image; build it with: docker build --tag $image ." >&2; exit 1; }

start=$(FORNIX_HOME="$home" FORNIX_RUNTIME_PROJECT="$project" FORNIX_IMAGE="$image" FORNIX_PORT="$port" FORNIX_DOCKER_PATH="$docker_bin" "$binary" start --repo "$repo_root/fixtures/reference-repo" --json)
START_JSON="$start" python3 -c 'import json,os
value=json.loads(os.environ["START_JSON"])
assert value["ready"] is True
assert value["provider"] == "fake"
print("local runtime smoke: Docker services ready")'

prompt='Explain this repository and identify its primary evidence sources'
first=$(FORNIX_HOME="$home" FORNIX_RUNTIME_PROJECT="$project" FORNIX_IMAGE="$image" FORNIX_PORT="$port" FORNIX_DOCKER_PATH="$docker_bin" "$binary" run --repo "$repo_root/fixtures/reference-repo" --provider fake --max-cost 0.25 --max-time 45s --max-turns 3 --max-output-tokens 512 --max-context-bytes 32768 --max-context-tokens 2048 --json "$prompt")
FIRST_JSON="$first" python3 -c 'import json,os
value=json.loads(os.environ["FIRST_JSON"])
assert value["run"]["run"]["state"] == "succeeded"
assert value["run"]["run"]["termination"] == "completed"
assert value["replay_verified"] is True
assert value["artifact"]["created"] is True
assert value["evidence"]["created"] is True
print("local runtime smoke: reference workflow succeeded and replayed")'

second=$(FORNIX_HOME="$home" FORNIX_RUNTIME_PROJECT="$project" FORNIX_IMAGE="$image" FORNIX_PORT="$port" FORNIX_DOCKER_PATH="$docker_bin" "$binary" run --repo "$repo_root/fixtures/reference-repo" --provider fake --max-cost 0.25 --max-time 45s --max-turns 3 --max-output-tokens 512 --max-context-bytes 32768 --max-context-tokens 2048 --json "$prompt")
FIRST_JSON="$first" SECOND_JSON="$second" python3 -c 'import json,os
first=json.loads(os.environ["FIRST_JSON"])
second=json.loads(os.environ["SECOND_JSON"])
assert second["run"]["deduplicated"] is True
assert second["run"]["run"]["id"] == first["run"]["run"]["id"]
assert second["replay_verified"] is True
assert second["artifact"]["created"] is False
assert second["evidence"]["created"] is False
print("local runtime smoke: duplicate workflow was idempotent")'

status=$(FORNIX_HOME="$home" FORNIX_RUNTIME_PROJECT="$project" FORNIX_IMAGE="$image" FORNIX_PORT="$port" FORNIX_DOCKER_PATH="$docker_bin" "$binary" status --json)
STATUS_JSON="$status" python3 -c 'import json,os
value=json.loads(os.environ["STATUS_JSON"])
assert len(value["services"]) == 2
assert all(service["State"] == "running" for service in value["services"])
print("local runtime smoke: status and service isolation ok")'
