#!/bin/sh

# Generate and validate one bounded, redacted offline qualification bundle.
# No database, provider, model, tool, network, or deployment system is used.
set -eu

go clean -cache -testcache || true
file="${FORNIX_QUALIFICATION_BUNDLE_FILE:-${TMPDIR:-/tmp}/fornix-portable-qualification.json}"
cleanup=0
if [ -z "${FORNIX_QUALIFICATION_BUNDLE_FILE:-}" ]; then
  cleanup=1
fi
trap 'if [ "$cleanup" -eq 1 ]; then rm -f "$file"; fi' EXIT INT TERM
go run ./cmd/fornix qualification run \
  --file "$file" \
  --run-id "${FORNIX_QUALIFICATION_RUN_ID:-portable-qualification}" \
  --target-hash "${FORNIX_QUALIFICATION_TARGET_HASH:-}" \
  --runner-version "${FORNIX_QUALIFICATION_RUNNER_VERSION:-1}" \
  --env-names "${FORNIX_QUALIFICATION_ENV_NAMES:-}"
go run ./cmd/fornix qualification validate --file "$file"
