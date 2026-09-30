#!/bin/sh

# Verify a deployment-produced redacted qualification report without contacting
# a provider or database. The report may contain only hashes, bounded metrics,
# and explicit drill outcomes; the native CLI rejects unknown/raw fields.
set -eu

report_file=${FORNIX_QUALIFICATION_REPORT_FILE:-}
if [ -z "$report_file" ]; then
	printf '%s\n' 'Set FORNIX_QUALIFICATION_REPORT_FILE to a redacted qualification report.' >&2
	exit 1
fi

go clean -cache -testcache || true
exec go run ./cmd/fornix qualification validate --file "$report_file"
