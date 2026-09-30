#!/bin/sh
# Run the opt-in live deployment-authority qualification. The endpoint and
# token environment-variable name are metadata; the token value is never
# passed on the command line, written to disk, or echoed by this script.
set -eu

if [ -z "${FORNIX_LIVE_AUTHORITY_URL:-}" ]; then
	printf '%s\n' 'fornix credential authority qualification: FORNIX_LIVE_AUTHORITY_URL is required' >&2
	exit 1
fi

go test ./internal/credentials -run '^TestLiveAuthorityQualification$' -count=1 -v
