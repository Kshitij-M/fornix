#!/bin/sh
# Run the bounded federation authority qualification against an explicitly
# disposable PostgreSQL database. It never chooses the development database.
set -eu

if [ -z "${FORNIX_FEDERATION_CAPACITY_PG_DSN:-}" ]; then
	printf '%s\n' 'fornix federation capacity qualification: FORNIX_FEDERATION_CAPACITY_PG_DSN is required' >&2
	exit 1
fi

go test ./internal/store -run '^TestFederationCapacityQualification$' -count=1 -v
