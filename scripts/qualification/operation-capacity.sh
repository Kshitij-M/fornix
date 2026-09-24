#!/bin/sh
set -eu

if [ -z "${FORNIX_CAPACITY_PG_DSN:-}" ]; then
	printf '%s\n' 'fornix capacity qualification: FORNIX_CAPACITY_PG_DSN is required' >&2
	exit 1
fi

FORNIX_CAPACITY_PG_DSN="$FORNIX_CAPACITY_PG_DSN" go test ./internal/store \
	-run '^TestOperationCapacityQualification$' -count=1 -v
