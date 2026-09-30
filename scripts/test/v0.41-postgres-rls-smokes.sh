#!/bin/sh
set -eu

if [ -z "${FORNIX_RLS_TEST_DSN:-}" ]; then
	printf '%s\n' 'postgres RLS smoke: FORNIX_RLS_TEST_DSN is required' >&2
	exit 1
fi

FORNIX_RLS_TEST_DSN="$FORNIX_RLS_TEST_DSN" go test ./internal/store \
	-run '^TestPostgresWorkspaceIsolationQualification$' -count=1 -v
printf '%s\n' 'postgres RLS smoke: non-owner workspace isolation passed'
