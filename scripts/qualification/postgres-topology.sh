#!/bin/sh

# Run the provider-neutral PostgreSQL topology and pool qualification against
# an explicitly named deployment-owned database. The test never prints the DSN
# and does not claim to perform HA failover or PITR unless a deployment runs
# those drills separately.
set -eu

if [ -z "${FORNIX_TOPOLOGY_PG_DSN:-}" ]; then
	printf '%s\n' 'Set FORNIX_TOPOLOGY_PG_DSN to an explicitly named qualification database.' >&2
	exit 1
fi

go clean -cache -testcache || true
exec go test ./internal/store -run '^TestPostgresTopologyQualification$' -count=1 -v
