#!/bin/sh
set -eu

if [ -z "${FORNIX_TEST_PG_DSN:-}" ]; then
	printf '%s\n' 'universal federation smoke: FORNIX_TEST_PG_DSN is required' >&2
	exit 1
fi

go test ./internal/contracts ./internal/adapters/fakeincident ./internal/connector \
	-run 'Test(Federation|EffectfulExecution|EffectAuthority|ConnectorIsDeterministic)' -count=1 -v
FORNIX_TEST_PG_DSN="$FORNIX_TEST_PG_DSN" go test ./internal/store ./internal/federation ./internal/server \
	-run 'Test(Federation|Poller|FederationPeerAPI|SecurityMiddlewareAllowsWorkspaceFederationRoutes)' -count=1 -v
printf '%s\n' 'universal federation smoke: workspace peer authority, fencing, egress, recovery, reconciliation, quarantine, retention, and adapter identity passed'
