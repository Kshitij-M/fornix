#!/bin/sh

# Validate the hash-only external-boundary contract for every built-in network
# effect path without contacting a provider, database, broker, or network.
set -eu

go clean -cache -testcache || true
exec go run ./cmd/fornix qualification external-boundary
