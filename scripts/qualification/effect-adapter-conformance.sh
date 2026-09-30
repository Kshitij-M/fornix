#!/bin/sh

# Validate the server-composed dynamic effect manifest without contacting a
# provider, tool, database, broker, or network. The JSON is hash-stable and
# intentionally contains authority metadata only.
set -eu

go clean -cache -testcache || true
exec go run ./cmd/fornix qualification adapter-conformance
