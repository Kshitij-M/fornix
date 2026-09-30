#!/bin/sh

# Run the built-in fixture/recorded adapter matrix. This is deterministic and
# side-effect free; live provider qualification requires an explicit
# deployment-owned runner and credentials outside this default command.
set -eu

go clean -cache -testcache || true
exec go test ./internal/qualification -run '^TestMatrixQualifiesBuiltInReadAdaptersDeterministically$' -count=1 -v
