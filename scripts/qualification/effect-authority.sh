#!/bin/sh

# Run the authority-backed effect qualification only against an explicitly
# disposable PostgreSQL database. This is intentionally opt-in and is not a
# substitute for hosted production qualification.
set -eu

: "${FORNIX_TEST_PG_DSN:?Set FORNIX_TEST_PG_DSN to a disposable PostgreSQL database.}"
: "${FORNIX_DISPOSABLE_DATABASE_CONFIRM:?Set FORNIX_DISPOSABLE_DATABASE_CONFIRM=1 to confirm the database is disposable.}"
if [ "$FORNIX_DISPOSABLE_DATABASE_CONFIRM" != "1" ]; then
  echo 'FORNIX_DISPOSABLE_DATABASE_CONFIRM must equal 1.' >&2
  exit 1
fi

go clean -cache -testcache || true
FORNIX_RUN_EFFECT_AUTHORITY_PROBE=1 \
  FORNIX_DISPOSABLE_DATABASE_CONFIRM=1 \
  go test ./internal/qualification -run '^TestPostgresEffectAuthorityQualification$' -count=1 -v
