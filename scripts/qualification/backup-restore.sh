#!/bin/sh
# Execute a destructive-restore qualification against an explicitly separate
# PostgreSQL database. This script never runs in the default test path.
set -eu

fail() {
	printf 'fornix backup/restore qualification: %s\n' "$1" >&2
	exit 1
}

source_dsn=${FORNIX_BACKUP_SOURCE_DSN:-}
restore_dsn=${FORNIX_BACKUP_RESTORE_DSN:-}
backup_file=${FORNIX_BACKUP_FILE:-}
confirm=${FORNIX_BACKUP_RESTORE_CONFIRM:-}

[ -n "$source_dsn" ] || fail 'FORNIX_BACKUP_SOURCE_DSN is required'
[ -n "$restore_dsn" ] || fail 'FORNIX_BACKUP_RESTORE_DSN is required'
[ -n "$backup_file" ] || fail 'FORNIX_BACKUP_FILE is required'
[ "$source_dsn" != "$restore_dsn" ] || fail 'source and restore DSNs must differ'
[ "$confirm" = 'I_UNDERSTAND_THIS_RESTORES_TO_A_SEPARATE_DATABASE' ] || fail 'explicit restore confirmation is required'
case "$backup_file" in
	/*) ;;
	*) fail 'FORNIX_BACKUP_FILE must be an absolute path' ;;
esac
[ ! -e "$backup_file" ] || fail "backup file already exists; choose a new path: $backup_file"

command -v pg_dump >/dev/null 2>&1 || fail 'pg_dump is required'
command -v pg_restore >/dev/null 2>&1 || fail 'pg_restore is required'
command -v psql >/dev/null 2>&1 || fail 'psql is required'

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
		return
	fi
	command -v shasum >/dev/null 2>&1 || fail 'sha256sum or shasum is required'
	shasum -a 256 "$1" | awk '{print $1}'
}

# This fingerprint intentionally contains only counts, migration version, and
# hashes. It never emits prompts, credentials, raw payloads, or artifact bytes.
fingerprint_sql=$(cat <<'SQL'
SELECT
  (SELECT count(*)::text FROM fornix.schema_migrations),
  (SELECT COALESCE(max(version), '') FROM fornix.schema_migrations),
  (SELECT count(*)::text FROM fornix.control_events),
  (SELECT COALESCE(sum(octet_length(raw_payload)), 0)::text FROM fornix.control_events),
  (SELECT md5(COALESCE(string_agg(workspace_id || ':' || sequence || ':' || request_hash, E'\n' ORDER BY workspace_id, sequence), '')) FROM fornix.control_events),
  (SELECT count(*)::text FROM fornix.artifacts),
  (SELECT md5(COALESCE(string_agg(workspace_id || ':' || content_hash, E'\n' ORDER BY workspace_id, content_hash), '')) FROM fornix.artifacts),
  (SELECT count(*)::text FROM fornix.operations),
  (SELECT md5(COALESCE(string_agg(workspace_id || ':' || id || ':' || operation_hash, E'\n' ORDER BY workspace_id, id), '')) FROM fornix.operations);
SQL
)

source_fingerprint=$(psql "$source_dsn" -X -v ON_ERROR_STOP=1 -Atqc "$fingerprint_sql")
started_at=$(date +%s)
pg_dump --format=custom --no-owner --no-privileges --file="$backup_file" "$source_dsn"
backup_finished_at=$(date +%s)
backup_checksum=$(sha256 "$backup_file")
backup_bytes=$(wc -c <"$backup_file" | tr -d ' ')

pg_restore --exit-on-error --clean --if-exists --no-owner --no-privileges --dbname="$restore_dsn" "$backup_file"
restore_finished_at=$(date +%s)
restore_fingerprint=$(psql "$restore_dsn" -X -v ON_ERROR_STOP=1 -Atqc "$fingerprint_sql")

[ "$source_fingerprint" = "$restore_fingerprint" ] || fail 'restored fingerprint differs from source'

printf 'backup_file=%s\n' "$backup_file"
printf 'backup_sha256=%s\n' "$backup_checksum"
printf 'backup_bytes=%s\n' "$backup_bytes"
printf 'backup_seconds=%s\n' "$((backup_finished_at - started_at))"
printf 'restore_seconds=%s\n' "$((restore_finished_at - backup_finished_at))"
printf 'source_restore_fingerprint=%s\n' "$source_fingerprint"
printf '%s\n' 'replay_input=append-only control history and operation hashes preserved'
