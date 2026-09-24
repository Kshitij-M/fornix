# Backup and restore qualification foundation

Status: implemented as an explicit destructive-restore drill; measured RPO/RTO and production scheduling remain operator-owned.

## Purpose

Durable history is not production-trustworthy until an operator can create a
consistent backup, restore it into a separate database, and prove that
migrations, workspace identity, event history, artifact hashes, and operation
authority survive the round trip.

## Safety boundary

The qualification script is never part of `make check` or the normal smoke
suite. It requires:

- separate source and restore DSNs;
- an absolute output path that does not already exist;
- explicit confirmation text for the destructive restore target;
- `pg_dump`, `pg_restore`, and `psql`.

The script does not print DSNs, credentials, prompts, raw payloads, or artifact
bytes. It prints only the backup path, SHA-256, byte size, durations, and a
redacted catalog fingerprint.

## Verification contract

The drill captures a source fingerprint containing migration count/version,
control-event count and raw-payload bytes, workspace/sequence/request-hash
event digest, artifact count/content digest, and operation count/hash digest.
It performs a custom-format `pg_dump`, restores into the explicitly separate
target with `pg_restore`, recomputes the fingerprint, and fails closed on any
mismatch.

The fingerprint is not a substitute for replay. After a successful restore,
operators must run the bounded operation replay and representative workspace
isolation smokes against the restored database, then record those hashes in
the qualification evidence.

## RPO/RTO and cost

The script reports backup and restore wall time and backup size. It does not
invent an RPO: the measured RPO depends on the backup schedule and WAL/PITR
configuration supplied by the deployment. It adds no runtime database rows
and no service dependency; the cost is one database read snapshot plus the
PostgreSQL backup/restore stream.

## Licensing and reuse

The script uses the standard PostgreSQL client tools and existing Fornix
workspace/hash conventions. No reference-repository source is copied; the
repository remains MIT licensed.

## Acceptance tests

- missing or equal DSNs fail before any destructive command;
- missing explicit confirmation fails closed;
- relative or pre-existing backup paths fail closed;
- source backup completes and emits a checksum without secret material;
- restore into a clean separate database succeeds;
- migration, event, artifact, and operation fingerprints match;
- a changed restore fingerprint fails;
- post-restore replay and workspace-isolation qualification can run against
  the restored DSN.
