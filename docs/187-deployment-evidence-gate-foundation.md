# Task 81 — Deployment release and live-evidence gate foundation

Status: feature note; implementation follows.

## Problem and boundary

Fornix can now produce bounded signed qualification evidence, authorize its
import through a workspace/deployment signer catalog, and distribute the
exact trust snapshot used for new imports. Operators still have to assemble
release identity, migration, recovery, topology, provider, and external-effect
evidence outside the control plane. That makes it difficult to answer a
small but important production question:

> Which release, for which workspace and deployment, has which independently
> imported evidence, and is it currently admissible?

This slice adds a durable release/evidence index and a deterministic
qualification gate. It does not run backups, migrations, providers, tools,
failovers, or external effects. Those remain deployment-owned operations that
produce signed bundles through the existing qualification runner and import
boundary.

The new tables reference accepted Task 79/80 imports by identity and hash;
they do not duplicate signed evidence bytes. PostgreSQL remains the authority.

## Invariants

1. A release is scoped to one workspace and deployment. Its release hash,
   target hash, version, commit hash, and imported trust-snapshot revision/hash
   are immutable. A duplicate identity is idempotent; conflicting metadata
   fails closed.
2. A release can be registered only against a current, non-revoked trust
   snapshot. The exact snapshot revision/hash is retained with the release;
   readiness does not silently move to a newer snapshot.
3. Evidence links reference one accepted qualification import. The link
   preserves import ID, signed/observation/report hashes, evidence kind,
   outcome, recovery state, source hash, and actor metadata, but never copies
   the signed payload or credentials.
4. Evidence kinds are a closed, bounded vocabulary: `release`, `migration`,
   `backup_restore`, `topology`, `provider`, and `external_effect`. A release
   may have at most one link per kind and imported evidence must match the
   release target and trust-snapshot hash.
5. A gate is ready only when the configured required kinds are present, every
   linked import is accepted and passed, no link is stale/contradictory, the
   release snapshot is still current and valid, and no linked observation
   reports an unresolved recovery-required/unknown external outcome.
6. Release and link history is append-only. Current rows are projections for
   indexed lookup; event rows preserve publication and replacement attempts.
   Releasing or superseding a deployment does not erase prior evidence.
7. All reads and writes are workspace-scoped, actor-authorized, bounded, and
   redacted. Raw evidence is disclosed only by the existing import disclosure
   path. No DSN, credential, private key, prompt, SQL text, or provider body
   is representable in these contracts.
8. Gate evaluation is read-only and deterministic. It may re-verify the
   current snapshot/import hashes but never calls a provider, tool, broker,
   migration runner, backup utility, or external effect.

## Contracts and schema

- Add `DeploymentRelease`, `DeploymentEvidenceKind`, `DeploymentEvidenceLink`,
  `DeploymentReleaseRequest`, `DeploymentEvidenceLinkRequest`,
  `DeploymentQualificationGate`, and bounded page contracts.
- Add migration 068 with workspace/deployment-scoped release rows, evidence
  link rows, append-only lifecycle events, uniqueness constraints, bounded
  metadata, and RLS.
- Reuse Task 79/80 qualification imports as the raw-evidence authority. The
  new store resolves the imported signed bundle inside a transaction, derives
  only stable redacted hashes/outcomes, and stores the exact trust-snapshot
  binding without copying bytes.
- Add authenticated API/CLI inspection and linking operations. Creation and
  link operations are idempotent by release identity and evidence identity;
  gate inspection is paginated/bounded and side-effect free.

## Release, recovery, and external-effect semantics

Release registration proves only that a deployment-owned release identity was
bound to a currently trusted snapshot. Migration, backup/restore, topology,
provider, and effect cases remain deployment-owned evidence. An external
effect whose outcome is unknown must be imported as `recovery_required` or
`unknown` and blocks the gate; the index never upgrades at-least-once work to
exactly-once success. Replay reads imported facts only and never re-executes
remote work.

Evidence from an older snapshot cannot be linked to a release bound to a
newer snapshot. This prevents a valid historical signature from being used as
current release qualification without rewriting history. Previous releases
remain inspectable.

## Reuse, licensing, and cost

The implementation reuses `QualificationBundle`/`SignedQualificationBundle`,
Task 79 import identity, Task 80 snapshot binding, existing workspace
transactions/RLS, actor propagation, strict bounded JSON, release archive
verification, backup/topology/effect probes, and the existing qualification
CLI/API conventions. Orloj controller/evaluation status patterns,
agentmemory bounded diagnostics/replay, ClawMem recorded-result replay, and
FornixDB provenance/retention discipline inform the design. No reference code
is copied; Kronaxis remains excluded because it is BSL 1.1. Fornix remains
MIT-licensed.

Release registration and link operations are one bounded Postgres transaction
with indexed identity lookups. Evidence links add hashes and small metadata,
not another copy of a signed report. Gate reads are bounded by six evidence
kinds and one current-snapshot verification. No model tokens, provider calls,
new infrastructure, or durable raw output are added.

## Acceptance tests

- Fresh and existing databases apply migration 068 without changing prior
  import bytes, hashes, or snapshot bindings.
- A release binds the current snapshot exactly once; duplicate replay is
  stable and conflicting target/version/snapshot data fails closed.
- Evidence links accept only accepted same-workspace/same-target imports with
  matching snapshot hash and closed evidence kind; cross-workspace, stale,
  revoked, contradictory, and unresolved-recovery evidence fails closed.
- Linking is idempotent and never duplicates imported raw bytes.
- Gate evaluation is deterministic, read-only, bounded, and fails closed when
  a required kind is absent, failed, stale, unknown, or recovery-required.
- Snapshot or signer revocation makes a current gate unavailable without
  deleting historical release/evidence links.
- Concurrent release/link delivery yields one durable identity; rollback
  before commit creates no release/link; replay after commit is safe.
- API/CLI authorization, pagination, redaction, race checks, migration checks,
  CI, smokes, and documentation checks remain green.

## Explicit limitations

This slice does not certify a hosted release, backup schedule, PITR restore,
HA failover, provider SLA, sandbox, or external-effect truth. It indexes
deployment-owned signed evidence and provides a fail-closed gate; operators
must still run the drills, protect private keys, retain raw operational logs
outside Fornix where required, and qualify the intended topology.
