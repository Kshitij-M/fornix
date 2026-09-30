# Task 78 — Signed deployment-evidence import and qualification bundle

Status: implemented repository-owned qualification slice; not a
production-readiness declaration.

## Problem

Fornix can already validate and merge bounded, redacted qualification bundles,
but an unsigned JSON file does not identify who attested to deployment-owned
facts. The importer must add authenticity without making Fornix the authority
for deployment secrets, topology, or private signing keys.

## Scope and invariants

1. A signed bundle contains one normalized `QualificationBundle`, one explicit
   Ed25519 signature, a bounded signer key identifier, and the public key
   required for offline verification. Public verification material is not a
   credential; private keys are never representable in the contract.
2. The signed subject binds schema version, workspace, target hash, report
   hash, manifest hash, runner version, commit hash, sorted environment-name
   set, and a separately exposed observation hash. Editing any of these facts
   invalidates the signature.
3. The observation hash is derived only from normalized redacted report and
   manifest evidence. Raw prompts, provider payloads, DSNs, certificate bytes,
   credential values, arbitrary environment values, and private keys cannot
   enter the subject.
4. Import and validation require a valid signature, matching public key,
   matching signed subject, normalized bundle, and bounded JSON. Missing,
   malformed, stale, or cross-workspace material fails closed.
5. Duplicate signed bundles with the same signed hash are idempotent for merge;
   distinct signatures or evidence for the same check identity fail closed.
   Merging verifies every input before composing a new deterministic unsigned
   aggregate. A caller may explicitly sign the aggregate with a deployment key.
6. A local `fornix qualification sign` command is a convenience for operators
   holding a key outside the repository. It reads the key only for the process,
   never stores or prints it, and is not a production-certification claim.

## Signature and key lifecycle

Ed25519 is selected because it is in the Go standard library, has a fixed
bounded key/signature size, and does not add infrastructure. `key_id` is an
operator-controlled reference to an external key registry or deployment
identity. The embedded public key permits offline verification but does not
replace key rotation, revocation, or trust-policy management. Production
importers must authorize the key identifier against their deployment policy;
Fornix's offline validator proves cryptographic integrity only.

Private key input is accepted only from an explicit operator-selected file by
the CLI signer. The bytes are parsed in memory, excluded from all errors,
reports, events, artifacts, and output, and are eligible for process cleanup
after signing. The repository never creates, persists, rotates, or backs up
private keys.

## Merge, scope, and replay semantics

Every signed input is normalized and verified before merge. Workspace and target
scope must match the caller's requested scope. The aggregate is produced by
the existing deterministic `MergeBundles` path, so case/check conflicts,
duplicate identities, over-budget reports, and cross-workspace inputs fail
closed. Existing signatures are evidence for their source bundles; they do not
silently become a signature over the aggregate. An explicit signer is required
to produce a signed aggregate.

## Reuse and licensing

This slice reuses the existing qualification contracts, stable hashes, bundle
merge, strict bounded JSON readers, atomic file writer, and CLI output pattern.
It uses Go's standard `crypto/ed25519` implementation and copies no source
from reference repositories. Orloj's signed/admission boundaries, ClawMem's
replay evidence discipline, agentmemory's diagnostics, and FornixDB's
immutable disclosure model inform the design. Kronaxis BSL 1.1 source remains
excluded; Fornix remains MIT-licensed.

## Cost and storage budget

Signing and verification are local CPU operations with fixed-size key material;
no database, model, provider, broker, network, migration, or artifact is
required. Signed bundles remain within the existing 128 KiB bounded evidence
limit. The only additional persistent bytes are the public key, key reference,
signature, signed subject hash, and observation hash in an operator-selected
file or deployment evidence system.

## Acceptance tests

- Identical bundles signed by the same key produce identical signed subjects,
  observation hashes, and signatures.
- Valid signatures verify offline; modified report, manifest, workspace,
  target, commit, environment-name set, observation, key, or signature fails.
- Unknown JSON fields, private-key-shaped fields, oversized bundles, malformed
  keys, unsupported algorithms, and missing signer identity fail closed.
- Cross-workspace and cross-target imports fail closed.
- Duplicate signed inputs merge idempotently; conflicting evidence fails.
- Merging signed inputs is deterministic and does not falsely reuse a source
  signature for the aggregate.
- CLI signing reads a private key without printing or persisting it.
- Offline validation never contacts Fornix, PostgreSQL, providers, tools,
  brokers, or key services.
- Existing unsigned offline qualification and all prior tests remain green.

## Explicit limitation

An offline signature proves possession of a private key and integrity of the
redacted evidence subject. It does not prove that the key is trusted by a
deployment, that the evidence is truthful, or that HA/PITR, failover,
credential rotation, provider idempotency, sandbox strength, or load/soak
behavior occurred. Those claims require deployment-owned policy and drills.

## Delivered

- Added `QualificationSignature` and `SignedQualificationBundle` contracts
  using standard-library Ed25519 with bounded public key, signature, key ID,
  observation hash, and signed-subject fields.
- Bound the signed subject to schema version, workspace, target, report and
  manifest hashes, runner version, commit hash, environment-name set, and
  normalized redacted observations.
- Added offline `Verify`, external trust-key binding, deterministic signed
  merge, duplicate suppression, conflicting-evidence rejection, and exact
  workspace/target scope checks.
- Added `fornix qualification sign`, `validate-signed`, `import`,
  `merge-signed`, and `hash-signed` commands. Signing reads an explicit key
  file only for the process; it never generates, persists, logs, or prints
  private key material.
- Added restrictive atomic signed-bundle writes, strict unknown-field
  rejection, bounded key/bundle reads, focused unit tests, a Make target, and
  offline CI coverage.

## Operator workflow

Generate a portable bundle first, then sign it with a deployment-owned key
that is stored outside the repository:

    fornix qualification run --file portable.json
    fornix qualification sign --file portable.json \
      --output signed.json --key-file /protected/qualification-key.hex \
      --key-id deployment-key-v1

Validate cryptographic integrity and bind the bundle to an external trust
catalog at import time:

    fornix qualification validate-signed --file signed.json \
      --workspace WORKSPACE_ID --target-hash TARGET_HASH \
      --key-id deployment-key-v1 --public-key-file /protected/key.pub
    fornix qualification import --file signed.json \
      --workspace WORKSPACE_ID --target-hash TARGET_HASH \
      --key-id deployment-key-v1 --public-key-file /protected/key.pub

The `import` command is an offline validation/import boundary in this slice;
it does not silently write a Fornix database row. A deployment-owned caller
must decide where verified evidence is retained. To combine independently
signed evidence, validate and merge the inputs, then explicitly sign the new
aggregate:

    fornix qualification merge-signed --file aggregate.json \
      --inputs signed-a.json,signed-b.json \
      --key-file /protected/qualification-key.hex \
      --key-id deployment-key-v1

The embedded public key supports offline verification but is not itself a
trust decision. Key rotation, revocation, and signer authorization remain
deployment-owned. The command output contains hashes and bounded metadata
only; it never contains prompts, credentials, DSNs, provider payloads, or
private key values.
