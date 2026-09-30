# Task 87 feature note — deployment observation publisher boundary

Status: implementation design for a deployment-side evidence publisher. This
slice makes the Task 86 evidence contract usable by deployment automation; it
does not make Fornix a hosted infrastructure checker.

## Problem and scope

Task 86 can consume a signed, redacted observation of credential resolution,
workload identity, mTLS, DNS/rebinding, proxy/firewall enforcement, provider
idempotency, and recovery. The remaining usability gap is that an operator or
deployment adapter has no explicit, typed publisher boundary that says which
observations may be signed for ordinary evidence and which are strong enough
to bind an external effect.

This task adds a small offline publisher/validator over the existing
`QualificationBundle` and `SignedQualificationBundle`. It is suitable for a
deployment-owned sidecar, CI job, or runbook implementation to call after it
has performed its own checks. It does not execute those checks itself.

## Invariants

1. The publisher accepts only the existing bounded qualification bundle. It
   never accepts a secret, credential value, URL, certificate, token, header,
   provider payload, environment value, or arbitrary deployment output.
2. Every boundary observation is normalized before signing. Its source,
   boundary, request/response identities, and expiry remain hashes only.
3. A publisher result is deterministic for identical normalized input and key;
   the private key is process-only and is never included in a request, result,
   error, log, event, artifact, or report.
4. Ordinary boundary evidence may contain a bounded set of typed observations.
   An external-effect bundle must contain exactly one passed, measured
   observation whose expiry is after the explicit validation time.
5. Publisher validation is fail-closed for unknown kinds, failed or unmeasured
   passed evidence, expired evidence, duplicate IDs, cross-target bundles,
   invalid hashes, oversized bundles, and missing signatures.
6. The publisher never contacts Postgres, a secret authority, DNS, a proxy,
   certificate authority, model provider, connector, broker, or external tool.
7. Publishing does not authorize an effect. Release admission and the
   Postgres operation authority remain the only Fornix decision points.
8. Repeated signing and validation are replay-safe. A deployment may publish
   the same signed hash again; changing any observed fact creates a different
   hash and must be reviewed as a new observation.

## API and schema decisions

- Add `BoundaryPublisherOptions` with an explicit `RequireExternalEffect`
  mode and an explicit `AsOf` time for expiry validation. No implicit wall
  clock is used by the library; the CLI may supply the current UTC time when
  the operator does not provide one.
- Add `PublishBoundaryBundle` and `ValidateBoundaryBundle` helpers that reuse
  the existing report normalization, signed-subject hash, and Ed25519
  implementation.
- Add `qualification boundary-sign` and `qualification boundary-validate`
  commands. They read and write bounded local files, print hashes and status,
  and never print key material. The ordinary `qualification sign` command
  remains available for non-boundary bundles.
- No database migration is required. The output is the existing signed bundle
  consumed by Task 86; no second authority or alternate evidence format is
  introduced.

## Crash, idempotency, and failure semantics

The publisher writes only through the existing restrictive atomic file writer.
A failed signing or validation leaves the destination unchanged. Re-running a
command with the same normalized input and key is safe and yields the same
signed hash. A changed output path is the caller's responsibility; Fornix
does not delete or overwrite an existing file implicitly.

## Reuse and licensing

The implementation reuses the qualification contracts, bounded JSON readers,
Ed25519 signing, stable hashes, and atomic file handling already present in
Fornix. No reference-repository source is copied. Patterns from Orloj,
ClawMem, agentmemory, and FornixDB inform the signed evidence, replay,
redaction, and bounded disclosure model. Kronaxis remains excluded because its
repository is BSL 1.1. Fornix remains MIT licensed.

## Cost and storage budget

The library performs bounded in-process normalization and Ed25519 signing.
There is no database work, network call, model/token cost, container, or new
persistent table. The output remains within the existing 128 KiB signed
qualification limit. Deployment-side checks may have their own cost and time
budgets; this publisher records only their redacted hashes and must not hide
those costs in Fornix's local qualification result.

## Acceptance tests

- Identical normalized bundles and keys produce identical signed hashes.
- Ordinary typed boundary evidence validates offline and preserves all hashes.
- External-effect mode requires exactly one passed, measured, non-expired
  observation and rejects zero, multiple, failed, unmeasured, or expired data.
- Unknown kinds, duplicate IDs, cross-target inputs, invalid hashes,
  unknown JSON fields, oversized files, and secret-shaped fields fail closed.
- Tampering with evidence, expiry, target, workspace, manifest, signature, or
  public key fails validation.
- Private key bytes do not occur in command output, error text, serialized
  results, or repository files.
- Dry-run/validation never mutates the input or output file.
- Existing unit, race, vet, package, docs, smoke, and offline qualification
  gates remain green. Database-backed gates remain conditional on a configured
  disposable PostgreSQL DSN.
