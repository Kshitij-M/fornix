# Loop 117 — Platform-bound sandbox qualification contract

Status: contract implementation complete; OCI runtime implementation and
deployment qualification remain open.

## Delivered

- Added a normalized `SandboxImagePlatform` contract (Linux OS, architecture,
  and optional architecture variant) and required it for every image-backed
  non-local sandbox profile and signed runtime identity.
- Bound platform identity through sandbox profile/tool-definition hashes,
  signed qualification evidence v2, sandbox admission, runner protocol v2,
  and the sealed OCI plan fingerprint.
- Added fail-closed comparisons so a request/policy profile cannot drift from
  the registered image platform or the signed qualification.
- Used `omitzero` for local-process profile serialization so a zero-valued
  platform does not change the existing local profile JSON/hash shape.
- Kept the image field semantics explicit: `ImageDigest` is the local image
  ID/config digest for Docker inspection, not a registry manifest/index digest.
  Platform identity is separate from the runner host platform.
- Added normalization, missing/invalid platform, hash-binding, legacy
  protocol/evidence rejection, qualification drift, and OCI plan mutation
  tests. The Moby image-inspection/no-pull tests remain part of the runtime
  implementation, which is not present yet.

## Verification

- `GOPROXY=off GOCACHE=<temporary cache> go test ./...` — passed.
- `GOPROXY=off GOCACHE=<temporary cache> go vet ./...` — passed.
- `git diff --check` and the documentation checker are run after the
  documentation updates for this loop.
- PostgreSQL-backed runtime-role and recovery qualification remains
  environment-gated; this loop adds no database migration.
- The temporary Go build cache is removed after verification.

## Compatibility and limits

Signed sandbox evidence is now schema v2 with a v2 domain-separated hash.
Existing v1 evidence remains historical evidence but cannot authorize new
non-local execution; deployments must re-run and re-sign qualification with
explicit platform facts. Runner protocol v1 is rejected. Local-process profile
serialization remains stable.

This change does not implement the Moby SDK adapter, image inspection, a host
runner lifecycle, container execution, image provenance/signature checking,
or OS-level isolation proof. OCI remains unavailable until those behaviors
and the supported Linux/macOS live qualification gates pass. See
[`254-moby-oci-runtime-foundation.md`](254-moby-oci-runtime-foundation.md).
