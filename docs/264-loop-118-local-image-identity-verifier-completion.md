# Loop 118 — Local sandbox image identity verifier

Status: pure pre-create verification slice complete; Moby adapter and live OCI
execution remain open.

## Delivered

- Added `LocalImageInspector`, a deliberately inspection-only interface with
  no pull, tag-resolution, build, import, or mutable-name operation.
- Added a plan-bound verifier that asks for exactly the sealed full local image
  ID and compares the inspected image ID plus normalized OS/architecture/
  variant against the plan before a caller can create a container.
- Invalid plans fail before inspection. Missing images, mismatched IDs, and
  mismatched platforms fail closed with stable redacted errors. Cancellation
  is propagated without exposing the Engine error.
- Added tests for exact match, missing image, abbreviated/tag/different IDs,
  OS/architecture/variant mismatch, invalid plan, cancellation, and error
  redaction.

## Verification

- `GOPROXY=off GOCACHE=<temporary cache> go test ./internal/sandboxrunner ./internal/contracts ./internal/tool ./internal/qualification` — passed.
- The temporary Go cache is removed after verification.

## Remaining proof

This verifier has no Engine implementation and performs no container
operation. Tests prove only the plan-to-inspected-metadata comparison. The
official Moby adapter must call it before create; a fake inspector cannot
prove that integration, Docker's no-pull behavior, runtime enforcement,
cleanup, or crash recovery. OCI remains unavailable until the Moby lifecycle,
Postgres authority checks, and supported-host qualification gates in
[`254-moby-oci-runtime-foundation.md`](254-moby-oci-runtime-foundation.md)
pass.
