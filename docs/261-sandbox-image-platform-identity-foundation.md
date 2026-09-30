# Sandbox image platform identity foundation

Status: feature note and acceptance contract; implementation is not complete.
This note closes a contract gap discovered while preparing the Moby-backed OCI
runner. OCI remains unavailable until the implementation and live qualification
gates in [`254-moby-oci-runtime-foundation.md`](254-moby-oci-runtime-foundation.md)
pass.

## Problem

The sandbox profile pins a local Docker image ID/config digest, but neither the
profile nor signed runtime qualification currently states which OS and CPU
platform that image is expected to run on. A digest prevents an image from
silently changing; it does not make the intended platform explicit to policy,
qualification, or operators. A runner must not infer the platform from its
host and thereby run a different qualified workload on another machine.

## Invariants

- OCI sandbox profiles carry a normalized, typed image platform: OS,
  architecture, and optional architecture variant. The current OCI backend
  accepts Linux platforms only. The currently admitted architecture values
  are `386`, `amd64`, `arm` with required `v5`/`v6`/`v7`, `arm64` with optional
  `v8`, `ppc64le`, `riscv64`, and `s390x`; adding another value requires
  explicit test coverage and qualification on that target.
- The platform is part of normalized profile identity, tool-definition
  identity, sandbox-runner request identity, and the sealed OCI plan hash.
- Signed sandbox qualification binds the exact image ID/config digest and the
  exact platform. Provider runtime identity must match both before admission.
- The Moby adapter inspects the exact local image ID and compares its reported
  OS/architecture/variant to the admitted platform before container creation.
  It fails closed if the image is missing, malformed, or mismatched. It never
  pulls, resolves a tag, or chooses a platform from the host implicitly.
- Local-process profiles remain unchanged and reject image identity/platform
  fields. No cross-workspace or caller-controlled image lookup is introduced.
- Adding a platform field changes canonical hashes. The sandbox qualification
  evidence schema and domain-separated stable-hash version must be bumped;
  old signed non-local qualification evidence is not silently upgraded or
  admitted without an explicit platform-bound requalification.

## Contract and persistence changes

Add a `SandboxImagePlatform` value with `OS`, `Architecture`, and `Variant`
fields plus strict normalization. Carry it through `SandboxProfile`, the
path-free `SandboxRunnerProfile`, `SandboxRuntimeIdentity`, and
`OCIContainerPlan`. Include each canonical component in stable hashes and
compare value equality in sandbox qualification verification.

The Engine boundary must expose a local-inspection-only interface; it has no
pull or tag-resolution method. A pure verifier compares the exact inspected
full image ID and normalized OS/architecture/variant to the sealed OCI plan.
Its stable error must not disclose image IDs, host details, or repository
paths. The Moby adapter must call this verifier before creating a container.

This slice changes serialized contracts and signed qualification payloads but
does not add a PostgreSQL table or migration. The zero-valued platform is
omitted from local-process profile JSON so existing local profile/tool hashes
remain stable. Existing local-process settings remain readable. Any persisted
OCI profile/qualification lacking platform identity fails closed and must be
re-issued through the versioned contract. The runner wire protocol is bumped
so an old runner cannot silently accept the new required field.

## Reuse, licensing, and cost

Use the platform tuple already defined by the OCI image/platform model and the
official Moby client types; do not copy implementation code from reference
repositories. The Moby client/API modules retain their upstream Apache-2.0
notices. No Kronaxis Fabric source is used. Runtime cost is one additional
comparison during deterministic profile admission and image inspection; no
additional service, image pull, or background job is introduced.

## Acceptance tests

- Platform normalization is deterministic and rejects empty OS/architecture,
  unsupported OS, invalid architecture/variant combinations, and noncanonical
  values.
- OCI profiles and runner profiles require the platform; local-process
  profiles reject it.
- Changing OS, architecture, or variant changes profile, request, plan, and
  signed qualification hashes.
- Zero-valued platform remains omitted from local profile JSON so existing
  local-profile serialization does not gain an empty object.
- Qualification evidence with a missing or mismatched platform fails closed;
  old evidence schema is rejected rather than silently reinterpreted.
- The pure local-image verifier accepts an exact inspected image ID/platform
  match and rejects missing image, config-ID mismatch, OS mismatch,
  architecture mismatch, and variant mismatch before container creation.
  Unit tests of this verifier are not evidence that the future Moby adapter
  calls it.
- The runner never invokes image-pull APIs, including on missing image or
  platform mismatch.
- Existing local-process tests and offline qualification remain unchanged.

## Operational limits

This contract is not evidence that Docker enforces CPU, memory, PID, network,
mount, user, or output controls. Unit tests prove only deterministic mapping
and fail-closed decisions. Linux Engine and macOS Docker Desktop require
separate live qualification. Manifest-level signature/provenance verification
is also out of scope: `ImageDigest` remains the local image ID/config digest,
not a registry manifest/index digest.
