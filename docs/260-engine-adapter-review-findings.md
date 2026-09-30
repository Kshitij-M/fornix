# Engine adapter review findings

Status: implementation review. The official Moby SDK remains the selected
Engine client as specified in [`254-moby-oci-runtime-foundation.md`](254-moby-oci-runtime-foundation.md).
This note records contract gaps found while preparing that implementation;
it is not evidence of a working OCI runtime.

## Confirmed identity defect

`SandboxExecutionIdentity.RuntimeName` and `BuildOCIContainerPlan` previously
derived different names from the same identity (`fornix-<40 hash characters>`
versus `fornix-sbx-<32 hash characters>`). That would make the execution plan,
restart reconciliation, and durable cleanup disagree about which Engine
object represents an attempt. The execution identity now owns the canonical
name derivation, and plan construction and validation consume that function.
The fix is covered by a cross-package regression test.

## Gaps to close before enabling OCI

- The existing Moby implementation note remains authoritative. Do not replace
  the selected SDK with Docker CLI calls or a hand-written Engine API client.
  The module is not available in this restricted environment, so dependency
  resolution and compilation still need a normal network-enabled CI run.
- `SandboxProfile.ImageDigest` is explicitly defined as the local image
  ID/config digest (`ID`), not a registry manifest/index digest
  (`RepoDigests`/descriptor). Loop 117 adds typed OS/architecture/variant to
  the profile and signed `SandboxRuntimeIdentity`, bumps qualification and
  runner protocol versions, and checks the platform at admission. Loop 118
  adds an inspection-only interface and pure plan verifier for the exact local
  ID/platform. The Moby adapter must still implement that interface, call the
  verifier before create, and never pull; no adapter currently exists.
- Reconciliation receives only the durable `SandboxExecutionIdentity`, not the
  original runner request. Runtime labels and result recovery must distinguish
  execution identity, canonical tool-request hash, and transport request hash;
  it may not claim to revalidate a hash it cannot reconstruct.
- A static IPC credential does not prove that task/operation fences remain
  current. The control plane must validate authority before dispatch or any
  continuation that could start a not-yet-started container.
- Output quotas and crash recovery must be designed together. If an Engine
  logging/capture path cannot prove complete bounded output after restart, the
  result remains unknown and the exact container is retained; it must not be
  reported as success or executed again.
- Inspect results are useful conformance evidence but do not prove kernel
  enforcement. Network isolation, mount behavior, resource enforcement, and
  process-tree termination require supported-host qualification.

## Required regression evidence

- An identity's `RuntimeName` is the one and only container name used by plan
  construction, reconciliation, and cleanup.
- The plan validator rejects a name inconsistent with its execution hash.
- Identical identity yields the same name; changing a fence changes the name.
- OCI remains unavailable until SDK-backed lifecycle, crash, output, cleanup,
  Postgres authority, and live Linux/Docker Desktop qualification gates pass.

The review also checked Orloj's container execution implementation and
Dagger's container-driver abstractions. Those references inform explicit
backend selection and fake-driver testing, but neither is copied. Orloj's
Docker CLI lifecycle is not the authority-aware, crash-reconcilable Moby
boundary Fornix requires. No Kronaxis Fabric source is used.
