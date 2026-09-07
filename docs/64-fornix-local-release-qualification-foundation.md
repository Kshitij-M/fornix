# Fornix Local release and clean-room qualification

Status: implementation note for the Issue #32 release-completion slice.

This note closes the gap between having a local-runtime implementation in the
repository and having a package that a new user can install and qualify
without a source checkout. The canonical user outcome remains:

```text
install → start → prompt → inspect a verified result
```

The release slice owns distribution evidence and clean-room qualification. It
does not silently broaden the product boundary: Docker Desktop on macOS or
Docker Engine plus Compose on Linux remains the one privileged host
prerequisite, PostgreSQL remains the control-plane authority, and the default
provider remains deterministic and offline.

## Invariants

- The primary artifact is one native `fornix` executable per supported
  macOS/Linux architecture. Auxiliary watcher and evaluation binaries are
  distributed separately as advanced tools and are not required by the local
  workflow.
- A release is identified by one concrete semantic version. The same version
  appears in the executable, archive name, application image tag, runtime
  manifest, and release notes.
- The installer downloads a specific archive and its checksum manifest before
  extraction, verifies the digest, rejects unsafe archive members and special
  files, and replaces an existing executable atomically.
- Runtime manifests contain image references, topology, health checks, and
  environment references only. They never contain API keys, database
  passwords, prompts, repository contents, or arbitrary host environment
  values.
- Package verification is bounded and deterministic. It checks the expected
  platform matrix, regular-file archive members, checksum coverage, version
  metadata, license notices, and credential absence.
- A release attestation covers the checksum manifest. The installer does not
  require GitHub CLI, Sigstore tooling, or network services beyond downloading
  the selected release; operators can independently verify the attestation
  before installation with the documented command.
- The managed runtime uses loopback-only application access, no host database
  port, named persistent data, explicit read-only repository mounts, and a
  non-root application container.
- A release smoke never uses an external model or tool. The fake provider is
  the default qualification path. OpenAI remains an explicit environment-only
  optional smoke and its key is never printed or persisted.
- Package and runtime qualification are evidence, not a universal performance
  promise. Measurements state host OS/architecture, Docker/Compose versions,
  image versions, repository fixture, provider, and whether the result is a
  design target or an observed local value.

## Release boundary

The release workflow builds the four canonical archives:

```text
fornix_<version>_linux_amd64.tar.gz
fornix_<version>_linux_arm64.tar.gz
fornix_<version>_darwin_amd64.tar.gz
fornix_<version>_darwin_arm64.tar.gz
```

The auxiliary `fornix_tools_<version>_<os>_<arch>.tar.gz` archives contain
watcher/evaluation binaries for advanced operators. The installer downloads
only the canonical archive for the current host.

GoReleaser produces deterministic, trimmed, CGO-free binaries, checksums,
archive SBOMs, and a reviewed `THIRD_PARTY_NOTICES.md`. GitHub Artifact
Attestations bind the checksum file to the release workflow. The release
workflow also runs the repository verifier before publishing and the
container workflow publishes matching multi-platform application images.

The hosted convenience URL is a distribution alias, not a second installer
implementation. Until DNS/hosting for `get.fornix.dev` is configured, the
reviewable GitHub raw URL is the supported fallback:

```sh
curl -fsSL https://raw.githubusercontent.com/Kshitij-M/fornix/main/scripts/install.sh | sh
```

The script itself defaults to the public GitHub release channel and supports a
concrete `FORNIX_VERSION`. Local tests override only the release base URL; they
never weaken checksum or archive validation.

## Installer security

The installer:

1. supports only macOS/Linux amd64/arm64;
2. accepts a pinned release version; local URL overrides exist only for the
   package smoke and are never part of the normal install instructions;
3. downloads into a private temporary directory;
4. verifies a checksum entry for the exact archive;
5. rejects absolute, parent-traversal, nested, symlink, hard-link, device,
   FIFO, and other special archive members;
6. rejects a missing or non-regular `fornix` member;
7. validates the destination as an absolute non-root directory;
8. writes a mode `0755` staged file and atomically renames it into place;
9. leaves an existing binary in place if any earlier step fails; and
10. never accepts a provider credential as an argument or writes one to disk.

The installer verifies integrity with SHA-256. The release attestation is an
independent provenance layer for operators who want to verify the checksum
subject with GitHub CLI. The minimal installer path does not pretend that a
local checksum alone proves who published a release.

## Clean-room tests

The package smoke creates a temporary release fixture, serves it over a local
HTTP server, installs it into a fresh private prefix, and verifies:

- version and JSON schema output;
- help and first-run doctor output;
- checksum mutation rejection;
- unsafe archive member rejection;
- destination/path safety;
- no-secret package metadata;
- no overwrite after a failed verification; and
- host-native archive execution without a source checkout at invocation time.

The CI package job additionally runs GoReleaser snapshot output through the
archive verifier and requires all four canonical platform archives. A Linux
amd64 archive is executed in CI; the other three are cross-compiled and
verified structurally. Native macOS amd64/arm64 and Linux arm64 execution are
release qualification matrix entries, not claims inferred from a cross-build.

## Runtime and workflow tests

The existing managed-runtime smoke remains the authoritative end-to-end local
test. It uses an isolated profile and Compose project, starts the pinned
PostgreSQL/pgvector and Fornix services, runs the deterministic reference
workflow, checks replay and duplicate-request hashes, checks service status,
and purges only its own disposable data. The release qualification record
must capture cold start, warm start, readiness, workflow, restart, and stop
timings plus binary/image/profile/database sizes.

The OpenAI smoke is optional and may run only when a CI/operator environment
explicitly supplies `FORNIX_OPENAI_API_KEY`. It must use hard token, time, and
cost budgets and report at-least-once remote execution. No test, log, artifact,
or release asset may contain the key.

## Reuse and licensing

The implementation reuses the repository's existing runtime manager, embedded
Compose manifest, profile/credential store, fake provider, Work Receipt,
artifact, and smoke contracts. Packaging patterns were compared with
GoReleaser-based Go projects and the reference projects' bounded installers,
but no reference source is copied. Kronaxis Fabric remains BSL 1.1 and is not
used as source material.

Fornix remains MIT-licensed. Release archives include the Fornix license and
reviewed third-party notices. PostgreSQL/pgvector, Docker, Debian base-image,
Go, and Go module terms apply to their respective runtime or build
components; they are not silently relicensed as Fornix code.

## Cost and operational budget

- Installer verification reads one checksum manifest and one host archive and
  uses bounded temporary disk proportional to the archive size.
- Snapshot package CI performs one cross-platform build and one structural
  verification pass; it does not pull model weights or call providers.
- Release runtime startup pulls the application and database images only when
  absent or explicitly upgraded. Database volumes are reused across warm
  starts.
- Package metrics record archive bytes, installed binary bytes, image bytes,
  profile growth, cold/warm start, readiness, workflow, restart, and cleanup
  time. Missing hosted-release values remain explicitly unqualified.

## Acceptance tests

- Issue #32's canonical macOS/Linux amd64/arm64 archive matrix is built and
  structurally verified.
- Checksums, SBOMs, notices, and the checksum attestation are generated by the
  release workflow.
- The installer rejects checksum changes, unsafe members, special files,
  unsupported platforms, and unsafe destinations without replacing a good
  binary.
- A clean package smoke installs the native artifact without a source
  checkout and verifies stable version/help/doctor output.
- A fresh managed runtime reaches readiness with a pinned application image,
  pinned database image, named volume, and loopback-only host access.
- The fake-provider demo and repository run are deterministic, idempotent,
  replayable, bounded, and artifact/evidence/receipt-backed.
- Restart and normal stop preserve authoritative database data; explicit purge
  removes only the exact managed profile/project after confirmation.
- No release, archive, generated manifest, test output, or support bundle
  contains credentials, private source paths, or raw prompts.
- Documentation names the hosted installer state, Docker prerequisite,
  supported matrix, observed measurements, backup/upgrade limitations, and
  unqualified platforms.

## Known external prerequisites

Code can provide the release workflow and a reviewable installer, but it cannot
create DNS records, a hosted `get.fornix.dev` endpoint, GitHub release signing
keys, or a user workstation's Docker installation. Those are release-owner
operations. The repository must not claim the public curl path is live until
the first release is published and the endpoint is pointed at the checked-in
installer.
