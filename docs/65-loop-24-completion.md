# Loop 24 completion — Fornix Local release qualification

Status: implementation complete on the release-qualification branch; hosted
release publication and the optional `get.fornix.dev` alias remain release-owner
operations after merge.

Issue #32 closes a product-facing gap rather than adding another control-plane
subsystem. Before this work, the managed local CLI existed in the source tree,
but a new user still had to build from a checkout and the release pipeline did
not independently verify the package that users would install. This loop makes
the canonical install → start → prompt → inspect path reviewable and testable.

## Delivered

- Added the single-package release contract to GoReleaser for Linux and macOS
  amd64/arm64 archives.
- Added `LICENSE`, `README.md`, and `THIRD_PARTY_NOTICES.md` to the canonical
  archives. Auxiliary watcher/evaluation archives remain separate.
- Added archive SBOM generation and checksum-manifest attestation to the
  release path.
- Added `scripts/release/verify-artifacts.sh`, an independent verifier for
  checksums, archive completeness, flat safe paths, regular-file members,
  notices, likely credential material, and the four-platform matrix.
- Hardened `scripts/install.sh` with restrictive umask, absolute destination
  checks, version validation, checksum-first installation, unsafe path and
  special-file rejection, regular-binary validation, and atomic replacement.
- Added a local clean-room package smoke that serves a temporary release over
  HTTP, installs into a private prefix, checks version/help/doctor/completion,
  rejects checksum mutation, rejects a symlink binary, and proves a failed
  verification does not overwrite an existing executable.
- Added `make smoke-package` and `make release-check`, plus a CI package job
  that verifies GoReleaser snapshot output.
- Added shell completions for Bash, Zsh, and Fish, a bounded `fornix runs`
  operator view, provider inspection/test commands, `support bundle` syntax,
  and dry-run/versioned upgrade validation.
- Added a workspace-scoped, transcript-free agent-run list endpoint so the
  CLI's `runs` command does not disclose goals, history, pending arguments, or
  output accidentally.
- Updated the user README, release guide, local operations guide, production
  qualification summary, changelog, and documentation map.

## Qualification evidence

The following checks passed on the development host (macOS arm64, Docker
Desktop's Linux engine, Go 1.25.13, PostgreSQL/pgvector pg17 image):

```text
go test ./... -count=1                         PASS
go test -race ./... -count=1                   PASS
make check                                     PASS
make smoke-package                             PASS (3.89 seconds)
make smoke-local-runtime                       PASS (18.95 seconds, cached image)
make release-check                             PASS (4 canonical archives)
```

GoReleaser v2 snapshot output also passed the verifier for:

```text
fornix_*_linux_amd64.tar.gz
fornix_*_linux_arm64.tar.gz
fornix_*_darwin_amd64.tar.gz
fornix_*_darwin_arm64.tar.gz
```

The snapshot build produced archive SBOMs and completed in approximately
74 seconds in the local Docker-based check. The package smoke's temporary
host-native archive measured approximately 4.9 MB on macOS arm64. The
cross-platform snapshot archives measured approximately 4.7–5.3 MB for the
canonical CLI and 15 KB per CLI archive SBOM. The managed local application
image measured approximately 111 MB and the pgvector/pg17 image approximately
464 MB in the local Docker cache. These are observed development values, not
capacity or download-time guarantees.

The managed-runtime smoke reached readiness, ran the deterministic reference
workflow, verified replay, verified duplicate-run idempotency, and confirmed
two-service status. It used the fake provider and no OpenAI key. Remote
provider behavior remains opt-in and at-least-once; it is not required for
package qualification.

## Security and licensing decisions

The canonical package contains the native CLI and reviewed notices. It does
not contain provider keys, database passwords, repository contents, prompts,
or a Docker engine. Docker Desktop on macOS or Docker Engine plus Compose v2
on Linux remains the one privileged host prerequisite. The installer has no
privileged fallback and does not silently install Docker.

The local release path verifies SHA-256 bytes before extraction. GitHub
Artifact Attestations provide the separate publisher-provenance layer for the
checksum manifest. The installer does not require GitHub CLI or Sigstore
tools. Fornix remains MIT-licensed. Reference repositories informed the
architecture; no reference source was copied, and Kronaxis Fabric's BSL 1.1
source was not reused.

## Remaining qualification boundaries

- No tagged public GitHub release existed at implementation time. The release
  workflow and verifier are ready; the release owner must merge, tag, inspect
  the generated assets, verify the attestation, and run the published-asset
  smoke before advertising the curl command.
- `get.fornix.dev` still needs DNS/hosting configuration. The raw GitHub
  installer URL is the reviewable fallback until that alias is verified.
- The native package does not include Homebrew, Debian, or RPM adapters.
- Docker remains required, and the managed runtime is single-node without
  automatic backup/restore or high availability.
- Native execution qualification is complete here only for macOS arm64.
  Linux amd64 is structurally verified in CI; macOS amd64 and Linux arm64
  require native release-owner matrix runs.
- The package makes the current alpha easy to evaluate; it does not claim the
  remaining autonomous patch-synthesis, general sandbox, HA, SSO/KMS, or
  object-storage product gaps are solved.
