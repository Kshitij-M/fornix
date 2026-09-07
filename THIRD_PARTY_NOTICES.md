# Fornix third-party notices

Fornix is distributed under the MIT License. This notice describes the
runtime and build dependencies that may be included in a source checkout,
release archive, or managed local runtime.

## Go dependencies

Fornix uses the modules listed in `go.mod` and `go.sum`. Their individual
licenses and copyright notices remain with their respective authors. A
release is built from the exact, reviewed module versions committed to the
repository. Consumers who redistribute a modified build must preserve the
applicable upstream notices.

## Managed runtime components

`fornix start` can manage a local PostgreSQL + pgvector container and a
Fornix application container. These images are pinned by the generated
runtime manifest. PostgreSQL, pgvector, Docker, and the Debian base image
are separate projects with their own licenses and terms; the image metadata
and project documentation are the authoritative source for each release.

The managed runtime is an optional convenience. Fornix does not bundle or
silently install Docker, PostgreSQL, or any host service.

## Release verification

Release archives include this notice, `LICENSE`, and `README.md`. The release
workflow publishes SHA-256 checksums and an artifact attestation for the
checksum manifest. Maintainers can verify a downloaded release with:

```text
gh attestation verify checksums.txt --repo Kshitij-M/fornix
```

The repository's release verifier also rejects unsafe archive paths,
non-regular archive members, missing notices, and likely credential material
before publication.
