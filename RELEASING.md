# Releasing Fornix

Fornix releases are tag-driven. The `Release` workflow builds release
artifacts with GoReleaser, creates the GitHub Release, and attests the checksum
file. The `Publish containers` workflow publishes multi-architecture `fornix`
and `fornix-watcher` images to GitHub Container Registry.

## Before tagging

1. Confirm `main` is green, including the Postgres/HTTP smoke job.
2. Update [`CHANGELOG.md`](CHANGELOG.md) and move verified items from
   `Unreleased` into a dated release section.
3. Confirm the version follows semantic versioning. Use a prerelease suffix
   such as `v0.11.0-alpha.1` while the project remains alpha.
4. Review the generated release scope and confirm no credentials, private
   data, or model transcripts are present in artifacts or notes.

## Create a release

From a clean checkout of `main`:

```sh
git fetch --tags origin
git tag -a v0.11.0-alpha.1 -m "Release v0.11.0-alpha.1"
git push origin v0.11.0-alpha.1
```

The tag starts both release workflows. GoReleaser cross-compiles the service,
watcher, and offline evaluation CLI for Linux, macOS, and Windows on amd64 and
arm64. The release binaries report the Git tag as their version; local builds
continue to use the default in `internal/version/version.go`.

## After the release

- Check the GitHub Release assets, checksum attestation, and generated notes.
- Check both GHCR packages and their visibility.
- Smoke-test the published binary and image using a fake provider and a local
  Postgres instance before recommending the release to users.
- Announce known limitations and upgrade notes in the release body or linked
  documentation.

Never publish a release from a dirty working tree or with provider credentials
in the environment unless the command explicitly requires them. The release
workflows do not need `FORNIX_OPENAI_API_KEY` or any other Fornix secret.
