# Local support-bundle redaction foundation

Status: implemented locally; focused offline tests are recorded in
[`228-loop-101-completion.md`](228-loop-101-completion.md).

## Problem

`fornix support bundle` currently labels its output redacted but includes the
absolute profile root and configured server URL. A URL path can contain
tenant-specific or private routing data even though the profile validator
rejects userinfo and query parameters. The bundle also uses `os.WriteFile`
with mode `0600`; that mode does not change permissions when the destination
already exists, so a previously shared file can remain broadly readable.

## Invariants

- A support bundle contains only an explicit, versioned allowlist of bounded
  diagnostic facts. It never serializes the profile document or arbitrary
  profile/environment values.
- Absolute profile/repository paths, workspace/actor identifiers, credential
  references or bytes, server hosts/paths, runtime project names, data-volume
  names, prompts, and logs are excluded.
- The bundle records only whether a profile/server configuration exists,
  Fornix version, the local profile schema/migration numbers, a generated-at
  timestamp, and an explicit redacted marker. It does not report runtime
  version, endpoint host/path, or profile identity.
- Gathering profile state is read-only. A missing profile root remains absent,
  and an insecure existing root is reported coarsely without chmodding it.
- Bundle output is capped at 4 KiB, created with owner-only permissions, and
  never follows or truncates an existing destination. A collision fails
  without changing the existing file.
- Bundles are written locally and are never uploaded automatically. Output
  errors do not include profile contents.
- This local operator artifact is not an authoritative event, database row,
  artifact, or receipt and does not change control-plane history.

## API and schema

No database migration or public API is needed. Replace the untyped support
map with a typed local DTO and a small pure builder. The JSON envelope has a
schema version so future operators can evolve it deliberately; unknown profile
load failures are represented by coarse status only, never by raw error text.

## File safety and failure behavior

Open the destination with exclusive creation and mode `0600`, explicitly
enforce the mode on the opened file, write the bounded bytes, sync, and close.
If serialization or writing fails after creation, remove only the file created
by this invocation. If the destination already exists, fail closed and ask the
operator to select a new path; do not overwrite a possibly shared support
artifact.

## Research, reuse, and licensing

Issue #40 requires bounded diagnostics and privacy-safe support bundles.
Fornix's existing profile package supplies validated local metadata, but its
validation is not a redaction policy. The agentmemory doctor catalog was
reviewed for stable diagnostic identifiers and explicit check/fix boundaries;
it does not define a directly reusable safe support-bundle format. Reuse the
design principle of an explicit allowlist, not source code. No third-party
code or dependency is added, so licensing and SBOM impact are unchanged.

## Cost and storage

There is no network or database work and no persistent application storage.
The locally generated JSON is at most 4 KiB plus filesystem allocation. The
allowlist and file-mode checks add negligible CPU work; no performance claim
requires benchmarking.

## Acceptance tests

- Secret-like profile fields, private URLs, workspace/actor IDs, mount paths,
  profile paths, project names, and credential references do not appear in
  serialized support output.
- The result declares its schema version and redacted state, and is below the
  hard byte limit.
- New output files have exactly owner-only permissions on permission-aware
  platforms.
- An existing file, including one with permissive mode, is neither truncated
  nor chmodded; the command returns an error.
- Missing/corrupt profile data produces only a coarse redacted status.
- Missing and insecure profile roots are not created or permission-modified.
- Context cancellation and write failures do not leave partial files.
- Existing CLI, profile, formatting, documentation, and smoke checks remain
  green; no database or external service is contacted.
