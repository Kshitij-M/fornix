# Loop 101 completion — privacy-safe local support bundles

Status: implementation and focused offline qualification added. The Issue #40
production-readiness program remains open.

## Outcome

`fornix support bundle --output PATH` now writes a versioned, typed JSON
allowlist rather than serializing profile fields. The bundle reports coarse
profile state, safe configured/not-configured indicators, Fornix version, local
profile schema/migration versions, and a UTC timestamp. It contains no server
URL, workspace or actor identity, profile/repository path, credential
reference, runtime project/version, prompt, log, or raw diagnostic error.
Profile inspection is read-only: a missing root is not created and an insecure
root is not chmodded as a side effect of requesting diagnostics.

The support artifact is bounded to 4 KiB. It is created exclusively with mode
`0600`, explicitly chmodded after creation, synced, and closed. A destination
collision fails without truncating or changing the existing file. Failed
writes remove only the new partial file created by the command. Cancellation is
checked before profile work and immediately before file creation. The bundle
is local-only and is never uploaded.

## Reuse, storage, and cost

No database migration, network call, external dependency, or authoritative
history mutation was added. The implementation reuses profile validation but
does not mistake validation for a redaction boundary; the support DTO itself is
an explicit allowlist. The only persistent bytes are the operator-requested
local JSON file, capped at 4 KiB. There is no latency or database-work claim.

## Verification

Added offline tests cover private sentinel fields, missing/invalid profile
states, schema and redaction markers, the hard byte ceiling, file mode,
non-destructive collision behavior, oversized payload rejection, and
cancellation before output, plus proof that missing/insecure profile roots are
not created or permission-modified. Run them with:

```sh
make qualification-support-bundle
```

The production-readiness issue remains open: this change does not provide
alert delivery, log collection, support upload, or deployment diagnostics.
