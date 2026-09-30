# Task 75 — Portable qualification runner and evidence bundle

Status: implemented repository-owned qualification slice; not a production-
readiness declaration.

## Problem

Fornix already has bounded connector reports, recovery-evidence contracts, and
an offline qualification validate command. Those pieces are useful but
fragmented: CI and operators run independent checks and receive logs rather
than one deterministic, redacted qualification artifact that can be compared,
validated, and imported without contacting the deployment.

The portable runner closes that evidence-composition gap. It does not turn
offline checks into proof of production HA, provider behavior, secret-manager
integration, sandbox strength, or load capacity. Those remain deployment-owned
gates.

## Invariants

1. Check registration is explicit, bounded, unique, and sorted by stable check
   name before execution. A duplicate name fails closed.
2. A check has a bounded name, version, category, input hash, and function. It
   returns a redacted QualificationCase; raw prompts, SQL, DSNs, credentials,
   provider payloads, and arbitrary user text are not representable in the
   manifest.
3. The runner performs no retries and no implicit network, model, tool, broker,
   or database work. Deployment checks are imported as already-produced,
   validated evidence.
4. Cancellation and deadline expiry produce deterministic blocked or skipped
   cases, never a false pass. The runner stops starting new checks after the
   bounded deadline.
5. Report identity excludes wall-clock timestamps and measured duration.
   Manifest identity includes stable check metadata, input hashes, report hash,
   commit metadata, and environment variable names only.
6. The report and manifest have independent validation and stable hashes.
   Merging evidence requires one target hash and one workspace scope; conflicts
   fail closed.
7. Report bytes, check count, and execution duration are hard limits. The
   runner never truncates a valid report into an unverifiable artifact.

## Reuse and licensing

The implementation reuses QualificationReport, Builder, connector conformance
reports, readQualificationReport, and the existing bounded CLI patterns. It is
informed by ClawMem replay/gold evidence, agentmemory diagnostic/evaluation
envelopes, FornixDB budgeted reports, and Orloj telemetry/evaluation lifecycle
patterns. No reference source is copied. Kronaxis BSL 1.1 code remains
excluded; Fornix remains MIT-licensed.

## Schema and storage

No database migration is required. A qualification bundle is a bounded local
JSON evidence file, not a second authority. CI writes it to ephemeral storage
and validates it before publishing its hash. Deployment systems own raw
evidence and secrets; Fornix stores only hashes and bounded metadata when a
caller later chooses the existing artifact path.

## Cost and failure budget

Offline checks add no network or provider cost. The runner uses one process,
one bounded check invocation per registered check, and no retries. The default
profile caps checks at 64, runtime at five minutes, and report output at
128 KiB. Timing measurements are diagnostics and do not cause hash drift.

## Acceptance tests

- Different registration order produces the same report and manifest hashes.
- Timing changes do not change stable hashes.
- Duplicate check names, cross-workspace evidence, target mismatches, and
  malformed manifests fail closed.
- Check count, duration, and report-byte budgets are enforced.
- Cancellation produces a deterministic incomplete outcome.
- Offline runner execution performs no external I/O.
- Raw DSNs, tokens, prompts, SQL, payloads, and provider error text never enter
  a report or manifest.
- Imported deployment evidence is validated without contacting the deployment.
- Dispatcher-backed fake checks prove reconciliation, uncertain recovery,
  stale-fence rejection, duplicate suppression, and receipt/link hash binding.
- Existing tests, race checks, package checks, smoke checks, and documentation
  checks remain green.

## Deliberate limitation

The portable runner can validate deployment evidence but cannot generate genuine
proof of a deployment-owned property without running in that deployment. The
output therefore distinguishes local passed, skipped, and blocked cases and
keeps live qualification opt-in.
