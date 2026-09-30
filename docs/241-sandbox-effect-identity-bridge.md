# Sandbox effect-identity bridge feature note

Status: implementation slice for Issue #28; not a container runtime or a
production isolation qualification.

## Problem and decision

The current sandbox registry selects a backend by declared capabilities, but
the provider execution method receives only a tool request and definition. A
future OCI, gVisor, or microVM provider would therefore not receive the exact
durable attempt and effect reservation that authorized the call. That gap can
lead to a runtime object being created under an identity the control plane
cannot safely reconcile after a crash or takeover.

This slice binds non-local sandbox invocation to the existing generic effect
authority. The effect dispatcher already reserves an operation attempt and
effect in Postgres and validates the current operation/task fences before it
calls the adapter. The typed sandbox identity will carry the workspace, tool
run, operation owner/fence, attempt/effect identifiers, request and
reservation hashes, task fence when applicable, agent-run owner/fence when
applicable, and definition/profile hashes.
Runtime names can be derived from this opaque identity without exposing host
paths, argv, prompts, credentials, or environment values.

## Invariants

1. A non-local execution identity is complete, normalized, workspace-scoped,
   and hash-bound to one tool run, operation attempt, reserved effect, tool
   definition, and effective sandbox profile.
2. A task-bound identity includes the same non-zero task owner/fence carried by
   the dispatcher authority. An agent-run-bound attempt also includes the
   tool ledger's agent-run owner and fence. Mismatched workspace, request hash,
   task fence, or missing dispatch reservation fails before provider
   invocation.
3. Non-local providers implement both attempt-aware execution and attempt
   reconciliation. A plain `ProcessExecutor` or a capability claim alone is
   insufficient. The selected backend never falls back to another provider.
4. The identity contains no raw command arguments, host paths, environment
   values, credentials, or output. Runtime labels/names derived from it are
   stable but do not grant authority; live authorization remains in Fornix.
5. Reconciliation must distinguish a known result/terminal state from an
   unknown external outcome. Unknown state is recovery-required; it must never
   be converted into a fresh execution under a new identity.
6. This interface does not itself terminate an already-running stale process,
   attest kernel isolation, or prove runtime cleanup. Those remain runtime and
   deployment qualification requirements.

## Schema and authority reuse

No migration is added in this slice. Migration 036's `operation_effects` and
append-only effect transitions remain the authoritative reservation and
dispatch journal; the dispatcher passes the exact reservation authority to
the provider callback. Tool-run lifecycle remains the specialized source of
tool metadata and output. This avoids a second effect ledger.

A future runtime may be reconciled by deterministic labels derived from the
effect identity, but only if the backend can prove uniqueness, queryability,
and result recovery across a Fornix crash. If those guarantees require storing
an opaque runtime ID, durable lifecycle state, or reconciliation cursor, add a
new monotonic migration then; do not infer that such persistence already
exists. An OCI provider is still unavailable in this slice.

## Reuse, licensing, and cost

The design reuses Fornix's effect dispatcher, operation/task fences, tool-run
identity, and sandbox registry. Local source study included Dagger's typed
container backend seam and OpenSandbox's explicit execution/reconciliation
lifecycle. Both reference repositories declare Apache-2.0; no source is copied
and no dependency is added, so Fornix's MIT license and attribution files do
not change.

The new identity is a small in-memory/serialized contract; it adds no database
rows or external services. Stable identity hashing is bounded CPU work. The
cost of the future OCI/gVisor/VM backend, image storage, runner process, and
runtime reconciliation is not measured or claimed here.

## Acceptance tests

- Identity normalization rejects missing, malformed, cross-workspace, stale
  fence, absent reservation, and invalid hash facts.
- Equivalent authority facts produce the same opaque runtime identity;
  changing the workspace, effect, fence, request, definition, or profile
  changes it.
- The production effect adapter passes the dispatcher's exact authority into
  the tool executor/provider boundary.
- A non-local provider lacking attempt-aware invocation or reconciliation is
  rejected at registration, even if it advertises all sandbox controls.
- A valid test provider receives the identity and is selected only for its
  exact backend; no-local-fallback behavior remains intact.
- Default local execution and its current capability declaration remain
  unchanged.
- Unit, race, formatting, docs, and focused integration checks pass; no claim
  is made that a runtime-backed sandbox was exercised.
