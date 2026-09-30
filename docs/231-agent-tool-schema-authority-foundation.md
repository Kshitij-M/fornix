# Agent tool-schema authority foundation

Status: implemented locally; qualification and remaining limitations are
recorded in [`232-loop-103-completion.md`](232-loop-103-completion.md).

## Problem and scope

Task 102 made the durable per-run tool catalog an execution allowlist, but the
requester still supplies the function description and JSON Schema sent to the
model. That lets an untrusted caller misrepresent a registered capability or
advertise arguments the executor does not accept. The model-facing catalog
must be a projection of the registered tool definition and the actual bounded
argument contract.

This slice does not add generic JSON Schema execution, a new tool language,
policy authority, or a new registry service. The current agent-loop adapter
executes a structured `argv` envelope. Its model schema will describe that
envelope and the bounded optional environment/working-directory fields the
registered executor already supports.

## Invariants

- A request selects registered tool names only. Registered metadata—not
  caller-supplied text or schema—defines the description and parameters shown
  to the provider.
- A selected tool must pass the executor's workspace/actor/task policy
  admission before the run is reserved or any model request is sent. Execution
  re-evaluates the actual arguments and current policy before every effect.
- The registry preserves path-argument restrictions across registration and
  lookup copies. Model schema guidance and read-only executor checks consume
  the same registered path indexes.
- If a caller includes a description, parameter schema, or authority hash, it
  must match the registered projection; mismatches fail before durable run
  reservation and before model egress.
- The normalized registered definition and derived model schema are bound into
  the persisted per-run catalog. Every dispatch recomputes that binding and
  fails closed if current registered metadata no longer matches the durable
  run snapshot.
- A schema is guidance to a model, not authorization or input validation.
  Returned arguments remain untrusted and pass through strict structured
  decoding, tool request normalization, policy, approval, budgets, and the
  existing task/agent-run fencing and durable effect boundaries.
- Registry metadata cannot grant a capability. Workspace authorization,
  enabled state, tool registration, capability policy, approvals, and fencing
  remain independent requirements.
- Tool names and the model-visible catalog are deterministic and immutable for
  a durable run. Tool descriptions never become system/developer instructions.
- The model supplies only dynamic argv values. Fornix inserts the registered
  executable and fixed argv prefix after strict argument decoding, so hidden
  paths and trusted command structure do not need to be exposed to the model.
- Provider-specific function-name restrictions are handled by a reversible,
  deterministic adapter mapping; durable Fornix history retains the canonical
  internal tool name.
- Ollama and OpenAI-compatible chat adapters both preserve tool definitions,
  assistant tool-call history, and tool results; a provider must not silently
  discard a requested tool catalog.
- No raw credentials, executable paths, working-directory roots, or policy
  records are added to provider-visible descriptions or schemas. The internal
  fingerprint is a digest of the normalized registration/configuration and
  is not serialized to provider wire formats.
- Replay/state hashes bind the immutable request/catalog identity as well as
  the mutable loop state and history.

## Schema and persistence decisions

No SQL migration is expected. `AgentRun.Tools` already stores the durable
provider-facing catalog and is covered by the request hash and immutable-store
check. Add an internal definition fingerprint to each model tool contract so
the persisted catalog binds to the normalized registered definition. Provider
adapters must continue projecting only provider-standard name, description,
and parameters; the fingerprint is internal and must never appear on the wire.

The derived JSON Schema must describe only the supported structured input:
required dynamic `argv`, read-only path argument guidance, optional environment
keys limited to the registered allowlist, and optional `workdir` only when the
registration has a workdir root. Fornix prepends the registered executable and
fixed argv prefix at dispatch. Item counts and conservative character lengths
come from normalized sandbox limits. JSON Schema length is character-based
while execution limits are byte-based, so strict decoding and executor byte
checks remain authoritative if a provider returns malformed or
schema-violating arguments. An empty dynamic argv list is valid when a tool is
fully described by its registered executable and fixed prefix; those values
still pass through the normal request limits and execution policy.

For compatibility, callers may continue to submit `ModelToolDefinition` values
to select tools. Omitted display metadata is filled from the registry. Supplied
metadata is accepted only when equivalent to the canonical registered
projection; otherwise create returns a stable validation error. Existing
durable runs retain their catalog and will fail closed at dispatch if the
current registry no longer has the matching definition fingerprint.

## Recovery and failure behavior

Unregistered or disabled tools, conflicting requester metadata, malformed
registered definitions, and stale definition fingerprints fail before tool
execution. Create-time failures occur before the run is reserved or the model
is called. A mismatch discovered while resuming an already durable run commits
the normal deterministic tool-failure transition and performs no external
effect. There is no fallback to a caller-provided schema.

## Reference reuse and licensing

DeepSeek Harness compiles JSON Schema from registered typed tool definitions
(`reference_repos/deepseek-harness/packages/core/tools/src/schema.ts`). Orloj
builds a governed runtime from configured tool names and resolves model-facing
descriptions/input schemas through its capability registry
(`reference_repos/orloj/runtime/tool_runtime_governed.go` and
`agent_worker.go`). Fornix reuses these architectural seams, not source code.
The inspected DeepSeek Harness repository is MIT and Orloj is Apache-2.0; this
slice copies no code and adds no dependency. Kronaxis BSL source is not used.

## Cost and storage impact

The fingerprint and schema are bounded by existing tool/run catalog limits and
are persisted inside the existing JSON request snapshot; no table, index, or
service is added. Create and dispatch each perform bounded in-memory work over
the existing maximum catalog size. Invalid requests fail before model cost is
incurred. The schema adds a small, deterministic number of model input bytes;
existing context/token budgets continue to account for the complete tool
catalog.

## Acceptance tests

- Caller-supplied descriptions and schemas cannot spoof the registered tool;
  conflicting values fail before reservation/model calls.
- An omitted description/schema is enriched from the registered capability
  and current executor argument contract.
- Derived schemas are deterministic, bounded, describe dynamic arguments and
  registered path positions, and reflect allowed environment and
  working-directory fields without exposing the executable, fixed argv prefix,
  or private roots.
- A persisted run's catalog includes a stable definition fingerprint; changing
  registered metadata after reservation rejects dispatch before execution.
- Workspace/actor/task policy denial rejects a requested tool before durable
  run reservation and model egress; current policy is checked again at tool
  execution.
- Provider wire requests contain the canonical schema and description but do
  not contain internal definition fingerprints, executable paths, or roots.
- OpenAI-compatible names satisfy the provider's documented restricted
  function-name grammar, and responses map deterministically back to Fornix
  names. Ollama native chat carries tools and multi-turn tool history rather
  than silently dropping either; unsupported streaming tool flow fails before
  provider egress.
- Replay state hashes change when the immutable request or catalog changes.
- Malformed model arguments still fail closed at the executor boundary.
- Declared registered tools continue through approval, authorization, fencing,
  duplicate-delivery, replay, and workspace-isolation behavior unchanged.
- No prompt, credential, arbitrary request text, or private filesystem path is
  added to telemetry or events by catalog binding.
- Existing tests, race checks, docs checks, builds, and local smokes remain
  green. PostgreSQL persistence tests must run against a disposable database;
  local tests without it remain explicitly unqualified.

## Known limitation

Registry descriptions are still text presented to a model and can themselves
be misleading or adversarial if a trusted administrator registers them that
way. They do not become policy, and independent authorization/fencing must
continue to constrain effects. The fingerprint binds registered metadata and
configuration, not the bytes of an executable at a mutable filesystem path;
artifact/package integrity is a separate production gate.
