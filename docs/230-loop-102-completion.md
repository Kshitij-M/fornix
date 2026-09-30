# Loop 102 completion — agent-loop trust boundaries

Status: implementation complete for the offline agent-loop slice. Durable
catalog persistence and mutation rejection remain unqualified locally because
no disposable PostgreSQL endpoint is available.

## Outcome

Retrieved context is now appended to model history as a `user`-role message
with explicit untrusted-evidence framing. The stable rendered pack still
contains its content hash, source references, and evidence hashes. This keeps
retrieved content out of privileged system instructions; it does not claim to
solve prompt injection.

An `AgentRunRequest` rejects duplicate case-insensitive model function names.
Run creation resolves every declared function name against the registered tool
catalog before it reserves a run or calls a model. At dispatch, every
model-returned tool name must exactly match the persisted run catalog before
the registry or executor is consulted. A registered-but-undeclared, unknown,
or mismatched tool fails closed. Approval/resume uses the same dispatch guard.
The production Postgres commit transaction compares the proposed tool catalog
and request hash to the locked run, rejecting any change to immutable run input.

Declaring a model-visible function is not authorization. The tool registry,
workspace policy, capability admission, approval decision, task/agent-run
fences, budget limits, durable tool-run identity, and external-effect recovery
remain independently enforced.

## Schema, storage, and cost

No migration, public contract field, dependency, or infrastructure was added.
The allowlist uses the existing bounded tool catalog and the store reuses the
locked agent-run row; no new SQL read is required. Tool-registration preflight
is bounded by the existing 64-tool maximum and prevents model spend for
unavailable declared tools. Retrieved-context framing adds a small fixed
number of bytes to the existing bounded history. No latency or throughput
benchmark is claimed.

Reference research reused the role-separation pattern from DeepSeek Harness
and the governed tool-set/strict unknown-tool rejection pattern from Orloj.
No reference source was copied. DeepSeek Harness is MIT and Orloj is
Apache-2.0; license notices and dependency manifests are unchanged.

## Verification

Passed locally:

- `make qualification-agent-trust-boundary`
- `make qualification-support-bundle`
- `make qualification-workflow-retry-deadline`
- Full tests for `internal/agentloop`, `internal/contracts`, `cmd/fornix`,
  `internal/profile`, and `internal/runtime`
- Focused race tests for agent-loop trust cases, support bundles, and retry
  contracts
- `go test ./... -run '^$' -count=1` (all Go test packages compile)
- Focused `go vet`, `make fmt-check`, and `make docs-check`
- CI workflow YAML parsing and `git diff --check`

`make package-check` was attempted but could not complete because the sandbox
denied loopback binding in
`TestHTTPSecretManagerUsesMetadataOnlyRequestAndRejectsRedirects`. The
credentials package passed with only that local-listener test skipped. CI's
`actionlint` binary is not installed here; the workflow parsed successfully
with Ruby's YAML parser.

The following PostgreSQL-backed tests were skipped because
`FORNIX_TEST_PG_DSN` is unset:

- `TestAgentRunStorePersistsExecutionMetadata`
- `TestAgentRunStoreRejectsToolCatalogMutation`
- Task 100 retry deadline, runtime resume, and retry-budget cleanup tests

The new PostgreSQL test target is
`make qualification-agent-trust-boundary-postgres`; it must run against a
disposable database before persistence and immutable-catalog behavior can be
called database-qualified. CI was not run from this local environment.

## Remaining limits

User-role placement and clear framing reduce privilege confusion but do not
make model input immune to prompt injection. Registry-authoritative tool
descriptions and schema binding are addressed by Task 103; a compromised or
misleading registration can still influence the model, while independent
policy and execution limits remain authoritative. The local process executor
still does not provide complete kernel-enforced filesystem/network isolation
on every supported host.
Issue #40 production qualification, live provider/effect conformance, and
backup/restore, HA, and load evidence remain open.
