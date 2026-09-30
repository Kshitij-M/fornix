# Fornix concepts

Status: current public concept reference.

Fornix uses a small set of domain-neutral concepts. Adapters translate their
own systems into these concepts; they do not bypass them.

| Concept | Meaning |
| --- | --- |
| Workspace | The isolation boundary for identity, data, authority, budgets, and replay. Every durable record belongs to one workspace. |
| Actor | The authenticated human, service, or worker responsible for a command. Caller-supplied actor fields do not override authentication. |
| Operation | A durable intent to perform typed work against a target resource. It has an identity, plan, lifecycle, lease, and replayable history. |
| Connector | An explicit adapter for one external system or capability family. Registration is typed and workspace-scoped. |
| Capability | One named, versioned operation an adapter can validate, plan, and execute. Its effect class and budgets are part of admission. |
| Policy | An immutable rule snapshot that decides which actors, capabilities, resources, budgets, and effects are allowed. |
| Approval | A durable decision bound to one operation, capability, target, input hash, and policy hash. Approval is never a free-form boolean. |
| Lease | Time-bounded ownership of a task, operation, run, or consumer. It prevents concurrent authoritative mutation. |
| Fence | A monotonically increasing number attached to a lease. A stale fence fails closed even if the old worker is still running. |
| Effect reservation | The durable record created before a remote side effect. It makes uncertainty visible; it does not execute the provider call. |
| Evidence | Immutable, content-hashed source material or a bounded reference to it. Evidence supports a result without replacing authoritative history. |
| Artifact | Content-addressed bytes or a derived report stored with workspace scope, provenance, retention, and disclosure budgets. |
| Work Receipt | A hash-stable, inspectable summary of what was admitted, executed, evidenced, validated, and costed. |
| Replay | A read-only reconstruction or hash-chain verification using recorded dependencies. Replay never invokes a live model or external tool. |

## One universal lifecycle

```text
intent
  → workspace and actor
  → capability and policy admission
  → approval and budgets
  → lease/fence ownership
  → bounded connector/model/tool work
  → evidence, artifacts, and effect reconciliation
  → validation and verification
  → Work Receipt
  → replay and evaluation
```

The repository adapter is the first concrete adapter. The lifecycle is not
repository-specific: an API, database, cloud, ticketing, or internal-system
adapter must satisfy the same identity, policy, evidence, effect, and replay
boundaries.
