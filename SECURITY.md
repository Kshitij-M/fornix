# Security policy

Fornix is an alpha, single-node substrate for verifiable AI work on important
repositories. Treat the current implementation as a project for development,
evaluation, and controlled internal use—not as a complete security boundary
for untrusted multi-tenant workloads or unattended production changes.

## Reporting a vulnerability

Please do not publish credentials, private repository content, working
exploits, or detailed reproduction steps in a public issue or pull request.
Use the repository’s [private vulnerability-reporting channel on GitHub](https://github.com/Kshitij-M/fornix/security/advisories/new)
when it is enabled. If no private channel is available, open a minimal public
issue containing only a non-sensitive summary and ask the maintainers for a
private follow-up channel.

Include, when safe to share privately:

- the affected commit, route, package, or configuration;
- the impact and required permissions or deployment conditions;
- a minimal reproduction that contains no secrets or private data;
- any mitigation that has already been applied.

There is no promise of a particular response time while the project is alpha.
The maintainers will assess the report, protect affected users where possible,
and document a fix or mitigation when appropriate.

## Security model in the current alpha

The current design has several deliberate controls:

- **Workspace scope:** authenticated workspace API keys and authorization
  checks are applied at the boundary. Cross-workspace reads and writes are
  intended to fail closed.
- **Identity and RBAC:** production-shaped workspace mode uses hashed,
  expirable, revocable, and rotatable API keys, deterministic role/capability
  checks, and authorization audit records.
- **Credential handling:** model credentials are supplied through environment
  or configured credential references. They must not enter events, artifacts,
  evidence, metrics, checkpoints, errors, tests, or logs.
- **Task ownership:** task-bound workers use leases and monotonic fencing;
  stale workers cannot advance the authoritative task or run state.
- **Tool admission:** tools are explicitly registered, deny-by-default, and
  invoked as structured argument vectors rather than through an implicit shell.
  Arguments, environment, output, and time are bounded.
- **Evidence integrity:** raw evidence and artifact bytes are content-hashed
  and immutable. Provenance, supersession, disclosure, and retention preserve
  auditability rather than overwriting history.
- **Operational bounds:** request bodies, context, outputs, retries, provider
  work, and disclosures have explicit limits. Redaction is applied before
  durable observability and evaluation surfaces.

The [HTTP API reference](docs/53-http-api-reference.md) describes the
authentication, idempotency, workspace, and external-effect rules for the
current routes.

## Safe deployment and development practices

For local development:

- copy `.env.example` to `.env`, keep `.env` local, and replace development
  secrets before sharing a machine or environment;
- use the fake provider unless a real provider is explicitly required;
- keep `FORNIX_AUTH_MODE=development` limited to local compatibility testing;
- never paste `FORNIX_OPENAI_API_KEY`, `FORNIX_BOOTSTRAP_KEY`, or any API key
  into chat, source, logs, issues, fixtures, or command history;
- expose only the repository mount and tool capabilities needed by the test;
- do not expose the development Postgres port or an unauthenticated service to
  an untrusted network.

For a production-shaped evaluation:

- set `FORNIX_AUTH_MODE=workspace` and create workspace-scoped API keys;
- use strong, separately managed database, bootstrap, and provider secrets;
- keep PostgreSQL on a private network and apply normal database backup,
  access-control, and encryption practices;
- configure explicit repository mounts and least-privilege tool policies;
- set hard model, tool, retrieval, artifact, and retention budgets;
- inspect the qualification note before treating a control as operationally
  sufficient.

## Known security limitations

The following are not implemented or not fully qualified in the current alpha:

- OAuth/SSO, external KMS or secret-manager resolution, and automated key
  rotation policy;
- PostgreSQL row-level security as a second authorization layer;
- a general kernel-level sandbox or complete network/filesystem isolation for
  every local process tool;
- exactly-once guarantees for remote model calls or external processes after
  an effect has begun;
- backup/restore drills, high availability, capacity qualification,
  operational backpressure, and external object-storage isolation;
- a full multi-tenant administration experience.

These are product limitations, not configuration suggestions. The complete
current list is maintained in [`docs/14-production-readiness-qualification.md`](docs/14-production-readiness-qualification.md).

## Secret and data handling expectations

Fornix can process repository content, model inputs, tool outputs, evidence,
and artifacts. Operators are responsible for deciding whether those inputs are
appropriate for the configured database, provider, filesystem mount, and
retention policy. Do not assume that a local development deployment has the
controls required for regulated or confidential data.

When reporting a problem, replace secrets and private content with synthetic
values. Hashes, IDs, route names, error classes, and bounded metadata are
usually safer than raw payloads, but verify before sharing them.
