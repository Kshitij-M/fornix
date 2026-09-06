# Fornix Local: single-package CLI and managed runtime foundation

Status: implemented alpha slice; release qualification
and public distribution remain in progress. This document records the product
requirements and qualification contract for the local package. The current
implementation is described in [`63-fornix-local-operations.md`](63-fornix-local-operations.md).

## Decision summary

Fornix will have one canonical end-user package: the native `fornix` CLI.
The CLI will install, start, inspect, operate, upgrade, and stop a local
Fornix runtime. The normal user must not need a source checkout, Go, Make,
manual `.env` files, direct PostgreSQL commands, or direct Docker Compose
commands.

The product path is:

```text
install → start → prompt → inspect a verified result
```

The first supported workflow is:

```sh
curl -fsSL https://get.fornix.dev/install.sh | sh
cd my-repository
fornix start
fornix run --repo . "Review this repository and identify the highest-risk issues"
```

The default provider is the deterministic fake provider. A model provider is
never enabled implicitly. OpenAI-compatible execution remains explicit,
bounded, and opt-in through environment configuration or a future secure
provider-login command.

The local runtime is managed, not hidden dishonestly. `fornix start` may use
Docker Desktop on macOS or Docker Engine plus Compose on Linux to run the
version-pinned Fornix server and PostgreSQL/pgvector. Docker itself is a
privileged host prerequisite and is not silently installed by the Fornix
installer. Fornix owns the runtime lifecycle after that prerequisite is met.

Postgres remains the only control-plane authority. The CLI is a user-facing
client and runtime manager; it is not a second database, event log, or
execution authority.

## Product problem and outcome

Fornix already provides durable tasks, ownership, retrieval, model/tool
boundaries, artifacts, evidence, validation, receipts, and replay. However,
the current path begins with development operations:

```sh
cp .env.example .env
make dev-up
make dev-run
```

That makes the system difficult for a new user to evaluate and prevents the
project from receiving useful real-world feedback. The packaging problem is
therefore a product problem: users must reach a safe, bounded, inspectable
repository workflow with minimum setup.

The outcome of this workstream is not merely a downloadable binary. It is a
qualified local product that lets a user:

1. install a verified artifact;
2. start a persistent local runtime;
3. create or select a workspace automatically;
4. provide a repository through an explicit safe mount;
5. submit a natural-language task through the CLI;
6. use the fake provider without a credential;
7. optionally use an external provider with explicit budgets;
8. inspect evidence, provenance, artifacts, validation, and a Work Receipt;
9. stop, restart, upgrade, diagnose, and report failures without losing data.

## Canonical package boundary

### What one package means

The canonical package is one `fornix` executable per supported platform. It
contains:

- the operator CLI;
- the local runtime manager;
- the API client used by CLI commands;
- the stable version and compatibility metadata;
- the embedded release runtime manifest and safe defaults;
- the `run` façade for the end-to-end workflow;
- health, diagnostics, support-bundle, and lifecycle commands;
- shell completion generation;
- offline demo orchestration.

The executable may also run the server through the existing `fornix serve`
entry point for advanced deployments. Normal users do not need to know that
entry point exists.

The first package does not separately expose `fornix-watcher` or
`fornix-eval`. Their capabilities must be reachable through `fornix` commands
or remain explicitly advanced until their dependencies and user contracts are
self-contained. A separate binary is not a reason to make the primary user
install more complicated.

### What the package manages

On the managed local path, the CLI owns:

- a versioned Fornix application container;
- a versioned PostgreSQL/pgvector container;
- named persistent volumes;
- a private local profile directory;
- readiness and migration checks;
- workspace bootstrap;
- local actor and API-key bootstrap;
- explicit repository mount grants;
- runtime logs and bounded diagnostics;
- upgrade and non-destructive stop behavior.

The package does not bundle Docker Engine, Docker Desktop, or a second
database implementation. It also does not silently install large Ollama model
weights. Ollama remains an optional provider dependency and is not required by
the offline path.

### Distribution policy

The direct signed archive and installer are the canonical package. Homebrew,
Debian, and RPM formats may be added later as installation adapters that
install the same CLI and version contract. They must not create different
runtime behavior, configuration semantics, or security defaults.

This workstream is complete when the canonical package is useful and
qualified. It must not be blocked by a tap, distro package, or native watcher
bundle that does not improve the primary install → start → prompt path.

## User-facing command contract

The CLI has three audiences without splitting the product into multiple
applications:

- end users run `start`, `run`, `runs`, `receipt`, and `artifact`;
- local operators run `status`, `logs`, `doctor`, `upgrade`, and `stop`;
- advanced operators use workspace, identity, task, evaluation, policy,
  validation, and `serve` commands.

All control-plane actions must be possible through the CLI. HTTP and MCP stay
available as compatibility and automation surfaces, but a user must not need
to call them directly.

### Required commands

The command names below are the target contract. Existing commands remain
compatible where practical, with aliases and deprecation messages during the
alpha period.

```text
fornix version [--json]
fornix help

fornix start [--repo PATH] [--detach] [--pull]
fornix stop [--keep-data]
fornix restart
fornix status [--json]
fornix logs [--service app|db] [--follow]
fornix doctor [--json] [--check database|repository|provider|all]

fornix setup [--workspace NAME] [--yes]
fornix workspace get|list|bootstrap
fornix identity create|disable|list
fornix role bind|unbind|list
fornix api-key create|rotate|revoke|list

fornix provider list
fornix provider configure openai
fornix provider test openai --budget ...

fornix demo
fornix run --repo PATH [run options] "PROMPT"
fornix task create|list|get|cancel
fornix run list|get|replay|cancel
fornix receipt show|disclose
fornix artifact list|disclose|verify
fornix evidence list|disclose

fornix ingest submit|status|resume|cancel|report
fornix retrieve
fornix evaluation dataset|run|status|results|report|replay
fornix metrics
fornix validation run|status|results|disclose
fornix policy list|get|resolve|audit
fornix change propose|approve|apply|disclose

fornix upgrade [--version VERSION] [--dry-run]
fornix uninstall [--purge-data]
fornix support bundle --output PATH
fornix serve
```

The exact command set can be implemented incrementally, but every command
must have a defined workspace, actor, authorization, idempotency, output,
failure, and redaction contract.

### Natural-language run contract

`fornix run` is the primary product command. It must hide the internal sequence
of task creation, fenced claim, ingestion/retrieval, context compilation,
model selection, tool policy, agent-loop advancement, artifact creation,
validation, and Work Receipt assembly.

```sh
fornix run --repo . "Explain the architecture of this repository"
```

The default behavior is:

- use the selected local workspace and authenticated local actor;
- use a read-only repository mount;
- use deterministic fake-provider behavior if no provider is selected;
- enforce hard wall-clock, model-call, token, tool, output, and cost budgets;
- produce progress on stderr and the final bounded result on stdout;
- return the run ID, receipt ID, artifact references, evidence references, and
  terminal reason without dumping an unbounded transcript;
- preserve every authoritative transition and external-effect boundary;
- require a separate approval-gated command for repository mutation.

The command must support `--json` for automation and stable exit codes. It
must not print credentials, raw prompts in metrics, private environment
values, or unbounded tool/model output.

### Repository access contract

Repository access is explicit and read-only by default:

```sh
fornix run --repo /absolute/path/to/repository "..."
```

The CLI must:

- resolve the path without following an escaping symlink;
- reject the filesystem root, home directory, and overly broad mounts unless
  the user explicitly overrides a documented safety check;
- normalize and record the source identity and manifest;
- pass only the selected repository path to the managed runtime;
- never mount the entire host filesystem;
- refuse a path that the runtime cannot safely bind;
- preserve the repository as source authority and never write to it during a
  read-only run.

When `--repo` is omitted, `fornix run` may use the repository discovered from
the current working directory only if it is an explicit configured local
profile mount. It must not silently mount an arbitrary current directory.

## First-run lifecycle

### Install

The convenience installer may be:

```sh
curl -fsSL https://get.fornix.dev/install.sh | sh
```

The security contract underneath that convenience command is stricter:

- the installer is versioned and reviewable;
- a pinned `FORNIX_VERSION` is supported;
- the archive and checksum manifest are downloaded into a private temporary
  directory;
- the checksum is verified before extraction or replacement;
- archive paths are validated against absolute and `..` traversal;
- installation is atomic and prefers a user-writable directory;
- `/usr/local/bin` requires an explicit user decision;
- existing binaries remain in place until verification succeeds;
- the installer never accepts a provider key as an argument;
- the installer never phones home or requires a provider credential;
- the installer prints version, platform, checksum result, and next steps,
  but never environment values or secrets.

The project must also document a download-first path for users who want to
inspect the script and verify the artifact manually. A blind convenience
installer is not the only trust model.

### Start

`fornix start` is idempotent. It must:

1. load the local profile without leaking its contents;
2. detect the supported OS and architecture;
3. detect Docker CLI and Compose support;
4. verify that the Docker daemon is running;
5. explain the exact official installation path if the runtime is missing;
6. create the private local state directory with restrictive permissions;
7. select a stable runtime project name and local port;
8. render the version-pinned runtime manifest without credentials in it;
9. pull only the selected images, with a clear first-download message;
10. create named volumes without deleting existing data;
11. start the application and database with health checks;
12. wait with bounded exponential backoff for database readiness;
13. apply supported migrations exactly once;
14. bootstrap the local workspace and actor idempotently;
15. create or load a local API credential through the profile store;
16. verify the authenticated readiness endpoint;
17. record a redacted startup observation;
18. print the next `fornix run` command.

If any step fails, `start` must identify the failed boundary and provide a
bounded remediation. It must not remove volumes, reset data, or leave a
credential in command output.

### Stop and restart

`fornix stop` stops managed services and preserves volumes by default.
`fornix restart` is equivalent to a bounded stop/start while preserving the
profile, workspace, credentials, database, and runtime version.

Destructive actions require a separate explicit command and confirmation:

```sh
fornix uninstall --purge-data
```

The CLI must state exactly which data is being removed. Normal uninstall must
not remove repository files, named volumes, authoritative artifacts, or
workspace history.

## Local profile and credential design

The profile directory defaults to:

```text
~/.fornix/
```

It is configurable through `FORNIX_HOME` or an explicit global flag. The
profile contains non-secret runtime metadata such as:

- package version and runtime version;
- local server address;
- workspace ID and display name;
- actor ID;
- credential reference, never an unredacted key in ordinary configuration;
- selected repository mount grants;
- runtime project name and volume names;
- last known migration and readiness state.

Secrets must use this precedence:

```text
explicit CLI input → environment/CI input → OS credential store
  → restrictive local-file fallback
```

Interactive provider credentials must be entered without echoing. Where an OS
credential facility is available it is preferred. The documented fallback is
a `0600` file owned by the user, never a project `.env` or Compose file.
Environment variables remain the recommended non-interactive CI mechanism.

The credential contract is:

- local API credentials are generated by the bootstrap flow and are scoped to
  the local workspace;
- provider keys are references or process inputs and are never copied into the
  database, events, artifacts, logs, images, or support bundles;
- rotation creates a new credential before revocation of the old one where
  safe, and the CLI reports only metadata;
- revocation and expiry fail closed;
- concurrent CLI processes cannot corrupt the profile or rotate credentials
  unexpectedly;
- profile writes are atomic and protected by a local lock;
- `fornix doctor` reports configured/not configured, never the credential.

Backward-compatible development authentication may remain available only
behind an explicit development configuration. The managed local profile must
not make development authentication the default for a network-exposed
deployment.

## Managed runtime design

### Runtime manifest

The CLI must generate or extract a versioned runtime manifest from an embedded
template. It must include:

- Fornix image reference and immutable version/digest;
- PostgreSQL/pgvector image reference and tested version/digest;
- application and database health checks;
- internal network configuration;
- named persistent volumes;
- non-root application execution;
- no host Postgres exposure by default;
- no source-tree bind mount by default;
- explicit repository mount grants only;
- fake-provider defaults;
- opt-in external-provider environment forwarding;
- bounded log configuration;
- migration compatibility metadata.

Generated files must contain no API keys, database passwords, prompts, source
content, or arbitrary environment values. The CLI should pass secrets through
the narrowest supported runtime mechanism and redact command diagnostics.

### Runtime command execution

The first implementation may use `docker compose` as the managed runtime
adapter. The adapter must be isolated behind an internal interface so that:

- command construction is deterministic and testable without Docker;
- stdout/stderr are captured with bounded buffers;
- exit codes and timeout errors are stable;
- user input is passed as structured arguments, never interpolated into a
  shell command;
- concurrent `start`, `stop`, and `upgrade` operations are serialized per
  profile;
- no arbitrary Compose file or host path is accepted without validation.

The adapter must not use `docker compose down -v` for normal stop, upgrade,
failure recovery, or uninstall. Volume deletion is a separate explicit
operation.

### Repository mount implementation

`fornix run --repo` must use one of these implementation strategies, selected
by qualification against the current server architecture:

1. a stable explicit bind mount established by the runtime profile; or
2. a short-lived, per-run worker/runtime with only the selected repository
   bind-mounted read-only; or
3. an equivalent server-side ingestion and tool boundary that never exposes a
   broader host path.

The chosen strategy must preserve the existing workspace, task fencing,
ingestion, artifact, and provenance invariants. It must not use a broad
`/Users`, `/home`, or host-root mount as a shortcut.

## Provider and execution experience

### Offline default

The first run must work without Ollama, OpenAI, Anthropic, or any remote
service beyond pulling the packaged runtime images:

```sh
fornix start
fornix demo
```

The demo must use a small bundled fixture only, create a task/run/report/
artifact/evidence/receipt, verify replay hashes, and never modify an arbitrary
user repository.

### External provider opt-in

The provider path must remain explicit:

```sh
FORNIX_OPENAI_API_KEY=... fornix run --provider openai --model MODEL \
  --max-cost 0.25 --max-time 2m --repo . "..."
```

The CLI must validate that the provider is enabled, the credential is
available, and the requested budgets are valid before creating durable work.
It must not echo or persist the key. The provider gateway continues to report
measured, estimated, and unknown usage separately and makes at-least-once
remote execution explicit.

### Output and inspection

The normal result is a concise human-readable summary followed by stable
identifiers:

```text
Run:       run_...
Status:    completed
Receipt:   receipt_...
Report:    artifact_...
Evidence:  evidence_...
Replay:    verified
```

Detailed content is disclosed through bounded commands:

```sh
fornix receipt show receipt_...
fornix artifact disclose artifact_... --level detail --max-bytes 20000
fornix run replay run_...
```

The CLI must preserve workspace authorization and redact sensitive content in
human output, JSON output, errors, logs, and support bundles.

## Efficiency and stability budget

The package should make the common path cheap and predictable:

- one native process for CLI orchestration;
- persistent database volumes so startup does not rebuild or re-index data;
- no Ollama image/model download on the default path;
- image pulls only on first start or explicit upgrade;
- health checks use bounded polling with exponential backoff;
- profile locking prevents duplicate lifecycle work;
- command output and logs have hard byte limits;
- repeated starts and runs are idempotent by request identity;
- all model, tool, retrieval, and database budgets remain enforced by the
  server authority;
- the CLI does not introduce a second scheduler, cache, or event store.

Qualification must measure, rather than assume:

- compressed and installed CLI size;
- container image sizes and first-pull bytes;
- first start time and warm start time;
- migration and readiness time;
- `fornix run` CLI overhead separate from server latency;
- profile and log storage growth;
- restart and recovery time;
- concurrent-command contention;
- offline demo throughput;
- OpenAI smoke latency, token usage, and cost when explicitly enabled.

The acceptance budget is not a universal performance promise. The release
report must state the hardware, Docker version, image versions, repository
size, provider, and measured results.

## Security, privacy, and licensing

The CLI is a security boundary, not just a convenience wrapper. It must:

- default to loopback binding for local mode;
- fail closed when workspace or actor authorization is missing;
- never accept provider secrets as positional arguments;
- never put secrets in generated Compose, systemd, archives, logs, metrics,
  events, artifacts, prompts, or issue templates;
- validate repository paths and reject symlink/traversal escape;
- keep read-only mounts read-only;
- require explicit approval for mutation;
- make destructive data operations separate, visible, and confirmed;
- redact support data by construction rather than by best effort post-processing;
- pin and verify downloaded artifacts and images where the runtime supports it.

Fornix remains MIT-licensed. Reference repositories may inform architecture,
but their source is not copied by this plan. Kronaxis Fabric is BSL 1.1 and
must not be copied. The release must include reviewed notices for bundled
dependencies and must document the licenses and terms of Docker, PostgreSQL,
pgvector, base images, and any installer dependencies.

## Implementation plan

The work is divided into reviewable slices. Each slice must include code,
tests, documentation, and measured evidence. Workstreams may be delegated in
parallel only when their write sets are disjoint.

### Phase 0 — Contract and migration inventory

Paths:

```text
AGENTS.md
docs/00-fornix-foundation.md
docs/14-production-readiness-qualification.md
docs/52-documentation-guide.md
cmd/fornix/
internal/config/
internal/server/
compose.yaml
Dockerfile
```

Deliver:

- final user journey and command/exit-code contract;
- inventory of existing CLI/API parity and bootstrap seams;
- decision for repository mount implementation;
- decision on local credential store and fallback;
- decision on runtime manifest/image pinning;
- migration decision: no new migration unless durable server state is
  required; local profile state belongs outside Postgres;
- feature note and risk register accepted before code changes.

### Phase 1 — CLI foundation and profile store

Paths:

```text
cmd/fornix/
internal/cli/
internal/profile/
internal/credentials/
internal/version/
```

Deliver:

- stable parser and help behavior;
- `version --json` with version, commit, build time, OS, arch, and schema
  compatibility;
- stable exit codes and human/JSON output modes;
- `~/.fornix` profile discovery and atomic writes;
- local lock for concurrent CLI processes;
- credential reference and secure fallback store;
- no-secret tests and backward-compatible command aliases.

### Phase 2 — Runtime manager

Paths:

```text
internal/runtime/
deploy/runtime/compose.yaml
deploy/runtime/README.md
Dockerfile
compose.yaml
```

Deliver:

- runtime adapter interface;
- Docker/Compose detection and version checks;
- embedded or generated release manifest;
- `start`, `stop`, `restart`, `status`, and `logs`;
- migration/readiness polling;
- stable project, volume, and port handling;
- pinned images and no development defaults in the managed path;
- non-destructive recovery from interrupted lifecycle commands.

### Phase 3 — Bootstrap and diagnostics

Paths:

```text
internal/bootstrap/
internal/diagnostics/
cmd/fornix/
scripts/test/
```

Deliver:

- idempotent workspace/actor/API-key bootstrap;
- `setup`, `doctor`, and `support bundle`;
- bounded redacted diagnostics;
- explicit provider registration/configuration status;
- profile-to-server authentication;
- recovery guidance for missing Docker, bad permissions, stale migrations,
  unavailable ports, and failed health checks.

### Phase 4 — Single-command execution façade

Paths:

```text
internal/client/
internal/workflow/
cmd/fornix/cli.go
```

Deliver:

- `fornix demo`;
- `fornix run --repo PATH "PROMPT"`;
- automatic task/claim/retrieval/context/model/tool/receipt orchestration;
- provider and budget flags;
- human-readable progress and stable JSON result;
- safe repository mount grant;
- run inspection, replay, artifact, evidence, and receipt disclosure;
- no raw transcript or secret leakage.

### Phase 5 — Release artifact and installer

Paths:

```text
.goreleaser.yaml
.github/workflows/release.yml
scripts/install.sh
scripts/release/
RELEASING.md
```

Deliver:

- macOS arm64/amd64 and Linux arm64/amd64 archives;
- verified checksum manifest and signature/attestation path;
- installer with pinned version, non-root prefix, atomic replacement, and
  unsafe-archive rejection;
- release manifest, SBOM, license notices, and no-secret archive checks;
- direct download and install smoke tests from a clean environment;
- a reproducible version contract across binary, image, and runtime manifest.

Homebrew, Debian, and RPM publication are follow-up adapters after the
canonical package has passed clean-room qualification.

### Phase 6 — Clean-room qualification and documentation

Paths:

```text
README.md
DEVELOPMENT.md
docs/README.md
docs/14-production-readiness-qualification.md
docs/63-fornix-local-operations.md
scripts/test/
.github/workflows/ci.yml
.github/workflows/release.yml
```

Deliver:

- quickstart that begins with install/start/run;
- CLI reference and troubleshooting;
- macOS/Linux support matrix;
- fresh install, warm start, upgrade, restart, backup/restore, and uninstall
  guidance;
- fake-provider end-to-end smoke from published artifacts;
- optional OpenAI smoke only when a secret is supplied by the environment;
- crash and recovery tests at start, migration, run, and stop boundaries;
- installation-feedback issue template with redaction instructions;
- final qualification report with measured size, time, storage, cost, and
  remaining limitations.

## Acceptance tests

### Clean installation

- A clean macOS arm64/amd64 and Linux arm64/amd64 environment installs the
  selected verified artifact without a source checkout.
- The installer rejects unsupported platforms, checksum mutation, unsafe
  archive paths, and unavailable installation prefixes safely.
- `fornix version`, `fornix --help`, and `fornix doctor` succeed with stable
  output and exit codes.
- No installer, archive, generated manifest, or package contains credentials,
  private repository paths, or development API keys.

### Runtime lifecycle

- Missing Docker produces actionable instructions and no partial destructive
  state.
- `fornix start` creates the runtime and reaches readiness with one command
  after the Docker prerequisite is installed.
- Repeated starts are idempotent.
- `stop`, `restart`, and `status` behave correctly after process interruption.
- Normal stop and uninstall preserve database volumes and repository files.
- Concurrent lifecycle commands are serialized and cannot corrupt the profile.
- Migrations run once, remain clean on restart, and fail with remediation.

### First workflow

- `fornix demo` succeeds without an LLM, Ollama, or provider key.
- `fornix run --repo . "..."` creates one durable task/run effect for one
  request identity.
- Repository path, symlink, and workspace boundaries fail closed.
- The run produces bounded output, artifact/evidence/provenance references,
  and a Work Receipt.
- Replay produces the same context, artifact, receipt, and terminal hashes.
- The fake provider is deterministic across clean installations.
- OpenAI is unavailable unless explicitly enabled and credentialed.
- OpenAI requests remain bounded by token, time, and cost budgets.
- A crash before and after each major boundary is recoverable without
  duplicating authoritative effects.

### Security and privacy

- API keys never appear in process diagnostics, errors, logs, events,
  artifacts, metrics, support bundles, Compose files, or command output.
- Provider credentials are not stored in the repository or generated runtime
  manifest.
- Workspace and actor authorization is preserved through CLI, API, task, run,
  model, tool, artifact, evidence, and receipt operations.
- Read-only runs cannot mutate the mounted repository.
- Destructive commands require explicit intent and identify the exact data
  target.

### Efficiency and operations

- First start, warm start, image pull, migration, readiness, run overhead,
  demo throughput, storage growth, and recovery time are measured.
- Logs, diagnostics, profile files, and command output remain bounded.
- The managed path adds no broker, Redis, NATS, second database, or duplicate
  scheduler.
- Published artifacts, image digests, schema compatibility, and notices agree
  with the release manifest.

## Definition of done

Task 24 is complete only when a new user can install the canonical package,
run `fornix start`, execute the offline demo or a repository prompt, inspect a
verified result, restart the runtime, and safely stop it without learning
Fornix internals.

The completion note must state:

- supported OS/architecture and Docker versions;
- exact install/start/run commands;
- measured package, image, storage, latency, and recovery results;
- tested provider behavior and explicit external-effect limits;
- migration, backup, upgrade, and uninstall guarantees;
- security and credential-storage behavior;
- known unqualified platforms and remaining product gaps.

This work makes Fornix easy to evaluate and use. It does not by itself claim
that unattended multi-agent repository modification, high availability,
external object storage, or enterprise identity is complete.
