# Fornix Local operations guide

Status: implemented alpha path; public release
qualification is still in progress.

Fornix Local is the simplest way to evaluate the Fornix control plane on a
developer workstation:

```text
install → start → run → inspect → stop
```

The native `fornix` executable owns the local user experience. It creates a
private profile, prepares a versioned Compose manifest, starts the Fornix and
PostgreSQL/pgvector containers, waits for readiness, bootstraps a local
workspace, and calls the existing authenticated API. Postgres remains the
authority for tasks, events, runs, evidence, artifacts, receipts, and replay.
The CLI does not maintain a second local database or event log.

## Prerequisites

Fornix requires a Docker-compatible runtime that is already installed and
running:

- macOS: Docker Desktop with the host repository shared with Docker Desktop;
- Linux: Docker Engine and the Compose v2 plugin.

Fornix does not silently install Docker. Docker is a privileged host runtime
with platform-specific installation and licensing terms, so `fornix doctor`
checks it and prints a targeted remediation when it is unavailable. After
Docker is installed, users do not need to operate Compose or run PostgreSQL
commands themselves.

The currently qualified development path builds the binary from a checkout:

```sh
cd /path/to/fornix
make build
./bin/fornix doctor
```

The public release installer will use the same interface once a signed GitHub
release channel and `get.fornix.dev` endpoint are published. Until then, do
not present the curl installer as an available hosted service:

```sh
# Planned public release path; not a claim that this endpoint is live today.
curl -fsSL https://get.fornix.dev/install.sh | sh
```

The installer itself is already checked into `scripts/install.sh`. It selects
the macOS/Linux and amd64/arm64 archive, verifies its SHA-256 checksum, rejects
unsafe archive paths, and installs only the `fornix` executable. Release
signatures and hosted provenance are still open qualification work.

## First run

From a repository that Fornix is allowed to inspect:

```sh
./bin/fornix start --repo .
./bin/fornix run --repo . "Explain the architecture of this repository"
```

The default provider is the deterministic fake provider. No model key, Ollama
installation, or network access is needed for the offline path. To exercise
the complete reference workflow without a remote provider:

```sh
./bin/fornix demo --repo .
```

`demo` runs the repository ingest, task ownership, bounded retrieval, fake
model step, read-only tool step, artifact/evidence capture, Work Receipt, and
replay checks. It is intended as a product smoke and as a first contribution
for a new user.

The local runtime prints its workspace, provider, database, and loopback
address when ready. The API is bound to loopback by default; the database has
no host-published port.

## Command reference

The local commands are deliberately small and deterministic:

| Command | Purpose |
| --- | --- |
| `fornix start` | Create or load the local profile, start the runtime, migrate, and wait for readiness. |
| `fornix run --repo PATH PROMPT` | Submit one bounded repository task and print its durable result summary. |
| `fornix demo --repo PATH` | Run the offline reference workflow and verify replay. |
| `fornix status` | Report profile, endpoint, workspace, provider, and container health. |
| `fornix logs` | Read bounded service logs; use `--follow` for a live view. |
| `fornix doctor` | Check Docker, profile safety, runtime configuration, and readiness. |
| `fornix stop` | Stop services while preserving the profile and database volume. |
| `fornix restart` | Restart the managed runtime and re-check readiness. |
| `fornix upgrade` | Pull the configured image and restart with the existing profile. |
| `fornix uninstall` | Stop and remove the managed project while preserving data by default. |
| `fornix uninstall --purge-data --yes` | Explicitly remove the local profile and managed database volume. |
| `fornix support --output PATH` | Write a redacted diagnostic bundle for a support issue. |
| `fornix version` | Print build, platform, and schema compatibility information. |

Every command accepts the common local options shown by `fornix help`,
including `--home`, `--workspace`, `--url`, `--json`, and `--bootstrap-key`.
The CLI uses bounded output and deterministic JSON fields so scripts can use
it without scraping human prose. Repeating a request with the same workspace,
repository, prompt, and idempotency identity returns the existing durable
effect rather than creating another task or run.

`--detach` is accepted for compatibility with the managed-runtime mental
model: the runtime manager starts containers detached, then the CLI waits for
readiness unless a command only requests lifecycle status. `--keep-data` is
also accepted and is the safe default for stop/uninstall operations. Neither
flag changes the authority or creates a second runtime.

## Providers and budgets

The fake provider is the default and is the only provider required for local
development. Provider selection is explicit:

```sh
./bin/fornix run --repo . --provider fake \
  --max-turns 2 --max-cost 0 "Summarize the repository"
```

OpenAI-compatible chat is opt-in and bounded. Supply its credential only in
the invoking process environment; never put it in a profile, Compose file,
repository, command argument, issue, or support bundle:

```sh
FORNIX_OPENAI_API_KEY='set-in-your-shell' ./bin/fornix run \
  --provider openai --model gpt-4o-mini --max-cost 0.25 --repo . \
  "Explain the highest-risk area of this repository"
```

The example intentionally does not contain a real credential. Fornix does not
print the variable, persist its value, or include it in errors, events,
metrics, artifacts, or evidence. Remote model calls have an explicit
at-least-once boundary: provider idempotency is used where supported, but
Fornix does not claim exactly-once external execution.

The run path applies turn, token, byte, wall-clock, tool, and cost budgets
before work is admitted. A budget failure is a durable, inspectable outcome;
it is not silently converted into an unbounded retry.

## Profile and data layout

The profile root defaults to `~/.fornix` and can be changed with `--home` or
`FORNIX_HOME`. A profile contains only local control metadata and references:

```text
~/.fornix/
  profiles/<name>/profile.json
  credentials/<hashed-reference>.secret
  runtime/<name>/compose.yaml
  runtime/<name>/manifest.json
```

The profile and credential directories are created with owner-only access.
Credential files contain secret material only when the local operator
explicitly stores it; the profile stores a reference and hash, never a raw
provider key. Generated runtime manifests use Compose environment references,
not secret values. The path is designed for a single local operator, not as a
replacement for an enterprise secret manager or OS keychain.

Useful environment overrides are:

| Variable | Meaning |
| --- | --- |
| `FORNIX_HOME` | Profile root. |
| `FORNIX_LOCAL_IMAGE` / `FORNIX_IMAGE` | Pinned Fornix image or local test image. |
| `FORNIX_DB_IMAGE` | Explicit PostgreSQL/pgvector image override; the default is pinned to a multi-platform digest. |
| `FORNIX_PORT` | Loopback application port. |
| `FORNIX_DOCKER_PATH` | Explicit Docker executable when it is not on `PATH`. |
| `FORNIX_RUNTIME_PROJECT` | Compose project name for disposable or isolated runs. |
| `FORNIX_OPENAI_API_KEY` | Opt-in process environment credential; never persisted by the CLI. |
| `FORNIX_OPENAI_MODEL` | Default OpenAI model when `--model` is omitted. |
| `FORNIX_OLLAMA_MODEL` | Explicit Ollama model for an Ollama-enabled path. |

Runtime names, ports, image references, and paths are validated before a
manifest is written. A runtime project cannot escape the profile, and a
repository path must resolve to an existing directory without a final
symlink. The repository is mounted read-only for the reference workflow.
Path traversal and symlink escapes fail closed.

## Lifecycle and recovery

`start` is safe to repeat. It uses an exclusive profile lock, writes the
manifest atomically, starts the versioned services, applies migrations, and
polls the authenticated readiness endpoint. Existing volumes are reused. A
stopped or interrupted process can be followed by `status`, `doctor`, or
`start`; the database remains the source of truth.

The intended lifecycle is:

```text
start → run/demo → status/logs/doctor → stop
          ↘ crash → start/doctor → durable checkpointed recovery
```

The reference workflow uses task and worker fencing, durable agent checkpoints,
idempotency records, append-only events, and artifact/evidence links. A crash
before a database transaction commits leaves that transaction absent. A crash
after commit is safely replayable; an external model or tool boundary remains
at-least-once and is reported as such.

`restart` preserves data. `upgrade` changes the configured image and runs the
same readiness/migration checks. Before a production upgrade, operators should
take a database backup; automatic backup/restore and high-availability
topologies are not yet part of Fornix Local.

`uninstall` preserves the named volume by default. Purging is deliberately
explicit and requires `--yes`:

```sh
./bin/fornix uninstall --purge-data --yes
```

This removes the exact managed profile and Compose project data. It does not
remove repository files or unrelated Docker images, containers, volumes, or
projects.

## Inspecting results

The CLI summary identifies the durable task, agent run, report artifact,
evidence, and Work Receipt. The HTTP and MCP surfaces expose equivalent task
semantics for integrations; all are authenticated and workspace-scoped.
Use `--json` when handing an identifier to an integration. Detailed disclosure
continues to enforce item, byte, and token budgets, and preserves evidence
hashes and provenance references.

The report is not merely a model transcript. A reviewer can trace the result
to the ingested source, selected context, model/tool records, validation
outcome, artifacts, cost observations, and replay hash. This is the central
Fornix promise: useful AI work should remain bounded, inspectable, and
recoverable.

## Troubleshooting

### Docker is missing or not running

Run `fornix doctor`. Start Docker Desktop on macOS or the Docker service on
Linux, then repeat `fornix start`. If Docker is installed outside the standard
path, set `FORNIX_DOCKER_PATH` to its absolute executable path.

### The port is already in use

Run `fornix status` to see the configured endpoint. Select another loopback
port with `--port` or `FORNIX_PORT`, then restart. The profile persists the
chosen value so status and start agree. An explicit `--port` takes precedence
over the environment for that invocation.

### A run is not ready

Use `fornix logs --service fornix`, followed by `fornix doctor`. Migration,
database health, image compatibility, and authorization failures are reported
without printing credential values. A failed run remains inspectable in
Postgres and can be diagnosed or replayed; do not delete the volume as a first
response.

### The repository is rejected

Use an explicit absolute or current-directory path and ensure the directory is
within the host paths shared with Docker. Fornix rejects traversal, final
symlink escapes, unreadable roots, and unsupported file sizes/types by design.

### OpenAI is rejected

Confirm that `--provider openai` was explicitly selected and that
`FORNIX_OPENAI_API_KEY` is present in the same process environment. The key is
never read from a file generated by Fornix. Check the selected model and cost,
token, timeout, and response budgets before retrying.

## Security and privacy boundaries

The local path is designed to make safe behavior easy, not to imply a hostile
multi-tenant desktop sandbox. The current guarantees are:

- API keys are hashed, scoped, expirable, revocable, and rotatable;
- workspace and actor identity propagate through durable control-plane paths;
- secrets are redacted from logs, events, metrics, evidence, artifacts,
  checkpoints, errors, and support bundles;
- repository mounts are explicit and read-only in the reference workflow;
- tools use structured argv and deny-by-default policy;
- destructive data purge requires an exact command and confirmation flag.

The current alpha does not provide kernel-level sandboxing for arbitrary host
processes, enterprise SSO, an external secret manager, remote multi-tenant
isolation, or a hosted service. Treat the mounted repository and local Docker
daemon as trusted operator infrastructure until those boundaries are
separately qualified.

## Qualification snapshot

The disposable local-runtime smoke has been exercised on macOS arm64 with
Docker Engine 27.5.1 and Compose 2.32.4, PostgreSQL/pgvector `pg17` at the
default pinned manifest digest, and a local Fornix image. Its fixture contains two files, approximately 600 bytes,
two indexed chunks, and one symbol. It verifies readiness, deterministic fake
provider execution, artifact/evidence creation, Work Receipt verification,
duplicate request idempotency, replay, and service status. These are local
qualification results, not a promise of identical timings on every host.

The release qualification report must still publish artifact and image sizes,
cold/warm startup, migration and workflow latency, profile/database storage,
recovery time, supported architecture matrix, and hosted installer provenance.
The single-node runtime has no HA, automatic backup, object storage, or
large-repository capacity guarantee yet.

For the broader design rationale and the complete qualification contract, read
[`62-packaging-distribution-foundation.md`](62-packaging-distribution-foundation.md)
and [`14-production-readiness-qualification.md`](14-production-readiness-qualification.md).
