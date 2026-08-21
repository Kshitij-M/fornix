# Loop 17 completion: retrieval-surface capture and operator evaluation

Status: complete.

## Delivered

- Added typed redacted `RetrievalSurface` contracts with request/plan/context
  hashes, budgets, trace stages, source/evidence references, actor scope,
  causation/correlation IDs, bounded latency/SQL/cost measurements, and a
  canonical payload hash.
- Added migration `024_retrieval_surface_capture.sql`. The new table is
  workspace-scoped, append-only, indexed for bounded reads, and has a unique
  idempotency boundary. Raw queries, prompts, embeddings, credentials, and
  rendered context are not stored.
- Installed automatic capture at the retrieval store boundary, so HTTP and
  future internal retrieval callers share one capture path.
- Added transactional capture, duplicate/conflict handling, bounded batch
  reads, opaque cursor pagination, workspace isolation, and crash rollback.
- Added authenticated operator endpoints for dataset registration, surface
  registration/listing, bounded recorded retrieval evaluation, dry runs,
  baseline comparison, and run/status/report reads.
- Added `cmd/fornix-eval`, a deterministic offline CLI that consumes only
  redacted recorded surfaces and never contacts a model, tool, broker, or
  external service.
- Reused the existing `RetrievalScorer`, `EvidenceStore`, `EvaluationStore`,
  `ArtifactStore`, RBAC middleware, and observability paths. No reference
  source was copied; Fornix remains MIT and no BSL code was used.
- Added Make/CI/smoke coverage, authorization tests, defensive concurrent
  capture copying, and stable report-artifact reference loading.

## Qualification

Green checks:

- fresh database migration 001–024 plus retrieval-surface integration tests;
- `make check` with Postgres, including Go tests, vet, and Python checks;
- `go test -race ./...` with Postgres;
- `make build`, including `fornix-eval`;
- complete `make smoke` chain v0.10–v0.26;
- automatic capture, duplicate/conflict capture, append-only mutation
  rejection, rollback injection, bounded cursor pagination, workspace
  isolation, operator authentication, dry-run non-persistence, baseline
  regression comparison, durable scoring, artifact-backed report disclosure,
  and byte-identical CLI replay.

## Measurements

Measured on the local Docker Postgres/Go environment, with the existing
retrieval path included in HTTP timings:

| Measurement | Result |
| --- | ---: |
| Retrieval + capture HTTP, 20 samples p50 | 2.64 ms |
| Retrieval + capture HTTP, 20 samples p95 | 5.12 ms |
| Retrieval + capture HTTP, maximum | 7.76 ms |
| Captured surface row logical size, average of 20 | 1,916 bytes |
| Captured surface row logical bytes, 20 samples | 38,328 bytes |
| Surface relation size after local qualification data | 456 KiB |
| Surface table/index SQL shape | 1 bounded INSERT + 1 keyed read per capture |
| Evaluation surface resolution | 1 indexed batch read per evaluation batch |
| Evaluation evidence resolution | 1 workspace-scoped authority read per batch |
| Offline scorer test repetitions | 1,000 in 6.74 s including test-process setup |

Durable evaluation writes one idempotent run, one result per case, and one
terminal run update. Oversized reports are content-addressed through the
existing artifact plane; the report test verified a 66,573-byte report was
stored and disclosed with the same content hash. The capture row contains
references and hashes only, so storage grows with bounded trace/reference
metadata rather than prompt or context bytes.

## Remaining limitations

- No background evaluation scheduler or general historical retrieval import;
  operators still register datasets and use bounded recorded surfaces.
- The offline CLI intentionally trusts already-resolved evidence hashes; the
  authenticated HTTP path remains the authority-resolving path.
- Evaluation currently uses binary gold evidence labels and does not infer
  relevance or quality with a model.
- Metrics are durable Postgres snapshots, not a Prometheus/OTel exporter, and
  surface retention/compaction is not yet scheduled.
- The service remains a single-node Postgres control-plane substrate; external
  object storage, HA operations, backups/restore drills, and full admin UX are
  still roadmap work.

## Next task prompt

Task 18 — Build Fornix’s operator control CLI and workspace bootstrap surface.

Before coding:

1. Read the chats directory end to end and the latest foundation/completion
   notes through `docs/47-loop-17-completion.md`.
2. Study DeepSeek Harness CLI/TUI command boundaries and approval UX, Orloj
   CLI/API resource and evaluation commands, agentmemory diagnostics/cleanup
   commands, and the current Fornix RBAC, model, tool, agent, scheduler,
   artifact, observability, and retrieval-evaluation APIs.
3. Write a feature note covering command/resource invariants, workspace
   bootstrap, API-key provisioning and rotation, authorization, pagination,
   idempotency, redaction, offline behavior, failure semantics, licensing,
   cost impact, and acceptance tests.

Implement the smallest production-quality vertical slice:

- Add a deterministic `fornix` operator CLI with workspace bootstrap,
  identity/role/API-key lifecycle, health/readiness, task/run inspection,
  retrieval-surface listing, bounded evaluation execution, metrics, and
  artifact disclosure commands.
- Keep secrets write-only: never print API keys after creation, raw prompts,
  credentials, or arbitrary report text; support explicit JSON/table output.
- Use the existing authenticated HTTP/store contracts and RBAC; do not add a
  second authority or a new service.
- Make mutating commands idempotent and workspace-scoped, with bounded
  pagination, confirmation for destructive retention actions, and offline
  commands restricted to recorded data.
- Add deterministic shell/CI/smoke tests, race coverage where relevant,
  documentation, latency/SQL/storage measurements, and remaining limitations.
- Keep Postgres as the only authority and do not introduce brokers, Redis,
  NATS, LLM orchestration frameworks, or new infrastructure.
