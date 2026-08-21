# Loop 12 completion: workspace identity, RBAC, and credential references

Status: complete.

Task 12 adds the first durable security boundary for Fornix. PostgreSQL now
authoritatively stores workspace-scoped identities, roles, role bindings, API
key lifecycle, non-secret credential references, and append-only authorization
decisions. Protected HTTP requests authenticate to one workspace, receive a
typed principal, and use a deterministic capability mapping before reaching a
handler.

## Delivered

- Added typed `Principal`, `Identity`, `Role`, `Permission`, `APIKey`,
  `CredentialRef`, `AuthorizationDecision`, and `AuditActor` contracts.
- Added migration `018_identity_rbac_credentials.sql` for identity/RBAC/key/
  credential/audit records.
- Added immutable follow-up migration
  `019_authorization_audit_identity_scope.sql`, which scopes authorization
  idempotency to workspace, request, identity, API key, permission, and
  resource. This closes the cross-principal duplicate-decision escalation
  case without changing migration 018 after it was applied.
- Added `AuthStore` for identity creation, role grants, constant-time API-key
  verification, expiry/revocation/transactional rotation, credential-ref
  rotation/revocation, and durable authorization audit.
- Replaced the implicit shared-bearer assumption in the running HTTP path with
  workspace mode. The old key remains only under explicit
  `FORNIX_AUTH_MODE=development`; that mode is rejected for production.
- Added fail-closed request workspace consistency checks across header, query,
  and JSON body, and authenticated actor propagation into model, tool,
  approval, agent-run, and task mutations/events.
- Added `FORNIX_WORKER_ENABLED`, defaulting to true for service operation and
  set false in CI integration startup so standalone scheduler tests do not
  compete with a live worker against the same Postgres database.
- Added identity server/store tests, an identity smoke, CI coverage, Makefile
  commands, development guidance, and architecture qualification updates.

## Invariants and failure behavior

Non-development API keys bind to exactly one workspace and active identity.
Expired, revoked, rotated, or malformed keys fail authentication. Secrets are
never placed in JSON contracts, events, authorization records, logs, evidence,
or error messages; only a SHA-256 digest and display prefix are durable.

RBAC is normalized, sorted, deduplicated, and deny-by-default. Unknown
permissions are rejected. A development principal is explicit and local-only.
Authorization database failure fails closed. Authorization audit is append-only
and duplicate-safe per authenticated principal. The authenticated actor wins
over spoofable request-body or header actor fields.

Credential references contain only provider/name/reference metadata and
versioned lifecycle state. The provider value is not resolved or persisted by
this slice; the existing process-level resolver remains an explicit limitation.

## Validation

Fresh and existing migration checks passed. The fresh check created a new
Postgres database, applied the complete migration set through 019, executed
the API-key/RBAC lifecycle test, and dropped only that temporary database.

The following checks passed after the final hardening change:

- `go test ./... -count=1`
- `go test -race ./... -count=1`
- `go vet ./...`
- `go build -trimpath -o /tmp/fornix ./cmd/fornix`
- `go build -trimpath -o /tmp/fornix-watcher ./cmd/fornix-watcher`
- Python compile checks and `git diff --check`
- Complete v0.10–v0.21 smoke chain, including identity smoke

The focused tests cover API-key lifecycle, expiry, revocation, rotation,
credential references, RBAC allow/deny, duplicate authorization concurrency,
cross-identity idempotency isolation, request workspace mismatch, actor
propagation, audit persistence, and secret serialization safety.

## Measurements

On the local Docker Postgres development database:

- Warm AuthStore authorization: p50 `217.333µs`, p95 `373.25µs`, max `4.968ms`
  across 24 unique decisions. This includes the audit insert and read-back.
- Identity authorization work is bounded to indexed key/role reads plus one
  append-only audit decision and one idempotent read-back; no model, embedding,
  broker, or cache work is introduced.
- After the full test/smoke run, the append-only audit held 7,879 records and
  occupied 4,595,712 bytes including indexes. Empty/small metadata relations
  measured approximately 57–98KiB each in this PostgreSQL instance. Audit
  storage therefore grows linearly with unique authorization decisions and
  needs retention/partitioning before high-volume production use.

## Reuse and licensing

The implementation reuses design lessons, not source code, from DeepSeek
Harness (MIT), Orloj (Apache-2.0), and agentmemory (Apache-2.0): explicit
credential references, fail-closed permissions, timing-safe verification,
scoped admission, and non-secret diagnostics. Kronaxis Fabric's BSL-1.1 code
was not copied. Fornix remains MIT-licensed.

## Remaining limitations

There is no public identity-administration API/CLI, OAuth/SSO, external KMS or
secret-manager resolver, Postgres RLS policy, automated rotation controller,
tenant provisioning workflow, or audit retention/partition policy. The
OpenAI/Ollama credential path still resolves process-level references until a
workspace-aware secret provider is added. These are explicit next-stage gaps,
not implicit security fallbacks.
