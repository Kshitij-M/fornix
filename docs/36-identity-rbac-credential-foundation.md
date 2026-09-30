# Workspace identity, RBAC, and credential-reference foundation

Status: implementation note for Task 12 / Loop 12.

## Scope

This slice replaces Fornix's shared-bearer-key assumption with a durable,
workspace-scoped API-key boundary. PostgreSQL remains the authority for
identities, role bindings, API-key lifecycle, credential references, and
authorization decisions. The development compatibility path is retained only
when `FORNIX_AUTH_MODE=development` is explicitly configured.

The authenticated principal is copied into the existing typed `ActorRef` at
every request boundary. Request bodies and headers cannot select a different
workspace or actor after authentication. The service never persists or emits
the API-key secret or a resolved provider credential.

## Reference reuse and licensing

The design was informed by the local reference repositories:

- DeepSeek Harness: the provider-neutral credential-reference seam, per-use
  resolution, scoped registration, and permission/approval seams.
- Orloj: explicit auth modes, durable users/tokens, role admission, model/tool
  authorization, secret references, and fail-closed policy checks.
- agentmemory: timing-safe secret comparison, explicit secret configuration,
  agent-scope isolation, and diagnostics that do not expose secret material.

Fornix independently reimplements these behaviors in Go and copies no source.
DeepSeek Harness is MIT; Orloj and agentmemory are Apache-2.0. Fornix remains
MIT and no third-party source notice is required for this independent slice.
Kronaxis Fabric remains excluded because its BSL-1.1 license is incompatible
with copying its implementation into Fornix.

## Invariants

- A non-development principal belongs to exactly one workspace. Every
  workspace-bearing request must match it; mismatches fail closed with 403.
- An API key identifies one identity and one workspace. Authentication checks
  key status, identity status, expiry, and role-binding expiry against the
  PostgreSQL clock.
- API keys store only a key identifier, display prefix, and SHA-256 digest of
  a high-entropy secret. Verification uses constant-time digest comparison.
- Rotation creates a new key and revokes the old key in one transaction.
  Revocation and expiry are terminal for authentication; old credentials are
  never silently reactivated.
- Roles and permissions are normalized and sorted before authorization.
  `admin:*` is an explicit wildcard; unknown permissions are denied.
- Authorization is deny-by-default. Each protected HTTP route maps to one
  deterministic capability. The audit record stores the decision and reason,
  never the bearer token.
- Development mode uses the existing shared key only as an explicit local
  compatibility bypass. It is rejected when `FORNIX_ENV=production`.
- Credential references identify provider-managed values (for example an
  environment variable or external secret name). The reference and lifecycle
  metadata are durable; the value is not.
- Existing append-only events remain authoritative. Actor propagation changes
  attribution only; it does not rewrite event history or expose credentials.

## Schema changes

Migration `018_identity_rbac_credentials.sql` adds:

- `identities`, workspace-scoped principals;
- `roles` and `identity_role_bindings`, with normalized permission arrays;
- `api_keys`, with key digest, expiry, revocation, and rotation lineage;
- `credential_references`, containing non-secret provider references and
  rotation/revocation metadata; and
- append-only `authorization_audit` records for allow and deny decisions.

Because migrations are immutable after application, migration
`019_authorization_audit_identity_scope.sql` hardens the audit idempotency
constraint to include `identity_id` and `api_key_id`. This prevents an allowed
decision from being reused by a different principal that submits the same
request identifier. Migration
`078_authorization_audit_decision_fingerprint.sql` further makes the unique
key include the decision fingerprint. The fingerprint binds the current
allow/deny outcome, reason, actor/key, capability/resource, and normalized
HTTP method/path. Repeated identical decisions deduplicate; changed decisions
are appended as separate immutable audit rows instead of colliding with or
replaying a prior allow. Migration 078 changes the conflict target expected
by the updated application, so an older binary must not run against the
migrated schema. Deployments need a coordinated code/schema rollout until
backward-compatible rolling-upgrade behavior is separately qualified.

`Authorize` reloads API-key status, identity status, and the effective,
unexpired role permissions inside the same workspace-scoped transaction that
records the decision. It does not trust the permission slice cached in an
already-authenticated `Principal`. Row locks provide a clear database
serialization point against concurrent role/key revocation: a revocation
committed before authorization is observed; a revocation that begins after
the decision commits does not retroactively cancel an already-authorized
in-flight request. Effectful handlers must still enforce their own task,
operation, and resource fences. Authorization audit idempotency is not a
substitute for effect/request idempotency.

All identity, role, key, credential, and audit indexes include workspace scope
where applicable. The API-key uniqueness boundary is `(workspace_id, key_id)`
and the credential uniqueness boundary is `(workspace_id, provider, name,
version)`.

## Failure and crash semantics

Authentication and authorization are synchronous Postgres reads. An
unavailable database fails closed rather than falling back to a cached
principal. Each authorization call uses current durable credential and role
state. A duplicate with an unchanged decision fingerprint receives the same
current result and one audit effect; a changed role/key state or route produces
a distinct append-only audit row, and an earlier allow cannot suppress a
current denial. A request identifier is not an idempotency key for the
operation itself. A crash during key rotation leaves the old key unchanged or
commits both the new key and revocation; a partial rotation is not visible.

Provider credentials remain configuration/provider-owned. A process crash at a
remote model boundary retains the existing at-least-once limitation; this
slice adds authorization and reference lifecycle but does not claim exactly
once external execution.

## Cost and storage budget

- Authentication performs an indexed credential lookup and bounded role read;
  authorization then reloads the credential and effective roles in its
  workspace transaction and records/reads the decision fingerprint. This
  additional durable read prevents stale in-memory grants from authorizing a
  request after a committed role or key revocation. The audit stores one row
  per unique decision fingerprint, including a changed decision for a reused
  request ID.
- No model, embedding, broker, cache, or new service is introduced.
- Identity rows are small metadata records. Audit records are bounded by
  request ID, path, permission, resource, and a compact actor reference; raw
  bearer tokens and credential values are excluded.
- Target overhead is under 3 ms p95 in-process for development auth and under
  8 ms p95 for a warm Postgres auth+authorization decision, excluding network
  queueing. Actual timings are reported in the completion note.

## Acceptance tests

- Fresh and existing databases apply migration 018 and preserve checksums.
- API-key creation, constant-time verification, expiry, revocation, and
  transactional rotation work without secret leakage.
- Concurrent authentication and rotation produce one valid lifecycle result.
- A principal cannot read or write another workspace, even when body, query,
  or header workspace values disagree.
- Role permission evaluation is deterministic, sorted, deny-by-default, and
  resistant to privilege escalation.
- Model, tool, task, retrieval, evidence, agent-run, and scheduler capabilities
  reject unauthorized principals.
- The authenticated actor is preserved in durable events and audit records;
  spoofed body/header actors are ignored.
- Repeated request identities produce one authorization audit effect, scoped to
  the authenticated identity, key, and exact decision fingerprint.
- A stale `Principal` cannot reuse permissions after role removal, identity
  disablement, key revocation, or key expiry; the current request is denied and
  the changed decision is appended to the audit trail.
- Reusing a request identity after a route or effective-decision change never
  replays the earlier allow; authorization evaluates current state and records
  the new outcome.
- Database failure, stale/revoked credentials, and missing permissions fail
  closed without exposing secrets.
- Existing Go tests, race checks, builds, CI, and all smokes remain green.

The Postgres-backed concurrent-retry and authorization-replay tests—including
direct stale-principal checks after role/key revocation, changed-decision audit
fingerprints, and an HTTP middleware check between requests—are run by
`make qualification-authorization-audit-postgres` against an explicitly
configured disposable database; CI invokes the same target.

## Remaining limitations

This slice intentionally does not add OAuth/SSO, password authentication,
external KMS/secret-manager integration, a public identity-administration API,
Postgres row-level-security policies, or a general tenant-management control
plane. Operators create the initial workspace identity/key through the typed
store API or a future administrative CLI. Migration 047 now supplies durable
workspace-scoped lease and revocation authority, but an external KMS/secret
manager still owns provider secret bytes in hosted deployments; the local
profile/environment resolver is development-only.
