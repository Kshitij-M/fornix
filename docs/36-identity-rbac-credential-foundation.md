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
request identifier.

All identity, role, key, credential, and audit indexes include workspace scope
where applicable. The API-key uniqueness boundary is `(workspace_id, key_id)`
and the credential uniqueness boundary is `(workspace_id, provider, name,
version)`.

## Failure and crash semantics

Authentication and authorization are synchronous Postgres reads. An
unavailable database fails closed rather than falling back to a cached
principal. A duplicated request with the same valid key and capability has the
same authorization result; audit insertion is idempotent for a supplied
request identity and capability/resource pair. A crash during key rotation
leaves the old key unchanged or commits both the new key and revocation; a
partial rotation is not visible.

Provider credentials remain configuration/provider-owned. A process crash at a
remote model boundary retains the existing at-least-once limitation; this
slice adds authorization and reference lifecycle but does not claim exactly
once external execution.

## Cost and storage budget

- One indexed key lookup and one bounded role/permission read per authenticated
  request. The authorization audit is one append-only row per unique request
  capability/resource decision.
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
  the authenticated identity and key.
- Database failure, stale/revoked credentials, and missing permissions fail
  closed without exposing secrets.
- Existing Go tests, race checks, builds, CI, and all smokes remain green.

## Remaining limitations

This slice intentionally does not add OAuth/SSO, password authentication,
external KMS/secret-manager integration, a public identity-administration API,
Postgres row-level-security policies, or a general tenant-management control
plane. Operators create the initial workspace identity/key through the typed
store API or a future administrative CLI. The configured OpenAI/Ollama
provider values remain process-level references until that credential resolver
is moved behind a workspace-aware secret manager.
