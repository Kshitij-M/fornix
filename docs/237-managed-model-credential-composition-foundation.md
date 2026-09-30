# Managed model credential composition

Status: feature note for Loop 106. This closes a server-composition gap; it is
not evidence that a deployment's secret manager, network boundary, or live
provider has been qualified.

## Problem and decision

OpenAI-compatible chat is configured as opt-in, and the provider already has a
short-lived, fenced `CredentialLease` integration. However, server composition
only offers the process-environment compatibility resolver. Production config
rejects OpenAI enablement while saying an injected resolver is required, but
`ServerDependencies` has no model credential injection seam and the CLI only
constructs its credential manager when federation polling is enabled. This
leaves the provider lease implementation unreachable in the ordinary
production server path.

Add explicit model credential-manager and lease-resolver dependencies. Reuse
the current Postgres `CredentialLeaseStore`, `ManagedSecretResolver`, and
bounded HTTP secret-manager protocol. Production model calls require a
workspace-scoped logical `CredentialRef` and a lease authority; they never
fall back to API-key environment variables. Development retains the existing
explicit environment-key path. Close adjacent reviewed egress gaps in the
same slice: authority-bound HTTP execution must not fall back to the legacy
resolver, revocation must be checked after request construction immediately
before `Do`, partial secret results must be cleared on errors, and request
objects must drop authorization headers as soon as transport returns. Make
task-lease lock acquisition deterministic in the same order as task mutation.
There is no new database schema or service.

## Invariants

1. In production, an enabled OpenAI-compatible provider has a syntactically
   valid logical credential reference, HTTPS provider and credential-manager
   endpoints, and a server-injected `LeaseResolver`. Missing authority or
   plaintext HTTP configuration fails startup closed.
2. The production path never reads `FORNIX_OPENAI_API_KEY` or another
   environment variable as model credential material. A secret manager is
   contacted only when a model request is admitted.
3. Model egress acquires a short-lived, workspace/purpose-scoped lease, checks
   scope, expiry, positive fence and revocation epoch, revalidates against
   current authority immediately before the outbound `Do` call after request
   construction, clears owned secret byte copies, drops request authorization
   headers after transport completion, and releases the lease on every exit
   path. HTTP connector and federation requests use the same final-boundary
   check. A failed last check means the transport receives no request.
4. The logical reference is metadata, not a secret. Secret bytes do not enter
   configuration reports, model requests, logs, events, artifacts, errors, or
   evidence. Resolver results that contain bytes alongside an error are
   explicitly cleared. The managed HTTP response buffer and decoded base64
   bytes are cleared after parsing; immutable Go strings created for
   authorization values remain a documented limitation.
5. Existing local CLI behavior remains explicit: the fake provider is the
   default, and opted-in development OpenAI may resolve the key from the
   invoking process environment. That compatibility mode is unavailable in
   production. `ExecuteWithAuthority` cannot use the HTTP adapter's legacy
   direct resolver even if one is configured.
6. Managed credentials remain workspace-scoped and use the exact durable
   credential reference lifecycle already enforced by `CredentialLeaseStore`.
   Model calls do not gain broader database or connector authority.

## Configuration and composition

- `FORNIX_OPENAI_ENABLED=true` remains opt-in.
- In development, default `FORNIX_OPENAI_CREDENTIAL_REF=FORNIX_OPENAI_API_KEY`
  continues to name the environment variable used by the development
  resolver.
- In production, `FORNIX_OPENAI_CREDENTIAL_REF` must be explicitly set to a
  valid logical reference (for example `openai/production-api-key`). A raw
  provider key in the environment is not consumed.
- The binary constructs the existing HTTP secret-manager adapter whenever an
  enabled feature (model chat or federation polling) needs it. Model-only
  deployments no longer depend on federation being enabled.
- `ServerDependencies` supports an injected model `LeaseResolver` for
  workload-identity, mTLS, or deployment-owned authorities, or an injected
  `SecretManager` that Fornix wraps with the existing Postgres lease store.
- If model and federation paths are enabled with a shared manager, they may
  share the manager implementation but retain purpose-separated leases.

No migration is required. Credential reference creation, rotation, revocation,
audit, and actor authorization remain owned by the existing identity and
credential lifecycle. The deployment must provision the referenced credential
before its first paid model request.

## Reuse, licensing, and cost

Reuse `credentials.SecretManager`, `ManagedSecretResolver`,
`CredentialLeaseStore`, `LeaseResolver`, the model provider's existing lease
path, and controlled outbound HTTP transport. No external repository source is
copied; no Kronaxis source is used. The repository remains MIT-licensed.

There is no schema/storage growth. Each model request adds the existing
bounded lease acquisition, authority revalidation, and release database work
already implemented by the lease store. Secret-manager network calls and
provider usage remain subject to existing timeout, token, time, and cost
budgets. Measure startup, lease validation, and model-request latency in the
qualified environment; do not infer production latency from unit tests.

## Failure and recovery

- Invalid/missing logical reference, missing lease authority, absent durable
  credential registration, expired/revoked/stale lease, manager timeout, or
  provider authentication failure fails closed with redacted errors.
- Startup validates composition and reference syntax, not secret availability;
  it does not fetch a key or make a provider request.
- Rotation/revocation uses existing credential-reference events and lease
  epochs. A lease rejected immediately before egress causes no provider call.
- The external manager is not queried by the final local `ValidateLease` check.
  Therefore it must keep a leased source version valid for the lease TTL. For
  urgent rotation/revocation, operators must first update or revoke the
  corresponding Fornix credential reference/epoch; manager-only rotation is
  not immediately visible to an already-issued lease. Deployments must qualify
  this synchronization contract with their secret manager.
- A network timeout after request transmission retains existing at-least-once
  model-call semantics; this change does not claim exactly-once provider work.

## Acceptance tests

- Production configuration accepts enabled OpenAI only with an explicit valid
  logical reference and does not require or consume an API-key environment
  variable; production rejects non-HTTPS provider and credential-manager URLs.
- Development configuration continues to require the explicitly enabled
  environment key and preserves current defaults.
- Production server composition fails closed without a model lease authority;
  accepts injected resolver/manager authorities without reading key material.
- Model-only CLI composition constructs the configured manager even when
  federation polling is disabled.
- A model request validates current lease authority immediately before HTTP
  egress after request construction; revoked/stale/missing authority sends no
  request. The same is true for HTTP connector and federation polling.
- An authority-bound HTTP request configured with only the compatibility
  resolver fails before that resolver or the transport is called.
- Task authority validation locks the task row before its lease row, matching
  renewal/completion lock order and avoiding the identified inversion.
- Dispatcher duplicates in any non-reserved state either replay the one
  immutable requested result or return recovery-required; they never return
  false success with a missing result or report an in-progress dispatch as
  complete. Concurrent tests force overlap both at the invocation boundary and
  between observing a reserved effect and committing dispatch intent.
- Managed-response decoding clears secret buffers even when a later metadata
  field is malformed; lease-release cleanup uses a fresh bounded context
  (resolvers must honor Go context deadlines).
- Request objects no longer retain authorization headers after `Do` returns,
  and managed/store resolver errors clear any secret bytes returned alongside
  the error.
- Lease bytes are cleared and released on success, validation failure,
  transport failure, cancellation, and provider failure; errors and durable
  metadata contain no secret.
- Verification results and environment-specific omissions for this slice are
  recorded in `docs/238-loop-106-completion.md`; do not infer that skipped
  PostgreSQL or loopback integration tests passed locally.
- Disposable PostgreSQL tests qualify lease acquisition, revocation, and
  workspace isolation; live manager/provider qualification remains a
  deployment-owned release gate.

## Explicit limits

This composes existing authorities; it does not provision credentials, create
an operator UI for a third-party KMS, provide workload identity itself, prove
secret zeroization inside Go's immutable strings, or validate a hosted network
boundary. Production still requires signed authority catalogs, credential
manager authentication, role-separated PostgreSQL/RLS, and measured live
provider evidence.
