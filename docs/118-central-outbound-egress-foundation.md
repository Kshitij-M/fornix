# Central outbound policy and egress foundation

Status: implemented in the universal production-qualification workstream.

Fornix controls work against arbitrary production systems through typed,
workspace-scoped adapters. That makes outbound network behavior a universal
control-plane concern, not an HTTP-connector implementation detail. This
slice moves destination, DNS, redirect, proxy, timeout, and byte-budget
enforcement to one reusable HTTP boundary used by both the generic HTTP
connector and model providers.

## Problem and scope

Previously, the HTTP connector had DNS/private-network and redirect controls,
while model providers had separate URL validation and could follow a supplied
client's transport behavior. That split made the universal contract weaker
than its strongest adapter. A new network-capable adapter must not be able to
turn a typed destination policy into an uncontrolled dial, redirect, or body
read.

This slice provides:

- `connector.EgressPolicy`, a secret-free, hashable combination of destination
  policy and hard request/response/time budgets;
- `connector.NewEgressClient`, which clones a caller client and installs a
  controlled transport rather than mutating caller-owned state;
- DNS resolution through an injectable resolver, with private, loopback,
  link-local, multicast, and unspecified address rejection unless the
  workspace policy explicitly allows private networks;
- proxy disablement by default and explicit opt-in for deployments that own
  the associated risk;
- redirect re-authorization and hop budgets;
- request and response body caps enforced by the transport itself;
- adoption by the HTTP connector, OpenAI-compatible provider, and Ollama
  provider, including the existing embedding path.

It is an in-process boundary. It is not a network proxy, service mesh, DNS
firewall, or guarantee that an explicitly enabled proxy is trustworthy.

## Invariants

1. A client cannot execute without a normalized destination policy, positive
   request/response byte budgets, and a bounded timeout.
2. Every request URL is authorized again at `RoundTrip`, after construction
   and before dialing. URL credentials, unsupported schemes, unknown hosts,
   and paths outside the configured prefixes fail closed.
3. Redirects are denied by default. Enabled redirects remain bounded and every
   target is re-authorized against the same policy.
4. A transport with an uninspectable `RoundTripper` is rejected. An
   inspectable `*http.Transport` is cloned, its TLS dial bypass is removed,
   and its dial path is replaced with the policy-controlled resolver/dialer.
5. Proxies are disabled unless `AllowProxy` is explicit. When a proxy is
   enabled, deployment policy owns the fact that destination-IP inspection is
   delegated to that proxy.
6. The transport never returns more than the configured response bytes and
   returns a typed error when another byte is available. Unknown request-body
   lengths are bounded by a read wrapper; declared oversized requests are
   rejected before network I/O.
7. Provider and connector credentials remain request-local. The egress policy,
   errors, tests, and evidence contain no credential, prompt, or body content.
8. Lease-backed HTTP credentials must pass both local scope validation and an
   optional durable `LeaseValidator` before use. Revocation therefore fails
   closed at the connector boundary as well as the model boundary.
9. Replay and evaluation use recorded policy hashes and never construct an
   egress client or contact a destination.

## Authority and failure behavior

Postgres remains authoritative for workspace identity, operation admission,
approvals, capability trust, credential lease fences, external-effect
reservations, and audit history. `EgressPolicy` is a deterministic execution
input. Its stable hash should be recorded alongside the operation, capability,
actor, lease, and effect attempt by the surrounding durable operation path.

An invalid policy, stale/revoked lease, private DNS result, unauthorized
redirect, uncontrolled transport, timeout, or byte overflow is a terminal
boundary failure for that attempt. The adapter may classify the failure for
its bounded retry policy, but it must not silently weaken the policy or retry
after an external effect has started.

## Reuse and licensing

The implementation reuses Fornix's existing `DestinationPolicy`, connector
registry, capability admission, credential lease interfaces, model provider
registry, and HTTP/model byte budgets. The design was informed by the
reference repositories' explicit provider, capability, retry, and lease
boundaries; no reference source was copied. Kronaxis Fabric remains excluded
because its BSL 1.1 license is incompatible with Fornix's MIT distribution.

## Cost and performance budget

Policy normalization and URL authorization are bounded in-process work. Each
new hostname connection may perform one resolver lookup and one or more
bounded address dials. No SQL query, migration, broker, proxy service, or
additional container is introduced. Response/request wrappers add a bounded
counter and no unbounded buffering. Production qualification must measure
DNS latency, connection latency, redirect rejection, policy admission, and
the effect of resolver caching under the intended deployment topology.

## Acceptance tests

- equivalent policies have the same stable hash and malformed policies fail
  closed;
- oversized declared and streaming request bodies are rejected;
- response bodies never exceed the configured limit and report a typed error;
- DNS answers containing private addresses are rejected when private networks
  are disabled;
- redirects are disabled by default, bounded when enabled, and cannot escape
  the allowed host/path policy;
- uninspectable transports are rejected and proxy use is explicit;
- OpenAI-compatible chat, streaming, Ollama chat, and Ollama embeddings use
  the shared boundary;
- lease revocation is checked before a connector or model request;
- credentials are absent from errors, evidence, metrics, and durable output;
- existing unit, Postgres integration, race, build, CI, smoke, and replay
  checks remain green.

## Remaining qualification gates

This slice does not provide a central egress proxy, enterprise DNS policy,
external secret manager, durable signed policy catalog, managed certificate
rotation, network-level tenant isolation, or production load/soak evidence.
Those remain explicit Issue #38 production gates. `AllowProxy` must stay off
for deployments that require Fornix itself to prove destination IP safety.
