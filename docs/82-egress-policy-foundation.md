# Universal destination and egress policy foundation

Status: implementation note for the next Issue [#40](https://github.com/Kshitij-M/fornix/issues/40) qualification slice.

Fornix is intended to control work against many production systems, not only
repositories. Every connector therefore needs one common, inspectable
destination boundary before its adapter-specific transport logic runs. This
slice adds a small reusable policy value: allowed schemes, hosts, paths,
redirect budget, and the private-network decision. It is an admission
contract, not a network proxy and not a replacement for DNS-rebinding-aware
transport controls.

## Problem and scope

Without a common destination policy, each connector can accidentally interpret
"allowed destination" differently. That creates a confused-deputy risk:
operation policy may approve one target while an adapter follows a different
host, path, or redirect. The shared policy makes the authority visible and
hashable. The adapter still owns checks that require I/O context, including DNS
resolution, private-address rejection, proxy behavior, SQL read-only rules,
and provider-specific verification.

This is intentionally a narrow vertical slice:

- `connector.DestinationPolicy` normalizes and hashes bounded allowlists;
- URL admission rejects credentials, unsupported schemes, unapproved hosts,
  and unapproved paths;
- the HTTP connector applies the policy at initial URL construction and
  redirect handling;
- existing HTTP host/path, redirect, private-network, timeout, and byte limits
  remain active as defense in depth;
- no database migration or new network service is introduced.

## Invariants

1. A policy must contain at least one supported scheme and one host. Empty
   allowlists fail closed rather than becoming "allow all".
2. Policy strings are trimmed, lower-cased, deduplicated, and sorted before
   comparison or hashing. Equivalent declarations have the same stable hash.
3. URL user information is never accepted. Credentials must be supplied by a
   scoped credential lease at the outbound boundary, never in a destination
   URL.
4. Host admission is exact or a deliberate subdomain match in the shared
   value. The HTTP adapter retains its stricter configured host comparison and
   port handling.
5. Path admission is prefix-bounded and normalized. A path such as `/admin`
   cannot satisfy a `/api` policy, and path traversal is rejected by the
   adapter before transport.
6. Redirects are disabled by default. When enabled by a binding, both the
   shared policy and the adapter-specific policy are applied to every hop and
   the redirect count remains bounded.
7. `AllowPrivateNetworks` is descriptive at this value layer. The actual HTTP
   resolver/dialer remains responsible for rejecting private, loopback,
   link-local, multicast, and unspecified addresses when the flag is false.
8. A policy hash contains only normalized policy values. It contains no
   credentials, prompts, request bodies, response content, or arbitrary user
   text.

## Authority, lifecycle, and failure behavior

Postgres remains the authority for workspace identity, operation admission,
approval, leases, effects, and audit history. This policy is a deterministic
in-process input to connector admission and must be persisted or referenced by
the surrounding operation record when a connector executes. A policy change
must be an explicit control-plane change; it must not be inferred from a URL
received in an operation payload.

The shared policy is immutable by convention after normalization. An adapter
should construct it from an authenticated workspace binding, retain its stable
hash with the execution evidence, and fail closed on normalization or URL
authorization errors. Replay and evaluation must use recorded policy hashes;
they must not contact the destination or resolve DNS.

## Schema and API impact

There is no migration in this slice. `DestinationPolicy` is a typed internal
contract used by connector adapters. Future durable execution records should
record its stable hash alongside connector identity, capability definition
hash, workspace, actor, operation, and effect attempt. Such a migration is
deferred until a generic executor consumes connectors through this boundary.

The HTTP adapter applies the policy in three locations:

1. after constructing a request URL from a binding and typed relative path;
2. before accepting a redirect target;
3. alongside its existing exact host/path and transport-level private-network
   checks.

## Reuse and licensing

The design reuses Fornix's existing connector registry, capability definition,
HTTP binding, approval, credential lease, and operation admission seams. Orloj
host/resource admission, DeepSeek Harness capability boundaries, and
agentmemory lease/recovery patterns informed the shape; no reference source was
copied. Kronaxis Fabric remains excluded because its BSL 1.1 license is not
compatible with Fornix's MIT distribution. The new code is original Fornix
code and remains covered by the repository license.

## Cost and performance budget

Admission is bounded in the number of configured schemes, hosts, path prefixes,
and redirect hops. Normalization and stable hashing are in-process and add no
SQL or network round trip. URL checks are linear in the bounded allowlists. The
HTTP transport still pays the existing DNS and dial cost only after policy
admission succeeds. Implementations must measure policy-admission latency and
include the policy hash—not a URL, credential, prompt, or request ID—in
observability dimensions.

## Acceptance tests

- supported schemes, configured hosts, subdomains, and path prefixes are
  admitted deterministically;
- unsupported schemes, unknown hosts, unapproved paths, URL credentials, and
  malformed policies fail closed;
- policy normalization is order-independent and stable hashes match for
  equivalent declarations;
- HTTP request construction applies the shared policy without weakening its
  existing SSRF, private-network, redirect, timeout, and byte controls;
- redirect targets are re-authorized and bounded;
- policy diagnostics contain no secret material or raw payloads;
- existing connector, operation, workflow, race, smoke, build, and
  documentation checks remain green.

## Remaining qualification gates

This slice is not a network egress proxy, DNS firewall, signed policy catalog,
external secret manager, tenant database isolation policy, quota system, or
capacity qualification. Those remain Issue #40 work. A future generic
executor must make the policy hash a durable part of effect admission and must
require every network-capable adapter to implement the same contract.
