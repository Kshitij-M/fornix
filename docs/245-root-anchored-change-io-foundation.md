# Root-anchored repository change I/O foundation

Status: implemented in the current working tree. The change package unit and
race suites, full-repository compile pass, vet, formatting, and documentation
checks pass. Postgres and deployment qualification remain separate gates.
Audience: Fornix contributors, operators, and security reviewers.

## Problem

Repository change validation currently resolves paths under a configured root,
checks preconditions, and later performs host-path mutations. A directory or
symlink can change between those operations. The proposal may then act on a
different path than the one inspected. This is especially dangerous when the
replacement points outside the configured repository mount.

## Scope

Keep the change vocabulary, approval flow, Postgres authority, packet hashes,
and public APIs unchanged. Refactor the filesystem boundary so one opened
`os.Root` is held across precondition checks, mutations, and post-state
hashing. All packet paths remain normalized relative paths; the executor must
not reconstruct an absolute target and pass it to `os.*` after opening the
root. Reads used by observation and recovery use the same rooted operations.

No migration, dependency, shell execution, runtime provider, or new
infrastructure is introduced.

## Invariants

1. The configured root is checked as an absolute, non-symlink directory before
   it is opened.
2. Every packet path is normalized and every filesystem operation is scoped
   through the same opened root handle for the lifetime of one apply or
   observation.
3. Parent components and existing targets are inspected through the root;
   observed symlinks and non-regular file targets fail closed.
4. A symlink swapped to an outside target after precondition checking cannot
   make a read, create, replace, delete, rename, chmod, or final hash escape
   the opened root.
5. Chmod is performed on an opened file handle. Temporary files are created
   exclusively beneath the root. A create publishes with a no-overwrite hard
   link; rename also uses link-then-unlink so a concurrent destination cannot
   be silently replaced. A crash between those rename operations can leave
   both names and must remain a recoverable, non-atomic external effect.
6. Source hash conflicts remain conflicts. Rooted I/O does not relax approval,
   task fencing, workspace authorization, budgets, or durable recovery state.
7. Filesystem and Postgres are still separate authorities. Multi-operation
   packets are not crash-atomic; recovery continues to verify observed hashes
   and preserve ambiguous effects for operator resolution.

## Reuse and cost

Reuse Go 1.25+ standard-library `os.Root` rather than adding a filesystem
dependency or new runtime. This adds one root handle per apply/observation and
uses rooted stat/read/mutation calls. It does not add a database round trip,
artifact write, cache, or external service. The expected latency change is
limited to rooted path operations and is not represented as a benchmark until
measured.

`os.Root` prevents path traversal outside its directory on supported desktop
platforms, but it is not a kernel sandbox. It does not prevent mount/bind-mount
traversal or arbitrary host access by another process, and some `Root` methods
have documented platform-specific race limitations. The opened root also
cannot prove that the caller selected the correct authorized mount; the mount
registry and service authorization remain responsible for that decision.

## Acceptance tests

- Existing create/replace/delete/rename/chmod and packet-hash tests remain
  green.
- A resolver-triggered directory-to-outside-symlink swap after precondition
  checking cannot read or mutate the outside target.
- A symlink present before the operation continues to fail closed.
- Rename, delete, chmod, create, and replace retain their expected hashes and
  modes; absent source paths are not included in the resulting-tree hash.
- Stale content still returns `ErrSourceConflict` without mutation.
- Cancellation, temp-file cleanup, and post-state mismatch remain bounded and
  recoverable.
- Full Go formatting, change package tests/race tests, repository compile,
  vet, documentation checks, and `git diff --check` pass.
- Database-backed change application and production filesystem containment
  remain unqualified without the supported Postgres and deployment topology.
