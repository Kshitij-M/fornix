# Pull request

## User problem and product outcome

<!-- What repository-work problem does this solve? Link the issue or discussion with `Fixes #123` or `Refs #123`. Explain whether the change improves admission, execution, proof, or improvement of a Verified Change Packet. -->

## Summary

<!-- Summarize the implementation and the user-visible result. -->

## Contract and scope

- [ ] This change has a focused scope and preserves the documented authority boundary.
- [ ] Workspace, actor, task, session, and credential boundaries are explicit where relevant.
- [ ] Idempotency, retry, timeout, cancellation, crash, and stale-owner behavior are covered where relevant.
- [ ] API, schema, migration, compatibility, cost, security, and licensing impact are described below.

## Verification

- [ ] `make fmt-check`
- [ ] `make test`
- [ ] `make test-race` (or explain why it is not applicable)
- [ ] `make vet`
- [ ] `make python-check`
- [ ] `make docs-check`
- [ ] `make build`
- [ ] Relevant Postgres or HTTP smoke tests

## Impact and limitations

Describe user-visible behavior, operational impact, security implications,
known limitations, and follow-up work. Never include credentials, private
repository content, raw prompts, or unredacted provider/tool output.
