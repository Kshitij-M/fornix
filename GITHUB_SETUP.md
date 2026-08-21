# GitHub maintainer setup

This file is the checklist for turning the public `Kshitij-M/fornix`
repository into the community control plane for Fornix's product vision:
verifiable AI work infrastructure for long-running repository operations. The
repository contains the repeatable parts as code; the repository-owner
settings below still need to be enabled once in GitHub.

## Repository features

- Keep the repository public with `main` as the default branch.
- Enable Issues, Discussions, Projects, the dependency graph, Dependabot
  alerts, Dependabot security updates, secret scanning/push protection, and
  private vulnerability reporting.
- Use Discussions for questions and design exploration. Keep Issues focused on
  actionable bugs and feature proposals.
- Set the repository description to: “Open-source infrastructure for safe,
  bounded, evidence-backed AI work on important repositories.”
- Use discoverable topics that describe the product rather than only its
  implementation: `verifiable-ai-work`, `repository-automation`,
  `agent-reliability`, `ai-agents`, `ai-harness`, `golang`, `postgresql`, and
  `developer-tools`.
- Keep the Wiki disabled until it has a clear owner; the versioned `docs/`
  tree should remain the canonical technical record.

The issue forms, pull request template, support guidance, code owners,
Dependabot schedule, CodeQL workflow, dependency review, Scorecard scan, and
release workflows are already versioned in `.github/`.

## Protect `main` with a ruleset

Create a ruleset that targets the `main` branch and initially applies to
everyone after the first successful run. For the current solo-maintainer
phase, either allow an administrator bypass for the required approval or add a
second maintainer before enforcing that requirement; keep force-push and
deletion protection enforced immediately. Enable:

- Require a pull request before merging.
- Require at least one approving review and require review from code owners.
- Dismiss stale approvals when new commits are pushed.
- Require approval of the most recent push and resolve all review threads.
- Require these checks once GitHub has recorded their exact names:
  `Go checks`, `Python helper checks`, `Postgres and HTTP smoke`,
  `Vulnerability scan`, `Dependency review`, and the two CodeQL analyses.
- Block force pushes and branch deletion.
- Require a linear history if the team is comfortable with squash or rebase
  merges; keep merge-commit history available if preserving branch topology is
  more important.

Do not make release or container workflows required checks for `main`; they are
tag workflows and should only publish after an explicit maintainer action.

## Actions and security settings

- Set the default `GITHUB_TOKEN` permission to read-only.
- Require approval before workflows from fork pull requests can run when the
  repository setting offers that control.
- Allow only actions from GitHub and verified creators unless a new action has
  been reviewed and is pinned or maintained through Dependabot.
- Keep untrusted code on `pull_request`, not `pull_request_target`.
- Use environment protection rules if a future deployment workflow is added.
  Prefer short-lived cloud credentials through GitHub OIDC over long-lived
  cloud secrets.
- Review CodeQL and Scorecard alerts weekly. Treat a workflow permission change
  as a security-sensitive pull request.

## Labels, milestones, and projects

Keep the default labels and add a small, stable set:

- `documentation`, `dependencies`, `security`, `performance`,
  `breaking-change`, `good first issue`, and `help wanted`.
- Use milestones for alpha slices and release trains, not as a replacement for
  the architecture/completion records in `docs/`.
- Create a project with views for `Inbox`, `Ready`, `In progress`, `Blocked`,
  and `Done`; automatically add issues and pull requests, then use milestones
  for release planning.

## Release and package operations

- Use annotated `vMAJOR.MINOR.PATCH` or prerelease tags such as
  `v0.11.0-alpha.1`; follow [`RELEASING.md`](RELEASING.md).
- Keep GHCR packages linked to this repository and make the released images
  public when the corresponding release is intended for public consumption.
- Review release assets and checksum attestations before announcing a tag.
- Do not add model-provider credentials to repository, environment, or Actions
  secrets for CI. The deterministic fake provider is the CI path.

## Maintainer operating loop

1. Triage new issues using the forms and move design questions to Discussions.
2. Label security, dependency, and compatibility impact before implementation.
3. Require a focused pull request with the repository template and relevant
   tests, smokes, documentation, and license review.
4. Merge only after required checks are green and code-owner review is present.
5. Close the loop with a changelog entry, a completion note for new slices, or
   a release note for user-visible behavior.
