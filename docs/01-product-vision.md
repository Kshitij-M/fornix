# Fornix product vision

Status: active public product definition and direction.

Fornix is being built for a simple reason: capable AI agents are now able to
perform meaningful work, but teams still cannot safely delegate enough of that
work to them.

The missing ingredient is not another model, prompt, memory store, or tool
registry. It is operational trust.

## The problem

Long-running AI work becomes difficult to authorize when it touches an
important repository or project. A run can use the wrong source version, lose
its place after a crash, repeat an expensive tool call, exceed a cost budget,
or produce an answer that cannot be traced back to evidence.

Teams therefore use AI for suggestions and small experiments, while keeping
dependency upgrades, security remediation, migrations, large refactors, CI
repair, and repository-wide maintenance behind manual review or avoiding them
entirely.

The practical gap is:

```text
model capability  ───────────────────────────────▶
                  safe, bounded, verifiable work
```

Fornix exists to close that gap.

## The product definition

> **Fornix is verifiable AI work infrastructure for long-running repository
> operations.**

In practical terms:

> **Fornix lets teams delegate serious repository work to AI without losing
> control of scope, cost, evidence, approval, or recovery.**

Fornix is still an AI harness technically, but “AI harness” is not the product
category we want people to remember. The product is safe autonomous work. The
harness is the mechanism that makes that work admissible.

## The first valuable workflow

The first product-shaped workflow is repository maintenance:

1. admit a task against one authenticated workspace and source snapshot;
2. apply a policy, approval boundary, and hard execution budget;
3. retrieve only the relevant context and evidence;
4. execute a bounded model/tool loop with durable ownership and recovery;
5. validate the result with deterministic checks;
6. produce a **Verified Change Packet** containing the result, evidence,
   artifacts, cost, and validation history;
7. allow a reviewer or downstream system to inspect and replay the recorded
   control-plane history without re-running external effects.

The initial alpha demonstrates the control and retrieval substrate for this
workflow. It already provides workspace scope, tasks, fencing, checkpoints,
deterministic retrieval, model/tool boundaries, evidence, artifacts,
observability, evaluation, and repository ingestion. It does not yet claim to
be a complete unattended repository-maintenance product: the current reference
workflow is deliberately bounded and read-only, and the qualification note
records the remaining gaps.

## The Work Receipt

The user-facing outcome is the Verified Change Packet. Its machine-verifiable
foundation is the **Work Receipt**.

A Work Receipt connects:

- the workspace, actor, task, and source manifest;
- the policy, approval, and budget under which work was admitted;
- retrieval plans, context hashes, evidence, and provenance;
- model calls, provider usage, tool runs, retries, and failures;
- generated artifacts and validation results;
- the terminal state, replay hash, and known at-least-once boundaries.

The receipt is not a substitute for the result. It is what makes the result
understandable, auditable, comparable, and safe to resume. Fornix must never
claim exactly-once execution for an arbitrary remote provider or external
process; the receipt makes that boundary explicit instead of hiding it.

## Four product responsibilities

Fornix should be understandable as four responsibilities:

```text
Admit  →  Execute  →  Prove  →  Improve
```

- **Admit:** scope identity, permissions, policies, approvals, source, and
  budgets before work begins.
- **Execute:** durable tasks, fenced ownership, checkpoints, retries,
  cancellation, model calls, and policy-controlled tools.
- **Prove:** immutable evidence, provenance, artifacts, validation, cost, and
  replayable history.
- **Improve:** offline evaluation, quality gates, cost attribution, and
  regression detection from recorded work.

## Who Fornix is for

The first audience is not someone looking for another chat window. It is a
platform, security, developer-productivity, or engineering team that wants to
run AI against important codebases and needs to justify that decision to
reviewers, operators, and the people responsible for cost and risk.

Fornix should be useful to open-source maintainers as well: a local-first,
MIT-licensed control plane can help a maintainer automate repository hygiene
without requiring a hosted agent service or surrendering source data to a
vendor.

## What Fornix is not trying to be

Fornix is not trying to win by having the largest plugin catalog, the broadest
memory feature set, the most general multi-agent graph, or another chat UI. It
should integrate with existing agent runtimes where possible and make their
important work safer and more measurable.

It is also not currently a kernel-level sandbox, a high-availability database
platform, an OAuth/SSO provider, an object store, or an exactly-once wrapper
around external effects. Those are qualification boundaries, not marketing
claims. See the [production-readiness qualification](14-production-readiness-qualification.md).

## How success will be judged

The product is succeeding when a team says:

> “There is repository work we want AI to perform, but we currently cannot
> permit it unattended. Fornix gives us enough control and proof to authorize
> it.”

The first validation measures should therefore be task-level outcomes:

- reviewer time to understand and approve a result;
- total model, tool, database, and artifact cost;
- repeated work after crashes or duplicate delivery;
- time to recover and resume a run;
- evidence and provenance coverage;
- replay consistency;
- the number of tasks a team is willing to delegate unattended.

The roadmap should prioritize a small number of real repository workflows and
these measurements over a larger collection of generic harness features.

## Relationship to the engineering record

The [Fornix foundation](00-fornix-foundation.md) defines the authority,
determinism, and cost principles that implement this vision. The numbered
foundation and completion notes preserve the engineering history. The
[reference reuse matrix](13-reference-reuse-matrix.md) records research and
license boundaries. The [documentation guide](52-documentation-guide.md)
requires public claims to distinguish current behavior from future direction.
