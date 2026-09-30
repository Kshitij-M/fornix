SHELL := /bin/sh

GO_IMAGE ?= golang:1.25.13
FORNIX_TEST_PG_DSN_DOCKER := $(subst 127.0.0.1,host.docker.internal,$(subst localhost,host.docker.internal,$(FORNIX_TEST_PG_DSN)))
GO_RUN ?= docker run --rm --add-host=host.docker.internal:host-gateway -u "$$(id -u):$$(id -g)" -e HOME=/tmp -e GOPATH=/tmp/go -e GOCACHE=/tmp/go-build -e GOMODCACHE=/tmp/go-mod -e FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN_DOCKER)" -v "$(CURDIR):/workspace" -w /workspace $(GO_IMAGE)
HOST_GO := $(shell command -v go 2>/dev/null)
GO_CMD := $(if $(HOST_GO),go,$(GO_RUN) go)
GOFMT_CMD := $(if $(HOST_GO),gofmt,$(GO_RUN) gofmt)
GO_FILES := $$(find . -name '*.go' -not -path './vendor/*' -print)
PYTHON ?= python3
PYTHON_VENV ?= .venv
PYTHON_ENV_BIN := $(PYTHON_VENV)/bin/python
PYTHON_BIN := $(if $(wildcard $(PYTHON_ENV_BIN)),$(PYTHON_ENV_BIN),$(PYTHON))
PYTHON_CHECK_BIN := $(if $(wildcard $(PYTHON_BIN)),$(PYTHON_BIN),$(PYTHON))
FORNIX_URL ?= http://localhost:8201
DOCKER ?= docker
# Keep local smokes authenticated when .env is absent. An explicit environment
# or command-line value still wins, while the fallback matches CI and the
# development-only key used by the smoke scripts.
FORNIX_KEY ?= $(shell value=$$(sed -n 's/^FORNIX_KEY=//p' .env 2>/dev/null | head -n 1); if [ -n "$$value" ]; then printf '%s' "$$value"; else printf '%s' 'fornix-ci-test-key'; fi)
# Docker Compose mounts this fixture at the container path below. Override it
# with the host path when the service is running directly on the host.
FORNIX_REFERENCE_WORKDIR ?= /workspace/fixtures/reference-repo
# Host-run Go commands reach the published development database through
# loopback. Containerized Go commands need the Docker host gateway instead.
# Callers and CI may always override this with FORNIX_TEST_PG_DSN or an
# explicit PROJECTION_PG_DSN.
PROJECTION_PG_DSN ?= $(if $(HOST_GO),postgres://fornix:fornix-dev-only@127.0.0.1:55433/fornix?sslmode=disable,postgres://fornix:fornix-dev-only@host.docker.internal:55433/fornix?sslmode=disable)
FORNIX_TEST_PG_DSN ?=
# Allow CI and disposable qualification runs to inject an isolated database
# without changing the developer default or touching the persistent dev DB.
UNIVERSAL_TEST_PG_DSN ?= $(if $(strip $(FORNIX_TEST_PG_DSN)),$(FORNIX_TEST_PG_DSN),$(PROJECTION_PG_DSN))

.PHONY: fmt fmt-check test test-race vet build package-check release-check smoke-package qualification-offline qualification-signed qualification-trust qualification-trust-distribution qualification-deployment-evidence qualification-deployment-refresh qualification-deployment-refresh-scheduler qualification-multidomain-reference qualification-generic-workflow qualification-generic-effect-verification qualification-generic-effect-verification-postgres qualification-release-admission qualification-admission-reference qualification-adapter-matrix qualification-effect-conformance qualification-external-boundary qualification-backup-restore qualification-credential-authority qualification-postgres-topology qualification-recovery-evidence qualification-workspace-isolation qualification-role-separated-postgres qualification-capacity qualification-federation-capacity qualification-embedding-recovery qualification-retrieval-consistency test-agent-run-effects test-operation-worker test-operation-supervisor python-install python-check docs-check check verify hooks-install install-hooks hooks-uninstall uninstall-hooks hooks-check test-connectors smoke smoke-local-cli smoke-local-runtime smoke-events smoke-projection smoke-leases smoke-tasks smoke-retrieval smoke-provenance smoke-model smoke-tools smoke-agent smoke-scheduler smoke-identity smoke-artifacts smoke-artifact-output smoke-observability smoke-retrieval-quality smoke-retrieval-evaluation smoke-reference-workflow smoke-reference-openai smoke-ingestion smoke-work-receipts smoke-changes smoke-validation smoke-policy smoke-operation-admission smoke-universal-operation smoke-universal-trust smoke-universal-egress smoke-universal-execution smoke-universal-effects smoke-universal-authority smoke-universal-credentials smoke-universal-schema smoke-universal-effect-authority smoke-universal-authority-conformance smoke-universal-effect-dispatch smoke-universal-embeddings smoke-universal-containment smoke-universal-coordination smoke-universal-federation smoke-reference-connectors smoke-workflow smoke-multidomain operator-reference dev-up dev-up-ai dev-up-watcher dev-run dev-logs dev-down

fmt:
	$(GOFMT_CMD) -w $(GO_FILES)

fmt-check:
	@diff="$$( $(GOFMT_CMD) -d $(GO_FILES) )"; \
	if [ -n "$$diff" ]; then \
		printf '%s\n' "$$diff"; \
		echo 'Go files are not formatted; run make fmt.' >&2; \
		exit 1; \
	fi

test:
	$(GO_CMD) test ./...

test-race:
	$(GO_CMD) test -race ./...

test-connectors:
	$(GO_CMD) test ./internal/connector ./internal/adapters/repository ./internal/adapters/httpapi ./internal/adapters/sqlreadonly -count=1 -v

vet:
	$(GO_CMD) vet ./...

build:
	mkdir -p bin
	$(GO_CMD) build -trimpath -o bin/fornix ./cmd/fornix
	$(GO_CMD) build -trimpath -o bin/fornix-watcher ./cmd/fornix-watcher
	$(GO_CMD) build -trimpath -o bin/fornix-eval ./cmd/fornix-eval

package-check:
	sh -n scripts/install.sh
	sh -n scripts/release/verify-artifacts.sh
	sh -n scripts/test/v0.36-package-smokes.sh
	sh -n scripts/test/v0.39-universal-operation-smokes.sh
	sh -n scripts/test/v0.40-universal-effect-smokes.sh
	sh -n scripts/test/v0.41-postgres-rls-smokes.sh
	sh -n scripts/test/v0.42-universal-federation-smokes.sh
	sh -n scripts/qualification/backup-restore.sh
	sh -n scripts/qualification/adapter-matrix.sh
	sh -n scripts/qualification/credential-authority.sh
	sh -n scripts/qualification/federation-capacity.sh
	sh -n scripts/qualification/operation-capacity.sh
	sh -n scripts/qualification/postgres-topology.sh
	sh -n scripts/qualification/recovery-evidence.sh
	sh -n scripts/qualification/role-separated-postgres.sh
	sh -n scripts/qualification/portable.sh
	sh -n scripts/qualification/effect-authority.sh
	sh -n scripts/qualification/effect-adapter-conformance.sh
	sh -n scripts/qualification/external-boundary-conformance.sh
	@test -x scripts/install.sh
	@test -x scripts/release/verify-artifacts.sh
	@test -x scripts/test/v0.36-package-smokes.sh
	@test -x scripts/test/v0.39-universal-operation-smokes.sh
	@test -x scripts/test/v0.41-postgres-rls-smokes.sh
	@test -x scripts/test/v0.42-universal-federation-smokes.sh
	@test -x scripts/qualification/backup-restore.sh
	@test -x scripts/qualification/adapter-matrix.sh
	@test -x scripts/qualification/credential-authority.sh
	@test -x scripts/qualification/federation-capacity.sh
	@test -x scripts/qualification/operation-capacity.sh
	@test -x scripts/qualification/postgres-topology.sh
	@test -x scripts/qualification/recovery-evidence.sh
	@test -x scripts/qualification/role-separated-postgres.sh
	@test -x scripts/qualification/portable.sh
	@test -x scripts/qualification/effect-authority.sh
	@test -x scripts/qualification/effect-adapter-conformance.sh
	@test -x scripts/qualification/external-boundary-conformance.sh
	$(GO_CMD) test ./cmd/fornix ./internal/credentials ./internal/profile ./internal/runtime -count=1

release-check:
	sh scripts/release/verify-artifacts.sh dist

qualification-offline:
	scripts/qualification/portable.sh

.PHONY: qualification-effect-observation
qualification-effect-observation:
	$(GO_CMD) test ./internal/contracts ./internal/qualification -run 'TestEffectAuthorityObservation|TestExternalEffectQualification' -count=1 -v

.PHONY: qualification-effect-authority-probe
qualification-effect-authority-probe:
	scripts/qualification/effect-authority.sh

.PHONY: qualification-signed
qualification-signed:
	$(GO_CMD) test ./internal/contracts ./internal/qualification ./cmd/fornix -run 'TestQualificationSignature|TestMergeSignedBundles|TestSignedQualificationBundle|TestQualificationKeyReaders' -count=1 -v

# Run the deployment-owned qualification trust/import slice. PostgreSQL-backed
# tests are intentionally pointed at an explicit disposable DSN when supplied;
# without one, contract and store compilation still run and integration tests
# skip rather than touching a developer database.
qualification-trust:
	$(GO_CMD) test ./internal/contracts ./internal/server ./cmd/fornix -run 'TestQualification|TestQualificationRoutesUseSeparateTrustPermissions' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestQualificationTrust' -count=1 -v

# Run the Task 80 signed trust-distribution and startup-conformance slice.
# Snapshot store tests require an explicitly supplied disposable PostgreSQL DSN.
qualification-trust-distribution:
	$(GO_CMD) test ./internal/contracts ./internal/config ./internal/server ./cmd/fornix -run 'TestQualification|TestLoadQualification' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestQualificationTrust' -count=1 -v

# Run the Task 81 release/evidence index and deterministic gate slice. The
# PostgreSQL-backed tests require an explicitly supplied disposable DSN.
qualification-deployment-evidence:
	$(GO_CMD) test ./internal/contracts ./internal/server ./cmd/fornix -run 'Test(Deployment|QualificationRoutes)' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestDeploymentEvidence' -count=1 -v

# Run the Task 92 bounded deployment qualification refresh slice. Database
# tests use only an explicitly supplied disposable DSN; without one the
# contract/server/CLI coverage still runs and store integration tests skip.
qualification-deployment-refresh:
	$(GO_CMD) test ./internal/contracts ./internal/server ./cmd/fornix -run 'QualificationRefresh|QualificationRoutesUseSeparateTrustPermissions' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestQualificationRefresh' -count=1 -v

# Run the Task 93 durable qualification scheduler and recovery-handoff slice.
# The scheduler claims work and validates existing refresh reports; it never
# invokes deployment systems. Database tests require an explicitly supplied
# disposable DSN and skip safely when none is configured.
qualification-deployment-refresh-scheduler:
	$(GO_CMD) test ./internal/contracts ./internal/server ./cmd/fornix -run 'QualificationRefreshSchedule|QualificationRoutesUseSeparateTrustPermissions' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestQualificationRefreshSchedule' -count=1 -v

# Run the Task 94 fake-first multi-domain reference workflow qualification.
# This is deliberately offline and does not claim live connector behavior.
qualification-multidomain-reference:
	$(GO_CMD) test ./internal/contracts ./internal/workflows/reference -run 'Test(AllReference|Reference)' -count=1 -v

# Run the Task 95 domain-neutral workflow service and offline adapter slice.
# This target never starts Postgres, a model provider, a connector, or an
# external system; database and live-effect qualification remain explicit.
qualification-generic-workflow:
	$(GO_CMD) test ./internal/contracts ./internal/adapters/fakedomains ./internal/workflows/generic ./internal/server ./cmd/fornix -run 'Test(Workflow|GenericWorkflow|Domains|EffectCapability)' -count=1 -v

# Run the Task 96/97 verifier retry, lease-fenced reconciliation,
# duplicate-suppression, adapter, HTTP authorization, and CLI contract slice.
# This target is offline; PostgreSQL integration remains DSN-gated.
qualification-generic-effect-verification:
	$(GO_CMD) test ./internal/contracts ./internal/effectdispatch ./internal/adapters/fakedomains ./internal/workflows/generic ./internal/workflow ./internal/server ./cmd/fornix -run 'Test(EffectVerification|FakeDomainVerifier|UnknownVerification|VerificationOutcome|DeterministicResumeKey|AuthenticatedWorkflow|Workflow|GenericWorkflow|Domains|EffectCapability|AuthorizationRoute)' -count=1 -v

# Run the Task 97 Postgres-backed verification and workflow qualification only
# against an explicitly supplied disposable Postgres/pgvector DSN compatible
# with the CI migration topology. Fixtures use unique workspace IDs and make
# no provider calls; append-only test history may remain.
qualification-generic-effect-verification-postgres:
	@test -n "$(FORNIX_TEST_PG_DSN)" || (echo 'Set FORNIX_TEST_PG_DSN to a disposable Postgres database.' >&2; exit 1)
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/effectdispatch -run '^Test(ReconcileIsFencedAtomicCrashSafeAndDuplicateSafe|UnknownVerificationCanBeRetriedWithFreshIdempotencyKey)$$' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/workflows/generic -run '^TestGenericWorkflowUnknownVerificationResumesWithoutRepeatingVerifier$$' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/server -run '^TestAuthenticatedWorkflowCLIOverHTTPResumesUnknownEffectWithoutDuplicateVerifierCall$$' -count=1 -v

.PHONY: qualification-authorization-audit-postgres
qualification-authorization-audit-postgres:
	@test -n "$(FORNIX_TEST_PG_DSN)" || (echo 'Set FORNIX_TEST_PG_DSN to a disposable Postgres database.' >&2; exit 1)
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestAuthStoreAuthorization(AuditIsIdempotentUnderConcurrentDuplicateRequests|ReplayFailsClosedOnChangedDecision|RejectsForgedKeylessPrincipal|IdempotencyCannotCrossIdentity)$$' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/server -run '^TestSecurityMiddlewareDoesNotReplayAuthorizationAfterRoleRevocation$$' -count=1 -v

.PHONY: qualification-capability-rate-admission qualification-capability-rate-admission-postgres qualification-workflow-retry-deadline qualification-workflow-retry-deadline-postgres
qualification-capability-rate-admission:
	$(GO_CMD) test ./internal/policy ./internal/server -run '^(TestAdmissionCapabilityRateLimitBoundaryAndRuntimeFactHash|TestOperationAdmissionHTTPStatusMapsCapabilityRateLimit)$$' -count=1 -v
	$(GO_CMD) test ./internal/workflows/generic -run '^(TestReadOnlyConnectorStepRequiresDurableAdmission|TestWorkflowAdmissionRateLimitRequiresDurableRetryDeadline)$$' -count=1 -v

.PHONY: qualification-capability-rate-admission-postgres
qualification-capability-rate-admission-postgres:
	@test -n "$(FORNIX_TEST_PG_DSN)" || (echo 'Set FORNIX_TEST_PG_DSN to a disposable Postgres database.' >&2; exit 1)
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestAdmissionStore(CapabilityRateLimitIsDurableScopedAndDuplicateSafe|CapabilityRateLimitIsWorkspaceScoped|CapabilityRateLimitCountsApprovalPending|CapabilityRateLimitRollbackDoesNotConsumeSlot|SerializesConcurrentCapabilityRateReservations|SerializesConcurrentQuotaReservations)$$' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/workflows/generic -run '^TestGenericWorkflowReadStepsUseDurableCapabilityRateAdmission$$' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/server -run '^TestServerOperationWorkerExecutesReadOnlyOperationAndDeduplicates$$' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/workflows/incident -run '^TestIncidentWorkflowEndToEndDuplicateApprovalConflictIsolationAndReplay$$' -count=1 -v

.PHONY: qualification-workflow-retry-deadline
qualification-workflow-retry-deadline:
	$(GO_CMD) test ./internal/contracts -run '^TestWorkflowRetryWaitMustMatchRetryState$$' -count=1 -v
	$(GO_CMD) test ./internal/store -run '^TestWorkflow(RetryDeadlineUsesEarliestPendingStep|StatusPreservesPendingWaits)$$' -count=1 -v

.PHONY: qualification-workflow-retry-deadline-postgres
qualification-workflow-retry-deadline-postgres:
	@test -n "$(FORNIX_TEST_PG_DSN)" || (echo 'Set FORNIX_TEST_PG_DSN to a disposable Postgres database.' >&2; exit 1)
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestWorkflow(RetryDeadlineProtectsQueueAndDirectStarts|RetryBudgetExhaustionClearsWait)$$' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/workflow -run '^TestRuntime(ResumesDueRetryStep|EarlyRetryIsAnUnchangedWait|ReturnsAfterOneRetryTransition|LaterDueRetryIsNotHiddenByEarlierWait)$$' -count=1 -v

# Verify support bundles stay redacted, bounded, owner-only, and non-destructive.
.PHONY: qualification-support-bundle
qualification-support-bundle:
	$(GO_CMD) test ./cmd/fornix -run '^(Test(Local|RunLocal)SupportBundle|TestWriteLocalSupportBundle)' -count=1 -v

# Verify the agent-loop evidence-role and per-run tool-catalog boundaries.
.PHONY: qualification-agent-trust-boundary qualification-agent-trust-boundary-postgres
qualification-agent-trust-boundary:
	$(GO_CMD) test ./internal/agentloop ./internal/contracts ./internal/model -run '^(TestRunCompilesContextOnceAndReplaysDeterministically|TestRegisteredButUndeclaredToolFailsBeforeExecution|TestUnknownToolCallFailsBeforeExecution|TestOrchestratorRejectsDeclaredUnregisteredToolBeforeModel|TestToolAllowlistFailsClosedForMismatchedNames|TestAgentRunRejectsAmbiguousToolCatalogNames|TestRunToolStepIsOrderedAndCancellationIsDurable|TestRunApprovalPausesAndResumesWithoutRepeatingModelEffect|TestAgentRunLeaseIsPropagatedToToolEffects|TestCreateDerivesModelToolSchemaFromRegisteredDefinition|TestCreateRejectsRequesterToolMetadataDriftBeforeReservation|TestCreateRejectsToolCatalogOverContextBudgetBeforeReservation|TestChangedRegisteredToolDefinitionFailsBeforeExecution|TestDecodeToolArgumentsRejectsUnknownFieldsAndTrailingValues|TestModelToolDefinitionAuthorityHashMustBeSHA256Hex|TestToolDefinitionModelMetadataAndEnvironmentCatalogAreBounded|TestOpenAIProviderProjectsOnlyProviderStandardToolFields)$$' -count=1 -v

.PHONY: qualification-agent-tool-schema-authority
qualification-agent-tool-schema-authority:
	$(GO_CMD) test ./internal/agentloop ./internal/contracts ./internal/tool ./internal/model -run '^(TestCreateDerivesModelToolSchemaFromRegisteredDefinition|TestRegistryPathArgumentRestrictionSurvivesModelCatalogProjection|TestAgentLoopPrependsRegisteredArgvPrefix|TestAgentLoopAllowsRegisteredCommandWithNoDynamicArguments|TestCreateRejectsRequesterToolMetadataDriftBeforeReservation|TestCreateRejectsToolCatalogOverContextBudgetBeforeReservation|TestChangedRegisteredToolDefinitionFailsBeforeExecution|TestAdvanceModelRevalidatesPersistedToolCatalogBeforeProviderEgress|TestDecodeToolArgumentsRejectsUnknownFieldsAndTrailingValues|TestModelToolDefinitionAuthorityHashMustBeSHA256Hex|TestToolDefinitionModelMetadataAndEnvironmentCatalogAreBounded|TestToolDefinitionRejectsExecutableAsPathArgument|TestRegistryPreservesAndDefensivelyCopiesPathArgumentRestrictions|TestExecutorAdmitsModelCatalogOnlyForAuthorizedWorkspaceAndActor|TestExecutorDoesNotExposeExplicitlyDeniedToolToModel|TestLocalExecutorRejectsMissingLeafThroughOutsideSymlink|TestOllamaChatPreservesToolCatalogAndMultiTurnToolHistory|TestOpenAIProviderProjectsOnlyProviderStandardToolFields|TestOpenAIToolNameMappingIsDeterministicAndCollisionChecked|TestOpenAIToolNameMappingRejectsCaseFoldAmbiguity|TestParseOpenAIToolCallsRejectsDuplicateIDs)$$' -count=1 -v

qualification-agent-trust-boundary-postgres:
	@test -n "$(FORNIX_TEST_PG_DSN)" || (echo 'Set FORNIX_TEST_PG_DSN to a disposable Postgres database.' >&2; exit 1)
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestAgentRunStore(PersistsExecutionMetadata|RejectsToolCatalogMutation)$$' -count=1 -v

# Prove a stale agent-run lease is rejected at generic effect dispatch before
# the adapter invoker is reached. This must run against disposable Postgres.
.PHONY: qualification-agent-run-effect-dispatch-postgres
qualification-agent-run-effect-dispatch-postgres:
	@test -n "$(FORNIX_TEST_PG_DSN)" || (echo 'Set FORNIX_TEST_PG_DSN to a disposable Postgres database.' >&2; exit 1)
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/effectdispatch -run '^TestDispatcherStaleAgentRunFenceFailsBeforeInvoker$$' -count=1 -v

# Run the Task 82 release-verification and startup/admission slice. The
# PostgreSQL-backed tests require an explicitly supplied disposable DSN.
qualification-release-admission:
	$(GO_CMD) test ./internal/contracts ./internal/config ./internal/server ./cmd/fornix -run 'Test(Deployment|LoadQualification)' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestDeploymentReleaseVerification' -count=1 -v

# Run the Task 83 generic-operation admission-consumption slice. The
# PostgreSQL-backed reference validation test requires an explicitly supplied
# disposable DSN; without one the store test skips safely.
qualification-admission-reference:
	$(GO_CMD) test ./internal/contracts ./internal/config ./internal/server ./cmd/fornix -run 'Test(DeploymentAdmissionReference|OperationHashIncludesDeploymentAdmissionReference|LoadReleaseAdmissionForEffects)' -count=1 -v
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run '^TestDeploymentReleaseVerificationBindsGateAndAdmission$$' -count=1 -v

qualification-backup-restore:
	scripts/qualification/backup-restore.sh

qualification-adapter-matrix:
	scripts/qualification/adapter-matrix.sh

qualification-effect-conformance:
	scripts/qualification/effect-adapter-conformance.sh

.PHONY: qualification-retention
qualification-retention:
	$(GO_CMD) test ./internal/contracts ./internal/store ./internal/server ./cmd/fornix -run 'QualificationRetention|Retention|Readiness' -count=1 -v

qualification-external-boundary:
	scripts/qualification/external-boundary-conformance.sh

qualification-credential-authority:
	scripts/qualification/credential-authority.sh

qualification-postgres-topology:
	scripts/qualification/postgres-topology.sh

qualification-recovery-evidence:
	scripts/qualification/recovery-evidence.sh

qualification-workspace-isolation:
	scripts/test/v0.41-postgres-rls-smokes.sh

qualification-role-separated-postgres:
	scripts/qualification/role-separated-postgres.sh

qualification-capacity:
	scripts/qualification/operation-capacity.sh

qualification-federation-capacity:
	scripts/qualification/federation-capacity.sh

# Run the Task 61 recovery qualification only against an explicitly supplied
# disposable Postgres/pgvector DSN. This target refuses the persistent
# development database by default.
qualification-embedding-recovery:
	@test -n "$(FORNIX_TEST_PG_DSN)" || (echo 'Set FORNIX_TEST_PG_DSN to a disposable Postgres/pgvector database.' >&2; exit 1)
	$(GO_CMD) clean -cache -testcache || true
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/store -run 'TestEmbeddingCallStore|TestEmbeddingRecoveryCoordinatorIsAtomicFencedAndReplayable|TestDomainEffectLink' -count=1 -v

# Run the Task 62 legacy-route retrieval qualification only against an
# explicitly supplied disposable Postgres/pgvector DSN. The test creates and
# cleans its own workspace-scoped rows; it must never use the persistent dev DB.
qualification-retrieval-consistency:
	@test -n "$(FORNIX_TEST_PG_DSN)" || (echo 'Set FORNIX_TEST_PG_DSN to a disposable Postgres/pgvector database.' >&2; exit 1)
	$(GO_CMD) clean -cache -testcache || true
	FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" $(GO_CMD) test ./internal/server -run '^TestLegacyRetrievalUsesExplicitReferenceTimeAndStableOrdering$$' -count=1 -v

# Run the agent-run effect-fencing slice. Postgres-backed stale-worker cases
# require an explicitly supplied disposable DSN; contract and loop tests still
# run when no DSN is available.
test-agent-run-effects:
	$(GO_CMD) test ./internal/contracts ./internal/agentloop ./internal/model ./internal/tool ./internal/store -run 'Test(AgentRunLease|ModelRequestAgentRun|ToolRequestAgentRun|ModelCallStoreAgentRun|ToolRunStoreAgentRun)' -count=1 -v

test-operation-worker:
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/operationworker ./internal/store -run 'TestWorker|TestOperationQueue' -count=1 -v

test-operation-supervisor:
	$(GO_CMD) test ./internal/operationsupervisor ./internal/operationworker ./internal/store -run 'TestSupervisor|TestWorker|TestOperationQueue' -count=1 -v

smoke-package: build
	scripts/test/v0.36-package-smokes.sh

python-install:
	$(PYTHON) -m venv $(PYTHON_VENV)
	$(PYTHON_ENV_BIN) -m pip install --upgrade pip
	$(PYTHON_ENV_BIN) -m pip install --requirement scripts/requirements.txt

python-check:
	$(PYTHON_CHECK_BIN) -m py_compile scripts/*.py
	$(PYTHON_CHECK_BIN) -m compileall -q scripts

docs-check:
	$(PYTHON_CHECK_BIN) scripts/check_docs.py

smoke-events:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.11-event-smokes.sh

smoke-projection:
	FORNIX_PROJECTION_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.12-projection-smokes.sh

smoke-leases:
	FORNIX_LEASE_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.13-lease-smokes.sh

smoke-tasks:
	FORNIX_TASK_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.14-task-smokes.sh

smoke-retrieval:
	FORNIX_RETRIEVAL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.15-retrieval-smokes.sh

smoke-provenance:
	FORNIX_PROVENANCE_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.16-provenance-smokes.sh

smoke-model:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.17-model-smokes.sh

smoke-tools:
	FORNIX_TOOL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.18-tool-smokes.sh

smoke-agent:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.19-agent-smokes.sh

smoke-scheduler:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.20-scheduler-smokes.sh

smoke-identity:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.21-identity-smokes.sh

smoke-artifacts:
	FORNIX_ARTIFACT_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.22-artifact-smokes.sh

smoke-artifact-output:
	FORNIX_ARTIFACT_OUTPUT_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.23-artifact-output-smokes.sh

smoke-observability:
	FORNIX_OBSERVABILITY_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.24-observability-smokes.sh

smoke-retrieval-quality:
	FORNIX_EVAL_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.25-retrieval-quality-smokes.sh

smoke-retrieval-evaluation:
	FORNIX_RETRIEVAL_EVAL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.26-retrieval-evaluation-smokes.sh

smoke-reference-workflow:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.27-reference-workflow-smokes.sh

smoke-reference-openai:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.28-openai-smoke.sh

smoke-ingestion:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.29-ingestion-smokes.sh

smoke-work-receipts:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.30-work-receipt-smokes.sh

smoke-changes:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.31-change-smokes.sh

smoke-validation:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.32-validation-smokes.sh

smoke-policy:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.33-policy-smokes.sh

smoke-operation-admission:
	FORNIX_TEST_PG_DSN=$(PROJECTION_PG_DSN) $(GO_CMD) test ./internal/contracts ./internal/policy ./internal/store -run 'TestAdmission|TestOperation' -count=1 -v

smoke-universal-operation:
	FORNIX_TEST_PG_DSN=$(PROJECTION_PG_DSN) $(GO_CMD) test ./internal/server -run 'Test(GenericOperationHTTP|SecurityMiddleware.*GenericOperation)' -count=1 -v
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_WORKSPACE_ID=$${FORNIX_WORKSPACE_ID:-universal-operation-smoke} scripts/test/v0.39-universal-operation-smokes.sh

smoke-universal-trust:
	$(GO_CMD) test ./internal/credentials ./internal/connector ./internal/adapters/httpapi ./internal/model -run 'Test(SignedTrust|Lease|Trust|RegistryTrust|HTTPUsesExpiring|HTTPRevalidatesCredentialLease|OpenAIRevalidatesCredentialLease)' -count=1 -v

smoke-universal-egress:
	$(GO_CMD) test ./internal/connector ./internal/adapters/httpapi ./internal/model -run 'Test(DestinationPolicy|Egress|HTTP|OpenAI|Ollama)' -count=1 -v

smoke-universal-execution:
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/store ./internal/server -run 'Test(OperationPlanAndResult|OperationResultCrash|GenericOperationHTTPExecutesTrustedReadAndDeduplicates|GenericOperationHTTPRejectsEffectfulCapabilityWithoutReservation)' -count=1 -v

smoke-universal-effects:
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/store ./internal/server -run 'Test(AdmissionStoreEffectRecoveryIsFencedAndReplayable|GenericOperationHTTPReservesAndReconcilesExternalEffect)' -count=1 -v

smoke-universal-authority:
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/contracts ./internal/store ./internal/server -run 'Test(Authority|WorkReceiptOperationLink|OperationPlanAndResult|OperationResultCrash)' -count=1 -v

smoke-universal-credentials:
	$(GO_CMD) test ./internal/credentials -run 'Test(Managed|HTTPSecret|DeploymentAuthority|Certificate)' -count=1 -v
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/store -run 'TestCredentialLease' -count=1 -v

smoke-universal-schema:
	$(GO_CMD) test ./internal/connector -run 'Test(SchemaCatalog|SignedSchemaCatalog)' -count=1 -v
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/store -run 'TestTrustCatalog.*Schema' -count=1 -v

smoke-universal-effect-authority:
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/contracts ./internal/connector ./internal/credentials ./internal/store ./internal/adapters/httpapi ./internal/server -run 'Test(EffectAuthority|AuthorityLinksInheritManagedCredentialSourceFacts|CredentialLeaseResolvesTheExactAdmittedFence|SignedSchemaCatalogBindsEffectAuthorityFacts|GenericOperationHTTPReservesAndReconcilesExternalEffect|OperationIntegrationEffectAuthorityRevalidatesLiveFence|ServerStartupReloadsSignedWorkspaceAuthority)' -count=1 -v

smoke-universal-authority-conformance:
	$(GO_CMD) test ./internal/connector ./internal/adapters/fakeincident ./internal/server -run 'Test(EffectAuthority|ConnectorIsDeterministic)' -count=1 -v
	$(GO_CMD) test ./internal/connector ./internal/effectdispatch -run 'Test(AuthorityConformance|BuiltinConformance|ConformanceRegistry)' -count=1 -v
	$(GO_CMD) run ./cmd/fornix qualification external-boundary >/dev/null

smoke-universal-effect-dispatch:
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/effectdispatch -run '^TestDispatcher' -count=1 -v
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/store -run '^TestDomainEffectLink' -count=1 -v
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/workflows/incident -run '^TestIncidentWorkflowEndToEndDuplicateApprovalConflictIsolationAndReplay$$' -count=1 -v

smoke-universal-embeddings:
	$(GO_CMD) test ./internal/contracts ./internal/model -run 'TestEmbedding' -count=1 -v
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/store -run '^TestEmbeddingCallStore' -count=1 -v

smoke-universal-containment:
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/server -run 'TestSecurityMiddleware(RejectsUnknownAndLegacyGlobalRoutes|AllowsLegacyGlobalRoutesOnlyWhenOptedIn|RejectsLegacyGlobalRoutesWithoutDedicatedPermission|AllowsWorkspaceScopedCoordinationAndRouterRoutes)' -count=1 -v
	$(GO_CMD) test ./internal/adapters/httpapi ./internal/connector -run 'TestHTTPSubmitPreservesDurableEffectAuthorityIdentity|TestEffectAuthority' -count=1 -v

smoke-universal-coordination:
	$(GO_CMD) test ./internal/contracts -run 'Test(CoordinationMessage|RouterObservation)' -count=1 -v
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) $(GO_CMD) test ./internal/store -run 'TestWorkspace(Coordination|Router)' -count=1 -v
	$(GO_CMD) test ./internal/server -run '^TestSecurityMiddlewareAllowsWorkspaceScopedCoordinationAndRouterRoutes$$' -count=1 -v

smoke-universal-federation:
	FORNIX_TEST_PG_DSN=$(UNIVERSAL_TEST_PG_DSN) scripts/test/v0.42-universal-federation-smokes.sh

smoke-reference-connectors:
	FORNIX_TEST_PG_DSN=$(PROJECTION_PG_DSN) $(GO_CMD) test ./internal/connector ./internal/adapters/httpapi ./internal/adapters/sqlreadonly -count=1 -v
	FORNIX_TEST_PG_DSN=$(PROJECTION_PG_DSN) $(GO_CMD) test ./internal/store -run 'TestConnectorBinding' -count=1 -v

smoke-workflow:
	FORNIX_TEST_PG_DSN=$(PROJECTION_PG_DSN) $(GO_CMD) test ./internal/contracts ./internal/store ./internal/workflow -run 'Test(Workflow|Runtime)' -count=1 -v

smoke-multidomain:
	FORNIX_TEST_PG_DSN=$(PROJECTION_PG_DSN) $(GO_CMD) test ./internal/contracts ./internal/adapters/fakeincident ./internal/workflows/incident -run 'Test(Incident|Connector)' -count=1 -v
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.38-multidomain-smokes.sh

operator-reference: build
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) bin/fornix reference-workflow --workspace $${FORNIX_WORKSPACE_ID:-reference-local} --fixture fixtures/reference-repo

smoke:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) PYTHON_BIN=$(PYTHON_BIN) scripts/test/v0.10-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.11-event-smokes.sh
	FORNIX_PROJECTION_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.12-projection-smokes.sh
	FORNIX_LEASE_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.13-lease-smokes.sh
	FORNIX_TASK_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.14-task-smokes.sh
	FORNIX_RETRIEVAL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.15-retrieval-smokes.sh
	FORNIX_PROVENANCE_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.16-provenance-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.17-model-smokes.sh
	FORNIX_TOOL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.18-tool-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.19-agent-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.20-scheduler-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.21-identity-smokes.sh
	FORNIX_ARTIFACT_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.22-artifact-smokes.sh
	FORNIX_ARTIFACT_OUTPUT_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.23-artifact-output-smokes.sh
	FORNIX_OBSERVABILITY_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.24-observability-smokes.sh
	FORNIX_EVAL_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.25-retrieval-quality-smokes.sh
	FORNIX_RETRIEVAL_EVAL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.26-retrieval-evaluation-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.27-reference-workflow-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.29-ingestion-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.30-work-receipt-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.31-change-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.32-validation-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.33-policy-smokes.sh
	$(MAKE) smoke-operation-admission
	$(MAKE) smoke-reference-connectors
	$(MAKE) smoke-workflow
	$(MAKE) smoke-multidomain
	$(MAKE) smoke-universal-operation
	$(MAKE) smoke-universal-trust
	$(MAKE) smoke-universal-egress
	$(MAKE) smoke-universal-execution
	$(MAKE) smoke-universal-effects
	$(MAKE) smoke-universal-authority
	$(MAKE) smoke-universal-credentials
	$(MAKE) smoke-universal-schema
	$(MAKE) smoke-universal-effect-authority
	$(MAKE) smoke-universal-authority-conformance
	$(MAKE) smoke-universal-effect-dispatch
	$(MAKE) smoke-universal-embeddings
	$(MAKE) smoke-universal-containment
	$(MAKE) smoke-universal-coordination
	$(MAKE) smoke-universal-federation
	$(MAKE) smoke-local-cli

check: fmt-check test vet python-check docs-check package-check

smoke-local-cli: build
	scripts/test/v0.34-local-cli-smokes.sh

smoke-local-runtime: build
	$(DOCKER) build --tag $${FORNIX_LOCAL_IMAGE:-fornix-local:smoke} .
	FORNIX_LOCAL_IMAGE=$${FORNIX_LOCAL_IMAGE:-fornix-local:smoke} scripts/test/v0.35-local-runtime-smokes.sh

verify: check test-race build

hooks-install install-hooks:
	git config core.hooksPath .githooks
	@echo 'Installed Fornix hooks through core.hooksPath=.githooks'

hooks-uninstall uninstall-hooks:
	git config --unset core.hooksPath 2>/dev/null || true
	@echo 'Removed the repository-local hook override'

hooks-check:
	@test -x .githooks/pre-commit
	@test -x .githooks/pre-push
	@echo 'Fornix hooks are present and executable'

dev-up:
	docker compose --env-file .env up -d db

dev-up-ai:
	docker compose --env-file .env --profile ai up -d db ollama

dev-up-watcher:
	docker compose --env-file .env --profile app --profile watch up --build -d db fornix watcher

dev-run:
	docker compose --env-file .env --profile app up --build

dev-logs:
	docker compose --env-file .env --profile app --profile ai logs -f

dev-down:
	docker compose --env-file .env --profile app --profile ai down
