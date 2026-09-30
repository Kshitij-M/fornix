package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestToolSandboxRecoveryIsAtomicFencedAndReplayable(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	ctx := context.Background()
	events := NewEventStore(pool)
	admission := NewAdmissionStore(pool, events)
	links := NewDomainEffectLinkStore(pool)
	toolRuns := NewToolRunStore(pool, events)
	cleanupStore := NewSandboxCleanupStore(pool)
	coordinator := NewToolSandboxRecoveryCoordinator(pool, toolRuns, admission, links)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.domain_effect_link_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.domain_effect_links WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_effect_leases WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_effect_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_effect_state WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_effects WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_attempts WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.tool_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspace)
	})

	actor := contracts.ActorRef{ID: "tool-recovery-actor", Kind: "user", WorkspaceID: workspace}
	definition := admissionDefinition(t, workspace, contracts.EffectClassReversibleWrite, false)
	operationRequest := operationTestRequest(t, workspace, "tool-recovery-operation")
	operationRequest.Actor = actor
	operationRequest.Capability = definition.Ref
	if err := operationRequest.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := operationTestPlan(t, operationRequest)
	created, err := operationStore.Create(ctx, OperationCreateInput{Request: operationRequest, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admission.Admit(ctx, admissionInputForOperation(t, created.Operation, definition, admissionPolicy(t, workspace, definition), "tool-recovery-admission")); err != nil {
		t.Fatal(err)
	}
	operationLease, err := operationStore.AcquireLease(ctx, workspace, created.Operation.ID, "tool-recovery-operation-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if _, err := operationStore.Transition(ctx, OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, Actor: actor, RequestID: "tool-recovery-operation-" + status, IdempotencyKey: "tool-recovery-operation-" + status, ToStatus: status}); err != nil {
			t.Fatal(err)
		}
	}
	operationHash := created.Operation.OperationHash
	attempt, _, err := operationStore.ReserveAttempt(ctx, OperationAttemptInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", Attempt: 1, AttemptID: "tool-recovery-attempt", OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, RequestHash: operationHash, IdempotencyKey: "tool-recovery-attempt"})
	if err != nil {
		t.Fatal(err)
	}
	profile := contracts.DefaultSandboxProfile()
	profile.Backend = string(contracts.SandboxBackendOCI)
	profile.ImageDigest = "sha256:" + strings.Repeat("a", 64)
	profile.ImagePlatform = contracts.SandboxImagePlatform{OS: "linux", Architecture: "amd64"}
	profile.ReadOnlyRootFS, profile.ReadOnlyWorkdir = true, true
	profile.CPUQuotaMilli, profile.MemoryBytes, profile.PIDsLimit, profile.ScratchBytes = 500, 256<<20, 32, 64<<20
	if err := profile.Normalize(); err != nil {
		t.Fatal(err)
	}
	profileHash, err := profile.Hash()
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.ToolRequest{
		WorkspaceID: workspace, RequestID: "tool-request-1", IdempotencyKey: "tool-recovery-run-key", Actor: actor,
		ToolID: "repo.inspect", Capability: "repo.inspect", Argv: []string{"/usr/bin/true"}, Budget: profile,
		ToolDefinitionHash: contracts.HashStrings("effective tool definition"), SandboxProfileHash: profileHash,
		SandboxQualificationHash: contracts.HashStrings("sandbox qualification"),
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	runStore := toolRuns
	noEffectRequest := request
	noEffectRequest.RequestID += "-no-effect"
	noEffectRequest.IdempotencyKey += "-no-effect"
	if err := noEffectRequest.Normalize(); err != nil {
		t.Fatal(err)
	}
	noEffectRun, duplicate, err := runStore.Reserve(ctx, noEffectRequest, contracts.ToolModeAutomatic)
	if err != nil || duplicate {
		t.Fatalf("reserve unavailable non-local tool run duplicate=%t err=%v", duplicate, err)
	}
	noEffectRun, err = runStore.MarkStarted(ctx, noEffectRun)
	if err != nil {
		t.Fatal(err)
	}
	noEffectResult := contracts.ToolResult{
		Status:  contracts.ToolRunFailed,
		Failure: &contracts.ToolFailure{Code: contracts.ToolFailureSandboxUnavailable, Message: "non-local runner is unavailable"},
	}
	noEffectRun, err = runStore.Finish(ctx, noEffectRun, noEffectResult)
	if err != nil || noEffectRun.Status != contracts.ToolRunFailed {
		t.Fatalf("unavailable non-local run failed to persist terminal result: run=%+v err=%v", noEffectRun, err)
	}
	if _, err := cleanupStore.GetForAttempt(ctx, workspace, noEffectRun.ID, noEffectRun.Attempt); !errors.Is(err, ErrSandboxCleanupMissing) {
		t.Fatalf("run without effect authority unexpectedly received cleanup intent: %v", err)
	}
	run, duplicate, err := runStore.Reserve(ctx, request, contracts.ToolModeAutomatic)
	if err != nil || duplicate {
		t.Fatalf("reserve tool run duplicate=%t err=%v", duplicate, err)
	}
	readByID, err := runStore.GetByID(ctx, workspace, run.ID)
	if err != nil || readByID.ID != run.ID {
		t.Fatalf("workspace-scoped tool read by id: run=%+v err=%v", readByID, err)
	}
	if _, err := runStore.GetByID(ctx, workspace+"-other", run.ID); !errors.Is(err, ErrToolRunMissing) {
		t.Fatalf("cross-workspace tool read error=%v, want not found", err)
	}
	run, err = runStore.MarkStarted(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runStore.Finish(ctx, run, contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "unbound result", ContentHash: strings.Repeat("0", 64)}); !errors.Is(err, ErrToolResultHashConflict) {
		t.Fatalf("ordinary result with a noncanonical content hash error=%v", err)
	}
	effect, _, err := operationStore.ReserveEffect(ctx, OperationEffectInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", AttemptID: attempt.AttemptID,
		OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, RequestHash: operationHash,
		Effect: contracts.ExternalEffect{WorkspaceID: workspace, Boundary: string(contracts.SandboxBackendOCI), Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: "tool-recovery-effect", VerificationStatus: contracts.ExternalVerificationPending, CompensationStatus: contracts.ExternalCompensationAvailable},
	})
	if err != nil {
		t.Fatal(err)
	}
	link, err := links.Bind(ctx, contracts.DomainEffectLink{
		WorkspaceID: workspace, OperationID: created.Operation.ID, OperationHash: operationHash,
		StepID: effect.StepID, AttemptID: effect.AttemptID, EffectID: effect.EffectID,
		EffectReservationHash: storedEffectIdentityHash(effect), DomainKind: contracts.DomainEffectKindToolRun,
		DomainID: run.ID, DomainHash: run.RequestHash, LinkRole: contracts.DomainEffectLinkRolePrimary,
		RequestHash: operationHash, Boundary: effect.Boundary, EffectClass: contracts.EffectClass(effect.EffectClass),
		DeliveryGuarantee: effect.DeliverySemantics, ProviderIdempotency: effect.ProviderIdempotency,
		ProviderRequestID: effect.ProviderRequestID, VerificationStatus: effect.VerificationStatus,
		OperationOwnerID: operationLease.Lease.OwnerID, OperationFence: operationLease.Lease.Fence,
		Actor: actor, RequestID: request.RequestID, IdempotencyKey: "tool-recovery-domain-link",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admission.UpdateEffect(ctx, contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, RequestID: request.RequestID, IdempotencyKey: "tool-recovery-dispatch", State: contracts.ExternalEffectDispatching}); err != nil {
		t.Fatal(err)
	}
	result := contracts.ToolResult{RequestID: run.RequestID, RunID: run.ID, ToolID: run.ToolID, Status: contracts.ToolRunSucceeded, Stdout: "Bearer test-secret-value"}
	if _, err := admission.UpdateEffect(ctx, contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, RequestID: request.RequestID, IdempotencyKey: "tool-recovery-dispatched", State: contracts.ExternalEffectDispatched, ResponseHash: result.Hash()}); err != nil {
		t.Fatal(err)
	}
	if _, err := admission.UpdateEffect(ctx, contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, RequestID: request.RequestID, IdempotencyKey: "tool-recovery-acknowledged", State: contracts.ExternalEffectAcknowledged, ResponseHash: result.Hash()}); err != nil {
		t.Fatal(err)
	}
	if _, err := links.Transition(ctx, contracts.DomainEffectLinkTransitionRequest{
		WorkspaceID: workspace, LinkID: link.Link.ID, ExpectedVersion: 1,
		FromStatus: contracts.DomainEffectLinkStatusLinked, ToStatus: contracts.DomainEffectLinkStatusReconciled,
		ResultHash: contracts.HashStrings("premature sandbox reconciliation"), IdempotencyKey: "tool-recovery-link-premature", Actor: actor,
	}); !errors.Is(err, ErrDomainEffectLinkEffectNotVerified) {
		t.Fatalf("unverified sandbox link reconciliation error=%v, want effect-not-verified", err)
	}
	state, err := admission.GetEffectState(ctx, workspace, effect.EffectID)
	if err != nil {
		t.Fatal(err)
	}
	recoveryLease, err := admission.AcquireEffectLease(ctx, workspace, created.Operation.ID, effect.EffectID, "tool-recovery-worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.SetFailureHook(func(point string) error {
		if point == "tool_recovery_prepare_after_effect" {
			return errors.New("simulated crash while normalizing interrupted dispatch")
		}
		return nil
	})
	if _, err := coordinator.Prepare(ctx, workspace, run.ID, "tool-recovery-prepare", recoveryLease.Lease.OwnerID, recoveryLease.Lease.Fence, state.Version, 1, actor); err == nil {
		t.Fatal("expected injected recovery-preparation failure")
	}
	coordinator.SetFailureHook(nil)
	unchangedRun, err := runStore.Get(ctx, workspace, run.IdempotencyKey)
	if err != nil || unchangedRun.Status != contracts.ToolRunRunning {
		t.Fatalf("tool run after preparation rollback=%+v err=%v", unchangedRun, err)
	}
	unchangedEffect, err := admission.GetEffectState(ctx, workspace, effect.EffectID)
	if err != nil || unchangedEffect.Version != state.Version || unchangedEffect.State != contracts.ExternalEffectAcknowledged {
		t.Fatalf("effect after preparation rollback=%+v err=%v", unchangedEffect, err)
	}
	unchangedLink, err := links.CurrentByDomain(ctx, workspace, contracts.DomainEffectKindToolRun, run.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil || unchangedLink.Transition.Version != 1 || unchangedLink.Transition.ToStatus != contracts.DomainEffectLinkStatusLinked {
		t.Fatalf("link after preparation rollback=%+v err=%v", unchangedLink, err)
	}
	prepared, err := coordinator.Prepare(ctx, workspace, run.ID, "tool-recovery-prepare", recoveryLease.Lease.OwnerID, recoveryLease.Lease.Fence, state.Version, 1, actor)
	if err != nil {
		t.Fatalf("prepare interrupted dispatch recovery: %v", err)
	}
	run, state, linkRecovery := prepared.Run, prepared.Effect, prepared.Link
	if run.Status != contracts.ToolRunRecoveryRequired || state.State != contracts.ExternalEffectRecoveryRequired || linkRecovery.Transition.ToStatus != contracts.DomainEffectLinkStatusRecoveryRequired {
		t.Fatalf("recovery preparation did not atomically normalize state: run=%s effect=%s link=%s", run.Status, state.State, linkRecovery.Transition.ToStatus)
	}
	duplicatePreparation, err := coordinator.Prepare(ctx, workspace, run.ID, "tool-recovery-prepare", recoveryLease.Lease.OwnerID, recoveryLease.Lease.Fence, state.Version, linkRecovery.Transition.Version, actor)
	if err != nil || !duplicatePreparation.Duplicate || duplicatePreparation.Effect.Version != state.Version || duplicatePreparation.Link.Transition.Version != linkRecovery.Transition.Version {
		t.Fatalf("duplicate recovery preparation=%+v err=%v", duplicatePreparation, err)
	}
	if _, err := cleanupStore.GetForAttempt(ctx, workspace, run.ID, run.Attempt); !errors.Is(err, ErrSandboxCleanupMissing) {
		t.Fatalf("recovery-required attempt unexpectedly has cleanup authority: err=%v", err)
	}
	identity := contracts.SandboxExecutionIdentity{
		SchemaVersion: contracts.SandboxExecutionIdentitySchemaVersion, WorkspaceID: workspace, ToolRunID: run.ID,
		ToolAttempt: run.Attempt, Backend: contracts.SandboxBackendOCI, OperationID: created.Operation.ID,
		OperationOwnerID: operationLease.Lease.OwnerID, OperationFence: operationLease.Lease.Fence,
		AttemptID: attempt.AttemptID, EffectID: effect.EffectID, ToolRequestHash: run.RequestHash,
		OperationRequestHash: operationHash, EffectReservationHash: storedEffectIdentityHash(effect),
		ToolDefinitionHash: request.ToolDefinitionHash, SandboxProfileHash: profileHash,
		QualificationHash: request.SandboxQualificationHash,
	}
	command := contracts.ToolRecoveryFinalizeRequest{
		WorkspaceID: workspace, ToolRunID: run.ID, OwnerID: recoveryLease.Lease.OwnerID, Fence: recoveryLease.Lease.Fence,
		ExpectedEffectVer: state.Version, ExpectedLinkVersion: linkRecovery.Transition.Version,
		RequestID: "tool-recovery-finalize-request", ToolRequestID: run.RequestID, IdempotencyKey: "tool-recovery-finalize",
		Actor: actor, Identity: identity, Observation: contracts.SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: contracts.SandboxAttemptCompleted, ResultHash: result.Hash(), Result: &result},
	}
	conflictingObservation := command
	conflictingResult := result
	conflictingResult.Stdout = "different observed output"
	conflictingObservation.Observation = contracts.SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: contracts.SandboxAttemptCompleted, ResultHash: conflictingResult.Hash(), Result: &conflictingResult}
	if _, err := coordinator.Finalize(ctx, conflictingObservation); !errors.Is(err, ErrToolRecoveryConflict) {
		t.Fatalf("mismatched observed result hash error=%v, want conflict", err)
	}
	coordinator.SetFailureHook(func(point string) error {
		if point == "tool_recovery_after_link" {
			return errors.New("simulated crash before tool result commit")
		}
		return nil
	})
	if _, err := coordinator.Finalize(ctx, command); err == nil {
		t.Fatal("expected injected transaction failure")
	}
	coordinator.SetFailureHook(nil)
	afterFinalizeRollbackRun, err := toolRuns.Get(ctx, workspace, run.IdempotencyKey)
	if err != nil || afterFinalizeRollbackRun.Status != contracts.ToolRunRecoveryRequired {
		t.Fatalf("tool run after rollback=%+v err=%v", afterFinalizeRollbackRun, err)
	}
	afterFinalizeRollbackEffect, err := admission.GetEffectState(ctx, workspace, effect.EffectID)
	if err != nil || afterFinalizeRollbackEffect.Version != state.Version || afterFinalizeRollbackEffect.State != contracts.ExternalEffectRecoveryRequired {
		t.Fatalf("effect after rollback=%+v err=%v", afterFinalizeRollbackEffect, err)
	}
	afterFinalizeRollbackLink, err := links.CurrentByDomain(ctx, workspace, contracts.DomainEffectKindToolRun, run.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil || afterFinalizeRollbackLink.Transition.Version != linkRecovery.Transition.Version || afterFinalizeRollbackLink.Transition.ToStatus != contracts.DomainEffectLinkStatusRecoveryRequired {
		t.Fatalf("link after rollback=%+v err=%v", afterFinalizeRollbackLink, err)
	}
	if _, err := cleanupStore.GetForAttempt(ctx, workspace, run.ID, run.Attempt); !errors.Is(err, ErrSandboxCleanupMissing) {
		t.Fatalf("rolled-back recovery unexpectedly has cleanup authority: err=%v", err)
	}
	if err := admission.ReleaseEffectLease(ctx, recoveryLease.Lease); err != nil {
		t.Fatal(err)
	}
	takeover, err := admission.AcquireEffectLease(ctx, workspace, created.Operation.ID, effect.EffectID, "tool-recovery-worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	stale := command
	if _, err := coordinator.Finalize(ctx, stale); !errors.Is(err, ErrEffectLeaseFenced) {
		t.Fatalf("stale recovery lease error=%v, want fenced", err)
	}
	command.OwnerID, command.Fence = takeover.Lease.OwnerID, takeover.Lease.Fence
	finalized, err := coordinator.Finalize(ctx, command)
	if err != nil {
		t.Fatalf("finalize recovery: %v", err)
	}
	if finalized.Duplicate || finalized.Run.Status != contracts.ToolRunSucceeded || finalized.Effect.State != contracts.ExternalEffectVerified || finalized.Link.Status != contracts.DomainEffectLinkStatusReconciled {
		t.Fatalf("unexpected recovery finalization: %+v", finalized)
	}
	cleanupJob, err := cleanupStore.GetForAttempt(ctx, workspace, run.ID, run.Attempt)
	if err != nil || cleanupJob.Status != contracts.SandboxCleanupPending || cleanupJob.Intent.ResultHash != result.Hash() || cleanupJob.Intent.Identity.StableHash() != identity.StableHash() {
		t.Fatalf("verified recovery did not atomically create exact cleanup intent: job=%+v err=%v", cleanupJob, err)
	}
	if strings.Contains(finalized.Run.Result.Stdout, "test-secret-value") || finalized.Run.Result.ContentHash != result.Hash() {
		t.Fatalf("recovered output was not redacted/hash-bound: %+v", finalized.Run.Result)
	}
	duplicateRecovery, err := coordinator.Finalize(ctx, command)
	if err != nil || !duplicateRecovery.Duplicate || duplicateRecovery.Run.Result.ContentHash != finalized.Run.Result.ContentHash {
		t.Fatalf("duplicate recovery=%+v err=%v", duplicateRecovery, err)
	}
	var concurrent sync.WaitGroup
	concurrentErrors := make(chan error, 2)
	for i := 0; i < 2; i++ {
		concurrent.Add(1)
		go func() {
			defer concurrent.Done()
			result, finalizeErr := coordinator.Finalize(ctx, command)
			if finalizeErr == nil && !result.Duplicate {
				finalizeErr = errors.New("concurrent recovery replay was not a duplicate")
			}
			concurrentErrors <- finalizeErr
		}()
	}
	concurrent.Wait()
	close(concurrentErrors)
	for finalizeErr := range concurrentErrors {
		if finalizeErr != nil {
			t.Fatal(finalizeErr)
		}
	}

	type cleanupClaimResult struct {
		jobs []contracts.SandboxCleanupJob
		err  error
	}
	claimResults := make(chan cleanupClaimResult, 2)
	var claimWG sync.WaitGroup
	for _, owner := range []string{"cleanup-worker-a", "cleanup-worker-b"} {
		claimWG.Add(1)
		go func(owner string) {
			defer claimWG.Done()
			jobs, claimErr := cleanupStore.Claim(ctx, workspace, owner, 1, time.Minute)
			claimResults <- cleanupClaimResult{jobs: jobs, err: claimErr}
		}(owner)
	}
	claimWG.Wait()
	close(claimResults)
	var firstClaim []contracts.SandboxCleanupJob
	for result := range claimResults {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if len(result.jobs) == 1 {
			if len(firstClaim) != 0 {
				t.Fatal("concurrent cleanup claims both acquired the same job")
			}
			firstClaim = result.jobs
		} else if len(result.jobs) != 0 {
			t.Fatalf("claim exceeded its bounded batch: %+v", result.jobs)
		}
	}
	if len(firstClaim) != 1 || firstClaim[0].Status != contracts.SandboxCleanupLeased || firstClaim[0].Fence != 1 || firstClaim[0].Attempts != 1 {
		t.Fatalf("concurrent cleanup claim=%+v", firstClaim)
	}
	leaseA := contracts.SandboxCleanupLease{WorkspaceID: workspace, JobID: firstClaim[0].ID, OwnerID: firstClaim[0].OwnerID, Fence: firstClaim[0].Fence, LeaseUntil: *firstClaim[0].LeaseUntil}
	unknown := cleanupObservation(cleanupJob, leaseA, contracts.SandboxCleanupUnknown, "runtime_unavailable")
	if _, _, err := cleanupStore.Complete(ctx, leaseA, unknown); !errors.Is(err, ErrSandboxCleanupConflict) {
		t.Fatalf("unknown cleanup observation completed a job: %v", err)
	}
	retried, err := cleanupStore.Retry(ctx, leaseA, unknown)
	if err != nil || retried.Status != contracts.SandboxCleanupRetryWait || retried.Attempts != 1 {
		t.Fatalf("cleanup retry state=%+v err=%v", retried, err)
	}
	forceCleanupJobDue(t, pool, workspace, retried.ID)
	secondClaim, err := cleanupStore.Claim(ctx, workspace, "cleanup-worker-b", 1, time.Minute)
	if err != nil || len(secondClaim) != 1 || secondClaim[0].Fence != 2 || secondClaim[0].Attempts != 2 {
		t.Fatalf("retry cleanup claim=%+v err=%v", secondClaim, err)
	}
	leaseB := contracts.SandboxCleanupLease{WorkspaceID: workspace, JobID: secondClaim[0].ID, OwnerID: secondClaim[0].OwnerID, Fence: secondClaim[0].Fence, LeaseUntil: *secondClaim[0].LeaseUntil}
	forceCleanupLeaseExpired(t, pool, workspace, leaseB.JobID)
	thirdClaim, err := cleanupStore.Claim(ctx, workspace, "cleanup-worker-c", 1, time.Minute)
	if err != nil || len(thirdClaim) != 1 || thirdClaim[0].Fence != 3 || thirdClaim[0].Attempts != 3 {
		t.Fatalf("expired cleanup takeover=%+v err=%v", thirdClaim, err)
	}
	leaseC := contracts.SandboxCleanupLease{WorkspaceID: workspace, JobID: thirdClaim[0].ID, OwnerID: thirdClaim[0].OwnerID, Fence: thirdClaim[0].Fence, LeaseUntil: *thirdClaim[0].LeaseUntil}
	if _, err := cleanupStore.Renew(ctx, leaseA, time.Minute); !errors.Is(err, ErrSandboxCleanupFence) {
		t.Fatalf("stale cleanup renewal was not fenced: %v", err)
	}
	if _, _, err := cleanupStore.Complete(ctx, leaseA, cleanupObservation(cleanupJob, leaseA, contracts.SandboxCleanupRemoved, "")); !errors.Is(err, ErrSandboxCleanupFence) {
		t.Fatalf("stale cleanup worker A was not fenced: %v", err)
	}
	if _, _, err := cleanupStore.Complete(ctx, leaseB, cleanupObservation(cleanupJob, leaseB, contracts.SandboxCleanupRemoved, "")); !errors.Is(err, ErrSandboxCleanupFence) {
		t.Fatalf("expired cleanup worker B was not fenced: %v", err)
	}
	removed := cleanupObservation(cleanupJob, leaseC, contracts.SandboxCleanupRemoved, "")
	completed, duplicate, err := cleanupStore.Complete(ctx, leaseC, removed)
	if err != nil || duplicate || completed.Status != contracts.SandboxCleanupCompleted || completed.CompletionState != contracts.SandboxCleanupRemoved {
		t.Fatalf("cleanup completion=%+v duplicate=%t err=%v", completed, duplicate, err)
	}
	if _, duplicate, err := cleanupStore.Complete(ctx, leaseC, removed); err != nil || !duplicate {
		t.Fatalf("duplicate cleanup completion duplicate=%t err=%v", duplicate, err)
	}
	staleCompletion := cleanupObservation(cleanupJob, leaseB, contracts.SandboxCleanupRemoved, "")
	if _, _, err := cleanupStore.Complete(ctx, leaseB, staleCompletion); !errors.Is(err, ErrSandboxCleanupFence) {
		t.Fatalf("stale fence received duplicate cleanup success: %v", err)
	}
	wrongOwner := leaseC
	wrongOwner.OwnerID = "cleanup-worker-forged"
	forgedCompletion := cleanupObservation(cleanupJob, wrongOwner, contracts.SandboxCleanupRemoved, "")
	if _, _, err := cleanupStore.Complete(ctx, wrongOwner, forgedCompletion); !errors.Is(err, ErrSandboxCleanupFence) {
		t.Fatalf("wrong owner received duplicate cleanup success: %v", err)
	}
	if _, err := cleanupStore.GetForAttempt(ctx, workspace+"-other", run.ID, run.Attempt); !errors.Is(err, ErrSandboxCleanupMissing) {
		t.Fatalf("cross-workspace cleanup lookup error=%v, want not found", err)
	}
	cleanupTx, err := beginWorkspaceTx(ctx, pool, workspace)
	if err != nil {
		t.Fatal(err)
	}
	var eventCount, minVersion, maxVersion int
	if err := cleanupTx.QueryRow(ctx, `SELECT COUNT(*),MIN(version),MAX(version) FROM fornix.sandbox_cleanup_events WHERE workspace_id=$1 AND job_id=$2`, workspace, cleanupJob.ID).Scan(&eventCount, &minVersion, &maxVersion); err != nil {
		_ = cleanupTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := cleanupTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if eventCount != 6 || minVersion != 1 || maxVersion != 6 {
		t.Fatalf("cleanup event history count=%d version=%d..%d, want 6 versioned transitions", eventCount, minVersion, maxVersion)
	}
	_ = unchangedLink
}

func cleanupObservation(job contracts.SandboxCleanupJob, lease contracts.SandboxCleanupLease, state, failureCode string) contracts.SandboxCleanupObservation {
	return contracts.SandboxCleanupObservation{
		WorkspaceID: job.Intent.WorkspaceID, JobID: lease.JobID, OwnerID: lease.OwnerID, Fence: lease.Fence,
		IdentityHash: job.Intent.Identity.StableHash(), RequestHash: job.Intent.Identity.ToolRequestHash,
		State: state, FailureCode: failureCode,
	}
}

func forceCleanupJobDue(t *testing.T, pool *pgxpool.Pool, workspaceID, jobID string) {
	t.Helper()
	tx, err := beginWorkspaceTx(context.Background(), pool, workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `UPDATE fornix.sandbox_cleanup_jobs SET retry_at=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND id=$2`, workspaceID, jobID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func forceCleanupLeaseExpired(t *testing.T, pool *pgxpool.Pool, workspaceID, jobID string) {
	t.Helper()
	tx, err := beginWorkspaceTx(context.Background(), pool, workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `UPDATE fornix.sandbox_cleanup_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND id=$2`, workspaceID, jobID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}
