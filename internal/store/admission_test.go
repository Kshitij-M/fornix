package store

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func admissionDefinition(t *testing.T, workspace string, effect contracts.EffectClass, approval bool) contracts.CapabilityDefinition {
	t.Helper()
	hashA, hashB := testHash("admission-input"), testHash("admission-output")
	definition := contracts.CapabilityDefinition{
		WorkspaceID: workspace,
		Ref:         contracts.CapabilityRef{WorkspaceID: workspace, Connector: contracts.ConnectorRef{WorkspaceID: workspace, Name: "fixture", Version: "1"}, Name: "read", Version: "1"},
		Description: "bounded admission fixture", InputSchemaVersion: 1, InputSchemaHash: hashA, OutputSchemaVersion: 1, OutputSchemaHash: hashB,
		Effect: effect, Profile: contracts.DefaultExecutionProfile(), RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"},
		ResourceKinds: []string{"record"}, MaxRows: 10, RateLimitPerMinute: 10, RequiresApproval: approval, Enabled: true,
		SupportsIdempotency: true, SupportsVerification: true,
	}
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	return definition
}

func admissionPolicy(t *testing.T, workspace string, definition contracts.CapabilityDefinition) contracts.AdmissionPolicy {
	t.Helper()
	value := contracts.AdmissionPolicy{
		WorkspaceID: workspace, PolicyID: "universal-safe", Version: "1",
		AllowedConnectors: []contracts.ConnectorRef{definition.Ref.Connector}, AllowedResourceKinds: []string{"record"}, AllowedActorIDs: []string{"operator"},
		RequireTaskFence: true, MaxCostMicros: 100, MaxOperationsPerWindow: 10, MaxCostPerWindowMicros: 1000,
	}
	if err := value.Normalize(); err != nil {
		t.Fatal(err)
	}
	return value
}

func createAdmittedOperation(t *testing.T, operationStore *OperationStore, workspace, key string, definition contracts.CapabilityDefinition) OperationCreateResult {
	t.Helper()
	request := operationTestRequest(t, workspace, key)
	request.Capability = definition.Ref
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := operationTestPlan(t, request)
	result, err := operationStore.Create(context.Background(), OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func admissionInputForOperation(t *testing.T, operation Operation, definition contracts.CapabilityDefinition, policy contracts.AdmissionPolicy, key string) contracts.AdmissionInput {
	t.Helper()
	return contracts.AdmissionInput{
		WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OperationHash: operation.OperationHash,
		RequestID: operation.RequestID, IdempotencyKey: key, Actor: operation.Request.Actor,
		Capability: definition, Target: operation.Request.Target, Policy: policy,
		ConnectorAvailable: true, ResourceAllowed: true, EvidenceSatisfied: true, TaskFenceValid: true,
		RequestedCostMicros: 10,
	}
}

func TestAdmissionStoreReadAndApprovalLifecycleIsIdempotent(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := admissionDefinition(t, workspace, contracts.EffectClassReadOnly, false)
	policy := admissionPolicy(t, workspace, definition)
	created := createAdmittedOperation(t, operationStore, workspace, "admission-operation", definition)
	input := admissionInputForOperation(t, created.Operation, definition, policy, "admission-read")
	first, err := admissionStore.Admit(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || first.Decision.Status != contracts.AdmissionAllowed || first.Decision.DecisionHash == "" {
		t.Fatalf("first admission = %+v", first)
	}
	duplicateInput := input
	duplicateInput.RequestID = "different-transport-request"
	duplicateInput.ConnectorAvailable = false
	duplicateInput.ResourceAllowed = false
	duplicateInput.EvidenceSatisfied = false
	duplicateInput.QuotaOperations, duplicateInput.QuotaCostMicros = 9, 999
	duplicate, err := admissionStore.Admit(context.Background(), duplicateInput)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.Decision.ID != first.Decision.ID {
		t.Fatalf("duplicate admission = %+v", duplicate)
	}
	conflict := input
	conflict.RequestedCostMicros = 11
	if _, err := admissionStore.Admit(context.Background(), conflict); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("admission conflict error = %v", err)
	}

	writeDefinition := admissionDefinition(t, workspace, contracts.EffectClassIrreversibleWrite, true)
	writeOperation := createAdmittedOperation(t, operationStore, workspace, "admission-write-operation", writeDefinition)
	writeInput := admissionInputForOperation(t, writeOperation.Operation, writeDefinition, policy, "admission-write")
	write, err := admissionStore.Admit(context.Background(), writeInput)
	if err != nil {
		t.Fatal(err)
	}
	if write.Decision.Status != contracts.AdmissionAwaitingApproval || write.Approval == nil {
		t.Fatalf("write admission = %+v", write)
	}
	if _, _, err := admissionStore.DecideApproval(context.Background(), contracts.OperationApprovalDecision{
		WorkspaceID: workspace, ApprovalID: write.Approval.ID, RequestID: "self", IdempotencyKey: "self-approval",
		Decision: contracts.ApprovalRequestApproved, Actor: write.Approval.RequestedBy,
	}); !errors.Is(err, ErrAdmissionApprover) {
		t.Fatalf("self approval error = %v", err)
	}
	approved, duplicateApproval, err := admissionStore.DecideApproval(context.Background(), contracts.OperationApprovalDecision{
		WorkspaceID: workspace, ApprovalID: write.Approval.ID, RequestID: "approval-request", IdempotencyKey: "approval-decision",
		Decision: contracts.ApprovalRequestApproved, Actor: contracts.ActorRef{ID: "reviewer", Kind: "human", WorkspaceID: workspace},
	})
	if err != nil || duplicateApproval || approved.Status != contracts.ApprovalRequestApproved {
		t.Fatalf("approval result = %+v duplicate=%v err=%v", approved, duplicateApproval, err)
	}
	replayedApproval, duplicateApproval, err := admissionStore.DecideApproval(context.Background(), contracts.OperationApprovalDecision{
		WorkspaceID: workspace, ApprovalID: write.Approval.ID, RequestID: "different-request", IdempotencyKey: "approval-decision",
		Decision: contracts.ApprovalRequestApproved, Actor: contracts.ActorRef{ID: "reviewer", Kind: "human", WorkspaceID: workspace},
	})
	if err != nil || !duplicateApproval || replayedApproval.Status != contracts.ApprovalRequestApproved {
		t.Fatalf("duplicate approval result = %+v duplicate=%v err=%v", replayedApproval, duplicateApproval, err)
	}
}

func TestAdmissionStoreFailsClosedForWorkspaceCredentialAndQuota(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := admissionDefinition(t, workspace, contracts.EffectClassReadOnly, false)
	definition.RequiredCredentialRefs = []string{"credential-a"}
	definition.Ref.DefinitionHash = ""
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	policy := admissionPolicy(t, workspace, definition)
	created := createAdmittedOperation(t, operationStore, workspace, "admission-facts-operation", definition)
	input := admissionInputForOperation(t, created.Operation, definition, policy, "admission-bad-credential")
	input.CredentialStates = []contracts.CredentialState{{Reference: "credential-a", Status: contracts.CredentialStateRevoked}}
	result, err := admissionStore.Admit(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision.ReasonCode != contracts.AdmissionReasonCredentialInvalid {
		t.Fatalf("credential decision = %+v", result.Decision)
	}
	quotaInput := input
	quotaInput.CredentialStates = []contracts.CredentialState{{Reference: "credential-a", Status: contracts.CredentialStateActive}}
	quotaInput.IdempotencyKey = "admission-quota"
	quotaInput.Policy.MaxOperationsPerWindow = 0
	quotaInput.Policy.MaxCostPerWindowMicros = 1
	quotaInput.Policy.PolicyHash = ""
	quotaResult, err := admissionStore.Admit(context.Background(), quotaInput)
	if err != nil {
		t.Fatal(err)
	}
	if quotaResult.Decision.ReasonCode != contracts.AdmissionReasonQuotaExceeded && quotaResult.Decision.ReasonCode != contracts.AdmissionReasonCredentialInvalid {
		t.Fatalf("quota decision = %+v", quotaResult.Decision)
	}
}

func TestAdmissionStoreSerializesConcurrentQuotaReservations(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := admissionDefinition(t, workspace, contracts.EffectClassReadOnly, false)
	policy := admissionPolicy(t, workspace, definition)
	policy.MaxOperationsPerWindow = 1
	policy.MaxCostPerWindowMicros = 0
	policy.PolicyHash = ""
	if err := policy.Normalize(); err != nil {
		t.Fatal(err)
	}
	firstOperation := createAdmittedOperation(t, operationStore, workspace, "concurrent-operation-a", definition)
	secondOperation := createAdmittedOperation(t, operationStore, workspace, "concurrent-operation-b", definition)
	inputs := []contracts.AdmissionInput{
		admissionInputForOperation(t, firstOperation.Operation, definition, policy, "concurrent-admission-a"),
		admissionInputForOperation(t, secondOperation.Operation, definition, policy, "concurrent-admission-b"),
	}
	results := make([]AdmissionResult, len(inputs))
	errorsByIndex := make([]error, len(inputs))
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range inputs {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			results[index], errorsByIndex[index] = admissionStore.Admit(context.Background(), inputs[index])
		}(index)
	}
	close(start)
	group.Wait()
	allowed, quotaDenied := 0, 0
	for index := range results {
		if errorsByIndex[index] != nil {
			t.Fatalf("concurrent admission %d error = %v", index, errorsByIndex[index])
		}
		switch results[index].Decision.ReasonCode {
		case contracts.AdmissionReasonApproved:
			allowed++
		case contracts.AdmissionReasonQuotaExceeded:
			quotaDenied++
		default:
			t.Fatalf("concurrent admission %d decision = %+v", index, results[index].Decision)
		}
	}
	if allowed != 1 || quotaDenied != 1 {
		t.Fatalf("concurrent quota results allowed=%d quota_denied=%d, want one each", allowed, quotaDenied)
	}
}

func TestAdmissionStoreEffectRecoveryIsFencedAndReplayable(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := admissionDefinition(t, workspace, contracts.EffectClassReversibleWrite, false)
	request := operationTestRequest(t, workspace, "effect-operation")
	request.Capability = definition.Ref
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := operationTestPlan(t, request)
	created, err := operationStore.Create(context.Background(), OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := operationStore.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-a", 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	attempt, inserted, err := operationStore.ReserveAttempt(context.Background(), OperationAttemptInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", Attempt: 1, AttemptID: "attempt-effect", OwnerID: "worker-a", Fence: lease.Lease.Fence, RequestHash: testHash("effect-request"), IdempotencyKey: "attempt-effect"})
	if err != nil || !inserted {
		t.Fatalf("attempt = %+v inserted=%v err=%v", attempt, inserted, err)
	}
	effect, inserted, err := operationStore.ReserveEffect(context.Background(), OperationEffectInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", AttemptID: attempt.AttemptID, OwnerID: "worker-a", Fence: lease.Lease.Fence, RequestHash: testHash("effect-request"), Effect: contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture", Class: contracts.EffectClassReversibleWrite, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: "provider-key", VerificationRequired: true, VerificationStatus: contracts.ExternalVerificationPending, CompensationStatus: contracts.ExternalCompensationAvailable}})
	if err != nil || !inserted {
		t.Fatalf("effect = %+v inserted=%v err=%v", effect, inserted, err)
	}
	dispatching, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "worker-a", Fence: lease.Lease.Fence, RequestID: "dispatch-intent", IdempotencyKey: "dispatch-intent", State: contracts.ExternalEffectDispatching})
	if err != nil || dispatching.State.State != contracts.ExternalEffectDispatching {
		t.Fatalf("dispatch intent = %+v err=%v", dispatching, err)
	}
	dispatched, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "worker-a", Fence: lease.Lease.Fence, RequestID: "dispatch-request", IdempotencyKey: "dispatch-effect", State: contracts.ExternalEffectDispatched, ProviderRequestID: "provider-request"})
	if err != nil || dispatched.State.State != contracts.ExternalEffectDispatched {
		t.Fatalf("dispatch = %+v err=%v", dispatched, err)
	}
	dispatchedDuplicate, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "worker-a", Fence: lease.Lease.Fence, RequestID: "different-request", IdempotencyKey: "dispatch-effect", State: contracts.ExternalEffectDispatched, ProviderRequestID: "provider-request"})
	if err != nil || !dispatchedDuplicate.Duplicate {
		t.Fatalf("duplicate dispatch = %+v err=%v", dispatchedDuplicate, err)
	}
	time.Sleep(50 * time.Millisecond)
	takeover, err := operationStore.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-b", time.Minute)
	if err != nil || !takeover.Takeover {
		t.Fatalf("takeover = %+v err=%v", takeover, err)
	}
	if _, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "worker-a", Fence: lease.Lease.Fence, RequestID: "ack-stale", IdempotencyKey: "ack-stale", State: contracts.ExternalEffectAcknowledged}); !errors.Is(err, ErrOperationLeaseFenced) && !errors.Is(err, ErrOperationLeaseExpired) {
		t.Fatalf("stale effect update error = %v", err)
	}
	acknowledged, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "worker-b", Fence: takeover.Lease.Fence, RequestID: "ack-request", IdempotencyKey: "ack-effect", State: contracts.ExternalEffectAcknowledged, ResponseHash: testHash("response")})
	if err != nil || acknowledged.State.State != contracts.ExternalEffectAcknowledged {
		t.Fatalf("acknowledge = %+v err=%v", acknowledged, err)
	}
	var history int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_effect_transitions WHERE workspace_id=$1 AND effect_id=$2`, workspace, effect.EffectID).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if history != 4 {
		t.Fatalf("effect history rows=%d, want initial+dispatching+dispatch+ack", history)
	}
}

func TestAdmissionStoreIndependentEffectLeaseRecoversAfterTerminalOperation(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := admissionDefinition(t, workspace, contracts.EffectClassReversibleWrite, false)
	request := operationTestRequest(t, workspace, "effect-independent-lease")
	request.Capability = definition.Ref
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := operationTestPlan(t, request)
	created, err := operationStore.Create(context.Background(), OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	operationLease, err := operationStore.AcquireLease(context.Background(), workspace, created.Operation.ID, "operation-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	hash := testHash("effect-independent-request")
	attempt, inserted, err := operationStore.ReserveAttempt(context.Background(), OperationAttemptInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", Attempt: 1, AttemptID: "attempt-independent-lease", OwnerID: "operation-worker", Fence: operationLease.Lease.Fence, RequestHash: hash, IdempotencyKey: "attempt-independent-lease"})
	if err != nil || !inserted {
		t.Fatalf("reserve attempt inserted=%v err=%v", inserted, err)
	}
	effect, inserted, err := operationStore.ReserveEffect(context.Background(), OperationEffectInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", AttemptID: attempt.AttemptID, OwnerID: "operation-worker", Fence: operationLease.Lease.Fence, RequestHash: hash, Effect: contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture", Class: contracts.EffectClassReversibleWrite, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: "provider-independent-lease", VerificationRequired: true, VerificationStatus: contracts.ExternalVerificationPending, CompensationStatus: contracts.ExternalCompensationAvailable}})
	if err != nil || !inserted {
		t.Fatalf("reserve effect inserted=%v err=%v", inserted, err)
	}
	if _, err := admissionStore.AcquireEffectLease(context.Background(), workspace, created.Operation.ID+"-wrong", effect.EffectID, "wrong-operation", time.Minute); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("wrong operation lease error=%v, want operation not found", err)
	}
	premature, err := admissionStore.AcquireEffectLease(context.Background(), workspace, created.Operation.ID, effect.EffectID, "premature-recovery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "premature-recovery", Fence: premature.Lease.Fence, LeaseKind: "effect", RequestID: "premature-dispatch", IdempotencyKey: "premature-dispatch", State: contracts.ExternalEffectDispatched}); !errors.Is(err, ErrAdmissionEffect) {
		t.Fatalf("effect lease dispatch error=%v, want admission rejection", err)
	}
	if err := admissionStore.ReleaseEffectLease(context.Background(), premature.Lease); err != nil {
		t.Fatal(err)
	}
	expiring, err := admissionStore.AcquireEffectLease(context.Background(), workspace, created.Operation.ID, effect.EffectID, "expiring-recovery", 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	takeover, err := admissionStore.AcquireEffectLease(context.Background(), workspace, created.Operation.ID, effect.EffectID, "takeover-recovery", time.Minute)
	if err != nil || !takeover.Takeover || takeover.Lease.Fence <= expiring.Lease.Fence {
		t.Fatalf("effect lease takeover=%+v err=%v", takeover, err)
	}
	if _, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "expiring-recovery", Fence: expiring.Lease.Fence, LeaseKind: "effect", RequestID: "expired-recovery", IdempotencyKey: "expired-recovery", State: contracts.ExternalEffectDispatching}); !errors.Is(err, ErrEffectLeaseFenced) && !errors.Is(err, ErrEffectLeaseExpired) {
		t.Fatalf("expired effect lease error=%v, want fenced or expired", err)
	}
	if err := admissionStore.ReleaseEffectLease(context.Background(), takeover.Lease); err != nil {
		t.Fatal(err)
	}
	if _, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "operation-worker", Fence: operationLease.Lease.Fence, RequestID: "dispatching-independent", IdempotencyKey: "dispatching-independent", State: contracts.ExternalEffectDispatching}); err != nil {
		t.Fatalf("record dispatch intent: %v", err)
	}
	if _, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "operation-worker", Fence: operationLease.Lease.Fence, RequestID: "dispatched-independent", IdempotencyKey: "dispatched-independent", State: contracts.ExternalEffectDispatched, ProviderRequestID: "provider-independent"}); err != nil {
		t.Fatalf("record dispatched effect: %v", err)
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning, contracts.OperationStatusAwaitingExternal, contracts.OperationStatusVerifying, contracts.OperationStatusSucceeded} {
		if _, err := operationStore.Transition(context.Background(), OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: "operation-worker", Fence: operationLease.Lease.Fence, Actor: request.Actor, RequestID: "terminal-" + status, IdempotencyKey: "terminal-" + status, ToStatus: status, ReasonCode: "qualification"}); err != nil {
			t.Fatalf("transition %s: %v", status, err)
		}
	}
	recovery, err := admissionStore.AcquireEffectLease(context.Background(), workspace, created.Operation.ID, effect.EffectID, "recovery-worker-a", time.Minute)
	if err != nil || recovery.Lease.Fence == 0 {
		t.Fatalf("acquire recovery lease=%+v err=%v", recovery, err)
	}
	var contenders sync.WaitGroup
	contenders.Add(1)
	var heldErr error
	go func() {
		defer contenders.Done()
		_, heldErr = admissionStore.AcquireEffectLease(context.Background(), workspace, created.Operation.ID, effect.EffectID, "recovery-worker-b", time.Minute)
	}()
	contenders.Wait()
	if !errors.Is(heldErr, ErrEffectLeaseHeld) {
		t.Fatalf("concurrent recovery owner error=%v, want held", heldErr)
	}
	if _, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "recovery-worker-b", Fence: recovery.Lease.Fence, LeaseKind: "effect", RequestID: "stale-recovery", IdempotencyKey: "stale-recovery", State: contracts.ExternalEffectAcknowledged}); !errors.Is(err, ErrEffectLeaseOwned) {
		t.Fatalf("wrong recovery owner error=%v, want owned", err)
	}
	updates := make([]EffectStateResult, 2)
	updateErrors := make([]error, 2)
	var updatesGroup sync.WaitGroup
	for index := range updates {
		updatesGroup.Add(1)
		go func(index int) {
			defer updatesGroup.Done()
			updates[index], updateErrors[index] = admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "recovery-worker-a", Fence: recovery.Lease.Fence, LeaseKind: "effect", RequestID: "recovery-ack-" + strconv.Itoa(index), IdempotencyKey: "recovery-ack", State: contracts.ExternalEffectAcknowledged, ResponseHash: testHash("provider-recovered")})
		}(index)
	}
	updatesGroup.Wait()
	duplicates := 0
	for index := range updates {
		if updateErrors[index] != nil || updates[index].State.State != contracts.ExternalEffectAcknowledged {
			t.Fatalf("terminal-operation recovery update[%d]=%+v err=%v", index, updates[index], updateErrors[index])
		}
		if updates[index].Duplicate {
			duplicates++
		}
	}
	if duplicates != 1 {
		t.Fatalf("concurrent duplicate recovery commands=%d, want one duplicate", duplicates)
	}
	if err := admissionStore.ReleaseEffectLease(context.Background(), recovery.Lease); err != nil {
		t.Fatal(err)
	}
	if _, err := admissionStore.UpdateEffect(context.Background(), contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: "recovery-worker-a", Fence: recovery.Lease.Fence, LeaseKind: "effect", RequestID: "released-recovery", IdempotencyKey: "released-recovery", State: contracts.ExternalEffectAcknowledged}); !errors.Is(err, ErrEffectLeaseReleased) {
		t.Fatalf("released recovery lease error=%v, want released", err)
	}
}

func TestAdmissionStoreCrashRollsBackDecision(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := admissionDefinition(t, workspace, contracts.EffectClassReadOnly, false)
	policy := admissionPolicy(t, workspace, definition)
	created := createAdmittedOperation(t, operationStore, workspace, "admission-crash-operation", definition)
	input := admissionInputForOperation(t, created.Operation, definition, policy, "admission-crash")
	admissionStore.SetFailureHook(func(stage string) error {
		if stage == "operation_admission_committed" {
			return errors.New("injected admission crash")
		}
		return nil
	})
	if _, err := admissionStore.Admit(context.Background(), input); err == nil {
		t.Fatal("injected admission crash unexpectedly committed")
	}
	admissionStore.SetFailureHook(nil)
	var decisions, events int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_admission_decisions WHERE workspace_id=$1`, workspace).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type='operation.admission_decided'`, workspace).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if decisions != 0 || events != 0 {
		t.Fatalf("crash left durable admission state: decisions=%d events=%d", decisions, events)
	}
	if _, err := admissionStore.Admit(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionInputDoesNotAcceptSecretLikeMetadata(t *testing.T) {
	input := contracts.AdmissionInput{}
	if strings.Contains(input.StableHash(), "secret") {
		t.Fatal("admission hash unexpectedly contains a secret-like value")
	}
}
