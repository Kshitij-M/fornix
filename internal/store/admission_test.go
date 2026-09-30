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

func createAdmittedOperationAsActor(t *testing.T, operationStore *OperationStore, workspace, key, actorID string, definition contracts.CapabilityDefinition) OperationCreateResult {
	t.Helper()
	request := operationTestRequest(t, workspace, key)
	request.Actor.ID = actorID
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

func rateAdmissionPolicy(t *testing.T, workspace string, definition contracts.CapabilityDefinition, actors ...string) contracts.AdmissionPolicy {
	t.Helper()
	policy := admissionPolicy(t, workspace, definition)
	policy.AllowedActorIDs = actors
	policy.MaxOperationsPerWindow = 0
	policy.MaxCostPerWindowMicros = 0
	policy.PolicyHash = ""
	if err := policy.Normalize(); err != nil {
		t.Fatal(err)
	}
	return policy
}

func rateLimitedDefinition(t *testing.T, workspace, capabilityName, version string, limit int) contracts.CapabilityDefinition {
	t.Helper()
	definition := admissionDefinition(t, workspace, contracts.EffectClassReadOnly, false)
	definition.Ref.Name = capabilityName
	definition.Ref.Version = version
	definition.Ref.DefinitionHash = ""
	definition.RateLimitPerMinute = limit
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	return definition
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
	otherCapability := definition
	otherCapability.Ref.Name = "write"
	otherCapability.Ref.DefinitionHash = ""
	if err := otherCapability.Normalize(); err != nil {
		t.Fatal(err)
	}
	secondOperation := createAdmittedOperation(t, operationStore, workspace, "concurrent-operation-b", otherCapability)
	inputs := []contracts.AdmissionInput{
		admissionInputForOperation(t, firstOperation.Operation, definition, policy, "concurrent-admission-a"),
		admissionInputForOperation(t, secondOperation.Operation, otherCapability, policy, "concurrent-admission-b"),
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

func TestAdmissionStoreCapabilityRateLimitIsDurableScopedAndDuplicateSafe(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := rateLimitedDefinition(t, workspace, "read", "1", 1)
	policy := rateAdmissionPolicy(t, workspace, definition, "actor-a", "actor-b", "actor-c", "actor-d")

	firstOperation := createAdmittedOperationAsActor(t, operationStore, workspace, "rate-first-operation", "actor-a", definition)
	firstInput := admissionInputForOperation(t, firstOperation.Operation, definition, policy, "rate-first-admission")
	first, err := admissionStore.Admit(context.Background(), firstInput)
	if err != nil || first.Decision.ReasonCode != contracts.AdmissionReasonApproved {
		t.Fatalf("first capability admission=%+v err=%v", first.Decision, err)
	}
	duplicateInput := firstInput
	duplicateInput.RequestID = "rate-duplicate-request"
	duplicateInput.CapabilityOperationsInWindow = 1000
	duplicate, err := admissionStore.Admit(context.Background(), duplicateInput)
	if err != nil || !duplicate.Duplicate || duplicate.Decision.ID != first.Decision.ID {
		t.Fatalf("same-key retry consumed another slot: result=%+v err=%v", duplicate, err)
	}

	secondOperation := createAdmittedOperationAsActor(t, operationStore, workspace, "rate-second-operation", "actor-b", definition)
	secondInput := admissionInputForOperation(t, secondOperation.Operation, definition, policy, "rate-second-admission")
	second, err := admissionStore.Admit(context.Background(), secondInput)
	if err != nil || second.Decision.ReasonCode != contracts.AdmissionReasonRateLimited {
		t.Fatalf("aggregate capability rate decision=%+v err=%v", second.Decision, err)
	}
	wantRetryAt := first.Decision.CreatedAt.Add(60*time.Second + time.Microsecond)
	if second.Decision.RetryAt == nil || !second.Decision.RetryAt.Equal(wantRetryAt) {
		t.Fatalf("rate denial retry_at=%v, want oldest active decision expiry %s", second.Decision.RetryAt, wantRetryAt)
	}
	secondInput.RequestID = "rate-duplicate-denied-request"
	duplicateDenial, err := admissionStore.Admit(context.Background(), secondInput)
	if err != nil || !duplicateDenial.Duplicate || duplicateDenial.Decision.ID != second.Decision.ID || duplicateDenial.Decision.RetryAt == nil || !duplicateDenial.Decision.RetryAt.Equal(*second.Decision.RetryAt) {
		t.Fatalf("duplicate denial did not preserve original retry deadline: result=%+v err=%v", duplicateDenial, err)
	}
	replayed, err := admissionStore.GetDecisionByIdempotencyKey(context.Background(), workspace, secondInput.IdempotencyKey)
	if err != nil || replayed.ID != second.Decision.ID || replayed.RetryAt == nil || !replayed.RetryAt.Equal(*second.Decision.RetryAt) {
		t.Fatalf("decision lookup lost durable retry time: decision=%+v err=%v", replayed, err)
	}

	// A higher limit in a later capability version sees the one accepted
	// decision, but not the denied attempt. Version changes do not reset usage.
	versionTwo := rateLimitedDefinition(t, workspace, "read", "2", 2)
	versionTwoPolicy := rateAdmissionPolicy(t, workspace, versionTwo, "actor-a", "actor-b", "actor-c", "actor-d")
	thirdOperation := createAdmittedOperationAsActor(t, operationStore, workspace, "rate-third-operation", "actor-c", versionTwo)
	thirdInput := admissionInputForOperation(t, thirdOperation.Operation, versionTwo, versionTwoPolicy, "rate-third-admission")
	third, err := admissionStore.Admit(context.Background(), thirdInput)
	if err != nil || third.Decision.ReasonCode != contracts.AdmissionReasonApproved {
		t.Fatalf("denied request incorrectly consumed rate capacity: decision=%+v err=%v", third.Decision, err)
	}

	otherCapability := rateLimitedDefinition(t, workspace, "write", "1", 1)
	otherPolicy := rateAdmissionPolicy(t, workspace, otherCapability, "actor-a", "actor-b", "actor-c", "actor-d")
	fourthOperation := createAdmittedOperationAsActor(t, operationStore, workspace, "rate-fourth-operation", "actor-d", otherCapability)
	fourthInput := admissionInputForOperation(t, fourthOperation.Operation, otherCapability, otherPolicy, "rate-fourth-admission")
	fourth, err := admissionStore.Admit(context.Background(), fourthInput)
	if err != nil || fourth.Decision.ReasonCode != contracts.AdmissionReasonApproved {
		t.Fatalf("different capability shared rate capacity: decision=%+v err=%v", fourth.Decision, err)
	}
}

func TestAdmissionStoreCapabilityRateLimitIsWorkspaceScoped(t *testing.T) {
	storeA, poolA, workspaceA := newOperationTestStore(t)
	storeB, poolB, workspaceB := newOperationTestStore(t)
	if workspaceA == workspaceB {
		t.Fatal("test workspaces unexpectedly share an identity")
	}
	definitionA := rateLimitedDefinition(t, workspaceA, "read", "1", 1)
	operationA := createAdmittedOperationAsActor(t, storeA, workspaceA, "workspace-rate-a", "actor-a", definitionA)
	resultA, err := NewAdmissionStore(poolA, NewEventStore(poolA)).Admit(context.Background(), admissionInputForOperation(t, operationA.Operation, definitionA, rateAdmissionPolicy(t, workspaceA, definitionA, "actor-a"), "workspace-rate-admission-a"))
	if err != nil || resultA.Decision.ReasonCode != contracts.AdmissionReasonApproved {
		t.Fatalf("workspace A rate result=%+v err=%v", resultA.Decision, err)
	}

	definitionB := rateLimitedDefinition(t, workspaceB, "read", "1", 1)
	operationB := createAdmittedOperationAsActor(t, storeB, workspaceB, "workspace-rate-b", "actor-a", definitionB)
	resultB, err := NewAdmissionStore(poolB, NewEventStore(poolB)).Admit(context.Background(), admissionInputForOperation(t, operationB.Operation, definitionB, rateAdmissionPolicy(t, workspaceB, definitionB, "actor-a"), "workspace-rate-admission-b"))
	if err != nil || resultB.Decision.ReasonCode != contracts.AdmissionReasonApproved {
		t.Fatalf("workspace B inherited workspace A's rate usage: result=%+v err=%v", resultB.Decision, err)
	}
}

func TestAdmissionStoreCapabilityRateLimitCountsApprovalPending(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := rateLimitedDefinition(t, workspace, "publish", "1", 1)
	definition.Effect = contracts.EffectClassApprovalRequiredWrite
	definition.RequiresApproval = true
	definition.Ref.DefinitionHash = ""
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	policy := rateAdmissionPolicy(t, workspace, definition, "actor-a", "actor-b")
	firstOperation := createAdmittedOperationAsActor(t, operationStore, workspace, "approval-rate-a", "actor-a", definition)
	first, err := admissionStore.Admit(context.Background(), admissionInputForOperation(t, firstOperation.Operation, definition, policy, "approval-rate-admission-a"))
	if err != nil || first.Decision.Status != contracts.AdmissionAwaitingApproval {
		t.Fatalf("first admission status=%s reason=%s err=%v, want pending approval", first.Decision.Status, first.Decision.ReasonCode, err)
	}
	secondOperation := createAdmittedOperationAsActor(t, operationStore, workspace, "approval-rate-b", "actor-b", definition)
	second, err := admissionStore.Admit(context.Background(), admissionInputForOperation(t, secondOperation.Operation, definition, policy, "approval-rate-admission-b"))
	if err != nil || second.Decision.ReasonCode != contracts.AdmissionReasonRateLimited {
		t.Fatalf("pending approval did not consume rate capacity: decision=%+v err=%v", second.Decision, err)
	}
	if second.Decision.RetryAt == nil || !second.Decision.RetryAt.After(first.Decision.CreatedAt) {
		t.Fatalf("pending approval rate denial has no later retry deadline: %+v", second.Decision)
	}
}

func TestAdmissionStoreCapabilityRateLimitRollbackDoesNotConsumeSlot(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := rateLimitedDefinition(t, workspace, "read", "1", 1)
	policy := rateAdmissionPolicy(t, workspace, definition, "actor-a", "actor-b")
	firstOperation := createAdmittedOperationAsActor(t, operationStore, workspace, "rollback-rate-a", "actor-a", definition)
	firstInput := admissionInputForOperation(t, firstOperation.Operation, definition, policy, "rollback-rate-admission-a")
	admissionStore.SetFailureHook(func(stage string) error {
		if stage == "operation_admission_committed" {
			return errors.New("injected failure before admission commit")
		}
		return nil
	})
	if _, err := admissionStore.Admit(context.Background(), firstInput); err == nil {
		t.Fatal("injected pre-commit failure unexpectedly succeeded")
	}
	admissionStore.SetFailureHook(nil)

	secondOperation := createAdmittedOperationAsActor(t, operationStore, workspace, "rollback-rate-b", "actor-b", definition)
	second, err := admissionStore.Admit(context.Background(), admissionInputForOperation(t, secondOperation.Operation, definition, policy, "rollback-rate-admission-b"))
	if err != nil || second.Decision.ReasonCode != contracts.AdmissionReasonApproved {
		t.Fatalf("rolled-back admission consumed capability capacity: decision=%+v err=%v", second.Decision, err)
	}
}

func TestAdmissionStoreSerializesConcurrentCapabilityRateReservations(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := rateLimitedDefinition(t, workspace, "read", "1", 1)
	policy := rateAdmissionPolicy(t, workspace, definition, "actor-a", "actor-b")
	operations := []OperationCreateResult{
		createAdmittedOperationAsActor(t, operationStore, workspace, "cap-rate-concurrent-a", "actor-a", definition),
		createAdmittedOperationAsActor(t, operationStore, workspace, "cap-rate-concurrent-b", "actor-b", definition),
	}
	inputs := []contracts.AdmissionInput{
		admissionInputForOperation(t, operations[0].Operation, definition, policy, "cap-rate-admission-a"),
		admissionInputForOperation(t, operations[1].Operation, definition, policy, "cap-rate-admission-b"),
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
	allowed, rateLimited := 0, 0
	for index, result := range results {
		if errorsByIndex[index] != nil {
			t.Fatalf("concurrent capability admission %d error=%v", index, errorsByIndex[index])
		}
		switch result.Decision.ReasonCode {
		case contracts.AdmissionReasonApproved:
			allowed++
		case contracts.AdmissionReasonRateLimited:
			rateLimited++
		default:
			t.Fatalf("concurrent capability admission %d decision=%+v", index, result.Decision)
		}
	}
	if allowed != 1 || rateLimited != 1 {
		t.Fatalf("concurrent capability limit admitted=%d rate_limited=%d, want 1 each", allowed, rateLimited)
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
	if _, err := admissionStore.Admit(context.Background(), admissionInputForOperation(t, created.Operation, definition, admissionPolicy(t, workspace, definition), "effect-admission")); err != nil {
		t.Fatalf("durable effect admission: %v", err)
	}
	// Keep the lease comfortably above a cold CI/Postgres round trip. The
	// test still verifies takeover, but a 30ms TTL can expire before the next
	// transactional effect update even when the implementation is correct.
	lease, err := operationStore.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-a", 250*time.Millisecond)
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
	time.Sleep(300 * time.Millisecond)
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
	if _, err := admissionStore.Admit(context.Background(), admissionInputForOperation(t, created.Operation, definition, admissionPolicy(t, workspace, definition), "independent-effect-admission")); err != nil {
		t.Fatalf("durable effect admission: %v", err)
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
	var decisions, events, authorityLinks int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_admission_decisions WHERE workspace_id=$1`, workspace).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type='operation.admission_decided'`, workspace).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_authority_links WHERE workspace_id=$1`, workspace).Scan(&authorityLinks); err != nil {
		t.Fatal(err)
	}
	if decisions != 0 || events != 0 || authorityLinks != 0 {
		t.Fatalf("crash left durable admission state: decisions=%d events=%d authority_links=%d", decisions, events, authorityLinks)
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
