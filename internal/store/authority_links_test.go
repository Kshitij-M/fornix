package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestAuthorityLinksBindAdmissionResultAndReplayInspection(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	definition := admissionDefinition(t, workspace, contracts.EffectClassReadOnly, false)
	policy := admissionPolicy(t, workspace, definition)
	created := createAdmittedOperation(t, operationStore, workspace, "authority-operation", definition)
	input := admissionInputForOperation(t, created.Operation, definition, policy, "authority-admission")
	admitted, err := admissionStore.Admit(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Decision.ID == "" {
		t.Fatal("admission decision has no durable identity")
	}
	lease, err := operationStore.AcquireLease(context.Background(), workspace, created.Operation.ID, "authority-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if _, err := operationStore.Transition(context.Background(), OperationTransitionInput{
			WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence,
			Actor: created.Operation.Request.Actor, IdempotencyKey: "authority-transition-" + status, ToStatus: status,
		}); err != nil {
			t.Fatal(err)
		}
	}
	result := contracts.OperationResult{
		ID: "authority-result", OperationID: created.Operation.ID, OperationHash: created.Operation.OperationHash,
		RequestID: created.Operation.RequestID, WorkspaceID: workspace, Actor: created.Operation.Request.Actor,
		Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: 1, OutputSchemaHash: testHash("authority-output-schema"), OutputHash: testHash("authority-output"),
		Evidence: []contracts.OperationEvidenceRef{{WorkspaceID: workspace, SourceReference: "evidence-1", EvidenceHash: testHash("authority-evidence"), Role: "result"}},
	}
	written, err := operationStore.RecordResult(context.Background(), OperationResultInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence,
		Actor: created.Operation.Request.Actor, IdempotencyKey: "authority-result", Result: result,
	})
	if err != nil {
		t.Fatal(err)
	}
	if written.Record.ResultHash == "" {
		t.Fatal("result hash was not persisted")
	}
	links, err := operationStore.AuthorityLinks(context.Background(), workspace, created.Operation.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 || links[0].Stage != contracts.AuthorityStageAdmission || links[1].Stage != contracts.AuthorityStageResult {
		t.Fatalf("unexpected authority links: %+v", links)
	}
	if links[0].AdmissionDecisionID != admitted.Decision.ID || links[1].ResultHash != written.Record.ResultHash || links[1].OperationFence != lease.Lease.Fence {
		t.Fatalf("authority linkage lost source identity: %+v", links)
	}
	if len(links[1].Evidence) != 1 || links[1].Evidence[0].Hash != testHash("authority-evidence") {
		t.Fatalf("result evidence was not linked by hash: %+v", links[1].Evidence)
	}
	duplicate, err := operationStore.RecordResult(context.Background(), OperationResultInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: "stale-worker", Fence: 1,
		Actor: created.Operation.Request.Actor, IdempotencyKey: "different-delivery", Result: result,
	})
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate result delivery err=%v duplicate=%v", err, duplicate.Duplicate)
	}
	links, err = operationStore.AuthorityLinks(context.Background(), workspace, created.Operation.ID, 10)
	if err != nil || len(links) != 2 {
		t.Fatalf("duplicate result changed authority history links=%d err=%v", len(links), err)
	}
	foreign, err := operationStore.AuthorityLinks(context.Background(), "other-authority-workspace", created.Operation.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(foreign) != 0 {
		t.Fatalf("cross-workspace authority inspection leaked %d links", len(foreign))
	}
}

func TestAuthorityLinkRejectsStaleOperationFence(t *testing.T) {
	operationStore, _, workspace := newOperationTestStore(t)
	request := operationTestRequest(t, workspace, "authority-stale-operation")
	created, err := operationStore.Create(context.Background(), OperationCreateInput{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	first, err := operationStore.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-a", 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	second, err := operationStore.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.Lease.Fence <= first.Lease.Fence {
		t.Fatalf("takeover did not advance fence: first=%d second=%d", first.Lease.Fence, second.Lease.Fence)
	}
	tx, err := beginWorkspaceTx(context.Background(), operationStore.pool, workspace)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, _, err = appendAuthorityLinkTx(context.Background(), tx, contracts.OperationAuthorityLink{
		WorkspaceID: workspace, Stage: contracts.AuthorityStageResult, OperationID: created.Operation.ID, OperationHash: created.Operation.OperationHash,
		OperationOwnerID: first.Lease.OwnerID, OperationFence: first.Lease.Fence,
		Actor: request.Actor, RequestID: "stale-authority-request", IdempotencyKey: "stale-authority-link",
	})
	if !errors.Is(err, ErrAuthorityLinkStale) {
		t.Fatalf("stale authority error=%v, want ErrAuthorityLinkStale", err)
	}
}

func TestAuthorityLinksInheritManagedCredentialSourceFacts(t *testing.T) {
	leaseStore, _, pool, workspace, reference := newCredentialLeaseTestStore(t)
	operationStore := NewOperationStore(pool, NewEventStore(pool))
	definition := admissionDefinition(t, workspace, contracts.EffectClassReadOnly, false)
	request := operationTestRequest(t, workspace, "authority-source-operation")
	request.Capability = definition.Ref
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := operationTestPlan(t, request)
	created, err := operationStore.Create(context.Background(), OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := leaseStore.Acquire(context.Background(), workspace, reference, "model:openai", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	admissionStore := NewAdmissionStore(pool, NewEventStore(pool))
	input := admissionInputForOperation(t, created.Operation, definition, admissionPolicy(t, workspace, definition), "authority-source-admission")
	input.CredentialLeaseID, input.CredentialLeaseFence, input.CredentialRevocationEpoch = lease.LeaseID, lease.Fence, lease.RevocationEpoch
	input.CredentialSourceVersion, input.CredentialSourceExpiresAt = lease.SourceVersion, lease.SourceExpiresAt
	if _, err := admissionStore.Admit(context.Background(), input); err != nil {
		t.Fatalf("credential-bound admission: %v", err)
	}
	operationLease, err := operationStore.AcquireLease(context.Background(), workspace, created.Operation.ID, "authority-source-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if _, err := operationStore.Transition(context.Background(), OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, Actor: request.Actor, IdempotencyKey: "authority-source-" + status, ToStatus: status}); err != nil {
			t.Fatal(err)
		}
	}
	result := contracts.OperationResult{ID: "authority-source-result", OperationID: created.Operation.ID, OperationHash: created.Operation.OperationHash, RequestID: created.Operation.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: 1, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: testHash("authority-source-output")}
	if _, err := operationStore.RecordResult(context.Background(), OperationResultInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, Actor: request.Actor, IdempotencyKey: "authority-source-result", Result: result}); err != nil {
		t.Fatal(err)
	}
	links, err := operationStore.AuthorityLinks(context.Background(), workspace, created.Operation.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 {
		t.Fatalf("authority links=%d, want admission and result", len(links))
	}
	for _, link := range links {
		if link.CredentialLeaseID != lease.LeaseID || link.CredentialLeaseFence != lease.Fence || link.CredentialRevocationEpoch != lease.RevocationEpoch || link.CredentialSourceVersion != lease.SourceVersion {
			t.Fatalf("link lost credential source authority: %+v", link)
		}
	}
	tx, err := beginWorkspaceTx(context.Background(), pool, workspace)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := validateAuthorityFactsTx(context.Background(), tx, workspace, "", "", lease.LeaseID, lease.Fence, lease.RevocationEpoch, "rotated-version", nil); !errors.Is(err, ErrAuthorityLinkStale) {
		t.Fatalf("mismatched source version error=%v, want stale authority", err)
	}
}
