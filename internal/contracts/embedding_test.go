package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEmbeddingRequestExcludesRawTextAndBindsSourceHash(t *testing.T) {
	request := EmbeddingRequest{
		RequestID: "request-1", IdempotencyKey: "key-1", WorkspaceID: "workspace-a",
		Actor:    ActorRef{ID: "actor-a", Kind: "user", WorkspaceID: "workspace-a"},
		Provider: ProviderRef{Provider: "fake", Model: "fake-model"}, Model: "fake-model",
		SourceKind: "memo", SourceID: "memo-1", Text: "secret prompt content",
		Budget: EmbeddingBudget{MaxInputBytes: MaxEmbeddingInputBytes, Dimension: EmbeddingDimension, TimeoutMS: 1000},
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), request.Text) {
		t.Fatal("raw embedding text appeared in serialized request")
	}
	firstHash := request.RequestHash()
	request.Text = "different text with the same source identity"
	if request.RequestHash() != firstHash {
		t.Fatal("request hash changed when only excluded text changed")
	}
}

func TestEmbeddingVectorHashIsStableAndRejectsWrongDimension(t *testing.T) {
	vector := make([]float32, EmbeddingDimension)
	for i := range vector {
		vector[i] = float32(i%13) / 13
	}
	first, err := EmbeddingVectorHash(vector)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EmbeddingVectorHash(append([]float32(nil), vector...))
	if err != nil || first != second {
		t.Fatalf("hashes = %s/%s err=%v", first, second, err)
	}
	if _, err := EmbeddingVectorHash(vector[:EmbeddingDimension-1]); err == nil {
		t.Fatal("expected wrong dimension rejection")
	}
}

func TestEmbeddingRequestRejectsCrossWorkspaceFenceAndBudget(t *testing.T) {
	request := EmbeddingRequest{
		RequestID: "request-1", IdempotencyKey: "key-1", WorkspaceID: "workspace-a",
		Actor: ActorRef{ID: "actor-a", Kind: "user", WorkspaceID: "workspace-a"},
		Task:  &EntityRef{ID: "task-1", Kind: "task", WorkspaceID: "workspace-b"}, TaskOwnerID: "worker", TaskFence: 1,
		Provider: ProviderRef{Provider: "fake", Model: "fake-model"}, Model: "fake-model",
		SourceKind: "memo", SourceID: "memo-1", Text: "text",
		Budget: EmbeddingBudget{MaxInputBytes: 1, Dimension: EmbeddingDimension, TimeoutMS: 1000},
	}
	if err := request.Normalize(); err == nil {
		t.Fatal("expected cross-workspace task rejection")
	}
	request.Task = &EntityRef{ID: "task-1", Kind: "task", WorkspaceID: "workspace-a"}
	if err := request.Normalize(); err == nil {
		t.Fatal("expected input budget rejection")
	}
}

func TestEmbeddingRecoveryContractsAreHashOnlyAndFenced(t *testing.T) {
	vector := make([]float32, EmbeddingDimension)
	result := EmbeddingReconciliationResult{
		WorkspaceID: "workspace-a", RequestID: "request-1", Status: EmbeddingCallSucceeded,
		Provider: ProviderRef{Provider: "fake", Model: "fake-model"}, SourceHash: EmbeddingSourceHash("source"),
		ProviderRequestID: "provider-request", Vector: vector,
	}
	request := EmbeddingRecoveryFinalizeRequest{
		WorkspaceID: "workspace-a", RequestID: "request-1", OwnerID: "recovery-worker", Fence: 4,
		ExpectedEffectVersion: 3, ExpectedLinkVersion: 2, IdempotencyKey: "recovery-command-1",
		Actor: ActorRef{ID: "operator", Kind: "user", WorkspaceID: "workspace-a"}, Result: result,
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "vector") || strings.Contains(string(encoded), "0.052631") {
		t.Fatalf("recovery request serialized provider vector: %s", encoded)
	}
	request.ExpectedEffectVersion = 0
	if err := request.Normalize(); err == nil {
		t.Fatal("expected missing expected effect version rejection")
	}
}

func TestEmbeddingQueryUseRejectsRawIdentityAndContradictoryUsage(t *testing.T) {
	use := EmbeddingQueryUse{
		WorkspaceID: "workspace-a", IdempotencyKey: "use-1", RequestID: "request-1", EmbeddingRequestID: "embedding-1",
		SourceHash: EmbeddingSourceHash("query"), Provider: ProviderRef{Provider: "fake", Model: "fake-model"},
		Actor: ActorRef{ID: "actor", Kind: "user", WorkspaceID: "workspace-a"}, Route: "memo", GateReason: "cache_hit",
		UsageMeasured: true, UsageEstimated: true,
	}
	if err := use.Normalize(); err == nil {
		t.Fatal("expected contradictory usage flags rejection")
	}
	use.UsageEstimated = false
	if err := use.Normalize(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(use)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "query") {
		t.Fatal("raw query appeared in query-use identity")
	}
}
