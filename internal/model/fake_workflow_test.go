package model

import (
	"context"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestFakeReferenceWorkflowToolCallIsBounded(t *testing.T) {
	provider := NewFakeProvider(FakeConfig{})
	request := contracts.ModelRequest{
		SchemaVersion: contracts.ModelSchemaVersion,
		RequestID:     "reference-request", IdempotencyKey: "reference-request",
		WorkspaceID: "workspace-a", Provider: contracts.ProviderRef{Provider: "fake"},
		Prompt: "[fornix-reference-workflow] workdir=/workspace/fixtures/reference-repo read README.md",
		Tools:  []contracts.ModelToolDefinition{{Name: "fornix.repository.read"}},
		Budget: contracts.ModelBudget{MaxOutputTokens: 64}, RetryPolicy: contracts.DefaultRetryPolicy(),
	}
	first, err := provider.Complete(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.ToolCalls) != 1 || first.ToolCalls[0].ToolID != "fornix.repository.read" {
		t.Fatalf("expected one repository tool call, got %+v", first.ToolCalls)
	}
	request.Messages = []contracts.ModelMessage{{Role: "assistant", Content: first.Content, ToolCalls: first.ToolCalls}, {Role: "tool", Content: "read-only output"}}
	second, err := provider.Complete(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.ToolCalls) != 0 || second.Content == "" {
		t.Fatalf("expected terminal fake response after tool output, got %+v", second)
	}
}
