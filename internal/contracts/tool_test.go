package contracts

import (
	"strings"
	"testing"
)

func TestToolRequestHashExcludesApprovalAndTransportIdentity(t *testing.T) {
	request := ToolRequest{
		WorkspaceID: "w1", RequestID: "request-a", IdempotencyKey: "run-a",
		ToolID: "fornix.echo", Argv: []string{"/bin/echo", "hello"},
		Mode: ToolModeInteractive, ApprovalID: "approval-a", CausationID: "cause-a",
		Budget: DefaultSandboxProfile(),
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	first, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	request.RequestID = "request-b"
	request.IdempotencyKey = "run-b"
	request.Mode = ToolModePreApproved
	request.ApprovalID = "approval-b"
	request.CausationID = "cause-b"
	second, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("authorization/transport fields changed request hash: %s != %s", first, second)
	}
}

func TestToolRequestRedactedEvidenceDoesNotContainEnvironmentValue(t *testing.T) {
	request := ToolRequest{WorkspaceID: "w1", IdempotencyKey: "run-a", ToolID: "fornix.echo", Argv: []string{"/bin/echo", "x"}, Environment: map[string]string{"TOKEN": "secret-value"}, Budget: DefaultSandboxProfile()}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	evidence, err := request.RedactedEvidence()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(evidence), "secret-value") || !strings.Contains(string(evidence), "[REDACTED]") {
		t.Fatalf("environment secret was not redacted: %s", evidence)
	}
}

func TestToolDefinitionRejectsShellExecutable(t *testing.T) {
	definition := ToolDefinition{ID: "shell", Name: "shell", Version: "1", Capability: "process", Executable: "/bin/sh", Enabled: true, Sandbox: DefaultSandboxProfile()}
	if err := definition.Normalize(); err == nil {
		t.Fatal("expected shell executable to be rejected")
	}
}
