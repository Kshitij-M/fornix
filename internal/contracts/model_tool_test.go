package contracts

import (
	"strings"
	"testing"
)

func TestModelToolDefinitionAuthorityHashMustBeSHA256Hex(t *testing.T) {
	valid := ModelToolDefinition{Name: "inspect", DefinitionHash: strings.Repeat("A", 64)}
	if err := valid.Normalize(); err != nil {
		t.Fatalf("valid authority hash rejected: %v", err)
	}
	if valid.DefinitionHash != strings.Repeat("a", 64) {
		t.Fatalf("authority hash was not normalized: %q", valid.DefinitionHash)
	}
	for _, hash := range []string{"short", strings.Repeat("g", 64), strings.Repeat("a", 63)} {
		definition := ModelToolDefinition{Name: "inspect", DefinitionHash: hash}
		if err := definition.Normalize(); err == nil {
			t.Errorf("accepted malformed authority hash %q", hash)
		}
	}
}

func TestAgentRunStateHashBindsImmutableToolCatalog(t *testing.T) {
	base := AgentRun{
		ID: "run-1", WorkspaceID: "workspace-1", RequestHash: "request-hash-1",
		Tools: []ModelToolDefinition{{Name: "inspect", Description: "Inspect safely", DefinitionHash: strings.Repeat("a", 64), Parameters: []byte(`{"type":"object"}`)}},
		State: AgentRunRunning, Phase: AgentPhaseModel,
	}
	first := base.ComputeStateHash()
	changed := base
	changed.Tools = []ModelToolDefinition{{Name: "inspect", Description: "Different schema", DefinitionHash: strings.Repeat("b", 64), Parameters: []byte(`{"type":"object","required":["argv"]}`)}}
	if second := changed.ComputeStateHash(); first == second {
		t.Fatal("state hash ignored the immutable tool catalog")
	}
	changed = base
	changed.RequestHash = "request-hash-2"
	if second := changed.ComputeStateHash(); first == second {
		t.Fatal("state hash ignored the immutable request identity")
	}
}
