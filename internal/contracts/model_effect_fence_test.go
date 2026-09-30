package contracts

import "testing"

func TestModelRequestAgentRunFenceRequiresCompleteScopedLease(t *testing.T) {
	request := NewModelRequest("w1", "fake", "fake-model", "hello")
	request.AgentRun = &EntityRef{ID: "agent-1", Kind: "agent_run", WorkspaceID: "w1"}
	if err := request.Normalize(); err == nil {
		t.Fatal("expected incomplete agent-run fence to fail closed")
	}
	request.AgentRunOwnerID = "worker-a"
	request.AgentRunFence = 3
	if err := request.Normalize(); err != nil {
		t.Fatalf("valid agent-run fence rejected: %v", err)
	}
	wrongWorkspace := request
	wrongWorkspace.AgentRun = &EntityRef{ID: "agent-1", Kind: "agent_run", WorkspaceID: "w2"}
	if err := wrongWorkspace.Normalize(); err == nil {
		t.Fatal("expected cross-workspace agent-run reference to fail closed")
	}
}

func TestModelRequestHashIgnoresDeliveryFence(t *testing.T) {
	request := NewModelRequest("w1", "fake", "fake-model", "hello")
	request.AgentRun = &EntityRef{ID: "agent-1", Kind: "agent_run", WorkspaceID: "w1"}
	request.AgentRunOwnerID, request.AgentRunFence = "worker-a", 1
	first, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	request.AgentRunOwnerID, request.AgentRunFence = "worker-b", 2
	second, err := request.RequestHash()
	if err != nil || first != second {
		t.Fatalf("delivery fence changed logical model hash: first=%s second=%s err=%v", first, second, err)
	}
}
