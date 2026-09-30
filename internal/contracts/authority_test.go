package contracts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func authorityTestHash(value string) string { return HashStrings("authority-test", value) }

func TestOperationAuthorityLinkHashExcludesDeliveryIdentity(t *testing.T) {
	link := OperationAuthorityLink{
		WorkspaceID: "workspace-authority", Stage: AuthorityStageResult,
		OperationID: "operation-1", OperationHash: authorityTestHash("operation"),
		OperationOwnerID: "worker-a", OperationFence: 3,
		ResultID: "result-1", ResultHash: authorityTestHash("result"),
		Actor:     ActorRef{ID: "operator", Kind: "human", WorkspaceID: "workspace-authority"},
		RequestID: "request-1", IdempotencyKey: "result:key",
		Evidence:  []AuthorityLinkReference{{Kind: "evidence", ID: "ev-1", Hash: authorityTestHash("evidence")}},
		CreatedAt: time.Now().UTC(), ID: "link-a",
	}
	if err := link.Normalize(); err != nil {
		t.Fatal(err)
	}
	firstHash := link.LinkHash
	link.ID = "link-b"
	link.CreatedAt = link.CreatedAt.Add(time.Hour)
	if err := link.Normalize(); err != nil {
		t.Fatal(err)
	}
	if link.LinkHash != firstHash || link.StableHash() != firstHash {
		t.Fatalf("delivery identity changed authority hash: %s != %s", link.LinkHash, firstHash)
	}
}

func TestOperationAuthorityLinkRejectsIncompleteAndCrossWorkspaceAuthority(t *testing.T) {
	base := OperationAuthorityLink{
		WorkspaceID: "workspace-authority", Stage: AuthorityStageResult,
		OperationID: "operation-1", OperationHash: authorityTestHash("operation"),
		Actor:     ActorRef{ID: "operator", Kind: "human", WorkspaceID: "workspace-authority"},
		RequestID: "request-1", IdempotencyKey: "result:key",
	}
	base.CredentialLeaseFence = 1
	if err := base.Normalize(); err == nil || !strings.Contains(err.Error(), "credential lease") {
		t.Fatalf("incomplete credential authority error=%v", err)
	}
	base.CredentialLeaseFence = 0
	base.Actor.WorkspaceID = "other-workspace"
	if err := base.Normalize(); err == nil || !strings.Contains(err.Error(), "actor crosses workspace") {
		t.Fatalf("cross-workspace actor error=%v", err)
	}
}

func TestOperationAuthorityLinkCanonicalizesReferencesAndEffects(t *testing.T) {
	link := OperationAuthorityLink{
		WorkspaceID: "workspace-authority", Stage: AuthorityStageAdmission,
		OperationID: "operation-1", OperationHash: authorityTestHash("operation"),
		Actor:     ActorRef{ID: "operator", Kind: "human", WorkspaceID: "workspace-authority"},
		RequestID: "request-1", IdempotencyKey: "admission:key",
		EffectIDs: []string{"effect-b", "effect-a"},
		Artifacts: []AuthorityLinkReference{{Kind: "artifact", ID: "artifact-b"}, {Kind: "artifact", ID: "artifact-a"}},
	}
	if err := link.Normalize(); err != nil {
		t.Fatal(err)
	}
	if link.EffectIDs[0] != "effect-a" || link.Artifacts[0].ID != "artifact-a" {
		t.Fatalf("authority references were not canonicalized: %+v %+v", link.EffectIDs, link.Artifacts)
	}
}

func TestEffectAuthorityIsSecretFreeAndHashBoundToSourceFacts(t *testing.T) {
	expires := time.Now().UTC().Add(time.Minute)
	authority := EffectAuthority{
		WorkspaceID: "workspace-authority", OperationID: "operation-1", OperationOwnerID: "worker-a", OperationFence: 4,
		SchemaCatalogHash: authorityTestHash("catalog"), SchemaCatalogRevision: "7",
		CredentialLeaseID: "lease-1", CredentialLeaseFence: 3, CredentialRevocationEpoch: 2,
		CredentialSourceVersion: "vault-v7", CredentialSourceExpiresAt: &expires,
	}
	if err := authority.Normalize(); err != nil {
		t.Fatal(err)
	}
	first := authority.StableHash()
	authority.CredentialSourceVersion = "vault-v8"
	if first == authority.StableHash() {
		t.Fatal("source version did not change effect authority hash")
	}
	if strings.Contains(string(mustJSON(authority)), "secret") {
		t.Fatal("effect authority serialized secret-like data")
	}
}

func TestEffectAuthorityAgentRunFenceRequiresCompleteTuple(t *testing.T) {
	base := EffectAuthority{WorkspaceID: "workspace-authority", OperationID: "operation-1", OperationOwnerID: "worker-a", OperationFence: 4}
	tests := []struct {
		name      string
		runID     string
		ownerID   string
		fence     uint64
		wantValid bool
	}{
		{name: "unbound", wantValid: true},
		{name: "missing run", ownerID: "worker-a", fence: 1},
		{name: "missing owner", runID: "run-1", fence: 1},
		{name: "missing fence", runID: "run-1", ownerID: "worker-a"},
		{name: "complete", runID: "run-1", ownerID: "worker-a", fence: 7, wantValid: true},
		{name: "out of database range", runID: "run-1", ownerID: "worker-a", fence: uint64(1 << 63)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			candidate.AgentRunID, candidate.AgentRunOwnerID, candidate.AgentRunFence = test.runID, test.ownerID, test.fence
			err := candidate.Normalize()
			if (err == nil) != test.wantValid {
				t.Fatalf("Normalize() error=%v, want valid=%t", err, test.wantValid)
			}
		})
	}
}

func TestEffectAuthorityAgentRunFenceIsHashBoundAndOptionalWhenUnbound(t *testing.T) {
	base := EffectAuthority{WorkspaceID: "workspace-authority", OperationID: "operation-1", OperationOwnerID: "worker-a", OperationFence: 4}
	unboundJSON := string(mustJSON(base))
	if strings.Contains(unboundJSON, "agent_run_") || strings.Contains(unboundJSON, "agent_run_id") {
		t.Fatalf("unbound authority serialized agent-run fields: %s", unboundJSON)
	}
	base.AgentRunID, base.AgentRunOwnerID, base.AgentRunFence = "run-1", "run-worker-a", 7
	first := base.StableHash()
	base.AgentRunFence++
	if first == base.StableHash() {
		t.Fatal("agent-run fence did not change effect authority hash")
	}
}

func mustJSON(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}
