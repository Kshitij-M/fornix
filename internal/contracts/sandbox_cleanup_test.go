package contracts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validSandboxCleanupIntent() SandboxCleanupIntent {
	identity := validSandboxExecutionIdentity()
	return SandboxCleanupIntent{
		WorkspaceID: identity.WorkspaceID, ToolRunID: identity.ToolRunID, ToolAttempt: identity.ToolAttempt,
		DomainLinkID: "domain-link-a", Identity: identity, ResultHash: HashStrings("canonical result"),
		Actor: ActorRef{ID: "operator-a", Kind: "user", WorkspaceID: identity.WorkspaceID},
	}
}

func TestSandboxCleanupIntentIsWorkspaceAndIdentityBound(t *testing.T) {
	intent := validSandboxCleanupIntent()
	if err := intent.Normalize(); err != nil {
		t.Fatal(err)
	}
	first := intent.StableHash()
	if first == "" {
		t.Fatal("valid cleanup intent has no stable hash")
	}
	changed := intent
	changed.ResultHash = HashStrings("different result")
	if changed.StableHash() == first {
		t.Fatal("result hash did not alter cleanup intent identity")
	}
	changed = intent
	changed.Identity.WorkspaceID = "workspace-b"
	if err := changed.Normalize(); err == nil {
		t.Fatal("cross-workspace execution identity was accepted")
	}
	changed = intent
	changed.Identity.Backend = SandboxBackendLocalProcess
	if err := changed.Normalize(); err == nil {
		t.Fatal("local process was accepted as an externally managed cleanup target")
	}
	changed = intent
	changed.Actor.WorkspaceID = "workspace-b"
	if err := changed.Normalize(); err == nil {
		t.Fatal("cross-workspace cleanup actor was accepted")
	}
	serialized, err := json.Marshal(intent.Identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret-value", "environment", "credentials", "/Users/"} {
		if strings.Contains(strings.ToLower(string(serialized)), strings.ToLower(forbidden)) {
			t.Fatalf("cleanup identity serialized forbidden field %q", forbidden)
		}
	}
}

func TestSandboxCleanupObservationRequiresExactLeaseAndIdentity(t *testing.T) {
	intent := validSandboxCleanupIntent()
	if err := intent.Normalize(); err != nil {
		t.Fatal(err)
	}
	lease := SandboxCleanupLease{WorkspaceID: intent.WorkspaceID, JobID: "cleanup-a", OwnerID: "runner-a", Fence: 7, LeaseUntil: time.Now().Add(time.Minute)}
	job := SandboxCleanupJob{ID: lease.JobID, Intent: intent, Status: SandboxCleanupLeased, Fence: lease.Fence, LeaseUntil: &lease.LeaseUntil}
	observation := SandboxCleanupObservation{
		WorkspaceID: intent.WorkspaceID, JobID: lease.JobID, OwnerID: lease.OwnerID, Fence: lease.Fence,
		IdentityHash: intent.Identity.StableHash(), RequestHash: intent.Identity.ToolRequestHash, State: SandboxCleanupAlreadyAbsent,
	}
	if err := observation.Normalize(job, lease); err != nil {
		t.Fatalf("exact already-absent observation rejected: %v", err)
	}
	stale := observation
	stale.Fence--
	if err := stale.Normalize(job, lease); err == nil {
		t.Fatal("stale cleanup fence was accepted")
	}
	wrongRequest := observation
	wrongRequest.RequestHash = HashStrings("other request")
	if err := wrongRequest.Normalize(job, lease); err == nil {
		t.Fatal("observation for a different request was accepted")
	}
	unknown := observation
	unknown.State = SandboxCleanupUnknown
	if err := unknown.Normalize(job, lease); err == nil {
		t.Fatal("unknown outcome without a stable failure was accepted")
	}
	unknown.FailureCode = "engine_unavailable"
	if err := unknown.Normalize(job, lease); err != nil {
		t.Fatalf("bounded unknown outcome rejected: %v", err)
	}
}
