package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func operationAdmissionReferenceTest(workspace string) DeploymentAdmissionReference {
	return DeploymentAdmissionReference{
		WorkspaceID: workspace, DeploymentID: "deployment-a", ReleaseID: "release-a",
		ReleaseHash: HashStrings("release"), ArtifactKind: DeploymentArtifactImage,
		ArtifactHash: HashStrings("artifact"), TrustSnapshotRevision: 3,
		TrustSnapshotHash: HashStrings("snapshot"), GateHash: HashStrings("gate"),
		DecisionHash: HashStrings("decision"),
	}
}

func TestDeploymentAdmissionReferenceIsBoundedAndStable(t *testing.T) {
	first := operationAdmissionReferenceTest("workspace-a")
	second := first
	if err := first.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := second.Normalize(); err != nil {
		t.Fatal(err)
	}
	if first.StableHash() == "" || first.StableHash() != second.StableHash() {
		t.Fatalf("reference hash is not stable: %q != %q", first.StableHash(), second.StableHash())
	}
	second.GateHash = HashStrings("other-gate")
	if first.StableHash() == second.StableHash() {
		t.Fatal("gate hash did not contribute to reference identity")
	}
	raw, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "prompt") || strings.Contains(string(raw), "secret") || strings.Contains(string(raw), "attestation") {
		t.Fatalf("reference contains a raw or secret field: %s", raw)
	}
}

func TestReadyAdmissionDecisionBuildsExactReference(t *testing.T) {
	decision := DeploymentAdmissionDecision{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-a",
		ReleaseHash: HashStrings("release"), ArtifactKind: DeploymentArtifactImage,
		ArtifactHash: HashStrings("artifact"), TrustSnapshotRevision: 1,
		TrustSnapshotHash: HashStrings("snapshot"), GateHash: HashStrings("gate"), Ready: true,
	}
	reference, err := decision.Reference()
	if err != nil {
		t.Fatal(err)
	}
	if reference.DecisionHash == "" || reference.StableHash() == "" || reference.WorkspaceID != decision.WorkspaceID {
		t.Fatalf("unexpected reference: %+v", reference)
	}
	decision.Ready = false
	if _, err := decision.Reference(); err == nil {
		t.Fatal("blocked admission decision produced an operation reference")
	}
}

func TestOperationRejectsCrossWorkspaceDeploymentAdmissionReference(t *testing.T) {
	request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	reference := operationAdmissionReferenceTest("workspace-b")
	request.DeploymentAdmission = &reference
	if err := request.Normalize(); err == nil {
		t.Fatal("cross-workspace deployment admission reference was accepted")
	}
}

func TestOperationHashIncludesDeploymentAdmissionReference(t *testing.T) {
	first := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	reference := operationAdmissionReferenceTest(first.WorkspaceID)
	first.DeploymentAdmission = &reference
	second := first
	other := reference
	other.DecisionHash = HashStrings("other-decision")
	second.DeploymentAdmission = &other
	if first.StableHash() == "" || second.StableHash() == "" || first.StableHash() == second.StableHash() {
		t.Fatal("admission reference did not affect operation identity")
	}
}
