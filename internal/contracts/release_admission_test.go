package contracts

import (
	"strings"
	"testing"
	"time"
)

func TestDeploymentAdmissionDecisionHashIsStableAndRedacted(t *testing.T) {
	now := time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC)
	decision := DeploymentAdmissionDecision{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-1",
		ReleaseHash: HashStrings("release"), ArtifactKind: DeploymentArtifactImage,
		ArtifactHash: HashStrings("image"), TrustSnapshotRevision: 1, TrustSnapshotHash: HashStrings("snapshot"),
		GateHash: HashStrings("gate"), Ready: false, BlockedReasons: []string{"z_reason", "a_reason"},
		Verification: &DeploymentReleaseVerification{
			ID: "verification-1", WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-1",
			ReleaseHash: HashStrings("release"), TargetHash: HashStrings("target"), ArtifactKind: DeploymentArtifactImage,
			ArtifactHash: HashStrings("image"), AttestationHash: HashStrings("attestation"), GateHash: HashStrings("gate"),
			TrustSnapshotRevision: 1, TrustSnapshotHash: HashStrings("snapshot"), Status: DeploymentVerificationVerified,
			Actor: deploymentEvidenceTestActor("workspace-a"), CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		},
	}
	other := decision
	other.BlockedReasons = []string{"a_reason", "z_reason"}
	if err := decision.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := other.Normalize(); err != nil {
		t.Fatal(err)
	}
	if decision.StableHash() == "" || decision.StableHash() != other.StableHash() {
		t.Fatalf("decision hash is unstable: %q != %q", decision.StableHash(), other.StableHash())
	}
	if strings.Contains(decision.StableHash(), "operator") || strings.Contains(decision.StableHash(), "secret") {
		t.Fatalf("decision hash contains raw text: %q", decision.StableHash())
	}
}

func TestDeploymentReleaseVerificationRejectsUnknownKindAndExpiredWindow(t *testing.T) {
	now := time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC)
	verification := DeploymentReleaseVerification{
		ID: "verification-1", WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-1",
		ReleaseHash: HashStrings("release"), TargetHash: HashStrings("target"), ArtifactKind: "registry",
		ArtifactHash: HashStrings("image"), AttestationHash: HashStrings("attestation"), GateHash: HashStrings("gate"),
		TrustSnapshotRevision: 1, TrustSnapshotHash: HashStrings("snapshot"), Status: DeploymentVerificationVerified,
		Actor: deploymentEvidenceTestActor("workspace-a"), CreatedAt: now, ExpiresAt: now.Add(-time.Hour),
	}
	if err := verification.Normalize(); err == nil {
		t.Fatal("expected unknown artifact kind to fail")
	}
	verification.ArtifactKind = DeploymentArtifactImage
	if err := verification.Normalize(); err == nil {
		t.Fatal("expected expired verification window to fail")
	}
}
