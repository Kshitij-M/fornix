package contracts

import (
	"strings"
	"testing"
	"time"
)

func deploymentEvidenceTestActor(workspace string) AuditActor {
	return AuditActor{ID: "operator-1", WorkspaceID: workspace, Kind: "test"}
}

func deploymentEvidenceTestRelease(workspace string) DeploymentRelease {
	now := time.Date(2026, 9, 27, 1, 2, 3, 0, time.UTC)
	return DeploymentRelease{
		ID: "release-1", WorkspaceID: workspace, DeploymentID: "deployment-a",
		ReleaseHash: HashStrings("release"), TargetHash: HashStrings("target"),
		Version: "2026.09.27", CommitHash: HashStrings("commit"),
		TrustSnapshotRevision: 1, TrustSnapshotHash: HashStrings("snapshot"),
		Status: DeploymentReleaseActive, Actor: deploymentEvidenceTestActor(workspace), CreatedAt: now,
	}
}

func deploymentEvidenceTestLink(workspace, kind string) DeploymentEvidenceLink {
	return DeploymentEvidenceLink{
		ID: "evidence-" + kind, WorkspaceID: workspace, DeploymentID: "deployment-a",
		ReleaseID: "release-1", Kind: kind, ImportID: "import-1",
		TargetHash: HashStrings("target"), SignedHash: HashStrings("signed"),
		ObservationHash: HashStrings("observation"), ReportHash: HashStrings("report"),
		ManifestHash: HashStrings("manifest"), SourceHash: HashStrings("source"),
		TrustSnapshotRevision: 1, TrustSnapshotHash: HashStrings("snapshot"),
		Outcome: QualificationOutcomePassed, RecoveryState: DeploymentEvidenceNotApplicable,
		Actor: deploymentEvidenceTestActor(workspace),
	}
}

func TestDeploymentQualificationGateHashIsOrderIndependent(t *testing.T) {
	workspace := "workspace-a"
	first := DeploymentQualificationGate{
		WorkspaceID: workspace, DeploymentID: "deployment-a", ReleaseID: "release-1",
		ReleaseHash: HashStrings("release"), TrustSnapshotRevision: 1, TrustSnapshotHash: HashStrings("snapshot"),
		RequiredKinds:   []string{DeploymentEvidenceProvider, DeploymentEvidenceMigration},
		Links:           []DeploymentEvidenceLink{deploymentEvidenceTestLink(workspace, DeploymentEvidenceProvider), deploymentEvidenceTestLink(workspace, DeploymentEvidenceMigration)},
		SnapshotCurrent: true, Ready: true,
	}
	second := first
	second.RequiredKinds = []string{DeploymentEvidenceMigration, DeploymentEvidenceProvider}
	second.Links = []DeploymentEvidenceLink{first.Links[1], first.Links[0]}
	if err := first.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := second.Normalize(); err != nil {
		t.Fatal(err)
	}
	if first.StableHash() != second.StableHash() || first.StableHash() == "" {
		t.Fatalf("gate hash is not stable: %q != %q", first.StableHash(), second.StableHash())
	}
}

func TestDeploymentReleaseAndLinkNormalizationRejectsUnsafeValues(t *testing.T) {
	release := deploymentEvidenceTestRelease("workspace-a")
	release.ReleaseHash = "not-a-hash"
	if err := release.Normalize(); err == nil {
		t.Fatal("expected invalid release hash to fail")
	}
	link := deploymentEvidenceTestLink("workspace-a", DeploymentEvidenceProvider)
	link.Kind = "unknown"
	if err := link.Normalize(); err == nil {
		t.Fatal("expected unknown evidence kind to fail")
	}
	gate := DeploymentQualificationGate{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-1",
		ReleaseHash: HashStrings("release"), TrustSnapshotRevision: 1, TrustSnapshotHash: HashStrings("snapshot"),
		RequiredKinds: []string{DeploymentEvidenceProvider, DeploymentEvidenceProvider},
	}
	if err := gate.Normalize(); err == nil {
		t.Fatal("expected duplicate required evidence kinds to fail")
	}
}

func TestDeploymentEvidenceStableHashDoesNotContainRawActorText(t *testing.T) {
	gate := DeploymentQualificationGate{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-1",
		ReleaseHash: HashStrings("release"), TrustSnapshotRevision: 1, TrustSnapshotHash: HashStrings("snapshot"),
		RequiredKinds:   []string{DeploymentEvidenceProvider},
		Links:           []DeploymentEvidenceLink{deploymentEvidenceTestLink("workspace-a", DeploymentEvidenceProvider)},
		SnapshotCurrent: true, Ready: true,
	}
	if err := gate.Normalize(); err != nil {
		t.Fatal(err)
	}
	hash := gate.StableHash()
	if hash == "" || strings.Contains(hash, "operator-1") || strings.Contains(hash, "prompt") {
		t.Fatalf("gate hash leaked raw content or was empty: %q", hash)
	}
}

func TestDeploymentEvidenceLifecycleNormalizesAndPreservesHistoricalIdentity(t *testing.T) {
	active := deploymentEvidenceTestLink("workspace-a", DeploymentEvidenceProvider)
	if err := active.Normalize(); err != nil {
		t.Fatal(err)
	}
	if active.Status != DeploymentEvidenceLinkActive {
		t.Fatalf("default link status=%q, want active", active.Status)
	}
	revoked := active
	revoked.Status = DeploymentEvidenceLinkRevoked
	revoked.RevokedAt = ptrTime(time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC))
	revoked.RevocationReason = "provider boundary withdrawn"
	if err := revoked.Normalize(); err != nil {
		t.Fatal(err)
	}
	if deploymentEvidenceGateHash(active) == deploymentEvidenceGateHash(revoked) {
		t.Fatal("lifecycle state did not change its gate identity")
	}
	superseded := active
	superseded.Status = DeploymentEvidenceLinkSuperseded
	superseded.SupersededByLinkID = "evidence-successor"
	if err := superseded.Normalize(); err != nil {
		t.Fatal(err)
	}
	invalid := revoked
	invalid.RevocationReason = "bad\nreason"
	if err := invalid.Normalize(); err == nil {
		t.Fatal("newline-bearing revocation reason was accepted")
	}
}

func TestDeploymentEvidenceRevocationRequestIsBounded(t *testing.T) {
	request := DeploymentEvidenceRevocationRequest{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-1",
		Kind: DeploymentEvidenceProvider, LinkID: "evidence-1", Reason: "incident response",
		IdempotencyKey: "revoke-1", Actor: deploymentEvidenceTestActor("workspace-a"),
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	request.Actor.WorkspaceID = "workspace-b"
	if err := request.Normalize(); err == nil {
		t.Fatal("cross-workspace revocation actor was accepted")
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

func deploymentEvidenceGateHash(l DeploymentEvidenceLink) string {
	gate := DeploymentQualificationGate{
		WorkspaceID: l.WorkspaceID, DeploymentID: l.DeploymentID, ReleaseID: l.ReleaseID,
		ReleaseHash: HashStrings("release"), TrustSnapshotRevision: l.TrustSnapshotRevision,
		TrustSnapshotHash: l.TrustSnapshotHash, RequiredKinds: []string{l.Kind}, Links: []DeploymentEvidenceLink{l},
	}
	return gate.StableHash()
}
