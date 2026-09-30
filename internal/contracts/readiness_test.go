package contracts

import (
	"testing"
	"time"
)

func TestReadinessSnapshotHashIgnoresObservationMetadata(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	one := ReadinessSnapshot{
		ID: "snapshot-one", WorkspaceID: "workspace", DeploymentID: "deployment", ReleaseID: "release",
		ReleaseHash: HashStrings("release"), TrustSnapshotRevision: 4, TrustSnapshotHash: HashStrings("trust"),
		RequiredKinds: []string{"provider", "release"}, ActiveEvidenceIDs: []string{"evidence-b", "evidence-a"},
		MissingKinds: []string{"topology"}, BlockedReasons: []string{"evidence_revoked:provider"}, GateHash: HashStrings("gate"),
		Ready: false, EvaluatedAt: now, CreatedAt: now,
		Actor: AuditActor{ID: "operator", WorkspaceID: "workspace", Kind: "human"}, IdempotencyKey: "request-one",
	}
	one.SnapshotHash = one.StableHash()
	two := one
	two.ID = "snapshot-two"
	two.EvaluatedAt = now.Add(3 * time.Hour)
	two.CreatedAt = now.Add(3 * time.Hour)
	two.RequestID = "request-two"
	two.IdempotencyKey = "request-two"
	two.Actor.ID = "another-operator"
	two.RequiredKinds = []string{"release", "provider"}
	two.ActiveEvidenceIDs = []string{"evidence-a", "evidence-b"}
	two.SnapshotHash = one.SnapshotHash
	if err := two.Normalize(); err != nil {
		t.Fatal(err)
	}
	if got := two.StableHash(); got != one.SnapshotHash {
		t.Fatalf("snapshot hash changed with metadata/order: got %q want %q", got, one.SnapshotHash)
	}
}

func TestReadinessSnapshotRejectsUnboundedOrUnsupportedFacts(t *testing.T) {
	snapshot := ReadinessSnapshot{
		ID: "snapshot", WorkspaceID: "workspace", DeploymentID: "deployment", ReleaseID: "release",
		ReleaseHash: HashStrings("release"), TrustSnapshotRevision: 1, TrustSnapshotHash: HashStrings("trust"),
		RequiredKinds: []string{"not-a-kind"}, GateHash: HashStrings("gate"), SnapshotHash: HashStrings("snapshot"),
		EvaluatedAt: time.Now().UTC(), Actor: AuditActor{ID: "operator", WorkspaceID: "workspace", Kind: "human"}, IdempotencyKey: "key",
	}
	if err := snapshot.Normalize(); err == nil {
		t.Fatal("unsupported required kind was accepted")
	}
}

func TestIncidentAnnotationNormalizationAndHashInputs(t *testing.T) {
	request := IncidentAnnotationRequest{
		WorkspaceID: "workspace", DeploymentID: "deployment", ReleaseID: "release", SnapshotID: "snapshot",
		Code: "provider-timeout", Disposition: IncidentDispositionAcknowledged,
		ReferenceHash: HashStrings("provider-observation"), IdempotencyKey: "annotation-key",
		Actor: AuditActor{ID: "operator", WorkspaceID: "workspace", Kind: "human"},
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	request.Disposition = "arbitrary-text"
	if err := request.Normalize(); err == nil {
		t.Fatal("unsupported disposition was accepted")
	}
}
