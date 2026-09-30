package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestDeploymentReleaseVerificationBindsGateAndAdmission(t *testing.T) {
	f := newQualificationTrustFixture(t)
	deployment := NewDeploymentEvidenceStore(f.pool)
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	registerDeploymentEvidenceSigner(t, f, from)
	entry := contracts.QualificationTrustSnapshotEntry{DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public), ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour)}
	f.publishSnapshot(t, 1, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{entry})
	now := from.Add(2 * time.Hour)
	release, _, err := deployment.RegisterRelease(context.Background(), deploymentReleaseRequest(f, "release-verify-1", "verify"), now)
	if err != nil {
		t.Fatal(err)
	}
	signed, raw := f.signed(t, f.keyID, f.private, "qualification-provider-verify")
	imported, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signed, raw, "import-provider-verify", false), from.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = deployment.LinkEvidence(context.Background(), contracts.DeploymentEvidenceLinkRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID, Kind: contracts.DeploymentEvidenceProvider,
		ImportID: imported.Record.ID, IdempotencyKey: "evidence-provider-verify", Actor: f.actor,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range contracts.DeploymentDefaultRequiredEvidenceKinds {
		signedRequired, rawRequired := f.signed(t, f.keyID, f.private, "qualification-"+kind+"-verify")
		importRequired, importErr := f.store.ImportAuthorized(context.Background(), f.importRequest(signedRequired, rawRequired, "import-"+kind+"-verify", false), from.Add(time.Hour))
		if importErr != nil {
			t.Fatalf("import %s qualification: %v", kind, importErr)
		}
		if _, _, linkErr := deployment.LinkEvidence(context.Background(), contracts.DeploymentEvidenceLinkRequest{
			WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
			Kind: kind, ImportID: importRequired.Record.ID,
			IdempotencyKey: "evidence-" + kind + "-verify", Actor: f.actor,
		}, now); linkErr != nil {
			t.Fatalf("link %s qualification: %v", kind, linkErr)
		}
	}
	gate, err := deployment.EvaluateGate(context.Background(), f.workspace, "deployment-a", release.ID, nil, now)
	if err != nil || !gate.Ready {
		t.Fatalf("gate=%+v err=%v", gate, err)
	}
	artifactHash := contracts.HashStrings("image", "verify")
	verificationRequest := contracts.DeploymentReleaseVerificationRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
		ReleaseHash: release.ReleaseHash, TargetHash: release.TargetHash, ArtifactKind: contracts.DeploymentArtifactImage,
		ArtifactHash: artifactHash, AttestationHash: contracts.HashStrings("attestation", "verify"), GateHash: gate.GateHash,
		SourceReference: "deployment-proof-1", ExpiresAt: now.Add(time.Hour), IdempotencyKey: "verification-1", Actor: f.actor,
	}
	verification, created, err := deployment.RegisterVerification(context.Background(), verificationRequest, now)
	if err != nil || !created {
		t.Fatalf("verification=%+v created=%v err=%v", verification, created, err)
	}
	replayed, created, err := deployment.RegisterVerification(context.Background(), verificationRequest, now)
	if err != nil || created || replayed.ID != verification.ID {
		t.Fatalf("verification replay=%+v created=%v err=%v", replayed, created, err)
	}
	decision, err := deployment.EvaluateAdmission(context.Background(), f.workspace, "deployment-a", release.ID, contracts.DeploymentArtifactImage, artifactHash, now)
	if err != nil || !decision.Ready || decision.DecisionHash == "" {
		t.Fatalf("admission=%+v err=%v", decision, err)
	}
	reference := contracts.DeploymentAdmissionReference{
		WorkspaceID: decision.WorkspaceID, DeploymentID: decision.DeploymentID, ReleaseID: decision.ReleaseID,
		ReleaseHash: decision.ReleaseHash, ArtifactKind: decision.ArtifactKind, ArtifactHash: decision.ArtifactHash,
		TrustSnapshotRevision: decision.TrustSnapshotRevision, TrustSnapshotHash: decision.TrustSnapshotHash,
		GateHash: decision.GateHash, DecisionHash: decision.DecisionHash,
	}
	tx, err := beginWorkspaceTx(context.Background(), f.pool, f.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := deployment.ValidateAdmissionReferenceTx(context.Background(), tx, reference, now); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatalf("validate current admission reference: %v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	wrongReference := reference
	wrongReference.DecisionHash = contracts.HashStrings("stale-decision")
	tx, err = beginWorkspaceTx(context.Background(), f.pool, f.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := deployment.ValidateAdmissionReferenceTx(context.Background(), tx, wrongReference, now); !errors.Is(err, ErrDeploymentAdmissionReference) {
		_ = tx.Rollback(context.Background())
		t.Fatalf("stale admission reference err=%v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	wrong, err := deployment.EvaluateAdmission(context.Background(), f.workspace, "deployment-a", release.ID, contracts.DeploymentArtifactImage, contracts.HashStrings("wrong"), now)
	if err != nil || wrong.Ready || !containsString(wrong.BlockedReasons, "artifact_hash_mismatch") {
		t.Fatalf("wrong artifact admission=%+v err=%v", wrong, err)
	}
	if err := deployment.RevokeVerification(context.Background(), f.workspace, "deployment-a", release.ID, contracts.DeploymentArtifactImage, f.actor, now); err != nil {
		t.Fatal(err)
	}
	revoked, err := deployment.EvaluateAdmission(context.Background(), f.workspace, "deployment-a", release.ID, contracts.DeploymentArtifactImage, artifactHash, now)
	if err != nil || revoked.Ready || !containsString(revoked.BlockedReasons, "release_verification_revoked") {
		t.Fatalf("revoked admission=%+v err=%v", revoked, err)
	}
}

func TestDeploymentReleaseVerificationFailsClosedForStaleGateAndWorkspace(t *testing.T) {
	f := newQualificationTrustFixture(t)
	deployment := NewDeploymentEvidenceStore(f.pool)
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	registerDeploymentEvidenceSigner(t, f, from)
	entry := contracts.QualificationTrustSnapshotEntry{DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public), ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour)}
	f.publishSnapshot(t, 1, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{entry})
	now := from.Add(2 * time.Hour)
	release, _, err := deployment.RegisterRelease(context.Background(), deploymentReleaseRequest(f, "release-stale-verification", "stale-verification"), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deployment.GetVerification(context.Background(), "foreign-workspace", "deployment-a", release.ID, contracts.DeploymentArtifactRelease); !errors.Is(err, ErrDeploymentVerificationNotFound) {
		t.Fatalf("cross-workspace verification err=%v", err)
	}
	if _, _, err := deployment.RegisterVerification(context.Background(), contracts.DeploymentReleaseVerificationRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID, ReleaseHash: release.ReleaseHash,
		TargetHash: release.TargetHash, ArtifactKind: contracts.DeploymentArtifactRelease, ArtifactHash: contracts.HashStrings("release-artifact"),
		AttestationHash: contracts.HashStrings("attestation-stale"), GateHash: contracts.HashStrings("not-current"), ExpiresAt: now.Add(time.Hour),
		IdempotencyKey: "verification-stale-gate", Actor: f.actor,
	}, now); !errors.Is(err, ErrDeploymentAdmissionNotReady) {
		t.Fatalf("stale gate registration err=%v", err)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
