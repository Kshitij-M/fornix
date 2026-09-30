package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func deploymentReleaseRequest(f *qualificationTrustFixture, idempotency, releaseSeed string) contracts.DeploymentReleaseRequest {
	return contracts.DeploymentReleaseRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a",
		ReleaseHash: contracts.HashStrings("release", releaseSeed), TargetHash: f.target,
		Version: "2026.09.27", CommitHash: contracts.HashStrings("commit", releaseSeed),
		RequestID: "release-request-" + idempotency, IdempotencyKey: idempotency,
		Actor: f.actor,
	}
}

func registerDeploymentEvidenceSigner(t *testing.T, f *qualificationTrustFixture, from time.Time) {
	t.Helper()
	_, _, err := f.store.RegisterSigner(context.Background(), contracts.QualificationTrustedSignerInput{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", KeyID: f.keyID,
		PublicKey: fmt.Sprintf("%x", f.public), ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour), Actor: f.actor,
	})
	if err != nil {
		t.Fatalf("register qualification signer: %v", err)
	}
}

func TestDeploymentEvidenceRegistrationIsIdempotentAndGateable(t *testing.T) {
	f := newQualificationTrustFixture(t)
	deployment := NewDeploymentEvidenceStore(f.pool)
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	registerDeploymentEvidenceSigner(t, f, from)
	f.publishSnapshot(t, 1, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{{
		DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public),
		ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour),
	}})
	now := from.Add(2 * time.Hour)
	request := deploymentReleaseRequest(f, "release-1", "one")
	first, created, err := deployment.RegisterRelease(context.Background(), request, now)
	if err != nil || !created {
		t.Fatalf("register release=%+v created=%v err=%v", first, created, err)
	}
	second, created, err := deployment.RegisterRelease(context.Background(), request, now)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("release replay=%+v created=%v err=%v", second, created, err)
	}
	page, err := deployment.ListReleases(context.Background(), f.workspace, "deployment-a", 10, "")
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != first.ID {
		t.Fatalf("release page=%+v err=%v", page, err)
	}
	if _, err := deployment.GetRelease(context.Background(), "foreign-workspace", "deployment-a", first.ID); !errors.Is(err, ErrDeploymentReleaseNotFound) {
		t.Fatalf("cross-workspace release read err=%v", err)
	}

	before, err := deployment.EvaluateGate(context.Background(), f.workspace, "deployment-a", first.ID, []string{contracts.DeploymentEvidenceProvider}, now)
	if err != nil || before.Ready || len(before.MissingKinds) != 1 || before.GateHash == "" {
		t.Fatalf("incomplete gate=%+v err=%v", before, err)
	}
	signed, raw := f.signed(t, f.keyID, f.private, "qualification-provider-1")
	imported, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signed, raw, "import-provider-1", false), from.Add(time.Hour))
	if err != nil || !imported.Created {
		t.Fatalf("import=%+v err=%v", imported, err)
	}
	linkRequest := contracts.DeploymentEvidenceLinkRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: first.ID,
		Kind: contracts.DeploymentEvidenceProvider, ImportID: imported.Record.ID,
		IdempotencyKey: "evidence-provider-1", Actor: f.actor,
	}
	link, created, err := deployment.LinkEvidence(context.Background(), linkRequest, now)
	if err != nil || !created || link.SignedHash != imported.Record.SignedHash {
		t.Fatalf("link=%+v created=%v err=%v", link, created, err)
	}
	replayed, created, err := deployment.LinkEvidence(context.Background(), linkRequest, now)
	if err != nil || created || replayed.ID != link.ID {
		t.Fatalf("link replay=%+v created=%v err=%v", replayed, created, err)
	}
	after, err := deployment.EvaluateGate(context.Background(), f.workspace, "deployment-a", first.ID, []string{contracts.DeploymentEvidenceProvider}, now)
	if err != nil || !after.Ready || after.GateHash == before.GateHash || len(after.Links) != 1 {
		t.Fatalf("complete gate=%+v err=%v", after, err)
	}
}

func TestDeploymentEvidenceRejectsLinkFromStaleTrustSnapshot(t *testing.T) {
	f := newQualificationTrustFixture(t)
	deployment := NewDeploymentEvidenceStore(f.pool)
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	registerDeploymentEvidenceSigner(t, f, from)
	entry := contracts.QualificationTrustSnapshotEntry{DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public), ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour)}
	f.publishSnapshot(t, 1, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{entry})
	now := from.Add(2 * time.Hour)
	release, _, err := deployment.RegisterRelease(context.Background(), deploymentReleaseRequest(f, "release-stale-1", "stale"), now)
	if err != nil {
		t.Fatal(err)
	}
	f.publishSnapshot(t, 2, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{entry})
	signed, raw := f.signed(t, f.keyID, f.private, "qualification-provider-stale")
	imported, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signed, raw, "import-provider-stale", false), from.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = deployment.LinkEvidence(context.Background(), contracts.DeploymentEvidenceLinkRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
		Kind: contracts.DeploymentEvidenceProvider, ImportID: imported.Record.ID,
		IdempotencyKey: "evidence-provider-stale", Actor: f.actor,
	}, from.Add(3*time.Hour))
	if !errors.Is(err, ErrDeploymentEvidenceStale) {
		t.Fatalf("stale link err=%v", err)
	}
}

func TestDeploymentEvidenceRevocationAndReplacementAreAuditable(t *testing.T) {
	f := newQualificationTrustFixture(t)
	deployment := NewDeploymentEvidenceStore(f.pool)
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	registerDeploymentEvidenceSigner(t, f, from)
	f.publishSnapshot(t, 1, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{{
		DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public),
		ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour),
	}})
	now := from.Add(2 * time.Hour)
	release, _, err := deployment.RegisterRelease(context.Background(), deploymentReleaseRequest(f, "release-lifecycle", "lifecycle"), now)
	if err != nil {
		t.Fatal(err)
	}
	signedOne, rawOne := f.signed(t, f.keyID, f.private, "qualification-provider-lifecycle-one")
	importOne, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signedOne, rawOne, "import-provider-lifecycle-one", false), from.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	linkOne, created, err := deployment.LinkEvidence(context.Background(), contracts.DeploymentEvidenceLinkRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
		Kind: contracts.DeploymentEvidenceProvider, ImportID: importOne.Record.ID,
		IdempotencyKey: "evidence-provider-lifecycle-one", Actor: f.actor,
	}, now)
	if err != nil || !created || linkOne.Status != contracts.DeploymentEvidenceLinkActive {
		t.Fatalf("initial link=%+v created=%v err=%v", linkOne, created, err)
	}
	revoked, changed, err := deployment.RevokeEvidence(context.Background(), contracts.DeploymentEvidenceRevocationRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
		Kind: contracts.DeploymentEvidenceProvider, LinkID: linkOne.ID, Reason: "provider boundary withdrawn",
		IdempotencyKey: "evidence-revoke-lifecycle-one", Actor: f.actor,
	}, now.Add(time.Minute))
	if err != nil || !changed || revoked.Status != contracts.DeploymentEvidenceLinkRevoked {
		t.Fatalf("revoked link=%+v changed=%v err=%v", revoked, changed, err)
	}
	replayed, changed, err := deployment.RevokeEvidence(context.Background(), contracts.DeploymentEvidenceRevocationRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
		Kind: contracts.DeploymentEvidenceProvider, LinkID: linkOne.ID, Reason: "provider boundary withdrawn",
		IdempotencyKey: "evidence-revoke-lifecycle-replay", Actor: f.actor,
	}, now.Add(2*time.Minute))
	if err != nil || changed || replayed.ID != linkOne.ID {
		t.Fatalf("revocation replay=%+v changed=%v err=%v", replayed, changed, err)
	}
	blocked, err := deployment.EvaluateGate(context.Background(), f.workspace, "deployment-a", release.ID, []string{contracts.DeploymentEvidenceProvider}, now.Add(2*time.Minute))
	if err != nil || blocked.Ready || !containsDeploymentReason(blocked.BlockedReasons, "evidence_revoked:provider") {
		t.Fatalf("revoked gate=%+v err=%v", blocked, err)
	}
	signedTwo, rawTwo := f.signed(t, f.keyID, f.private, "qualification-provider-lifecycle-two")
	importTwo, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signedTwo, rawTwo, "import-provider-lifecycle-two", false), from.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	invalidReplacement := contracts.DeploymentEvidenceLinkRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
		Kind: contracts.DeploymentEvidenceProvider, ImportID: importTwo.Record.ID, SupersedesLinkID: linkOne.ID,
		IdempotencyKey: "evidence-provider-lifecycle-two-invalid", Actor: f.actor,
	}
	if _, created, err := deployment.LinkEvidence(context.Background(), invalidReplacement, now.Add(3*time.Minute)); !errors.Is(err, ErrDeploymentEvidenceConflict) || created {
		t.Fatalf("revoked predecessor supersession created=%v err=%v, want fail-closed conflict", created, err)
	}
	linkTwo, created, err := deployment.LinkEvidence(context.Background(), contracts.DeploymentEvidenceLinkRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
		Kind: contracts.DeploymentEvidenceProvider, ImportID: importTwo.Record.ID,
		IdempotencyKey: "evidence-provider-lifecycle-two", Actor: f.actor,
	}, now.Add(3*time.Minute))
	if err != nil || !created || linkTwo.Status != contracts.DeploymentEvidenceLinkActive || linkTwo.SupersedesLinkID != "" {
		t.Fatalf("fresh evidence link after revocation=%+v created=%v err=%v", linkTwo, created, err)
	}
	gate, err := deployment.EvaluateGate(context.Background(), f.workspace, "deployment-a", release.ID, []string{contracts.DeploymentEvidenceProvider}, now.Add(3*time.Minute))
	if err != nil || !gate.Ready || len(gate.Links) != 2 {
		t.Fatalf("replacement gate=%+v err=%v", gate, err)
	}
	for _, link := range gate.Links {
		if link.ID == linkOne.ID && (link.Status != contracts.DeploymentEvidenceLinkRevoked || link.SupersededByLinkID != "") {
			t.Fatalf("fresh evidence link rewrote revoked predecessor: %+v", link)
		}
	}
	if _, _, err := deployment.RevokeEvidence(context.Background(), contracts.DeploymentEvidenceRevocationRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
		Kind: contracts.DeploymentEvidenceProvider, LinkID: linkOne.ID, Reason: "late mutation",
		IdempotencyKey: "evidence-revoke-superseded", Actor: f.actor,
	}, now.Add(4*time.Minute)); !errors.Is(err, ErrDeploymentEvidenceLifecycle) {
		t.Fatalf("superseded link revocation err=%v", err)
	}
}

func containsDeploymentReason(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func TestQualificationRefreshIsAtomicIdempotentAndHistoryPreserving(t *testing.T) {
	f := newQualificationTrustFixture(t)
	deployment := NewDeploymentEvidenceStore(f.pool)
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	registerDeploymentEvidenceSigner(t, f, from)
	f.publishSnapshot(t, 1, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{{
		DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public), ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour),
	}})
	now := from.Add(2 * time.Hour)
	release, _, err := deployment.RegisterRelease(context.Background(), deploymentReleaseRequest(f, "release-refresh", "refresh"), now)
	if err != nil {
		t.Fatal(err)
	}
	signedOne, rawOne := f.signed(t, f.keyID, f.private, "qualification-provider-refresh-one")
	importOne, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signedOne, rawOne, "import-provider-refresh-one", false), from.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	linkOne, _, err := deployment.LinkEvidence(context.Background(), contracts.DeploymentEvidenceLinkRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID, Kind: contracts.DeploymentEvidenceProvider,
		ImportID: importOne.Record.ID, IdempotencyKey: "link-provider-refresh-one", Actor: f.actor,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	signedTwo, rawTwo := f.signed(t, f.keyID, f.private, "qualification-provider-refresh-two")
	importTwo, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signedTwo, rawTwo, "import-provider-refresh-two", false), from.Add(90*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.QualificationRefreshRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: release.ID,
		Items: []contracts.QualificationRefreshItem{{Kind: contracts.DeploymentEvidenceProvider, ImportID: importTwo.Record.ID, SupersedesLinkID: linkOne.ID}},
		AsOf:  now.Add(time.Minute), IdempotencyKey: "refresh-provider-one", Actor: f.actor,
	}
	dryRun := request
	dryRun.IdempotencyKey = "refresh-provider-dry-run"
	dryRun.DryRun = true
	planned, err := deployment.RefreshQualificationEvidence(context.Background(), dryRun, now.Add(time.Minute))
	if err != nil || !planned.DryRun || planned.Report.ReportHash == "" || planned.Created {
		t.Fatalf("refresh dry-run=%+v err=%v", planned, err)
	}
	page, err := deployment.ListQualificationRefreshes(context.Background(), f.workspace, "deployment-a", release.ID, 10, "")
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("dry-run created durable refresh=%+v err=%v", page, err)
	}
	evidence, err := deployment.ListEvidence(context.Background(), f.workspace, "deployment-a", release.ID, 10, "")
	if err != nil || len(evidence.Items) != 1 || evidence.Items[0].ID != linkOne.ID {
		t.Fatalf("dry-run changed evidence history=%+v err=%v", evidence, err)
	}
	first, err := deployment.RefreshQualificationEvidence(context.Background(), request, now.Add(time.Minute))
	if err != nil || !first.Created || first.Report.ReportHash == "" || first.Report.Items[0].PreviousLinkID != linkOne.ID {
		t.Fatalf("refresh=%+v err=%v", first, err)
	}
	replayed, err := deployment.RefreshQualificationEvidence(context.Background(), request, now.Add(2*time.Minute))
	if err != nil || !replayed.Deduplicated || replayed.Report.ReportHash != first.Report.ReportHash {
		t.Fatalf("refresh replay=%+v err=%v", replayed, err)
	}
	page, err = deployment.ListQualificationRefreshes(context.Background(), f.workspace, "deployment-a", release.ID, 10, "")
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("refresh page=%+v err=%v", page, err)
	}
	evidence, err = deployment.ListEvidence(context.Background(), f.workspace, "deployment-a", release.ID, 10, "")
	if err != nil || len(evidence.Items) != 2 {
		t.Fatalf("evidence history=%+v err=%v", evidence, err)
	}
	signedThree, rawThree := f.signed(t, f.keyID, f.private, "qualification-provider-refresh-three")
	importThree, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signedThree, rawThree, "import-provider-refresh-three", false), from.Add(100*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	failed := request
	failed.IdempotencyKey = "refresh-provider-atomic-failure"
	failed.AsOf = now.Add(4 * time.Minute)
	failed.Items[0].ImportID = importThree.Record.ID
	failed.Items[0].SupersedesLinkID = first.Report.Items[0].LinkID
	failed.Items = append(failed.Items, contracts.QualificationRefreshItem{Kind: contracts.DeploymentEvidenceMigration, ImportID: "missing-import"})
	if _, err := deployment.RefreshQualificationEvidence(context.Background(), failed, now.Add(4*time.Minute)); err == nil {
		t.Fatal("invalid later item did not fail")
	}
	page, err = deployment.ListQualificationRefreshes(context.Background(), f.workspace, "deployment-a", release.ID, 10, "")
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("failed refresh committed=%+v err=%v", page, err)
	}
}
