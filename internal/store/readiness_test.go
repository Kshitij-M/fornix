package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func readinessFixture(t *testing.T) (*qualificationTrustFixture, *DeploymentEvidenceStore, contracts.DeploymentRelease, time.Time) {
	t.Helper()
	f := newQualificationTrustFixture(t)
	deployment := NewDeploymentEvidenceStore(f.pool)
	now := time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC)
	registerDeploymentEvidenceSigner(t, f, now)
	f.publishSnapshot(t, 1, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{{
		DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public),
		ValidFrom: now.Add(-2 * time.Hour), ValidUntil: now.Add(24 * time.Hour),
	}})
	release, _, err := deployment.RegisterRelease(context.Background(), deploymentReleaseRequest(f, "readiness-release", "readiness"), now)
	if err != nil {
		t.Fatalf("register readiness release: %v", err)
	}
	return f, deployment, release, now
}

func TestReadinessCaptureIsStableIdempotentAndWorkspaceScoped(t *testing.T) {
	f, deployment, release, now := readinessFixture(t)
	readiness := NewReadinessStore(f.pool, deployment)
	request := contracts.ReadinessSnapshotRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID,
		IdempotencyKey: "readiness-capture-one", Actor: f.actor,
	}
	first, created, err := readiness.Capture(context.Background(), request, now)
	if err != nil || !created || first.SnapshotHash == "" || first.Ready {
		t.Fatalf("first readiness snapshot=%+v created=%v err=%v", first, created, err)
	}
	replayed, created, err := readiness.Capture(context.Background(), request, now.Add(5*time.Minute))
	if err != nil || created || replayed.ID != first.ID || replayed.SnapshotHash != first.SnapshotHash || !replayed.EvaluatedAt.Equal(first.EvaluatedAt) {
		t.Fatalf("readiness replay=%+v created=%v err=%v", replayed, created, err)
	}
	dryRun, created, err := readiness.Capture(context.Background(), contracts.ReadinessSnapshotRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID,
		IdempotencyKey: "readiness-dry-run", Actor: f.actor, DryRun: true,
	}, now.Add(time.Hour))
	if err != nil || created || len(dryRun.ID) < len("dry-run-") || dryRun.SnapshotHash != first.SnapshotHash {
		t.Fatalf("readiness dry run=%+v created=%v err=%v", dryRun, created, err)
	}
	if _, err := readiness.Get(context.Background(), "foreign-workspace", release.DeploymentID, first.ID); err == nil {
		t.Fatal("cross-workspace readiness read succeeded")
	}
	page, err := readiness.List(context.Background(), f.workspace, release.DeploymentID, release.ID, 10, "")
	if err != nil || len(page.Items) != 1 || page.Items[0].SnapshotHash != first.SnapshotHash {
		t.Fatalf("readiness page=%+v err=%v", page, err)
	}
}

func TestReadinessConcurrentCapturesDeduplicateAuthorityFacts(t *testing.T) {
	f, deployment, release, now := readinessFixture(t)
	readiness := NewReadinessStore(f.pool, deployment)
	const workers = 4
	results := make([]contracts.ReadinessSnapshot, workers)
	created := make([]bool, workers)
	errs := make([]error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			results[index], created[index], errs[index] = readiness.Capture(context.Background(), contracts.ReadinessSnapshotRequest{
				WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID,
				IdempotencyKey: fmt.Sprintf("readiness-concurrent-%d", index), Actor: f.actor,
			}, now)
		}(i)
	}
	wait.Wait()
	var canonical string
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("concurrent capture %d: %v", i, errs[i])
		}
		if results[i].SnapshotHash == "" {
			t.Fatalf("concurrent capture %d has no hash", i)
		}
		if canonical == "" {
			canonical = results[i].SnapshotHash
		}
		if results[i].SnapshotHash != canonical {
			t.Fatalf("capture %d hash=%q want=%q", i, results[i].SnapshotHash, canonical)
		}
	}
	page, err := readiness.List(context.Background(), f.workspace, release.DeploymentID, release.ID, 10, "")
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("deduplicated page=%+v err=%v", page, err)
	}
}

func TestIncidentAnnotationIsBoundedIdempotentAndAuditable(t *testing.T) {
	f, deployment, release, now := readinessFixture(t)
	readiness := NewReadinessStore(f.pool, deployment)
	snapshot, _, err := readiness.Capture(context.Background(), contracts.ReadinessSnapshotRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID,
		IdempotencyKey: "readiness-for-annotation", Actor: f.actor,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.IncidentAnnotationRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID, SnapshotID: snapshot.ID,
		Code: "provider-timeout", Disposition: contracts.IncidentDispositionAcknowledged,
		ReferenceHash: contracts.HashStrings("provider-observation"), IdempotencyKey: "annotation-one", Actor: f.actor,
	}
	first, created, err := readiness.Annotate(context.Background(), request, now.Add(time.Minute))
	if err != nil || !created || first.AnnotationHash == "" || first.SnapshotHash != snapshot.SnapshotHash {
		t.Fatalf("annotation=%+v created=%v err=%v", first, created, err)
	}
	replayed, created, err := readiness.Annotate(context.Background(), request, now.Add(2*time.Minute))
	if err != nil || created || replayed.ID != first.ID {
		t.Fatalf("annotation replay=%+v created=%v err=%v", replayed, created, err)
	}
	page, err := readiness.ListAnnotations(context.Background(), f.workspace, release.DeploymentID, release.ID, snapshot.ID, 10, "")
	if err != nil || len(page.Items) != 1 || page.Items[0].AnnotationHash != first.AnnotationHash {
		t.Fatalf("annotation page=%+v err=%v", page, err)
	}
	if _, _, err := readiness.Annotate(context.Background(), contracts.IncidentAnnotationRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID, SnapshotID: snapshot.ID,
		Code: "provider-timeout", Disposition: contracts.IncidentDispositionAcknowledged,
		IdempotencyKey: "annotation-conflict", Actor: contracts.AuditActor{ID: "foreign", WorkspaceID: "foreign", Kind: "human"},
	}, now); err == nil {
		t.Fatal("cross-workspace annotation succeeded")
	}
}

func TestFreshnessPolicyAndReadinessReviewAreDeterministicAndReadOnly(t *testing.T) {
	f, deployment, release, now := readinessFixture(t)
	readiness := NewReadinessStore(f.pool, deployment)
	policyRequest := contracts.ReadinessFreshnessPolicyRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, MaxAgeSeconds: 3600,
		RequireReady: false, IdempotencyKey: "freshness-policy-one", Actor: f.actor,
	}
	policy, created, err := readiness.PublishFreshnessPolicy(context.Background(), policyRequest, now)
	if err != nil || !created || policy.Revision != 1 || policy.PolicyHash == "" {
		t.Fatalf("policy=%+v created=%v err=%v", policy, created, err)
	}
	replayed, created, err := readiness.PublishFreshnessPolicy(context.Background(), policyRequest, now.Add(time.Hour))
	if err != nil || created || replayed.ID != policy.ID {
		t.Fatalf("policy replay=%+v created=%v err=%v", replayed, created, err)
	}
	left, _, err := readiness.Capture(context.Background(), contracts.ReadinessSnapshotRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID,
		RequiredKinds: []string{contracts.DeploymentEvidenceProvider}, IdempotencyKey: "review-left", Actor: f.actor,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	right, _, err := readiness.Capture(context.Background(), contracts.ReadinessSnapshotRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID,
		RequiredKinds: []string{contracts.DeploymentEvidenceMigration}, IdempotencyKey: "review-right", Actor: f.actor,
	}, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	reviewRequest := contracts.ReadinessReviewRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID,
		LeftSnapshotID: left.ID, RightSnapshotID: right.ID, AsOf: now.Add(6 * time.Minute), Actor: f.actor,
	}
	review, err := readiness.Review(context.Background(), reviewRequest, now.Add(6*time.Minute))
	if err != nil || review.Outcome != contracts.ReadinessReviewChanged || review.ReviewHash == "" || !review.GateChanged {
		t.Fatalf("review=%+v err=%v", review, err)
	}
	repeated, err := readiness.Review(context.Background(), reviewRequest, now.Add(6*time.Minute))
	if err != nil || repeated.ReviewHash != review.ReviewHash {
		t.Fatalf("review replay=%+v err=%v", repeated, err)
	}
	stale, err := readiness.Review(context.Background(), contracts.ReadinessReviewRequest{
		WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, ReleaseID: release.ID,
		LeftSnapshotID: left.ID, RightSnapshotID: right.ID, AsOf: now.Add(2 * time.Hour), Actor: f.actor,
	}, now.Add(2*time.Hour))
	if err != nil || stale.Outcome != contracts.ReadinessReviewStale || len(stale.StaleReasons) == 0 {
		t.Fatalf("stale review=%+v err=%v", stale, err)
	}
	if _, err := readiness.Review(context.Background(), contracts.ReadinessReviewRequest{
		WorkspaceID: "foreign", DeploymentID: release.DeploymentID, ReleaseID: release.ID,
		LeftSnapshotID: left.ID, RightSnapshotID: right.ID, Actor: contracts.AuditActor{ID: "foreign", WorkspaceID: "foreign", Kind: "human"},
	}, now); err == nil {
		t.Fatal("cross-workspace review succeeded")
	}
}

func TestFreshnessPolicyConcurrentPublicationIsMonotonic(t *testing.T) {
	f, deployment, release, now := readinessFixture(t)
	readiness := NewReadinessStore(f.pool, deployment)
	const workers = 4
	policies := make([]contracts.ReadinessFreshnessPolicy, workers)
	errs := make([]error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			policies[index], _, errs[index] = readiness.PublishFreshnessPolicy(context.Background(), contracts.ReadinessFreshnessPolicyRequest{
				WorkspaceID: f.workspace, DeploymentID: release.DeploymentID, MaxAgeSeconds: 3600 + int64(index),
				IdempotencyKey: fmt.Sprintf("freshness-policy-concurrent-%d", index), Actor: f.actor,
			}, now)
		}(i)
	}
	wait.Wait()
	seen := make(map[int64]struct{}, workers)
	for i := range policies {
		if errs[i] != nil {
			t.Fatalf("policy %d: %v", i, errs[i])
		}
		seen[policies[i].Revision] = struct{}{}
	}
	if len(seen) != workers {
		t.Fatalf("concurrent policy revisions were not unique: %+v", policies)
	}
	page, err := readiness.ListFreshnessPolicies(context.Background(), f.workspace, release.DeploymentID, 20, "")
	if err != nil || len(page.Items) != workers {
		t.Fatalf("policy page=%+v err=%v", page, err)
	}
}
