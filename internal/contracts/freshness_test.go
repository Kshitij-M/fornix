package contracts

import (
	"testing"
	"time"
)

func TestReadinessFreshnessPolicyHashIsStable(t *testing.T) {
	policy := ReadinessFreshnessPolicy{
		ID: "policy", WorkspaceID: "workspace", DeploymentID: "deployment", Revision: 1,
		MaxAgeSeconds: ReadinessFreshnessDefaultAge, RequireReady: true,
		PolicyHash: HashStrings("placeholder"), Actor: AuditActor{ID: "operator", WorkspaceID: "workspace", Kind: "human"},
		IdempotencyKey: "policy-key",
	}
	policy.PolicyHash = policy.StableHash()
	if err := policy.Normalize(); err != nil {
		t.Fatal(err)
	}
	other := policy
	other.ID, other.Revision, other.IdempotencyKey = "policy-two", 2, "policy-two-key"
	if err := other.Normalize(); err != nil {
		t.Fatal(err)
	}
	if other.PolicyHash != policy.PolicyHash {
		t.Fatalf("policy hash changed with identity metadata")
	}
}

func TestReadinessReviewNormalizesDeterministically(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	review := ReadinessReview{
		WorkspaceID: "workspace", DeploymentID: "deployment", ReleaseID: "release",
		LeftSnapshotID: "left", RightSnapshotID: "right", LeftSnapshotHash: HashStrings("left"), RightSnapshotHash: HashStrings("right"),
		PolicyID: "policy", PolicyRevision: 1, PolicyHash: HashStrings("policy"), MaxAgeSeconds: 3600,
		AsOf: now, LeftEvaluatedAt: now.Add(-time.Minute), RightEvaluatedAt: now.Add(-2 * time.Minute),
		LeftAgeSeconds: 60, RightAgeSeconds: 120, LeftFresh: true, RightFresh: true,
		LeftReady: false, RightReady: true, GateChanged: true, EvidenceAdded: []string{"evidence-b", "evidence-a"},
		BlockedResolved: []string{"evidence_revoked:provider"}, Outcome: ReadinessReviewImproved,
		EvaluatedBy: AuditActor{ID: "operator", WorkspaceID: "workspace", Kind: "human"},
	}
	review.ReviewHash = review.StableHash()
	if err := review.Normalize(); err != nil {
		t.Fatal(err)
	}
	if review.EvidenceAdded[0] != "evidence-a" || review.ReviewHash != review.StableHash() {
		t.Fatalf("review did not normalize deterministically: %+v", review)
	}
}

func TestReadinessReviewRejectsSameSnapshotAndInvalidAge(t *testing.T) {
	review := ReadinessReview{WorkspaceID: "workspace", DeploymentID: "deployment", ReleaseID: "release", LeftSnapshotID: "same", RightSnapshotID: "same"}
	if err := review.Normalize(); err == nil {
		t.Fatal("same snapshot comparison was accepted")
	}
	policy := ReadinessFreshnessPolicyRequest{WorkspaceID: "workspace", DeploymentID: "deployment", MaxAgeSeconds: 0, IdempotencyKey: "key", Actor: AuditActor{ID: "operator", WorkspaceID: "workspace", Kind: "human"}}
	if err := policy.Normalize(); err == nil {
		t.Fatal("zero freshness age was accepted")
	}
}
