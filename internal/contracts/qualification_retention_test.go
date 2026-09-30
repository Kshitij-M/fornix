package contracts

import (
	"testing"
	"time"
)

func retentionTestActor() AuditActor {
	return AuditActor{ID: "operator", WorkspaceID: "workspace", Kind: "human"}
}

func TestQualificationRetentionPolicyHashIsStableAndBounded(t *testing.T) {
	policy := QualificationRetentionPolicy{
		ID: "policy-one", WorkspaceID: "workspace", DeploymentID: "deployment", Revision: 1,
		SnapshotRetentionSeconds: 86400, IncidentRetentionSeconds: 172800, PolicyRetentionSeconds: 259200,
		KeepLatestSnapshots: 5, KeepLatestIncidents: 3, ProtectIncidents: true,
		PolicyHash: "", Actor: retentionTestActor(), IdempotencyKey: "policy-key",
	}
	policy.PolicyHash = policy.StableHash()
	if err := policy.Normalize(); err != nil {
		t.Fatal(err)
	}
	replay := policy
	replay.ID = "different-id"
	replay.Actor.ID = "another-operator"
	replay.CreatedAt = time.Now().UTC()
	if err := replay.Normalize(); err != nil {
		t.Fatal(err)
	}
	if replay.StableHash() != policy.PolicyHash {
		t.Fatalf("policy hash changed with provenance metadata: got %q want %q", replay.StableHash(), policy.PolicyHash)
	}
	policy.SnapshotRetentionSeconds = MaxQualificationRetentionSeconds + 1
	if err := policy.Normalize(); err == nil {
		t.Fatal("out-of-range retention duration was accepted")
	}
}

func TestQualificationRetentionMetadataAndRecoveryHashesRejectMutation(t *testing.T) {
	metadata := QualificationRetentionMetadata{
		ID: "metadata-one", WorkspaceID: "workspace", DeploymentID: "deployment",
		RecordKind: QualificationRecordSnapshot, RecordID: "snapshot-one", RecordHash: HashStrings("snapshot"),
		RetainUntil: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), CreatedAt: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC),
		IdempotencyKey: "metadata-key", Actor: retentionTestActor(),
	}
	metadata.MetadataHash = metadata.StableHash()
	if err := metadata.Normalize(); err != nil {
		t.Fatal(err)
	}
	metadata.RecordHash = HashStrings("different")
	if err := metadata.Normalize(); err == nil {
		t.Fatal("metadata authority mutation was accepted")
	}
	report := QualificationRecoveryReport{
		WorkspaceID: "workspace", DeploymentID: "deployment", AsOf: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC),
		Healthy: false, Issues: []string{"retention_metadata_hash_mismatch", "retention_metadata_missing"}, EvaluatedBy: retentionTestActor(),
	}
	report.ReportHash = report.StableHash()
	if err := report.Normalize(); err != nil {
		t.Fatal(err)
	}
	report.Issues = []string{"retention_metadata_missing", "retention_metadata_hash_mismatch"}
	if err := report.Normalize(); err != nil {
		t.Fatal(err)
	}
	if report.StableHash() == "" {
		t.Fatal("recovery report hash is empty")
	}
}

func TestQualificationRetentionPlanSortsCandidatesBeforeHashing(t *testing.T) {
	plan := QualificationRetentionPlan{
		WorkspaceID: "workspace", DeploymentID: "deployment", PolicyID: "policy", PolicyRevision: 1,
		PolicyHash: HashStrings("policy"), AsOf: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC), BatchSize: 2,
		EvaluatedBy: retentionTestActor(), Candidates: []QualificationRetentionCandidate{
			{RecordKind: QualificationRecordPolicy, RecordID: "policy-b", RecordHash: HashStrings("b"), CreatedAt: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), RetainUntil: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), Reason: RetentionCandidateNotDue},
			{RecordKind: QualificationRecordSnapshot, RecordID: "snapshot-a", RecordHash: HashStrings("a"), CreatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), RetainUntil: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Reason: RetentionCandidateEligible},
		},
	}
	plan.PlanHash = plan.StableHash()
	if err := plan.Normalize(); err != nil {
		t.Fatal(err)
	}
	if plan.Candidates[0].RecordKind != QualificationRecordPolicy {
		t.Fatalf("unexpected candidate order: %+v", plan.Candidates)
	}
	reordered := plan
	reordered.Candidates = []QualificationRetentionCandidate{plan.Candidates[0], plan.Candidates[1]}
	if err := reordered.Normalize(); err != nil {
		t.Fatal(err)
	}
	if reordered.PlanHash != plan.PlanHash {
		t.Fatalf("plan hash changed after canonical ordering: got %q want %q", reordered.PlanHash, plan.PlanHash)
	}
}
