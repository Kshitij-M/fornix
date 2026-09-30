package contracts

import (
	"strings"
	"testing"
	"time"
)

func validQualificationScheduleRequest() QualificationRefreshScheduleRequest {
	return QualificationRefreshScheduleRequest{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-a",
		RequiredEvidenceKinds:  []string{DeploymentEvidenceProvider, DeploymentEvidenceBackupRestore},
		RequiredRecoveryDrills: []string{RecoveryDrillPITR}, FirstDueAt: time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC),
		IdempotencyKey: "schedule-a", Actor: AuditActor{ID: "operator", Kind: "human", WorkspaceID: "workspace-a"},
	}
}

func TestQualificationRefreshScheduleHashIsStableAndBounded(t *testing.T) {
	request := validQualificationScheduleRequest()
	first := request.StableHash()
	if first == "" || len(first) != MaxDomainHashLength {
		t.Fatalf("expected stable hash, got %q", first)
	}
	request.Actor.ID = "different-operator"
	request.RequestID = "request-2"
	if got := request.StableHash(); got != first {
		t.Fatalf("audit and delivery metadata changed config hash: %s != %s", got, first)
	}
}

func TestQualificationRefreshScheduleRejectsUnknownRequirements(t *testing.T) {
	request := validQualificationScheduleRequest()
	request.RequiredEvidenceKinds = []string{"unknown"}
	if err := request.Normalize(); err == nil {
		t.Fatal("expected unknown evidence kind to fail closed")
	}
	request = validQualificationScheduleRequest()
	request.RequiredRecoveryDrills = []string{"unknown"}
	if err := request.Normalize(); err == nil {
		t.Fatal("expected unknown recovery drill to fail closed")
	}
}

func TestQualificationRefreshSchedulePlanHashBindsFence(t *testing.T) {
	request := validQualificationScheduleRequest()
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	schedule := QualificationRefreshSchedule{ID: "schedule-a", ConfigHash: request.StableHash()}
	asOf := request.FirstDueAt
	first := QualificationRefreshSchedulePlanHash(schedule, "attempt-a", "worker-a", 1, asOf, 1)
	if first == "" || strings.Contains(first, "worker-a") {
		t.Fatalf("plan hash should be opaque: %q", first)
	}
	if second := QualificationRefreshSchedulePlanHash(schedule, "attempt-a", "worker-a", 2, asOf, 1); second == first {
		t.Fatal("takeover fence must change plan hash")
	}
}

func TestQualificationRefreshScheduleBackoffIsCapped(t *testing.T) {
	if got := QualificationRefreshScheduleBackoffMS(100, 350, 1); got != 100 {
		t.Fatalf("attempt 1 backoff = %d", got)
	}
	if got := QualificationRefreshScheduleBackoffMS(100, 350, 3); got != 350 {
		t.Fatalf("capped backoff = %d", got)
	}
	if got := QualificationRefreshScheduleBackoffMS(100, 350, 20); got != 350 {
		t.Fatalf("large attempt backoff = %d", got)
	}
}

func TestQualificationRefreshScheduleCompletionRequiresReportForSuccess(t *testing.T) {
	request := QualificationRefreshScheduleCompletionRequest{WorkspaceID: "workspace-a", ScheduleID: "schedule-a", AttemptID: "attempt-a", OwnerID: "worker-a", Fence: 1, PlanHash: strings.Repeat("a", 64), AsOf: time.Now(), Outcome: QualificationRefreshScheduleOutcomeSucceeded, IdempotencyKey: "attempt-key", Actor: AuditActor{ID: "worker-a", Kind: "worker", WorkspaceID: "workspace-a"}}
	if err := request.Normalize(); err == nil {
		t.Fatal("successful completion without refresh report must fail")
	}
}
