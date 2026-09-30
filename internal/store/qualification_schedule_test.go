package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func qualificationScheduleTestRequest(f *qualificationTrustFixture, key string, due time.Time) contracts.QualificationRefreshScheduleRequest {
	return contracts.QualificationRefreshScheduleRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: "release-schedule-a",
		RequiredEvidenceKinds: []string{contracts.DeploymentEvidenceProvider}, FirstDueAt: due,
		IdempotencyKey: key, Actor: f.actor,
	}
}

func TestQualificationRefreshScheduleClaimTakeoverAndStaleFence(t *testing.T) {
	f := newQualificationTrustFixture(t)
	deployment := NewDeploymentEvidenceStore(f.pool)
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	schedule, created, err := deployment.RegisterQualificationRefreshSchedule(context.Background(), qualificationScheduleTestRequest(f, "schedule-1", now), now)
	if err != nil || !created {
		t.Fatalf("register schedule=%+v created=%v err=%v", schedule, created, err)
	}
	replayed, created, err := deployment.RegisterQualificationRefreshSchedule(context.Background(), qualificationScheduleTestRequest(f, "schedule-1", now), now.Add(time.Minute))
	if err != nil || created || replayed.ID != schedule.ID {
		t.Fatalf("schedule replay=%+v created=%v err=%v", replayed, created, err)
	}
	first, found, err := deployment.ClaimQualificationRefreshSchedule(context.Background(), f.workspace, "worker-a", 30*time.Second, now)
	if err != nil || !found || first.Fence != 1 || first.AttemptNumber != 1 {
		t.Fatalf("first claim=%+v found=%v err=%v", first, found, err)
	}
	if _, found, err := deployment.ClaimQualificationRefreshSchedule(context.Background(), f.workspace, "worker-b", 30*time.Second, now); err != nil || found {
		t.Fatalf("active lease claim found=%v err=%v", found, err)
	}
	second, found, err := deployment.ClaimQualificationRefreshSchedule(context.Background(), f.workspace, "worker-b", 30*time.Second, now.Add(31*time.Second))
	if err != nil || !found || second.Fence != 2 || !second.Takeover {
		t.Fatalf("takeover claim=%+v found=%v err=%v", second, found, err)
	}
	staleRefresh := contracts.QualificationRefreshRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", ReleaseID: "release-schedule-a",
		Items: []contracts.QualificationRefreshItem{{Kind: contracts.DeploymentEvidenceProvider, ImportID: "import-1"}},
		AsOf:  now, IdempotencyKey: "stale-refresh", Actor: f.actor,
		ScheduleAuthorization: &contracts.QualificationRefreshScheduleAuthorization{
			ScheduleID: first.Schedule.ID, AttemptID: first.AttemptID, OwnerID: first.OwnerID, Fence: first.Fence, PlanHash: first.PlanHash,
		},
	}
	if _, err := deployment.RefreshQualificationEvidence(context.Background(), staleRefresh, now.Add(31*time.Second)); !errors.Is(err, ErrQualificationScheduleOwned) && !errors.Is(err, ErrQualificationScheduleFenced) {
		t.Fatalf("stale refresh authorization err=%v", err)
	}
	if _, err := deployment.RenewQualificationRefreshSchedule(context.Background(), f.workspace, schedule.ID, "worker-a", first.Fence, time.Minute, now.Add(31*time.Second)); !errors.Is(err, ErrQualificationScheduleOwned) {
		t.Fatalf("stale renewal err=%v", err)
	}
	if _, err := deployment.RenewQualificationRefreshSchedule(context.Background(), f.workspace, schedule.ID, "worker-b", second.Fence, time.Minute, now.Add(31*time.Second)); err != nil {
		t.Fatalf("current renewal err=%v", err)
	}
	paused, err := deployment.SetQualificationRefreshScheduleState(context.Background(), f.workspace, schedule.ID, contracts.QualificationRefreshSchedulePaused, "worker-b", second.Fence, f.actor, "operator_pause", now.Add(32*time.Second))
	if err != nil || paused.Status != contracts.QualificationRefreshSchedulePaused {
		t.Fatalf("pause=%+v err=%v", paused, err)
	}
	resumed, err := deployment.SetQualificationRefreshScheduleState(context.Background(), f.workspace, schedule.ID, contracts.QualificationRefreshScheduleActive, "", 0, f.actor, "", now.Add(33*time.Second))
	if err != nil || resumed.Status != contracts.QualificationRefreshScheduleActive || resumed.NextDueAt == nil {
		t.Fatalf("resume=%+v err=%v", resumed, err)
	}
}

func TestQualificationRefreshScheduleConcurrentClaimHasOneOwner(t *testing.T) {
	f := newQualificationTrustFixture(t)
	deployment := NewDeploymentEvidenceStore(f.pool)
	now := time.Date(2026, 9, 27, 11, 0, 0, 0, time.UTC)
	if _, _, err := deployment.RegisterQualificationRefreshSchedule(context.Background(), qualificationScheduleTestRequest(f, "schedule-concurrent", now), now); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	claims := make(chan bool, 2)
	for _, owner := range []string{"worker-a", "worker-b"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			_, found, err := deployment.ClaimQualificationRefreshSchedule(context.Background(), f.workspace, owner, time.Minute, now)
			claims <- err == nil && found
		}(owner)
	}
	wg.Wait()
	close(claims)
	found := 0
	for claimed := range claims {
		if claimed {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("concurrent claims produced %d owners", found)
	}
}
