package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

// qualificationGeneration is the bounded process view of one durable trust
// snapshot. It contains only identifiers and hashes; the signed source bytes
// remain in PostgreSQL and are disclosed explicitly when needed.
type qualificationGeneration struct {
	WorkspaceID  string    `json:"workspace_id"`
	DeploymentID string    `json:"deployment_id"`
	SnapshotID   string    `json:"snapshot_id"`
	Revision     int64     `json:"revision"`
	SnapshotHash string    `json:"snapshot_hash"`
	ExpiresAt    time.Time `json:"expires_at"`
	LoadedAt     time.Time `json:"loaded_at"`
}

func (s *server) reloadQualificationAuthority(ctx context.Context, workspaceID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if !s.qualificationTrustRequired {
		return nil
	}
	if workspaceID == "" || strings.TrimSpace(s.qualificationDeploymentID) == "" {
		return fmt.Errorf("qualification trust workspace and deployment are required")
	}
	if s.qualificationTrust == nil {
		return fmt.Errorf("qualification trust store is not configured")
	}
	record, err := s.qualificationTrust.CurrentSnapshot(ctx, workspaceID, s.qualificationDeploymentID, time.Now().UTC())
	if err != nil {
		return err
	}
	if record.Snapshot.WorkspaceID != workspaceID || record.Snapshot.DeploymentID != s.qualificationDeploymentID {
		return fmt.Errorf("qualification trust snapshot crosses workspace or deployment scope")
	}
	now := time.Now().UTC()
	s.authorityMu.Lock()
	if s.qualificationGenerations == nil {
		s.qualificationGenerations = make(map[string]qualificationGeneration)
	}
	s.qualificationGenerations[workspaceID] = qualificationGeneration{
		WorkspaceID: workspaceID, DeploymentID: record.Snapshot.DeploymentID,
		SnapshotID: record.ID, Revision: record.Snapshot.Revision,
		SnapshotHash: record.Snapshot.SnapshotHash, ExpiresAt: record.Snapshot.ExpiresAt,
		LoadedAt: now,
	}
	s.authorityMu.Unlock()
	return nil
}

func (s *server) setQualificationAuthorityStatus(ready bool, failure string, loadedAt time.Time, workspaceCount int) {
	s.authorityMu.Lock()
	s.qualificationAuthorityReady = ready
	s.qualificationAuthorityError = failure
	if !loadedAt.IsZero() {
		s.qualificationAuthorityLoaded = loadedAt
	}
	if workspaceCount >= 0 {
		s.qualificationAuthorityCount = workspaceCount
	}
	s.authorityMu.Unlock()
}

func (s *server) qualificationAuthorityStatusValues() (bool, string) {
	s.authorityMu.RLock()
	defer s.authorityMu.RUnlock()
	return s.qualificationAuthorityReady, s.qualificationAuthorityError
}

func (s *server) qualificationAuthorityWorkspaceCount() int {
	s.authorityMu.RLock()
	defer s.authorityMu.RUnlock()
	return s.qualificationAuthorityCount
}

func (s *server) qualificationAuthorityStatus() map[string]any {
	s.authorityMu.RLock()
	defer s.authorityMu.RUnlock()
	status := map[string]any{
		"required":   s.qualificationTrustRequired,
		"ready":      s.qualificationAuthorityReady,
		"deployment": s.qualificationDeploymentID,
		"workspaces": s.qualificationAuthorityCount,
	}
	if !s.qualificationAuthorityLoaded.IsZero() {
		status["loaded_at"] = s.qualificationAuthorityLoaded.UTC().Format(time.RFC3339Nano)
	}
	if s.qualificationAuthorityError != "" {
		status["error"] = s.qualificationAuthorityError
	}
	generations := make([]qualificationGeneration, 0, len(s.qualificationGenerations))
	for _, generation := range s.qualificationGenerations {
		generations = append(generations, generation)
	}
	// Workspace IDs are stable normalized identifiers; deterministic status
	// output matters for operator diffs and replay diagnostics.
	for i := 1; i < len(generations); i++ {
		for j := i; j > 0 && generations[j].WorkspaceID < generations[j-1].WorkspaceID; j-- {
			generations[j], generations[j-1] = generations[j-1], generations[j]
		}
	}
	if len(generations) > 0 {
		status["generations"] = generations
	}
	return status
}

func (s *server) qualificationAuthorityImportReady(ctx context.Context, workspaceID string) error {
	if !s.qualificationTrustRequired {
		return nil
	}
	if err := s.reloadQualificationAuthority(ctx, workspaceID); err != nil {
		s.setQualificationAuthorityStatus(false, shortError(err, 320), time.Time{}, s.qualificationAuthorityWorkspaceCount())
		return err
	}
	return nil
}

func (s *server) reloadQualificationRelease(ctx context.Context, workspaceID string) error {
	if !s.qualificationReleaseRequired {
		return nil
	}
	if workspaceID == "" || strings.TrimSpace(s.qualificationDeploymentID) == "" || strings.TrimSpace(s.qualificationReleaseID) == "" {
		return fmt.Errorf("qualification release workspace, deployment, and release are required")
	}
	if s.deploymentEvidence == nil {
		return fmt.Errorf("deployment evidence store is not configured")
	}
	decision, err := s.deploymentEvidence.EvaluateAdmission(ctx, workspaceID, s.qualificationDeploymentID, s.qualificationReleaseID, s.qualificationReleaseKind, "", time.Now().UTC())
	if err != nil {
		return err
	}
	if !decision.Ready {
		return fmt.Errorf("qualification release admission blocked: %s", strings.Join(decision.BlockedReasons, ","))
	}
	reference, err := decision.Reference()
	if err != nil {
		return fmt.Errorf("create qualification release admission reference: %w", err)
	}
	s.authorityMu.Lock()
	if s.qualificationReleaseReferences == nil {
		s.qualificationReleaseReferences = make(map[string]contracts.DeploymentAdmissionReference)
	}
	s.qualificationReleaseReferences[workspaceID] = reference
	s.authorityMu.Unlock()
	return nil
}

// deploymentAdmissionReference supplies the latest startup/refreshed
// hash-only release authority to built-in dynamic adapters. OperationStore
// revalidates the reference in the same transaction as effect reservation, so
// this process-local copy is a bounded hint rather than a second authority.
func (s *server) deploymentAdmissionReference(ctx context.Context, workspaceID string) (*contracts.DeploymentAdmissionReference, error) {
	if s == nil || !s.qualificationReleaseRequired {
		return nil, nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	s.authorityMu.RLock()
	reference, found := s.qualificationReleaseReferences[workspaceID]
	s.authorityMu.RUnlock()
	if found {
		return &reference, nil
	}
	if err := s.reloadQualificationRelease(ctx, workspaceID); err != nil {
		return nil, err
	}
	s.authorityMu.RLock()
	reference, found = s.qualificationReleaseReferences[workspaceID]
	s.authorityMu.RUnlock()
	if !found {
		return nil, fmt.Errorf("qualification release admission reference is unavailable")
	}
	return &reference, nil
}

func (s *server) setQualificationReleaseStatus(ready bool, failure string, loadedAt time.Time, workspaceCount int) {
	s.authorityMu.Lock()
	s.qualificationReleaseReady = ready
	s.qualificationReleaseError = failure
	if !loadedAt.IsZero() {
		s.qualificationReleaseLoaded = loadedAt
	}
	if workspaceCount >= 0 {
		s.qualificationReleaseCount = workspaceCount
	}
	s.authorityMu.Unlock()
}

func (s *server) qualificationReleaseStatusValues() (bool, string) {
	s.authorityMu.RLock()
	defer s.authorityMu.RUnlock()
	return s.qualificationReleaseReady, s.qualificationReleaseError
}

func (s *server) qualificationReleaseStatus() map[string]any {
	s.authorityMu.RLock()
	defer s.authorityMu.RUnlock()
	status := map[string]any{
		"required":      s.qualificationReleaseRequired,
		"ready":         s.qualificationReleaseReady,
		"release_id":    s.qualificationReleaseID,
		"artifact_kind": s.qualificationReleaseKind,
		"workspaces":    s.qualificationReleaseCount,
	}
	if !s.qualificationReleaseLoaded.IsZero() {
		status["loaded_at"] = s.qualificationReleaseLoaded.UTC().Format(time.RFC3339Nano)
	}
	if s.qualificationReleaseError != "" {
		status["error"] = s.qualificationReleaseError
	}
	return status
}
