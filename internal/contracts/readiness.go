package contracts

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ReadinessSnapshotSchemaVersion versions the advisory, hash-only readiness
// observation independently from release and evidence schemas.
const ReadinessSnapshotSchemaVersion = 1

const (
	MaxReadinessSnapshotKinds       = MaxDeploymentEvidenceKinds
	MaxReadinessSnapshotEvidenceIDs = MaxDeploymentEvidenceKinds * 2
	MaxReadinessSnapshotReasons     = MaxDeploymentEvidenceKinds + 8
	MaxIncidentAnnotationCode       = MaxDomainNameLength
	MaxIncidentAnnotationHash       = MaxDomainHashLength
)

const (
	IncidentDispositionObserved     = "observed"
	IncidentDispositionAcknowledged = "acknowledged"
	IncidentDispositionMitigated    = "mitigated"
	IncidentDispositionEscalated    = "escalated"
)

// ReadinessSnapshot is a durable observation of one release-gate evaluation.
// It intentionally contains identities and hashes only. It is advisory
// evidence for operators and never grants admission by itself.
type ReadinessSnapshot struct {
	SchemaVersion         int        `json:"schema_version"`
	ID                    string     `json:"id"`
	WorkspaceID           string     `json:"workspace_id"`
	DeploymentID          string     `json:"deployment_id"`
	ReleaseID             string     `json:"release_id"`
	ReleaseHash           string     `json:"release_hash"`
	TrustSnapshotRevision int64      `json:"trust_snapshot_revision"`
	TrustSnapshotHash     string     `json:"trust_snapshot_hash"`
	RequiredKinds         []string   `json:"required_kinds"`
	ActiveEvidenceIDs     []string   `json:"active_evidence_ids"`
	MissingKinds          []string   `json:"missing_kinds,omitempty"`
	BlockedReasons        []string   `json:"blocked_reasons,omitempty"`
	GateHash              string     `json:"gate_hash"`
	Ready                 bool       `json:"ready"`
	SnapshotHash          string     `json:"snapshot_hash"`
	EvaluatedAt           time.Time  `json:"evaluated_at"`
	CreatedAt             time.Time  `json:"created_at"`
	Actor                 AuditActor `json:"actor"`
	RequestID             string     `json:"request_id,omitempty"`
	IdempotencyKey        string     `json:"idempotency_key"`
	CausationID           string     `json:"causation_id,omitempty"`
	CorrelationID         string     `json:"correlation_id,omitempty"`
}

func (s *ReadinessSnapshot) Normalize() error {
	if s == nil {
		return fmt.Errorf("readiness snapshot is nil")
	}
	if s.SchemaVersion == 0 {
		s.SchemaVersion = ReadinessSnapshotSchemaVersion
	}
	if s.SchemaVersion != ReadinessSnapshotSchemaVersion {
		return fmt.Errorf("unsupported readiness snapshot schema_version %d", s.SchemaVersion)
	}
	var err error
	if s.ID, err = normalizeDomainIdentifier(s.ID, "readiness snapshot id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if s.WorkspaceID, err = normalizeDomainWorkspace(s.WorkspaceID); err != nil {
		return err
	}
	if s.DeploymentID, err = normalizeDomainIdentifier(s.DeploymentID, "readiness snapshot deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if s.ReleaseID, err = normalizeDomainIdentifier(s.ReleaseID, "readiness snapshot release_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"release_hash": &s.ReleaseHash, "trust_snapshot_hash": &s.TrustSnapshotHash,
		"gate_hash": &s.GateHash, "snapshot_hash": &s.SnapshotHash,
	} {
		if field == "snapshot_hash" && strings.TrimSpace(*value) == "" {
			continue
		}
		if *value, err = normalizeDomainHash(*value, "readiness snapshot "+field, true); err != nil {
			return err
		}
	}
	if s.TrustSnapshotRevision < 1 {
		return fmt.Errorf("readiness snapshot trust snapshot revision must be positive")
	}
	if s.RequiredKinds, err = normalizeReadinessKinds(s.RequiredKinds, "readiness snapshot required_kinds"); err != nil {
		return err
	}
	if len(s.RequiredKinds) == 0 {
		return fmt.Errorf("readiness snapshot required_kinds must not be empty")
	}
	if s.ActiveEvidenceIDs, err = normalizeDomainStrings(s.ActiveEvidenceIDs, "readiness snapshot active_evidence_ids", MaxReadinessSnapshotEvidenceIDs); err != nil {
		return err
	}
	if s.MissingKinds, err = normalizeReadinessKinds(s.MissingKinds, "readiness snapshot missing_kinds"); err != nil {
		return err
	}
	if len(s.BlockedReasons) > MaxReadinessSnapshotReasons {
		return fmt.Errorf("readiness snapshot blocked_reasons exceeds %d values", MaxReadinessSnapshotReasons)
	}
	for i := range s.BlockedReasons {
		s.BlockedReasons[i], err = normalizeDomainIdentifier(s.BlockedReasons[i], "readiness snapshot blocked_reason", MaxDomainIDLength, true)
		if err != nil {
			return err
		}
	}
	sort.Strings(s.BlockedReasons)
	if s.EvaluatedAt.IsZero() {
		return fmt.Errorf("readiness snapshot evaluated_at is required")
	}
	s.EvaluatedAt = s.EvaluatedAt.UTC()
	if !s.CreatedAt.IsZero() {
		s.CreatedAt = s.CreatedAt.UTC()
	}
	for field, value := range map[string]*string{
		"request_id": &s.RequestID, "idempotency_key": &s.IdempotencyKey,
		"causation_id": &s.CausationID, "correlation_id": &s.CorrelationID,
	} {
		required := field == "idempotency_key"
		if *value, err = normalizeDomainIdentifier(*value, "readiness snapshot "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	if s.SnapshotHash != "" && s.SnapshotHash != readinessSnapshotAuthorityHash(*s) {
		return fmt.Errorf("readiness snapshot snapshot_hash does not match authority facts")
	}
	return normalizeQualificationAuditActor(&s.Actor, s.WorkspaceID)
}

// StableHash identifies the authority facts, not observation metadata such as
// actor or evaluation time. This makes repeated replay comparisons stable.
func (s ReadinessSnapshot) StableHash() string {
	if err := s.Normalize(); err != nil {
		return ""
	}
	return readinessSnapshotAuthorityHash(s)
}

func readinessSnapshotAuthorityHash(s ReadinessSnapshot) string {
	parts := []string{"readiness-snapshot", s.WorkspaceID, s.DeploymentID, s.ReleaseID, s.ReleaseHash, fmt.Sprint(s.TrustSnapshotRevision), s.TrustSnapshotHash, s.GateHash, fmt.Sprint(s.Ready)}
	parts = append(parts, s.RequiredKinds...)
	parts = append(parts, s.ActiveEvidenceIDs...)
	parts = append(parts, s.MissingKinds...)
	parts = append(parts, s.BlockedReasons...)
	return HashStrings(parts...)
}

// ReadinessSnapshotRequest asks the store to evaluate and persist one current
// gate observation. RequiredKinds is a set-like override; the authoritative
// release and evidence rows are always read by the store.
type ReadinessSnapshotRequest struct {
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	ReleaseID      string     `json:"release_id"`
	RequiredKinds  []string   `json:"required_kinds,omitempty"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
	Actor          AuditActor `json:"actor"`
	DryRun         bool       `json:"dry_run,omitempty"`
}

func (r *ReadinessSnapshotRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("readiness snapshot request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "readiness snapshot request deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.ReleaseID, err = normalizeDomainIdentifier(r.ReleaseID, "readiness snapshot request release_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.RequiredKinds, err = normalizeReadinessKinds(r.RequiredKinds, "readiness snapshot request required_kinds"); err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"request_id": &r.RequestID, "idempotency_key": &r.IdempotencyKey,
		"causation_id": &r.CausationID, "correlation_id": &r.CorrelationID,
	} {
		required := field == "idempotency_key"
		if *value, err = normalizeDomainIdentifier(*value, "readiness snapshot request "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

type ReadinessSnapshotPage struct {
	Items      []ReadinessSnapshot `json:"items"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

// IncidentAnnotation is a bounded operator observation linked to a readiness
// snapshot. Code and disposition are intentionally structured; raw incident
// logs and deployment payloads belong outside the control plane.
type IncidentAnnotation struct {
	SchemaVersion  int        `json:"schema_version"`
	ID             string     `json:"id"`
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	ReleaseID      string     `json:"release_id"`
	SnapshotID     string     `json:"snapshot_id"`
	SnapshotHash   string     `json:"snapshot_hash"`
	Code           string     `json:"code"`
	Disposition    string     `json:"disposition"`
	ReferenceHash  string     `json:"reference_hash,omitempty"`
	AnnotationHash string     `json:"annotation_hash"`
	Actor          AuditActor `json:"actor"`
	CreatedAt      time.Time  `json:"created_at"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
}

func (a *IncidentAnnotation) Normalize() error {
	if a == nil {
		return fmt.Errorf("incident annotation is nil")
	}
	if a.SchemaVersion == 0 {
		a.SchemaVersion = ReadinessSnapshotSchemaVersion
	}
	if a.SchemaVersion != ReadinessSnapshotSchemaVersion {
		return fmt.Errorf("unsupported incident annotation schema_version %d", a.SchemaVersion)
	}
	var err error
	for field, value := range map[string]*string{
		"id": &a.ID, "deployment_id": &a.DeploymentID, "release_id": &a.ReleaseID, "snapshot_id": &a.SnapshotID,
	} {
		if *value, err = normalizeDomainIdentifier(*value, "incident annotation "+field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	if a.WorkspaceID, err = normalizeDomainWorkspace(a.WorkspaceID); err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"snapshot_hash": &a.SnapshotHash, "annotation_hash": &a.AnnotationHash, "reference_hash": &a.ReferenceHash,
	} {
		required := field != "reference_hash"
		if *value, err = normalizeDomainHash(*value, "incident annotation "+field, required); err != nil {
			return err
		}
	}
	if a.Code, err = normalizeDomainName(a.Code, "incident annotation code", MaxIncidentAnnotationCode, true); err != nil {
		return err
	}
	a.Disposition = strings.ToLower(strings.TrimSpace(a.Disposition))
	if !validIncidentDisposition(a.Disposition) {
		return fmt.Errorf("unsupported incident annotation disposition %q", a.Disposition)
	}
	if !a.CreatedAt.IsZero() {
		a.CreatedAt = a.CreatedAt.UTC()
	}
	for field, value := range map[string]*string{
		"request_id": &a.RequestID, "idempotency_key": &a.IdempotencyKey,
		"causation_id": &a.CausationID, "correlation_id": &a.CorrelationID,
	} {
		required := field == "idempotency_key"
		if *value, err = normalizeDomainIdentifier(*value, "incident annotation "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	if a.AnnotationHash != "" && a.AnnotationHash != HashStrings("incident-annotation", a.WorkspaceID, a.DeploymentID, a.ReleaseID, a.SnapshotID, a.SnapshotHash, a.Code, a.Disposition, a.ReferenceHash) {
		return fmt.Errorf("incident annotation annotation_hash does not match bounded facts")
	}
	return normalizeQualificationAuditActor(&a.Actor, a.WorkspaceID)
}

// SetDerivedHashes fills the hashes that are derived from the bounded
// snapshot/annotation facts. It is used by stores before final normalization.
func (s *ReadinessSnapshot) SetDerivedHashes() {
	if s != nil {
		s.SnapshotHash = s.StableHash()
	}
}

func (a *IncidentAnnotation) SetDerivedHash() {
	if a != nil {
		a.AnnotationHash = HashStrings("incident-annotation", a.WorkspaceID, a.DeploymentID, a.ReleaseID, a.SnapshotID, a.SnapshotHash, a.Code, a.Disposition, a.ReferenceHash)
	}
}

type IncidentAnnotationRequest struct {
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	ReleaseID      string     `json:"release_id"`
	SnapshotID     string     `json:"snapshot_id"`
	Code           string     `json:"code"`
	Disposition    string     `json:"disposition"`
	ReferenceHash  string     `json:"reference_hash,omitempty"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
	Actor          AuditActor `json:"actor"`
	DryRun         bool       `json:"dry_run,omitempty"`
}

func (r *IncidentAnnotationRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("incident annotation request is nil")
	}
	var err error
	for field, value := range map[string]*string{
		"workspace_id": &r.WorkspaceID, "deployment_id": &r.DeploymentID, "release_id": &r.ReleaseID, "snapshot_id": &r.SnapshotID,
	} {
		if field == "workspace_id" {
			if *value, err = normalizeDomainWorkspace(*value); err != nil {
				return err
			}
			continue
		}
		if *value, err = normalizeDomainIdentifier(*value, "incident annotation request "+field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	if r.Code, err = normalizeDomainName(r.Code, "incident annotation request code", MaxIncidentAnnotationCode, true); err != nil {
		return err
	}
	r.Disposition = strings.ToLower(strings.TrimSpace(r.Disposition))
	if !validIncidentDisposition(r.Disposition) {
		return fmt.Errorf("unsupported incident annotation disposition %q", r.Disposition)
	}
	if r.ReferenceHash, err = normalizeDomainHash(r.ReferenceHash, "incident annotation request reference_hash", false); err != nil {
		return err
	}
	for field, value := range map[string]*string{"request_id": &r.RequestID, "idempotency_key": &r.IdempotencyKey, "causation_id": &r.CausationID, "correlation_id": &r.CorrelationID} {
		required := field == "idempotency_key"
		if *value, err = normalizeDomainIdentifier(*value, "incident annotation request "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

type IncidentAnnotationPage struct {
	Items      []IncidentAnnotation `json:"items"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

func normalizeReadinessKinds(values []string, field string) ([]string, error) {
	if len(values) > MaxReadinessSnapshotKinds {
		return nil, fmt.Errorf("%s exceeds %d values", field, MaxReadinessSnapshotKinds)
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value, err := normalizeDomainName(value, field, MaxDomainNameLength, true)
		if err != nil || !isDeploymentEvidenceKind(value) {
			return nil, fmt.Errorf("unsupported %s value %q", field, value)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("%s repeats %q", field, value)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func validIncidentDisposition(value string) bool {
	switch value {
	case IncidentDispositionObserved, IncidentDispositionAcknowledged, IncidentDispositionMitigated, IncidentDispositionEscalated:
		return true
	default:
		return false
	}
}
