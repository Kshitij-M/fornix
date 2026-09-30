package contracts

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// QualificationRetentionSchemaVersion versions the hash-only qualification
// retention and recovery contracts independently from readiness authority.
const QualificationRetentionSchemaVersion = 1

const (
	DefaultQualificationRetentionBatch  = 100
	MaxQualificationRetentionBatch      = 1000
	MinQualificationRetentionSeconds    = 24 * 60 * 60
	MaxQualificationRetentionSeconds    = 10 * 365 * 24 * 60 * 60
	DefaultQualificationRetentionDays   = 90
	MaxQualificationRetentionCandidates = 256
	MaxQualificationRecoveryIssues      = 64
)

const (
	QualificationRecordSnapshot = "readiness_snapshot"
	QualificationRecordIncident = "incident_annotation"
	QualificationRecordPolicy   = "freshness_policy"
)

const (
	RetentionCandidateEligible          = "eligible_external_archive"
	RetentionCandidateProtectedLatest   = "protected_latest"
	RetentionCandidateProtectedIncident = "protected_incident"
	RetentionCandidateMetadataMissing   = "metadata_missing"
	RetentionCandidateNotDue            = "not_due"
)

// QualificationRetentionPolicy is an immutable workspace/deployment policy
// for external archival planning. It never authorizes deletion of Fornix
// qualification history or changes release admission.
type QualificationRetentionPolicy struct {
	SchemaVersion            int        `json:"schema_version"`
	ID                       string     `json:"id"`
	WorkspaceID              string     `json:"workspace_id"`
	DeploymentID             string     `json:"deployment_id"`
	Revision                 int64      `json:"revision"`
	SnapshotRetentionSeconds int64      `json:"snapshot_retention_seconds"`
	IncidentRetentionSeconds int64      `json:"incident_retention_seconds"`
	PolicyRetentionSeconds   int64      `json:"policy_retention_seconds"`
	KeepLatestSnapshots      int        `json:"keep_latest_snapshots"`
	KeepLatestIncidents      int        `json:"keep_latest_incidents"`
	ProtectIncidents         bool       `json:"protect_incidents"`
	PolicyHash               string     `json:"policy_hash"`
	Actor                    AuditActor `json:"actor"`
	CreatedAt                time.Time  `json:"created_at"`
	RequestID                string     `json:"request_id,omitempty"`
	IdempotencyKey           string     `json:"idempotency_key"`
	CausationID              string     `json:"causation_id,omitempty"`
	CorrelationID            string     `json:"correlation_id,omitempty"`
}

func (p *QualificationRetentionPolicy) Normalize() error {
	if p == nil {
		return fmt.Errorf("qualification retention policy is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = QualificationRetentionSchemaVersion
	}
	if p.SchemaVersion != QualificationRetentionSchemaVersion {
		return fmt.Errorf("unsupported qualification retention schema_version %d", p.SchemaVersion)
	}
	var err error
	if p.ID, err = normalizeDomainIdentifier(p.ID, "qualification retention policy id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if p.WorkspaceID, err = normalizeDomainWorkspace(p.WorkspaceID); err != nil {
		return err
	}
	if p.DeploymentID, err = normalizeDomainIdentifier(p.DeploymentID, "qualification retention policy deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if p.Revision < 1 {
		return fmt.Errorf("qualification retention policy revision must be positive")
	}
	for name, value := range map[string]int64{
		"snapshot_retention_seconds": p.SnapshotRetentionSeconds,
		"incident_retention_seconds": p.IncidentRetentionSeconds,
		"policy_retention_seconds":   p.PolicyRetentionSeconds,
	} {
		if value < MinQualificationRetentionSeconds || value > MaxQualificationRetentionSeconds {
			return fmt.Errorf("qualification retention policy %s is outside bounds", name)
		}
	}
	if p.KeepLatestSnapshots < 1 || p.KeepLatestSnapshots > MaxQualificationRetentionBatch {
		return fmt.Errorf("qualification retention policy keep_latest_snapshots is outside bounds")
	}
	if p.KeepLatestIncidents < 1 || p.KeepLatestIncidents > MaxQualificationRetentionBatch {
		return fmt.Errorf("qualification retention policy keep_latest_incidents is outside bounds")
	}
	if p.PolicyHash, err = normalizeDomainHash(p.PolicyHash, "qualification retention policy policy_hash", true); err != nil {
		return err
	}
	if p.PolicyHash != p.StableHash() {
		return fmt.Errorf("qualification retention policy policy_hash does not match policy facts")
	}
	if !p.CreatedAt.IsZero() {
		p.CreatedAt = p.CreatedAt.UTC()
	}
	for field, value := range map[string]*string{
		"request_id": &p.RequestID, "idempotency_key": &p.IdempotencyKey,
		"causation_id": &p.CausationID, "correlation_id": &p.CorrelationID,
	} {
		required := field == "idempotency_key"
		if *value, err = normalizeDomainIdentifier(*value, "qualification retention policy "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	return normalizeQualificationAuditActor(&p.Actor, p.WorkspaceID)
}

// StableHash excludes identity and provenance metadata so policy replay can be
// compared across operators and database retries.
func (p QualificationRetentionPolicy) StableHash() string {
	return HashStrings("qualification-retention-policy", p.WorkspaceID, p.DeploymentID,
		fmt.Sprint(p.SnapshotRetentionSeconds), fmt.Sprint(p.IncidentRetentionSeconds),
		fmt.Sprint(p.PolicyRetentionSeconds), fmt.Sprint(p.KeepLatestSnapshots),
		fmt.Sprint(p.KeepLatestIncidents), fmt.Sprint(p.ProtectIncidents))
}

type QualificationRetentionPolicyRequest struct {
	WorkspaceID              string     `json:"workspace_id"`
	DeploymentID             string     `json:"deployment_id"`
	SnapshotRetentionSeconds int64      `json:"snapshot_retention_seconds"`
	IncidentRetentionSeconds int64      `json:"incident_retention_seconds"`
	PolicyRetentionSeconds   int64      `json:"policy_retention_seconds"`
	KeepLatestSnapshots      int        `json:"keep_latest_snapshots"`
	KeepLatestIncidents      int        `json:"keep_latest_incidents"`
	ProtectIncidents         bool       `json:"protect_incidents"`
	RequestID                string     `json:"request_id,omitempty"`
	IdempotencyKey           string     `json:"idempotency_key"`
	CausationID              string     `json:"causation_id,omitempty"`
	CorrelationID            string     `json:"correlation_id,omitempty"`
	Actor                    AuditActor `json:"actor"`
	DryRun                   bool       `json:"dry_run,omitempty"`
}

func (r *QualificationRetentionPolicyRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification retention policy request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification retention policy request deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	for name, value := range map[string]int64{
		"snapshot_retention_seconds": r.SnapshotRetentionSeconds,
		"incident_retention_seconds": r.IncidentRetentionSeconds,
		"policy_retention_seconds":   r.PolicyRetentionSeconds,
	} {
		if value < MinQualificationRetentionSeconds || value > MaxQualificationRetentionSeconds {
			return fmt.Errorf("qualification retention policy request %s is outside bounds", name)
		}
	}
	if r.KeepLatestSnapshots < 1 || r.KeepLatestSnapshots > MaxQualificationRetentionBatch || r.KeepLatestIncidents < 1 || r.KeepLatestIncidents > MaxQualificationRetentionBatch {
		return fmt.Errorf("qualification retention policy request keep_latest value is outside bounds")
	}
	for field, value := range map[string]*string{
		"request_id": &r.RequestID, "idempotency_key": &r.IdempotencyKey,
		"causation_id": &r.CausationID, "correlation_id": &r.CorrelationID,
	} {
		required := field == "idempotency_key"
		if *value, err = normalizeDomainIdentifier(*value, "qualification retention policy request "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

type QualificationRetentionPolicyPage struct {
	Items      []QualificationRetentionPolicy `json:"items"`
	NextCursor string                         `json:"next_cursor,omitempty"`
}

// QualificationRetentionMetadata records the retention decision inputs for a
// single immutable qualification record. It is operational metadata, not a
// replacement for the source record.
type QualificationRetentionMetadata struct {
	SchemaVersion  int        `json:"schema_version"`
	ID             string     `json:"id"`
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	RecordKind     string     `json:"record_kind"`
	RecordID       string     `json:"record_id"`
	RecordHash     string     `json:"record_hash"`
	PolicyID       string     `json:"policy_id,omitempty"`
	PolicyRevision int64      `json:"policy_revision,omitempty"`
	RetainUntil    time.Time  `json:"retain_until"`
	MetadataHash   string     `json:"metadata_hash"`
	Actor          AuditActor `json:"actor"`
	CreatedAt      time.Time  `json:"created_at"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
}

func (m *QualificationRetentionMetadata) Normalize() error {
	if m == nil {
		return fmt.Errorf("qualification retention metadata is nil")
	}
	if m.SchemaVersion == 0 {
		m.SchemaVersion = QualificationRetentionSchemaVersion
	}
	if m.SchemaVersion != QualificationRetentionSchemaVersion {
		return fmt.Errorf("unsupported qualification retention metadata schema_version %d", m.SchemaVersion)
	}
	var err error
	if m.ID, err = normalizeDomainIdentifier(m.ID, "qualification retention metadata id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if m.WorkspaceID, err = normalizeDomainWorkspace(m.WorkspaceID); err != nil {
		return err
	}
	if m.DeploymentID, err = normalizeDomainIdentifier(m.DeploymentID, "qualification retention metadata deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if !validQualificationRetentionRecordKind(m.RecordKind) {
		return fmt.Errorf("unsupported qualification retention record_kind %q", m.RecordKind)
	}
	if m.RecordID, err = normalizeDomainIdentifier(m.RecordID, "qualification retention metadata record_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if m.RecordHash, err = normalizeDomainHash(m.RecordHash, "qualification retention metadata record_hash", true); err != nil {
		return err
	}
	if m.PolicyID != "" {
		if m.PolicyID, err = normalizeDomainIdentifier(m.PolicyID, "qualification retention metadata policy_id", MaxDomainIDLength, true); err != nil {
			return err
		}
		if m.PolicyRevision < 1 {
			return fmt.Errorf("qualification retention metadata policy_revision must be positive")
		}
	} else if m.PolicyRevision != 0 {
		return fmt.Errorf("qualification retention metadata policy_revision requires policy_id")
	}
	if m.RetainUntil.IsZero() {
		return fmt.Errorf("qualification retention metadata retain_until is required")
	}
	m.RetainUntil = m.RetainUntil.UTC()
	if m.MetadataHash, err = normalizeDomainHash(m.MetadataHash, "qualification retention metadata metadata_hash", true); err != nil {
		return err
	}
	if m.MetadataHash != m.StableHash() {
		return fmt.Errorf("qualification retention metadata metadata_hash does not match facts")
	}
	if !m.CreatedAt.IsZero() {
		m.CreatedAt = m.CreatedAt.UTC()
	}
	for field, value := range map[string]*string{
		"request_id": &m.RequestID, "idempotency_key": &m.IdempotencyKey,
		"causation_id": &m.CausationID, "correlation_id": &m.CorrelationID,
	} {
		required := field == "idempotency_key"
		if *value, err = normalizeDomainIdentifier(*value, "qualification retention metadata "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	return normalizeQualificationAuditActor(&m.Actor, m.WorkspaceID)
}

func (m QualificationRetentionMetadata) StableHash() string {
	return HashStrings("qualification-retention-metadata", m.WorkspaceID, m.DeploymentID,
		m.RecordKind, m.RecordID, m.RecordHash, m.PolicyID, fmt.Sprint(m.PolicyRevision), m.RetainUntil.UTC().Format(time.RFC3339Nano))
}

type QualificationRetentionSyncRequest struct {
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	Cursor         string     `json:"cursor,omitempty"`
	BatchSize      int        `json:"batch_size,omitempty"`
	DryRun         bool       `json:"dry_run,omitempty"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
	Actor          AuditActor `json:"actor"`
}

func (r *QualificationRetentionSyncRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification retention sync request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification retention sync deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.BatchSize < 0 || r.BatchSize > MaxQualificationRetentionBatch {
		return fmt.Errorf("qualification retention sync batch_size exceeds %d", MaxQualificationRetentionBatch)
	}
	for field, value := range map[string]*string{
		"request_id": &r.RequestID, "idempotency_key": &r.IdempotencyKey,
		"causation_id": &r.CausationID, "correlation_id": &r.CorrelationID,
	} {
		required := field == "idempotency_key" && !r.DryRun
		if *value, err = normalizeDomainIdentifier(*value, "qualification retention sync "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

type QualificationRetentionSyncResult struct {
	WorkspaceID  string `json:"workspace_id"`
	DeploymentID string `json:"deployment_id"`
	Cursor       string `json:"cursor,omitempty"`
	NextCursor   string `json:"next_cursor,omitempty"`
	BatchSize    int    `json:"batch_size"`
	DryRun       bool   `json:"dry_run"`
	Examined     int    `json:"examined"`
	Missing      int    `json:"missing"`
	Registered   int    `json:"registered"`
	Skipped      int    `json:"skipped"`
	ResultHash   string `json:"result_hash"`
}

// QualificationRetentionCandidate contains only bounded identity and hash
// facts suitable for an external archival manifest.
type QualificationRetentionCandidate struct {
	RecordKind  string    `json:"record_kind"`
	RecordID    string    `json:"record_id"`
	RecordHash  string    `json:"record_hash"`
	CreatedAt   time.Time `json:"created_at"`
	RetainUntil time.Time `json:"retain_until"`
	Reason      string    `json:"reason"`
}

func (c *QualificationRetentionCandidate) Normalize() error {
	if c == nil {
		return fmt.Errorf("qualification retention candidate is nil")
	}
	if !validQualificationRetentionRecordKind(c.RecordKind) {
		return fmt.Errorf("unsupported qualification retention candidate record_kind %q", c.RecordKind)
	}
	var err error
	if c.RecordID, err = normalizeDomainIdentifier(c.RecordID, "qualification retention candidate record_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if c.RecordHash, err = normalizeDomainHash(c.RecordHash, "qualification retention candidate record_hash", true); err != nil {
		return err
	}
	if c.CreatedAt.IsZero() || c.RetainUntil.IsZero() {
		return fmt.Errorf("qualification retention candidate timestamps are required")
	}
	c.CreatedAt, c.RetainUntil = c.CreatedAt.UTC(), c.RetainUntil.UTC()
	c.Reason = strings.TrimSpace(c.Reason)
	switch c.Reason {
	case RetentionCandidateEligible, RetentionCandidateProtectedLatest, RetentionCandidateProtectedIncident, RetentionCandidateMetadataMissing, RetentionCandidateNotDue:
	default:
		return fmt.Errorf("unsupported qualification retention candidate reason %q", c.Reason)
	}
	return nil
}

type QualificationRetentionPlanRequest struct {
	WorkspaceID  string     `json:"workspace_id"`
	DeploymentID string     `json:"deployment_id"`
	AsOf         time.Time  `json:"as_of,omitempty"`
	Cursor       string     `json:"cursor,omitempty"`
	BatchSize    int        `json:"batch_size,omitempty"`
	Actor        AuditActor `json:"actor"`
}

func (r *QualificationRetentionPlanRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification retention plan request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification retention plan deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.BatchSize < 0 || r.BatchSize > MaxQualificationRetentionBatch {
		return fmt.Errorf("qualification retention plan batch_size exceeds %d", MaxQualificationRetentionBatch)
	}
	if !r.AsOf.IsZero() {
		r.AsOf = r.AsOf.UTC()
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

type QualificationRetentionPlan struct {
	SchemaVersion     int                               `json:"schema_version"`
	WorkspaceID       string                            `json:"workspace_id"`
	DeploymentID      string                            `json:"deployment_id"`
	PolicyID          string                            `json:"policy_id"`
	PolicyRevision    int64                             `json:"policy_revision"`
	PolicyHash        string                            `json:"policy_hash"`
	AsOf              time.Time                         `json:"as_of"`
	Cursor            string                            `json:"cursor,omitempty"`
	NextCursor        string                            `json:"next_cursor,omitempty"`
	BatchSize         int                               `json:"batch_size"`
	Examined          int                               `json:"examined"`
	Eligible          int                               `json:"eligible"`
	ProtectedLatest   int                               `json:"protected_latest"`
	ProtectedIncident int                               `json:"protected_incident"`
	MetadataMissing   int                               `json:"metadata_missing"`
	NotDue            int                               `json:"not_due"`
	Candidates        []QualificationRetentionCandidate `json:"candidates,omitempty"`
	PlanHash          string                            `json:"plan_hash"`
	EvaluatedBy       AuditActor                        `json:"evaluated_by"`
}

func (p *QualificationRetentionPlan) Normalize() error {
	if p == nil {
		return fmt.Errorf("qualification retention plan is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = QualificationRetentionSchemaVersion
	}
	if p.SchemaVersion != QualificationRetentionSchemaVersion {
		return fmt.Errorf("unsupported qualification retention plan schema_version %d", p.SchemaVersion)
	}
	var err error
	if p.WorkspaceID, err = normalizeDomainWorkspace(p.WorkspaceID); err != nil {
		return err
	}
	if p.DeploymentID, err = normalizeDomainIdentifier(p.DeploymentID, "qualification retention plan deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if p.PolicyID, err = normalizeDomainIdentifier(p.PolicyID, "qualification retention plan policy_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if p.PolicyRevision < 0 || (p.PolicyRevision == 0 && p.PolicyID != "default-retention-policy") {
		return fmt.Errorf("qualification retention plan policy_revision is invalid")
	}
	var hErr error
	if p.PolicyHash, hErr = normalizeDomainHash(p.PolicyHash, "qualification retention plan policy_hash", true); hErr != nil {
		return hErr
	}
	if p.AsOf.IsZero() {
		return fmt.Errorf("qualification retention plan as_of is required")
	}
	p.AsOf = p.AsOf.UTC()
	if p.BatchSize < 1 || p.BatchSize > MaxQualificationRetentionBatch {
		return fmt.Errorf("qualification retention plan batch_size is outside bounds")
	}
	if p.Examined < 0 || p.Eligible < 0 || p.ProtectedLatest < 0 || p.ProtectedIncident < 0 || p.MetadataMissing < 0 || p.NotDue < 0 {
		return fmt.Errorf("qualification retention plan counts cannot be negative")
	}
	if len(p.Candidates) > MaxQualificationRetentionCandidates {
		return fmt.Errorf("qualification retention plan candidates exceeds %d", MaxQualificationRetentionCandidates)
	}
	for i := range p.Candidates {
		if err := p.Candidates[i].Normalize(); err != nil {
			return err
		}
	}
	sort.Slice(p.Candidates, func(i, j int) bool {
		return retentionCandidateKey(p.Candidates[i]) < retentionCandidateKey(p.Candidates[j])
	})
	if p.PlanHash, hErr = normalizeDomainHash(p.PlanHash, "qualification retention plan plan_hash", true); hErr != nil {
		return hErr
	}
	if p.PlanHash != p.StableHash() {
		return fmt.Errorf("qualification retention plan plan_hash does not match facts")
	}
	return normalizeQualificationAuditActor(&p.EvaluatedBy, p.WorkspaceID)
}

func (p QualificationRetentionPlan) StableHash() string {
	parts := []string{"qualification-retention-plan", p.WorkspaceID, p.DeploymentID, p.PolicyID, fmt.Sprint(p.PolicyRevision), p.PolicyHash, p.AsOf.UTC().Format(time.RFC3339Nano), p.Cursor, p.NextCursor, fmt.Sprint(p.BatchSize), fmt.Sprint(p.Examined), fmt.Sprint(p.Eligible), fmt.Sprint(p.ProtectedLatest), fmt.Sprint(p.ProtectedIncident), fmt.Sprint(p.MetadataMissing), fmt.Sprint(p.NotDue)}
	for _, candidate := range p.Candidates {
		parts = append(parts, candidate.RecordKind, candidate.RecordID, candidate.RecordHash, candidate.CreatedAt.UTC().Format(time.RFC3339Nano), candidate.RetainUntil.UTC().Format(time.RFC3339Nano), candidate.Reason)
	}
	return HashStrings(parts...)
}

type QualificationRecoveryReportRequest struct {
	WorkspaceID  string     `json:"workspace_id"`
	DeploymentID string     `json:"deployment_id"`
	AsOf         time.Time  `json:"as_of,omitempty"`
	Actor        AuditActor `json:"actor"`
}

func (r *QualificationRecoveryReportRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification recovery report request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification recovery report deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if !r.AsOf.IsZero() {
		r.AsOf = r.AsOf.UTC()
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

type QualificationRecoveryReport struct {
	SchemaVersion               int        `json:"schema_version"`
	WorkspaceID                 string     `json:"workspace_id"`
	DeploymentID                string     `json:"deployment_id"`
	AsOf                        time.Time  `json:"as_of"`
	SnapshotCount               int64      `json:"snapshot_count"`
	IncidentCount               int64      `json:"incident_count"`
	PolicyCount                 int64      `json:"policy_count"`
	MetadataCount               int64      `json:"metadata_count"`
	MetadataMissingCount        int64      `json:"metadata_missing_count"`
	DanglingMetadataCount       int64      `json:"dangling_metadata_count"`
	HashMismatchCount           int64      `json:"hash_mismatch_count"`
	DanglingAnnotationCount     int64      `json:"dangling_annotation_count"`
	EventReferenceMismatchCount int64      `json:"event_reference_mismatch_count"`
	Healthy                     bool       `json:"healthy"`
	Issues                      []string   `json:"issues,omitempty"`
	ReportHash                  string     `json:"report_hash"`
	EvaluatedBy                 AuditActor `json:"evaluated_by"`
}

func (r *QualificationRecoveryReport) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification recovery report is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = QualificationRetentionSchemaVersion
	}
	if r.SchemaVersion != QualificationRetentionSchemaVersion {
		return fmt.Errorf("unsupported qualification recovery report schema_version %d", r.SchemaVersion)
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification recovery report deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.AsOf.IsZero() {
		return fmt.Errorf("qualification recovery report as_of is required")
	}
	r.AsOf = r.AsOf.UTC()
	for name, value := range map[string]int64{
		"snapshot_count": r.SnapshotCount, "incident_count": r.IncidentCount, "policy_count": r.PolicyCount,
		"metadata_count": r.MetadataCount, "metadata_missing_count": r.MetadataMissingCount,
		"dangling_metadata_count": r.DanglingMetadataCount, "hash_mismatch_count": r.HashMismatchCount,
		"dangling_annotation_count": r.DanglingAnnotationCount, "event_reference_mismatch_count": r.EventReferenceMismatchCount,
	} {
		if value < 0 {
			return fmt.Errorf("qualification recovery report %s cannot be negative", name)
		}
	}
	if len(r.Issues) > MaxQualificationRecoveryIssues {
		return fmt.Errorf("qualification recovery report issues exceeds %d", MaxQualificationRecoveryIssues)
	}
	for i := range r.Issues {
		r.Issues[i], err = normalizeDomainIdentifier(r.Issues[i], "qualification recovery report issue", MaxDomainIDLength, true)
		if err != nil {
			return err
		}
	}
	sort.Strings(r.Issues)
	if r.ReportHash, err = normalizeDomainHash(r.ReportHash, "qualification recovery report report_hash", true); err != nil {
		return err
	}
	if r.ReportHash != r.StableHash() {
		return fmt.Errorf("qualification recovery report report_hash does not match facts")
	}
	return normalizeQualificationAuditActor(&r.EvaluatedBy, r.WorkspaceID)
}

func (r QualificationRecoveryReport) StableHash() string {
	parts := []string{"qualification-recovery-report", r.WorkspaceID, r.DeploymentID, r.AsOf.UTC().Format(time.RFC3339Nano), fmt.Sprint(r.SnapshotCount), fmt.Sprint(r.IncidentCount), fmt.Sprint(r.PolicyCount), fmt.Sprint(r.MetadataCount), fmt.Sprint(r.MetadataMissingCount), fmt.Sprint(r.DanglingMetadataCount), fmt.Sprint(r.HashMismatchCount), fmt.Sprint(r.DanglingAnnotationCount), fmt.Sprint(r.EventReferenceMismatchCount), fmt.Sprint(r.Healthy)}
	return HashStrings(append(parts, r.Issues...)...)
}

func validQualificationRetentionRecordKind(kind string) bool {
	switch kind {
	case QualificationRecordSnapshot, QualificationRecordIncident, QualificationRecordPolicy:
		return true
	default:
		return false
	}
}

func retentionCandidateKey(candidate QualificationRetentionCandidate) string {
	return candidate.RecordKind + "\x00" + candidate.RecordID + "\x00" + candidate.RecordHash
}
