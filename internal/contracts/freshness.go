package contracts

import (
	"fmt"
	"sort"
	"time"
)

const ReadinessFreshnessSchemaVersion = 1

const (
	ReadinessFreshnessMinAgeSeconds = 1
	ReadinessFreshnessMaxAgeSeconds = 30 * 24 * 60 * 60
	ReadinessFreshnessDefaultAge    = 24 * 60 * 60
)

const (
	ReadinessReviewUnchanged = "unchanged"
	ReadinessReviewImproved  = "improved"
	ReadinessReviewDegraded  = "degraded"
	ReadinessReviewChanged   = "changed"
	ReadinessReviewStale     = "stale"
)

// ReadinessFreshnessPolicy is an immutable versioned review policy. It is an
// operator diagnostic rule, not a release-admission rule.
type ReadinessFreshnessPolicy struct {
	SchemaVersion  int        `json:"schema_version"`
	ID             string     `json:"id"`
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	Revision       int64      `json:"revision"`
	MaxAgeSeconds  int64      `json:"max_age_seconds"`
	RequireReady   bool       `json:"require_ready"`
	PolicyHash     string     `json:"policy_hash"`
	Actor          AuditActor `json:"actor"`
	CreatedAt      time.Time  `json:"created_at"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
}

func (p *ReadinessFreshnessPolicy) Normalize() error {
	if p == nil {
		return fmt.Errorf("readiness freshness policy is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = ReadinessFreshnessSchemaVersion
	}
	if p.SchemaVersion != ReadinessFreshnessSchemaVersion {
		return fmt.Errorf("unsupported readiness freshness schema_version %d", p.SchemaVersion)
	}
	var err error
	if p.ID, err = normalizeDomainIdentifier(p.ID, "readiness freshness policy id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if p.WorkspaceID, err = normalizeDomainWorkspace(p.WorkspaceID); err != nil {
		return err
	}
	if p.DeploymentID, err = normalizeDomainIdentifier(p.DeploymentID, "readiness freshness policy deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if p.Revision < 1 {
		return fmt.Errorf("readiness freshness policy revision must be positive")
	}
	if p.MaxAgeSeconds < ReadinessFreshnessMinAgeSeconds || p.MaxAgeSeconds > ReadinessFreshnessMaxAgeSeconds {
		return fmt.Errorf("readiness freshness policy max_age_seconds is outside bounds")
	}
	if p.PolicyHash, err = normalizeDomainHash(p.PolicyHash, "readiness freshness policy policy_hash", true); err != nil {
		return err
	}
	if p.PolicyHash != p.StableHash() {
		return fmt.Errorf("readiness freshness policy policy_hash does not match policy facts")
	}
	if !p.CreatedAt.IsZero() {
		p.CreatedAt = p.CreatedAt.UTC()
	}
	for field, value := range map[string]*string{
		"request_id": &p.RequestID, "idempotency_key": &p.IdempotencyKey,
		"causation_id": &p.CausationID, "correlation_id": &p.CorrelationID,
	} {
		required := field == "idempotency_key"
		if *value, err = normalizeDomainIdentifier(*value, "readiness freshness policy "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	return normalizeQualificationAuditActor(&p.Actor, p.WorkspaceID)
}

// StableHash contains only policy authority facts, excluding identity and
// provenance metadata.
func (p ReadinessFreshnessPolicy) StableHash() string {
	return HashStrings("readiness-freshness-policy", p.WorkspaceID, p.DeploymentID, fmt.Sprint(p.MaxAgeSeconds), fmt.Sprint(p.RequireReady))
}

type ReadinessFreshnessPolicyRequest struct {
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	MaxAgeSeconds  int64      `json:"max_age_seconds"`
	RequireReady   bool       `json:"require_ready"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
	Actor          AuditActor `json:"actor"`
	DryRun         bool       `json:"dry_run,omitempty"`
}

func (r *ReadinessFreshnessPolicyRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("readiness freshness policy request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "readiness freshness policy request deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.MaxAgeSeconds < ReadinessFreshnessMinAgeSeconds || r.MaxAgeSeconds > ReadinessFreshnessMaxAgeSeconds {
		return fmt.Errorf("readiness freshness policy request max_age_seconds is outside bounds")
	}
	for field, value := range map[string]*string{
		"request_id": &r.RequestID, "idempotency_key": &r.IdempotencyKey,
		"causation_id": &r.CausationID, "correlation_id": &r.CorrelationID,
	} {
		required := field == "idempotency_key"
		if *value, err = normalizeDomainIdentifier(*value, "readiness freshness policy request "+field, MaxIdempotencyLength, required); err != nil {
			return err
		}
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

type ReadinessFreshnessPolicyPage struct {
	Items      []ReadinessFreshnessPolicy `json:"items"`
	NextCursor string                     `json:"next_cursor,omitempty"`
}

type ReadinessReviewRequest struct {
	WorkspaceID     string     `json:"workspace_id"`
	DeploymentID    string     `json:"deployment_id"`
	ReleaseID       string     `json:"release_id"`
	LeftSnapshotID  string     `json:"left_snapshot_id"`
	RightSnapshotID string     `json:"right_snapshot_id"`
	AsOf            time.Time  `json:"as_of,omitempty"`
	Actor           AuditActor `json:"actor"`
}

func (r *ReadinessReviewRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("readiness review request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"deployment_id": &r.DeploymentID, "release_id": &r.ReleaseID,
		"left_snapshot_id": &r.LeftSnapshotID, "right_snapshot_id": &r.RightSnapshotID,
	} {
		if *value, err = normalizeDomainIdentifier(*value, "readiness review "+field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	if !r.AsOf.IsZero() {
		r.AsOf = r.AsOf.UTC()
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

// ReadinessReview is a deterministic, read-only comparison of two snapshots.
// It is not an admission decision and carries no raw deployment content.
type ReadinessReview struct {
	SchemaVersion     int        `json:"schema_version"`
	WorkspaceID       string     `json:"workspace_id"`
	DeploymentID      string     `json:"deployment_id"`
	ReleaseID         string     `json:"release_id"`
	LeftSnapshotID    string     `json:"left_snapshot_id"`
	RightSnapshotID   string     `json:"right_snapshot_id"`
	LeftSnapshotHash  string     `json:"left_snapshot_hash"`
	RightSnapshotHash string     `json:"right_snapshot_hash"`
	PolicyID          string     `json:"policy_id"`
	PolicyRevision    int64      `json:"policy_revision"`
	PolicyHash        string     `json:"policy_hash"`
	MaxAgeSeconds     int64      `json:"max_age_seconds"`
	AsOf              time.Time  `json:"as_of"`
	LeftEvaluatedAt   time.Time  `json:"left_evaluated_at"`
	RightEvaluatedAt  time.Time  `json:"right_evaluated_at"`
	LeftAgeSeconds    int64      `json:"left_age_seconds"`
	RightAgeSeconds   int64      `json:"right_age_seconds"`
	LeftFresh         bool       `json:"left_fresh"`
	RightFresh        bool       `json:"right_fresh"`
	LeftReady         bool       `json:"left_ready"`
	RightReady        bool       `json:"right_ready"`
	GateChanged       bool       `json:"gate_changed"`
	EvidenceAdded     []string   `json:"evidence_added,omitempty"`
	EvidenceRemoved   []string   `json:"evidence_removed,omitempty"`
	BlockedAdded      []string   `json:"blocked_added,omitempty"`
	BlockedResolved   []string   `json:"blocked_resolved,omitempty"`
	StaleReasons      []string   `json:"stale_reasons,omitempty"`
	Outcome           string     `json:"outcome"`
	ReviewHash        string     `json:"review_hash"`
	EvaluatedBy       AuditActor `json:"evaluated_by"`
}

func (r *ReadinessReview) Normalize() error {
	if r == nil {
		return fmt.Errorf("readiness review is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ReadinessFreshnessSchemaVersion
	}
	if r.SchemaVersion != ReadinessFreshnessSchemaVersion {
		return fmt.Errorf("unsupported readiness review schema_version %d", r.SchemaVersion)
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	for field, value := range map[string]*string{"deployment_id": &r.DeploymentID, "release_id": &r.ReleaseID, "left_snapshot_id": &r.LeftSnapshotID, "right_snapshot_id": &r.RightSnapshotID, "policy_id": &r.PolicyID} {
		if *value, err = normalizeDomainIdentifier(*value, "readiness review "+field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	if r.LeftSnapshotID == r.RightSnapshotID {
		return fmt.Errorf("readiness review requires two distinct snapshots")
	}
	for field, value := range map[string]*string{"left_snapshot_hash": &r.LeftSnapshotHash, "right_snapshot_hash": &r.RightSnapshotHash, "policy_hash": &r.PolicyHash, "review_hash": &r.ReviewHash} {
		if *value, err = normalizeDomainHash(*value, "readiness review "+field, true); err != nil {
			return err
		}
	}
	if r.PolicyRevision < 1 || r.MaxAgeSeconds < ReadinessFreshnessMinAgeSeconds || r.MaxAgeSeconds > ReadinessFreshnessMaxAgeSeconds {
		return fmt.Errorf("readiness review policy bounds are invalid")
	}
	if r.AsOf.IsZero() || r.LeftEvaluatedAt.IsZero() || r.RightEvaluatedAt.IsZero() {
		return fmt.Errorf("readiness review timestamps are required")
	}
	r.AsOf, r.LeftEvaluatedAt, r.RightEvaluatedAt = r.AsOf.UTC(), r.LeftEvaluatedAt.UTC(), r.RightEvaluatedAt.UTC()
	if r.LeftAgeSeconds < 0 || r.RightAgeSeconds < 0 {
		return fmt.Errorf("readiness review age cannot be negative")
	}
	for field, values := range map[string]*[]string{"evidence_added": &r.EvidenceAdded, "evidence_removed": &r.EvidenceRemoved, "blocked_added": &r.BlockedAdded, "blocked_resolved": &r.BlockedResolved, "stale_reasons": &r.StaleReasons} {
		if len(*values) > MaxReadinessSnapshotReasons {
			return fmt.Errorf("readiness review %s exceeds bounds", field)
		}
		for i := range *values {
			(*values)[i], err = normalizeDomainIdentifier((*values)[i], "readiness review "+field, MaxDomainIDLength, true)
			if err != nil {
				return err
			}
		}
		sort.Strings(*values)
	}
	switch r.Outcome {
	case ReadinessReviewUnchanged, ReadinessReviewImproved, ReadinessReviewDegraded, ReadinessReviewChanged, ReadinessReviewStale:
	default:
		return fmt.Errorf("unsupported readiness review outcome %q", r.Outcome)
	}
	if r.ReviewHash != r.StableHash() {
		return fmt.Errorf("readiness review review_hash does not match comparison facts")
	}
	return normalizeQualificationAuditActor(&r.EvaluatedBy, r.WorkspaceID)
}

func (r ReadinessReview) StableHash() string {
	evidenceAdded := append([]string(nil), r.EvidenceAdded...)
	evidenceRemoved := append([]string(nil), r.EvidenceRemoved...)
	blockedAdded := append([]string(nil), r.BlockedAdded...)
	blockedResolved := append([]string(nil), r.BlockedResolved...)
	staleReasons := append([]string(nil), r.StaleReasons...)
	sort.Strings(evidenceAdded)
	sort.Strings(evidenceRemoved)
	sort.Strings(blockedAdded)
	sort.Strings(blockedResolved)
	sort.Strings(staleReasons)
	parts := []string{"readiness-review", r.WorkspaceID, r.DeploymentID, r.ReleaseID, r.LeftSnapshotHash, r.RightSnapshotHash, r.PolicyHash, fmt.Sprint(r.PolicyRevision), fmt.Sprint(r.MaxAgeSeconds), fmt.Sprint(r.LeftFresh), fmt.Sprint(r.RightFresh), fmt.Sprint(r.LeftReady), fmt.Sprint(r.RightReady), fmt.Sprint(r.GateChanged), r.Outcome}
	parts = append(parts, evidenceAdded...)
	parts = append(parts, evidenceRemoved...)
	parts = append(parts, blockedAdded...)
	parts = append(parts, blockedResolved...)
	parts = append(parts, staleReasons...)
	return HashStrings(parts...)
}
