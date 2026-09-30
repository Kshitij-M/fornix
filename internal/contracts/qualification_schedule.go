package contracts

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// QualificationRefreshScheduleSchemaVersion versions the durable deployment
// owned qualification handoff. The scheduler stores references to refresh
// reports; it never stores or produces deployment credentials or raw evidence.
const QualificationRefreshScheduleSchemaVersion = 1

const (
	QualificationRefreshScheduleActive     = "active"
	QualificationRefreshSchedulePaused     = "paused"
	QualificationRefreshScheduleCancelled  = "cancelled"
	QualificationRefreshScheduleDeadLetter = "dead_letter"

	QualificationRefreshScheduleOutcomeSucceeded = "succeeded"
	QualificationRefreshScheduleOutcomeRetryable = "retryable"
	QualificationRefreshScheduleOutcomeFailed    = "failed"
)

const (
	DefaultQualificationRefreshScheduleIntervalMS = int64(24 * time.Hour / time.Millisecond)
	MinQualificationRefreshScheduleIntervalMS     = int64(time.Minute / time.Millisecond)
	MaxQualificationRefreshScheduleIntervalMS     = int64(365 * 24 * time.Hour / time.Millisecond)
	MinQualificationRefreshScheduleFreshnessMS    = int64(time.Minute / time.Millisecond)
	MaxQualificationRefreshScheduleFreshnessMS    = int64(365 * 24 * time.Hour / time.Millisecond)
	DefaultQualificationRefreshScheduleBackoffMS  = int64(time.Second / time.Millisecond)
	MaxQualificationRefreshScheduleBackoffMS      = int64(24 * time.Hour / time.Millisecond)
	MaxQualificationRefreshScheduleAttempts       = 32
	MaxQualificationRefreshScheduleDrills         = 5
	MaxQualificationRefreshScheduleEventsMetadata = 4096
	DefaultQualificationRefreshScheduleLeaseMS    = int64(30 * time.Second / time.Millisecond)
	MaxQualificationRefreshScheduleLeaseMS        = int64(10 * time.Minute / time.Millisecond)
)

// QualificationRefreshScheduleRequest registers one immutable recurring
// qualification handoff. Every requirement is a closed identity or hash; a
// caller cannot smuggle raw deployment output into the scheduler.
type QualificationRefreshScheduleRequest struct {
	SchemaVersion          int        `json:"schema_version,omitempty"`
	WorkspaceID            string     `json:"workspace_id"`
	DeploymentID           string     `json:"deployment_id"`
	ReleaseID              string     `json:"release_id"`
	RequiredEvidenceKinds  []string   `json:"required_evidence_kinds"`
	RequiredRecoveryDrills []string   `json:"required_recovery_drills,omitempty"`
	IntervalMS             int64      `json:"interval_ms"`
	FreshnessMS            int64      `json:"freshness_ms"`
	MaxAttempts            int        `json:"max_attempts"`
	BackoffBaseMS          int64      `json:"backoff_base_ms"`
	BackoffMaxMS           int64      `json:"backoff_max_ms"`
	FirstDueAt             time.Time  `json:"first_due_at"`
	RequestID              string     `json:"request_id,omitempty"`
	IdempotencyKey         string     `json:"idempotency_key"`
	CausationID            string     `json:"causation_id,omitempty"`
	CorrelationID          string     `json:"correlation_id,omitempty"`
	Actor                  AuditActor `json:"actor"`
}

func (r *QualificationRefreshScheduleRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification refresh schedule request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = QualificationRefreshScheduleSchemaVersion
	}
	if r.SchemaVersion != QualificationRefreshScheduleSchemaVersion {
		return fmt.Errorf("unsupported qualification refresh schedule schema_version %d", r.SchemaVersion)
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification schedule deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.ReleaseID, err = normalizeDomainIdentifier(r.ReleaseID, "qualification schedule release_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.RequiredEvidenceKinds, err = normalizeScheduleEvidenceKinds(r.RequiredEvidenceKinds); err != nil {
		return err
	}
	if r.RequiredRecoveryDrills, err = normalizeScheduleRecoveryDrills(r.RequiredRecoveryDrills); err != nil {
		return err
	}
	if r.IntervalMS == 0 {
		r.IntervalMS = DefaultQualificationRefreshScheduleIntervalMS
	}
	if r.IntervalMS < MinQualificationRefreshScheduleIntervalMS || r.IntervalMS > MaxQualificationRefreshScheduleIntervalMS {
		return fmt.Errorf("qualification schedule interval_ms is outside bounds")
	}
	if r.FreshnessMS == 0 {
		r.FreshnessMS = r.IntervalMS
	}
	if r.FreshnessMS < MinQualificationRefreshScheduleFreshnessMS || r.FreshnessMS > MaxQualificationRefreshScheduleFreshnessMS {
		return fmt.Errorf("qualification schedule freshness_ms is outside bounds")
	}
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 5
	}
	if r.MaxAttempts < 1 || r.MaxAttempts > MaxQualificationRefreshScheduleAttempts {
		return fmt.Errorf("qualification schedule max_attempts is outside bounds")
	}
	if r.BackoffBaseMS == 0 {
		r.BackoffBaseMS = DefaultQualificationRefreshScheduleBackoffMS
	}
	if r.BackoffMaxMS == 0 {
		r.BackoffMaxMS = MaxQualificationRefreshScheduleBackoffMS
	}
	if r.BackoffBaseMS < 1 || r.BackoffBaseMS > MaxQualificationRefreshScheduleBackoffMS || r.BackoffMaxMS < r.BackoffBaseMS || r.BackoffMaxMS > MaxQualificationRefreshScheduleBackoffMS {
		return fmt.Errorf("qualification schedule backoff is outside bounds")
	}
	if r.FirstDueAt.IsZero() {
		return fmt.Errorf("qualification schedule first_due_at is required")
	}
	r.FirstDueAt = r.FirstDueAt.UTC()
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "qualification schedule request_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "qualification schedule idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "qualification schedule causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "qualification schedule correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

func (r QualificationRefreshScheduleRequest) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	parts := []string{"qualification-refresh-schedule", r.WorkspaceID, r.DeploymentID, r.ReleaseID, strconv.FormatInt(r.IntervalMS, 10), strconv.FormatInt(r.FreshnessMS, 10), strconv.Itoa(r.MaxAttempts), strconv.FormatInt(r.BackoffBaseMS, 10), strconv.FormatInt(r.BackoffMaxMS, 10), r.FirstDueAt.Format(time.RFC3339Nano)}
	parts = append(parts, r.RequiredEvidenceKinds...)
	parts = append(parts, r.RequiredRecoveryDrills...)
	return HashStrings(parts...)
}

// QualificationRefreshSchedule is the mutable projection. Its transitions
// are fenced and its attempts/events remain append-only history.
type QualificationRefreshSchedule struct {
	SchemaVersion          int        `json:"schema_version"`
	ID                     string     `json:"id"`
	WorkspaceID            string     `json:"workspace_id"`
	DeploymentID           string     `json:"deployment_id"`
	ReleaseID              string     `json:"release_id"`
	ConfigHash             string     `json:"config_hash"`
	RequiredEvidenceKinds  []string   `json:"required_evidence_kinds"`
	RequiredRecoveryDrills []string   `json:"required_recovery_drills,omitempty"`
	IntervalMS             int64      `json:"interval_ms"`
	FreshnessMS            int64      `json:"freshness_ms"`
	MaxAttempts            int        `json:"max_attempts"`
	BackoffBaseMS          int64      `json:"backoff_base_ms"`
	BackoffMaxMS           int64      `json:"backoff_max_ms"`
	Status                 string     `json:"status"`
	NextDueAt              *time.Time `json:"next_due_at,omitempty"`
	AttemptCount           int        `json:"attempt_count"`
	RetryCount             int        `json:"retry_count"`
	LastAsOf               *time.Time `json:"last_as_of,omitempty"`
	LastAttemptID          string     `json:"last_attempt_id,omitempty"`
	LastReportID           string     `json:"last_report_id,omitempty"`
	LastReportHash         string     `json:"last_report_hash,omitempty"`
	LastOutcome            string     `json:"last_outcome,omitempty"`
	LastErrorCode          string     `json:"last_error_code,omitempty"`
	LeaseOwnerID           string     `json:"lease_owner_id,omitempty"`
	LeaseFence             uint64     `json:"lease_fence,omitempty"`
	LeaseUntil             *time.Time `json:"lease_until,omitempty"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
}

type QualificationRefreshSchedulePage struct {
	Items      []QualificationRefreshSchedule `json:"items"`
	NextCursor string                         `json:"next_cursor,omitempty"`
}

// QualificationRefreshScheduleClaim is the deterministic work handoff a
// deployment-owned scheduler receives after a transactional claim.
type QualificationRefreshScheduleClaim struct {
	Schedule      QualificationRefreshSchedule `json:"schedule"`
	AttemptID     string                       `json:"attempt_id"`
	AttemptNumber int                          `json:"attempt_number"`
	OwnerID       string                       `json:"owner_id"`
	Fence         uint64                       `json:"fence"`
	LeaseUntil    time.Time                    `json:"lease_until"`
	AsOf          time.Time                    `json:"as_of"`
	PlanHash      string                       `json:"plan_hash"`
	Takeover      bool                         `json:"takeover"`
}

type QualificationRefreshScheduleCompletionRequest struct {
	WorkspaceID    string     `json:"workspace_id"`
	ScheduleID     string     `json:"schedule_id"`
	AttemptID      string     `json:"attempt_id"`
	OwnerID        string     `json:"owner_id"`
	Fence          uint64     `json:"fence"`
	AsOf           time.Time  `json:"as_of"`
	PlanHash       string     `json:"plan_hash"`
	RefreshID      string     `json:"refresh_id,omitempty"`
	RefreshHash    string     `json:"refresh_hash,omitempty"`
	Outcome        string     `json:"outcome"`
	Retryable      bool       `json:"retryable"`
	ErrorCode      string     `json:"error_code,omitempty"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
	Actor          AuditActor `json:"actor"`
}

func (r *QualificationRefreshScheduleCompletionRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification schedule completion is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	for field, value := range map[string]*string{"schedule_id": &r.ScheduleID, "attempt_id": &r.AttemptID, "owner_id": &r.OwnerID, "plan_hash": &r.PlanHash, "refresh_id": &r.RefreshID, "refresh_hash": &r.RefreshHash} {
		required := field != "refresh_id" && field != "refresh_hash"
		if strings.HasSuffix(field, "hash") {
			*value, err = normalizeDomainHash(*value, "qualification schedule "+field, required)
		} else {
			*value, err = normalizeDomainIdentifier(*value, "qualification schedule "+field, MaxDomainIDLength, required)
		}
		if err != nil {
			return err
		}
	}
	if r.Fence == 0 {
		return fmt.Errorf("qualification schedule fence is required")
	}
	if r.AsOf.IsZero() {
		return fmt.Errorf("qualification schedule as_of is required")
	}
	r.AsOf = r.AsOf.UTC()
	r.Outcome = strings.ToLower(strings.TrimSpace(r.Outcome))
	if r.Outcome != QualificationRefreshScheduleOutcomeSucceeded && r.Outcome != QualificationRefreshScheduleOutcomeRetryable && r.Outcome != QualificationRefreshScheduleOutcomeFailed {
		return fmt.Errorf("invalid qualification schedule outcome")
	}
	if r.Outcome == QualificationRefreshScheduleOutcomeSucceeded {
		if r.RefreshID == "" || r.RefreshHash == "" || r.Retryable || r.ErrorCode != "" {
			return fmt.Errorf("successful qualification schedule completion requires a refresh report only")
		}
	} else if r.Outcome == QualificationRefreshScheduleOutcomeRetryable {
		if !r.Retryable {
			return fmt.Errorf("retryable qualification schedule outcome must set retryable=true")
		}
	} else if r.Retryable {
		return fmt.Errorf("failed qualification schedule outcome cannot be retryable")
	}
	if r.ErrorCode, err = normalizeDomainName(r.ErrorCode, "qualification schedule error_code", MaxDomainNameLength, false); err != nil {
		return err
	}
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "qualification schedule request_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "qualification schedule idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "qualification schedule causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "qualification schedule correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

type QualificationRefreshScheduleAttempt struct {
	SchemaVersion  int        `json:"schema_version"`
	ID             string     `json:"id"`
	ScheduleID     string     `json:"schedule_id"`
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	ReleaseID      string     `json:"release_id"`
	AttemptNumber  int        `json:"attempt_number"`
	OwnerID        string     `json:"owner_id"`
	Fence          uint64     `json:"fence"`
	AsOf           time.Time  `json:"as_of"`
	PlanHash       string     `json:"plan_hash"`
	RefreshID      string     `json:"refresh_id,omitempty"`
	RefreshHash    string     `json:"refresh_hash,omitempty"`
	Outcome        string     `json:"outcome"`
	Retryable      bool       `json:"retryable"`
	ErrorCode      string     `json:"error_code,omitempty"`
	RetryAt        *time.Time `json:"retry_at,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CreatedAt      time.Time  `json:"created_at"`
}

type QualificationRefreshScheduleAttemptPage struct {
	Items      []QualificationRefreshScheduleAttempt `json:"items"`
	NextCursor string                                `json:"next_cursor,omitempty"`
}

func QualificationRefreshScheduleBackoffMS(base, max int64, attempt int) int64 {
	if base < 1 {
		base = DefaultQualificationRefreshScheduleBackoffMS
	}
	if max < base {
		max = base
	}
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 62 {
		attempt = 62
	}
	value := base
	for i := 1; i < attempt && value < max; i++ {
		if value > max/2 {
			value = max
		} else {
			value *= 2
		}
	}
	if value > max {
		return max
	}
	return value
}

// QualificationRefreshSchedulePlanHash is the stable identity of one claimed
// handoff. Owner and fence are included so a takeover cannot reuse a stale
// plan token.
func QualificationRefreshSchedulePlanHash(schedule QualificationRefreshSchedule, attemptID, ownerID string, fence uint64, asOf time.Time, attemptNumber int) string {
	return HashStrings("qualification-refresh-schedule-plan", schedule.ConfigHash, schedule.ID, attemptID, ownerID, strconv.FormatUint(fence, 10), asOf.UTC().Format(time.RFC3339Nano), strconv.Itoa(attemptNumber))
}

func normalizeScheduleEvidenceKinds(values []string) ([]string, error) {
	if len(values) == 0 || len(values) > MaxDeploymentEvidenceKinds {
		return nil, fmt.Errorf("qualification schedule requires between 1 and %d evidence kinds", MaxDeploymentEvidenceKinds)
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if !isDeploymentEvidenceKind(value) {
			return nil, fmt.Errorf("qualification schedule has unsupported evidence kind %q", value)
		}
		if _, ok := seen[value]; ok {
			return nil, fmt.Errorf("qualification schedule repeats evidence kind %q", value)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func normalizeScheduleRecoveryDrills(values []string) ([]string, error) {
	if len(values) > MaxQualificationRefreshScheduleDrills {
		return nil, fmt.Errorf("qualification schedule recovery drills exceed %d", MaxQualificationRefreshScheduleDrills)
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if !validRecoveryDrillKind(value) {
			return nil, fmt.Errorf("qualification schedule has unsupported recovery drill %q", value)
		}
		if _, ok := seen[value]; ok {
			return nil, fmt.Errorf("qualification schedule repeats recovery drill %q", value)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func normalizeQualificationScheduleStatus(value string) error {
	switch value {
	case QualificationRefreshScheduleActive, QualificationRefreshSchedulePaused, QualificationRefreshScheduleCancelled, QualificationRefreshScheduleDeadLetter:
		return nil
	default:
		return fmt.Errorf("invalid qualification schedule status %q", value)
	}
}

// ValidateQualificationRefreshScheduleStatus validates a value read from the
// durable projection before it is exposed to callers.
func ValidateQualificationRefreshScheduleStatus(value string) error {
	return normalizeQualificationScheduleStatus(value)
}
