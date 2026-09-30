package contracts

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// QualificationRefreshSchemaVersion versions the hash-only refresh contract.
const QualificationRefreshSchemaVersion = 1

const (
	MaxQualificationRefreshItems        = MaxDeploymentEvidenceKinds
	MaxQualificationRefreshReasonLength = 256
)

// QualificationRefreshItem identifies one accepted signed import to link to a
// release. Raw qualification bytes never cross this contract.
type QualificationRefreshItem struct {
	Kind             string `json:"kind"`
	ImportID         string `json:"import_id"`
	SupersedesLinkID string `json:"supersedes_link_id,omitempty"`
}

func (i *QualificationRefreshItem) Normalize(index int) error {
	if i == nil {
		return fmt.Errorf("qualification refresh item %d is nil", index)
	}
	var err error
	if i.Kind, err = normalizeDomainName(i.Kind, fmt.Sprintf("qualification refresh item %d kind", index), MaxDomainNameLength, true); err != nil {
		return err
	}
	if !isDeploymentEvidenceKind(i.Kind) {
		return fmt.Errorf("qualification refresh item %d has unsupported kind %q", index, i.Kind)
	}
	if i.ImportID, err = normalizeDomainIdentifier(i.ImportID, fmt.Sprintf("qualification refresh item %d import_id", index), MaxDomainIDLength, true); err != nil {
		return err
	}
	if i.SupersedesLinkID, err = normalizeDomainIdentifier(i.SupersedesLinkID, fmt.Sprintf("qualification refresh item %d supersedes_link_id", index), MaxDomainIDLength, false); err != nil {
		return err
	}
	return nil
}

// QualificationRefreshRequest is an explicit, bounded refresh plan. AsOf is
// required so a replay evaluates expiry against the same instant.
type QualificationRefreshRequest struct {
	WorkspaceID    string                     `json:"workspace_id"`
	DeploymentID   string                     `json:"deployment_id"`
	ReleaseID      string                     `json:"release_id"`
	Items          []QualificationRefreshItem `json:"items"`
	AsOf           time.Time                  `json:"as_of"`
	RequestID      string                     `json:"request_id,omitempty"`
	IdempotencyKey string                     `json:"idempotency_key"`
	CausationID    string                     `json:"causation_id,omitempty"`
	CorrelationID  string                     `json:"correlation_id,omitempty"`
	Actor          AuditActor                 `json:"actor"`
	DryRun         bool                       `json:"dry_run,omitempty"`
	// ScheduleAuthorization is present only when a deployment-owned scheduler
	// is submitting a refresh for a claimed handoff. It is validated inside the
	// same Postgres transaction as the evidence mutation.
	ScheduleAuthorization *QualificationRefreshScheduleAuthorization `json:"schedule_authorization,omitempty"`
}

// QualificationRefreshScheduleAuthorization prevents a worker that lost a
// schedule lease from mutating the authoritative evidence projection.
type QualificationRefreshScheduleAuthorization struct {
	ScheduleID string `json:"schedule_id"`
	AttemptID  string `json:"attempt_id"`
	OwnerID    string `json:"owner_id"`
	Fence      uint64 `json:"fence"`
	PlanHash   string `json:"plan_hash"`
}

func (a *QualificationRefreshScheduleAuthorization) Normalize() error {
	if a == nil {
		return fmt.Errorf("qualification schedule authorization is nil")
	}
	var err error
	if a.ScheduleID, err = normalizeDomainIdentifier(a.ScheduleID, "qualification schedule authorization schedule_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if a.AttemptID, err = normalizeDomainIdentifier(a.AttemptID, "qualification schedule authorization attempt_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if a.OwnerID, err = normalizeDomainIdentifier(a.OwnerID, "qualification schedule authorization owner_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if a.Fence == 0 {
		return fmt.Errorf("qualification schedule authorization fence is required")
	}
	if a.PlanHash, err = normalizeDomainHash(a.PlanHash, "qualification schedule authorization plan_hash", true); err != nil {
		return err
	}
	return nil
}

func (r *QualificationRefreshRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification refresh request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification refresh deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.ReleaseID, err = normalizeDomainIdentifier(r.ReleaseID, "qualification refresh release_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if len(r.Items) == 0 || len(r.Items) > MaxQualificationRefreshItems {
		return fmt.Errorf("qualification refresh requires between 1 and %d items", MaxQualificationRefreshItems)
	}
	seenKinds := make(map[string]struct{}, len(r.Items))
	seenImports := make(map[string]struct{}, len(r.Items))
	for index := range r.Items {
		if err := r.Items[index].Normalize(index); err != nil {
			return err
		}
		if _, exists := seenKinds[r.Items[index].Kind]; exists {
			return fmt.Errorf("qualification refresh repeats kind %q", r.Items[index].Kind)
		}
		if _, exists := seenImports[r.Items[index].ImportID]; exists {
			return fmt.Errorf("qualification refresh repeats import_id %q", r.Items[index].ImportID)
		}
		seenKinds[r.Items[index].Kind] = struct{}{}
		seenImports[r.Items[index].ImportID] = struct{}{}
	}
	sort.Slice(r.Items, func(i, j int) bool {
		if r.Items[i].Kind != r.Items[j].Kind {
			return r.Items[i].Kind < r.Items[j].Kind
		}
		return r.Items[i].ImportID < r.Items[j].ImportID
	})
	if r.AsOf.IsZero() {
		return fmt.Errorf("qualification refresh as_of is required")
	}
	r.AsOf = r.AsOf.UTC()
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "qualification refresh request_id", MaxIdempotencyLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "qualification refresh idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "qualification refresh causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "qualification refresh correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.ScheduleAuthorization != nil {
		if err := r.ScheduleAuthorization.Normalize(); err != nil {
			return err
		}
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

// StableHash identifies the requested facts and intentionally excludes actor,
// request ids, and DryRun so an operator retry cannot create a second plan.
func (r QualificationRefreshRequest) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	parts := []string{"qualification-refresh-request", r.WorkspaceID, r.DeploymentID, r.ReleaseID, r.AsOf.Format(time.RFC3339Nano)}
	for _, item := range r.Items {
		parts = append(parts, item.Kind, item.ImportID, item.SupersedesLinkID)
	}
	return HashStrings(parts...)
}

// QualificationRefreshItemResult is a bounded hash-only outcome for one
// candidate. Reasons are stable machine-readable diagnostics, not raw errors.
type QualificationRefreshItemResult struct {
	Kind                  string     `json:"kind"`
	ImportID              string     `json:"import_id"`
	PreviousLinkID        string     `json:"previous_link_id,omitempty"`
	LinkID                string     `json:"link_id"`
	SignedHash            string     `json:"signed_hash"`
	ObservationHash       string     `json:"observation_hash"`
	ReportHash            string     `json:"report_hash"`
	ManifestHash          string     `json:"manifest_hash"`
	SourceHash            string     `json:"source_hash"`
	BoundaryEvidenceHash  string     `json:"boundary_evidence_hash,omitempty"`
	BoundaryEvidenceUntil *time.Time `json:"boundary_evidence_until,omitempty"`
	Outcome               string     `json:"outcome"`
	Reason                string     `json:"reason,omitempty"`
}

func (i *QualificationRefreshItemResult) Normalize(index int) error {
	if i == nil {
		return fmt.Errorf("qualification refresh item result %d is nil", index)
	}
	var err error
	if i.Kind, err = normalizeDomainName(i.Kind, fmt.Sprintf("qualification refresh result item %d kind", index), MaxDomainNameLength, true); err != nil {
		return err
	}
	if !isDeploymentEvidenceKind(i.Kind) {
		return fmt.Errorf("qualification refresh result item %d has unsupported kind", index)
	}
	for field, value := range map[string]*string{
		"import_id": &i.ImportID, "previous_link_id": &i.PreviousLinkID, "link_id": &i.LinkID,
	} {
		if *value, err = normalizeDomainIdentifier(*value, "qualification refresh result "+field, MaxDomainIDLength, field != "previous_link_id"); err != nil {
			return err
		}
	}
	for field, value := range map[string]*string{
		"signed_hash": &i.SignedHash, "observation_hash": &i.ObservationHash, "report_hash": &i.ReportHash, "manifest_hash": &i.ManifestHash, "source_hash": &i.SourceHash, "boundary_evidence_hash": &i.BoundaryEvidenceHash,
	} {
		if *value, err = normalizeDomainHash(*value, "qualification refresh result "+field, field != "boundary_evidence_hash"); err != nil {
			return err
		}
	}
	if i.BoundaryEvidenceUntil != nil {
		value := i.BoundaryEvidenceUntil.UTC()
		i.BoundaryEvidenceUntil = &value
	}
	i.Outcome = strings.ToLower(strings.TrimSpace(i.Outcome))
	if err := normalizeQualificationOutcome(i.Outcome); err != nil {
		return err
	}
	i.Reason = strings.TrimSpace(i.Reason)
	if len(i.Reason) > MaxQualificationRefreshReasonLength || strings.ContainsAny(i.Reason, "\x00\r\n") {
		return fmt.Errorf("qualification refresh result reason is invalid")
	}
	return nil
}

// QualificationRefreshReport is the durable, replay-comparable refresh
// result. It contains no raw signed bytes or deployment secrets.
type QualificationRefreshReport struct {
	SchemaVersion  int                              `json:"schema_version"`
	ID             string                           `json:"id"`
	WorkspaceID    string                           `json:"workspace_id"`
	DeploymentID   string                           `json:"deployment_id"`
	ReleaseID      string                           `json:"release_id"`
	AsOf           time.Time                        `json:"as_of"`
	RequestHash    string                           `json:"request_hash"`
	BeforeGateHash string                           `json:"before_gate_hash"`
	AfterGateHash  string                           `json:"after_gate_hash"`
	Items          []QualificationRefreshItemResult `json:"items"`
	Outcome        string                           `json:"outcome"`
	DryRun         bool                             `json:"dry_run,omitempty"`
	ReportHash     string                           `json:"report_hash"`
	CreatedAt      time.Time                        `json:"created_at"`
}

func (r *QualificationRefreshReport) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification refresh report is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = QualificationRefreshSchemaVersion
	}
	if r.SchemaVersion != QualificationRefreshSchemaVersion {
		return fmt.Errorf("unsupported qualification refresh schema_version %d", r.SchemaVersion)
	}
	var err error
	if r.ID, err = normalizeDomainIdentifier(r.ID, "qualification refresh report id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification refresh report deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.ReleaseID, err = normalizeDomainIdentifier(r.ReleaseID, "qualification refresh report release_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.AsOf.IsZero() {
		return fmt.Errorf("qualification refresh report as_of is required")
	}
	r.AsOf = r.AsOf.UTC()
	for field, value := range map[string]*string{"request_hash": &r.RequestHash, "before_gate_hash": &r.BeforeGateHash, "after_gate_hash": &r.AfterGateHash} {
		if *value, err = normalizeDomainHash(*value, "qualification refresh report "+field, true); err != nil {
			return err
		}
	}
	if len(r.Items) == 0 || len(r.Items) > MaxQualificationRefreshItems {
		return fmt.Errorf("qualification refresh report item count is invalid")
	}
	seen := make(map[string]struct{}, len(r.Items))
	for index := range r.Items {
		if err := r.Items[index].Normalize(index); err != nil {
			return err
		}
		if _, ok := seen[r.Items[index].Kind]; ok {
			return fmt.Errorf("qualification refresh report repeats kind %q", r.Items[index].Kind)
		}
		seen[r.Items[index].Kind] = struct{}{}
	}
	sort.Slice(r.Items, func(i, j int) bool { return r.Items[i].Kind < r.Items[j].Kind })
	r.Outcome = strings.ToLower(strings.TrimSpace(r.Outcome))
	if err := normalizeQualificationOutcome(r.Outcome); err != nil {
		return err
	}
	if r.ReportHash != "" {
		if r.ReportHash, err = normalizeDomainHash(r.ReportHash, "qualification refresh report report_hash", true); err != nil {
			return err
		}
	}
	if !r.CreatedAt.IsZero() {
		r.CreatedAt = r.CreatedAt.UTC()
	}
	calculated := qualificationRefreshReportHash(*r)
	if r.ReportHash != "" && r.ReportHash != calculated {
		return fmt.Errorf("qualification refresh report hash does not match report")
	}
	r.ReportHash = calculated
	return nil
}

func (r QualificationRefreshReport) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	return qualificationRefreshReportHash(r)
}

func qualificationRefreshReportHash(r QualificationRefreshReport) string {
	parts := []string{"qualification-refresh-report", r.WorkspaceID, r.DeploymentID, r.ReleaseID, r.AsOf.UTC().Format(time.RFC3339Nano), r.RequestHash, r.BeforeGateHash, r.AfterGateHash, r.Outcome, fmt.Sprint(r.DryRun)}
	items := append([]QualificationRefreshItemResult(nil), r.Items...)
	sort.Slice(items, func(i, j int) bool { return items[i].Kind < items[j].Kind })
	for _, item := range items {
		parts = append(parts, item.Kind, item.ImportID, item.PreviousLinkID, item.LinkID, item.SignedHash, item.ObservationHash, item.ReportHash, item.ManifestHash, item.SourceHash, item.BoundaryEvidenceHash, item.Outcome, item.Reason)
		if item.BoundaryEvidenceUntil != nil {
			parts = append(parts, item.BoundaryEvidenceUntil.UTC().Format(time.RFC3339Nano))
		}
	}
	return HashStrings(parts...)
}

type QualificationRefreshResult struct {
	Report       QualificationRefreshReport `json:"report"`
	Created      bool                       `json:"created"`
	Deduplicated bool                       `json:"deduplicated"`
	DryRun       bool                       `json:"dry_run"`
}

type QualificationRefreshPage struct {
	Items      []QualificationRefreshReport `json:"items"`
	NextCursor string                       `json:"next_cursor,omitempty"`
}
