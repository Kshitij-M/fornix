package contracts

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const DeploymentEvidenceSchemaVersion = 1

const (
	DeploymentReleaseActive  = "active"
	DeploymentReleaseRetired = "retired"

	DeploymentEvidenceRelease        = "release"
	DeploymentEvidenceMigration      = "migration"
	DeploymentEvidenceBackupRestore  = "backup_restore"
	DeploymentEvidenceTopology       = "topology"
	DeploymentEvidenceProvider       = "provider"
	DeploymentEvidenceExternalEffect = "external_effect"

	DeploymentEvidenceResolved         = "resolved"
	DeploymentEvidenceRecoveryRequired = "recovery_required"
	DeploymentEvidenceUnknown          = "unknown"
	DeploymentEvidenceNotApplicable    = "not_applicable"
	DeploymentEvidenceLinkActive       = "active"
	DeploymentEvidenceLinkRevoked      = "revoked"
	DeploymentEvidenceLinkSuperseded   = "superseded"

	MaxDeploymentEvidenceKinds        = 6
	MaxDeploymentVersionLength        = 128
	MaxDeploymentEvidenceReasonLength = 256
)

// DeploymentEvidenceKinds is the closed evidence vocabulary used by a
// release gate. The kinds describe deployment-owned proof; they do not cause
// Fornix to execute the corresponding operation.
var DeploymentEvidenceKinds = []string{
	DeploymentEvidenceRelease, DeploymentEvidenceMigration, DeploymentEvidenceBackupRestore,
	DeploymentEvidenceTopology, DeploymentEvidenceProvider, DeploymentEvidenceExternalEffect,
}

var DeploymentDefaultRequiredEvidenceKinds = []string{
	DeploymentEvidenceMigration, DeploymentEvidenceBackupRestore,
	DeploymentEvidenceTopology, DeploymentEvidenceProvider, DeploymentEvidenceExternalEffect,
}

// DeploymentRelease is an immutable workspace/deployment release identity
// bound to the exact qualification trust snapshot used to register it.
type DeploymentRelease struct {
	SchemaVersion         int        `json:"schema_version"`
	ID                    string     `json:"id"`
	WorkspaceID           string     `json:"workspace_id"`
	DeploymentID          string     `json:"deployment_id"`
	ReleaseHash           string     `json:"release_hash"`
	TargetHash            string     `json:"target_hash"`
	Version               string     `json:"version"`
	CommitHash            string     `json:"commit_hash,omitempty"`
	TrustSnapshotRevision int64      `json:"trust_snapshot_revision"`
	TrustSnapshotHash     string     `json:"trust_snapshot_hash"`
	Status                string     `json:"status"`
	Actor                 AuditActor `json:"actor"`
	CreatedAt             time.Time  `json:"created_at"`
}

func (r *DeploymentRelease) Normalize() error {
	if r == nil {
		return fmt.Errorf("deployment release is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DeploymentEvidenceSchemaVersion
	}
	if r.SchemaVersion != DeploymentEvidenceSchemaVersion {
		return fmt.Errorf("unsupported deployment release schema_version %d", r.SchemaVersion)
	}
	var err error
	if r.ID, err = normalizeDomainIdentifier(r.ID, "deployment release id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "deployment release deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.ReleaseHash, err = normalizeDomainHash(r.ReleaseHash, "deployment release release_hash", true); err != nil {
		return err
	}
	if r.TargetHash, err = normalizeDomainHash(r.TargetHash, "deployment release target_hash", true); err != nil {
		return err
	}
	if r.Version, err = normalizeDomainIdentifier(r.Version, "deployment release version", MaxDeploymentVersionLength, true); err != nil {
		return err
	}
	if r.CommitHash, err = normalizeDomainHash(r.CommitHash, "deployment release commit_hash", false); err != nil {
		return err
	}
	if r.TrustSnapshotRevision < 1 {
		return fmt.Errorf("deployment release trust snapshot revision must be positive")
	}
	if r.TrustSnapshotHash, err = normalizeDomainHash(r.TrustSnapshotHash, "deployment release trust_snapshot_hash", true); err != nil {
		return err
	}
	r.Status = strings.ToLower(strings.TrimSpace(r.Status))
	if r.Status == "" {
		r.Status = DeploymentReleaseActive
	}
	if r.Status != DeploymentReleaseActive && r.Status != DeploymentReleaseRetired {
		return fmt.Errorf("invalid deployment release status %q", r.Status)
	}
	if err := normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID); err != nil {
		return err
	}
	if !r.CreatedAt.IsZero() {
		r.CreatedAt = r.CreatedAt.UTC()
	}
	return nil
}

// DeploymentReleaseRequest registers one immutable release identity. The
// store supplies the current trust snapshot and ignores caller authority
// claims that are not represented by this bounded contract.
type DeploymentReleaseRequest struct {
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	ReleaseHash    string     `json:"release_hash"`
	TargetHash     string     `json:"target_hash"`
	Version        string     `json:"version"`
	CommitHash     string     `json:"commit_hash,omitempty"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
	Actor          AuditActor `json:"actor"`
	DryRun         bool       `json:"dry_run,omitempty"`
}

func (r *DeploymentReleaseRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("deployment release request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "deployment release request deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.ReleaseHash, err = normalizeDomainHash(r.ReleaseHash, "deployment release request release_hash", true); err != nil {
		return err
	}
	if r.TargetHash, err = normalizeDomainHash(r.TargetHash, "deployment release request target_hash", true); err != nil {
		return err
	}
	if r.Version, err = normalizeDomainIdentifier(r.Version, "deployment release request version", MaxDeploymentVersionLength, true); err != nil {
		return err
	}
	if r.CommitHash, err = normalizeDomainHash(r.CommitHash, "deployment release request commit_hash", false); err != nil {
		return err
	}
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "deployment release request request_id", MaxIdempotencyLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "deployment release request idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "deployment release request causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "deployment release request correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

// DeploymentEvidenceLink is a hash-only reference to one accepted signed
// qualification import. It never duplicates the imported raw bytes.
type DeploymentEvidenceLink struct {
	SchemaVersion             int        `json:"schema_version"`
	ID                        string     `json:"id"`
	WorkspaceID               string     `json:"workspace_id"`
	DeploymentID              string     `json:"deployment_id"`
	ReleaseID                 string     `json:"release_id"`
	Kind                      string     `json:"kind"`
	ImportID                  string     `json:"import_id"`
	TargetHash                string     `json:"target_hash"`
	SignedHash                string     `json:"signed_hash"`
	ObservationHash           string     `json:"observation_hash"`
	ReportHash                string     `json:"report_hash"`
	ManifestHash              string     `json:"manifest_hash"`
	SourceHash                string     `json:"source_hash"`
	ExternalBoundaryHash      string     `json:"external_boundary_hash,omitempty"`
	BoundaryEvidenceHash      string     `json:"boundary_evidence_hash,omitempty"`
	BoundaryEvidenceExpiresAt *time.Time `json:"boundary_evidence_expires_at,omitempty"`
	TrustSnapshotRevision     int64      `json:"trust_snapshot_revision"`
	TrustSnapshotHash         string     `json:"trust_snapshot_hash"`
	Outcome                   string     `json:"outcome"`
	RecoveryState             string     `json:"recovery_state"`
	Status                    string     `json:"status"`
	SupersedesLinkID          string     `json:"supersedes_link_id,omitempty"`
	SupersededByLinkID        string     `json:"superseded_by_link_id,omitempty"`
	RevokedAt                 *time.Time `json:"revoked_at,omitempty"`
	RevocationReason          string     `json:"revocation_reason,omitempty"`
	Actor                     AuditActor `json:"actor"`
	CreatedAt                 time.Time  `json:"created_at"`
}

func (l *DeploymentEvidenceLink) Normalize() error {
	if l == nil {
		return fmt.Errorf("deployment evidence link is nil")
	}
	if l.SchemaVersion == 0 {
		l.SchemaVersion = DeploymentEvidenceSchemaVersion
	}
	if l.SchemaVersion != DeploymentEvidenceSchemaVersion {
		return fmt.Errorf("unsupported deployment evidence schema_version %d", l.SchemaVersion)
	}
	var err error
	for field, value := range map[string]*string{
		"id": &l.ID, "release_id": &l.ReleaseID, "import_id": &l.ImportID,
	} {
		if *value, err = normalizeDomainIdentifier(*value, "deployment evidence "+field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	for field, value := range map[string]*string{
		"supersedes_link_id": &l.SupersedesLinkID, "superseded_by_link_id": &l.SupersededByLinkID,
	} {
		if *value, err = normalizeDomainIdentifier(*value, "deployment evidence "+field, MaxDomainIDLength, false); err != nil {
			return err
		}
	}
	if l.BoundaryEvidenceExpiresAt != nil {
		value := l.BoundaryEvidenceExpiresAt.UTC()
		l.BoundaryEvidenceExpiresAt = &value
		if l.ExternalBoundaryHash == "" {
			return fmt.Errorf("deployment evidence boundary expiry requires external boundary hash")
		}
	}
	if l.ExternalBoundaryHash != "" && l.BoundaryEvidenceExpiresAt == nil {
		return fmt.Errorf("deployment evidence external boundary hash requires expiry")
	}
	if l.WorkspaceID, err = normalizeDomainWorkspace(l.WorkspaceID); err != nil {
		return err
	}
	if l.DeploymentID, err = normalizeDomainIdentifier(l.DeploymentID, "deployment evidence deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if l.Kind, err = normalizeDomainName(l.Kind, "deployment evidence kind", MaxDomainNameLength, true); err != nil {
		return err
	}
	if !isDeploymentEvidenceKind(l.Kind) {
		return fmt.Errorf("unsupported deployment evidence kind %q", l.Kind)
	}
	for field, value := range map[string]*string{
		"target_hash": &l.TargetHash, "signed_hash": &l.SignedHash, "observation_hash": &l.ObservationHash,
		"report_hash": &l.ReportHash, "manifest_hash": &l.ManifestHash, "source_hash": &l.SourceHash,
		"trust_snapshot_hash": &l.TrustSnapshotHash,
	} {
		if *value, err = normalizeDomainHash(*value, "deployment evidence "+field, true); err != nil {
			return err
		}
	}
	for field, value := range map[string]*string{
		"external_boundary_hash": &l.ExternalBoundaryHash, "boundary_evidence_hash": &l.BoundaryEvidenceHash,
	} {
		if *value, err = normalizeDomainHash(*value, "deployment evidence "+field, false); err != nil {
			return err
		}
	}
	if (l.ExternalBoundaryHash == "") != (l.BoundaryEvidenceHash == "") {
		return fmt.Errorf("deployment evidence boundary hashes must be supplied together")
	}
	if l.TrustSnapshotRevision < 1 {
		return fmt.Errorf("deployment evidence trust snapshot revision must be positive")
	}
	l.Outcome = strings.ToLower(strings.TrimSpace(l.Outcome))
	if err := normalizeQualificationOutcome(l.Outcome); err != nil {
		return err
	}
	l.RecoveryState = strings.ToLower(strings.TrimSpace(l.RecoveryState))
	if l.RecoveryState == "" {
		l.RecoveryState = DeploymentEvidenceNotApplicable
	}
	if !isDeploymentRecoveryState(l.RecoveryState) {
		return fmt.Errorf("unsupported deployment evidence recovery_state %q", l.RecoveryState)
	}
	l.Status = strings.ToLower(strings.TrimSpace(l.Status))
	if l.Status == "" {
		l.Status = DeploymentEvidenceLinkActive
	}
	if l.Status != DeploymentEvidenceLinkActive && l.Status != DeploymentEvidenceLinkRevoked && l.Status != DeploymentEvidenceLinkSuperseded {
		return fmt.Errorf("unsupported deployment evidence link status %q", l.Status)
	}
	if l.RevokedAt != nil {
		value := l.RevokedAt.UTC()
		l.RevokedAt = &value
		if l.Status != DeploymentEvidenceLinkRevoked {
			return fmt.Errorf("deployment evidence revoked_at requires revoked status")
		}
	}
	l.RevocationReason = strings.TrimSpace(l.RevocationReason)
	if len(l.RevocationReason) > MaxDeploymentEvidenceReasonLength || strings.ContainsAny(l.RevocationReason, "\x00\r\n") {
		return fmt.Errorf("deployment evidence revocation_reason is invalid")
	}
	switch l.Status {
	case DeploymentEvidenceLinkActive:
		if l.SupersededByLinkID != "" || l.RevokedAt != nil || l.RevocationReason != "" {
			return fmt.Errorf("active deployment evidence link has lifecycle terminal fields")
		}
	case DeploymentEvidenceLinkRevoked:
		if l.RevokedAt == nil || l.RevocationReason == "" || l.SupersededByLinkID != "" {
			return fmt.Errorf("revoked deployment evidence link requires reason and timestamp")
		}
	case DeploymentEvidenceLinkSuperseded:
		if l.SupersededByLinkID == "" || l.RevokedAt != nil || l.RevocationReason != "" {
			return fmt.Errorf("superseded deployment evidence link requires successor")
		}
	}
	if err := normalizeQualificationAuditActor(&l.Actor, l.WorkspaceID); err != nil {
		return err
	}
	if !l.CreatedAt.IsZero() {
		l.CreatedAt = l.CreatedAt.UTC()
	}
	return nil
}

// DeploymentEvidenceLinkRequest links an accepted import to a release. It is
// deliberately reference-only; callers cannot smuggle a second raw report.
type DeploymentEvidenceLinkRequest struct {
	WorkspaceID      string     `json:"workspace_id"`
	DeploymentID     string     `json:"deployment_id"`
	ReleaseID        string     `json:"release_id"`
	Kind             string     `json:"kind"`
	ImportID         string     `json:"import_id"`
	SupersedesLinkID string     `json:"supersedes_link_id,omitempty"`
	RequestID        string     `json:"request_id,omitempty"`
	IdempotencyKey   string     `json:"idempotency_key"`
	CausationID      string     `json:"causation_id,omitempty"`
	CorrelationID    string     `json:"correlation_id,omitempty"`
	Actor            AuditActor `json:"actor"`
	DryRun           bool       `json:"dry_run,omitempty"`
}

func (r *DeploymentEvidenceLinkRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("deployment evidence link request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "deployment evidence request deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	for field, value := range map[string]*string{"release_id": &r.ReleaseID, "import_id": &r.ImportID} {
		if *value, err = normalizeDomainIdentifier(*value, "deployment evidence request "+field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	if r.SupersedesLinkID, err = normalizeDomainIdentifier(r.SupersedesLinkID, "deployment evidence request supersedes_link_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.Kind, err = normalizeDomainName(r.Kind, "deployment evidence request kind", MaxDomainNameLength, true); err != nil {
		return err
	}
	if !isDeploymentEvidenceKind(r.Kind) {
		return fmt.Errorf("unsupported deployment evidence kind %q", r.Kind)
	}
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "deployment evidence request request_id", MaxIdempotencyLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "deployment evidence request idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "deployment evidence request causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "deployment evidence request correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

// DeploymentEvidenceRevocationRequest revokes one active evidence link while
// preserving the imported bundle and all lifecycle history.
type DeploymentEvidenceRevocationRequest struct {
	WorkspaceID    string     `json:"workspace_id"`
	DeploymentID   string     `json:"deployment_id"`
	ReleaseID      string     `json:"release_id"`
	Kind           string     `json:"kind"`
	LinkID         string     `json:"link_id"`
	Reason         string     `json:"reason"`
	RequestID      string     `json:"request_id,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	CausationID    string     `json:"causation_id,omitempty"`
	CorrelationID  string     `json:"correlation_id,omitempty"`
	Actor          AuditActor `json:"actor"`
	DryRun         bool       `json:"dry_run,omitempty"`
}

func (r *DeploymentEvidenceRevocationRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("deployment evidence revocation request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "deployment evidence revocation deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	for field, value := range map[string]*string{"release_id": &r.ReleaseID, "link_id": &r.LinkID} {
		if *value, err = normalizeDomainIdentifier(*value, "deployment evidence revocation "+field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	if r.Kind, err = normalizeDomainName(r.Kind, "deployment evidence revocation kind", MaxDomainNameLength, true); err != nil {
		return err
	}
	if !isDeploymentEvidenceKind(r.Kind) {
		return fmt.Errorf("unsupported deployment evidence revocation kind %q", r.Kind)
	}
	r.Reason = strings.TrimSpace(r.Reason)
	if r.Reason == "" || len(r.Reason) > MaxDeploymentEvidenceReasonLength || strings.ContainsAny(r.Reason, "\x00\r\n") {
		return fmt.Errorf("deployment evidence revocation reason is invalid")
	}
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "deployment evidence revocation request_id", MaxIdempotencyLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "deployment evidence revocation idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "deployment evidence revocation causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "deployment evidence revocation correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

// DeploymentQualificationGate is a deterministic read-only qualification
// projection. GateHash excludes timestamps and is stable for the same facts.
type DeploymentQualificationGate struct {
	SchemaVersion             int                      `json:"schema_version"`
	WorkspaceID               string                   `json:"workspace_id"`
	DeploymentID              string                   `json:"deployment_id"`
	ReleaseID                 string                   `json:"release_id"`
	ReleaseHash               string                   `json:"release_hash"`
	TrustSnapshotRevision     int64                    `json:"trust_snapshot_revision"`
	TrustSnapshotHash         string                   `json:"trust_snapshot_hash"`
	RequiredKinds             []string                 `json:"required_kinds"`
	Links                     []DeploymentEvidenceLink `json:"links"`
	MissingKinds              []string                 `json:"missing_kinds,omitempty"`
	BlockedReasons            []string                 `json:"blocked_reasons,omitempty"`
	SnapshotCurrent           bool                     `json:"snapshot_current"`
	Ready                     bool                     `json:"ready"`
	ExternalBoundaryHash      string                   `json:"external_boundary_hash,omitempty"`
	BoundaryEvidenceHash      string                   `json:"boundary_evidence_hash,omitempty"`
	BoundaryEvidenceExpiresAt *time.Time               `json:"boundary_evidence_expires_at,omitempty"`
	GateHash                  string                   `json:"gate_hash"`
}

type DeploymentReleasePage struct {
	Items      []DeploymentRelease `json:"items"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

type DeploymentEvidencePage struct {
	Items      []DeploymentEvidenceLink `json:"items"`
	NextCursor string                   `json:"next_cursor,omitempty"`
}

func (g *DeploymentQualificationGate) Normalize() error {
	if g == nil {
		return fmt.Errorf("deployment qualification gate is nil")
	}
	if g.SchemaVersion == 0 {
		g.SchemaVersion = DeploymentEvidenceSchemaVersion
	}
	if g.SchemaVersion != DeploymentEvidenceSchemaVersion {
		return fmt.Errorf("unsupported deployment qualification gate schema_version %d", g.SchemaVersion)
	}
	var err error
	if g.WorkspaceID, err = normalizeDomainWorkspace(g.WorkspaceID); err != nil {
		return err
	}
	if g.DeploymentID, err = normalizeDomainIdentifier(g.DeploymentID, "deployment gate deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if g.ReleaseID, err = normalizeDomainIdentifier(g.ReleaseID, "deployment gate release_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if g.ReleaseHash, err = normalizeDomainHash(g.ReleaseHash, "deployment gate release_hash", true); err != nil {
		return err
	}
	if g.TrustSnapshotRevision < 1 {
		return fmt.Errorf("deployment gate trust snapshot revision must be positive")
	}
	if g.TrustSnapshotHash, err = normalizeDomainHash(g.TrustSnapshotHash, "deployment gate trust_snapshot_hash", true); err != nil {
		return err
	}
	if g.ExternalBoundaryHash, err = normalizeDomainHash(g.ExternalBoundaryHash, "deployment gate external_boundary_hash", false); err != nil {
		return err
	}
	if g.BoundaryEvidenceHash, err = normalizeDomainHash(g.BoundaryEvidenceHash, "deployment gate boundary_evidence_hash", false); err != nil {
		return err
	}
	if g.BoundaryEvidenceExpiresAt != nil {
		value := g.BoundaryEvidenceExpiresAt.UTC()
		g.BoundaryEvidenceExpiresAt = &value
	}
	if (g.ExternalBoundaryHash == "") != (g.BoundaryEvidenceHash == "") {
		return fmt.Errorf("deployment gate boundary hashes must be supplied together")
	}
	if g.ExternalBoundaryHash != "" && g.BoundaryEvidenceExpiresAt == nil {
		return fmt.Errorf("deployment gate external boundary hash requires expiry")
	}
	if len(g.RequiredKinds) == 0 || len(g.RequiredKinds) > MaxDeploymentEvidenceKinds {
		return fmt.Errorf("deployment gate requires between 1 and %d evidence kinds", MaxDeploymentEvidenceKinds)
	}
	for i := range g.RequiredKinds {
		g.RequiredKinds[i], err = normalizeDomainName(g.RequiredKinds[i], "deployment gate required kind", MaxDomainNameLength, true)
		if err != nil || !isDeploymentEvidenceKind(g.RequiredKinds[i]) {
			return fmt.Errorf("unsupported deployment gate required kind %q", g.RequiredKinds[i])
		}
	}
	sort.Strings(g.RequiredKinds)
	for i := 1; i < len(g.RequiredKinds); i++ {
		if g.RequiredKinds[i] == g.RequiredKinds[i-1] {
			return fmt.Errorf("deployment gate repeats required kind %q", g.RequiredKinds[i])
		}
	}
	for i := range g.Links {
		if err := g.Links[i].Normalize(); err != nil {
			return err
		}
	}
	sort.Slice(g.Links, func(i, j int) bool {
		if g.Links[i].Kind != g.Links[j].Kind {
			return g.Links[i].Kind < g.Links[j].Kind
		}
		return g.Links[i].ID < g.Links[j].ID
	})
	if len(g.MissingKinds) > MaxDeploymentEvidenceKinds || len(g.BlockedReasons) > MaxDeploymentEvidenceKinds+2 {
		return fmt.Errorf("deployment gate diagnostics exceed bounds")
	}
	sort.Strings(g.MissingKinds)
	sort.Strings(g.BlockedReasons)
	return nil
}

func (g DeploymentQualificationGate) StableHash() string {
	if err := g.Normalize(); err != nil {
		return ""
	}
	parts := []string{"deployment-qualification-gate", g.WorkspaceID, g.DeploymentID, g.ReleaseID, g.ReleaseHash, fmt.Sprint(g.TrustSnapshotRevision), g.TrustSnapshotHash, fmt.Sprint(g.SnapshotCurrent), fmt.Sprint(g.Ready)}
	parts = append(parts, g.RequiredKinds...)
	for _, link := range g.Links {
		parts = append(parts, link.Kind, link.ImportID, link.SignedHash, link.ObservationHash, link.ReportHash, link.ManifestHash, link.SourceHash, link.ExternalBoundaryHash, link.BoundaryEvidenceHash, link.TrustSnapshotHash, link.Outcome, link.RecoveryState, link.Status, link.SupersedesLinkID, link.SupersededByLinkID, link.RevocationReason)
		if link.RevokedAt != nil {
			parts = append(parts, link.RevokedAt.UTC().Format(time.RFC3339Nano))
		}
	}
	parts = append(parts, g.ExternalBoundaryHash, g.BoundaryEvidenceHash)
	if g.BoundaryEvidenceExpiresAt != nil {
		parts = append(parts, g.BoundaryEvidenceExpiresAt.UTC().Format(time.RFC3339Nano))
	}
	parts = append(parts, g.MissingKinds...)
	parts = append(parts, g.BlockedReasons...)
	return HashStrings(parts...)
}

func isDeploymentEvidenceKind(kind string) bool {
	for _, candidate := range DeploymentEvidenceKinds {
		if kind == candidate {
			return true
		}
	}
	return false
}

func isDeploymentRecoveryState(state string) bool {
	return state == DeploymentEvidenceResolved || state == DeploymentEvidenceRecoveryRequired || state == DeploymentEvidenceUnknown || state == DeploymentEvidenceNotApplicable
}
