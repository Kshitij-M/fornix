package contracts

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const ReleaseAdmissionSchemaVersion = 1

const (
	DeploymentVerificationVerified = "verified"
	DeploymentVerificationFailed   = "failed"
	DeploymentVerificationRevoked  = "revoked"
	DeploymentVerificationExpired  = "expired"

	DeploymentArtifactRelease  = "release"
	DeploymentArtifactImage    = "image"
	DeploymentArtifactBinary   = "binary"
	DeploymentArtifactManifest = "manifest"

	MaxReleaseVerificationKinds = 4
	MaxReleaseSourceLength      = 256
)

var DeploymentArtifactKinds = []string{
	DeploymentArtifactRelease, DeploymentArtifactImage,
	DeploymentArtifactBinary, DeploymentArtifactManifest,
}

// DeploymentAdmissionReference is the hash-only proof a generic effectful
// operation carries when it must be admitted against a qualified deployment
// release. It is a reference to the deployment-evidence authority, not an
// execution token: the operation store re-evaluates the referenced facts in
// the same Postgres transaction that reserves the external effect.
//
// Keeping release, artifact, gate, trust-snapshot, and decision hashes in the
// request makes the operation identity sensitive to every authority fact while
// keeping raw manifests, attestations, credentials, and provider payloads out
// of operation history.
type DeploymentAdmissionReference struct {
	SchemaVersion             int        `json:"schema_version"`
	WorkspaceID               string     `json:"workspace_id"`
	DeploymentID              string     `json:"deployment_id"`
	ReleaseID                 string     `json:"release_id"`
	ReleaseHash               string     `json:"release_hash"`
	ArtifactKind              string     `json:"artifact_kind"`
	ArtifactHash              string     `json:"artifact_hash"`
	TrustSnapshotRevision     int64      `json:"trust_snapshot_revision"`
	TrustSnapshotHash         string     `json:"trust_snapshot_hash"`
	GateHash                  string     `json:"gate_hash"`
	DecisionHash              string     `json:"decision_hash"`
	ExternalBoundaryHash      string     `json:"external_boundary_hash,omitempty"`
	BoundaryEvidenceHash      string     `json:"boundary_evidence_hash,omitempty"`
	BoundaryEvidenceExpiresAt *time.Time `json:"boundary_evidence_expires_at,omitempty"`
}

// Normalize validates and canonicalizes an operation-bound admission
// reference. No caller-controlled prose or secret material is accepted.
func (r *DeploymentAdmissionReference) Normalize() error {
	if r == nil {
		return fmt.Errorf("deployment admission reference is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ReleaseAdmissionSchemaVersion
	}
	if r.SchemaVersion != ReleaseAdmissionSchemaVersion {
		return fmt.Errorf("unsupported deployment admission reference schema_version %d", r.SchemaVersion)
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "deployment admission reference deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.ReleaseID, err = normalizeDomainIdentifier(r.ReleaseID, "deployment admission reference release_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.ReleaseHash, err = normalizeDomainHash(r.ReleaseHash, "deployment admission reference release_hash", true); err != nil {
		return err
	}
	if r.ArtifactKind, err = normalizeDomainName(r.ArtifactKind, "deployment admission reference artifact_kind", MaxDomainNameLength, true); err != nil || !isDeploymentArtifactKind(r.ArtifactKind) {
		return fmt.Errorf("unsupported deployment admission reference artifact kind %q", r.ArtifactKind)
	}
	for field, value := range map[string]*string{
		"artifact_hash": &r.ArtifactHash, "trust_snapshot_hash": &r.TrustSnapshotHash, "gate_hash": &r.GateHash, "decision_hash": &r.DecisionHash,
	} {
		if *value, err = normalizeDomainHash(*value, "deployment admission reference "+field, true); err != nil {
			return err
		}
	}
	for field, value := range map[string]*string{
		"external_boundary_hash": &r.ExternalBoundaryHash, "boundary_evidence_hash": &r.BoundaryEvidenceHash,
	} {
		if *value, err = normalizeDomainHash(*value, "deployment admission reference "+field, false); err != nil {
			return err
		}
	}
	if r.BoundaryEvidenceExpiresAt != nil {
		value := r.BoundaryEvidenceExpiresAt.UTC()
		r.BoundaryEvidenceExpiresAt = &value
		if r.ExternalBoundaryHash == "" {
			return fmt.Errorf("deployment admission boundary expiry requires external boundary hash")
		}
	}
	if (r.ExternalBoundaryHash == "") != (r.BoundaryEvidenceHash == "") {
		return fmt.Errorf("deployment admission boundary hashes must be supplied together")
	}
	if r.ExternalBoundaryHash != "" && r.BoundaryEvidenceExpiresAt == nil {
		return fmt.Errorf("deployment admission external boundary hash requires expiry")
	}
	if r.TrustSnapshotRevision < 1 {
		return fmt.Errorf("deployment admission reference trust snapshot revision must be positive")
	}
	return nil
}

// StableHash returns the canonical identity of the admission reference.
func (r DeploymentAdmissionReference) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	boundaryExpiry := ""
	if r.BoundaryEvidenceExpiresAt != nil {
		boundaryExpiry = r.BoundaryEvidenceExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	return HashStrings("deployment-admission-reference", r.WorkspaceID, r.DeploymentID, r.ReleaseID, r.ReleaseHash, r.ArtifactKind, r.ArtifactHash, fmt.Sprint(r.TrustSnapshotRevision), r.TrustSnapshotHash, r.GateHash, r.DecisionHash, r.ExternalBoundaryHash, r.BoundaryEvidenceHash, boundaryExpiry)
}

// DeploymentReleaseVerification is a hash-only deployment-owned assertion
// that one release artifact was verified against one exact Fornix gate.
// Fornix stores the assertion identity and provenance, not raw signatures,
// manifests, image layers, credentials, or registry responses.
type DeploymentReleaseVerification struct {
	SchemaVersion             int        `json:"schema_version"`
	ID                        string     `json:"id"`
	WorkspaceID               string     `json:"workspace_id"`
	DeploymentID              string     `json:"deployment_id"`
	ReleaseID                 string     `json:"release_id"`
	ReleaseHash               string     `json:"release_hash"`
	TargetHash                string     `json:"target_hash"`
	ArtifactKind              string     `json:"artifact_kind"`
	ArtifactHash              string     `json:"artifact_hash"`
	AttestationHash           string     `json:"attestation_hash"`
	GateHash                  string     `json:"gate_hash"`
	ExternalBoundaryHash      string     `json:"external_boundary_hash,omitempty"`
	BoundaryEvidenceHash      string     `json:"boundary_evidence_hash,omitempty"`
	BoundaryEvidenceExpiresAt *time.Time `json:"boundary_evidence_expires_at,omitempty"`
	TrustSnapshotRevision     int64      `json:"trust_snapshot_revision"`
	TrustSnapshotHash         string     `json:"trust_snapshot_hash"`
	SourceReference           string     `json:"source_reference,omitempty"`
	Status                    string     `json:"status"`
	Actor                     AuditActor `json:"actor"`
	CreatedAt                 time.Time  `json:"created_at"`
	ExpiresAt                 time.Time  `json:"expires_at"`
	RevokedAt                 *time.Time `json:"revoked_at,omitempty"`
}

func (v *DeploymentReleaseVerification) Normalize() error {
	if v == nil {
		return fmt.Errorf("deployment release verification is nil")
	}
	if v.SchemaVersion == 0 {
		v.SchemaVersion = ReleaseAdmissionSchemaVersion
	}
	if v.SchemaVersion != ReleaseAdmissionSchemaVersion {
		return fmt.Errorf("unsupported deployment release verification schema_version %d", v.SchemaVersion)
	}
	var err error
	for field, value := range map[string]*string{
		"id": &v.ID, "release_id": &v.ReleaseID,
	} {
		if *value, err = normalizeDomainIdentifier(*value, "release verification "+field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	if v.WorkspaceID, err = normalizeDomainWorkspace(v.WorkspaceID); err != nil {
		return err
	}
	if v.DeploymentID, err = normalizeDomainIdentifier(v.DeploymentID, "release verification deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if v.ReleaseHash, err = normalizeDomainHash(v.ReleaseHash, "release verification release_hash", true); err != nil {
		return err
	}
	if v.TargetHash, err = normalizeDomainHash(v.TargetHash, "release verification target_hash", true); err != nil {
		return err
	}
	if v.ArtifactKind, err = normalizeDomainName(v.ArtifactKind, "release verification artifact_kind", MaxDomainNameLength, true); err != nil || !isDeploymentArtifactKind(v.ArtifactKind) {
		return fmt.Errorf("unsupported deployment artifact kind %q", v.ArtifactKind)
	}
	for field, value := range map[string]*string{
		"artifact_hash": &v.ArtifactHash, "attestation_hash": &v.AttestationHash, "gate_hash": &v.GateHash, "trust_snapshot_hash": &v.TrustSnapshotHash,
	} {
		if *value, err = normalizeDomainHash(*value, "release verification "+field, true); err != nil {
			return err
		}
	}
	for field, value := range map[string]*string{
		"external_boundary_hash": &v.ExternalBoundaryHash, "boundary_evidence_hash": &v.BoundaryEvidenceHash,
	} {
		if *value, err = normalizeDomainHash(*value, "release verification "+field, false); err != nil {
			return err
		}
	}
	if v.BoundaryEvidenceExpiresAt != nil {
		value := v.BoundaryEvidenceExpiresAt.UTC()
		v.BoundaryEvidenceExpiresAt = &value
		if v.ExternalBoundaryHash == "" {
			return fmt.Errorf("release verification boundary expiry requires external boundary hash")
		}
	}
	if (v.ExternalBoundaryHash == "") != (v.BoundaryEvidenceHash == "") {
		return fmt.Errorf("release verification boundary hashes must be supplied together")
	}
	if v.ExternalBoundaryHash != "" && v.BoundaryEvidenceExpiresAt == nil {
		return fmt.Errorf("release verification external boundary hash requires expiry")
	}
	if v.TrustSnapshotRevision < 1 {
		return fmt.Errorf("release verification trust snapshot revision must be positive")
	}
	v.SourceReference = strings.TrimSpace(v.SourceReference)
	if len(v.SourceReference) > MaxReleaseSourceLength || strings.ContainsAny(v.SourceReference, "\x00\r\n") {
		return fmt.Errorf("release verification source_reference is invalid")
	}
	v.Status = strings.ToLower(strings.TrimSpace(v.Status))
	if v.Status == "" {
		v.Status = DeploymentVerificationVerified
	}
	if !isDeploymentVerificationStatus(v.Status) {
		return fmt.Errorf("unsupported release verification status %q", v.Status)
	}
	if err := normalizeQualificationAuditActor(&v.Actor, v.WorkspaceID); err != nil {
		return err
	}
	if v.CreatedAt.IsZero() || v.ExpiresAt.IsZero() || !v.ExpiresAt.After(v.CreatedAt) {
		return fmt.Errorf("release verification created_at and expires_at are invalid")
	}
	v.CreatedAt, v.ExpiresAt = v.CreatedAt.UTC(), v.ExpiresAt.UTC()
	if v.RevokedAt != nil {
		value := v.RevokedAt.UTC()
		v.RevokedAt = &value
	}
	if v.Status == DeploymentVerificationRevoked && v.RevokedAt == nil {
		return fmt.Errorf("revoked release verification requires revoked_at")
	}
	return nil
}

// DeploymentReleaseVerificationRequest registers one deployment-owned
// verification reference. The store supplies release facts and the current
// gate facts transactionally; caller-supplied copies are compared, not trusted.
type DeploymentReleaseVerificationRequest struct {
	WorkspaceID          string     `json:"workspace_id"`
	DeploymentID         string     `json:"deployment_id"`
	ReleaseID            string     `json:"release_id"`
	ReleaseHash          string     `json:"release_hash"`
	TargetHash           string     `json:"target_hash"`
	ArtifactKind         string     `json:"artifact_kind"`
	ArtifactHash         string     `json:"artifact_hash"`
	AttestationHash      string     `json:"attestation_hash"`
	GateHash             string     `json:"gate_hash"`
	ExternalBoundaryHash string     `json:"external_boundary_hash,omitempty"`
	BoundaryEvidenceHash string     `json:"boundary_evidence_hash,omitempty"`
	SourceReference      string     `json:"source_reference,omitempty"`
	ExpiresAt            time.Time  `json:"expires_at"`
	RequestID            string     `json:"request_id,omitempty"`
	IdempotencyKey       string     `json:"idempotency_key"`
	CausationID          string     `json:"causation_id,omitempty"`
	CorrelationID        string     `json:"correlation_id,omitempty"`
	Actor                AuditActor `json:"actor"`
	DryRun               bool       `json:"dry_run,omitempty"`
}

func (r *DeploymentReleaseVerificationRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("deployment release verification request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "release verification request deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.ReleaseID, err = normalizeDomainIdentifier(r.ReleaseID, "release verification request release_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"release_hash": &r.ReleaseHash, "target_hash": &r.TargetHash, "artifact_hash": &r.ArtifactHash, "attestation_hash": &r.AttestationHash, "gate_hash": &r.GateHash,
	} {
		if *value, err = normalizeDomainHash(*value, "release verification request "+field, true); err != nil {
			return err
		}
	}
	for field, value := range map[string]*string{
		"external_boundary_hash": &r.ExternalBoundaryHash, "boundary_evidence_hash": &r.BoundaryEvidenceHash,
	} {
		if *value, err = normalizeDomainHash(*value, "release verification request "+field, false); err != nil {
			return err
		}
	}
	if r.ArtifactKind, err = normalizeDomainName(r.ArtifactKind, "release verification request artifact_kind", MaxDomainNameLength, true); err != nil || !isDeploymentArtifactKind(r.ArtifactKind) {
		return fmt.Errorf("unsupported deployment artifact kind %q", r.ArtifactKind)
	}
	r.SourceReference = strings.TrimSpace(r.SourceReference)
	if len(r.SourceReference) > MaxReleaseSourceLength || strings.ContainsAny(r.SourceReference, "\x00\r\n") {
		return fmt.Errorf("release verification request source_reference is invalid")
	}
	if r.ExpiresAt.IsZero() {
		return fmt.Errorf("release verification request expires_at is required")
	}
	r.ExpiresAt = r.ExpiresAt.UTC()
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "release verification request request_id", MaxIdempotencyLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "release verification request idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "release verification request causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "release verification request correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	return normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID)
}

// DeploymentAdmissionDecision is the read-only, deterministic answer used by
// startup or a deployment adapter. It is not an execution authorization token.
type DeploymentAdmissionDecision struct {
	SchemaVersion             int                            `json:"schema_version"`
	WorkspaceID               string                         `json:"workspace_id"`
	DeploymentID              string                         `json:"deployment_id"`
	ReleaseID                 string                         `json:"release_id"`
	ReleaseHash               string                         `json:"release_hash"`
	ArtifactKind              string                         `json:"artifact_kind"`
	ArtifactHash              string                         `json:"artifact_hash"`
	TrustSnapshotRevision     int64                          `json:"trust_snapshot_revision"`
	TrustSnapshotHash         string                         `json:"trust_snapshot_hash"`
	GateHash                  string                         `json:"gate_hash"`
	ExternalBoundaryHash      string                         `json:"external_boundary_hash,omitempty"`
	BoundaryEvidenceHash      string                         `json:"boundary_evidence_hash,omitempty"`
	BoundaryEvidenceExpiresAt *time.Time                     `json:"boundary_evidence_expires_at,omitempty"`
	Verification              *DeploymentReleaseVerification `json:"verification,omitempty"`
	Ready                     bool                           `json:"ready"`
	BlockedReasons            []string                       `json:"blocked_reasons,omitempty"`
	DecisionHash              string                         `json:"decision_hash"`
}

func (d *DeploymentAdmissionDecision) Normalize() error {
	if d == nil {
		return fmt.Errorf("deployment admission decision is nil")
	}
	if d.SchemaVersion == 0 {
		d.SchemaVersion = ReleaseAdmissionSchemaVersion
	}
	if d.SchemaVersion != ReleaseAdmissionSchemaVersion {
		return fmt.Errorf("unsupported deployment admission schema_version %d", d.SchemaVersion)
	}
	var err error
	if d.WorkspaceID, err = normalizeDomainWorkspace(d.WorkspaceID); err != nil {
		return err
	}
	if d.DeploymentID, err = normalizeDomainIdentifier(d.DeploymentID, "deployment admission deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if d.ReleaseID, err = normalizeDomainIdentifier(d.ReleaseID, "deployment admission release_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"release_hash": &d.ReleaseHash, "trust_snapshot_hash": &d.TrustSnapshotHash, "gate_hash": &d.GateHash,
	} {
		if *value, err = normalizeDomainHash(*value, "deployment admission "+field, true); err != nil {
			return err
		}
	}
	if d.BoundaryEvidenceExpiresAt != nil {
		value := d.BoundaryEvidenceExpiresAt.UTC()
		d.BoundaryEvidenceExpiresAt = &value
		if d.ExternalBoundaryHash == "" {
			return fmt.Errorf("deployment admission boundary expiry requires external boundary hash")
		}
	}
	for field, value := range map[string]*string{
		"external_boundary_hash": &d.ExternalBoundaryHash, "boundary_evidence_hash": &d.BoundaryEvidenceHash,
	} {
		if *value, err = normalizeDomainHash(*value, "deployment admission "+field, false); err != nil {
			return err
		}
	}
	if (d.ExternalBoundaryHash == "") != (d.BoundaryEvidenceHash == "") {
		return fmt.Errorf("deployment admission boundary hashes must be supplied together")
	}
	if d.ExternalBoundaryHash != "" && d.BoundaryEvidenceExpiresAt == nil {
		return fmt.Errorf("deployment admission external boundary hash requires expiry")
	}
	if d.ArtifactHash, err = normalizeDomainHash(d.ArtifactHash, "deployment admission artifact_hash", false); err != nil {
		return err
	}
	if d.ArtifactKind, err = normalizeDomainName(d.ArtifactKind, "deployment admission artifact_kind", MaxDomainNameLength, true); err != nil || !isDeploymentArtifactKind(d.ArtifactKind) {
		return fmt.Errorf("unsupported deployment admission artifact kind %q", d.ArtifactKind)
	}
	if d.TrustSnapshotRevision < 1 {
		return fmt.Errorf("deployment admission trust snapshot revision must be positive")
	}
	if d.Ready && d.ArtifactHash == "" {
		return fmt.Errorf("ready deployment admission requires artifact_hash")
	}
	if d.Verification != nil {
		if err := d.Verification.Normalize(); err != nil {
			return err
		}
		if d.Verification.WorkspaceID != d.WorkspaceID || d.Verification.DeploymentID != d.DeploymentID || d.Verification.ReleaseID != d.ReleaseID || d.Verification.ReleaseHash != d.ReleaseHash || d.Verification.ArtifactKind != d.ArtifactKind || (d.ArtifactHash != "" && d.Verification.ArtifactHash != d.ArtifactHash) || d.Verification.GateHash != d.GateHash || d.Verification.ExternalBoundaryHash != d.ExternalBoundaryHash || d.Verification.BoundaryEvidenceHash != d.BoundaryEvidenceHash || !sameOptionalTime(d.Verification.BoundaryEvidenceExpiresAt, d.BoundaryEvidenceExpiresAt) || d.Verification.TrustSnapshotRevision != d.TrustSnapshotRevision || d.Verification.TrustSnapshotHash != d.TrustSnapshotHash {
			return fmt.Errorf("deployment admission verification crosses its release binding")
		}
	}
	if len(d.BlockedReasons) > MaxDeploymentEvidenceKinds+6 {
		return fmt.Errorf("deployment admission blocked reasons exceed bounds")
	}
	sort.Strings(d.BlockedReasons)
	return nil
}

func (d DeploymentAdmissionDecision) StableHash() string {
	if err := d.Normalize(); err != nil {
		return ""
	}
	boundaryExpiry := ""
	if d.BoundaryEvidenceExpiresAt != nil {
		boundaryExpiry = d.BoundaryEvidenceExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	parts := []string{"deployment-admission", d.WorkspaceID, d.DeploymentID, d.ReleaseID, d.ReleaseHash, d.ArtifactKind, d.ArtifactHash, fmt.Sprint(d.TrustSnapshotRevision), d.TrustSnapshotHash, d.GateHash, d.ExternalBoundaryHash, d.BoundaryEvidenceHash, boundaryExpiry, fmt.Sprint(d.Ready)}
	if d.Verification != nil {
		parts = append(parts, d.Verification.ID, d.Verification.AttestationHash, d.Verification.Status, d.Verification.ExpiresAt.UTC().Format(time.RFC3339Nano))
	}
	parts = append(parts, d.BlockedReasons...)
	return HashStrings(parts...)
}

// Reference returns the exact hash-only operation reference for a ready
// decision. Callers should use this helper instead of reconstructing the
// authority fields by hand.
func (d DeploymentAdmissionDecision) Reference() (DeploymentAdmissionReference, error) {
	if err := d.Normalize(); err != nil {
		return DeploymentAdmissionReference{}, err
	}
	if !d.Ready {
		return DeploymentAdmissionReference{}, fmt.Errorf("deployment admission decision is not ready")
	}
	decisionHash := d.DecisionHash
	if decisionHash == "" {
		decisionHash = d.StableHash()
	}
	reference := DeploymentAdmissionReference{
		SchemaVersion: ReleaseAdmissionSchemaVersion, WorkspaceID: d.WorkspaceID,
		DeploymentID: d.DeploymentID, ReleaseID: d.ReleaseID, ReleaseHash: d.ReleaseHash,
		ArtifactKind: d.ArtifactKind, ArtifactHash: d.ArtifactHash,
		TrustSnapshotRevision: d.TrustSnapshotRevision, TrustSnapshotHash: d.TrustSnapshotHash,
		GateHash: d.GateHash, DecisionHash: decisionHash, ExternalBoundaryHash: d.ExternalBoundaryHash,
		BoundaryEvidenceHash: d.BoundaryEvidenceHash, BoundaryEvidenceExpiresAt: d.BoundaryEvidenceExpiresAt,
	}
	if err := reference.Normalize(); err != nil {
		return DeploymentAdmissionReference{}, err
	}
	return reference, nil
}

func isDeploymentArtifactKind(kind string) bool {
	for _, candidate := range DeploymentArtifactKinds {
		if kind == candidate {
			return true
		}
	}
	return false
}

func sameOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.UTC().Equal(right.UTC())
}

func isDeploymentVerificationStatus(status string) bool {
	return status == DeploymentVerificationVerified || status == DeploymentVerificationFailed || status == DeploymentVerificationRevoked || status == DeploymentVerificationExpired
}
