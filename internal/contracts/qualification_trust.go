package contracts

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// QualificationTrustSchemaVersion versions the deployment-owned signer and
// import records independently from the signed evidence envelope. The trust
// catalog is an authority for accepting evidence, not for generating it.
const QualificationTrustSchemaVersion = 1

const (
	QualificationSignerActive     = "active"
	QualificationSignerRevoked    = "revoked"
	QualificationSignerSuperseded = "superseded"
	QualificationImportAccepted   = "accepted"
)

const (
	MaxQualificationDeploymentIDLength = MaxDomainIDLength
	MaxQualificationSourceRefLength    = MaxDomainIDLength
	MaxQualificationPageSize           = 100
)

// QualificationTrustedSigner is public verification metadata for one
// workspace/deployment signer. PublicKey is safe to retain; private key
// material is deliberately absent. Status changes are mirrored by the
// append-only signer event history in PostgreSQL.
type QualificationTrustedSigner struct {
	SchemaVersion   int        `json:"schema_version"`
	ID              string     `json:"id"`
	WorkspaceID     string     `json:"workspace_id"`
	DeploymentID    string     `json:"deployment_id"`
	KeyID           string     `json:"key_id"`
	Algorithm       string     `json:"algorithm"`
	PublicKey       string     `json:"public_key"`
	PublicKeyHash   string     `json:"public_key_hash"`
	Status          string     `json:"status"`
	SupersedesKeyID string     `json:"supersedes_key_id,omitempty"`
	SupersededByKey string     `json:"superseded_by_key_id,omitempty"`
	ValidFrom       time.Time  `json:"valid_from"`
	ValidUntil      time.Time  `json:"valid_until"`
	RevokedAt       *time.Time `json:"revoked_at,omitempty"`
	SupersededAt    *time.Time `json:"superseded_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	Actor           AuditActor `json:"actor"`
}

// Normalize validates public signer metadata and derives its stable public
// key fingerprint. It never accepts or emits private key bytes.
func (s *QualificationTrustedSigner) Normalize() error {
	if s == nil {
		return fmt.Errorf("qualification trusted signer is nil")
	}
	if s.SchemaVersion == 0 {
		s.SchemaVersion = QualificationTrustSchemaVersion
	}
	if s.SchemaVersion != QualificationTrustSchemaVersion {
		return fmt.Errorf("unsupported qualification trust schema_version %d", s.SchemaVersion)
	}
	var err error
	if s.ID, err = normalizeDomainIdentifier(s.ID, "qualification signer id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if s.WorkspaceID, err = normalizeDomainWorkspace(s.WorkspaceID); err != nil {
		return err
	}
	if s.DeploymentID, err = normalizeDomainIdentifier(s.DeploymentID, "qualification signer deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if s.KeyID, err = normalizeDomainIdentifier(s.KeyID, "qualification signer key_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	s.Algorithm = strings.ToLower(strings.TrimSpace(s.Algorithm))
	if s.Algorithm == "" {
		s.Algorithm = QualificationSignatureAlgorithmEd25519
	}
	if s.Algorithm != QualificationSignatureAlgorithmEd25519 {
		return fmt.Errorf("unsupported qualification signer algorithm")
	}
	if s.PublicKey, err = normalizeQualificationHex(s.PublicKey, "qualification signer public_key", ed25519.PublicKeySize*2); err != nil {
		return err
	}
	if s.PublicKeyHash == "" {
		s.PublicKeyHash = HashStrings("qualification-signer-public-key", s.PublicKey)
	} else if s.PublicKeyHash, err = normalizeDomainHash(s.PublicKeyHash, "qualification signer public_key_hash", true); err != nil {
		return err
	}
	if s.PublicKeyHash != HashStrings("qualification-signer-public-key", s.PublicKey) {
		return fmt.Errorf("qualification signer public_key_hash does not match public key")
	}
	if s.SupersedesKeyID, err = normalizeDomainIdentifier(s.SupersedesKeyID, "qualification signer supersedes_key_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if s.SupersededByKey, err = normalizeDomainIdentifier(s.SupersededByKey, "qualification signer superseded_by_key_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	s.Status = strings.ToLower(strings.TrimSpace(s.Status))
	if s.Status != QualificationSignerActive && s.Status != QualificationSignerRevoked && s.Status != QualificationSignerSuperseded {
		return fmt.Errorf("invalid qualification signer status %q", s.Status)
	}
	if s.ValidFrom.IsZero() || s.ValidUntil.IsZero() || !s.ValidUntil.After(s.ValidFrom) {
		return fmt.Errorf("qualification signer validity window is invalid")
	}
	s.ValidFrom, s.ValidUntil = s.ValidFrom.UTC(), s.ValidUntil.UTC()
	if s.RevokedAt != nil {
		value := s.RevokedAt.UTC()
		s.RevokedAt = &value
	}
	if s.SupersededAt != nil {
		value := s.SupersededAt.UTC()
		s.SupersededAt = &value
	}
	if !s.CreatedAt.IsZero() {
		s.CreatedAt = s.CreatedAt.UTC()
	}
	if err := normalizeQualificationAuditActor(&s.Actor, s.WorkspaceID); err != nil {
		return err
	}
	return nil
}

// QualificationTrustedSignerInput is the public-key-only registration input
// for a deployment-owned signer. The caller must hold the matching private
// key outside Fornix; this value never contains it.
type QualificationTrustedSignerInput struct {
	WorkspaceID     string
	DeploymentID    string
	KeyID           string
	PublicKey       string
	ValidFrom       time.Time
	ValidUntil      time.Time
	SupersedesKeyID string
	Actor           AuditActor
}

// QualificationImportRequest is a bounded, workspace-scoped request to
// accept one signed qualification bundle. SignedBytes preserves the exact
// submitted JSON value and is never serialized in API responses.
type QualificationImportRequest struct {
	WorkspaceID     string                    `json:"workspace_id"`
	DeploymentID    string                    `json:"deployment_id"`
	TargetHash      string                    `json:"target_hash"`
	SourceReference string                    `json:"source_reference,omitempty"`
	RequestID       string                    `json:"request_id,omitempty"`
	IdempotencyKey  string                    `json:"idempotency_key"`
	CausationID     string                    `json:"causation_id,omitempty"`
	CorrelationID   string                    `json:"correlation_id,omitempty"`
	SignedBundle    SignedQualificationBundle `json:"signed_bundle"`
	SignedBytes     []byte                    `json:"-"`
	Actor           AuditActor                `json:"actor"`
	DryRun          bool                      `json:"dry_run,omitempty"`
}

// Normalize validates import scope and bounded identifiers. Signature and
// trust-catalog checks happen in the durable store after the signer is loaded.
func (r *QualificationImportRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification import request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification import deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.TargetHash, err = normalizeDomainHash(r.TargetHash, "qualification import target_hash", true); err != nil {
		return err
	}
	if r.SourceReference, err = normalizeDomainIdentifier(r.SourceReference, "qualification import source_reference", MaxQualificationSourceRefLength, false); err != nil {
		return err
	}
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "qualification import request_id", MaxIdempotencyLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "qualification import idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "qualification import causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "qualification import correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if err := r.SignedBundle.Normalize(); err != nil {
		return err
	}
	if r.SignedBundle.Bundle.Report.WorkspaceID != r.WorkspaceID || r.SignedBundle.Bundle.Report.TargetHash != r.TargetHash {
		return fmt.Errorf("qualification import crosses workspace or target scope")
	}
	if len(r.SignedBytes) > MaxQualificationReportBytes {
		return fmt.Errorf("qualification signed bytes exceed %d bytes", MaxQualificationReportBytes)
	}
	if err := normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID); err != nil {
		return err
	}
	return nil
}

// QualificationImportRecord is the durable, hash-addressed result of an
// authorized import. Raw signed bytes are disclosed only through an explicit
// bounded disclosure operation, not by ordinary list responses.
type QualificationImportRecord struct {
	SchemaVersion         int        `json:"schema_version"`
	ID                    string     `json:"id"`
	WorkspaceID           string     `json:"workspace_id"`
	DeploymentID          string     `json:"deployment_id"`
	TargetHash            string     `json:"target_hash"`
	KeyID                 string     `json:"key_id"`
	SignerRecordID        string     `json:"signer_record_id"`
	SignedHash            string     `json:"signed_hash"`
	ObservationHash       string     `json:"observation_hash"`
	SourceHash            string     `json:"source_hash"`
	SourceReference       string     `json:"source_reference,omitempty"`
	RequestID             string     `json:"request_id,omitempty"`
	IdempotencyKey        string     `json:"idempotency_key"`
	CausationID           string     `json:"causation_id,omitempty"`
	CorrelationID         string     `json:"correlation_id,omitempty"`
	TrustSnapshotRevision int64      `json:"trust_snapshot_revision,omitempty"`
	TrustSnapshotHash     string     `json:"trust_snapshot_hash,omitempty"`
	Status                string     `json:"status"`
	Actor                 AuditActor `json:"actor"`
	CreatedAt             time.Time  `json:"created_at"`
}

// Normalize validates a durable import record without requiring access to
// the raw signed envelope. The store verifies that envelope before insertion.
func (r *QualificationImportRecord) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification import record is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = QualificationTrustSchemaVersion
	}
	if r.SchemaVersion != QualificationTrustSchemaVersion {
		return fmt.Errorf("unsupported qualification import schema_version %d", r.SchemaVersion)
	}
	var err error
	if r.ID, err = normalizeDomainIdentifier(r.ID, "qualification import id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification import deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.TargetHash, err = normalizeDomainHash(r.TargetHash, "qualification import target_hash", true); err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"signed_hash": &r.SignedHash, "observation_hash": &r.ObservationHash, "source_hash": &r.SourceHash,
	} {
		if *value, err = normalizeDomainHash(*value, "qualification import "+field, true); err != nil {
			return err
		}
	}
	if r.KeyID, err = normalizeDomainIdentifier(r.KeyID, "qualification import key_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.SignerRecordID, err = normalizeDomainIdentifier(r.SignerRecordID, "qualification import signer_record_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.SourceReference, err = normalizeDomainIdentifier(r.SourceReference, "qualification import source_reference", MaxQualificationSourceRefLength, false); err != nil {
		return err
	}
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "qualification import request_id", MaxIdempotencyLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "qualification import idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "qualification import causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "qualification import correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.TrustSnapshotRevision < 0 {
		return fmt.Errorf("qualification import trust snapshot revision is invalid")
	}
	if r.TrustSnapshotRevision == 0 && strings.TrimSpace(r.TrustSnapshotHash) != "" {
		return fmt.Errorf("qualification import trust snapshot hash requires a revision")
	}
	if r.TrustSnapshotRevision > 0 {
		if r.TrustSnapshotHash, err = normalizeDomainHash(r.TrustSnapshotHash, "qualification import trust_snapshot_hash", true); err != nil {
			return err
		}
	}
	r.Status = strings.ToLower(strings.TrimSpace(r.Status))
	if r.Status != QualificationImportAccepted {
		return fmt.Errorf("invalid qualification import status %q", r.Status)
	}
	if err := normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID); err != nil {
		return err
	}
	if !r.CreatedAt.IsZero() {
		r.CreatedAt = r.CreatedAt.UTC()
	}
	return nil
}

// QualificationImportDisclosure is returned only by an explicit authorized
// disclosure request and keeps the exact source bytes available to a caller
// that needs independent verification.
type QualificationImportDisclosure struct {
	Record       QualificationImportRecord `json:"record"`
	SignedBundle SignedQualificationBundle `json:"signed_bundle"`
	SourceBytes  []byte                    `json:"-"`
}

// QualificationSignerPage is a bounded deterministic page of signer metadata.
type QualificationSignerPage struct {
	Items      []QualificationTrustedSigner `json:"items"`
	NextCursor string                       `json:"next_cursor,omitempty"`
}

// QualificationImportPage is a bounded deterministic page of import records.
type QualificationImportPage struct {
	Items      []QualificationImportRecord `json:"items"`
	NextCursor string                      `json:"next_cursor,omitempty"`
}

// QualificationImportResult reports validation and durable-effect semantics
// without exposing submitted signed bytes. Created is true only for the
// first committed import; Deduplicated identifies an exact replay.
type QualificationImportResult struct {
	Record       QualificationImportRecord `json:"record"`
	Validated    bool                      `json:"validated"`
	Created      bool                      `json:"created"`
	Deduplicated bool                      `json:"deduplicated"`
	DryRun       bool                      `json:"dry_run"`
}

func qualificationPublicKeyHash(publicKey string) string {
	return HashStrings("qualification-signer-public-key", strings.ToLower(strings.TrimSpace(publicKey)))
}

func normalizeQualificationAuditActor(actor *AuditActor, workspaceID string) error {
	if actor == nil {
		return fmt.Errorf("qualification audit actor is required")
	}
	var err error
	if actor.ID, err = normalizeDomainIdentifier(actor.ID, "qualification audit actor id", MaxDomainIDLength, true); err != nil {
		return err
	}
	actor.WorkspaceID, err = normalizeDomainWorkspace(actor.WorkspaceID)
	if err != nil {
		return err
	}
	if actor.WorkspaceID != workspaceID {
		return fmt.Errorf("qualification audit actor crosses workspace boundary")
	}
	if actor.Kind, err = normalizeDomainName(actor.Kind, "qualification audit actor kind", MaxDomainNameLength, false); err != nil {
		return err
	}
	if actor.APIKeyID, err = normalizeDomainIdentifier(actor.APIKeyID, "qualification audit actor api_key_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	return nil
}

func qualificationPublicKeyBytes(publicKey string) (ed25519.PublicKey, error) {
	decoded, err := hex.DecodeString(strings.TrimSpace(publicKey))
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("qualification public key is invalid")
	}
	return ed25519.PublicKey(decoded), nil
}
