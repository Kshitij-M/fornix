package contracts

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	QualificationTrustSnapshotSchemaVersion = 1
	QualificationSnapshotActive             = "active"
	QualificationSnapshotRevoked            = "revoked"
	QualificationSnapshotSignerActive       = "active"
	QualificationSnapshotSignerRevoked      = "revoked"
	MaxQualificationTrustSnapshotEntries    = 64
	MaxQualificationTrustSnapshotBytes      = 128 << 10
)

// QualificationTrustSnapshotEntry is public signer material distributed by
// one authorized deployment snapshot. It contains no private key or secret.
type QualificationTrustSnapshotEntry struct {
	DeploymentID  string    `json:"deployment_id"`
	KeyID         string    `json:"key_id"`
	Algorithm     string    `json:"algorithm"`
	PublicKey     string    `json:"public_key"`
	PublicKeyHash string    `json:"public_key_hash"`
	Status        string    `json:"status"`
	ValidFrom     time.Time `json:"valid_from"`
	ValidUntil    time.Time `json:"valid_until"`
}

func (e *QualificationTrustSnapshotEntry) Normalize() error {
	if e == nil {
		return fmt.Errorf("qualification trust snapshot entry is nil")
	}
	var err error
	if e.DeploymentID, err = normalizeDomainIdentifier(e.DeploymentID, "qualification snapshot entry deployment_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if e.KeyID, err = normalizeDomainIdentifier(e.KeyID, "qualification snapshot entry key_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	e.Algorithm = strings.ToLower(strings.TrimSpace(e.Algorithm))
	if e.Algorithm == "" {
		e.Algorithm = QualificationSignatureAlgorithmEd25519
	}
	if e.Algorithm != QualificationSignatureAlgorithmEd25519 {
		return fmt.Errorf("unsupported qualification snapshot entry algorithm")
	}
	if e.PublicKey, err = normalizeQualificationHex(e.PublicKey, "qualification snapshot entry public_key", ed25519.PublicKeySize*2); err != nil {
		return err
	}
	expectedHash := HashStrings("qualification-signer-public-key", e.PublicKey)
	if e.PublicKeyHash == "" {
		e.PublicKeyHash = expectedHash
	} else if e.PublicKeyHash, err = normalizeDomainHash(e.PublicKeyHash, "qualification snapshot entry public_key_hash", true); err != nil {
		return err
	}
	if e.PublicKeyHash != expectedHash {
		return fmt.Errorf("qualification snapshot entry public_key_hash does not match public key")
	}
	e.Status = strings.ToLower(strings.TrimSpace(e.Status))
	if e.Status == "" {
		e.Status = QualificationSnapshotSignerActive
	}
	if e.Status != QualificationSnapshotSignerActive && e.Status != QualificationSnapshotSignerRevoked {
		return fmt.Errorf("invalid qualification snapshot signer status %q", e.Status)
	}
	if e.ValidFrom.IsZero() || e.ValidUntil.IsZero() || !e.ValidUntil.After(e.ValidFrom) {
		return fmt.Errorf("qualification snapshot signer validity window is invalid")
	}
	e.ValidFrom, e.ValidUntil = e.ValidFrom.UTC(), e.ValidUntil.UTC()
	return nil
}

// QualificationTrustSnapshot is the signed, revisioned deployment trust set.
// The embedded public key is integrity material only; the durable publisher
// catalog must authorize SignerKeyID before this snapshot is accepted.
type QualificationTrustSnapshot struct {
	SchemaVersion   int                               `json:"schema_version"`
	WorkspaceID     string                            `json:"workspace_id"`
	DeploymentID    string                            `json:"deployment_id"`
	Revision        int64                             `json:"revision"`
	SnapshotHash    string                            `json:"snapshot_hash"`
	SignerKeyID     string                            `json:"signer_key_id"`
	SignerAlgorithm string                            `json:"signer_algorithm"`
	SignerPublicKey string                            `json:"signer_public_key"`
	SignatureScheme string                            `json:"signature_scheme"`
	Signature       string                            `json:"signature"`
	IssuedAt        time.Time                         `json:"issued_at"`
	ExpiresAt       time.Time                         `json:"expires_at"`
	Status          string                            `json:"status"`
	Entries         []QualificationTrustSnapshotEntry `json:"entries"`
}

func (s *QualificationTrustSnapshot) Normalize() error {
	if s == nil {
		return fmt.Errorf("qualification trust snapshot is nil")
	}
	if s.SchemaVersion == 0 {
		s.SchemaVersion = QualificationTrustSnapshotSchemaVersion
	}
	if s.SchemaVersion != QualificationTrustSnapshotSchemaVersion {
		return fmt.Errorf("unsupported qualification trust snapshot schema_version %d", s.SchemaVersion)
	}
	var err error
	if s.WorkspaceID, err = normalizeDomainWorkspace(s.WorkspaceID); err != nil {
		return err
	}
	if s.DeploymentID, err = normalizeDomainIdentifier(s.DeploymentID, "qualification snapshot deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if s.Revision < 1 {
		return fmt.Errorf("qualification snapshot revision must be positive")
	}
	if s.SignerKeyID, err = normalizeDomainIdentifier(s.SignerKeyID, "qualification snapshot signer_key_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	s.SignerAlgorithm = strings.ToLower(strings.TrimSpace(s.SignerAlgorithm))
	if s.SignerAlgorithm == "" {
		s.SignerAlgorithm = QualificationSignatureAlgorithmEd25519
	}
	if s.SignerAlgorithm != QualificationSignatureAlgorithmEd25519 {
		return fmt.Errorf("unsupported qualification snapshot signer algorithm")
	}
	if s.SignerPublicKey, err = normalizeQualificationHex(s.SignerPublicKey, "qualification snapshot signer public_key", ed25519.PublicKeySize*2); err != nil {
		return err
	}
	s.SignatureScheme = strings.ToLower(strings.TrimSpace(s.SignatureScheme))
	if s.SignatureScheme == "" {
		s.SignatureScheme = QualificationSignatureAlgorithmEd25519
	}
	if s.SignatureScheme != QualificationSignatureAlgorithmEd25519 {
		return fmt.Errorf("unsupported qualification snapshot signature scheme")
	}
	if s.IssuedAt.IsZero() || s.ExpiresAt.IsZero() || !s.ExpiresAt.After(s.IssuedAt) {
		return fmt.Errorf("qualification snapshot validity window is invalid")
	}
	s.IssuedAt, s.ExpiresAt = s.IssuedAt.UTC(), s.ExpiresAt.UTC()
	s.Status = strings.ToLower(strings.TrimSpace(s.Status))
	if s.Status == "" {
		s.Status = QualificationSnapshotActive
	}
	if s.Status != QualificationSnapshotActive && s.Status != QualificationSnapshotRevoked {
		return fmt.Errorf("invalid qualification snapshot status %q", s.Status)
	}
	if len(s.Entries) == 0 || len(s.Entries) > MaxQualificationTrustSnapshotEntries {
		return fmt.Errorf("qualification snapshot entries must contain between 1 and %d entries", MaxQualificationTrustSnapshotEntries)
	}
	for i := range s.Entries {
		if err := s.Entries[i].Normalize(); err != nil {
			return fmt.Errorf("qualification snapshot entry %d: %w", i, err)
		}
		if s.Entries[i].DeploymentID != s.DeploymentID {
			return fmt.Errorf("qualification snapshot entry crosses deployment scope")
		}
	}
	sort.Slice(s.Entries, func(i, j int) bool {
		if s.Entries[i].KeyID != s.Entries[j].KeyID {
			return s.Entries[i].KeyID < s.Entries[j].KeyID
		}
		return s.Entries[i].PublicKeyHash < s.Entries[j].PublicKeyHash
	})
	for i := 1; i < len(s.Entries); i++ {
		if s.Entries[i-1].KeyID == s.Entries[i].KeyID {
			return fmt.Errorf("qualification snapshot contains duplicate key_id")
		}
	}
	expectedHash := s.computeHash()
	if s.SnapshotHash == "" {
		s.SnapshotHash = expectedHash
	} else if s.SnapshotHash, err = normalizeDomainHash(s.SnapshotHash, "qualification snapshot snapshot_hash", true); err != nil {
		return err
	} else if s.SnapshotHash != expectedHash {
		return fmt.Errorf("qualification snapshot hash does not match content")
	}
	if s.Signature != "" {
		s.Signature, err = normalizeQualificationHex(s.Signature, "qualification snapshot signature", ed25519.SignatureSize*2)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s QualificationTrustSnapshot) computeHash() string {
	parts := []string{"qualification-trust-snapshot", strconv.Itoa(s.SchemaVersion), s.WorkspaceID, s.DeploymentID, strconv.FormatInt(s.Revision, 10), s.SignerKeyID, s.SignerAlgorithm, s.SignerPublicKey, s.IssuedAt.UTC().Format(time.RFC3339Nano), s.ExpiresAt.UTC().Format(time.RFC3339Nano)}
	for _, entry := range s.Entries {
		parts = append(parts, entry.DeploymentID, entry.KeyID, entry.Algorithm, entry.PublicKey, entry.PublicKeyHash, entry.Status, entry.ValidFrom.UTC().Format(time.RFC3339Nano), entry.ValidUntil.UTC().Format(time.RFC3339Nano))
	}
	return HashStrings(parts...)
}

func (s QualificationTrustSnapshot) SigningSubjectHash() (string, error) {
	if err := s.Normalize(); err != nil {
		return "", err
	}
	return HashStrings("qualification-trust-snapshot-signing-v1", strconv.Itoa(s.SchemaVersion), s.WorkspaceID, s.DeploymentID, strconv.FormatInt(s.Revision, 10), s.SnapshotHash, s.SignerKeyID, s.IssuedAt.UTC().Format(time.RFC3339Nano), s.ExpiresAt.UTC().Format(time.RFC3339Nano)), nil
}

// SignQualificationTrustSnapshot signs only the normalized snapshot subject.
// The private key is never part of the snapshot contract or durable record.
func SignQualificationTrustSnapshot(snapshot QualificationTrustSnapshot, keyID string, privateKey ed25519.PrivateKey) (QualificationTrustSnapshot, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return QualificationTrustSnapshot{}, fmt.Errorf("qualification snapshot private key has invalid size")
	}
	var err error
	if snapshot.SignerKeyID, err = normalizeDomainIdentifier(keyID, "qualification snapshot signer_key_id", MaxDomainIDLength, true); err != nil {
		return QualificationTrustSnapshot{}, err
	}
	snapshot.SignerPublicKey = hex.EncodeToString(privateKey.Public().(ed25519.PublicKey))
	snapshot.SignerAlgorithm = QualificationSignatureAlgorithmEd25519
	snapshot.SignatureScheme = QualificationSignatureAlgorithmEd25519
	if err := snapshot.Normalize(); err != nil {
		return QualificationTrustSnapshot{}, err
	}
	subject, err := snapshot.SigningSubjectHash()
	if err != nil {
		return QualificationTrustSnapshot{}, err
	}
	snapshot.Signature = hex.EncodeToString(ed25519.Sign(privateKey, []byte(subject)))
	return snapshot, snapshot.Normalize()
}

// VerifyWithKey verifies integrity and then binds the signature to the
// deployment-selected publisher key. The embedded public key never authorizes
// the snapshot by itself.
func (s QualificationTrustSnapshot) VerifyWithKey(expectedKeyID string, expectedPublicKey ed25519.PublicKey, now time.Time) error {
	if err := s.Normalize(); err != nil {
		return err
	}
	if s.Status != QualificationSnapshotActive || now.UTC().Before(s.IssuedAt) || !now.UTC().Before(s.ExpiresAt) {
		return fmt.Errorf("qualification trust snapshot is expired or inactive")
	}
	if strings.TrimSpace(expectedKeyID) != "" && s.SignerKeyID != strings.TrimSpace(expectedKeyID) {
		return fmt.Errorf("qualification snapshot signer is not trusted")
	}
	if len(expectedPublicKey) > 0 && s.SignerPublicKey != hex.EncodeToString(expectedPublicKey) {
		return fmt.Errorf("qualification snapshot public key is not trusted")
	}
	publicKey, err := qualificationPublicKeyBytes(s.SignerPublicKey)
	if err != nil {
		return err
	}
	subject, err := s.SigningSubjectHash()
	if err != nil {
		return err
	}
	signature, err := hex.DecodeString(s.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(publicKey, []byte(subject), signature) {
		return fmt.Errorf("qualification snapshot signature verification failed")
	}
	return nil
}

// QualificationTrustSnapshotPublishRequest is the bounded durable publish
// input. SignedBytes retain the exact submitted envelope and are never
// serialized in normal API responses.
type QualificationTrustSnapshotPublishRequest struct {
	WorkspaceID     string                     `json:"workspace_id"`
	DeploymentID    string                     `json:"deployment_id"`
	SourceReference string                     `json:"source_reference,omitempty"`
	RequestID       string                     `json:"request_id,omitempty"`
	IdempotencyKey  string                     `json:"idempotency_key"`
	CausationID     string                     `json:"causation_id,omitempty"`
	CorrelationID   string                     `json:"correlation_id,omitempty"`
	SignedSnapshot  QualificationTrustSnapshot `json:"signed_snapshot"`
	SignedBytes     []byte                     `json:"-"`
	Actor           AuditActor                 `json:"actor"`
	DryRun          bool                       `json:"dry_run,omitempty"`
}

func (r *QualificationTrustSnapshotPublishRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification trust snapshot publish request is nil")
	}
	var err error
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return err
	}
	if r.DeploymentID, err = normalizeDomainIdentifier(r.DeploymentID, "qualification snapshot publish deployment_id", MaxQualificationDeploymentIDLength, true); err != nil {
		return err
	}
	if r.SourceReference, err = normalizeDomainIdentifier(r.SourceReference, "qualification snapshot publish source_reference", MaxQualificationSourceRefLength, false); err != nil {
		return err
	}
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "qualification snapshot publish request_id", MaxIdempotencyLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "qualification snapshot publish idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "qualification snapshot publish causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "qualification snapshot publish correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if err := r.SignedSnapshot.Normalize(); err != nil {
		return err
	}
	if r.SignedSnapshot.WorkspaceID != r.WorkspaceID || r.SignedSnapshot.DeploymentID != r.DeploymentID {
		return fmt.Errorf("qualification snapshot publish crosses workspace or deployment scope")
	}
	if len(r.SignedBytes) > MaxQualificationTrustSnapshotBytes {
		return fmt.Errorf("qualification trust snapshot exceeds %d bytes", MaxQualificationTrustSnapshotBytes)
	}
	if err := normalizeQualificationAuditActor(&r.Actor, r.WorkspaceID); err != nil {
		return err
	}
	return nil
}

// QualificationTrustSnapshotRecord is the durable published snapshot and its
// source/provenance metadata. Raw source bytes are disclosed only explicitly.
type QualificationTrustSnapshotRecord struct {
	ID              string                     `json:"id"`
	Snapshot        QualificationTrustSnapshot `json:"snapshot"`
	SourceHash      string                     `json:"source_hash"`
	SourceReference string                     `json:"source_reference,omitempty"`
	RequestID       string                     `json:"request_id,omitempty"`
	IdempotencyKey  string                     `json:"idempotency_key"`
	CausationID     string                     `json:"causation_id,omitempty"`
	CorrelationID   string                     `json:"correlation_id,omitempty"`
	Actor           AuditActor                 `json:"actor"`
	CreatedAt       time.Time                  `json:"created_at"`
}

func (r *QualificationTrustSnapshotRecord) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification trust snapshot record is nil")
	}
	var err error
	if r.ID, err = normalizeDomainIdentifier(r.ID, "qualification snapshot record id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if err := r.Snapshot.Normalize(); err != nil {
		return err
	}
	if r.SourceHash, err = normalizeDomainHash(r.SourceHash, "qualification snapshot source_hash", true); err != nil {
		return err
	}
	if r.SourceReference, err = normalizeDomainIdentifier(r.SourceReference, "qualification snapshot source_reference", MaxQualificationSourceRefLength, false); err != nil {
		return err
	}
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "qualification snapshot request_id", MaxIdempotencyLength, false); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "qualification snapshot idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.CausationID, err = normalizeDomainIdentifier(r.CausationID, "qualification snapshot causation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.CorrelationID, err = normalizeDomainIdentifier(r.CorrelationID, "qualification snapshot correlation_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if err := normalizeQualificationAuditActor(&r.Actor, r.Snapshot.WorkspaceID); err != nil {
		return err
	}
	if !r.CreatedAt.IsZero() {
		r.CreatedAt = r.CreatedAt.UTC()
	}
	return nil
}

type QualificationTrustSnapshotDisclosure struct {
	Record         QualificationTrustSnapshotRecord `json:"record"`
	SignedSnapshot QualificationTrustSnapshot       `json:"signed_snapshot"`
	SourceBytes    []byte                           `json:"-"`
}

type QualificationTrustSnapshotPage struct {
	Items      []QualificationTrustSnapshotRecord `json:"items"`
	NextCursor string                             `json:"next_cursor,omitempty"`
}

func (s QualificationTrustSnapshot) AuthorizesSigner(keyID, publicKey string, now time.Time) bool {
	if s.Status != QualificationSnapshotActive || now.UTC().Before(s.IssuedAt) || !now.UTC().Before(s.ExpiresAt) {
		return false
	}
	publicKey = strings.ToLower(strings.TrimSpace(publicKey))
	for _, entry := range s.Entries {
		if entry.KeyID == strings.TrimSpace(keyID) && entry.Status == QualificationSnapshotSignerActive && !now.UTC().Before(entry.ValidFrom) && now.UTC().Before(entry.ValidUntil) && entry.PublicKey == publicKey {
			return true
		}
	}
	return false
}
