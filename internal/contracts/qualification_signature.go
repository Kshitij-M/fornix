package contracts

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	QualificationSignedBundleSchemaVersion = 1
	QualificationSignatureAlgorithmEd25519 = "ed25519"
)

// QualificationSignature is public verification metadata for one signed
// evidence subject. PublicKey is safe to transport; private key material is
// deliberately absent from this contract.
type QualificationSignature struct {
	Algorithm  string `json:"algorithm"`
	KeyID      string `json:"key_id"`
	PublicKey  string `json:"public_key"`
	SignedHash string `json:"signed_hash"`
	Signature  string `json:"signature"`
}

func (s *QualificationSignature) normalize() error {
	if s == nil {
		return fmt.Errorf("qualification signature is nil")
	}
	s.Algorithm = strings.ToLower(strings.TrimSpace(s.Algorithm))
	if s.Algorithm != QualificationSignatureAlgorithmEd25519 {
		return fmt.Errorf("unsupported qualification signature algorithm")
	}
	var err error
	if s.KeyID, err = normalizeDomainIdentifier(s.KeyID, "qualification signature key_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if s.PublicKey, err = normalizeQualificationHex(s.PublicKey, "qualification signature public_key", ed25519.PublicKeySize*2); err != nil {
		return err
	}
	if s.SignedHash, err = normalizeDomainHash(s.SignedHash, "qualification signature signed_hash", true); err != nil {
		return err
	}
	if s.Signature, err = normalizeQualificationHex(s.Signature, "qualification signature signature", ed25519.SignatureSize*2); err != nil {
		return err
	}
	return nil
}

// SignedQualificationBundle is an authenticated, still-redacted evidence
// envelope. It is not a deployment authority and contains no private key.
type SignedQualificationBundle struct {
	SchemaVersion   int                    `json:"schema_version"`
	Bundle          QualificationBundle    `json:"bundle"`
	ObservationHash string                 `json:"observation_hash"`
	Signature       QualificationSignature `json:"signature"`
}

// Normalize validates the bundle, its observation hash, and the signed
// subject identity. Cryptographic verification is separate so callers can
// first apply an external trust policy to KeyID/PublicKey.
func (s *SignedQualificationBundle) Normalize() error {
	if s == nil {
		return fmt.Errorf("signed qualification bundle is nil")
	}
	if s.SchemaVersion == 0 {
		s.SchemaVersion = QualificationSignedBundleSchemaVersion
	}
	if s.SchemaVersion != QualificationSignedBundleSchemaVersion {
		return fmt.Errorf("unsupported signed qualification schema_version %d", s.SchemaVersion)
	}
	if err := s.Bundle.Normalize(); err != nil {
		return err
	}
	var err error
	if s.ObservationHash, err = normalizeDomainHash(s.ObservationHash, "qualification observation_hash", true); err != nil {
		return err
	}
	expectedObservation, err := s.Bundle.ObservationHash()
	if err != nil {
		return err
	}
	if s.ObservationHash != expectedObservation {
		return fmt.Errorf("qualification observation_hash does not match bundle")
	}
	if err := s.Signature.normalize(); err != nil {
		return err
	}
	expectedSubject := signedQualificationSubjectHash(s.SchemaVersion, s.Bundle, s.ObservationHash)
	if s.Signature.SignedHash != expectedSubject {
		return fmt.Errorf("qualification signed_hash does not match bundle subject")
	}
	return nil
}

// ObservationHash returns the stable hash of normalized redacted evidence.
// It excludes wall-clock duration and all raw deployment material.
func (b QualificationBundle) ObservationHash() (string, error) {
	if err := b.Normalize(); err != nil {
		return "", err
	}
	parts := []string{"qualification-observation", b.Report.ReportHash, b.Manifest.ManifestHash}
	for _, item := range b.Report.Cases {
		parts = append(parts, item.Name, item.Category, item.Outcome, item.ErrorCode, item.EvidenceHash)
		for _, measurement := range item.Measurements {
			parts = append(parts, measurement.Name, strconv.FormatInt(measurement.Value, 10), measurement.Unit, strconv.FormatBool(measurement.Present))
		}
	}
	for _, drill := range b.Report.RecoveryDrills {
		parts = append(parts, drill.ID, drill.Kind, drill.Outcome, drill.TargetHash, drill.EvidenceHash, drill.SourceFingerprint, drill.RestoreFingerprint, drill.ReplayHash, drill.FailoverFromHash, drill.FailoverToHash, strconv.FormatBool(drill.WALArchiveVerified), strconv.FormatInt(drill.RPOSeconds, 10), strconv.FormatBool(drill.RPOMeasured), strconv.FormatInt(drill.RTOSeconds, 10), strconv.FormatBool(drill.RTOMeasured))
	}
	for _, evidence := range b.Report.BoundaryEvidence {
		parts = append(parts, evidence.ID, evidence.Kind, evidence.Outcome, evidence.BoundaryHash, evidence.CredentialSourceHash, evidence.IdentityHash, evidence.ProviderRequestHash, evidence.ProviderResponseHash, evidence.SourceFingerprint, evidence.EvidenceHash, strconv.FormatBool(evidence.Measured), evidence.ObservedAt.UTC().Format(time.RFC3339Nano))
		if !evidence.ExpiresAt.IsZero() {
			parts = append(parts, evidence.ExpiresAt.UTC().Format(time.RFC3339Nano))
		}
	}
	for _, check := range b.Manifest.Checks {
		parts = append(parts, check.Name, check.Version, check.Category, check.ExecutionMode, check.InputHash, check.Outcome, check.EvidenceHash)
	}
	return HashStrings(parts...), nil
}

// SigningSubjectHash returns the exact bounded identity covered by the
// signature. Environment names are included; their values are impossible to
// represent here.
func (s SignedQualificationBundle) SigningSubjectHash() (string, error) {
	if err := s.Normalize(); err != nil {
		return "", err
	}
	return signedQualificationSubjectHash(s.SchemaVersion, s.Bundle, s.ObservationHash), nil
}

func signedQualificationSubjectHash(schemaVersion int, bundle QualificationBundle, observationHash string) string {
	parts := []string{"qualification-signed-subject", strconv.Itoa(schemaVersion), bundle.Report.WorkspaceID, bundle.Report.TargetHash, bundle.Report.ReportHash, bundle.Manifest.ManifestHash, bundle.Manifest.RunnerVersion, bundle.Manifest.CommitHash, observationHash}
	parts = append(parts, bundle.Manifest.EnvironmentNames...)
	return HashStrings(parts...)
}

// SignQualificationBundle signs only the bounded subject. The private key is
// accepted by value for the duration of the call and is never serialized.
func SignQualificationBundle(bundle QualificationBundle, keyID string, privateKey ed25519.PrivateKey) (SignedQualificationBundle, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return SignedQualificationBundle{}, fmt.Errorf("qualification private key has invalid size")
	}
	if err := bundle.Normalize(); err != nil {
		return SignedQualificationBundle{}, err
	}
	keyID, err := normalizeDomainIdentifier(keyID, "qualification signature key_id", MaxDomainIDLength, true)
	if err != nil {
		return SignedQualificationBundle{}, err
	}
	observationHash, err := bundle.ObservationHash()
	if err != nil {
		return SignedQualificationBundle{}, err
	}
	signed := SignedQualificationBundle{SchemaVersion: QualificationSignedBundleSchemaVersion, Bundle: bundle, ObservationHash: observationHash}
	signedHash := signedQualificationSubjectHash(signed.SchemaVersion, signed.Bundle, observationHash)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	signed.Signature = QualificationSignature{Algorithm: QualificationSignatureAlgorithmEd25519, KeyID: keyID, PublicKey: hex.EncodeToString(publicKey), SignedHash: signedHash, Signature: hex.EncodeToString(ed25519.Sign(privateKey, []byte(signedHash)))}
	if err := signed.Normalize(); err != nil {
		return SignedQualificationBundle{}, err
	}
	return signed, nil
}

// Verify checks the embedded public key and signature after normalization.
// Deployment code should additionally authorize KeyID against its trust
// policy; embedded public material alone is not an authorization decision.
func (s SignedQualificationBundle) Verify() error {
	if err := s.Normalize(); err != nil {
		return err
	}
	publicKey, err := hex.DecodeString(s.Signature.PublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("qualification public key is invalid")
	}
	signature, err := hex.DecodeString(s.Signature.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("qualification signature is invalid")
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), []byte(s.Signature.SignedHash), signature) {
		return fmt.Errorf("qualification signature verification failed")
	}
	return nil
}

// VerifyWithKey additionally binds verification to the deployment-selected
// key reference. It is the import boundary for callers with an external trust
// catalog.
func (s SignedQualificationBundle) VerifyWithKey(expectedKeyID string, expectedPublicKey ed25519.PublicKey) error {
	if err := s.Verify(); err != nil {
		return err
	}
	if strings.TrimSpace(expectedKeyID) != "" && s.Signature.KeyID != strings.TrimSpace(expectedKeyID) {
		return fmt.Errorf("qualification signature key_id is not trusted")
	}
	if len(expectedPublicKey) > 0 && s.Signature.PublicKey != hex.EncodeToString(expectedPublicKey) {
		return fmt.Errorf("qualification public key is not trusted")
	}
	return nil
}

func normalizeQualificationHex(value, field string, length int) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != length {
		return "", fmt.Errorf("%s has invalid length", field)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return "", fmt.Errorf("%s is not valid hex", field)
	}
	return value, nil
}
