package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	DeploymentCertificateSchemaVersion = 1
	MaxCertificateServerNameLength     = 253
	MaxCertificateChainLength          = 8
	MaxCertificatePins                 = 32
	MaxCertificatePinLength            = 64
	MaxDeploymentSourceVersionLength   = 128
)

// CertificatePolicy is public, bounded policy used to construct and validate
// one deployment transport. Roots and private keys remain process-local
// inputs; this contract contains only server identity and public fingerprints.
type CertificatePolicy struct {
	SchemaVersion          int      `json:"schema_version"`
	ServerName             string   `json:"server_name"`
	ClockSkewSeconds       int      `json:"clock_skew_seconds"`
	MaxChainLength         int      `json:"max_chain_length"`
	SPKISHA256Pins         []string `json:"spki_sha256_pins,omitempty"`
	CertificateSHA256Pins  []string `json:"certificate_sha256_pins,omitempty"`
	RevokedFingerprints    []string `json:"revoked_fingerprints,omitempty"`
	RequireServerNameMatch bool     `json:"require_server_name_match"`
}

func (p *CertificatePolicy) Normalize() error {
	if p == nil {
		return fmt.Errorf("certificate policy is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = DeploymentCertificateSchemaVersion
	}
	if p.SchemaVersion != DeploymentCertificateSchemaVersion {
		return fmt.Errorf("unsupported certificate policy schema_version %d", p.SchemaVersion)
	}
	p.ServerName = strings.TrimSpace(strings.ToLower(p.ServerName))
	if p.ServerName == "" || len(p.ServerName) > MaxCertificateServerNameLength || strings.ContainsAny(p.ServerName, " /\\\t\r\n") {
		return fmt.Errorf("certificate server_name is invalid")
	}
	if p.ClockSkewSeconds == 0 {
		p.ClockSkewSeconds = 30
	}
	if p.ClockSkewSeconds < 0 || p.ClockSkewSeconds > 300 {
		return fmt.Errorf("certificate clock skew is outside bounds")
	}
	if p.MaxChainLength == 0 {
		p.MaxChainLength = MaxCertificateChainLength
	}
	if p.MaxChainLength < 1 || p.MaxChainLength > MaxCertificateChainLength {
		return fmt.Errorf("certificate chain length is outside bounds")
	}
	var err error
	p.SPKISHA256Pins, err = normalizeCertificatePins(p.SPKISHA256Pins, "spki")
	if err != nil {
		return err
	}
	p.CertificateSHA256Pins, err = normalizeCertificatePins(p.CertificateSHA256Pins, "certificate")
	if err != nil {
		return err
	}
	p.RevokedFingerprints, err = normalizeCertificatePins(p.RevokedFingerprints, "revoked")
	if err != nil {
		return err
	}
	if !p.RequireServerNameMatch {
		p.RequireServerNameMatch = true
	}
	return nil
}

func normalizeCertificatePins(values []string, field string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if len(value) != MaxCertificatePinLength || !isLowerHexHash(value) {
			return nil, fmt.Errorf("certificate %s pin is invalid", field)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	if len(result) > MaxCertificatePins {
		return nil, fmt.Errorf("certificate %s pins exceed limit", field)
	}
	sort.Strings(result)
	return result, nil
}

// CertificateObservation is a redacted validation result. Fingerprints are
// SHA-256 values of public certificate material and are safe to persist as
// bounded audit dimensions; certificate and private-key bytes are not.
type CertificateObservation struct {
	SchemaVersion      int       `json:"schema_version"`
	ServerName         string    `json:"server_name"`
	LeafFingerprint    string    `json:"leaf_fingerprint"`
	SPKIFingerprint    string    `json:"spki_fingerprint"`
	NotBefore          time.Time `json:"not_before"`
	NotAfter           time.Time `json:"not_after"`
	ChainLength        int       `json:"chain_length"`
	SourceVersion      string    `json:"source_version,omitempty"`
	RevocationEpoch    uint64    `json:"revocation_epoch,omitempty"`
	ValidatedAt        time.Time `json:"validated_at"`
	RotationObserved   bool      `json:"rotation_observed"`
	RevocationObserved bool      `json:"revocation_observed"`
}

func (o *CertificateObservation) Normalize() error {
	if o == nil {
		return fmt.Errorf("certificate observation is nil")
	}
	if o.SchemaVersion == 0 {
		o.SchemaVersion = DeploymentCertificateSchemaVersion
	}
	if o.SchemaVersion != DeploymentCertificateSchemaVersion || !isLowerHexHash(o.LeafFingerprint) || !isLowerHexHash(o.SPKIFingerprint) || o.ChainLength < 1 || o.ChainLength > MaxCertificateChainLength || o.NotAfter.IsZero() || o.NotBefore.IsZero() || o.ValidatedAt.IsZero() || o.NotAfter.Before(o.NotBefore) {
		return fmt.Errorf("certificate observation is invalid")
	}
	o.ServerName = strings.TrimSpace(strings.ToLower(o.ServerName))
	if o.ServerName == "" || len(o.ServerName) > MaxCertificateServerNameLength {
		return fmt.Errorf("certificate observation server_name is invalid")
	}
	o.SourceVersion = strings.TrimSpace(o.SourceVersion)
	if len(o.SourceVersion) > MaxDeploymentSourceVersionLength {
		return fmt.Errorf("certificate observation source_version is too large")
	}
	o.NotBefore = o.NotBefore.UTC()
	o.NotAfter = o.NotAfter.UTC()
	o.ValidatedAt = o.ValidatedAt.UTC()
	return nil
}

// DeploymentAuthorityObservation records only the non-secret facts needed to
// measure source rotation and revocation propagation.
type DeploymentAuthorityObservation struct {
	SchemaVersion     int       `json:"schema_version"`
	WorkspaceID       string    `json:"workspace_id"`
	Provider          string    `json:"provider"`
	SourceVersion     string    `json:"source_version"`
	PreviousVersion   string    `json:"previous_version,omitempty"`
	RevocationEpoch   uint64    `json:"revocation_epoch"`
	ObservedAt        time.Time `json:"observed_at"`
	RotatedAt         time.Time `json:"rotated_at,omitempty"`
	RevokedAt         time.Time `json:"revoked_at,omitempty"`
	RotationLagMillis int64     `json:"rotation_lag_ms,omitempty"`
	RevocationLagMS   int64     `json:"revocation_lag_ms,omitempty"`
	ObservationHash   string    `json:"observation_hash"`
}

func (o *DeploymentAuthorityObservation) Normalize() error {
	if o == nil {
		return fmt.Errorf("deployment authority observation is nil")
	}
	if o.SchemaVersion == 0 {
		o.SchemaVersion = DeploymentCertificateSchemaVersion
	}
	o.WorkspaceID = strings.TrimSpace(o.WorkspaceID)
	o.Provider = strings.TrimSpace(o.Provider)
	o.SourceVersion = strings.TrimSpace(o.SourceVersion)
	o.PreviousVersion = strings.TrimSpace(o.PreviousVersion)
	if o.SchemaVersion != DeploymentCertificateSchemaVersion || o.WorkspaceID == "" || o.Provider == "" || o.SourceVersion == "" || len(o.SourceVersion) > MaxDeploymentSourceVersionLength || o.ObservedAt.IsZero() || o.ObservationHash == "" || !isLowerHexHash(o.ObservationHash) || o.RotationLagMillis < 0 || o.RevocationLagMS < 0 {
		return fmt.Errorf("deployment authority observation is invalid")
	}
	o.ObservedAt = o.ObservedAt.UTC()
	if !o.RotatedAt.IsZero() {
		o.RotatedAt = o.RotatedAt.UTC()
	}
	if !o.RevokedAt.IsZero() {
		o.RevokedAt = o.RevokedAt.UTC()
	}
	return nil
}

// DeploymentAuthorityHash produces a stable redacted observation identity.
func DeploymentAuthorityHash(workspaceID, provider, sourceVersion string, epoch uint64) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{strings.TrimSpace(workspaceID), strings.TrimSpace(provider), strings.TrimSpace(sourceVersion), fmt.Sprint(epoch)}, "\x00")))
	return hex.EncodeToString(digest[:])
}
