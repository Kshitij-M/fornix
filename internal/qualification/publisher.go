package qualification

import (
	"crypto/ed25519"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

// BoundaryPublisherOptions controls the deployment-owned evidence boundary.
// AsOf is explicit so library callers do not accidentally make a replay or
// validation result depend on the process wall clock.
type BoundaryPublisherOptions struct {
	RequireExternalEffect bool
	AsOf                  time.Time
}

func (o BoundaryPublisherOptions) normalized() (BoundaryPublisherOptions, error) {
	if o.RequireExternalEffect && o.AsOf.IsZero() {
		return BoundaryPublisherOptions{}, fmt.Errorf("external boundary validation requires an explicit as_of time")
	}
	if !o.AsOf.IsZero() {
		o.AsOf = o.AsOf.UTC()
	}
	return o, nil
}

// ValidateBoundaryBundle validates a signed deployment observation bundle
// without making a network or database call. It is the consumer-side mirror
// of PublishBoundaryBundle and is safe for offline replay.
func ValidateBoundaryBundle(value contracts.SignedQualificationBundle, options BoundaryPublisherOptions) error {
	var err error
	if options, err = options.normalized(); err != nil {
		return err
	}
	if err := value.Verify(); err != nil {
		return fmt.Errorf("validate boundary bundle: %w", err)
	}
	if err := validateBoundaryReport(value.Bundle.Report, options); err != nil {
		return err
	}
	return nil
}

// PublishBoundaryBundle validates and signs a deployment-owned observation.
// The private key is used only for the duration of this call and is never
// copied into a durable contract by this package.
func PublishBoundaryBundle(bundle contracts.QualificationBundle, keyID string, privateKey ed25519.PrivateKey, options BoundaryPublisherOptions) (contracts.SignedQualificationBundle, error) {
	var err error
	if options, err = options.normalized(); err != nil {
		return contracts.SignedQualificationBundle{}, err
	}
	if err := validateBoundaryReport(bundle.Report, options); err != nil {
		return contracts.SignedQualificationBundle{}, err
	}
	signed, err := contracts.SignQualificationBundle(bundle, keyID, privateKey)
	if err != nil {
		return contracts.SignedQualificationBundle{}, fmt.Errorf("sign boundary bundle: %w", err)
	}
	if err := ValidateBoundaryBundle(signed, options); err != nil {
		return contracts.SignedQualificationBundle{}, err
	}
	return signed, nil
}

func validateBoundaryReport(report contracts.QualificationReport, options BoundaryPublisherOptions) error {
	if err := report.Normalize(); err != nil {
		return fmt.Errorf("boundary report is invalid: %w", err)
	}
	if len(report.BoundaryEvidence) == 0 {
		return fmt.Errorf("boundary report contains no deployment observations")
	}
	if report.Outcome != contracts.QualificationOutcomePassed {
		return fmt.Errorf("boundary report outcome %q cannot be published", report.Outcome)
	}
	if options.RequireExternalEffect {
		if len(report.BoundaryEvidence) != 1 {
			return fmt.Errorf("external-effect boundary publication requires exactly one observation")
		}
		observation := report.BoundaryEvidence[0]
		if observation.Outcome != contracts.QualificationOutcomePassed || !observation.Measured {
			return fmt.Errorf("external-effect boundary publication requires measured passed evidence")
		}
		if !observation.ExpiresAt.After(options.AsOf) {
			return fmt.Errorf("external-effect boundary observation is expired")
		}
	}
	return nil
}

// BoundaryPublisherSummary is a bounded, secret-free operator result. It is
// intentionally separate from the signed bundle so callers can print it
// without serializing any key material or deployment payload.
type BoundaryPublisherSummary struct {
	SignedHash       string `json:"signed_hash"`
	ObservationHash  string `json:"observation_hash"`
	ReportHash       string `json:"report_hash"`
	ManifestHash     string `json:"manifest_hash"`
	EvidenceCount    int    `json:"evidence_count"`
	ExternalEffect   bool   `json:"external_effect"`
	ExpiresAtRFC3339 string `json:"expires_at,omitempty"`
}

// Summary returns bounded hashes for an already validated bundle.
func Summary(value contracts.SignedQualificationBundle, externalEffect bool) (BoundaryPublisherSummary, error) {
	if err := value.Verify(); err != nil {
		return BoundaryPublisherSummary{}, err
	}
	if externalEffect && len(value.Bundle.Report.BoundaryEvidence) != 1 {
		return BoundaryPublisherSummary{}, fmt.Errorf("external-effect summary requires exactly one observation")
	}
	summary := BoundaryPublisherSummary{
		SignedHash:      value.Signature.SignedHash,
		ObservationHash: value.ObservationHash,
		ReportHash:      value.Bundle.Report.ReportHash,
		ManifestHash:    value.Bundle.Manifest.ManifestHash,
		EvidenceCount:   len(value.Bundle.Report.BoundaryEvidence),
		ExternalEffect:  externalEffect,
	}
	if externalEffect && len(value.Bundle.Report.BoundaryEvidence) == 1 {
		summary.ExpiresAtRFC3339 = value.Bundle.Report.BoundaryEvidence[0].ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	return summary, nil
}

// ParseAsOf is shared by operator surfaces so they use one strict timestamp
// grammar and do not leak arbitrary input into errors or persisted metadata.
func ParseAsOf(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("as_of must be RFC3339")
	}
	return parsed.UTC(), nil
}
