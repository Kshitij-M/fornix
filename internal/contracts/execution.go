package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
)

const (
	DefaultExecutionMaxSteps            = 64
	DefaultExecutionMaxConcurrency      = 1
	DefaultExecutionTimeoutMS           = 15 * 60 * 1000
	DefaultExecutionMaxInputBytes       = 4 << 20
	DefaultExecutionMaxOutputBytes      = 4 << 20
	DefaultExecutionMaxOutputTokens     = 32768
	DefaultExecutionMaxCostUSD          = 10.0
	DefaultExecutionMaxRetries          = 2
	DefaultExecutionMaxDisclosureBytes  = 1 << 20
	DefaultExecutionMaxDisclosureItems  = 128
	DefaultExecutionMaxDisclosureTokens = 65536
	DefaultExecutionCancellationGraceMS = 30 * 1000

	MaxExecutionSteps               = 1024
	MaxExecutionConcurrency         = 64
	MaxExecutionTimeoutMS           = 24 * 60 * 60 * 1000
	MaxExecutionInputBytes          = 64 << 20
	MaxExecutionOutputBytes         = 64 << 20
	MaxExecutionOutputTokens        = 1 << 20
	MaxExecutionCostUSD             = 10000.0
	MaxExecutionRetries             = 32
	MaxExecutionDisclosureBytes     = 16 << 20
	MaxExecutionDisclosureItems     = 4096
	MaxExecutionDisclosureTokens    = 1 << 20
	MaxExecutionCancellationGraceMS = 10 * 60 * 1000
)

// ExecutionProfile contains hard limits for one operation or step. Zero
// values receive conservative bounded defaults; callers cannot represent an
// unlimited execution profile through this contract.
type ExecutionProfile struct {
	SchemaVersion          int     `json:"schema_version"`
	MaxSteps               int     `json:"max_steps"`
	MaxConcurrency         int     `json:"max_concurrency"`
	TimeoutMS              int64   `json:"timeout_ms"`
	MaxInputBytes          int64   `json:"max_input_bytes"`
	MaxOutputBytes         int64   `json:"max_output_bytes"`
	MaxOutputTokens        int     `json:"max_output_tokens"`
	MaxCostUSD             float64 `json:"max_cost_usd"`
	MaxRetries             int     `json:"max_retries"`
	MaxDisclosureBytes     int64   `json:"max_disclosure_bytes"`
	MaxDisclosureItems     int     `json:"max_disclosure_items"`
	MaxDisclosureTokens    int     `json:"max_disclosure_tokens"`
	MaxCancellationGraceMS int64   `json:"max_cancellation_grace_ms"`
	AllowExternalEffects   bool    `json:"allow_external_effects"`
}

// DefaultExecutionProfile returns a bounded offline-first profile.
func DefaultExecutionProfile() ExecutionProfile {
	return ExecutionProfile{
		SchemaVersion: DomainNeutralSchemaVersion,
		MaxSteps:      DefaultExecutionMaxSteps, MaxConcurrency: DefaultExecutionMaxConcurrency,
		TimeoutMS: DefaultExecutionTimeoutMS, MaxInputBytes: DefaultExecutionMaxInputBytes,
		MaxOutputBytes: DefaultExecutionMaxOutputBytes, MaxOutputTokens: DefaultExecutionMaxOutputTokens,
		MaxCostUSD: DefaultExecutionMaxCostUSD, MaxRetries: DefaultExecutionMaxRetries,
		MaxDisclosureBytes: DefaultExecutionMaxDisclosureBytes, MaxDisclosureItems: DefaultExecutionMaxDisclosureItems,
		MaxDisclosureTokens: DefaultExecutionMaxDisclosureTokens, MaxCancellationGraceMS: DefaultExecutionCancellationGraceMS,
	}
}

// Normalize fills bounded defaults and rejects negative or excessive limits.
func (p *ExecutionProfile) Normalize() error {
	if p == nil {
		return fmt.Errorf("execution profile is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = DomainNeutralSchemaVersion
	}
	if p.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported execution profile schema_version %d", p.SchemaVersion)
	}
	defaults := DefaultExecutionProfile()
	if p.MaxSteps == 0 {
		p.MaxSteps = defaults.MaxSteps
	}
	if p.MaxConcurrency == 0 {
		p.MaxConcurrency = defaults.MaxConcurrency
	}
	if p.TimeoutMS == 0 {
		p.TimeoutMS = defaults.TimeoutMS
	}
	if p.MaxInputBytes == 0 {
		p.MaxInputBytes = defaults.MaxInputBytes
	}
	if p.MaxOutputBytes == 0 {
		p.MaxOutputBytes = defaults.MaxOutputBytes
	}
	if p.MaxOutputTokens == 0 {
		p.MaxOutputTokens = defaults.MaxOutputTokens
	}
	if p.MaxCostUSD == 0 {
		p.MaxCostUSD = defaults.MaxCostUSD
	}
	if p.MaxRetries == 0 {
		p.MaxRetries = defaults.MaxRetries
	}
	if p.MaxDisclosureBytes == 0 {
		p.MaxDisclosureBytes = defaults.MaxDisclosureBytes
	}
	if p.MaxDisclosureItems == 0 {
		p.MaxDisclosureItems = defaults.MaxDisclosureItems
	}
	if p.MaxDisclosureTokens == 0 {
		p.MaxDisclosureTokens = defaults.MaxDisclosureTokens
	}
	if p.MaxCancellationGraceMS == 0 {
		p.MaxCancellationGraceMS = defaults.MaxCancellationGraceMS
	}
	if p.MaxSteps < 1 || p.MaxSteps > MaxExecutionSteps || p.MaxConcurrency < 1 || p.MaxConcurrency > MaxExecutionConcurrency ||
		p.TimeoutMS < 1 || p.TimeoutMS > MaxExecutionTimeoutMS || p.MaxInputBytes < 1 || p.MaxInputBytes > MaxExecutionInputBytes ||
		p.MaxOutputBytes < 1 || p.MaxOutputBytes > MaxExecutionOutputBytes || p.MaxOutputTokens < 1 || p.MaxOutputTokens > MaxExecutionOutputTokens ||
		p.MaxCostUSD < 0 || p.MaxCostUSD > MaxExecutionCostUSD || math.IsNaN(p.MaxCostUSD) || math.IsInf(p.MaxCostUSD, 0) ||
		p.MaxRetries < 0 || p.MaxRetries > MaxExecutionRetries || p.MaxDisclosureBytes < 1 || p.MaxDisclosureBytes > MaxExecutionDisclosureBytes ||
		p.MaxDisclosureItems < 1 || p.MaxDisclosureItems > MaxExecutionDisclosureItems || p.MaxDisclosureTokens < 1 || p.MaxDisclosureTokens > MaxExecutionDisclosureTokens ||
		p.MaxCancellationGraceMS < 1 || p.MaxCancellationGraceMS > MaxExecutionCancellationGraceMS {
		return fmt.Errorf("execution profile is outside supported bounds")
	}
	return nil
}

// StableHash is the canonical identity of a normalized execution profile.
func (p ExecutionProfile) StableHash() string {
	if err := p.Normalize(); err != nil {
		return ""
	}
	raw, _ := json.Marshal(p)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
