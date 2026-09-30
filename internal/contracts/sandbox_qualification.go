package contracts

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

const SandboxQualificationSchemaVersion = 2

var sandboxImmutableImageDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// SandboxLimitEnvelope is the largest per-run budget exercised by the signed
// conformance suite. A requested profile may tighten, but never exceed, this
// envelope. The values use the same units as SandboxProfile.
type SandboxLimitEnvelope struct {
	TimeoutMS      int   `json:"timeout_ms"`
	MaxStdoutBytes int   `json:"max_stdout_bytes"`
	MaxStderrBytes int   `json:"max_stderr_bytes"`
	MaxArgCount    int   `json:"max_arg_count"`
	MaxArgBytes    int   `json:"max_arg_bytes"`
	MaxEnvEntries  int   `json:"max_env_entries"`
	MaxEnvBytes    int   `json:"max_env_bytes"`
	CPUQuotaMilli  int   `json:"cpu_quota_milli"`
	MemoryBytes    int64 `json:"memory_bytes"`
	PIDsLimit      int   `json:"pids_limit"`
	ScratchBytes   int64 `json:"scratch_bytes"`
}

func (l *SandboxLimitEnvelope) Normalize() error {
	if l == nil {
		return fmt.Errorf("sandbox qualification limit envelope is nil")
	}
	if l.TimeoutMS < 1 || l.TimeoutMS > int(MaxToolTimeout/time.Millisecond) ||
		l.MaxStdoutBytes < 1 || l.MaxStdoutBytes > MaxToolOutputBytes || l.MaxStderrBytes < 1 || l.MaxStderrBytes > MaxToolOutputBytes ||
		l.MaxArgCount < 1 || l.MaxArgCount > MaxToolArgCount || l.MaxArgBytes < 1 || l.MaxArgBytes > MaxToolArgBytes ||
		l.MaxEnvEntries < 0 || l.MaxEnvEntries > MaxToolEnvCount || l.MaxEnvBytes < 0 || l.MaxEnvBytes > MaxToolEnvBytes ||
		l.CPUQuotaMilli < 1 || l.CPUQuotaMilli > MaxToolCPUQuotaMilli || l.MemoryBytes < 1 || l.MemoryBytes > MaxToolMemoryBytes ||
		l.PIDsLimit < 1 || l.PIDsLimit > MaxToolProcessCount || l.ScratchBytes < 1 || l.ScratchBytes > MaxToolScratchBytes {
		return fmt.Errorf("sandbox qualification limit envelope is incomplete or outside tool bounds")
	}
	return nil
}

// Supports reports whether a normalized non-local profile stays within the
// measured resource and request-size envelope.
func (l SandboxLimitEnvelope) Supports(profile SandboxProfile) bool {
	if l.Normalize() != nil || profile.Normalize() != nil || profile.Backend == string(SandboxBackendLocalProcess) {
		return false
	}
	return profile.TimeoutMS <= l.TimeoutMS && profile.MaxStdoutBytes <= l.MaxStdoutBytes &&
		profile.MaxStderrBytes <= l.MaxStderrBytes && profile.MaxArgCount <= l.MaxArgCount &&
		profile.MaxArgBytes <= l.MaxArgBytes && profile.MaxEnvEntries <= l.MaxEnvEntries &&
		profile.MaxEnvBytes <= l.MaxEnvBytes && profile.CPUQuotaMilli <= l.CPUQuotaMilli &&
		profile.MemoryBytes <= l.MemoryBytes && profile.PIDsLimit <= l.PIDsLimit &&
		profile.ScratchBytes <= l.ScratchBytes
}

// SandboxRuntimeIdentity names the exact runtime material checked against a
// signed qualification. Hashes identify builds/configuration; they contain no
// executable paths, credentials, hostnames, or other deployment secrets.
type SandboxRuntimeIdentity struct {
	ProviderBuildHash        string               `json:"provider_build_hash"`
	RuntimeBuildHash         string               `json:"runtime_build_hash"`
	RuntimeConfigurationHash string               `json:"runtime_configuration_hash"`
	ImageDigest              string               `json:"image_digest"`
	ImagePlatform            SandboxImagePlatform `json:"image_platform"`
}

func (i *SandboxRuntimeIdentity) Normalize() error {
	if i == nil {
		return fmt.Errorf("sandbox runtime identity is nil")
	}
	for name, value := range map[string]*string{
		"provider_build_hash":        &i.ProviderBuildHash,
		"runtime_build_hash":         &i.RuntimeBuildHash,
		"runtime_configuration_hash": &i.RuntimeConfigurationHash,
	} {
		var err error
		if *value, err = normalizeDomainHash(*value, "sandbox "+name, true); err != nil {
			return err
		}
	}
	i.ImageDigest = strings.ToLower(strings.TrimSpace(i.ImageDigest))
	if !sandboxImmutableImageDigest.MatchString(i.ImageDigest) {
		return fmt.Errorf("sandbox runtime image_digest must be an immutable sha256 digest")
	}
	if err := i.ImagePlatform.Normalize(); err != nil {
		return fmt.Errorf("sandbox runtime image_platform is invalid: %w", err)
	}
	return nil
}

// SandboxQualificationEvidence is a bounded, hash-only attestation payload.
// Its StableHash must appear in a passing sandbox case in a trusted signed
// QualificationBundle before a runtime-backed provider is admitted.
type SandboxQualificationEvidence struct {
	SchemaVersion  int                    `json:"schema_version"`
	ID             string                 `json:"id"`
	Backend        SandboxBackend         `json:"backend"`
	Runtime        SandboxRuntimeIdentity `json:"runtime"`
	CapabilityHash string                 `json:"capability_hash"`
	Limits         SandboxLimitEnvelope   `json:"limits"`
	TestSuiteHash  string                 `json:"test_suite_hash"`
	Outcome        string                 `json:"outcome"`
	Measured       bool                   `json:"measured"`
	ObservedAt     time.Time              `json:"observed_at"`
	ExpiresAt      time.Time              `json:"expires_at"`
}

// NormalizeAt validates evidence against an explicit time. Passed evidence
// must be measured, current, and no more than one year old; callers must still
// verify the signature and deployment-selected signer/target.
func (e *SandboxQualificationEvidence) NormalizeAt(now time.Time) error {
	if e == nil {
		return fmt.Errorf("sandbox qualification evidence is nil")
	}
	if e.SchemaVersion == 0 {
		e.SchemaVersion = SandboxQualificationSchemaVersion
	}
	if e.SchemaVersion != SandboxQualificationSchemaVersion {
		return fmt.Errorf("unsupported sandbox qualification schema_version")
	}
	var err error
	if e.ID, err = normalizeDomainIdentifier(e.ID, "sandbox qualification id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if !knownSandboxBackend(e.Backend) || e.Backend == SandboxBackendLocalProcess {
		return fmt.Errorf("sandbox qualification must identify a non-local backend")
	}
	if err := e.Runtime.Normalize(); err != nil {
		return err
	}
	if e.CapabilityHash, err = normalizeDomainHash(e.CapabilityHash, "sandbox qualification capability_hash", true); err != nil {
		return err
	}
	if err := e.Limits.Normalize(); err != nil {
		return err
	}
	if e.TestSuiteHash, err = normalizeDomainHash(e.TestSuiteHash, "sandbox qualification test_suite_hash", true); err != nil {
		return err
	}
	e.Outcome = strings.ToLower(strings.TrimSpace(e.Outcome))
	if e.Outcome != QualificationOutcomePassed || !e.Measured {
		return fmt.Errorf("sandbox qualification must be measured and passed")
	}
	if e.ObservedAt.IsZero() || e.ExpiresAt.IsZero() {
		return fmt.Errorf("sandbox qualification observation and expiry are required")
	}
	e.ObservedAt, e.ExpiresAt = e.ObservedAt.UTC(), e.ExpiresAt.UTC()
	now = now.UTC()
	if now.IsZero() || e.ObservedAt.After(now) || !e.ExpiresAt.After(now) || !e.ExpiresAt.After(e.ObservedAt) || e.ExpiresAt.Sub(e.ObservedAt) > 365*24*time.Hour {
		return fmt.Errorf("sandbox qualification is future-dated, expired, or exceeds its one-year validity bound")
	}
	return nil
}

// StableHash returns the identity of evidence normalized at a non-expired
// reference time. Invalid or expired evidence has no usable hash.
func (e SandboxQualificationEvidence) StableHash() string {
	if err := e.NormalizeAt(e.ObservedAt); err != nil {
		return ""
	}
	return HashStrings(
		"sandbox-qualification-evidence-v2", fmt.Sprint(e.SchemaVersion), e.ID,
		string(e.Backend), e.Runtime.ProviderBuildHash, e.Runtime.RuntimeBuildHash,
		e.Runtime.RuntimeConfigurationHash, e.Runtime.ImageDigest,
		e.Runtime.ImagePlatform.OS, e.Runtime.ImagePlatform.Architecture, e.Runtime.ImagePlatform.Variant,
		e.CapabilityHash,
		fmt.Sprint(e.Limits.TimeoutMS), fmt.Sprint(e.Limits.MaxStdoutBytes), fmt.Sprint(e.Limits.MaxStderrBytes),
		fmt.Sprint(e.Limits.MaxArgCount), fmt.Sprint(e.Limits.MaxArgBytes), fmt.Sprint(e.Limits.MaxEnvEntries), fmt.Sprint(e.Limits.MaxEnvBytes),
		fmt.Sprint(e.Limits.CPUQuotaMilli), fmt.Sprint(e.Limits.MemoryBytes), fmt.Sprint(e.Limits.PIDsLimit), fmt.Sprint(e.Limits.ScratchBytes),
		e.TestSuiteHash, e.Outcome, fmt.Sprint(e.Measured),
		e.ObservedAt.UTC().Format(time.RFC3339Nano), e.ExpiresAt.UTC().Format(time.RFC3339Nano),
	)
}

// SandboxQualificationProof pairs the exact tested environment with an
// existing signed deployment qualification bundle. The bundle signs the
// evidence hash through its sandbox qualification case and manifest entry.
type SandboxQualificationProof struct {
	Evidence SandboxQualificationEvidence `json:"evidence"`
	Bundle   SignedQualificationBundle    `json:"bundle"`
}
