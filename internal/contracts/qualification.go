package contracts

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// QualificationSchemaVersion versions the redacted deployment-evidence
// envelope independently from operation and event schemas.
const QualificationSchemaVersion = 1

const (
	MaxQualificationCases            = 128
	MaxQualificationMeasurements     = 32
	MaxQualificationDrills           = 32
	MaxQualificationReportBytes      = 128 << 10
	MaxQualificationManifestChecks   = 128
	MaxQualificationEnvironmentNames = 32
)

const QualificationManifestSchemaVersion = 1

const (
	QualificationOutcomePassed  = "passed"
	QualificationOutcomeFailed  = "failed"
	QualificationOutcomeBlocked = "blocked"
	QualificationOutcomeSkipped = "skipped"
)

const (
	QualificationCategoryAdapter          = "adapter"
	QualificationCategoryAuthority        = "authority"
	QualificationCategoryCertificate      = "certificate"
	QualificationCategoryExternalBoundary = "external_boundary"
	QualificationCategorySandbox          = "sandbox"
	QualificationCategoryTopology         = "topology"
	QualificationCategoryRecovery         = "recovery"
	QualificationCategoryRetention        = "retention"
	QualificationCategoryLoad             = "load"
)

// BoundaryQualificationKind is a closed vocabulary for deployment-owned
// observations at an external boundary. The values identify the property
// being observed; they never imply that Fornix performed the observation.
const (
	BoundaryQualificationCredentialResolution = "credential_resolution"
	BoundaryQualificationWorkloadIdentity     = "workload_identity"
	BoundaryQualificationMTLS                 = "mtls"
	BoundaryQualificationDNSRebinding         = "dns_rebinding"
	BoundaryQualificationProxyFirewall        = "proxy_firewall"
	BoundaryQualificationProviderIdempotency  = "provider_idempotency"
	BoundaryQualificationExternalRecovery     = "external_recovery"

	MaxBoundaryQualificationEvidence = 32
)

const (
	RecoveryDrillBackupRestore        = "backup_restore"
	RecoveryDrillPITR                 = "pitr"
	RecoveryDrillFailover             = "failover"
	RecoveryDrillPartitionMaintenance = "partition_maintenance"
	RecoveryDrillIdentityRotation     = "identity_rotation"
)

// QualificationMeasurement is a bounded numeric observation. Present makes a
// measured zero distinct from an unavailable measurement; values have no
// implicit unit and must provide one when present.
type QualificationMeasurement struct {
	Name    string `json:"name"`
	Value   int64  `json:"value,omitempty"`
	Unit    string `json:"unit,omitempty"`
	Present bool   `json:"present"`
}

func (m *QualificationMeasurement) normalize(index int) error {
	if m == nil {
		return fmt.Errorf("qualification measurement %d is nil", index)
	}
	var err error
	if m.Name, err = normalizeDomainName(m.Name, fmt.Sprintf("qualification measurement %d name", index), MaxDomainNameLength, true); err != nil {
		return err
	}
	if m.Unit, err = normalizeDomainName(m.Unit, fmt.Sprintf("qualification measurement %d unit", index), MaxDomainNameLength, m.Present); err != nil {
		return err
	}
	if !m.Present && (m.Value != 0 || m.Unit != "") {
		return fmt.Errorf("qualification measurement %q has a value without present=true", m.Name)
	}
	return nil
}

// QualificationCase is one redacted, replay-comparable qualification result.
// DurationMS is diagnostic and intentionally excluded from the stable report
// hash; callers that need timing in the identity must add a measurement.
type QualificationCase struct {
	Name         string                     `json:"name"`
	Category     string                     `json:"category"`
	Outcome      string                     `json:"outcome"`
	ErrorCode    string                     `json:"error_code,omitempty"`
	DurationMS   int64                      `json:"duration_ms,omitempty"`
	EvidenceHash string                     `json:"evidence_hash,omitempty"`
	Measurements []QualificationMeasurement `json:"measurements,omitempty"`
}

// BoundaryQualificationEvidence is a deployment-owned, hash-only observation
// that is covered by the signed QualificationReport. It intentionally has no
// field for a secret, URL, certificate, token, prompt, header, or provider
// payload. BoundaryHash must be the stable hash of the exact
// ExternalBoundaryAuthority that was observed by the deployment.
type BoundaryQualificationEvidence struct {
	ID                   string    `json:"id"`
	Kind                 string    `json:"kind"`
	Outcome              string    `json:"outcome"`
	BoundaryHash         string    `json:"boundary_hash"`
	CredentialSourceHash string    `json:"credential_source_hash,omitempty"`
	IdentityHash         string    `json:"identity_hash,omitempty"`
	ProviderRequestHash  string    `json:"provider_request_hash,omitempty"`
	ProviderResponseHash string    `json:"provider_response_hash,omitempty"`
	SourceFingerprint    string    `json:"source_fingerprint,omitempty"`
	EvidenceHash         string    `json:"evidence_hash"`
	Measured             bool      `json:"measured"`
	ObservedAt           time.Time `json:"observed_at"`
	ExpiresAt            time.Time `json:"expires_at,omitempty"`
}

func (e *BoundaryQualificationEvidence) normalize(index int) error {
	if e == nil {
		return fmt.Errorf("boundary qualification evidence %d is nil", index)
	}
	var err error
	if e.ID, err = normalizeDomainIdentifier(e.ID, fmt.Sprintf("boundary qualification evidence %d id", index), MaxDomainIDLength, true); err != nil {
		return err
	}
	if e.Kind, err = normalizeDomainName(e.Kind, fmt.Sprintf("boundary qualification evidence %q kind", e.ID), MaxDomainNameLength, true); err != nil {
		return err
	}
	if !validBoundaryQualificationKind(e.Kind) {
		return fmt.Errorf("boundary qualification evidence %q has unsupported kind %q", e.ID, e.Kind)
	}
	e.Outcome = strings.ToLower(strings.TrimSpace(e.Outcome))
	if err := normalizeQualificationOutcome(e.Outcome); err != nil {
		return fmt.Errorf("boundary qualification evidence %q: %w", e.ID, err)
	}
	if e.BoundaryHash, err = normalizeDomainHash(e.BoundaryHash, fmt.Sprintf("boundary qualification evidence %q boundary_hash", e.ID), true); err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"credential_source_hash": &e.CredentialSourceHash,
		"identity_hash":          &e.IdentityHash,
		"provider_request_hash":  &e.ProviderRequestHash,
		"provider_response_hash": &e.ProviderResponseHash,
		"source_fingerprint":     &e.SourceFingerprint,
	} {
		if *value, err = normalizeDomainHash(*value, fmt.Sprintf("boundary qualification evidence %q %s", e.ID, field), false); err != nil {
			return err
		}
	}
	if e.ObservedAt.IsZero() {
		return fmt.Errorf("boundary qualification evidence %q observed_at is required", e.ID)
	}
	e.ObservedAt = e.ObservedAt.UTC()
	if !e.ExpiresAt.IsZero() {
		e.ExpiresAt = e.ExpiresAt.UTC()
		if !e.ExpiresAt.After(e.ObservedAt) {
			return fmt.Errorf("boundary qualification evidence %q expires_at must follow observed_at", e.ID)
		}
	}
	calculated := boundaryQualificationEvidenceHash(*e)
	if e.EvidenceHash != "" {
		if e.EvidenceHash, err = normalizeDomainHash(e.EvidenceHash, fmt.Sprintf("boundary qualification evidence %q evidence_hash", e.ID), true); err != nil {
			return err
		}
		if e.EvidenceHash != calculated {
			return fmt.Errorf("boundary qualification evidence %q evidence_hash does not match evidence", e.ID)
		}
	} else {
		e.EvidenceHash = calculated
	}
	if e.Outcome == QualificationOutcomePassed {
		if !e.Measured {
			return fmt.Errorf("passed boundary qualification evidence %q must be measured", e.ID)
		}
		if e.ExpiresAt.IsZero() {
			return fmt.Errorf("passed boundary qualification evidence %q requires expires_at", e.ID)
		}
	}
	return nil
}

// StableHash returns the identity of one normalized observation. Invalid
// observations return an empty hash so malformed deployment claims cannot be
// accidentally used as authority facts.
func (e BoundaryQualificationEvidence) StableHash() string {
	if err := e.normalize(-1); err != nil {
		return ""
	}
	return e.EvidenceHash
}

func boundaryQualificationEvidenceHash(e BoundaryQualificationEvidence) string {
	expires := ""
	if !e.ExpiresAt.IsZero() {
		expires = e.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	return HashStrings("boundary-qualification-evidence", e.ID, e.Kind, e.Outcome, e.BoundaryHash, e.CredentialSourceHash, e.IdentityHash, e.ProviderRequestHash, e.ProviderResponseHash, e.SourceFingerprint, fmt.Sprint(e.Measured), e.ObservedAt.UTC().Format(time.RFC3339Nano), expires)
}

func (c *QualificationCase) normalize(index int) error {
	if c == nil {
		return fmt.Errorf("qualification case %d is nil", index)
	}
	var err error
	if c.Name, err = normalizeDomainName(c.Name, fmt.Sprintf("qualification case %d name", index), MaxDomainNameLength, true); err != nil {
		return err
	}
	if c.Category, err = normalizeDomainName(c.Category, fmt.Sprintf("qualification case %d category", index), MaxDomainNameLength, true); err != nil {
		return err
	}
	if !validQualificationCategory(c.Category) {
		return fmt.Errorf("qualification case %q has unsupported category %q", c.Name, c.Category)
	}
	c.Outcome = strings.ToLower(strings.TrimSpace(c.Outcome))
	if err := normalizeQualificationOutcome(c.Outcome); err != nil {
		return fmt.Errorf("qualification case %q: %w", c.Name, err)
	}
	c.ErrorCode, err = normalizeDomainName(c.ErrorCode, fmt.Sprintf("qualification case %q error_code", c.Name), MaxDomainNameLength, false)
	if err != nil {
		return err
	}
	if c.DurationMS < 0 || c.DurationMS > 24*60*60*1000 {
		return fmt.Errorf("qualification case %q duration is outside bounds", c.Name)
	}
	if c.EvidenceHash, err = normalizeDomainHash(c.EvidenceHash, fmt.Sprintf("qualification case %q evidence_hash", c.Name), false); err != nil {
		return err
	}
	if len(c.Measurements) > MaxQualificationMeasurements {
		return fmt.Errorf("qualification case %q has too many measurements", c.Name)
	}
	seen := make(map[string]struct{}, len(c.Measurements))
	for measurementIndex := range c.Measurements {
		if err := c.Measurements[measurementIndex].normalize(measurementIndex); err != nil {
			return err
		}
		key := c.Measurements[measurementIndex].Name
		if _, exists := seen[key]; exists {
			return fmt.Errorf("qualification case %q repeats measurement %q", c.Name, key)
		}
		seen[key] = struct{}{}
	}
	sort.Slice(c.Measurements, func(i, j int) bool { return c.Measurements[i].Name < c.Measurements[j].Name })
	return nil
}

// RecoveryDrill is explicit evidence for a deployment-owned recovery or
// identity exercise. Fingerprints and hashes are safe references; raw backup,
// topology, certificate, and credential material is not representable here.
type RecoveryDrill struct {
	ID                 string    `json:"id"`
	Kind               string    `json:"kind"`
	Outcome            string    `json:"outcome"`
	TargetHash         string    `json:"target_hash"`
	EvidenceHash       string    `json:"evidence_hash,omitempty"`
	SourceFingerprint  string    `json:"source_fingerprint,omitempty"`
	RestoreFingerprint string    `json:"restore_fingerprint,omitempty"`
	ReplayHash         string    `json:"replay_hash,omitempty"`
	FailoverFromHash   string    `json:"failover_from_hash,omitempty"`
	FailoverToHash     string    `json:"failover_to_hash,omitempty"`
	WALArchiveVerified bool      `json:"wal_archive_verified"`
	RPOSeconds         int64     `json:"rpo_seconds,omitempty"`
	RPOMeasured        bool      `json:"rpo_measured"`
	RTOSeconds         int64     `json:"rto_seconds,omitempty"`
	RTOMeasured        bool      `json:"rto_measured"`
	ObservedAt         time.Time `json:"observed_at"`
}

func (d *RecoveryDrill) normalize(index int) error {
	if d == nil {
		return fmt.Errorf("recovery drill %d is nil", index)
	}
	var err error
	if d.ID, err = normalizeDomainIdentifier(d.ID, fmt.Sprintf("recovery drill %d id", index), MaxDomainIDLength, true); err != nil {
		return err
	}
	if d.Kind, err = normalizeDomainName(d.Kind, fmt.Sprintf("recovery drill %d kind", index), MaxDomainNameLength, true); err != nil {
		return err
	}
	if !validRecoveryDrillKind(d.Kind) {
		return fmt.Errorf("recovery drill %q has unsupported kind %q", d.ID, d.Kind)
	}
	d.Outcome = strings.ToLower(strings.TrimSpace(d.Outcome))
	if err := normalizeQualificationOutcome(d.Outcome); err != nil {
		return fmt.Errorf("recovery drill %q: %w", d.ID, err)
	}
	if d.TargetHash, err = normalizeDomainHash(d.TargetHash, fmt.Sprintf("recovery drill %q target_hash", d.ID), true); err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"evidence_hash":       &d.EvidenceHash,
		"source_fingerprint":  &d.SourceFingerprint,
		"restore_fingerprint": &d.RestoreFingerprint,
		"replay_hash":         &d.ReplayHash,
		"failover_from_hash":  &d.FailoverFromHash,
		"failover_to_hash":    &d.FailoverToHash,
	} {
		if *value, err = normalizeDomainHash(*value, fmt.Sprintf("recovery drill %q %s", d.ID, field), false); err != nil {
			return err
		}
	}
	if d.RPOSeconds < 0 || d.RTOSeconds < 0 || d.RPOSeconds > 365*24*60*60 || d.RTOSeconds > 365*24*60*60 {
		return fmt.Errorf("recovery drill %q recovery objective is outside bounds", d.ID)
	}
	if !d.RPOMeasured && d.RPOSeconds != 0 || !d.RTOMeasured && d.RTOSeconds != 0 {
		return fmt.Errorf("recovery drill %q has an objective value without measured=true", d.ID)
	}
	if d.ObservedAt.IsZero() {
		return fmt.Errorf("recovery drill %q observed_at is required", d.ID)
	}
	d.ObservedAt = d.ObservedAt.UTC()
	if d.Outcome == QualificationOutcomePassed && d.EvidenceHash == "" {
		return fmt.Errorf("recovery drill %q passed without evidence_hash", d.ID)
	}
	switch d.Kind {
	case RecoveryDrillBackupRestore:
		if d.Outcome == QualificationOutcomePassed && (d.SourceFingerprint == "" || d.RestoreFingerprint == "") {
			return fmt.Errorf("recovery drill %q passed without source and restore fingerprints", d.ID)
		}
	case RecoveryDrillPITR:
		if d.Outcome == QualificationOutcomePassed && (d.SourceFingerprint == "" || d.RestoreFingerprint == "" || d.ReplayHash == "" || !d.WALArchiveVerified) {
			return fmt.Errorf("recovery drill %q passed without WAL, restore, and replay evidence", d.ID)
		}
	case RecoveryDrillFailover:
		if d.Outcome == QualificationOutcomePassed && (d.FailoverFromHash == "" || d.FailoverToHash == "") {
			return fmt.Errorf("recovery drill %q passed without failover identity evidence", d.ID)
		}
	}
	return nil
}

// QualificationReport is the common redacted envelope for provider,
// authority, certificate, topology, recovery, retention, and load evidence.
// It is process-local unless a caller stores it through an existing artifact
// path; it is never the authority for the system being qualified.
type QualificationReport struct {
	SchemaVersion    int                             `json:"schema_version"`
	RunID            string                          `json:"run_id"`
	WorkspaceID      string                          `json:"workspace_id,omitempty"`
	TargetHash       string                          `json:"target_hash"`
	Outcome          string                          `json:"outcome"`
	Cases            []QualificationCase             `json:"cases"`
	BoundaryEvidence []BoundaryQualificationEvidence `json:"boundary_evidence,omitempty"`
	RecoveryDrills   []RecoveryDrill                 `json:"recovery_drills,omitempty"`
	StartedAt        time.Time                       `json:"started_at,omitempty"`
	FinishedAt       time.Time                       `json:"finished_at,omitempty"`
	ReportHash       string                          `json:"report_hash"`
}

// QualificationCheckManifest records only deterministic check metadata. It
// intentionally contains no check output, prompt, endpoint, credential, or
// provider payload.
type QualificationCheckManifest struct {
	Name          string `json:"name"`
	Version       string `json:"version"`
	Category      string `json:"category"`
	ExecutionMode string `json:"execution_mode"`
	InputHash     string `json:"input_hash"`
	Outcome       string `json:"outcome"`
	EvidenceHash  string `json:"evidence_hash,omitempty"`
}

// QualificationManifest binds a report to the exact portable checks that
// produced it. EnvironmentNames contains names only; values are never
// representable in this contract.
type QualificationManifest struct {
	SchemaVersion    int                          `json:"schema_version"`
	RunID            string                       `json:"run_id"`
	WorkspaceID      string                       `json:"workspace_id,omitempty"`
	TargetHash       string                       `json:"target_hash"`
	ReportHash       string                       `json:"report_hash"`
	RunnerVersion    string                       `json:"runner_version"`
	CommitHash       string                       `json:"commit_hash,omitempty"`
	EnvironmentNames []string                     `json:"environment_names,omitempty"`
	Checks           []QualificationCheckManifest `json:"checks"`
	ManifestHash     string                       `json:"manifest_hash"`
}

// QualificationBundle is a bounded, portable evidence file. It is not a
// system authority; it is a redacted proof envelope that can be validated
// offline or imported by a deployment-owned operator.
type QualificationBundle struct {
	Report   QualificationReport   `json:"report"`
	Manifest QualificationManifest `json:"manifest"`
}

func (m *QualificationCheckManifest) normalize(index int) error {
	if m == nil {
		return fmt.Errorf("qualification manifest check %d is nil", index)
	}
	var err error
	if m.Name, err = normalizeDomainIdentifier(m.Name, fmt.Sprintf("qualification manifest check %d name", index), MaxDomainNameLength, true); err != nil {
		return err
	}
	if m.Version, err = normalizeDomainVersion(m.Version, fmt.Sprintf("qualification manifest check %q version", m.Name), true); err != nil {
		return err
	}
	if m.Category, err = normalizeDomainName(m.Category, fmt.Sprintf("qualification manifest check %q category", m.Name), MaxDomainNameLength, true); err != nil {
		return err
	}
	if !validQualificationCategory(m.Category) {
		return fmt.Errorf("qualification manifest check %q has unsupported category %q", m.Name, m.Category)
	}
	m.ExecutionMode = strings.ToLower(strings.TrimSpace(m.ExecutionMode))
	if m.ExecutionMode == "" {
		m.ExecutionMode = "offline"
	}
	if m.ExecutionMode != "offline" && m.ExecutionMode != "external" {
		return fmt.Errorf("qualification manifest check %q has unsupported execution_mode", m.Name)
	}
	if m.InputHash, err = normalizeDomainHash(m.InputHash, fmt.Sprintf("qualification manifest check %q input_hash", m.Name), true); err != nil {
		return err
	}
	m.Outcome = strings.ToLower(strings.TrimSpace(m.Outcome))
	if err := normalizeQualificationOutcome(m.Outcome); err != nil {
		return err
	}
	if m.EvidenceHash, err = normalizeDomainHash(m.EvidenceHash, fmt.Sprintf("qualification manifest check %q evidence_hash", m.Name), false); err != nil {
		return err
	}
	return nil
}

// Normalize validates and hashes the portable manifest. The hash excludes
// manifest_hash itself and all wall-clock metadata.
func (m *QualificationManifest) Normalize() error {
	if m == nil {
		return fmt.Errorf("qualification manifest is nil")
	}
	if m.SchemaVersion == 0 {
		m.SchemaVersion = QualificationManifestSchemaVersion
	}
	if m.SchemaVersion != QualificationManifestSchemaVersion {
		return fmt.Errorf("unsupported qualification manifest schema_version %d", m.SchemaVersion)
	}
	var err error
	if m.RunID, err = normalizeDomainIdentifier(m.RunID, "qualification manifest run_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if m.WorkspaceID != "" {
		if m.WorkspaceID, err = normalizeDomainWorkspace(m.WorkspaceID); err != nil {
			return err
		}
	}
	if m.TargetHash, err = normalizeDomainHash(m.TargetHash, "qualification manifest target_hash", true); err != nil {
		return err
	}
	if m.ReportHash, err = normalizeDomainHash(m.ReportHash, "qualification manifest report_hash", true); err != nil {
		return err
	}
	if m.RunnerVersion, err = normalizeDomainVersion(m.RunnerVersion, "qualification manifest runner_version", true); err != nil {
		return err
	}
	if m.CommitHash, err = normalizeDomainHash(m.CommitHash, "qualification manifest commit_hash", false); err != nil {
		return err
	}
	if len(m.EnvironmentNames) > MaxQualificationEnvironmentNames {
		return fmt.Errorf("qualification manifest has too many environment names")
	}
	for index := range m.EnvironmentNames {
		value := strings.TrimSpace(m.EnvironmentNames[index])
		if !validQualificationEnvironmentName(value) {
			return fmt.Errorf("qualification manifest environment name %d is invalid", index)
		}
		m.EnvironmentNames[index] = value
	}
	sort.Strings(m.EnvironmentNames)
	for index := 1; index < len(m.EnvironmentNames); index++ {
		if m.EnvironmentNames[index] == m.EnvironmentNames[index-1] {
			return fmt.Errorf("qualification manifest repeats environment name %q", m.EnvironmentNames[index])
		}
	}
	if len(m.Checks) == 0 || len(m.Checks) > MaxQualificationManifestChecks {
		return fmt.Errorf("qualification manifest must contain between 1 and %d checks", MaxQualificationManifestChecks)
	}
	seen := make(map[string]struct{}, len(m.Checks))
	for index := range m.Checks {
		if err := m.Checks[index].normalize(index); err != nil {
			return err
		}
		key := m.Checks[index].Category + "\x00" + m.Checks[index].Name
		if _, exists := seen[key]; exists {
			return fmt.Errorf("qualification manifest repeats check %q", m.Checks[index].Name)
		}
		seen[key] = struct{}{}
	}
	sort.Slice(m.Checks, func(i, j int) bool {
		if m.Checks[i].Category != m.Checks[j].Category {
			return m.Checks[i].Category < m.Checks[j].Category
		}
		return m.Checks[i].Name < m.Checks[j].Name
	})
	calculated := qualificationManifestHash(*m)
	if m.ManifestHash != "" && strings.ToLower(strings.TrimSpace(m.ManifestHash)) != calculated {
		return fmt.Errorf("qualification manifest_hash does not match manifest contents")
	}
	m.ManifestHash = calculated
	raw, marshalErr := json.Marshal(m)
	if marshalErr != nil {
		return marshalErr
	}
	if len(raw) > MaxQualificationReportBytes {
		return fmt.Errorf("qualification manifest exceeds %d bytes", MaxQualificationReportBytes)
	}
	return nil
}

// Normalize validates the report/manifest relationship.
func (b *QualificationBundle) Normalize() error {
	if b == nil {
		return fmt.Errorf("qualification bundle is nil")
	}
	if err := b.Report.Normalize(); err != nil {
		return err
	}
	if err := b.Manifest.Normalize(); err != nil {
		return err
	}
	if b.Manifest.RunID != b.Report.RunID || b.Manifest.WorkspaceID != b.Report.WorkspaceID ||
		b.Manifest.TargetHash != b.Report.TargetHash || b.Manifest.ReportHash != b.Report.ReportHash {
		return fmt.Errorf("qualification bundle report and manifest do not match")
	}
	return nil
}

// Normalize validates and deterministically orders a qualification report.
// Timestamps and case duration are retained for operators but excluded from
// ReportHash so replay compares evidence rather than wall-clock noise.
func (r *QualificationReport) Normalize() error {
	if r == nil {
		return fmt.Errorf("qualification report is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = QualificationSchemaVersion
	}
	if r.SchemaVersion != QualificationSchemaVersion {
		return fmt.Errorf("unsupported qualification schema_version %d", r.SchemaVersion)
	}
	var err error
	if r.RunID, err = normalizeDomainIdentifier(r.RunID, "qualification run_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.WorkspaceID != "" {
		if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
			return err
		}
	}
	if r.TargetHash, err = normalizeDomainHash(r.TargetHash, "qualification target_hash", true); err != nil {
		return err
	}
	r.Outcome = strings.ToLower(strings.TrimSpace(r.Outcome))
	if err := normalizeQualificationOutcome(r.Outcome); err != nil {
		return err
	}
	if !r.StartedAt.IsZero() && !r.FinishedAt.IsZero() && r.FinishedAt.Before(r.StartedAt) {
		return fmt.Errorf("qualification report finished_at precedes started_at")
	}
	if len(r.Cases) > MaxQualificationCases {
		return fmt.Errorf("qualification report has too many cases")
	}
	caseKeys := make(map[string]struct{}, len(r.Cases))
	for index := range r.Cases {
		if err := r.Cases[index].normalize(index); err != nil {
			return err
		}
		key := r.Cases[index].Category + "\x00" + r.Cases[index].Name
		if _, exists := caseKeys[key]; exists {
			return fmt.Errorf("qualification report repeats case %q", r.Cases[index].Name)
		}
		caseKeys[key] = struct{}{}
	}
	if len(r.BoundaryEvidence) > MaxBoundaryQualificationEvidence {
		return fmt.Errorf("qualification report has too many boundary evidence items")
	}
	boundaryIDs := make(map[string]struct{}, len(r.BoundaryEvidence))
	for index := range r.BoundaryEvidence {
		if err := r.BoundaryEvidence[index].normalize(index); err != nil {
			return err
		}
		if _, exists := boundaryIDs[r.BoundaryEvidence[index].ID]; exists {
			return fmt.Errorf("qualification report repeats boundary evidence %q", r.BoundaryEvidence[index].ID)
		}
		boundaryIDs[r.BoundaryEvidence[index].ID] = struct{}{}
	}
	if len(r.RecoveryDrills) > MaxQualificationDrills {
		return fmt.Errorf("qualification report has too many recovery drills")
	}
	drillIDs := make(map[string]struct{}, len(r.RecoveryDrills))
	for index := range r.RecoveryDrills {
		if err := r.RecoveryDrills[index].normalize(index); err != nil {
			return err
		}
		if r.RecoveryDrills[index].TargetHash != r.TargetHash {
			return fmt.Errorf("recovery drill %q crosses qualification target", r.RecoveryDrills[index].ID)
		}
		if _, exists := drillIDs[r.RecoveryDrills[index].ID]; exists {
			return fmt.Errorf("qualification report repeats recovery drill %q", r.RecoveryDrills[index].ID)
		}
		drillIDs[r.RecoveryDrills[index].ID] = struct{}{}
	}
	sort.Slice(r.Cases, func(i, j int) bool {
		if r.Cases[i].Category != r.Cases[j].Category {
			return r.Cases[i].Category < r.Cases[j].Category
		}
		return r.Cases[i].Name < r.Cases[j].Name
	})
	sort.Slice(r.BoundaryEvidence, func(i, j int) bool { return r.BoundaryEvidence[i].ID < r.BoundaryEvidence[j].ID })
	sort.Slice(r.RecoveryDrills, func(i, j int) bool { return r.RecoveryDrills[i].ID < r.RecoveryDrills[j].ID })
	if r.Outcome == QualificationOutcomePassed {
		for _, item := range r.Cases {
			if item.Outcome != QualificationOutcomePassed {
				return fmt.Errorf("qualification report passed with non-passing case %q", item.Name)
			}
		}
		for _, item := range r.BoundaryEvidence {
			if item.Outcome != QualificationOutcomePassed {
				return fmt.Errorf("qualification report passed with non-passing boundary evidence %q", item.ID)
			}
		}
		for _, drill := range r.RecoveryDrills {
			if drill.Outcome != QualificationOutcomePassed {
				return fmt.Errorf("qualification report passed with non-passing recovery drill %q", drill.ID)
			}
		}
	}
	if !r.StartedAt.IsZero() {
		r.StartedAt = r.StartedAt.UTC()
	}
	if !r.FinishedAt.IsZero() {
		r.FinishedAt = r.FinishedAt.UTC()
	}
	calculated := qualificationReportHash(*r)
	if r.ReportHash != "" && strings.ToLower(strings.TrimSpace(r.ReportHash)) != calculated {
		return fmt.Errorf("qualification report_hash does not match report contents")
	}
	r.ReportHash = calculated
	raw, marshalErr := json.Marshal(r)
	if marshalErr != nil {
		return marshalErr
	}
	if len(raw) > MaxQualificationReportBytes {
		return fmt.Errorf("qualification report exceeds %d bytes", MaxQualificationReportBytes)
	}
	return nil
}

// StableHash returns the normalized report identity. Invalid reports return
// an empty string so callers cannot accidentally treat malformed evidence as
// qualified.
func (r QualificationReport) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	return r.ReportHash
}

// BoundaryEvidenceHash is the stable identity of the ordered boundary
// observations in this report. An empty result means the report is invalid or
// contains no boundary observations.
func (r QualificationReport) BoundaryEvidenceHash() string {
	if err := r.Normalize(); err != nil || len(r.BoundaryEvidence) == 0 {
		return ""
	}
	parts := []string{"qualification-boundary-evidence-set", r.TargetHash}
	for _, item := range r.BoundaryEvidence {
		parts = append(parts, item.EvidenceHash)
	}
	return HashStrings(parts...)
}

// ExternalBoundaryEvidenceHash returns the exact boundary authority hash
// when the report contains one and only one observation. This is the
// unambiguous form required by external-effect release admission.
func (r QualificationReport) ExternalBoundaryEvidenceHash() string {
	if err := r.Normalize(); err != nil || len(r.BoundaryEvidence) != 1 {
		return ""
	}
	return r.BoundaryEvidence[0].BoundaryHash
}

func normalizeQualificationOutcome(value string) error {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case QualificationOutcomePassed, QualificationOutcomeFailed, QualificationOutcomeBlocked, QualificationOutcomeSkipped:
		return nil
	default:
		return fmt.Errorf("invalid qualification outcome %q", value)
	}
}

func validQualificationCategory(value string) bool {
	switch value {
	case QualificationCategoryAdapter, QualificationCategoryAuthority, QualificationCategoryCertificate, QualificationCategoryExternalBoundary, QualificationCategorySandbox, QualificationCategoryTopology, QualificationCategoryRecovery, QualificationCategoryRetention, QualificationCategoryLoad:
		return true
	default:
		return false
	}
}

func validBoundaryQualificationKind(value string) bool {
	switch value {
	case BoundaryQualificationCredentialResolution, BoundaryQualificationWorkloadIdentity, BoundaryQualificationMTLS, BoundaryQualificationDNSRebinding, BoundaryQualificationProxyFirewall, BoundaryQualificationProviderIdempotency, BoundaryQualificationExternalRecovery:
		return true
	default:
		return false
	}
}

func validRecoveryDrillKind(value string) bool {
	switch value {
	case RecoveryDrillBackupRestore, RecoveryDrillPITR, RecoveryDrillFailover, RecoveryDrillPartitionMaintenance, RecoveryDrillIdentityRotation:
		return true
	default:
		return false
	}
}

func validQualificationEnvironmentName(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > MaxDomainNameLength {
		return false
	}
	for index, char := range value {
		if (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return false
	}
	return true
}

func qualificationReportHash(report QualificationReport) string {
	parts := []string{"qualification-report", fmt.Sprint(report.SchemaVersion), report.RunID, report.WorkspaceID, report.TargetHash, report.Outcome}
	for _, item := range report.Cases {
		parts = append(parts, item.Category, item.Name, item.Outcome, item.ErrorCode, item.EvidenceHash)
		for _, measurement := range item.Measurements {
			parts = append(parts, measurement.Name, fmt.Sprint(measurement.Value), measurement.Unit, fmt.Sprint(measurement.Present))
		}
	}
	for _, item := range report.BoundaryEvidence {
		parts = append(parts, item.ID, item.Kind, item.Outcome, item.BoundaryHash, item.CredentialSourceHash, item.IdentityHash, item.ProviderRequestHash, item.ProviderResponseHash, item.SourceFingerprint, item.EvidenceHash, fmt.Sprint(item.Measured), item.ObservedAt.UTC().Format(time.RFC3339Nano))
		if !item.ExpiresAt.IsZero() {
			parts = append(parts, item.ExpiresAt.UTC().Format(time.RFC3339Nano))
		}
	}
	for _, drill := range report.RecoveryDrills {
		parts = append(parts, drill.ID, drill.Kind, drill.Outcome, drill.TargetHash, drill.EvidenceHash, drill.SourceFingerprint, drill.RestoreFingerprint, drill.ReplayHash, drill.FailoverFromHash, drill.FailoverToHash, fmt.Sprint(drill.WALArchiveVerified), fmt.Sprint(drill.RPOSeconds), fmt.Sprint(drill.RPOMeasured), fmt.Sprint(drill.RTOSeconds), fmt.Sprint(drill.RTOMeasured))
	}
	return HashStrings(parts...)
}

func qualificationManifestHash(manifest QualificationManifest) string {
	parts := []string{"qualification-manifest", fmt.Sprint(manifest.SchemaVersion), manifest.RunID, manifest.WorkspaceID, manifest.TargetHash, manifest.ReportHash, manifest.RunnerVersion, manifest.CommitHash}
	for _, name := range manifest.EnvironmentNames {
		parts = append(parts, name)
	}
	for _, check := range manifest.Checks {
		parts = append(parts, check.Name, check.Version, check.Category, check.ExecutionMode, check.InputHash, check.Outcome, check.EvidenceHash)
	}
	return HashStrings(parts...)
}
