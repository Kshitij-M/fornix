package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ValidationSchemaVersion is the durable contract version for deterministic
// post-change validation and re-index handoffs.
const ValidationSchemaVersion = 1

const (
	DefaultValidationMaxValidators = 16
	DefaultValidationMaxFiles      = 10000
	DefaultValidationMaxBytes      = 512 << 20
	DefaultValidationMaxOutput     = 1 << 20
	DefaultValidationMaxWallTime   = 15 * time.Minute
	DefaultValidationMaxSQLQueries = 512
	DefaultValidationMaxRetries    = 2
	DefaultValidationMaxReport     = 64 << 10
	MaxValidationValidators        = 64
	MaxValidationFiles             = 100000
	MaxValidationBytes             = 1 << 30
	MaxValidationOutputBytes       = 16 << 20
	MaxValidationWallTime          = 24 * time.Hour
	MaxValidationSQLQueries        = 10000
	MaxValidationRetries           = 10
	MaxValidationReportBytes       = 1 << 20
	MaxValidationChecks            = 64
	MaxValidationEvidence          = 128
	MaxValidationDisclosureBytes   = 1 << 20
	MaxValidationDisclosureItems   = 128
)

const (
	ValidationPending          = "pending"
	ValidationRunning          = "running"
	ValidationPassed           = "passed"
	ValidationFailed           = "failed"
	ValidationAbstained        = "abstained"
	ValidationCancelled        = "cancelled"
	ValidationRecoveryRequired = "recovery_required"
)

const (
	ValidationOutcomePassed    = "passed"
	ValidationOutcomeFailed    = "failed"
	ValidationOutcomeAbstained = "abstained"
	ValidationOutcomeSkipped   = "skipped"
)

const (
	ValidationFailureInvalidRequest = "invalid_request"
	ValidationFailureUnauthorized   = "unauthorized"
	ValidationFailureWorkspace      = "workspace_isolation"
	ValidationFailureChangeMissing  = "change_missing"
	ValidationFailureSourceConflict = "source_conflict"
	ValidationFailureUnsafePath     = "unsafe_path"
	ValidationFailureValidator      = "validator_failure"
	ValidationFailureBudget         = "budget_exceeded"
	ValidationFailureStaleFence     = "stale_fence"
	ValidationFailureCancelled      = "cancelled"
	ValidationFailureRecovery       = "recovery_required"
	ValidationFailureEvidence       = "evidence_integrity"
	ValidationFailureInProgress     = "in_progress"
)

const (
	ReindexHandoffPending   = "pending"
	ReindexHandoffSubmitted = "submitted"
	ReindexHandoffRunning   = "running"
	ReindexHandoffSucceeded = "succeeded"
	ReindexHandoffFailed    = "failed"
	ReindexHandoffCancelled = "cancelled"
)

const (
	ValidationValidatorPreconditions = "change.preconditions"
	ValidationValidatorFiles         = "change.files"
	ValidationValidatorSafety        = "change.safety"
	ValidationValidatorTree          = "change.tree"
	ValidationValidatorReindex       = "change.reindex"
)

// ValidatorRef identifies an explicitly registered validator. Validator IDs
// and versions are stable, bounded, and never contain executable code.
type ValidatorRef struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// ValidationBudget contains hard limits for one validation run. Units are
// files, bytes, milliseconds, SQL statements, retries, and report bytes.
type ValidationBudget struct {
	MaxValidators  int   `json:"max_validators"`
	MaxFiles       int   `json:"max_files"`
	MaxBytes       int64 `json:"max_bytes"`
	MaxOutputBytes int64 `json:"max_output_bytes"`
	MaxWallTimeMS  int64 `json:"max_wall_time_ms"`
	MaxSQLQueries  int   `json:"max_sql_queries"`
	MaxRetries     int   `json:"max_retries"`
	MaxReportBytes int   `json:"max_report_bytes"`
}

// DefaultValidationBudget returns conservative limits for offline validation.
func DefaultValidationBudget() ValidationBudget {
	return ValidationBudget{
		MaxValidators: DefaultValidationMaxValidators, MaxFiles: DefaultValidationMaxFiles,
		MaxBytes: DefaultValidationMaxBytes, MaxOutputBytes: DefaultValidationMaxOutput,
		MaxWallTimeMS: DefaultValidationMaxWallTime.Milliseconds(), MaxSQLQueries: DefaultValidationMaxSQLQueries,
		MaxRetries: DefaultValidationMaxRetries, MaxReportBytes: DefaultValidationMaxReport,
	}
}

// Normalize fills safe budget defaults and rejects values outside the
// supported validation envelope.
func (b *ValidationBudget) Normalize() error {
	if b == nil {
		return fmt.Errorf("validation budget is nil")
	}
	defaults := DefaultValidationBudget()
	if b.MaxValidators == 0 {
		b.MaxValidators = defaults.MaxValidators
	}
	if b.MaxFiles == 0 {
		b.MaxFiles = defaults.MaxFiles
	}
	if b.MaxBytes == 0 {
		b.MaxBytes = defaults.MaxBytes
	}
	if b.MaxOutputBytes == 0 {
		b.MaxOutputBytes = defaults.MaxOutputBytes
	}
	if b.MaxWallTimeMS == 0 {
		b.MaxWallTimeMS = defaults.MaxWallTimeMS
	}
	if b.MaxSQLQueries == 0 {
		b.MaxSQLQueries = defaults.MaxSQLQueries
	}
	if b.MaxRetries == 0 {
		b.MaxRetries = defaults.MaxRetries
	}
	if b.MaxReportBytes == 0 {
		b.MaxReportBytes = defaults.MaxReportBytes
	}
	if b.MaxValidators < 1 || b.MaxValidators > MaxValidationValidators ||
		b.MaxFiles < 1 || b.MaxFiles > MaxValidationFiles ||
		b.MaxBytes < 1 || b.MaxBytes > MaxValidationBytes ||
		b.MaxOutputBytes < 1 || b.MaxOutputBytes > MaxValidationOutputBytes ||
		b.MaxWallTimeMS < 1 || time.Duration(b.MaxWallTimeMS)*time.Millisecond > MaxValidationWallTime ||
		b.MaxSQLQueries < 1 || b.MaxSQLQueries > MaxValidationSQLQueries ||
		b.MaxRetries < 0 || b.MaxRetries > MaxValidationRetries ||
		b.MaxReportBytes < 1 || b.MaxReportBytes > MaxValidationReportBytes {
		return fmt.Errorf("validation budget is outside supported bounds")
	}
	return nil
}

// ValidationRequest is the authenticated input for one validation identity.
// ChangeApplicationID and PacketHash bind validation to one approved result.
type ValidationRequest struct {
	SchemaVersion       int              `json:"schema_version,omitempty"`
	ID                  string           `json:"id,omitempty"`
	RequestID           string           `json:"request_id,omitempty"`
	IdempotencyKey      string           `json:"idempotency_key"`
	CausationID         string           `json:"causation_id,omitempty"`
	CorrelationID       string           `json:"correlation_id,omitempty"`
	WorkspaceID         string           `json:"workspace_id"`
	Actor               ActorRef         `json:"actor,omitempty"`
	Task                *EntityRef       `json:"task,omitempty"`
	Session             *EntityRef       `json:"session,omitempty"`
	AgentRun            *EntityRef       `json:"agent_run,omitempty"`
	TaskOwnerID         string           `json:"task_owner_id,omitempty"`
	TaskFence           uint64           `json:"task_fence,omitempty"`
	ChangeApplicationID string           `json:"change_application_id"`
	ProposalID          string           `json:"proposal_id"`
	PacketHash          string           `json:"packet_hash"`
	ExpectedTreeHash    string           `json:"expected_tree_hash"`
	Repository          string           `json:"repository"`
	Source              RepositorySource `json:"source"`
	SourceManifestHash  string           `json:"source_manifest_hash,omitempty"`
	Validators          []ValidatorRef   `json:"validators,omitempty"`
	Budget              ValidationBudget `json:"budget,omitempty"`
	DryRun              bool             `json:"dry_run,omitempty"`
}

// ValidationPlan is the deterministic validator plan derived from a request.
type ValidationPlan struct {
	SchemaVersion int              `json:"schema_version"`
	WorkspaceID   string           `json:"workspace_id"`
	RequestHash   string           `json:"request_hash"`
	Validators    []ValidatorRef   `json:"validators"`
	Budget        ValidationBudget `json:"budget"`
}

// ValidationFailure is a stable, redacted failure classification. Message is
// bounded diagnostic text and must never contain secrets or raw prompts.
type ValidationFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ValidationEvidence is a hash-only source reference supporting one result.
// Raw evidence is disclosed through the existing EvidenceStore/ArtifactStore.
type ValidationEvidence struct {
	Kind            string `json:"kind"`
	SourceReference string `json:"source_reference"`
	Hash            string `json:"hash"`
	Role            string `json:"role,omitempty"`
}

// ValidationResult is the immutable result of one registered validator. It
// contains bounded measurements and references, never unbounded output.
type ValidationResult struct {
	SchemaVersion  int                  `json:"schema_version"`
	ID             string               `json:"id"`
	RunID          string               `json:"run_id"`
	WorkspaceID    string               `json:"workspace_id"`
	Ordinal        int                  `json:"ordinal"`
	Validator      ValidatorRef         `json:"validator"`
	Attempt        int                  `json:"attempt"`
	Status         string               `json:"status"`
	Outcome        string               `json:"outcome"`
	InputHash      string               `json:"input_hash"`
	ResultHash     string               `json:"result_hash"`
	Summary        string               `json:"summary,omitempty"`
	Failure        *ValidationFailure   `json:"failure,omitempty"`
	Evidence       []ValidationEvidence `json:"evidence,omitempty"`
	OutputArtifact *ArtifactRef         `json:"output_artifact,omitempty"`
	Files          int                  `json:"files,omitempty"`
	Bytes          int64                `json:"bytes,omitempty"`
	SQLQueries     int                  `json:"sql_queries,omitempty"`
	DurationMS     int64                `json:"duration_ms,omitempty"`
	CreatedAt      time.Time            `json:"created_at"`
}

// ValidationReport is the bounded deterministic summary of one validation
// run. Detailed validator output belongs in content-addressed artifacts.
type ValidationReport struct {
	SchemaVersion    int                `json:"schema_version"`
	RunID            string             `json:"run_id"`
	WorkspaceID      string             `json:"workspace_id"`
	Status           string             `json:"status"`
	Outcome          string             `json:"outcome"`
	PacketHash       string             `json:"packet_hash"`
	ExpectedTreeHash string             `json:"expected_tree_hash"`
	ObservedTreeHash string             `json:"observed_tree_hash,omitempty"`
	ResultCount      int                `json:"result_count"`
	PassedCount      int                `json:"passed_count"`
	FailedCount      int                `json:"failed_count"`
	AbstainedCount   int                `json:"abstained_count"`
	Files            int                `json:"files,omitempty"`
	Bytes            int64              `json:"bytes,omitempty"`
	SQLQueries       int                `json:"sql_queries,omitempty"`
	DurationMS       int64              `json:"duration_ms,omitempty"`
	Results          []ValidationResult `json:"results,omitempty"`
	ReportHash       string             `json:"report_hash"`
	ReplayHash       string             `json:"replay_hash,omitempty"`
	LastError        string             `json:"last_error,omitempty"`
}

// ValidationRun is the durable lifecycle and replay identity of a validation.
// Its result history is append-only; Status is an operational current view.
type ValidationRun struct {
	SchemaVersion       int               `json:"schema_version"`
	ID                  string            `json:"id"`
	RequestID           string            `json:"request_id"`
	IdempotencyKey      string            `json:"idempotency_key"`
	RequestHash         string            `json:"request_hash"`
	WorkspaceID         string            `json:"workspace_id"`
	ChangeApplicationID string            `json:"change_application_id"`
	ProposalID          string            `json:"proposal_id"`
	PacketHash          string            `json:"packet_hash"`
	ExpectedTreeHash    string            `json:"expected_tree_hash"`
	ObservedTreeHash    string            `json:"observed_tree_hash,omitempty"`
	SourceManifestHash  string            `json:"source_manifest_hash,omitempty"`
	Repository          string            `json:"repository"`
	SourceRoot          string            `json:"source_root"`
	Actor               ActorRef          `json:"actor"`
	Task                *EntityRef        `json:"task,omitempty"`
	Session             *EntityRef        `json:"session,omitempty"`
	AgentRun            *EntityRef        `json:"agent_run,omitempty"`
	TaskOwnerID         string            `json:"task_owner_id,omitempty"`
	TaskFence           uint64            `json:"task_fence,omitempty"`
	Plan                ValidationPlan    `json:"plan"`
	Budget              ValidationBudget  `json:"budget"`
	Status              string            `json:"status"`
	Outcome             string            `json:"outcome,omitempty"`
	DryRun              bool              `json:"dry_run,omitempty"`
	ResultCount         int               `json:"result_count"`
	PassedCount         int               `json:"passed_count"`
	FailedCount         int               `json:"failed_count"`
	AbstainedCount      int               `json:"abstained_count"`
	Report              *ValidationReport `json:"report,omitempty"`
	ReportArtifact      *ArtifactRef      `json:"report_artifact,omitempty"`
	ReportHash          string            `json:"report_hash,omitempty"`
	ReplayHash          string            `json:"replay_hash,omitempty"`
	LastError           string            `json:"last_error,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
	StartedAt           *time.Time        `json:"started_at,omitempty"`
	FinishedAt          *time.Time        `json:"finished_at,omitempty"`
}

// ReindexHandoff is a durable request to create the next repository source
// snapshot. It never overwrites the source that was validated or indexed.
type ReindexHandoff struct {
	SchemaVersion        int                `json:"schema_version"`
	ID                   string             `json:"id"`
	WorkspaceID          string             `json:"workspace_id"`
	RequestID            string             `json:"request_id"`
	IdempotencyKey       string             `json:"idempotency_key"`
	RequestHash          string             `json:"request_hash"`
	ValidationRunID      string             `json:"validation_run_id"`
	ChangeApplicationID  string             `json:"change_application_id"`
	Repository           string             `json:"repository"`
	SourceRoot           string             `json:"source_root"`
	MountRoot            string             `json:"-"`
	PreviousManifestHash string             `json:"previous_manifest_hash,omitempty"`
	ExpectedTreeHash     string             `json:"expected_tree_hash"`
	ObservedTreeHash     string             `json:"observed_tree_hash"`
	ManifestHash         string             `json:"manifest_hash,omitempty"`
	IngestJobID          string             `json:"ingest_job_id,omitempty"`
	Status               string             `json:"status"`
	Actor                ActorRef           `json:"actor"`
	Task                 *EntityRef         `json:"task,omitempty"`
	Session              *EntityRef         `json:"session,omitempty"`
	TaskOwnerID          string             `json:"task_owner_id,omitempty"`
	TaskFence            uint64             `json:"task_fence,omitempty"`
	Failure              *ValidationFailure `json:"failure,omitempty"`
	CreatedAt            time.Time          `json:"created_at"`
	UpdatedAt            time.Time          `json:"updated_at"`
	SubmittedAt          *time.Time         `json:"submitted_at,omitempty"`
	CompletedAt          *time.Time         `json:"completed_at,omitempty"`
}

// ValidationReplayRequest selects a recorded validation history. Replay is a
// read-only operation and never executes a validator or external effect.
type ValidationReplayRequest struct {
	WorkspaceID     string `json:"workspace_id"`
	ValidationRunID string `json:"validation_run_id"`
	FromSequence    uint64 `json:"from_sequence,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

// ValidationReplay is a read-only reconstruction from durable results and
// control events. It contains no live filesystem or external-provider output.
type ValidationReplay struct {
	SchemaVersion   int                `json:"schema_version"`
	WorkspaceID     string             `json:"workspace_id"`
	ValidationRunID string             `json:"validation_run_id"`
	Run             ValidationRun      `json:"run"`
	Results         []ValidationResult `json:"results"`
	Events          []EventEnvelope    `json:"events,omitempty"`
	ReplayHash      string             `json:"replay_hash"`
}

// ValidationDisclosureRequest selects a bounded run/report view.
type ValidationDisclosureRequest struct {
	WorkspaceID     string `json:"workspace_id"`
	ValidationRunID string `json:"validation_run_id"`
	Level           string `json:"level,omitempty"`
	MaxBytes        int    `json:"max_bytes,omitempty"`
	MaxItems        int    `json:"max_items,omitempty"`
}

// ValidationDisclosureResult is a redacted hash-preserving view of a run.
type ValidationDisclosureResult struct {
	SchemaVersion   int               `json:"schema_version"`
	WorkspaceID     string            `json:"workspace_id"`
	ValidationRunID string            `json:"validation_run_id"`
	Level           string            `json:"level"`
	Status          string            `json:"status"`
	Outcome         string            `json:"outcome,omitempty"`
	PacketHash      string            `json:"packet_hash"`
	ReportHash      string            `json:"report_hash,omitempty"`
	ReplayHash      string            `json:"replay_hash,omitempty"`
	Report          *ValidationReport `json:"report,omitempty"`
	ReportArtifact  *ArtifactRef      `json:"report_artifact,omitempty"`
	Truncated       bool              `json:"truncated"`
	TotalBytes      int               `json:"total_bytes"`
	TotalItems      int               `json:"total_items"`
	ContentViewHash string            `json:"content_view_hash"`
}

// Normalize validates the workspace-bound change identity and hard budgets.
func (r *ValidationRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("validation request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ValidationSchemaVersion
	}
	if r.SchemaVersion != ValidationSchemaVersion {
		return fmt.Errorf("unsupported validation schema_version %d", r.SchemaVersion)
	}
	r.ID, r.RequestID, r.IdempotencyKey = strings.TrimSpace(r.ID), strings.TrimSpace(r.RequestID), strings.TrimSpace(r.IdempotencyKey)
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.ChangeApplicationID, r.ProposalID = strings.TrimSpace(r.ChangeApplicationID), strings.TrimSpace(r.ProposalID)
	r.PacketHash, r.ExpectedTreeHash = strings.ToLower(strings.TrimSpace(r.PacketHash)), strings.ToLower(strings.TrimSpace(r.ExpectedTreeHash))
	r.Repository = strings.TrimSpace(r.Repository)
	r.SourceManifestHash = strings.ToLower(strings.TrimSpace(r.SourceManifestHash))
	if r.ID == "" {
		r.ID = NewID("validation")
	}
	if r.RequestID == "" {
		r.RequestID = NewID("validation-request")
	}
	if r.WorkspaceID == "" || r.IdempotencyKey == "" || r.ChangeApplicationID == "" || r.ProposalID == "" || r.Repository == "" {
		return fmt.Errorf("workspace_id, idempotency_key, change_application_id, proposal_id, and repository are required")
	}
	if len(r.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("idempotency_key is too large")
	}
	for name, value := range map[string]string{"packet_hash": r.PacketHash, "expected_tree_hash": r.ExpectedTreeHash} {
		if !validSHA256(value) {
			return fmt.Errorf("%s must be a lowercase SHA-256 hash", name)
		}
	}
	if r.SourceManifestHash != "" && !validSHA256(r.SourceManifestHash) {
		return fmt.Errorf("source_manifest_hash must be a lowercase SHA-256 hash")
	}
	if r.Actor.WorkspaceID == "" {
		r.Actor.WorkspaceID = r.WorkspaceID
	}
	if r.Actor.WorkspaceID != r.WorkspaceID {
		return fmt.Errorf("actor crosses workspace boundary")
	}
	for name, ref := range map[string]*EntityRef{"task": r.Task, "session": r.Session, "agent_run": r.AgentRun} {
		if ref == nil {
			continue
		}
		if ref.WorkspaceID != "" && ref.WorkspaceID != r.WorkspaceID {
			return fmt.Errorf("%s crosses workspace boundary", name)
		}
	}
	if r.Task != nil && (strings.TrimSpace(r.TaskOwnerID) == "" || r.TaskFence == 0) {
		return fmt.Errorf("task-bound validation requires task_owner_id and task_fence")
	}
	if len(r.Validators) == 0 {
		r.Validators = DefaultValidatorRefs()
	}
	if len(r.Validators) > MaxValidationValidators {
		return fmt.Errorf("too many validators")
	}
	seen := make(map[string]struct{}, len(r.Validators))
	for i := range r.Validators {
		r.Validators[i].ID = strings.TrimSpace(r.Validators[i].ID)
		r.Validators[i].Version = strings.TrimSpace(r.Validators[i].Version)
		if r.Validators[i].ID == "" || r.Validators[i].Version == "" {
			return fmt.Errorf("validator %d requires id and version", i)
		}
		key := r.Validators[i].ID + "\x00" + r.Validators[i].Version
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate validator %q", r.Validators[i].ID)
		}
		seen[key] = struct{}{}
	}
	if err := r.Budget.Normalize(); err != nil {
		return err
	}
	if len(r.Validators) > r.Budget.MaxValidators {
		return fmt.Errorf("validator count exceeds budget")
	}
	return nil
}

// DefaultValidatorRefs returns the stable built-in validator plan.
func DefaultValidatorRefs() []ValidatorRef {
	return []ValidatorRef{
		{ID: ValidationValidatorPreconditions, Version: "1"},
		{ID: ValidationValidatorFiles, Version: "1"},
		{ID: ValidationValidatorSafety, Version: "1"},
		{ID: ValidationValidatorTree, Version: "1"},
		{ID: ValidationValidatorReindex, Version: "1"},
	}
}

// RequestHash returns the stable logical identity of a normalized request.
func (r ValidationRequest) RequestHash() string {
	clone := r
	clone.ID, clone.RequestID, clone.IdempotencyKey = "", "", ""
	clone.Actor = ActorRef{}
	clone.DryRun = false
	clone.Validators = append([]ValidatorRef(nil), r.Validators...)
	sort.Slice(clone.Validators, func(i, j int) bool {
		if clone.Validators[i].ID != clone.Validators[j].ID {
			return clone.Validators[i].ID < clone.Validators[j].ID
		}
		return clone.Validators[i].Version < clone.Validators[j].Version
	})
	clone.Source.MountRoot = ""
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// Plan returns the deterministic plan identity for a request.
func (r ValidationRequest) Plan() (ValidationPlan, error) {
	if err := r.Normalize(); err != nil {
		return ValidationPlan{}, err
	}
	validators := append([]ValidatorRef(nil), r.Validators...)
	sort.Slice(validators, func(i, j int) bool {
		if validators[i].ID != validators[j].ID {
			return validators[i].ID < validators[j].ID
		}
		return validators[i].Version < validators[j].Version
	})
	return ValidationPlan{SchemaVersion: ValidationSchemaVersion, WorkspaceID: r.WorkspaceID, RequestHash: r.RequestHash(), Validators: validators, Budget: r.Budget}, nil
}

// StableHash returns a deterministic immutable identity for one check result.
func (r ValidationResult) StableHash() string {
	clone := r
	clone.ID, clone.CreatedAt = "", time.Time{}
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// StableHash returns a deterministic identity for a bounded validation report.
func (r ValidationReport) StableHash() string {
	clone := r
	clone.ReportHash, clone.ReplayHash = "", ""
	clone.Results = append([]ValidationResult(nil), r.Results...)
	for i := range clone.Results {
		clone.Results[i].ID = ""
		clone.Results[i].CreatedAt = time.Time{}
	}
	sort.Slice(clone.Results, func(i, j int) bool { return clone.Results[i].Ordinal < clone.Results[j].Ordinal })
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// StableHash returns a deterministic identity for a re-index handoff.
func (h ReindexHandoff) StableHash() string {
	clone := h
	clone.ID, clone.RequestID, clone.IdempotencyKey, clone.MountRoot = "", "", "", ""
	clone.Actor = ActorRef{}
	clone.CreatedAt, clone.UpdatedAt, clone.SubmittedAt, clone.CompletedAt = time.Time{}, time.Time{}, nil, nil
	clone.Status, clone.IngestJobID, clone.Failure = "", "", nil
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// Normalize validates a bounded disclosure request.
func (r *ValidationDisclosureRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("validation disclosure request is nil")
	}
	r.WorkspaceID, r.ValidationRunID, r.Level = strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.ValidationRunID), strings.ToLower(strings.TrimSpace(r.Level))
	if r.WorkspaceID == "" || r.ValidationRunID == "" {
		return fmt.Errorf("workspace_id and validation_run_id are required")
	}
	if r.Level == "" {
		r.Level = string(DisclosureGist)
	}
	if r.Level != string(DisclosureGist) && r.Level != string(DisclosureDetail) && r.Level != string(DisclosureRaw) {
		return fmt.Errorf("unsupported validation disclosure level %q", r.Level)
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = DefaultDisclosureBytes
	}
	if r.MaxItems == 0 {
		r.MaxItems = MaxValidationDisclosureItems
	}
	if r.MaxBytes < 1 || r.MaxBytes > MaxValidationDisclosureBytes || r.MaxItems < 1 || r.MaxItems > MaxValidationDisclosureItems {
		return fmt.Errorf("validation disclosure budget exceeds bounds")
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, r := range value {
		if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// Normalize validates a bounded failure without exposing arbitrary diagnostics.
func (f *ValidationFailure) Normalize() error {
	if f == nil {
		return fmt.Errorf("validation failure is nil")
	}
	f.Code, f.Message = strings.TrimSpace(f.Code), strings.TrimSpace(f.Message)
	if f.Code == "" || len(f.Code) > 64 || len(f.Message) > 512 {
		return fmt.Errorf("validation failure is invalid")
	}
	return nil
}
