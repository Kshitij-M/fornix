package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	ObservabilitySchemaVersion = 1
	MaxObservationEvidence     = 16 << 10
	MaxObservationMetadata     = 32
	MaxDimensionLength         = 64
	MaxMetricWindow            = 24 * time.Hour
	MaxEvalCases               = 1000
	MaxEvalInlineBytes         = 64 << 10
	DefaultEvalRetrievalK      = 10
)

const (
	ObservationModel     = "model"
	ObservationTool      = "tool"
	ObservationRetrieval = "retrieval"
	ObservationAgent     = "agent"
	ObservationArtifact  = "artifact"
	ObservationRetry     = "retry"
	ObservationApproval  = "approval"
	ObservationScheduler = "scheduler"
)

const (
	OutcomeSucceeded = "succeeded"
	OutcomeFailed    = "failed"
	OutcomeSkipped   = "skipped"
	OutcomeWaiting   = "waiting"
	OutcomeCancelled = "cancelled"
)

const (
	CostModel     = "model"
	CostTool      = "tool"
	CostRetrieval = "retrieval"
	CostArtifact  = "artifact"
	CostRetry     = "retry"
	CostDuplicate = "duplicate"
)

// RunObservation is an append-only, bounded accounting record. It contains
// stable dimensions and references, never arbitrary user text or secrets.
type RunObservation struct {
	SchemaVersion  int               `json:"schema_version"`
	ID             string            `json:"id"`
	WorkspaceID    string            `json:"workspace_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	PayloadHash    string            `json:"payload_hash"`
	Kind           string            `json:"kind"`
	Component      string            `json:"component"`
	Operation      string            `json:"operation"`
	Outcome        string            `json:"outcome"`
	Actor          ActorRef          `json:"actor,omitempty"`
	Task           *EntityRef        `json:"task,omitempty"`
	Session        *EntityRef        `json:"session,omitempty"`
	CausationID    string            `json:"causation_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	SourceKind     string            `json:"source_kind,omitempty"`
	SourceID       string            `json:"source_id,omitempty"`
	StartedAt      time.Time         `json:"started_at"`
	FinishedAt     time.Time         `json:"finished_at,omitempty"`
	DurationMS     int64             `json:"duration_ms,omitempty"`
	DBQueries      int               `json:"db_queries,omitempty"`
	DBRows         int64             `json:"db_rows,omitempty"`
	InputBytes     int64             `json:"input_bytes,omitempty"`
	OutputBytes    int64             `json:"output_bytes,omitempty"`
	InputTokens    int               `json:"input_tokens,omitempty"`
	OutputTokens   int               `json:"output_tokens,omitempty"`
	TotalTokens    int               `json:"total_tokens,omitempty"`
	UsageMeasured  bool              `json:"usage_measured,omitempty"`
	UsageEstimated bool              `json:"usage_estimated,omitempty"`
	CostUSD        float64           `json:"cost_usd,omitempty"`
	CostKnown      bool              `json:"cost_known,omitempty"`
	RetryCount     int               `json:"retry_count,omitempty"`
	DuplicateWork  bool              `json:"duplicate_work,omitempty"`
	ArtifactBytes  int64             `json:"artifact_bytes,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Evidence       json.RawMessage   `json:"evidence,omitempty"`
}

// TraceSpan is a bounded child timing record. Attributes are restricted to
// fixed dimensions by Normalize and never contain prompts or credentials.
type TraceSpan struct {
	SchemaVersion  int               `json:"schema_version"`
	ID             string            `json:"id"`
	WorkspaceID    string            `json:"workspace_id"`
	TraceID        string            `json:"trace_id"`
	ParentID       string            `json:"parent_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key"`
	Component      string            `json:"component"`
	Operation      string            `json:"operation"`
	Outcome        string            `json:"outcome"`
	StartedAt      time.Time         `json:"started_at"`
	FinishedAt     time.Time         `json:"finished_at,omitempty"`
	DurationMS     int64             `json:"duration_ms,omitempty"`
	Attributes     map[string]string `json:"attributes,omitempty"`
}

// CostLedgerEntry attributes a bounded amount of work. Estimated and measured
// are separate so a missing provider usage value cannot look exact.
type CostLedgerEntry struct {
	SchemaVersion  int               `json:"schema_version"`
	ID             string            `json:"id"`
	WorkspaceID    string            `json:"workspace_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	PayloadHash    string            `json:"payload_hash"`
	Category       string            `json:"category"`
	Basis          string            `json:"basis"`
	SourceKind     string            `json:"source_kind"`
	SourceID       string            `json:"source_id"`
	Actor          ActorRef          `json:"actor,omitempty"`
	Task           *EntityRef        `json:"task,omitempty"`
	Session        *EntityRef        `json:"session,omitempty"`
	CausationID    string            `json:"causation_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	Units          float64           `json:"units,omitempty"`
	UnitCostUSD    float64           `json:"unit_cost_usd,omitempty"`
	AmountUSD      float64           `json:"amount_usd,omitempty"`
	AmountKnown    bool              `json:"amount_known"`
	Measured       bool              `json:"measured"`
	Estimated      bool              `json:"estimated"`
	InputTokens    int               `json:"input_tokens,omitempty"`
	OutputTokens   int               `json:"output_tokens,omitempty"`
	DurationMS     int64             `json:"duration_ms,omitempty"`
	Bytes          int64             `json:"bytes,omitempty"`
	DuplicateWork  bool              `json:"duplicate_work,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// MetricDimensions is deliberately a fixed vocabulary. Workspace is a row
// scope and is not a user-controlled label.
type MetricDimensions struct {
	Component string `json:"component,omitempty"`
	Operation string `json:"operation,omitempty"`
	Outcome   string `json:"outcome,omitempty"`
	Provider  string `json:"provider,omitempty"`
	Model     string `json:"model,omitempty"`
	Stage     string `json:"stage,omitempty"`
	Category  string `json:"category,omitempty"`
}

// MetricSample is one bounded workspace-scoped metric observation. Dimension
// names are fixed by MetricDimensions to prevent cardinality explosions.
type MetricSample struct {
	SchemaVersion  int              `json:"schema_version"`
	ID             string           `json:"id"`
	WorkspaceID    string           `json:"workspace_id"`
	IdempotencyKey string           `json:"idempotency_key"`
	Name           string           `json:"name"`
	Value          float64          `json:"value"`
	Count          int64            `json:"count,omitempty"`
	ObservedAt     time.Time        `json:"observed_at"`
	Dimensions     MetricDimensions `json:"dimensions,omitempty"`
}

// QualityGate records one deterministic pass/fail threshold evaluation.
type QualityGate struct {
	Name      string  `json:"name"`
	Metric    string  `json:"metric"`
	Operator  string  `json:"operator"`
	Threshold float64 `json:"threshold"`
	Actual    float64 `json:"actual"`
	Passed    bool    `json:"passed"`
	Reason    string  `json:"reason,omitempty"`
}

// RetrievalQualityMetrics is the bounded, deterministic score for one
// recorded retrieval surface. It contains no prompt or raw evidence text;
// evidence identity remains a hash/reference resolved by the authority.
type RetrievalQualityMetrics struct {
	SchemaVersion     int     `json:"schema_version"`
	K                 int     `json:"k"`
	RetrievedCount    int     `json:"retrieved_count"`
	GoldCount         int     `json:"gold_count"`
	RelevantAtK       int     `json:"relevant_at_k"`
	HitAtK            float64 `json:"hit_at_k"`
	ReciprocalRank    float64 `json:"reciprocal_rank"`
	PrecisionAtK      float64 `json:"precision_at_k"`
	RecallAtK         float64 `json:"recall_at_k"`
	NDCGAtK           float64 `json:"ndcg_at_k"`
	RankDrift         float64 `json:"rank_drift"`
	ContextHashMatch  bool    `json:"context_hash_match"`
	AbstentionCorrect bool    `json:"abstention_correct"`
	LatencyMS         int64   `json:"latency_ms"`
	SQLQueries        int     `json:"sql_queries"`
	CostUSD           float64 `json:"cost_usd"`
	CostKnown         bool    `json:"cost_known"`
	CostEstimated     bool    `json:"cost_estimated"`
}

// RetrievalQualitySummary is the canonical aggregate used by run reports and
// regression gates. Values are arithmetic means over scored cases except for
// Cases and UnknownCostCases.
type RetrievalQualitySummary struct {
	Cases             int     `json:"cases"`
	HitAtK            float64 `json:"hit_at_k"`
	ReciprocalRank    float64 `json:"reciprocal_rank"`
	PrecisionAtK      float64 `json:"precision_at_k"`
	RecallAtK         float64 `json:"recall_at_k"`
	NDCGAtK           float64 `json:"ndcg_at_k"`
	RankDrift         float64 `json:"rank_drift"`
	ContextHashMatch  float64 `json:"context_hash_match"`
	AbstentionCorrect float64 `json:"abstention_correct"`
	LatencyMS         float64 `json:"latency_ms"`
	SQLQueries        float64 `json:"sql_queries"`
	CostUSD           float64 `json:"cost_usd"`
	UnknownCostCases  int     `json:"unknown_cost_cases"`
}

// RegressionFinding is a durable, bounded explanation of a baseline
// comparison. RelativeDelta is baseline-normalized where possible; zero
// baselines use an absolute delta and remain deterministic.
type RegressionFinding struct {
	Name          string  `json:"name"`
	Metric        string  `json:"metric"`
	Baseline      float64 `json:"baseline"`
	Candidate     float64 `json:"candidate"`
	Delta         float64 `json:"delta"`
	RelativeDelta float64 `json:"relative_delta"`
	Threshold     float64 `json:"threshold"`
	Passed        bool    `json:"passed"`
	Reason        string  `json:"reason,omitempty"`
}

// EvalCase points to recorded state. It intentionally has no prompt field;
// input is recovered from the referenced durable run and reports expose only
// InputHash, preventing raw prompt leakage into evaluation output.
type EvalCase struct {
	ID                  string   `json:"id"`
	ReplayRunID         string   `json:"replay_run_id"`
	RetrievalSurfaceID  string   `json:"retrieval_surface_id,omitempty"`
	InputHash           string   `json:"input_hash"`
	GoldEvidence        []string `json:"gold_evidence,omitempty"`
	ExpectedContextHash string   `json:"expected_context_hash,omitempty"`
	ExpectedTermination string   `json:"expected_termination,omitempty"`
	MaxCostUSD          float64  `json:"max_cost_usd,omitempty"`
	ExpectedAbstention  bool     `json:"expected_abstention,omitempty"`
	RetrievalK          int      `json:"retrieval_k,omitempty"`
	Tags                []string `json:"tags,omitempty"`
}

// EvalDataset is an immutable, versioned set of references to recorded runs
// and evidence. It contains hashes and identities rather than raw prompts.
type EvalDataset struct {
	SchemaVersion int        `json:"schema_version"`
	ID            string     `json:"id"`
	WorkspaceID   string     `json:"workspace_id"`
	Name          string     `json:"name"`
	Version       int        `json:"version"`
	DatasetHash   string     `json:"dataset_hash"`
	Cases         []EvalCase `json:"cases"`
	CreatedAt     time.Time  `json:"created_at,omitempty"`
}

const (
	EvalRunPending   = "pending"
	EvalRunRunning   = "running"
	EvalRunSucceeded = "succeeded"
	EvalRunFailed    = "failed"
	EvalRunCancelled = "cancelled"
)

// EvalRun is the durable bounded lifecycle of an offline evaluation. Replay
// must not invoke remote models or external tools.
type EvalRun struct {
	SchemaVersion     int                      `json:"schema_version"`
	ID                string                   `json:"id"`
	WorkspaceID       string                   `json:"workspace_id"`
	DatasetID         string                   `json:"dataset_id"`
	DatasetHash       string                   `json:"dataset_hash"`
	IdempotencyKey    string                   `json:"idempotency_key"`
	RequestHash       string                   `json:"request_hash"`
	Status            string                   `json:"status"`
	DryRun            bool                     `json:"dry_run,omitempty"`
	BatchLimit        int                      `json:"batch_limit"`
	CasesTotal        int                      `json:"cases_total"`
	CasesCompleted    int                      `json:"cases_completed"`
	CasesPassed       int                      `json:"cases_passed"`
	CasesFailed       int                      `json:"cases_failed"`
	CostUSD           float64                  `json:"cost_usd,omitempty"`
	CostKnown         bool                     `json:"cost_known"`
	ReplayHash        string                   `json:"replay_hash,omitempty"`
	Gates             []QualityGate            `json:"gates,omitempty"`
	BaselineEvalRunID string                   `json:"baseline_eval_run_id,omitempty"`
	RetrievalQuality  *RetrievalQualitySummary `json:"retrieval_quality,omitempty"`
	Regressions       []RegressionFinding      `json:"regressions,omitempty"`
	Report            json.RawMessage          `json:"report,omitempty"`
	ReportArtifact    *ArtifactRef             `json:"report_artifact,omitempty"`
	CreatedAt         time.Time                `json:"created_at,omitempty"`
	FinishedAt        *time.Time               `json:"finished_at,omitempty"`
}

// EvalResult is the deterministic result for one evaluation case and replay
// history. Its evidence references remain workspace-scoped.
type EvalResult struct {
	SchemaVersion    int                      `json:"schema_version"`
	ID               string                   `json:"id"`
	WorkspaceID      string                   `json:"workspace_id"`
	EvalRunID        string                   `json:"eval_run_id"`
	CaseID           string                   `json:"case_id"`
	ReplayRunID      string                   `json:"replay_run_id"`
	InputHash        string                   `json:"input_hash"`
	ContextHash      string                   `json:"context_hash,omitempty"`
	Termination      string                   `json:"termination,omitempty"`
	ObservedCostUSD  float64                  `json:"observed_cost_usd,omitempty"`
	CostKnown        bool                     `json:"cost_known"`
	ReplayHash       string                   `json:"replay_hash"`
	Passed           bool                     `json:"passed"`
	Abstained        bool                     `json:"abstained"`
	Gates            []QualityGate            `json:"gates,omitempty"`
	RetrievalMetrics *RetrievalQualityMetrics `json:"retrieval_metrics,omitempty"`
	ResolvedEvidence []string                 `json:"resolved_evidence,omitempty"`
	Regressions      []RegressionFinding      `json:"regressions,omitempty"`
	Error            string                   `json:"error,omitempty"`
	CreatedAt        time.Time                `json:"created_at,omitempty"`
}

// MetricAggregate is a bounded read model used in operator snapshots.
type MetricAggregate struct {
	Component string  `json:"component,omitempty"`
	Operation string  `json:"operation,omitempty"`
	Outcome   string  `json:"outcome,omitempty"`
	Stage     string  `json:"stage,omitempty"`
	Name      string  `json:"name,omitempty"`
	Count     int64   `json:"count"`
	Value     float64 `json:"value,omitempty"`
	P95MS     int64   `json:"p95_ms,omitempty"`
}

// CostAggregate separates measured, estimated, and unknown accounting so
// missing provider usage is never presented as exact.
type CostAggregate struct {
	Category       string  `json:"category"`
	Entries        int64   `json:"entries"`
	AmountUSD      float64 `json:"amount_usd,omitempty"`
	MeasuredUSD    float64 `json:"measured_usd,omitempty"`
	EstimatedUSD   float64 `json:"estimated_usd,omitempty"`
	UnknownEntries int64   `json:"unknown_entries,omitempty"`
	Bytes          int64   `json:"bytes,omitempty"`
	DurationMS     int64   `json:"duration_ms,omitempty"`
}

// ObservabilitySnapshot is a workspace-scoped, bounded accounting view over a
// time window. It is derived from append-only observations and ledger entries.
type ObservabilitySnapshot struct {
	SchemaVersion      int               `json:"schema_version"`
	WorkspaceID        string            `json:"workspace_id"`
	Since              time.Time         `json:"since"`
	Until              time.Time         `json:"until"`
	ObservationCount   int64             `json:"observation_count"`
	SpanCount          int64             `json:"span_count"`
	MetricCount        int64             `json:"metric_count"`
	DuplicateWorkCount int64             `json:"duplicate_work_count"`
	DurationMS         int64             `json:"duration_ms"`
	DBQueries          int64             `json:"db_queries"`
	ArtifactBytes      int64             `json:"artifact_bytes"`
	MeasuredCostUSD    float64           `json:"measured_cost_usd"`
	EstimatedCostUSD   float64           `json:"estimated_cost_usd"`
	UnknownCostEntries int64             `json:"unknown_cost_entries"`
	Operations         []MetricAggregate `json:"operations,omitempty"`
	Costs              []CostAggregate   `json:"costs,omitempty"`
}

// Normalize validates redaction-safe dimensions, references, and accounting
// fields before an observation is persisted.
func (o *RunObservation) Normalize() error {
	if o == nil {
		return fmt.Errorf("observation is nil")
	}
	if o.SchemaVersion == 0 {
		o.SchemaVersion = ObservabilitySchemaVersion
	}
	if o.SchemaVersion != ObservabilitySchemaVersion {
		return fmt.Errorf("unsupported observation schema_version %d", o.SchemaVersion)
	}
	o.ID, o.WorkspaceID, o.IdempotencyKey = strings.TrimSpace(o.ID), strings.TrimSpace(o.WorkspaceID), strings.TrimSpace(o.IdempotencyKey)
	if o.ID == "" {
		o.ID = NewID("obs")
	}
	if o.WorkspaceID == "" || o.IdempotencyKey == "" {
		return fmt.Errorf("workspace_id and idempotency_key are required")
	}
	if len(o.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("idempotency_key is too large")
	}
	if err := validateDimension(o.Kind, "kind"); err != nil {
		return err
	}
	if !validObservationKind(o.Kind) {
		return fmt.Errorf("unsupported observation kind %q", o.Kind)
	}
	if err := validateDimension(o.Component, "component"); err != nil {
		return err
	}
	if err := validateDimension(o.Operation, "operation"); err != nil {
		return err
	}
	if err := validateDimension(o.Outcome, "outcome"); err != nil {
		return err
	}
	if !validObservationOutcome(o.Outcome) {
		return fmt.Errorf("unsupported observation outcome %q", o.Outcome)
	}
	if o.StartedAt.IsZero() {
		o.StartedAt = time.Now().UTC()
	}
	if o.FinishedAt.IsZero() {
		o.FinishedAt = o.StartedAt.Add(time.Duration(maxInt64(o.DurationMS, 0)) * time.Millisecond)
	}
	if o.DurationMS < 0 || o.DBQueries < 0 || o.DBRows < 0 || o.InputBytes < 0 || o.OutputBytes < 0 || o.InputTokens < 0 || o.OutputTokens < 0 || o.TotalTokens < 0 || o.RetryCount < 0 || o.ArtifactBytes < 0 {
		return fmt.Errorf("observation counters cannot be negative")
	}
	if math.IsNaN(o.CostUSD) || math.IsInf(o.CostUSD, 0) || o.CostUSD < 0 {
		return fmt.Errorf("observation cost is invalid")
	}
	if err := normalizeMetadata(&o.Metadata); err != nil {
		return err
	}
	o.Evidence = append([]byte(nil), o.Evidence...)
	if len(o.Evidence) > MaxObservationEvidence || (len(o.Evidence) > 0 && !json.Valid(o.Evidence)) {
		return fmt.Errorf("observation evidence is invalid or too large")
	}
	if o.PayloadHash == "" {
		o.PayloadHash = hashJSON(o.withoutIdentity())
	}
	return nil
}

func (o RunObservation) withoutIdentity() any {
	o.ID, o.PayloadHash = "", ""
	o.StartedAt, o.FinishedAt = time.Time{}, time.Time{}
	return o
}

// Normalize validates a trace span and bounds its fixed attributes.
func (s *TraceSpan) Normalize() error {
	if s == nil {
		return fmt.Errorf("span is nil")
	}
	if s.SchemaVersion == 0 {
		s.SchemaVersion = ObservabilitySchemaVersion
	}
	if s.SchemaVersion != ObservabilitySchemaVersion {
		return fmt.Errorf("unsupported span schema_version %d", s.SchemaVersion)
	}
	s.ID, s.WorkspaceID, s.TraceID, s.IdempotencyKey = strings.TrimSpace(s.ID), strings.TrimSpace(s.WorkspaceID), strings.TrimSpace(s.TraceID), strings.TrimSpace(s.IdempotencyKey)
	if s.ID == "" {
		s.ID = NewID("span")
	}
	if s.WorkspaceID == "" || s.TraceID == "" || s.IdempotencyKey == "" {
		return fmt.Errorf("workspace_id, trace_id, and idempotency_key are required")
	}
	for key, value := range map[string]string{"component": s.Component, "operation": s.Operation, "outcome": s.Outcome} {
		if err := validateDimension(value, key); err != nil {
			return err
		}
	}
	if s.StartedAt.IsZero() {
		s.StartedAt = time.Now().UTC()
	}
	if s.DurationMS < 0 {
		return fmt.Errorf("span duration cannot be negative")
	}
	if err := normalizeMetadata(&s.Attributes); err != nil {
		return err
	}
	return nil
}

// Normalize validates cost attribution and rejects contradictory measured or
// estimated flags.
func (c *CostLedgerEntry) Normalize() error {
	if c == nil {
		return fmt.Errorf("cost entry is nil")
	}
	if c.SchemaVersion == 0 {
		c.SchemaVersion = ObservabilitySchemaVersion
	}
	if c.SchemaVersion != ObservabilitySchemaVersion {
		return fmt.Errorf("unsupported cost schema_version %d", c.SchemaVersion)
	}
	c.ID, c.WorkspaceID, c.IdempotencyKey, c.Category, c.Basis, c.SourceKind, c.SourceID = strings.TrimSpace(c.ID), strings.TrimSpace(c.WorkspaceID), strings.TrimSpace(c.IdempotencyKey), strings.TrimSpace(c.Category), strings.TrimSpace(c.Basis), strings.TrimSpace(c.SourceKind), strings.TrimSpace(c.SourceID)
	if c.ID == "" {
		c.ID = NewID("cost")
	}
	if c.WorkspaceID == "" || c.IdempotencyKey == "" || c.Category == "" || c.SourceKind == "" || c.SourceID == "" {
		return fmt.Errorf("cost workspace, idempotency, category, and source are required")
	}
	if !validCostCategory(c.Category) {
		return fmt.Errorf("unsupported cost category %q", c.Category)
	}
	if c.Units < 0 || c.UnitCostUSD < 0 || c.AmountUSD < 0 || math.IsNaN(c.Units) || math.IsNaN(c.UnitCostUSD) || math.IsNaN(c.AmountUSD) {
		return fmt.Errorf("cost values are invalid")
	}
	if c.AmountKnown && c.AmountUSD == 0 && c.Units > 0 && c.UnitCostUSD > 0 {
		c.AmountUSD = c.Units * c.UnitCostUSD
	}
	if c.Measured && c.Estimated {
		return fmt.Errorf("cost cannot be both measured and estimated")
	}
	if err := normalizeMetadata(&c.Metadata); err != nil {
		return err
	}
	if c.PayloadHash == "" {
		c.PayloadHash = hashJSON(c.withoutIdentity())
	}
	return nil
}

func (c CostLedgerEntry) withoutIdentity() any { c.ID, c.PayloadHash = "", ""; return c }

// Normalize validates a metric sample and its bounded dimension vocabulary.
func (m *MetricSample) Normalize() error {
	if m == nil {
		return fmt.Errorf("metric sample is nil")
	}
	if m.SchemaVersion == 0 {
		m.SchemaVersion = ObservabilitySchemaVersion
	}
	if m.SchemaVersion != ObservabilitySchemaVersion {
		return fmt.Errorf("unsupported metric schema_version %d", m.SchemaVersion)
	}
	m.ID, m.WorkspaceID, m.IdempotencyKey, m.Name = strings.TrimSpace(m.ID), strings.TrimSpace(m.WorkspaceID), strings.TrimSpace(m.IdempotencyKey), strings.TrimSpace(m.Name)
	if m.ID == "" {
		m.ID = NewID("metric")
	}
	if m.WorkspaceID == "" || m.IdempotencyKey == "" || m.Name == "" {
		return fmt.Errorf("metric workspace, idempotency, and name are required")
	}
	if err := validateDimension(m.Name, "metric name"); err != nil {
		return err
	}
	if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) || m.Count < 0 {
		return fmt.Errorf("metric value is invalid")
	}
	if m.ObservedAt.IsZero() {
		m.ObservedAt = time.Now().UTC()
	}
	if err := m.Dimensions.normalize(); err != nil {
		return err
	}
	return nil
}

func (d *MetricDimensions) normalize() error {
	for key, value := range map[string]string{"component": d.Component, "operation": d.Operation, "outcome": d.Outcome, "provider": d.Provider, "model": d.Model, "stage": d.Stage, "category": d.Category} {
		if value != "" {
			if err := validateDimension(value, key); err != nil {
				return err
			}
		}
	}
	return nil
}

// Normalize validates and hashes a bounded, versioned evaluation dataset.
func (d *EvalDataset) Normalize() error {
	if d == nil {
		return fmt.Errorf("eval dataset is nil")
	}
	if d.SchemaVersion == 0 {
		d.SchemaVersion = ObservabilitySchemaVersion
	}
	if d.SchemaVersion != ObservabilitySchemaVersion {
		return fmt.Errorf("unsupported dataset schema_version %d", d.SchemaVersion)
	}
	d.ID, d.WorkspaceID, d.Name = strings.TrimSpace(d.ID), strings.TrimSpace(d.WorkspaceID), strings.TrimSpace(d.Name)
	if d.ID == "" {
		d.ID = NewID("dataset")
	}
	if d.WorkspaceID == "" || d.Name == "" || d.Version < 1 {
		return fmt.Errorf("dataset identity and positive version are required")
	}
	if len(d.Cases) == 0 || len(d.Cases) > MaxEvalCases {
		return fmt.Errorf("dataset cases must be between 1 and %d", MaxEvalCases)
	}
	seen := map[string]bool{}
	for i := range d.Cases {
		if err := normalizeEvalCase(&d.Cases[i], d.WorkspaceID); err != nil {
			return fmt.Errorf("case %d: %w", i, err)
		}
		if seen[d.Cases[i].ID] {
			return fmt.Errorf("duplicate eval case %q", d.Cases[i].ID)
		}
		seen[d.Cases[i].ID] = true
	}
	if d.DatasetHash == "" {
		d.DatasetHash = hashJSON(struct {
			ID        string     `json:"id"`
			Workspace string     `json:"workspace_id"`
			Name      string     `json:"name"`
			Version   int        `json:"version"`
			Cases     []EvalCase `json:"cases"`
		}{d.ID, d.WorkspaceID, d.Name, d.Version, d.Cases})
	}
	return nil
}

func normalizeEvalCase(c *EvalCase, workspace string) error {
	c.ID, c.ReplayRunID, c.RetrievalSurfaceID, c.InputHash, c.ExpectedContextHash, c.ExpectedTermination = strings.TrimSpace(c.ID), strings.TrimSpace(c.ReplayRunID), strings.TrimSpace(c.RetrievalSurfaceID), strings.TrimSpace(c.InputHash), strings.TrimSpace(c.ExpectedContextHash), strings.TrimSpace(c.ExpectedTermination)
	if c.ReplayRunID == "" && c.RetrievalSurfaceID != "" {
		c.ReplayRunID = c.RetrievalSurfaceID
	}
	if c.ID == "" || c.ReplayRunID == "" || c.InputHash == "" {
		return fmt.Errorf("id, replay_run_id, and input_hash are required")
	}
	if c.MaxCostUSD < 0 || math.IsNaN(c.MaxCostUSD) || math.IsInf(c.MaxCostUSD, 0) {
		return fmt.Errorf("max_cost_usd is invalid")
	}
	if c.RetrievalK == 0 {
		c.RetrievalK = DefaultEvalRetrievalK
	}
	if c.RetrievalK < 1 || c.RetrievalK > MaxRetrievalItems {
		return fmt.Errorf("retrieval_k must be between 1 and %d", MaxRetrievalItems)
	}
	c.GoldEvidence = normalizeStrings(c.GoldEvidence)
	c.Tags = normalizeStrings(c.Tags)
	return nil
}

// Normalize validates evaluation lifecycle state, batch bounds, and workspace
// identity before a run is stored.
func (r *EvalRun) Normalize() error {
	if r == nil {
		return fmt.Errorf("eval run is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ObservabilitySchemaVersion
	}
	if r.SchemaVersion != ObservabilitySchemaVersion {
		return fmt.Errorf("unsupported eval run schema_version %d", r.SchemaVersion)
	}
	r.ID, r.WorkspaceID, r.DatasetID, r.DatasetHash, r.IdempotencyKey = strings.TrimSpace(r.ID), strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.DatasetID), strings.TrimSpace(r.DatasetHash), strings.TrimSpace(r.IdempotencyKey)
	r.BaselineEvalRunID = strings.TrimSpace(r.BaselineEvalRunID)
	if r.ID == "" {
		r.ID = NewID("eval")
	}
	if r.WorkspaceID == "" || r.DatasetID == "" || r.DatasetHash == "" || r.IdempotencyKey == "" {
		return fmt.Errorf("eval run identity is incomplete")
	}
	if r.Status == "" {
		r.Status = EvalRunPending
	}
	if r.Status != EvalRunPending && r.Status != EvalRunRunning && r.Status != EvalRunSucceeded && r.Status != EvalRunFailed && r.Status != EvalRunCancelled {
		return fmt.Errorf("invalid eval run status %q", r.Status)
	}
	if r.BatchLimit == 0 {
		r.BatchLimit = 100
	}
	if r.BatchLimit < 1 || r.BatchLimit > MaxEvalCases {
		return fmt.Errorf("batch_limit is out of bounds")
	}
	if r.CasesTotal < 0 || r.CasesCompleted < 0 || r.CasesPassed < 0 || r.CasesFailed < 0 || r.CostUSD < 0 {
		return fmt.Errorf("eval counters cannot be negative")
	}
	for i := range r.Gates {
		if err := r.Gates[i].Normalize(); err != nil {
			return fmt.Errorf("gate %d: %w", i, err)
		}
	}
	for i := range r.Regressions {
		if err := r.Regressions[i].Normalize(); err != nil {
			return fmt.Errorf("regression %d: %w", i, err)
		}
	}
	return nil
}

// Normalize validates one redacted deterministic case result.
func (r *EvalResult) Normalize() error {
	if r == nil {
		return fmt.Errorf("eval result is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ObservabilitySchemaVersion
	}
	if r.SchemaVersion != ObservabilitySchemaVersion {
		return fmt.Errorf("unsupported eval result schema_version %d", r.SchemaVersion)
	}
	r.ID, r.WorkspaceID, r.EvalRunID, r.CaseID, r.ReplayRunID, r.InputHash, r.ReplayHash = strings.TrimSpace(r.ID), strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.EvalRunID), strings.TrimSpace(r.CaseID), strings.TrimSpace(r.ReplayRunID), strings.TrimSpace(r.InputHash), strings.TrimSpace(r.ReplayHash)
	if r.ID == "" {
		r.ID = NewID("evalresult")
	}
	if r.WorkspaceID == "" || r.EvalRunID == "" || r.CaseID == "" || r.ReplayRunID == "" || r.InputHash == "" || r.ReplayHash == "" {
		return fmt.Errorf("eval result identity is incomplete")
	}
	if r.ObservedCostUSD < 0 || math.IsNaN(r.ObservedCostUSD) || math.IsInf(r.ObservedCostUSD, 0) {
		return fmt.Errorf("observed cost is invalid")
	}
	r.ResolvedEvidence = normalizeStrings(r.ResolvedEvidence)
	if r.RetrievalMetrics != nil {
		if err := r.RetrievalMetrics.Normalize(); err != nil {
			return fmt.Errorf("retrieval metrics: %w", err)
		}
	}
	for i := range r.Gates {
		if err := r.Gates[i].Normalize(); err != nil {
			return fmt.Errorf("gate %d: %w", i, err)
		}
	}
	for i := range r.Regressions {
		if err := r.Regressions[i].Normalize(); err != nil {
			return fmt.Errorf("regression %d: %w", i, err)
		}
	}
	return nil
}

// Normalize validates bounded retrieval metrics before they enter a report.
func (m *RetrievalQualityMetrics) Normalize() error {
	if m == nil {
		return fmt.Errorf("retrieval metrics are nil")
	}
	if m.SchemaVersion == 0 {
		m.SchemaVersion = ObservabilitySchemaVersion
	}
	if m.SchemaVersion != ObservabilitySchemaVersion {
		return fmt.Errorf("unsupported retrieval metrics schema_version %d", m.SchemaVersion)
	}
	if m.K < 1 || m.K > MaxRetrievalItems || m.RetrievedCount < 0 || m.GoldCount < 0 || m.RelevantAtK < 0 || m.RelevantAtK > m.K || m.LatencyMS < 0 || m.SQLQueries < 0 {
		return fmt.Errorf("retrieval metric counters are invalid")
	}
	for name, value := range map[string]float64{
		"hit_at_k": m.HitAtK, "reciprocal_rank": m.ReciprocalRank,
		"precision_at_k": m.PrecisionAtK, "recall_at_k": m.RecallAtK,
		"ndcg_at_k": m.NDCGAtK, "rank_drift": m.RankDrift, "cost_usd": m.CostUSD,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return fmt.Errorf("retrieval metric %s is invalid", name)
		}
	}
	for name, value := range map[string]float64{"hit_at_k": m.HitAtK, "reciprocal_rank": m.ReciprocalRank, "precision_at_k": m.PrecisionAtK, "recall_at_k": m.RecallAtK, "ndcg_at_k": m.NDCGAtK} {
		if value > 1 {
			return fmt.Errorf("retrieval metric %s must be at most 1", name)
		}
	}
	if m.CostKnown && m.CostEstimated {
		return fmt.Errorf("retrieval cost cannot be both known and estimated")
	}
	return nil
}

// Normalize validates one deterministic baseline comparison.
func (f *RegressionFinding) Normalize() error {
	if f == nil {
		return fmt.Errorf("regression is nil")
	}
	f.Name, f.Metric, f.Reason = strings.TrimSpace(f.Name), strings.TrimSpace(f.Metric), strings.TrimSpace(f.Reason)
	if err := validateDimension(f.Name, "regression name"); err != nil {
		return err
	}
	if err := validateDimension(f.Metric, "regression metric"); err != nil {
		return err
	}
	for name, value := range map[string]float64{"baseline": f.Baseline, "candidate": f.Candidate, "delta": f.Delta, "relative_delta": f.RelativeDelta, "threshold": f.Threshold} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("regression %s is invalid", name)
		}
	}
	return nil
}

// Normalize validates the gate operator and finite threshold values.
func (g QualityGate) Normalize() error {
	if err := validateDimension(strings.TrimSpace(g.Name), "gate name"); err != nil {
		return err
	}
	if err := validateDimension(strings.TrimSpace(g.Metric), "gate metric"); err != nil {
		return err
	}
	switch strings.TrimSpace(g.Operator) {
	case ">=", "<=", "==", ">", "<":
	default:
		return fmt.Errorf("unsupported gate operator %q", g.Operator)
	}
	if math.IsNaN(g.Threshold) || math.IsNaN(g.Actual) || math.IsInf(g.Threshold, 0) || math.IsInf(g.Actual, 0) {
		return fmt.Errorf("gate values are invalid")
	}
	return nil
}

func validateDimension(value, name string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > MaxDimensionLength {
		return fmt.Errorf("%s exceeds %d characters", name, MaxDimensionLength)
	}
	if strings.ContainsAny(value, "\n\r\t") {
		return fmt.Errorf("%s contains control characters", name)
	}
	if strings.ContainsAny(value, " ") {
		return fmt.Errorf("%s cannot contain spaces", name)
	}
	lower := strings.ToLower(value)
	for _, forbidden := range []string{"prompt", "credential", "authorization", "bearer", "secret", "request_id"} {
		if strings.Contains(lower, forbidden) {
			return fmt.Errorf("%s contains a forbidden high-cardinality or secret label", name)
		}
	}
	return nil
}
func normalizeMetadata(values *map[string]string) error {
	if *values == nil {
		return nil
	}
	if len(*values) > MaxObservationMetadata {
		return fmt.Errorf("metadata has too many entries")
	}
	out := make(map[string]string, len(*values))
	for key, value := range *values {
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if key == "" || len(key) > MaxDimensionLength || len(value) > MaxDimensionLength {
			return fmt.Errorf("metadata entry is out of bounds")
		}
		if err := validateDimension(key, "metadata key"); err != nil {
			return err
		}
		if value != "" {
			if err := validateDimension(value, "metadata value"); err != nil {
				return err
			}
		}
		out[key] = value
	}
	*values = out
	return nil
}
func validCostCategory(value string) bool {
	switch value {
	case CostModel, CostTool, CostRetrieval, CostArtifact, CostRetry, CostDuplicate:
		return true
	default:
		return false
	}
}

func validObservationKind(value string) bool {
	switch value {
	case ObservationModel, ObservationTool, ObservationRetrieval, ObservationAgent, ObservationArtifact, ObservationRetry, ObservationApproval, ObservationScheduler:
		return true
	default:
		return false
	}
}

func validObservationOutcome(value string) bool {
	switch value {
	case OutcomeSucceeded, OutcomeFailed, OutcomeSkipped, OutcomeWaiting, OutcomeCancelled, "approved", "denied", "pending", "running", "takeover", "claim", "renew", "release":
		return true
	default:
		return false
	}
}
func maxInt64(v, zero int64) int64 {
	if v > zero {
		return v
	}
	return zero
}
func hashJSON(value any) string {
	raw, _ := json.Marshal(value)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
