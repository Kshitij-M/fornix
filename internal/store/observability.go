package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
)

var (
	ErrObservationConflict = errors.New("observation idempotency conflict")
	ErrCostConflict        = errors.New("cost ledger idempotency conflict")
	ErrMetricConflict      = errors.New("metric idempotency conflict")
	ErrEvalConflict        = errors.New("evaluation idempotency conflict")
	ErrEvalNotFound        = errors.New("evaluation record not found")
)

// ObservabilityStore persists bounded, redacted observations, spans, costs,
// and metrics. It attributes work to authoritative source records but does not
// replace those records as the system of truth.
type ObservabilityStore struct{ pool *pgxpool.Pool }

// NewObservabilityStore constructs an observability store over pool.
func NewObservabilityStore(pool *pgxpool.Pool) *ObservabilityStore {
	return &ObservabilityStore{pool: pool}
}

// ObserveModelCall reconciles a terminal model ledger record into the
// append-only observation and cost ledger. The model-call row remains the
// usage authority; this method only attributes it.
func (s *ObservabilityStore) ObserveModelCall(ctx context.Context, call contracts.ModelCallRecord) error {
	measuredUsage := strings.TrimSpace(call.Usage.Source) != ""
	estimatedUsage := !measuredUsage && call.Usage.TotalTokens > 0
	costKnown := strings.TrimSpace(call.Cost.Source) != "" || call.Cost.TotalCostUSD > 0
	started := call.CreatedAt
	observation := contracts.RunObservation{WorkspaceID: call.WorkspaceID, IdempotencyKey: "model-observation:" + call.RequestID, Kind: contracts.ObservationModel, Component: "model_gateway", Operation: "complete", Outcome: call.Status, Actor: call.Actor, Task: call.Task, Session: call.Session, CausationID: call.CausationID, CorrelationID: call.CorrelationID, SourceKind: "model_call", SourceID: call.RequestID, StartedAt: started, DurationMS: call.DurationMS, InputTokens: call.Usage.InputTokens, OutputTokens: call.Usage.OutputTokens, TotalTokens: call.Usage.TotalTokens, UsageMeasured: measuredUsage, UsageEstimated: estimatedUsage, CostUSD: call.Cost.TotalCostUSD, CostKnown: costKnown, RetryCount: maxInt(call.AttemptCount-1, 0), Evidence: call.ResponseEvidence}
	if len(observation.Evidence) > contracts.MaxObservationEvidence {
		observation.Evidence = []byte(`{"redacted":true,"truncated":true}`)
	}
	if call.FinishedAt != nil {
		observation.FinishedAt = *call.FinishedAt
	}
	if _, _, err := s.RecordObservation(ctx, observation); err != nil {
		return err
	}
	_, _, err := s.RecordCost(ctx, contracts.CostLedgerEntry{WorkspaceID: call.WorkspaceID, IdempotencyKey: "model-cost:" + call.RequestID, Category: contracts.CostModel, Basis: "provider_usage", SourceKind: "model_call", SourceID: call.RequestID, Actor: call.Actor, Task: call.Task, Session: call.Session, CausationID: call.CausationID, CorrelationID: call.CorrelationID, Units: float64(call.Usage.TotalTokens), AmountUSD: call.Cost.TotalCostUSD, AmountKnown: costKnown, Measured: measuredUsage, Estimated: estimatedUsage, InputTokens: call.Usage.InputTokens, OutputTokens: call.Usage.OutputTokens, DurationMS: call.DurationMS})
	return err
}

// ObserveToolRun attributes one tool run's bounded duration and output work to
// append-only observation and cost records.
func (s *ObservabilityStore) ObserveToolRun(ctx context.Context, run contracts.ToolRun) error {
	outputBytes := int64(0)
	if run.Result != nil {
		outputBytes = int64(len([]byte(run.Result.Stdout)) + len([]byte(run.Result.Stderr)))
	}
	observation := contracts.RunObservation{WorkspaceID: run.WorkspaceID, IdempotencyKey: "tool-observation:" + run.ID, Kind: contracts.ObservationTool, Component: "tool_runtime", Operation: "execute", Outcome: run.Status, Actor: run.Actor, Task: run.Task, Session: run.Session, CausationID: run.CausationID, CorrelationID: run.CorrelationID, SourceKind: "tool_run", SourceID: run.ID, StartedAt: run.CreatedAt, DurationMS: run.DurationMS, OutputBytes: outputBytes, ArtifactBytes: artifactBytesForTool(run)}
	if run.FinishedAt != nil {
		observation.FinishedAt = *run.FinishedAt
	}
	if _, _, err := s.RecordObservation(ctx, observation); err != nil {
		return err
	}
	_, _, err := s.RecordCost(ctx, contracts.CostLedgerEntry{WorkspaceID: run.WorkspaceID, IdempotencyKey: "tool-cost:" + run.ID, Category: contracts.CostTool, Basis: "duration_ms", SourceKind: "tool_run", SourceID: run.ID, Actor: run.Actor, Task: run.Task, Session: run.Session, CausationID: run.CausationID, CorrelationID: run.CorrelationID, Units: float64(run.DurationMS), DurationMS: run.DurationMS, Bytes: outputBytes, Measured: true})
	return err
}

// ObserveRetrieval records a deterministic retrieval plan's database work,
// budgets, and outcome without persisting the raw query.
func (s *ObservabilityStore) ObserveRetrieval(ctx context.Context, request contracts.RetrievalRequest, trace contracts.RetrievalTrace) error {
	requestHash, err := request.Hash()
	if err != nil {
		return err
	}
	observation := contracts.RunObservation{WorkspaceID: request.WorkspaceID, IdempotencyKey: "retrieval-observation:" + requestHash, Kind: contracts.ObservationRetrieval, Component: "retrieval", Operation: "compile", Outcome: outcomeForRetrieval(trace), Actor: request.Actor, CausationID: request.CausationID, CorrelationID: request.CorrelationID, SourceKind: "retrieval_plan", SourceID: requestHash, StartedAt: time.Now().UTC(), DurationMS: retrievalDuration(trace), DBQueries: retrievalQueries(trace), InputBytes: int64(len(request.Query)), OutputBytes: int64(trace.CompiledBytes), TotalTokens: trace.CompiledTokens}
	if _, _, err = s.RecordObservation(ctx, observation); err != nil {
		return err
	}
	_, _, err = s.RecordCost(ctx, contracts.CostLedgerEntry{WorkspaceID: request.WorkspaceID, IdempotencyKey: "retrieval-cost:" + requestHash, Category: contracts.CostRetrieval, Basis: "db_queries", SourceKind: "retrieval_plan", SourceID: requestHash, Actor: request.Actor, CausationID: request.CausationID, CorrelationID: request.CorrelationID, Units: float64(observation.DBQueries), Estimated: true, Metadata: map[string]string{"compiled_items": fmt.Sprintf("%d", trace.CompiledItems)}})
	return err
}

// ObserveAgentRun attributes an agent run's bounded context, token, and cost
// totals while leaving transitions authoritative in the run store.
func (s *ObservabilityStore) ObserveAgentRun(ctx context.Context, run contracts.AgentRun) error {
	observation := contracts.RunObservation{WorkspaceID: run.WorkspaceID, IdempotencyKey: "agent-observation:" + run.ID, Kind: contracts.ObservationAgent, Component: "agent_loop", Operation: "run", Outcome: run.State, Actor: run.Actor, Task: run.Task, Session: run.Session, CausationID: run.CausationID, CorrelationID: run.CorrelationID, SourceKind: "agent_run", SourceID: run.ID, StartedAt: run.CreatedAt, InputTokens: run.InputTokens, OutputTokens: run.OutputTokens, TotalTokens: run.TotalTokens, CostUSD: run.Cost.TotalCostUSD, CostKnown: strings.TrimSpace(run.Cost.Source) != "", InputBytes: int64(run.ContextBytes)}
	if run.FinishedAt != nil {
		observation.FinishedAt = *run.FinishedAt
	}
	_, _, err := s.RecordObservation(ctx, observation)
	return err
}

// ObserveArtifact records storage work and deduplication metrics for one
// artifact operation.
func (s *ObservabilityStore) ObserveArtifact(ctx context.Context, metrics contracts.ArtifactStorageMetrics, operation string) error {
	observation := contracts.RunObservation{WorkspaceID: metrics.WorkspaceID, IdempotencyKey: fmt.Sprintf("artifact-observation:%s:%s:%d:%d:%d", operation, metrics.WorkspaceID, metrics.Artifacts, metrics.ChunkBytes, metrics.LogicalBytes), Kind: contracts.ObservationArtifact, Component: "artifact_store", Operation: operation, Outcome: contracts.OutcomeSucceeded, SourceKind: "artifact_metrics", SourceID: metrics.WorkspaceID, StartedAt: time.Now().UTC(), ArtifactBytes: metrics.ChunkBytes, OutputBytes: metrics.LogicalBytes, Metadata: map[string]string{"dedup_ratio": fmt.Sprintf("%.6f", metrics.DedupRatio)}}
	if _, _, err := s.RecordObservation(ctx, observation); err != nil {
		return err
	}
	_, _, err := s.RecordCost(ctx, contracts.CostLedgerEntry{WorkspaceID: metrics.WorkspaceID, IdempotencyKey: observation.IdempotencyKey + ":cost", Category: contracts.CostArtifact, Basis: "content_bytes", SourceKind: "artifact_metrics", SourceID: metrics.WorkspaceID, Units: float64(metrics.LogicalBytes), Bytes: metrics.ChunkBytes, Measured: true, Metadata: map[string]string{"dedup_ratio": fmt.Sprintf("%.6f", metrics.DedupRatio)}})
	return err
}

func maxInt(v, zero int) int {
	if v > zero {
		return v
	}
	return zero
}
func artifactBytesForTool(run contracts.ToolRun) int64 {
	var total int64
	for _, ref := range []*contracts.ArtifactRef{run.StdoutArtifact, run.StderrArtifact, run.ResultArtifact} {
		if ref != nil {
			total += ref.ByteSize
		}
	}
	return total
}
func outcomeForRetrieval(trace contracts.RetrievalTrace) string {
	if trace.Abstained {
		return contracts.OutcomeSkipped
	}
	return contracts.OutcomeSucceeded
}
func retrievalDuration(trace contracts.RetrievalTrace) int64 {
	var total int64
	for _, stage := range trace.Stages {
		total += stage.DurationMicros / 1000
	}
	return total
}
func retrievalQueries(trace contracts.RetrievalTrace) int {
	total := 0
	for _, stage := range trace.Stages {
		total += stage.Queries
	}
	return total
}

// RecordObservation appends one redacted observation idempotently. Duplicate
// delivery returns the canonical stored record rather than creating a second
// effect; a payload mismatch is a conflict.
func (s *ObservabilityStore) RecordObservation(ctx context.Context, observation contracts.RunObservation) (contracts.RunObservation, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.RunObservation{}, false, fmt.Errorf("observability store is not configured")
	}
	if err := observation.Normalize(); err != nil {
		return contracts.RunObservation{}, false, err
	}
	actor, _ := json.Marshal(observation.Actor)
	task, err := jsonOrEmpty(observation.Task)
	if err != nil {
		return contracts.RunObservation{}, false, err
	}
	session, err := jsonOrEmpty(observation.Session)
	if err != nil {
		return contracts.RunObservation{}, false, err
	}
	metadata, _ := json.Marshal(observation.Metadata)
	evidence := validEvidence(model.RedactBytes(observation.Evidence))
	if len(evidence) == 0 {
		evidence = []byte(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.RunObservation{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `INSERT INTO fornix.run_observations(id,workspace_id,idempotency_key,payload_hash,schema_version,kind,component,operation,outcome,actor,task_ref,session_ref,causation_id,correlation_id,source_kind,source_id,started_at,finished_at,duration_ms,db_queries,db_rows,input_bytes,output_bytes,input_tokens,output_tokens,total_tokens,usage_measured,usage_estimated,cost_usd,cost_known,retry_count,duplicate_work,artifact_bytes,metadata,evidence,policy_id,policy_version,policy_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34::jsonb,$35::jsonb,$36,$37,$38) ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, observation.ID, observation.WorkspaceID, observation.IdempotencyKey, observation.PayloadHash, observation.SchemaVersion, observation.Kind, observation.Component, observation.Operation, observation.Outcome, actor, task, session, observation.CausationID, observation.CorrelationID, observation.SourceKind, observation.SourceID, observation.StartedAt, observation.FinishedAt, observation.DurationMS, observation.DBQueries, observation.DBRows, observation.InputBytes, observation.OutputBytes, observation.InputTokens, observation.OutputTokens, observation.TotalTokens, observation.UsageMeasured, observation.UsageEstimated, observation.CostUSD, observation.CostKnown, observation.RetryCount, observation.DuplicateWork, observation.ArtifactBytes, metadata, evidence, policyID(observation.Policy), policyVersion(observation.Policy), policyHash(observation.Policy))
	if err != nil {
		return contracts.RunObservation{}, false, fmt.Errorf("record observation: %w", err)
	}
	stored, err := readObservationTx(ctx, tx, observation.WorkspaceID, observation.IdempotencyKey)
	if err != nil {
		return contracts.RunObservation{}, false, err
	}
	if stored.PayloadHash != observation.PayloadHash {
		return contracts.RunObservation{}, false, fmt.Errorf("%w: %s", ErrObservationConflict, observation.IdempotencyKey)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.RunObservation{}, false, err
	}
	return stored, tag.RowsAffected() == 0, nil
}

// recordObservationTx is used by authoritative mutation stores when the
// observation must commit or roll back with the source transition.
func (s *ObservabilityStore) recordObservationTx(ctx context.Context, tx pgx.Tx, observation contracts.RunObservation) error {
	if s == nil {
		return nil
	}
	if err := observation.Normalize(); err != nil {
		return err
	}
	actor, _ := json.Marshal(observation.Actor)
	task, err := jsonOrEmpty(observation.Task)
	if err != nil {
		return err
	}
	session, err := jsonOrEmpty(observation.Session)
	if err != nil {
		return err
	}
	metadata, _ := json.Marshal(observation.Metadata)
	evidence := validEvidence(model.RedactBytes(observation.Evidence))
	if len(evidence) == 0 {
		evidence = []byte(`{}`)
	}
	_, err = tx.Exec(ctx, `INSERT INTO fornix.run_observations(id,workspace_id,idempotency_key,payload_hash,schema_version,kind,component,operation,outcome,actor,task_ref,session_ref,causation_id,correlation_id,source_kind,source_id,started_at,finished_at,duration_ms,db_queries,db_rows,input_bytes,output_bytes,input_tokens,output_tokens,total_tokens,usage_measured,usage_estimated,cost_usd,cost_known,retry_count,duplicate_work,artifact_bytes,metadata,evidence,policy_id,policy_version,policy_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34::jsonb,$35::jsonb,$36,$37,$38) ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, observation.ID, observation.WorkspaceID, observation.IdempotencyKey, observation.PayloadHash, observation.SchemaVersion, observation.Kind, observation.Component, observation.Operation, observation.Outcome, actor, task, session, observation.CausationID, observation.CorrelationID, observation.SourceKind, observation.SourceID, observation.StartedAt, observation.FinishedAt, observation.DurationMS, observation.DBQueries, observation.DBRows, observation.InputBytes, observation.OutputBytes, observation.InputTokens, observation.OutputTokens, observation.TotalTokens, observation.UsageMeasured, observation.UsageEstimated, observation.CostUSD, observation.CostKnown, observation.RetryCount, observation.DuplicateWork, observation.ArtifactBytes, metadata, evidence, policyID(observation.Policy), policyVersion(observation.Policy), policyHash(observation.Policy))
	if err != nil {
		return err
	}
	var existingHash string
	if err := tx.QueryRow(ctx, `SELECT payload_hash FROM fornix.run_observations WHERE workspace_id=$1 AND idempotency_key=$2`, observation.WorkspaceID, observation.IdempotencyKey).Scan(&existingHash); err != nil {
		return err
	}
	if existingHash != observation.PayloadHash {
		return fmt.Errorf("%w: %s", ErrObservationConflict, observation.IdempotencyKey)
	}
	return nil
}

func (s *ObservabilityStore) recordCostTx(ctx context.Context, tx pgx.Tx, entry contracts.CostLedgerEntry) error {
	if s == nil {
		return nil
	}
	if err := entry.Normalize(); err != nil {
		return err
	}
	actor, _ := json.Marshal(entry.Actor)
	task, err := jsonOrEmpty(entry.Task)
	if err != nil {
		return err
	}
	session, err := jsonOrEmpty(entry.Session)
	if err != nil {
		return err
	}
	metadata, _ := json.Marshal(entry.Metadata)
	_, err = tx.Exec(ctx, `INSERT INTO fornix.cost_ledger(id,workspace_id,idempotency_key,payload_hash,schema_version,category,basis,source_kind,source_id,actor,task_ref,session_ref,causation_id,correlation_id,units,unit_cost_usd,amount_usd,amount_known,measured,estimated,input_tokens,output_tokens,duration_ms,bytes,duplicate_work,metadata,policy_id,policy_version,policy_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26::jsonb,$27,$28,$29) ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, entry.ID, entry.WorkspaceID, entry.IdempotencyKey, entry.PayloadHash, entry.SchemaVersion, entry.Category, entry.Basis, entry.SourceKind, entry.SourceID, actor, task, session, entry.CausationID, entry.CorrelationID, entry.Units, entry.UnitCostUSD, entry.AmountUSD, entry.AmountKnown, entry.Measured, entry.Estimated, entry.InputTokens, entry.OutputTokens, entry.DurationMS, entry.Bytes, entry.DuplicateWork, metadata, policyID(entry.Policy), policyVersion(entry.Policy), policyHash(entry.Policy))
	if err != nil {
		return err
	}
	var existingHash string
	if err := tx.QueryRow(ctx, `SELECT payload_hash FROM fornix.cost_ledger WHERE workspace_id=$1 AND idempotency_key=$2`, entry.WorkspaceID, entry.IdempotencyKey).Scan(&existingHash); err != nil {
		return err
	}
	if existingHash != entry.PayloadHash {
		return fmt.Errorf("%w: %s", ErrCostConflict, entry.IdempotencyKey)
	}
	return nil
}

// RecordSpan appends one bounded trace span idempotently.
func (s *ObservabilityStore) RecordSpan(ctx context.Context, span contracts.TraceSpan) (contracts.TraceSpan, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.TraceSpan{}, false, fmt.Errorf("observability store is not configured")
	}
	if err := span.Normalize(); err != nil {
		return contracts.TraceSpan{}, false, err
	}
	attrs, _ := json.Marshal(span.Attributes)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.TraceSpan{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `INSERT INTO fornix.trace_spans(id,workspace_id,trace_id,parent_id,idempotency_key,schema_version,component,operation,outcome,started_at,finished_at,duration_ms,attributes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb) ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, span.ID, span.WorkspaceID, span.TraceID, span.ParentID, span.IdempotencyKey, span.SchemaVersion, span.Component, span.Operation, span.Outcome, span.StartedAt, span.FinishedAt, span.DurationMS, attrs)
	if err != nil {
		return contracts.TraceSpan{}, false, err
	}
	stored, err := readSpanTx(ctx, tx, span.WorkspaceID, span.IdempotencyKey)
	if err != nil {
		return contracts.TraceSpan{}, false, err
	}
	if stored.ID != span.ID && stored.IdempotencyKey == span.IdempotencyKey && stored.WorkspaceID == span.WorkspaceID { /* same logical delivery */
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.TraceSpan{}, false, err
	}
	return stored, tag.RowsAffected() == 0, nil
}

// RecordCost appends one cost attribution entry idempotently. Measured and
// estimated amounts remain distinct in the durable ledger.
func (s *ObservabilityStore) RecordCost(ctx context.Context, entry contracts.CostLedgerEntry) (contracts.CostLedgerEntry, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.CostLedgerEntry{}, false, fmt.Errorf("observability store is not configured")
	}
	if err := entry.Normalize(); err != nil {
		return contracts.CostLedgerEntry{}, false, err
	}
	actor, _ := json.Marshal(entry.Actor)
	task, err := jsonOrEmpty(entry.Task)
	if err != nil {
		return contracts.CostLedgerEntry{}, false, err
	}
	session, err := jsonOrEmpty(entry.Session)
	if err != nil {
		return contracts.CostLedgerEntry{}, false, err
	}
	metadata, _ := json.Marshal(entry.Metadata)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.CostLedgerEntry{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `INSERT INTO fornix.cost_ledger(id,workspace_id,idempotency_key,payload_hash,schema_version,category,basis,source_kind,source_id,actor,task_ref,session_ref,causation_id,correlation_id,units,unit_cost_usd,amount_usd,amount_known,measured,estimated,input_tokens,output_tokens,duration_ms,bytes,duplicate_work,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26::jsonb) ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, entry.ID, entry.WorkspaceID, entry.IdempotencyKey, entry.PayloadHash, entry.SchemaVersion, entry.Category, entry.Basis, entry.SourceKind, entry.SourceID, actor, task, session, entry.CausationID, entry.CorrelationID, entry.Units, entry.UnitCostUSD, entry.AmountUSD, entry.AmountKnown, entry.Measured, entry.Estimated, entry.InputTokens, entry.OutputTokens, entry.DurationMS, entry.Bytes, entry.DuplicateWork, metadata)
	if err != nil {
		return contracts.CostLedgerEntry{}, false, err
	}
	stored, err := readCostTx(ctx, tx, entry.WorkspaceID, entry.IdempotencyKey)
	if err != nil {
		return contracts.CostLedgerEntry{}, false, err
	}
	if stored.PayloadHash != entry.PayloadHash {
		return contracts.CostLedgerEntry{}, false, fmt.Errorf("%w: %s", ErrCostConflict, entry.IdempotencyKey)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.CostLedgerEntry{}, false, err
	}
	return stored, tag.RowsAffected() == 0, nil
}

// RecordMetric appends one bounded-dimension metric sample idempotently.
func (s *ObservabilityStore) RecordMetric(ctx context.Context, sample contracts.MetricSample) (contracts.MetricSample, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.MetricSample{}, false, fmt.Errorf("observability store is not configured")
	}
	if err := sample.Normalize(); err != nil {
		return contracts.MetricSample{}, false, err
	}
	dimensions, _ := json.Marshal(sample.Dimensions)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.MetricSample{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `INSERT INTO fornix.metric_samples(id,workspace_id,idempotency_key,schema_version,name,value,sample_count,observed_at,dimensions) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb) ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, sample.ID, sample.WorkspaceID, sample.IdempotencyKey, sample.SchemaVersion, sample.Name, sample.Value, sample.Count, sample.ObservedAt, dimensions)
	if err != nil {
		return contracts.MetricSample{}, false, err
	}
	stored, err := readMetricTx(ctx, tx, sample.WorkspaceID, sample.IdempotencyKey)
	if err != nil {
		return contracts.MetricSample{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.MetricSample{}, false, err
	}
	return stored, tag.RowsAffected() == 0, nil
}

// Snapshot returns a bounded workspace-scoped aggregate over the requested
// time window. Raw prompts, credentials, and unbounded labels are not exposed.
func (s *ObservabilityStore) Snapshot(ctx context.Context, workspaceID string, since, until time.Time) (contracts.ObservabilitySnapshot, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return contracts.ObservabilitySnapshot{}, fmt.Errorf("workspace_id is required")
	}
	if until.IsZero() {
		until = time.Now().UTC()
	}
	if since.IsZero() {
		since = until.Add(-24 * time.Hour)
	}
	if until.Before(since) || until.Sub(since) > contracts.MaxMetricWindow {
		return contracts.ObservabilitySnapshot{}, fmt.Errorf("metrics window is invalid")
	}
	var out contracts.ObservabilitySnapshot
	out.SchemaVersion, out.WorkspaceID, out.Since, out.Until = contracts.ObservabilitySchemaVersion, workspaceID, since, until
	if err := s.pool.QueryRow(ctx, `SELECT count(*),COALESCE(sum(duration_ms),0),COALESCE(sum(db_queries),0),COALESCE(sum(artifact_bytes),0),COALESCE(sum(CASE WHEN duplicate_work THEN 1 ELSE 0 END),0) FROM fornix.run_observations WHERE workspace_id=$1 AND created_at >= $2 AND created_at < $3`, workspaceID, since, until).Scan(&out.ObservationCount, &out.DurationMS, &out.DBQueries, &out.ArtifactBytes, &out.DuplicateWorkCount); err != nil {
		return contracts.ObservabilitySnapshot{}, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM fornix.trace_spans WHERE workspace_id=$1 AND created_at >= $2 AND created_at < $3`, workspaceID, since, until).Scan(&out.SpanCount); err != nil {
		return contracts.ObservabilitySnapshot{}, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM fornix.metric_samples WHERE workspace_id=$1 AND created_at >= $2 AND created_at < $3`, workspaceID, since, until).Scan(&out.MetricCount); err != nil {
		return contracts.ObservabilitySnapshot{}, err
	}
	rows, err := s.pool.Query(ctx, `SELECT component,operation,outcome,count(*),COALESCE(sum(duration_ms),0),COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms),0) FROM fornix.run_observations WHERE workspace_id=$1 AND created_at >= $2 AND created_at < $3 GROUP BY component,operation,outcome ORDER BY component,operation,outcome`, workspaceID, since, until)
	if err != nil {
		return contracts.ObservabilitySnapshot{}, err
	}
	for rows.Next() {
		var item contracts.MetricAggregate
		var total, sum int64
		var p95 float64
		if err := rows.Scan(&item.Component, &item.Operation, &item.Outcome, &total, &sum, &p95); err != nil {
			rows.Close()
			return contracts.ObservabilitySnapshot{}, err
		}
		item.Count, item.Value, item.P95MS = total, float64(sum), int64(p95)
		out.Operations = append(out.Operations, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return contracts.ObservabilitySnapshot{}, err
	}
	rows, err = s.pool.Query(ctx, `SELECT category,count(*),COALESCE(sum(amount_usd),0),COALESCE(sum(CASE WHEN measured THEN amount_usd ELSE 0 END),0),COALESCE(sum(CASE WHEN estimated THEN amount_usd ELSE 0 END),0),COALESCE(sum(CASE WHEN NOT amount_known THEN 1 ELSE 0 END),0),COALESCE(sum(bytes),0),COALESCE(sum(duration_ms),0) FROM fornix.cost_ledger WHERE workspace_id=$1 AND created_at >= $2 AND created_at < $3 GROUP BY category ORDER BY category`, workspaceID, since, until)
	if err != nil {
		return contracts.ObservabilitySnapshot{}, err
	}
	for rows.Next() {
		var item contracts.CostAggregate
		if err := rows.Scan(&item.Category, &item.Entries, &item.AmountUSD, &item.MeasuredUSD, &item.EstimatedUSD, &item.UnknownEntries, &item.Bytes, &item.DurationMS); err != nil {
			rows.Close()
			return contracts.ObservabilitySnapshot{}, err
		}
		out.Costs = append(out.Costs, item)
		out.MeasuredCostUSD += item.MeasuredUSD
		out.EstimatedCostUSD += item.EstimatedUSD
		out.UnknownCostEntries += item.UnknownEntries
	}
	rows.Close()
	return out, rows.Err()
}

func readObservationTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (contracts.RunObservation, error) {
	var o contracts.RunObservation
	var actor, task, session, metadata, evidence []byte
	var policyID, policyVersion, policyHash *string
	err := tx.QueryRow(ctx, `SELECT id,workspace_id,idempotency_key,payload_hash,schema_version,kind,component,operation,outcome,actor,task_ref,session_ref,causation_id,correlation_id,source_kind,source_id,started_at,finished_at,duration_ms,db_queries,db_rows,input_bytes,output_bytes,input_tokens,output_tokens,total_tokens,usage_measured,usage_estimated,cost_usd,cost_known,retry_count,duplicate_work,artifact_bytes,metadata,evidence,policy_id,policy_version,policy_hash FROM fornix.run_observations WHERE workspace_id=$1 AND idempotency_key=$2`, workspaceID, key).Scan(&o.ID, &o.WorkspaceID, &o.IdempotencyKey, &o.PayloadHash, &o.SchemaVersion, &o.Kind, &o.Component, &o.Operation, &o.Outcome, &actor, &task, &session, &o.CausationID, &o.CorrelationID, &o.SourceKind, &o.SourceID, &o.StartedAt, &o.FinishedAt, &o.DurationMS, &o.DBQueries, &o.DBRows, &o.InputBytes, &o.OutputBytes, &o.InputTokens, &o.OutputTokens, &o.TotalTokens, &o.UsageMeasured, &o.UsageEstimated, &o.CostUSD, &o.CostKnown, &o.RetryCount, &o.DuplicateWork, &o.ArtifactBytes, &metadata, &evidence, &policyID, &policyVersion, &policyHash)
	if err != nil {
		return o, err
	}
	_ = json.Unmarshal(actor, &o.Actor)
	o.Task, _ = decodeEntityRef(task)
	o.Session, _ = decodeEntityRef(session)
	o.Policy = policyReference(policyID, policyVersion, policyHash, o.WorkspaceID)
	_ = json.Unmarshal(metadata, &o.Metadata)
	o.Evidence = append([]byte(nil), evidence...)
	return o, nil
}
func readSpanTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (contracts.TraceSpan, error) {
	var s contracts.TraceSpan
	var attrs []byte
	err := tx.QueryRow(ctx, `SELECT id,workspace_id,trace_id,parent_id,idempotency_key,schema_version,component,operation,outcome,started_at,finished_at,duration_ms,attributes FROM fornix.trace_spans WHERE workspace_id=$1 AND idempotency_key=$2`, workspaceID, key).Scan(&s.ID, &s.WorkspaceID, &s.TraceID, &s.ParentID, &s.IdempotencyKey, &s.SchemaVersion, &s.Component, &s.Operation, &s.Outcome, &s.StartedAt, &s.FinishedAt, &s.DurationMS, &attrs)
	if err == nil {
		_ = json.Unmarshal(attrs, &s.Attributes)
	}
	return s, err
}
func readCostTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (contracts.CostLedgerEntry, error) {
	var c contracts.CostLedgerEntry
	var actor, task, session, metadata []byte
	var policyID, policyVersion, policyHash *string
	err := tx.QueryRow(ctx, `SELECT id,workspace_id,idempotency_key,payload_hash,schema_version,category,basis,source_kind,source_id,actor,task_ref,session_ref,causation_id,correlation_id,units,unit_cost_usd,amount_usd,amount_known,measured,estimated,input_tokens,output_tokens,duration_ms,bytes,duplicate_work,metadata,policy_id,policy_version,policy_hash FROM fornix.cost_ledger WHERE workspace_id=$1 AND idempotency_key=$2`, workspaceID, key).Scan(&c.ID, &c.WorkspaceID, &c.IdempotencyKey, &c.PayloadHash, &c.SchemaVersion, &c.Category, &c.Basis, &c.SourceKind, &c.SourceID, &actor, &task, &session, &c.CausationID, &c.CorrelationID, &c.Units, &c.UnitCostUSD, &c.AmountUSD, &c.AmountKnown, &c.Measured, &c.Estimated, &c.InputTokens, &c.OutputTokens, &c.DurationMS, &c.Bytes, &c.DuplicateWork, &metadata, &policyID, &policyVersion, &policyHash)
	if err != nil {
		return c, err
	}
	_ = json.Unmarshal(actor, &c.Actor)
	c.Task, _ = decodeEntityRef(task)
	c.Session, _ = decodeEntityRef(session)
	c.Policy = policyReference(policyID, policyVersion, policyHash, c.WorkspaceID)
	_ = json.Unmarshal(metadata, &c.Metadata)
	return c, nil
}
func readMetricTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (contracts.MetricSample, error) {
	var m contracts.MetricSample
	var dimensions []byte
	err := tx.QueryRow(ctx, `SELECT id,workspace_id,idempotency_key,schema_version,name,value,sample_count,observed_at,dimensions FROM fornix.metric_samples WHERE workspace_id=$1 AND idempotency_key=$2`, workspaceID, key).Scan(&m.ID, &m.WorkspaceID, &m.IdempotencyKey, &m.SchemaVersion, &m.Name, &m.Value, &m.Count, &m.ObservedAt, &dimensions)
	if err == nil {
		_ = json.Unmarshal(dimensions, &m.Dimensions)
	}
	return m, err
}
