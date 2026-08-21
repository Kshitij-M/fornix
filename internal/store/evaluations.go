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
)

// EvaluationStore persists bounded, replay-only evaluation datasets, runs,
// and results. It never executes an external model or tool.
type EvaluationStore struct {
	pool      *pgxpool.Pool
	artifacts *ArtifactStore
}

// NewEvaluationStore creates the Postgres evaluation store.
func NewEvaluationStore(pool *pgxpool.Pool) *EvaluationStore {
	return &EvaluationStore{pool: pool, artifacts: NewArtifactStore(pool)}
}

// CreateDataset idempotently registers an immutable versioned evaluation
// dataset and rejects conflicting reuse of its identity.
func (s *EvaluationStore) CreateDataset(ctx context.Context, dataset contracts.EvalDataset) (contracts.EvalDataset, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.EvalDataset{}, false, fmt.Errorf("evaluation store is not configured")
	}
	if err := dataset.Normalize(); err != nil {
		return contracts.EvalDataset{}, false, err
	}
	cases, _ := json.Marshal(dataset.Cases)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.EvalDataset{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `INSERT INTO fornix.eval_datasets(id,workspace_id,name,version,schema_version,dataset_hash,cases) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb) ON CONFLICT (workspace_id,name,version) DO NOTHING`, dataset.ID, dataset.WorkspaceID, dataset.Name, dataset.Version, dataset.SchemaVersion, dataset.DatasetHash, cases)
	if err != nil {
		return contracts.EvalDataset{}, false, fmt.Errorf("create eval dataset: %w", err)
	}
	stored, err := readDatasetTx(ctx, tx, dataset.WorkspaceID, dataset.Name, dataset.Version)
	if err != nil {
		return contracts.EvalDataset{}, false, err
	}
	if stored.DatasetHash != dataset.DatasetHash {
		return contracts.EvalDataset{}, false, fmt.Errorf("%w: dataset %s/%d", ErrEvalConflict, dataset.Name, dataset.Version)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EvalDataset{}, false, err
	}
	return stored, tag.RowsAffected() == 1, nil
}

// GetDataset reads a versioned dataset within one workspace.
func (s *EvaluationStore) GetDataset(ctx context.Context, workspaceID, name string, version int) (contracts.EvalDataset, error) {
	if s == nil || s.pool == nil {
		return contracts.EvalDataset{}, fmt.Errorf("evaluation store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.EvalDataset{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	dataset, err := readDatasetTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(name), version)
	if err != nil {
		return contracts.EvalDataset{}, err
	}
	_ = tx.Commit(ctx)
	return dataset, nil
}

// GetDatasetByID reads a dataset by its workspace-scoped identity.
func (s *EvaluationStore) GetDatasetByID(ctx context.Context, workspaceID, id string) (contracts.EvalDataset, error) {
	if s == nil || s.pool == nil {
		return contracts.EvalDataset{}, fmt.Errorf("evaluation store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.EvalDataset{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var dataset contracts.EvalDataset
	var cases []byte
	err = tx.QueryRow(ctx, `SELECT id,workspace_id,name,version,schema_version,dataset_hash,cases,created_at FROM fornix.eval_datasets WHERE workspace_id=$1 AND id=$2`, strings.TrimSpace(workspaceID), strings.TrimSpace(id)).Scan(&dataset.ID, &dataset.WorkspaceID, &dataset.Name, &dataset.Version, &dataset.SchemaVersion, &dataset.DatasetHash, &cases, &dataset.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.EvalDataset{}, ErrEvalNotFound
	}
	if err != nil {
		return contracts.EvalDataset{}, err
	}
	if err := json.Unmarshal(cases, &dataset.Cases); err != nil {
		return contracts.EvalDataset{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EvalDataset{}, err
	}
	return dataset, nil
}

// StartRun creates or replays an idempotent bounded evaluation run.
func (s *EvaluationStore) StartRun(ctx context.Context, run contracts.EvalRun) (contracts.EvalRun, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.EvalRun{}, false, fmt.Errorf("evaluation store is not configured")
	}
	if err := run.Normalize(); err != nil {
		return contracts.EvalRun{}, false, err
	}
	gates, _ := json.Marshal(run.Gates)
	quality, _ := json.Marshal(run.RetrievalQuality)
	regressions, _ := json.Marshal(run.Regressions)
	if string(quality) == "null" {
		quality = []byte(`{}`)
	}
	if string(regressions) == "null" {
		regressions = []byte(`[]`)
	}
	report := validEvidence(run.Report)
	if len(report) == 0 {
		report = []byte(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.EvalRun{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `INSERT INTO fornix.eval_runs(id,workspace_id,dataset_id,dataset_hash,idempotency_key,request_hash,schema_version,status,dry_run,batch_limit,cases_total,cases_completed,cases_passed,cases_failed,cost_usd,cost_known,replay_hash,gates,retrieval_quality,regressions,baseline_eval_run_id,report) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19::jsonb,$20::jsonb,$21,$22::jsonb) ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, run.ID, run.WorkspaceID, run.DatasetID, run.DatasetHash, run.IdempotencyKey, run.RequestHash, run.SchemaVersion, run.Status, run.DryRun, run.BatchLimit, run.CasesTotal, run.CasesCompleted, run.CasesPassed, run.CasesFailed, run.CostUSD, run.CostKnown, run.ReplayHash, gates, quality, regressions, run.BaselineEvalRunID, report)
	if err != nil {
		return contracts.EvalRun{}, false, err
	}
	stored, err := readEvalRunTx(ctx, tx, run.WorkspaceID, run.IdempotencyKey)
	if err != nil {
		return contracts.EvalRun{}, false, err
	}
	if stored.RequestHash != run.RequestHash {
		return contracts.EvalRun{}, false, fmt.Errorf("%w: eval run %s", ErrEvalConflict, run.IdempotencyKey)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EvalRun{}, false, err
	}
	return stored, tag.RowsAffected() == 1, nil
}

// GetRun reads one evaluation run and its optional artifact-backed report.
func (s *EvaluationStore) GetRun(ctx context.Context, workspaceID, id string) (contracts.EvalRun, error) {
	if s == nil || s.pool == nil {
		return contracts.EvalRun{}, fmt.Errorf("evaluation store is not configured")
	}
	var run contracts.EvalRun
	var gates, quality, regressions, report []byte
	var reportArtifactID *int64
	var finished *time.Time
	err := s.pool.QueryRow(ctx, `SELECT id,workspace_id,dataset_id,dataset_hash,idempotency_key,request_hash,schema_version,status,dry_run,batch_limit,cases_total,cases_completed,cases_passed,cases_failed,cost_usd,cost_known,replay_hash,gates,retrieval_quality,regressions,baseline_eval_run_id,report,report_artifact_id,created_at,finished_at FROM fornix.eval_runs WHERE workspace_id=$1 AND id=$2`, workspaceID, id).Scan(&run.ID, &run.WorkspaceID, &run.DatasetID, &run.DatasetHash, &run.IdempotencyKey, &run.RequestHash, &run.SchemaVersion, &run.Status, &run.DryRun, &run.BatchLimit, &run.CasesTotal, &run.CasesCompleted, &run.CasesPassed, &run.CasesFailed, &run.CostUSD, &run.CostKnown, &run.ReplayHash, &gates, &quality, &regressions, &run.BaselineEvalRunID, &report, &reportArtifactID, &run.CreatedAt, &finished)
	if errors.Is(err, pgx.ErrNoRows) {
		return run, ErrEvalNotFound
	}
	if err != nil {
		return run, err
	}
	_ = json.Unmarshal(gates, &run.Gates)
	_ = json.Unmarshal(quality, &run.RetrievalQuality)
	_ = json.Unmarshal(regressions, &run.Regressions)
	run.Report = append([]byte(nil), report...)
	run.FinishedAt = finished
	if reportArtifactID != nil && *reportArtifactID > 0 && s.artifacts != nil {
		artifact, err := s.artifacts.Get(ctx, workspaceID, *reportArtifactID)
		if err == nil {
			ref, refErr := readArtifactRef(ctx, s.pool, `WHERE r.workspace_id=$1 AND r.artifact_id=$2 AND r.source_kind='eval_run' AND r.source_id=$3 AND r.role='report'`, workspaceID, artifact.ID, id)
			if refErr == nil {
				run.ReportArtifact = &ref
			}
		}
	}
	return run, nil
}

// RecordResult idempotently stores one deterministic case result and updates
// aggregate counters only for a newly inserted case.
func (s *EvaluationStore) RecordResult(ctx context.Context, result contracts.EvalResult) (contracts.EvalResult, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.EvalResult{}, false, fmt.Errorf("evaluation store is not configured")
	}
	if err := result.Normalize(); err != nil {
		return contracts.EvalResult{}, false, err
	}
	gates, _ := json.Marshal(result.Gates)
	metrics, _ := json.Marshal(result.RetrievalMetrics)
	resolved, _ := json.Marshal(result.ResolvedEvidence)
	regressions, _ := json.Marshal(result.Regressions)
	if string(metrics) == "null" {
		metrics = []byte(`{}`)
	}
	if string(resolved) == "null" {
		resolved = []byte(`[]`)
	}
	if string(regressions) == "null" {
		regressions = []byte(`[]`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.EvalResult{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `INSERT INTO fornix.eval_results(id,workspace_id,eval_run_id,case_id,replay_run_id,schema_version,input_hash,context_hash,termination,observed_cost_usd,cost_known,replay_hash,passed,abstained,gates,retrieval_metrics,resolved_evidence,regressions,error) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16::jsonb,$17::jsonb,$18::jsonb,$19) ON CONFLICT (workspace_id,eval_run_id,case_id) DO NOTHING`, result.ID, result.WorkspaceID, result.EvalRunID, result.CaseID, result.ReplayRunID, result.SchemaVersion, result.InputHash, result.ContextHash, result.Termination, result.ObservedCostUSD, result.CostKnown, result.ReplayHash, result.Passed, result.Abstained, gates, metrics, resolved, regressions, result.Error)
	if err != nil {
		return contracts.EvalResult{}, false, err
	}
	stored, err := readEvalResultTx(ctx, tx, result.WorkspaceID, result.EvalRunID, result.CaseID)
	if err != nil {
		return contracts.EvalResult{}, false, err
	}
	if stored.ReplayHash != result.ReplayHash {
		return contracts.EvalResult{}, false, fmt.Errorf("%w: result %s", ErrEvalConflict, result.CaseID)
	}
	if tag.RowsAffected() == 1 {
		if _, err := tx.Exec(ctx, `UPDATE fornix.eval_runs SET cases_completed=cases_completed+1,cases_passed=cases_passed+$3,cases_failed=cases_failed+$4,cost_usd=cost_usd+$5,cost_known=cost_known AND $6 WHERE workspace_id=$1 AND id=$2`, result.WorkspaceID, result.EvalRunID, boolInt(result.Passed), boolInt(!result.Passed), result.ObservedCostUSD, result.CostKnown); err != nil {
			return contracts.EvalResult{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EvalResult{}, false, err
	}
	return stored, tag.RowsAffected() == 1, nil
}

// FinishRun atomically records terminal status and, for oversized reports,
// creates a content-addressed artifact in the same transaction.
func (s *EvaluationStore) FinishRun(ctx context.Context, run contracts.EvalRun, report []byte) (contracts.EvalRun, error) {
	if s == nil || s.pool == nil {
		return contracts.EvalRun{}, fmt.Errorf("evaluation store is not configured")
	}
	if run.Status != contracts.EvalRunFailed && run.Status != contracts.EvalRunCancelled {
		run.Status = contracts.EvalRunSucceeded
	}
	if len(report) > contracts.MaxEvalInlineBytes {
		run.Report = nil
	} else {
		run.Report = append([]byte(nil), report...)
	}
	now := time.Now().UTC()
	run.FinishedAt = &now
	gates, _ := json.Marshal(run.Gates)
	quality, _ := json.Marshal(run.RetrievalQuality)
	regressions, _ := json.Marshal(run.Regressions)
	if string(quality) == "null" {
		quality = []byte(`{}`)
	}
	if string(regressions) == "null" {
		regressions = []byte(`[]`)
	}
	inline := validEvidence(run.Report)
	if len(inline) == 0 {
		inline = []byte(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.EvalRun{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var artifactID any
	if len(report) > contracts.MaxEvalInlineBytes && s.artifacts != nil {
		artifact, err := s.artifacts.PutTx(ctx, tx, ArtifactPutInput{WorkspaceID: run.WorkspaceID, Kind: "evaluation-report", MediaType: "application/json", Raw: report, Manifest: contracts.ArtifactManifest{Gist: "deterministic evaluation report", Metadata: map[string]string{"eval_run_id": run.ID}}, SourceKind: "eval_run", SourceID: run.ID, Role: "report", IdempotencyKey: "eval-report:" + run.ID, Actor: contracts.ActorRef{WorkspaceID: run.WorkspaceID}})
		if err != nil {
			return contracts.EvalRun{}, err
		}
		artifactID = artifact.Artifact.ID
		run.ReportArtifact = &artifact.Reference
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.eval_runs SET status=$1,cases_total=$2,cases_completed=$3,cases_passed=$4,cases_failed=$5,cost_usd=$6,cost_known=$7,replay_hash=$8,gates=$9::jsonb,retrieval_quality=$10::jsonb,regressions=$11::jsonb,baseline_eval_run_id=$12,report=$13::jsonb,report_artifact_id=$14,finished_at=$15 WHERE workspace_id=$16 AND id=$17`, run.Status, run.CasesTotal, run.CasesCompleted, run.CasesPassed, run.CasesFailed, run.CostUSD, run.CostKnown, run.ReplayHash, gates, quality, regressions, run.BaselineEvalRunID, inline, artifactID, now, run.WorkspaceID, run.ID); err != nil {
		return contracts.EvalRun{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EvalRun{}, err
	}
	return run, nil
}

func readDatasetTx(ctx context.Context, tx pgx.Tx, workspaceID, name string, version int) (contracts.EvalDataset, error) {
	var d contracts.EvalDataset
	var cases []byte
	err := tx.QueryRow(ctx, `SELECT id,workspace_id,name,version,schema_version,dataset_hash,cases,created_at FROM fornix.eval_datasets WHERE workspace_id=$1 AND name=$2 AND version=$3`, workspaceID, name, version).Scan(&d.ID, &d.WorkspaceID, &d.Name, &d.Version, &d.SchemaVersion, &d.DatasetHash, &cases, &d.CreatedAt)
	if err != nil {
		return d, err
	}
	if err := json.Unmarshal(cases, &d.Cases); err != nil {
		return d, err
	}
	return d, nil
}
func readEvalRunTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (contracts.EvalRun, error) {
	var r contracts.EvalRun
	var gates, quality, regressions, report []byte
	var finished *time.Time
	err := tx.QueryRow(ctx, `SELECT id,workspace_id,dataset_id,dataset_hash,idempotency_key,request_hash,schema_version,status,dry_run,batch_limit,cases_total,cases_completed,cases_passed,cases_failed,cost_usd,cost_known,replay_hash,gates,retrieval_quality,regressions,baseline_eval_run_id,report,created_at,finished_at FROM fornix.eval_runs WHERE workspace_id=$1 AND idempotency_key=$2`, workspaceID, key).Scan(&r.ID, &r.WorkspaceID, &r.DatasetID, &r.DatasetHash, &r.IdempotencyKey, &r.RequestHash, &r.SchemaVersion, &r.Status, &r.DryRun, &r.BatchLimit, &r.CasesTotal, &r.CasesCompleted, &r.CasesPassed, &r.CasesFailed, &r.CostUSD, &r.CostKnown, &r.ReplayHash, &gates, &quality, &regressions, &r.BaselineEvalRunID, &report, &r.CreatedAt, &finished)
	if err != nil {
		return r, err
	}
	_ = json.Unmarshal(gates, &r.Gates)
	_ = json.Unmarshal(quality, &r.RetrievalQuality)
	_ = json.Unmarshal(regressions, &r.Regressions)
	r.Report = append([]byte(nil), report...)
	r.FinishedAt = finished
	return r, nil
}
func readEvalResultTx(ctx context.Context, tx pgx.Tx, workspaceID, runID, caseID string) (contracts.EvalResult, error) {
	var r contracts.EvalResult
	var gates, metrics, resolved, regressions []byte
	err := tx.QueryRow(ctx, `SELECT id,workspace_id,eval_run_id,case_id,replay_run_id,schema_version,input_hash,context_hash,termination,observed_cost_usd,cost_known,replay_hash,passed,abstained,gates,retrieval_metrics,resolved_evidence,regressions,error,created_at FROM fornix.eval_results WHERE workspace_id=$1 AND eval_run_id=$2 AND case_id=$3`, workspaceID, runID, caseID).Scan(&r.ID, &r.WorkspaceID, &r.EvalRunID, &r.CaseID, &r.ReplayRunID, &r.SchemaVersion, &r.InputHash, &r.ContextHash, &r.Termination, &r.ObservedCostUSD, &r.CostKnown, &r.ReplayHash, &r.Passed, &r.Abstained, &gates, &metrics, &resolved, &regressions, &r.Error, &r.CreatedAt)
	if err != nil {
		return r, err
	}
	_ = json.Unmarshal(gates, &r.Gates)
	_ = json.Unmarshal(metrics, &r.RetrievalMetrics)
	_ = json.Unmarshal(resolved, &r.ResolvedEvidence)
	_ = json.Unmarshal(regressions, &r.Regressions)
	return r, nil
}
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
