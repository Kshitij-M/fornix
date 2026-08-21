package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/omaveda/fornix/internal/contracts"
)

// EvaluateRetrievalDataset evaluates recorded context packs only. The caller
// supplies the recorded surfaces keyed by case ID; the runner never calls the
// live retriever, a model provider, or an external tool.
func (r Runner) EvaluateRetrievalDataset(ctx context.Context, dataset contracts.EvalDataset, run contracts.EvalRun, surfaces map[string]RetrievalScoreInput, policy RegressionPolicy) (contracts.EvalRun, []contracts.EvalResult, error) {
	if r.Evaluations == nil {
		return contracts.EvalRun{}, nil, fmt.Errorf("evaluation store is required")
	}
	if err := dataset.Normalize(); err != nil {
		return contracts.EvalRun{}, nil, err
	}
	if run.WorkspaceID == "" {
		run.WorkspaceID = dataset.WorkspaceID
	}
	if run.DatasetID == "" {
		run.DatasetID = dataset.ID
	}
	if run.DatasetHash == "" {
		run.DatasetHash = dataset.DatasetHash
	}
	if run.RequestHash == "" {
		digest := sha256.Sum256([]byte(dataset.DatasetHash + "\x00" + run.IdempotencyKey))
		run.RequestHash = hex.EncodeToString(digest[:])
	}
	if run.WorkspaceID != dataset.WorkspaceID || run.DatasetID != dataset.ID || run.DatasetHash != dataset.DatasetHash {
		return contracts.EvalRun{}, nil, fmt.Errorf("evaluation dataset and run workspace or identity mismatch")
	}
	if err := run.Normalize(); err != nil {
		return contracts.EvalRun{}, nil, err
	}
	if run.BaselineEvalRunID != "" && r.Evaluations != nil {
		baselineRun, err := r.Evaluations.GetRun(ctx, run.WorkspaceID, run.BaselineEvalRunID)
		if err != nil {
			return contracts.EvalRun{}, nil, fmt.Errorf("read baseline evaluation run: %w", err)
		}
		if baselineRun.RetrievalQuality == nil {
			return contracts.EvalRun{}, nil, fmt.Errorf("baseline evaluation run has no retrieval quality summary")
		}
	}

	cases := append([]contracts.EvalCase(nil), dataset.Cases...)
	sort.Slice(cases, func(i, j int) bool { return cases[i].ID < cases[j].ID })
	if len(cases) > run.BatchLimit {
		cases = cases[:run.BatchLimit]
	}
	results := make([]contracts.EvalResult, 0, len(cases))
	metrics := make([]contracts.RetrievalQualityMetrics, 0, len(cases))
	for _, c := range cases {
		input, ok := surfaces[c.ID]
		if !ok {
			return contracts.EvalRun{}, nil, fmt.Errorf("recorded retrieval surface %q is missing", c.ID)
		}
		input.WorkspaceID = dataset.WorkspaceID
		input.Case = c
		score, err := (RetrievalScorer{Evidence: r.Evidence}).ScoreCase(ctx, input, nil, nil, policy)
		if err != nil {
			return contracts.EvalRun{}, nil, fmt.Errorf("score retrieval case %s: %w", c.ID, err)
		}
		result, err := score.Result(run.ID, input)
		if err != nil {
			return contracts.EvalRun{}, nil, err
		}
		results = append(results, result)
		metrics = append(metrics, score.Metrics)
	}

	run.CasesTotal = len(results)
	run.CasesCompleted = len(results)
	run.CasesPassed = 0
	run.CasesFailed = 0
	run.CostUSD = 0
	run.CostKnown = true
	for _, result := range results {
		if result.Passed {
			run.CasesPassed++
		} else {
			run.CasesFailed++
		}
		run.CostUSD += result.ObservedCostUSD
		run.CostKnown = run.CostKnown && result.CostKnown
	}
	quality := SummarizeRetrieval(metrics)
	run.RetrievalQuality = &quality
	run.Gates = EvaluateRetrievalSummaryGates(quality, run.Gates)
	run.Regressions = nil
	if run.BaselineEvalRunID != "" {
		baselineRun, err := r.Evaluations.GetRun(ctx, run.WorkspaceID, run.BaselineEvalRunID)
		if err != nil {
			return contracts.EvalRun{}, nil, err
		}
		run.Regressions = CompareRetrievalSummary(*baselineRun.RetrievalQuality, quality, policy)
	}
	run.ReplayHash = hashResults(results)
	if anyGateFailed(run.Gates) || !allRegressionsPassed(run.Regressions) || run.CasesFailed > 0 {
		run.Status = contracts.EvalRunFailed
	} else {
		run.Status = contracts.EvalRunSucceeded
	}
	run.Report = retrievalReportBytes(run, results)
	if run.DryRun {
		return run, results, nil
	}
	stored, _, err := r.Evaluations.StartRun(ctx, run)
	if err != nil {
		return contracts.EvalRun{}, nil, err
	}
	for i := range results {
		results[i].EvalRunID = stored.ID
		if err := results[i].Normalize(); err != nil {
			return contracts.EvalRun{}, nil, err
		}
		result := results[i]
		if _, _, err := r.Evaluations.RecordResult(ctx, result); err != nil {
			return contracts.EvalRun{}, nil, err
		}
	}
	stored.CasesTotal, stored.CasesCompleted, stored.CasesPassed, stored.CasesFailed = run.CasesTotal, run.CasesCompleted, run.CasesPassed, run.CasesFailed
	stored.CostUSD, stored.CostKnown, stored.ReplayHash = run.CostUSD, run.CostKnown, run.ReplayHash
	stored.Gates, stored.RetrievalQuality, stored.Regressions = run.Gates, run.RetrievalQuality, run.Regressions
	stored.BaselineEvalRunID = run.BaselineEvalRunID
	stored.Status = run.Status
	finished, err := r.Evaluations.FinishRun(ctx, stored, run.Report)
	if err != nil {
		return contracts.EvalRun{}, nil, err
	}
	return finished, results, nil
}

func retrievalReportBytes(run contracts.EvalRun, results []contracts.EvalResult) []byte {
	type resultSummary struct {
		CaseID  string                             `json:"case_id"`
		Hash    string                             `json:"result_hash"`
		Passed  bool                               `json:"passed"`
		Metrics *contracts.RetrievalQualityMetrics `json:"metrics,omitempty"`
	}
	summaries := make([]resultSummary, 0, len(results))
	for _, result := range results {
		summaries = append(summaries, resultSummary{CaseID: result.CaseID, Hash: result.ReplayHash, Passed: result.Passed, Metrics: result.RetrievalMetrics})
	}
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].CaseID < summaries[j].CaseID })
	report, _ := json.Marshal(struct {
		SchemaVersion int                                `json:"schema_version"`
		RunID         string                             `json:"run_id"`
		DatasetHash   string                             `json:"dataset_hash"`
		ReplayHash    string                             `json:"replay_hash"`
		Quality       *contracts.RetrievalQualitySummary `json:"retrieval_quality,omitempty"`
		Regressions   []contracts.RegressionFinding      `json:"regressions,omitempty"`
		Cases         []resultSummary                    `json:"cases"`
		Gates         []contracts.QualityGate            `json:"gates,omitempty"`
	}{contracts.ObservabilitySchemaVersion, run.ID, run.DatasetHash, run.ReplayHash, run.RetrievalQuality, run.Regressions, summaries, run.Gates})
	return report
}
