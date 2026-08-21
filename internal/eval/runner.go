package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// Runner evaluates recorded runs only. It deliberately accepts an
// AgentRunStore, not a model or tool runtime, so evaluation cannot create an
// external effect. A dry run does not write any evaluation rows or artifacts.
type Runner struct {
	Runs        *store.AgentRunStore
	Evaluations *store.EvaluationStore
	Evidence    *store.EvidenceStore
}

// EvaluateDataset replays at most run.BatchLimit cases in canonical case-ID
// order. The same dataset, run identity, and durable history therefore produce
// the same result and report hashes across processes.
func (r Runner) EvaluateDataset(ctx context.Context, dataset contracts.EvalDataset, run contracts.EvalRun) (contracts.EvalRun, []contracts.EvalResult, error) {
	if r.Runs == nil {
		return contracts.EvalRun{}, nil, fmt.Errorf("agent run store is required")
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
		raw := []byte(dataset.DatasetHash + "\x00" + run.IdempotencyKey)
		digest := sha256.Sum256(raw)
		run.RequestHash = hex.EncodeToString(digest[:])
	}
	if run.WorkspaceID != dataset.WorkspaceID || run.DatasetID != dataset.ID || run.DatasetHash != dataset.DatasetHash {
		return contracts.EvalRun{}, nil, fmt.Errorf("evaluation dataset and run workspace or identity mismatch")
	}
	if err := run.Normalize(); err != nil {
		return contracts.EvalRun{}, nil, err
	}

	cases := append([]contracts.EvalCase(nil), dataset.Cases...)
	sort.Slice(cases, func(i, j int) bool { return cases[i].ID < cases[j].ID })
	if len(cases) > run.BatchLimit {
		cases = cases[:run.BatchLimit]
	}
	run.CasesTotal = len(cases)
	results := make([]contracts.EvalResult, 0, len(cases))
	for _, c := range cases {
		result, err := ReplayCase(ctx, r.Runs, dataset.WorkspaceID, c)
		if err != nil {
			return contracts.EvalRun{}, nil, err
		}
		if len(c.GoldEvidence) > 0 {
			if r.Evidence == nil {
				return contracts.EvalRun{}, nil, fmt.Errorf("authoritative evidence store is required to resolve gold evidence for case %s", c.ID)
			}
			resolved, err := r.Evidence.ResolveEvidenceHashes(ctx, dataset.WorkspaceID, c.GoldEvidence)
			if err != nil {
				return contracts.EvalRun{}, nil, fmt.Errorf("resolve gold evidence for case %s: %w", c.ID, err)
			}
			result.ResolvedEvidence = make([]string, 0, len(resolved))
			for _, evidence := range resolved {
				result.ResolvedEvidence = append(result.ResolvedEvidence, evidence.EvidenceHash)
			}
		}
		result.EvalRunID = run.ID
		if err := result.Normalize(); err != nil {
			return contracts.EvalRun{}, nil, err
		}
		results = append(results, result)
	}

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
	run.ReplayHash = hashResults(results)
	run.Gates = evaluateGates(run.Gates, run.CasesPassed, run.CasesCompleted, run.CostUSD)
	if anyGateFailed(run.Gates) {
		run.Status = contracts.EvalRunFailed
	} else {
		run.Status = contracts.EvalRunSucceeded
	}
	run.Report = reportBytes(run, results)

	if run.DryRun {
		return run, results, nil
	}
	if r.Evaluations == nil {
		return contracts.EvalRun{}, nil, fmt.Errorf("evaluation store is required for a non-dry run")
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
	stored.CostUSD, stored.CostKnown, stored.ReplayHash, stored.Gates = run.CostUSD, run.CostKnown, run.ReplayHash, run.Gates
	stored.Status = run.Status
	finished, err := r.Evaluations.FinishRun(ctx, stored, run.Report)
	if err != nil {
		return contracts.EvalRun{}, nil, err
	}
	return finished, results, nil
}

func hashResults(results []contracts.EvalResult) string {
	canonical := make([]struct {
		CaseID string `json:"case_id"`
		Hash   string `json:"replay_hash"`
		Passed bool   `json:"passed"`
	}, 0, len(results))
	for _, result := range results {
		canonical = append(canonical, struct {
			CaseID string `json:"case_id"`
			Hash   string `json:"replay_hash"`
			Passed bool   `json:"passed"`
		}{result.CaseID, result.ReplayHash, result.Passed})
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].CaseID < canonical[j].CaseID })
	raw, _ := json.Marshal(canonical)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func evaluateGates(gates []contracts.QualityGate, passed, total int, cost float64) []contracts.QualityGate {
	out := append([]contracts.QualityGate(nil), gates...)
	for i := range out {
		actual := out[i].Actual
		switch out[i].Metric {
		case "pass_rate":
			if total > 0 {
				actual = float64(passed) / float64(total)
			} else {
				actual = 0
			}
		case "cases_completed":
			actual = float64(total)
		case "cost_usd":
			actual = cost
		}
		out[i].Actual = actual
		out[i].Passed = gate("quality", out[i].Operator, out[i].Threshold, actual, out[i].Reason).Passed
	}
	return out
}

func anyGateFailed(gates []contracts.QualityGate) bool {
	for _, gate := range gates {
		if !gate.Passed {
			return true
		}
	}
	return false
}

func reportBytes(run contracts.EvalRun, results []contracts.EvalResult) []byte {
	type resultSummary struct {
		CaseID string `json:"case_id"`
		Hash   string `json:"replay_hash"`
		Passed bool   `json:"passed"`
	}
	summaries := make([]resultSummary, 0, len(results))
	for _, result := range results {
		summaries = append(summaries, resultSummary{CaseID: result.CaseID, Hash: result.ReplayHash, Passed: result.Passed})
	}
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].CaseID < summaries[j].CaseID })
	report, _ := json.Marshal(struct {
		SchemaVersion int                     `json:"schema_version"`
		RunID         string                  `json:"run_id"`
		DatasetHash   string                  `json:"dataset_hash"`
		ReplayHash    string                  `json:"replay_hash"`
		Cases         []resultSummary         `json:"cases"`
		Gates         []contracts.QualityGate `json:"gates,omitempty"`
	}{contracts.ObservabilitySchemaVersion, run.ID, run.DatasetHash, run.ReplayHash, summaries, run.Gates})
	return report
}
