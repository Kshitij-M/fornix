package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// ReplayCase reads only the durable agent checkpoint/event history and model
// or tool ledger projections already referenced by that run. It deliberately
// has no provider or process dependency, so calling it cannot cause an
// external effect.
func ReplayCase(ctx context.Context, runs *store.AgentRunStore, workspaceID string, c contracts.EvalCase) (contracts.EvalResult, error) {
	if runs == nil {
		return contracts.EvalResult{}, fmt.Errorf("agent run store is required")
	}
	workspaceID, c.ReplayRunID = strings.TrimSpace(workspaceID), strings.TrimSpace(c.ReplayRunID)
	if workspaceID == "" || c.ReplayRunID == "" {
		return contracts.EvalResult{}, fmt.Errorf("workspace_id and replay_run_id are required")
	}
	run, err := runs.Get(ctx, workspaceID, c.ReplayRunID)
	if err != nil {
		return contracts.EvalResult{}, err
	}
	checkpoint, replayErr := runs.ReplayCheckpoint(ctx, workspaceID, c.ReplayRunID, 0, 1000)
	gates := make([]contracts.QualityGate, 0, 4)
	if replayErr != nil {
		gates = append(gates, failedGate("replay_checkpoint", "==", 1, 0, replayErr.Error()))
	} else {
		gates = append(gates, gate("state_hash", "==", 1, boolFloat(checkpoint.StateHash == run.StateHash), "checkpoint matches durable run"))
	}
	inputMatch := c.InputHash == run.RequestHash || c.InputHash == run.ID
	gates = append(gates, gate("input_hash", "==", 1, boolFloat(inputMatch), "recorded input identity matches"))
	if c.ExpectedContextHash != "" {
		gates = append(gates, gate("context_hash", "==", 1, boolFloat(c.ExpectedContextHash == run.ContextHash), "expected context hash"))
	}
	if c.ExpectedTermination != "" {
		gates = append(gates, gate("termination", "==", 1, boolFloat(c.ExpectedTermination == run.Termination), "expected termination"))
	}
	abstained := run.Termination == contracts.AgentTerminationAbstained
	if c.ExpectedAbstention {
		gates = append(gates, gate("abstention", "==", 1, boolFloat(abstained), "expected abstention"))
	}
	if len(c.GoldEvidence) > 0 {
		valid := true
		for _, reference := range c.GoldEvidence {
			if len(reference) != 64 {
				valid = false
				break
			}
			for _, character := range reference {
				if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
					valid = false
					break
				}
			}
			if !valid {
				break
			}
		}
		gates = append(gates, gate("gold_evidence_references", "==", 1, boolFloat(valid), "gold evidence references are canonical hashes"))
	}
	costKnown := run.Cost.TotalCostUSD >= 0 && strings.TrimSpace(run.Cost.Source) != ""
	if c.MaxCostUSD > 0 {
		gates = append(gates, gate("cost_usd", "<=", c.MaxCostUSD, run.Cost.TotalCostUSD, "configured cost budget"))
	}
	passed := true
	for _, g := range gates {
		if !g.Passed {
			passed = false
			break
		}
	}
	material := struct {
		Workspace, RunID, RequestHash, ContextHash, StateHash, HistoryHash, State, Termination string
		Sequence                                                                               uint64
		Passed                                                                                 bool
		Gates                                                                                  []contracts.QualityGate
	}{workspaceID, run.ID, run.RequestHash, run.ContextHash, checkpoint.StateHash, checkpoint.HistoryHash, run.State, run.Termination, checkpoint.EventSequence, passed, gates}
	raw, _ := json.Marshal(material)
	digest := sha256.Sum256(raw)
	result := contracts.EvalResult{SchemaVersion: contracts.ObservabilitySchemaVersion, ID: contracts.NewID("evalresult"), WorkspaceID: workspaceID, CaseID: c.ID, ReplayRunID: run.ID, InputHash: c.InputHash, ContextHash: run.ContextHash, Termination: run.Termination, ObservedCostUSD: run.Cost.TotalCostUSD, CostKnown: costKnown, ReplayHash: hex.EncodeToString(digest[:]), Passed: passed, Abstained: abstained, Gates: gates}
	if replayErr != nil {
		result.Error = replayErr.Error()
	}
	return result, nil
}

func gate(name, operator string, threshold, actual float64, reason string) contracts.QualityGate {
	passed := false
	switch operator {
	case "==":
		passed = actual == threshold
	case "<=":
		passed = actual <= threshold
	case ">=":
		passed = actual >= threshold
	case "<":
		passed = actual < threshold
	case ">":
		passed = actual > threshold
	}
	return contracts.QualityGate{Name: name, Metric: name, Operator: operator, Threshold: threshold, Actual: actual, Passed: passed, Reason: reason}
}
func failedGate(name, operator string, threshold, actual float64, reason string) contracts.QualityGate {
	return gate(name, operator, threshold, actual, reason)
}
func boolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
