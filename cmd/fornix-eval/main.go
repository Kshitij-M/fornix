package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/eval"
)

type offlineBundle struct {
	WorkspaceID string                                `json:"workspace_id"`
	Dataset     contracts.EvalDataset                 `json:"dataset"`
	Surfaces    map[string]contracts.RetrievalSurface `json:"surfaces"`
	Gates       []contracts.QualityGate               `json:"gates,omitempty"`
}

type offlineResult struct {
	SchemaVersion int                               `json:"schema_version"`
	WorkspaceID   string                            `json:"workspace_id"`
	Cases         []contracts.EvalResult            `json:"cases"`
	Quality       contracts.RetrievalQualitySummary `json:"retrieval_quality"`
	Gates         []contracts.QualityGate           `json:"gates,omitempty"`
	ReplayHash    string                            `json:"replay_hash"`
	Passed        bool                              `json:"passed"`
}

func main() {
	inputPath := flag.String("input", "-", "redacted recorded retrieval bundle JSON, or - for stdin")
	flag.Parse()
	data, err := readInput(*inputPath)
	if err != nil {
		fatal(err)
	}
	var bundle offlineBundle
	if err := json.Unmarshal(data, &bundle); err != nil {
		fatal(fmt.Errorf("decode retrieval bundle: %w", err))
	}
	if bundle.WorkspaceID != "" {
		bundle.Dataset.WorkspaceID = bundle.WorkspaceID
	}
	if bundle.Dataset.ID == "" {
		bundle.Dataset.ID = "offline-dataset"
	}
	if bundle.Dataset.Name == "" {
		bundle.Dataset.Name = "offline"
	}
	if bundle.Dataset.Version == 0 {
		bundle.Dataset.Version = 1
	}
	if err := bundle.Dataset.Normalize(); err != nil {
		fatal(err)
	}
	if bundle.WorkspaceID == "" {
		bundle.WorkspaceID = bundle.Dataset.WorkspaceID
	}
	if bundle.WorkspaceID != bundle.Dataset.WorkspaceID {
		fatal(fmt.Errorf("bundle workspace does not match dataset workspace"))
	}
	bundle.Gates = canonicalGates(bundle.Gates)
	cases := append([]contracts.EvalCase(nil), bundle.Dataset.Cases...)
	sort.Slice(cases, func(i, j int) bool { return cases[i].ID < cases[j].ID })
	results := make([]contracts.EvalResult, 0, len(cases))
	metrics := make([]contracts.RetrievalQualityMetrics, 0, len(cases))
	for _, item := range cases {
		surfaceID := item.RetrievalSurfaceID
		if surfaceID == "" {
			surfaceID = item.ReplayRunID
		}
		surface, ok := bundle.Surfaces[surfaceID]
		if !ok {
			fatal(fmt.Errorf("case %s surface %s is missing", item.ID, surfaceID))
		}
		if surface.WorkspaceID != bundle.WorkspaceID {
			fatal(fmt.Errorf("case %s surface crosses workspace boundary", item.ID))
		}
		if err := surface.Normalize(); err != nil {
			fatal(fmt.Errorf("normalize surface %s: %w", surfaceID, err))
		}
		input := eval.RetrievalScoreInput{
			WorkspaceID: bundle.WorkspaceID, Case: item, Pack: surface.ContextPack(), Trace: surface.Trace,
			Measurement: eval.RetrievalMeasurement{LatencyMS: surface.DurationMS, SQLQueries: surface.SQLQueries, CostUSD: surface.CostUSD, CostKnown: surface.CostKnown, CostEstimated: surface.CostEstimated},
		}
		// The bundle is explicitly offline and must already contain the
		// authoritative evidence hashes. The production API performs the
		// additional Postgres EvidenceStore resolution before scoring.
		score, err := eval.ScoreRankedEvidence(input, item.GoldEvidence, bundle.Gates, nil, eval.DefaultRegressionPolicy())
		if err != nil {
			fatal(fmt.Errorf("score case %s: %w", item.ID, err))
		}
		result, err := score.Result("offline", input)
		if err != nil {
			fatal(err)
		}
		// EvalResult normally receives a random durable ID. Offline evaluation
		// must be byte-for-byte replayable, so derive its identity from the
		// already deterministic score hash.
		result.ID = "offline_result_" + score.ResultHash[:32]
		results = append(results, result)
		metrics = append(metrics, score.Metrics)
	}
	quality := eval.SummarizeRetrieval(metrics)
	gates := eval.EvaluateRetrievalSummaryGates(quality, bundle.Gates)
	passed := true
	for _, gate := range gates {
		passed = passed && gate.Passed
	}
	output := offlineResult{SchemaVersion: contracts.ObservabilitySchemaVersion, WorkspaceID: bundle.WorkspaceID, Cases: results, Quality: quality, Gates: gates, Passed: passed}
	output.ReplayHash = hashOutput(output)
	encoded, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		fatal(err)
	}
	encoded = append(encoded, '\n')
	if _, err := os.Stdout.Write(encoded); err != nil {
		fatal(err)
	}
}

func readInput(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func hashOutput(output offlineResult) string {
	clone := output
	clone.ReplayHash = ""
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func canonicalGates(gates []contracts.QualityGate) []contracts.QualityGate {
	result := append([]contracts.QualityGate(nil), gates...)
	for i := range result {
		result[i].Name = strings.TrimSpace(result[i].Name)
		result[i].Metric = strings.TrimSpace(result[i].Metric)
		result[i].Operator = strings.TrimSpace(result[i].Operator)
		result[i].Reason = ""
		result[i].Actual = 0
		result[i].Passed = false
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		if result[i].Metric != result[j].Metric {
			return result[i].Metric < result[j].Metric
		}
		if result[i].Operator != result[j].Operator {
			return result[i].Operator < result[j].Operator
		}
		return result[i].Threshold < result[j].Threshold
	})
	return result
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "fornix-eval:", err)
	os.Exit(1)
}
