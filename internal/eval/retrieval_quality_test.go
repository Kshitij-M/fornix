package eval

import (
	"math"
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestScoreRankedEvidenceIsDeterministicAndDeduplicated(t *testing.T) {
	goldOne := strings.Repeat("1", 64)
	goldTwo := strings.Repeat("2", 64)
	other := strings.Repeat("3", 64)
	pack := contracts.ContextPack{
		WorkspaceID: "workspace-a",
		ContentHash: "context-hash",
		Items: []contracts.ContextItem{
			{WorkspaceID: "workspace-a", SourceReference: "memo:other", EvidenceHash: other, Score: 0.8, Text: "other"},
			{WorkspaceID: "workspace-a", SourceReference: "memo:gold-two", EvidenceHash: goldTwo, Score: 0.7, Text: "gold two"},
			{WorkspaceID: "workspace-a", SourceReference: "memo:gold-one", EvidenceHash: goldOne, Score: 0.9, Text: "gold one"},
			{WorkspaceID: "workspace-a", SourceReference: "memo:duplicate", EvidenceHash: goldOne, Score: 0.6, Text: "duplicate"},
		},
	}
	input := RetrievalScoreInput{
		WorkspaceID:   "workspace-a",
		Case:          contracts.EvalCase{ID: "case-1", ReplayRunID: "run-1", InputHash: strings.Repeat("a", 64), GoldEvidence: []string{goldTwo, goldOne}, ExpectedContextHash: "context-hash", RetrievalK: 3},
		Pack:          pack,
		Measurement:   RetrievalMeasurement{LatencyMS: 12, SQLQueries: 4, CostUSD: 0.02, CostKnown: true},
		BaselineRanks: map[string]int{goldOne: 1, goldTwo: 2},
	}
	first, err := ScoreRankedEvidence(input, []string{goldTwo, goldOne}, []contracts.QualityGate{{Name: "recall", Metric: "recall_at_k", Operator: ">=", Threshold: 1}}, nil, RegressionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := ScoreRankedEvidence(input, []string{goldOne, goldTwo}, []contracts.QualityGate{{Name: "recall", Metric: "recall_at_k", Operator: ">=", Threshold: 1}}, nil, RegressionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if first.ResultHash != second.ResultHash || first.Metrics != second.Metrics || !first.Passed {
		t.Fatalf("score is not deterministic: first=%+v second=%+v", first, second)
	}
	if first.Metrics.RetrievedCount != 3 || first.Metrics.RelevantAtK != 2 || first.Metrics.HitAtK != 1 || first.Metrics.PrecisionAtK != 2.0/3.0 || first.Metrics.RecallAtK != 1 || first.Metrics.ReciprocalRank != 1 {
		t.Fatalf("unexpected ranking metrics: %+v", first.Metrics)
	}
	if math.Abs(first.Metrics.NDCGAtK-(1.5/(1+1/math.Log2(3)))) > 1e-9 || math.Abs(first.Metrics.RankDrift-1.0/6.0) > 1e-9 || !first.Metrics.ContextHashMatch {
		t.Fatalf("unexpected quality metrics: %+v", first.Metrics)
	}
}

func TestScoreRankedEvidenceAbstentionAndHardFailures(t *testing.T) {
	input := RetrievalScoreInput{
		WorkspaceID: "workspace-a",
		Case:        contracts.EvalCase{ID: "abstain", ReplayRunID: "run", InputHash: strings.Repeat("a", 64), ExpectedAbstention: true, RetrievalK: 1},
		Pack:        contracts.ContextPack{WorkspaceID: "workspace-a", ContentHash: "empty", Abstained: true},
	}
	score, err := ScoreRankedEvidence(input, nil, nil, nil, RegressionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if !score.Passed || !score.Metrics.AbstentionCorrect || score.Metrics.HitAtK != 0 {
		t.Fatalf("abstention was not scored correctly: %+v", score)
	}
	badWorkspace := input
	badWorkspace.Pack = contracts.ContextPack{WorkspaceID: "workspace-b", ContentHash: "bad"}
	if _, err := ScoreRankedEvidence(badWorkspace, nil, nil, nil, RegressionPolicy{}); err == nil {
		t.Fatal("expected cross-workspace failure")
	}
	badHash := input
	badHash.Pack = contracts.ContextPack{WorkspaceID: "workspace-a", Items: []contracts.ContextItem{{WorkspaceID: "workspace-a", SourceReference: "memo:1", EvidenceHash: "not-a-hash", Score: 1}}}
	if _, err := ScoreRankedEvidence(badHash, nil, nil, nil, RegressionPolicy{}); err == nil {
		t.Fatal("expected invalid evidence failure")
	}
}

func TestRetrievalGatesFailClosedForUnknownCostAndRegressionIsStable(t *testing.T) {
	metrics := contracts.RetrievalQualityMetrics{SchemaVersion: contracts.ObservabilitySchemaVersion, K: 3, HitAtK: 1, RecallAtK: 1, NDCGAtK: 1, CostUSD: 0.2, CostKnown: false}
	gates := EvaluateRetrievalGates(metrics, []contracts.QualityGate{{Name: "cost", Metric: "cost_usd", Operator: "<=", Threshold: 1}})
	if len(gates) != 1 || gates[0].Passed || gates[0].Reason != "metric is unknown" {
		t.Fatalf("unknown cost did not fail closed: %+v", gates)
	}
	baseline := contracts.RetrievalQualitySummary{Cases: 10, HitAtK: 1, RecallAtK: 1, NDCGAtK: 1, ContextHashMatch: 1, CostUSD: 1, LatencyMS: 10}
	candidate := contracts.RetrievalQualitySummary{Cases: 10, HitAtK: 0.8, RecallAtK: 0.8, NDCGAtK: 0.8, ContextHashMatch: 0.8, CostUSD: 1.5, LatencyMS: 20}
	policy := RegressionPolicy{MaxHitAtKDrop: 0.1, MaxRecallDrop: 0.1, MaxNDCGDrop: 0.1, MaxContextInstability: 0.1, MaxCostIncreaseRatio: 0.2, MaxLatencyIncreaseRatio: 0.2}
	first := CompareRetrievalSummary(baseline, candidate, policy)
	second := CompareRetrievalSummary(baseline, candidate, policy)
	if len(first) != 7 || !equalRegressionFindings(first, second) {
		t.Fatalf("regression comparison is unstable: first=%+v second=%+v", first, second)
	}
	for _, finding := range first {
		if finding.Metric == "cost_increase_ratio" && finding.Passed {
			t.Fatal("cost regression unexpectedly passed")
		}
	}
	if math.Abs(first[0].Delta+0.2) > 1e-9 {
		t.Fatalf("unexpected hit delta: %+v", first[0])
	}
}

func equalRegressionFindings(a, b []contracts.RegressionFinding) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
