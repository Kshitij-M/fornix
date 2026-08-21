package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// RetrievalMeasurement is the already-recorded operational cost of one
// retrieval. Evaluation never measures by invoking the live path again.
type RetrievalMeasurement struct {
	LatencyMS     int64
	SQLQueries    int
	CostUSD       float64
	CostKnown     bool
	CostEstimated bool
}

// RetrievalScoreInput contains the immutable recorded retrieval surface. The
// context pack is the ranked result; its text is never copied into a score.
type RetrievalScoreInput struct {
	WorkspaceID   string
	Case          contracts.EvalCase
	Pack          contracts.ContextPack
	Trace         contracts.RetrievalTrace
	Measurement   RetrievalMeasurement
	BaselineRanks map[string]int
}

type RetrievalScore struct {
	Metrics          contracts.RetrievalQualityMetrics
	ResolvedEvidence []string
	Gates            []contracts.QualityGate
	Regressions      []contracts.RegressionFinding
	Passed           bool
	ResultHash       string
}

// RegressionPolicy is deliberately explicit. Zero means no degradation is
// tolerated, which is appropriate for CI; callers can choose a measured
// tolerance for a noisy production corpus.
type RegressionPolicy struct {
	MaxHitAtKDrop           float64
	MaxRecallDrop           float64
	MaxNDCGDrop             float64
	MaxRankDrift            float64
	MaxCostIncreaseRatio    float64
	MaxLatencyIncreaseRatio float64
	MaxContextInstability   float64
}

func DefaultRegressionPolicy() RegressionPolicy {
	return RegressionPolicy{
		MaxHitAtKDrop:           0.05,
		MaxRecallDrop:           0.05,
		MaxNDCGDrop:             0.05,
		MaxRankDrift:            0.25,
		MaxCostIncreaseRatio:    0.20,
		MaxLatencyIncreaseRatio: 0.20,
		MaxContextInstability:   0.10,
	}
}

// RetrievalScorer resolves gold hashes through Postgres and then delegates to
// the pure scorer. A nil Evidence store is accepted only when gold is empty;
// this makes an unscoped production evaluation fail closed.
type RetrievalScorer struct {
	Evidence *store.EvidenceStore
}

func (s RetrievalScorer) ScoreCase(ctx context.Context, input RetrievalScoreInput, gates []contracts.QualityGate, baseline *contracts.RetrievalQualitySummary, policy RegressionPolicy) (RetrievalScore, error) {
	workspace := input.WorkspaceID
	if workspace == "" {
		workspace = input.Pack.WorkspaceID
	}
	if len(input.Case.GoldEvidence) > 0 {
		if s.Evidence == nil {
			return RetrievalScore{}, fmt.Errorf("authoritative evidence store is required for gold resolution")
		}
		authorityHashes := append([]string(nil), input.Case.GoldEvidence...)
		for _, item := range input.Pack.Items {
			authorityHashes = append(authorityHashes, item.EvidenceHash)
		}
		resolvedAll, err := s.Evidence.ResolveEvidenceHashes(ctx, workspace, authorityHashes)
		if err != nil {
			return RetrievalScore{}, err
		}
		goldSet := make(map[string]struct{}, len(input.Case.GoldEvidence))
		for _, hash := range input.Case.GoldEvidence {
			goldSet[stringLowerTrim(hash)] = struct{}{}
		}
		gold := make([]string, 0, len(goldSet))
		for _, item := range resolvedAll {
			if _, ok := goldSet[item.EvidenceHash]; ok {
				gold = append(gold, item.EvidenceHash)
			}
		}
		return ScoreRankedEvidence(input, gold, gates, baseline, policy)
	}
	if s.Evidence == nil && len(input.Pack.Items) > 0 {
		return RetrievalScore{}, fmt.Errorf("authoritative evidence store is required for observed evidence")
	}
	if s.Evidence != nil {
		observed := make([]string, 0, len(input.Pack.Items))
		for _, item := range input.Pack.Items {
			observed = append(observed, item.EvidenceHash)
		}
		if _, err := s.Evidence.ResolveEvidenceHashes(ctx, workspace, observed); err != nil {
			return RetrievalScore{}, err
		}
	}
	return ScoreRankedEvidence(input, nil, gates, baseline, policy)
}

// ScoreRankedEvidence is pure and is used by unit tests, offline replay, and
// callers that already resolved gold against an authoritative snapshot.
func ScoreRankedEvidence(input RetrievalScoreInput, resolvedGold []string, gates []contracts.QualityGate, baseline *contracts.RetrievalQualitySummary, policy RegressionPolicy) (RetrievalScore, error) {
	workspace := input.WorkspaceID
	if workspace == "" {
		workspace = input.Pack.WorkspaceID
	}
	if workspace == "" || input.Pack.WorkspaceID != workspace {
		return RetrievalScore{}, fmt.Errorf("retrieval workspace mismatch")
	}
	if input.Pack.Abstained != (len(input.Pack.Items) == 0) {
		return RetrievalScore{}, fmt.Errorf("context pack abstention state is inconsistent")
	}
	caseCopy := input.Case
	if err := normalizeScoringCase(&caseCopy); err != nil {
		return RetrievalScore{}, err
	}
	if input.Measurement.LatencyMS == 0 {
		input.Measurement.LatencyMS = traceLatencyMS(input.Trace)
	}
	if input.Measurement.SQLQueries == 0 {
		input.Measurement.SQLQueries = traceSQLQueries(input.Trace)
	}
	if input.Measurement.LatencyMS < 0 || input.Measurement.SQLQueries < 0 || input.Measurement.CostUSD < 0 || math.IsNaN(input.Measurement.CostUSD) || math.IsInf(input.Measurement.CostUSD, 0) {
		return RetrievalScore{}, fmt.Errorf("retrieval measurement is invalid")
	}

	for _, hash := range resolvedGold {
		if !isCanonicalHash(stringLowerTrim(hash)) {
			return RetrievalScore{}, fmt.Errorf("gold evidence contains an invalid hash")
		}
	}
	gold := canonicalHashes(resolvedGold)
	ranked, err := rankedEvidence(input.Pack)
	if err != nil {
		return RetrievalScore{}, err
	}
	metrics := scoreMetrics(caseCopy, input.Pack, ranked, gold, input.Measurement, input.BaselineRanks)
	if err := metrics.Normalize(); err != nil {
		return RetrievalScore{}, err
	}
	evaluatedGates := EvaluateRetrievalGates(metrics, gates)
	var regressions []contracts.RegressionFinding
	if baseline != nil {
		regressions = CompareRetrievalSummary(*baseline, SummarizeRetrieval([]contracts.RetrievalQualityMetrics{metrics}), policy)
	}
	passed := allGatesPassed(evaluatedGates) && allRegressionsPassed(regressions)
	hash := retrievalScoreHash(workspace, caseCopy.ID, input.Pack, metrics, gold, evaluatedGates, regressions, passed)
	return RetrievalScore{Metrics: metrics, ResolvedEvidence: gold, Gates: evaluatedGates, Regressions: regressions, Passed: passed, ResultHash: hash}, nil
}

// Result converts an offline retrieval score into the existing durable eval
// result contract. The caller may persist it transactionally through
// EvaluationStore; this function itself has no side effects.
func (s RetrievalScore) Result(runID string, input RetrievalScoreInput) (contracts.EvalResult, error) {
	if s.ResultHash == "" {
		return contracts.EvalResult{}, fmt.Errorf("retrieval score has no result hash")
	}
	result := contracts.EvalResult{
		SchemaVersion:    contracts.ObservabilitySchemaVersion,
		ID:               contracts.NewID("evalresult"),
		WorkspaceID:      input.WorkspaceID,
		EvalRunID:        runID,
		CaseID:           input.Case.ID,
		ReplayRunID:      input.Case.ReplayRunID,
		InputHash:        input.Case.InputHash,
		ContextHash:      input.Pack.ContentHash,
		ObservedCostUSD:  input.Measurement.CostUSD,
		CostKnown:        input.Measurement.CostKnown,
		ReplayHash:       s.ResultHash,
		Passed:           s.Passed,
		Abstained:        input.Pack.Abstained,
		Gates:            s.Gates,
		RetrievalMetrics: &s.Metrics,
		ResolvedEvidence: s.ResolvedEvidence,
		Regressions:      s.Regressions,
	}
	if err := result.Normalize(); err != nil {
		return contracts.EvalResult{}, err
	}
	return result, nil
}

func rankedEvidence(pack contracts.ContextPack) ([]string, error) {
	items := append([]contracts.ContextItem(nil), pack.Items...)
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score > items[j].Score
		}
		if items[i].SourceReference != items[j].SourceReference {
			return items[i].SourceReference < items[j].SourceReference
		}
		return items[i].EvidenceHash < items[j].EvidenceHash
	})
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item.WorkspaceID != pack.WorkspaceID || item.SourceReference == "" || !isCanonicalHash(item.EvidenceHash) {
			return nil, fmt.Errorf("context item has invalid or cross-workspace evidence")
		}
		if _, ok := seen[item.EvidenceHash]; ok {
			continue
		}
		seen[item.EvidenceHash] = struct{}{}
		out = append(out, item.EvidenceHash)
	}
	return out, nil
}

func scoreMetrics(c contracts.EvalCase, pack contracts.ContextPack, ranked, gold []string, measurement RetrievalMeasurement, baselineRanks map[string]int) contracts.RetrievalQualityMetrics {
	k := c.RetrievalK
	if k == 0 {
		k = contracts.DefaultEvalRetrievalK
	}
	goldSet := make(map[string]struct{}, len(gold))
	for _, hash := range gold {
		goldSet[hash] = struct{}{}
	}
	topK := ranked
	if len(topK) > k {
		topK = topK[:k]
	}
	relevant := 0
	for _, hash := range topK {
		if _, ok := goldSet[hash]; ok {
			relevant++
		}
	}
	firstRank := 0
	for index, hash := range ranked {
		if _, ok := goldSet[hash]; ok {
			firstRank = index + 1
			break
		}
	}
	precisionDenominator := len(topK)
	precision := 0.0
	if precisionDenominator > 0 {
		precision = float64(relevant) / float64(precisionDenominator)
	}
	recall := 0.0
	if len(gold) > 0 {
		recall = float64(relevant) / float64(len(gold))
	}
	n := 0.0
	if firstRank > 0 {
		n = 1 / float64(firstRank)
	}
	ndcg := ndcgAtK(topK, goldSet)
	return contracts.RetrievalQualityMetrics{
		SchemaVersion: contracts.ObservabilitySchemaVersion,
		K:             k, RetrievedCount: len(ranked), GoldCount: len(gold), RelevantAtK: relevant,
		HitAtK: metricBoolFloat(relevant > 0), ReciprocalRank: n, PrecisionAtK: precision, RecallAtK: recall,
		NDCGAtK: ndcg, RankDrift: rankDrift(gold, ranked, baselineRanks, k),
		ContextHashMatch:  c.ExpectedContextHash == "" || c.ExpectedContextHash == pack.ContentHash,
		AbstentionCorrect: c.ExpectedAbstention == pack.Abstained, LatencyMS: measurement.LatencyMS,
		SQLQueries: measurement.SQLQueries, CostUSD: measurement.CostUSD, CostKnown: measurement.CostKnown,
		CostEstimated: measurement.CostEstimated,
	}
}

func ndcgAtK(ranked []string, gold map[string]struct{}) float64 {
	if len(gold) == 0 {
		return 0
	}
	dcg := 0.0
	for index, hash := range ranked {
		if _, ok := gold[hash]; ok {
			dcg += 1 / math.Log2(float64(index+2))
		}
	}
	ideal := 0.0
	limit := len(gold)
	if limit > len(ranked) {
		limit = len(ranked)
	}
	for index := 0; index < limit; index++ {
		ideal += 1 / math.Log2(float64(index+2))
	}
	if ideal == 0 {
		return 0
	}
	return dcg / ideal
}

func rankDrift(gold, ranked []string, baseline map[string]int, k int) float64 {
	if len(gold) == 0 || len(baseline) == 0 {
		return 0
	}
	current := make(map[string]int, len(ranked))
	for index, hash := range ranked {
		current[hash] = index + 1
	}
	var total float64
	for _, hash := range gold {
		oldRank, oldOK := baseline[hash]
		if !oldOK {
			continue
		}
		newRank := current[hash]
		if newRank == 0 || newRank > k {
			newRank = k + 1
		}
		if oldRank <= 0 || oldRank > k {
			oldRank = k + 1
		}
		delta := newRank - oldRank
		if delta < 0 {
			delta = -delta
		}
		total += float64(delta) / float64(k)
	}
	return total / float64(len(gold))
}

// EvaluateRetrievalGates fills actual values using one canonical metric map.
// Unknown cost fails closed instead of being represented as zero.
func EvaluateRetrievalGates(metrics contracts.RetrievalQualityMetrics, gates []contracts.QualityGate) []contracts.QualityGate {
	out := append([]contracts.QualityGate(nil), gates...)
	for i := range out {
		actual, known := retrievalMetricValue(metrics, out[i].Metric)
		out[i].Actual = actual
		if !known {
			out[i].Passed = false
			out[i].Reason = "metric is unknown"
			continue
		}
		out[i].Passed = compareGate(out[i].Operator, out[i].Threshold, actual)
	}
	return out
}

// EvaluateRetrievalSummaryGates applies the same fail-closed policy to an
// aggregate. It is the run-level counterpart to EvaluateRetrievalGates.
func EvaluateRetrievalSummaryGates(summary contracts.RetrievalQualitySummary, gates []contracts.QualityGate) []contracts.QualityGate {
	out := append([]contracts.QualityGate(nil), gates...)
	for i := range out {
		actual, known := summaryMetricValue(summary, out[i].Metric)
		out[i].Actual = actual
		if !known {
			out[i].Passed = false
			out[i].Reason = "aggregate metric is unknown"
			continue
		}
		out[i].Passed = compareGate(out[i].Operator, out[i].Threshold, actual)
	}
	return out
}

func CompareRetrievalSummary(baseline, candidate contracts.RetrievalQualitySummary, policy RegressionPolicy) []contracts.RegressionFinding {
	findings := []contracts.RegressionFinding{
		finding("hit_at_k_regression", "hit_at_k_delta", baseline.HitAtK, candidate.HitAtK, candidate.HitAtK-baseline.HitAtK, -policy.MaxHitAtKDrop, candidate.HitAtK-baseline.HitAtK >= -policy.MaxHitAtKDrop, "candidate hit@k must not drop beyond tolerance"),
		finding("recall_at_k_regression", "recall_at_k_delta", baseline.RecallAtK, candidate.RecallAtK, candidate.RecallAtK-baseline.RecallAtK, -policy.MaxRecallDrop, candidate.RecallAtK-baseline.RecallAtK >= -policy.MaxRecallDrop, "candidate recall must not drop beyond tolerance"),
		finding("ndcg_at_k_regression", "ndcg_at_k_delta", baseline.NDCGAtK, candidate.NDCGAtK, candidate.NDCGAtK-baseline.NDCGAtK, -policy.MaxNDCGDrop, candidate.NDCGAtK-baseline.NDCGAtK >= -policy.MaxNDCGDrop, "candidate nDCG must not drop beyond tolerance"),
		finding("rank_drift_regression", "rank_drift", baseline.RankDrift, candidate.RankDrift, candidate.RankDrift, policy.MaxRankDrift, candidate.RankDrift <= policy.MaxRankDrift, "rank drift must remain bounded"),
		finding("context_instability_regression", "context_instability", baseline.ContextHashMatch, candidate.ContextHashMatch, 1-candidate.ContextHashMatch, policy.MaxContextInstability, candidate.ContextHashMatch >= 1-policy.MaxContextInstability, "context hash instability must remain bounded"),
	}
	costPassed := candidate.UnknownCostCases == 0 && ratio(candidate.CostUSD, baseline.CostUSD) <= policy.MaxCostIncreaseRatio
	latencyPassed := ratio(candidate.LatencyMS, baseline.LatencyMS) <= policy.MaxLatencyIncreaseRatio
	findings = append(findings,
		finding("cost_regression", "cost_increase_ratio", baseline.CostUSD, candidate.CostUSD, ratio(candidate.CostUSD, baseline.CostUSD), policy.MaxCostIncreaseRatio, costPassed, "cost usage must be known and remain within budget"),
		finding("latency_regression", "latency_increase_ratio", baseline.LatencyMS, candidate.LatencyMS, ratio(candidate.LatencyMS, baseline.LatencyMS), policy.MaxLatencyIncreaseRatio, latencyPassed, "latency must remain within budget"),
	)
	return findings
}

// SummarizeRetrieval aggregates scores in caller-provided order. The
// arithmetic is associative for the bounded finite values in the contract;
// callers should sort cases by ID before passing them for a stable report.
func SummarizeRetrieval(metrics []contracts.RetrievalQualityMetrics) contracts.RetrievalQualitySummary {
	var out contracts.RetrievalQualitySummary
	if len(metrics) == 0 {
		return out
	}
	out.Cases = len(metrics)
	for _, metric := range metrics {
		out.HitAtK += metric.HitAtK
		out.ReciprocalRank += metric.ReciprocalRank
		out.PrecisionAtK += metric.PrecisionAtK
		out.RecallAtK += metric.RecallAtK
		out.NDCGAtK += metric.NDCGAtK
		out.RankDrift += metric.RankDrift
		if metric.ContextHashMatch {
			out.ContextHashMatch++
		}
		if metric.AbstentionCorrect {
			out.AbstentionCorrect++
		}
		out.LatencyMS += float64(metric.LatencyMS)
		out.SQLQueries += float64(metric.SQLQueries)
		out.CostUSD += metric.CostUSD
		if !metric.CostKnown {
			out.UnknownCostCases++
		}
	}
	denominator := float64(out.Cases)
	out.HitAtK /= denominator
	out.ReciprocalRank /= denominator
	out.PrecisionAtK /= denominator
	out.RecallAtK /= denominator
	out.NDCGAtK /= denominator
	out.RankDrift /= denominator
	out.ContextHashMatch /= denominator
	out.AbstentionCorrect /= denominator
	out.LatencyMS /= denominator
	out.SQLQueries /= denominator
	out.CostUSD /= denominator
	return out
}

func retrievalMetricValue(m contracts.RetrievalQualityMetrics, name string) (float64, bool) {
	switch name {
	case "hit_at_k":
		return m.HitAtK, true
	case "reciprocal_rank":
		return m.ReciprocalRank, true
	case "precision_at_k":
		return m.PrecisionAtK, true
	case "recall_at_k":
		return m.RecallAtK, true
	case "ndcg_at_k":
		return m.NDCGAtK, true
	case "rank_drift":
		return m.RankDrift, true
	case "context_hash_match":
		return metricBoolFloat(m.ContextHashMatch), true
	case "abstention_correct":
		return metricBoolFloat(m.AbstentionCorrect), true
	case "latency_ms":
		return float64(m.LatencyMS), true
	case "sql_queries":
		return float64(m.SQLQueries), true
	case "cost_usd":
		return m.CostUSD, m.CostKnown
	default:
		return 0, false
	}
}

func summaryMetricValue(m contracts.RetrievalQualitySummary, name string) (float64, bool) {
	switch name {
	case "hit_at_k":
		return m.HitAtK, m.Cases > 0
	case "reciprocal_rank":
		return m.ReciprocalRank, m.Cases > 0
	case "precision_at_k":
		return m.PrecisionAtK, m.Cases > 0
	case "recall_at_k":
		return m.RecallAtK, m.Cases > 0
	case "ndcg_at_k":
		return m.NDCGAtK, m.Cases > 0
	case "rank_drift":
		return m.RankDrift, m.Cases > 0
	case "context_hash_match":
		return m.ContextHashMatch, m.Cases > 0
	case "abstention_correct":
		return m.AbstentionCorrect, m.Cases > 0
	case "latency_ms":
		return m.LatencyMS, m.Cases > 0
	case "sql_queries":
		return m.SQLQueries, m.Cases > 0
	case "cost_usd":
		return m.CostUSD, m.Cases > 0 && m.UnknownCostCases == 0
	default:
		return 0, false
	}
}

func retrievalScoreHash(workspace, caseID string, pack contracts.ContextPack, metrics contracts.RetrievalQualityMetrics, gold []string, gates []contracts.QualityGate, regressions []contracts.RegressionFinding, passed bool) string {
	canonical := struct {
		Workspace   string
		CaseID      string
		PackHash    string
		Metrics     contracts.RetrievalQualityMetrics
		Gold        []string
		Gates       []contracts.QualityGate
		Regressions []contracts.RegressionFinding
		Passed      bool
	}{workspace, caseID, pack.ContentHash, metrics, gold, gates, regressions, passed}
	raw, _ := json.Marshal(canonical)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func traceLatencyMS(trace contracts.RetrievalTrace) int64 {
	var micros int64
	for _, stage := range trace.Stages {
		micros += stage.DurationMicros
	}
	return micros / 1000
}

func traceSQLQueries(trace contracts.RetrievalTrace) int {
	var queries int
	for _, stage := range trace.Stages {
		queries += stage.Queries
	}
	return queries
}

func canonicalHashes(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = stringLowerTrim(value)
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func isCanonicalHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func stringLowerTrim(value string) string {
	for len(value) > 0 && (value[0] == ' ' || value[0] == '\t' || value[0] == '\n' || value[0] == '\r') {
		value = value[1:]
	}
	for len(value) > 0 && (value[len(value)-1] == ' ' || value[len(value)-1] == '\t' || value[len(value)-1] == '\n' || value[len(value)-1] == '\r') {
		value = value[:len(value)-1]
	}
	for i := 0; i < len(value); i++ {
		if value[i] >= 'A' && value[i] <= 'F' {
			value = value[:i] + string(value[i]-'A'+'a') + value[i+1:]
		}
	}
	return value
}

func metricBoolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
func allGatesPassed(gates []contracts.QualityGate) bool {
	for _, gate := range gates {
		if !gate.Passed {
			return false
		}
	}
	return true
}
func allRegressionsPassed(findings []contracts.RegressionFinding) bool {
	for _, finding := range findings {
		if !finding.Passed {
			return false
		}
	}
	return true
}
func compareGate(operator string, threshold, actual float64) bool {
	switch operator {
	case "==":
		return actual == threshold
	case "<=":
		return actual <= threshold
	case ">=":
		return actual >= threshold
	case "<":
		return actual < threshold
	case ">":
		return actual > threshold
	}
	return false
}
func finding(name, metric string, baseline, candidate, delta, threshold float64, passed bool, reason string) contracts.RegressionFinding {
	relative := delta
	if baseline != 0 {
		relative = delta / math.Abs(baseline)
	}
	return contracts.RegressionFinding{Name: name, Metric: metric, Baseline: baseline, Candidate: candidate, Delta: delta, RelativeDelta: relative, Threshold: threshold, Passed: passed, Reason: reason}
}
func ratio(candidate, baseline float64) float64 {
	if baseline == 0 {
		if candidate == 0 {
			return 0
		}
		return candidate
	}
	return (candidate - baseline) / math.Abs(baseline)
}

// normalizeScoringCase keeps the scorer independent from persistence while
// applying the same case bounds as the durable contract.
func normalizeScoringCase(c *contracts.EvalCase) error {
	if c == nil {
		return fmt.Errorf("eval case is nil")
	}
	if c.RetrievalK == 0 {
		c.RetrievalK = contracts.DefaultEvalRetrievalK
	}
	if c.RetrievalK < 1 || c.RetrievalK > contracts.MaxRetrievalItems {
		return fmt.Errorf("retrieval_k is out of bounds")
	}
	return nil
}
