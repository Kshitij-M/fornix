package server

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/store"
)

const (
	queryEmbeddingModeAdaptive = "adaptive-v1"
	queryEmbeddingModeLegacy   = "legacy-v1"
	queryEmbeddingModeRequired = "required-v1"
	queryEmbeddingModeDisabled = "disabled-v1"
	maxReferenceTimeFuture     = 5 * time.Minute
)

type deterministicGate struct {
	Satisfied bool
	Reason    string
	Count     int
	BestScore float64
}

// resolveReferenceTime makes time-dependent ranking an explicit request
// input. A missing value is captured once at the handler boundary; callers
// that need replay-stable results must persist and resend the returned value.
// Database clock functions are deliberately not used in ranking SQL.
func resolveReferenceTime(value *time.Time) (time.Time, error) {
	now := time.Now().UTC()
	if value == nil {
		return now.Truncate(time.Microsecond), nil
	}
	if value.IsZero() {
		return time.Time{}, fmt.Errorf("reference_time must not be zero")
	}
	resolved := value.UTC().Truncate(time.Microsecond)
	if resolved.Before(time.Unix(0, 0).UTC()) {
		return time.Time{}, fmt.Errorf("reference_time must not be before the Unix epoch")
	}
	if resolved.After(now.Add(maxReferenceTimeFuture)) {
		return time.Time{}, fmt.Errorf("reference_time is too far in the future")
	}
	return resolved, nil
}

func gateThreshold(topK int, best float64, count int, exact bool) deterministicGate {
	needed := topK
	if needed > 3 {
		needed = 3
	}
	if needed < 1 {
		needed = 1
	}
	if exact || (count >= needed && best >= 0.05) {
		return deterministicGate{Satisfied: true, Reason: "deterministic_sufficient", Count: count, BestScore: best}
	}
	if count == 0 {
		return deterministicGate{Reason: "insufficient_candidates", Count: count, BestScore: best}
	}
	return deterministicGate{Reason: "insufficient_confidence", Count: count, BestScore: best}
}

func (s *server) memoDeterministicGate(ctx context.Context, workspaceID, query, typ string, topK int) (deterministicGate, error) {
	tx, err := store.BeginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return deterministicGate{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var count int
	var best float64
	err = tx.QueryRow(ctx, `
		SELECT count(*)::int, COALESCE(max(score),0)
		FROM (
			SELECT ts_rank(tsv, plainto_tsquery('english',$1)) AS score
			FROM fornix.memos
			WHERE workspace_id=$2 AND deleted_at IS NULL
			  AND tsv @@ plainto_tsquery('english',$1)
			  AND ($3='' OR type=$3)
				ORDER BY score DESC,id
				LIMIT $4
		) candidates`, query, workspaceID, typ, topK).Scan(&count, &best)
	if err != nil {
		return deterministicGate{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return deterministicGate{}, err
	}
	return gateThreshold(topK, best, count, false), nil
}

func (s *server) symbolDeterministicGate(ctx context.Context, workspaceID, query, repo, kind string, topK int) (deterministicGate, error) {
	tx, err := store.BeginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return deterministicGate{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var count int
	var best float64
	var exact bool
	err = tx.QueryRow(ctx, `
		SELECT count(*)::int, COALESCE(max(score),0), COALESCE(bool_or(symbol_name=$1),false)
		FROM (
			SELECT symbol_name,
				CASE WHEN symbol_name=$1 THEN 1.0
				     WHEN symbol_name ILIKE $1 || '%' THEN 0.85
				     WHEN symbol_name ILIKE '%' || $1 || '%' THEN 0.6
				     ELSE 0.3 END AS score
			FROM fornix.symbols
			WHERE workspace_id=$2 AND deleted_at IS NULL
			  AND ($3='' OR repo=$3) AND ($4='' OR symbol_kind=$4)
			  AND symbol_name ILIKE '%' || $1 || '%'
				ORDER BY score DESC,symbol_name,id
				LIMIT $5
		) candidates`, query, workspaceID, repo, kind, topK).Scan(&count, &best, &exact)
	if err != nil {
		return deterministicGate{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return deterministicGate{}, err
	}
	return gateThreshold(topK, best, count, exact), nil
}

func (s *server) ragDeterministicGate(ctx context.Context, workspaceID, query string, filters ragFilters, topK int) (deterministicGate, error) {
	// A minimum composite score is evaluated after all ranking signals are
	// combined. Lexical preflight cannot prove that threshold safely, so keep
	// the expensive stage enabled instead of making an overconfident skip.
	if filters.MinScore > 0 {
		return deterministicGate{Reason: "min_score_requires_ranked_stage"}, nil
	}
	tx, err := store.BeginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return deterministicGate{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var count int
	var best float64
	args := []any{query, workspaceID}
	where := []string{"workspace_id=$2", "tsv @@ plainto_tsquery('english',$1)"}
	for _, path := range filters.SourcePaths {
		args = append(args, globToLike(path))
		where = append(where, fmt.Sprintf("source_path ILIKE $%d", len(args)))
	}
	if strings.TrimSpace(filters.Type) != "" {
		args = append(args, strings.TrimSpace(filters.Type))
		where = append(where, fmt.Sprintf("metadata->>'type' = $%d", len(args)))
	}
	args = append(args, topK)
	querySQL := fmt.Sprintf(`
		SELECT count(*)::int, COALESCE(max(score),0)
		FROM (
			SELECT ts_rank_cd(tsv, plainto_tsquery('english',$1)) AS score
			FROM fornix.chunks
			WHERE %s
			ORDER BY score DESC,id
			LIMIT $%d
		) candidates`, strings.Join(where, " AND "), len(args))
	err = tx.QueryRow(ctx, querySQL, args...).Scan(&count, &best)
	if err != nil {
		return deterministicGate{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return deterministicGate{}, err
	}
	return gateThreshold(topK, best, count, false), nil
}

func validQueryEmbeddingMode(value string) bool {
	switch value {
	case queryEmbeddingModeAdaptive, queryEmbeddingModeLegacy, queryEmbeddingModeRequired, queryEmbeddingModeDisabled:
		return true
	default:
		return false
	}
}

func normalizeQueryEmbeddingMode(value, defaultValue string) string {
	if value == "" {
		value = defaultValue
	}
	if value == "adaptive" {
		value = queryEmbeddingModeAdaptive
	}
	if value == "required" {
		value = queryEmbeddingModeRequired
	}
	if value == "disabled" {
		value = queryEmbeddingModeDisabled
	}
	if value == "legacy" {
		value = queryEmbeddingModeLegacy
	}
	if !validQueryEmbeddingMode(value) {
		return ""
	}
	return value
}

func finiteScore(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}
