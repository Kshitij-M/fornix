package retrieval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgvector "github.com/pgvector/pgvector-go"

	"github.com/omaveda/fornix/internal/contracts"
)

// Store owns read-only retrieval. It deliberately does not cache, mutate
// projections, or invoke a model: PostgreSQL and the request are the inputs.
type Store struct {
	pool     *pgxpool.Pool
	recorder SurfaceRecorder
}

// SurfaceRecorder receives the redacted result after the read-only retrieval
// snapshot commits. It is an accounting/evaluation boundary; a capture error
// fails the request so callers cannot mistake an unrecorded result for a
// replayable one.
type SurfaceRecorder func(context.Context, contracts.RetrievalRequest, Result, time.Duration) error

// Result contains the plan, redacted execution trace, and bounded context pack
// produced by one repeatable-read retrieval snapshot.
type Result struct {
	Plan  contracts.RetrievalPlan  `json:"plan"`
	Trace contracts.RetrievalTrace `json:"trace"`
	Pack  contracts.ContextPack    `json:"pack"`
}

type candidate struct {
	item     contracts.ContextItem
	symbolID int64
	stage    contracts.RetrievalStage
}

type candidateSet struct {
	byKey map[string]candidate
}

// NewStore creates a retrieval store backed solely by the supplied Postgres
// pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// SetSurfaceRecorder registers the append-only redacted capture hook.
func (s *Store) SetSurfaceRecorder(recorder SurfaceRecorder) {
	if s != nil {
		s.recorder = recorder
	}
}

// Retrieve executes the staged plan inside a repeatable-read, read-only
// snapshot and compiles a bounded deterministic context pack.
func (s *Store) Retrieve(ctx context.Context, request contracts.RetrievalRequest) (Result, error) {
	started := time.Now()
	plan, normalized, err := BuildPlan(request)
	if err != nil {
		return Result{}, err
	}
	if s == nil || s.pool == nil {
		return Result{}, fmt.Errorf("retrieval store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Result{}, fmt.Errorf("begin retrieval snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	set := &candidateSet{byKey: make(map[string]candidate)}
	trace := contracts.RetrievalTrace{PlanHash: PlanHash(plan), Stages: make([]contracts.RetrievalStageTrace, 0, len(plan.Stages))}
	for _, stagePlan := range plan.Stages {
		stageTrace := contracts.RetrievalStageTrace{Name: stagePlan.Name}
		if !stagePlan.Enabled {
			stageTrace.Status = "skipped"
			stageTrace.Reason = stagePlan.Reason
			trace.Stages = append(trace.Stages, stageTrace)
			continue
		}
		if set.satisfied(normalized) {
			stageTrace.Status = "skipped"
			stageTrace.Reason = "budget_or_confidence_satisfied"
			trace.Stages = append(trace.Stages, stageTrace)
			continue
		}
		started := time.Now()
		var candidates []candidate
		switch stagePlan.Name {
		case contracts.StageStructured:
			candidates, stageTrace.Queries, err = s.structured(ctx, tx, normalized, stagePlan.CandidateLimit)
		case contracts.StageLexical:
			candidates, stageTrace.Queries, err = s.lexical(ctx, tx, normalized, stagePlan.CandidateLimit)
		case contracts.StageGraph:
			roots := set.symbolRoots()
			if len(roots) == 0 {
				stageTrace.Status = "skipped"
				stageTrace.Reason = "no_symbol_anchors"
				trace.Stages = append(trace.Stages, stageTrace)
				continue
			}
			candidates, stageTrace.Queries, err = s.graph(ctx, tx, normalized, roots, stagePlan.CandidateLimit)
		case contracts.StageVector:
			candidates, stageTrace.Queries, err = s.vector(ctx, tx, normalized, stagePlan.CandidateLimit)
		default:
			err = fmt.Errorf("unknown retrieval stage %q", stagePlan.Name)
		}
		stageTrace.DurationMicros = time.Since(started).Microseconds()
		stageTrace.Candidates = len(candidates)
		if err != nil {
			stageTrace.Status = "failed"
			stageTrace.Error = err.Error()
			// A failed optional stage cannot erase already collected deterministic
			// evidence. The trace makes the degraded result explicit.
			trace.Stages = append(trace.Stages, stageTrace)
			continue
		}
		stageTrace.Status = "completed"
		accepted, duplicates := set.add(candidates)
		stageTrace.Accepted = accepted
		stageTrace.Duplicates = duplicates
		trace.Candidates += len(candidates)
		trace.Accepted = len(set.byKey)
		trace.Duplicates += duplicates
		trace.Stages = append(trace.Stages, stageTrace)
	}
	// Retrieval is strictly read-only. Roll back the repeatable-read snapshot
	// after all bounded reads so an optional stage failure cannot turn a
	// degraded, auditable result into pgx's ErrTxCommitRollback. The snapshot
	// has already served its purpose; no authoritative state is written here.
	if err := tx.Rollback(ctx); err != nil {
		return Result{}, fmt.Errorf("release retrieval snapshot: %w", err)
	}

	items := make([]contracts.ContextItem, 0, len(set.byKey))
	for _, value := range set.byKey {
		items = append(items, value.item)
	}
	pack := Compile(plan.RequestHash, normalized.WorkspaceID, plan.Budget, items)
	trace.CompiledItems = len(pack.Items)
	trace.CompiledBytes = pack.TotalBytes
	trace.CompiledTokens = pack.TotalTokens
	trace.Abstained = pack.Abstained
	for _, item := range pack.Items {
		if item.Truncated {
			trace.TruncatedItems++
		}
	}
	result := Result{Plan: plan, Trace: trace, Pack: pack}
	if s.recorder != nil {
		if err := s.recorder(ctx, normalized, result, time.Since(started)); err != nil {
			return Result{}, fmt.Errorf("record retrieval surface: %w", err)
		}
	}
	return result, nil
}

func (s *Store) structured(ctx context.Context, tx pgx.Tx, request contracts.RetrievalRequest, limit int) ([]candidate, int, error) {
	result := make([]candidate, 0)
	queries := 0
	ids := map[string][]int64{"memo": {}, "chunk": {}, "symbol": {}}
	for _, ref := range request.ExactSourceRefs {
		kind, id, ok := parseSourceRef(ref)
		if ok {
			ids[kind] = append(ids[kind], id)
		}
	}
	if len(ids["memo"]) > 0 {
		rows, err := tx.Query(ctx, `
			SELECT id, title, content, sha256
			FROM fornix.memos
			WHERE workspace_id=$1 AND id=ANY($2) AND deleted_at IS NULL
			ORDER BY id LIMIT $3`, request.WorkspaceID, ids["memo"], limit)
		queries++
		if err != nil {
			return result, queries, err
		}
		for rows.Next() {
			var id int64
			var title, content, evidence string
			if err := rows.Scan(&id, &title, &content, &evidence); err != nil {
				rows.Close()
				return result, queries, err
			}
			result = append(result, sourceCandidate(request.WorkspaceID, "memo", id, title+"\n"+content, evidence, 1.0, contracts.StageStructured, "detail", 0))
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return result, queries, err
		}
	}
	if len(ids["chunk"]) > 0 {
		rows, err := tx.Query(ctx, `
			SELECT id, source_path, source_range, content, content_sha256
			FROM fornix.chunks
			WHERE workspace_id=$1 AND id=ANY($2)
			ORDER BY id LIMIT $3`, request.WorkspaceID, ids["chunk"], limit)
		queries++
		if err != nil {
			return result, queries, err
		}
		for rows.Next() {
			var id int64
			var sourcePath, sourceRange, content, evidence string
			if err := rows.Scan(&id, &sourcePath, &sourceRange, &content, &evidence); err != nil {
				rows.Close()
				return result, queries, err
			}
			result = append(result, sourceCandidate(request.WorkspaceID, "chunk", id, content, evidence, 1.0, contracts.StageStructured, "detail", 0))
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return result, queries, err
		}
	}
	if len(ids["symbol"]) > 0 {
		rows, err := tx.Query(ctx, symbolSelect+`
			WHERE workspace_id=$1 AND id=ANY($2) AND deleted_at IS NULL
			ORDER BY id LIMIT $3`, request.WorkspaceID, ids["symbol"], limit)
		queries++
		if err != nil {
			return result, queries, err
		}
		var appendErr error
		result, appendErr = appendSymbolRows(result, rows, request.WorkspaceID, contracts.StageStructured, 1.0)
		if appendErr != nil {
			return result, queries, appendErr
		}
	}
	if request.MemoType != "" {
		rows, err := tx.Query(ctx, `
			SELECT id, title, content, sha256
			FROM fornix.memos
			WHERE workspace_id=$1 AND type=$2 AND deleted_at IS NULL
			ORDER BY id LIMIT $3`, request.WorkspaceID, request.MemoType, limit)
		queries++
		if err != nil {
			return result, queries, err
		}
		for rows.Next() {
			var id int64
			var title, content, evidence string
			if err := rows.Scan(&id, &title, &content, &evidence); err != nil {
				rows.Close()
				return result, queries, err
			}
			result = append(result, sourceCandidate(request.WorkspaceID, "memo", id, title+"\n"+content, evidence, 0.95, contracts.StageStructured, "detail", 0))
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return result, queries, err
		}
	}
	if request.Repo != "" {
		rows, err := tx.Query(ctx, symbolSelect+`
			WHERE workspace_id=$1 AND repo=$2 AND deleted_at IS NULL
			ORDER BY repo, file_path, symbol_name, id LIMIT $3`, request.WorkspaceID, request.Repo, limit)
		queries++
		if err != nil {
			return result, queries, err
		}
		var appendErr error
		result, appendErr = appendSymbolRows(result, rows, request.WorkspaceID, contracts.StageStructured, 0.95)
		if appendErr != nil {
			return result, queries, appendErr
		}
	}
	if request.TaskID != "" {
		rows, err := tx.Query(ctx, `
			SELECT sequence, event_id, raw_payload
			FROM fornix.control_events
			WHERE workspace_id=$1 AND task_ref->>'id'=$2
			ORDER BY sequence LIMIT $3`, request.WorkspaceID, request.TaskID, limit)
		queries++
		if err != nil {
			return result, queries, err
		}
		for rows.Next() {
			var sequence int64
			var eventID string
			var raw []byte
			if err := rows.Scan(&sequence, &eventID, &raw); err != nil {
				rows.Close()
				return result, queries, err
			}
			ref := "event:" + strconv.FormatInt(sequence, 10)
			result = append(result, eventCandidate(request.WorkspaceID, ref, raw, eventID, 1.0))
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return result, queries, err
		}
	}
	return result, queries, nil
}

func (s *Store) lexical(ctx context.Context, tx pgx.Tx, request contracts.RetrievalRequest, limit int) ([]candidate, int, error) {
	result := make([]candidate, 0)
	queries := 0
	rows, err := tx.Query(ctx, `
		SELECT id, title, content, sha256,
		       LEAST(1.0, GREATEST(0.0, ts_rank_cd(tsv, plainto_tsquery('english', $2)))) AS score
		FROM fornix.memos
		WHERE workspace_id=$1 AND deleted_at IS NULL
		  AND tsv @@ plainto_tsquery('english', $2)
		ORDER BY score DESC, id LIMIT $3`, request.WorkspaceID, request.Query, limit)
	queries++
	if err != nil {
		return result, queries, err
	}
	for rows.Next() {
		var id int64
		var title, content, evidence string
		var score float64
		if err := rows.Scan(&id, &title, &content, &evidence, &score); err != nil {
			rows.Close()
			return result, queries, err
		}
		result = append(result, sourceCandidate(request.WorkspaceID, "memo", id, title+"\n"+content, evidence, score, contracts.StageLexical, "detail", 0))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result, queries, err
	}

	rows, err = tx.Query(ctx, `
		SELECT id, source_path, source_range, content, content_sha256,
		       LEAST(1.0, GREATEST(0.0, ts_rank_cd(tsv, plainto_tsquery('english', $2)))) AS score
		FROM fornix.chunks
		WHERE workspace_id=$1 AND tsv @@ plainto_tsquery('english', $2)
		ORDER BY score DESC, id LIMIT $3`, request.WorkspaceID, request.Query, limit)
	queries++
	if err != nil {
		return result, queries, err
	}
	for rows.Next() {
		var id int64
		var sourcePath, sourceRange, content, evidence string
		var score float64
		if err := rows.Scan(&id, &sourcePath, &sourceRange, &content, &evidence, &score); err != nil {
			rows.Close()
			return result, queries, err
		}
		result = append(result, sourceCandidate(request.WorkspaceID, "chunk", id, content, evidence, score, contracts.StageLexical, "detail", 0))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result, queries, err
	}

	rows, err = tx.Query(ctx, symbolSelect+` ,
		       CASE
		         WHEN lower(symbol_name)=lower($2) THEN 1.0
		         WHEN symbol_name ILIKE $2 || '%' THEN 0.85
		         WHEN symbol_name ILIKE '%' || $2 || '%' THEN 0.65
		         WHEN signature ILIKE '%' || $2 || '%' OR docstring ILIKE '%' || $2 || '%' THEN 0.45
		         ELSE 0.0
		       END AS score
		FROM fornix.symbols
		WHERE workspace_id=$1 AND deleted_at IS NULL
		  AND (symbol_name ILIKE '%' || $2 || '%'
		       OR signature ILIKE '%' || $2 || '%'
		       OR docstring ILIKE '%' || $2 || '%')
		ORDER BY score DESC, repo, file_path, symbol_name, id LIMIT $3`, request.WorkspaceID, request.Query, limit)
	queries++
	if err != nil {
		return result, queries, err
	}
	var appendErr error
	result, appendErr = appendSymbolRowsWithScore(result, rows, request.WorkspaceID, contracts.StageLexical)
	if appendErr != nil {
		return result, queries, appendErr
	}
	return result, queries, nil
}

func (s *Store) graph(ctx context.Context, tx pgx.Tx, request contracts.RetrievalRequest, roots map[int64]candidate, limit int) ([]candidate, int, error) {
	ids := make([]int64, 0, len(roots))
	for id := range roots {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	rows, err := tx.Query(ctx, `
		SELECT e.src_id AS anchor_id, e.dst_id AS neighbor_id, e.edge_kind,
		       s.id, s.repo, s.file_path, s.symbol_name, s.symbol_kind,
		       s.language, s.line_start, s.line_end, COALESCE(s.signature,''),
		       COALESCE(s.docstring,''), s.sha256
		FROM fornix.symbol_edges e
		JOIN fornix.symbols root ON root.id=e.src_id AND root.workspace_id=$1
		JOIN fornix.symbols s ON s.id=e.dst_id AND s.workspace_id=$1
		WHERE e.src_id=ANY($2) AND s.deleted_at IS NULL
		UNION ALL
		SELECT e.dst_id AS anchor_id, e.src_id AS neighbor_id, e.edge_kind,
		       s.id, s.repo, s.file_path, s.symbol_name, s.symbol_kind,
		       s.language, s.line_start, s.line_end, COALESCE(s.signature,''),
		       COALESCE(s.docstring,''), s.sha256
		FROM fornix.symbol_edges e
		JOIN fornix.symbols root ON root.id=e.dst_id AND root.workspace_id=$1
		JOIN fornix.symbols s ON s.id=e.src_id AND s.workspace_id=$1
		WHERE e.dst_id=ANY($2) AND s.deleted_at IS NULL
		ORDER BY anchor_id, neighbor_id, edge_kind LIMIT $3`, request.WorkspaceID, ids, limit)
	if err != nil {
		return nil, 1, err
	}
	defer rows.Close()
	result := make([]candidate, 0)
	for rows.Next() {
		var anchorID, neighborID, id int64
		var edgeKind, repo, filePath, symbolName, symbolKind, language, signature, docstring, evidence string
		var lineStart, lineEnd int
		if err := rows.Scan(&anchorID, &neighborID, &edgeKind, &id, &repo, &filePath, &symbolName, &symbolKind, &language, &lineStart, &lineEnd, &signature, &docstring, &evidence); err != nil {
			return nil, 1, err
		}
		anchor, ok := roots[anchorID]
		if !ok {
			continue
		}
		item := sourceCandidate(request.WorkspaceID, "symbol", id, symbolText(repo, filePath, symbolName, symbolKind, language, lineStart, lineEnd, signature, docstring), evidence, clampScore(anchor.item.Score*0.8), contracts.StageGraph, "detail", id)
		item.item.Provenance = append(item.item.Provenance, contracts.Provenance{SourcePaths: []string{anchor.item.SourceReference + " --" + edgeKind + "--> symbol:" + strconv.FormatInt(neighborID, 10)}})
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 1, err
	}
	return result, 1, nil
}

func (s *Store) vector(ctx context.Context, tx pgx.Tx, request contracts.RetrievalRequest, limit int) ([]candidate, int, error) {
	vector := pgvector.NewVector(request.QueryEmbedding)
	result := make([]candidate, 0)
	queries := 0
	rows, err := tx.Query(ctx, `
		SELECT id, title, content, sha256,
		       LEAST(1.0, GREATEST(0.0, 1.0-(embedding <=> $2))) AS score
		FROM fornix.memos
		WHERE workspace_id=$1 AND deleted_at IS NULL AND embedding IS NOT NULL
		ORDER BY embedding <=> $2, id LIMIT $3`, request.WorkspaceID, vector, limit)
	queries++
	if err != nil {
		return result, queries, err
	}
	for rows.Next() {
		var id int64
		var title, content, evidence string
		var score float64
		if err := rows.Scan(&id, &title, &content, &evidence, &score); err != nil {
			rows.Close()
			return result, queries, err
		}
		result = append(result, sourceCandidate(request.WorkspaceID, "memo", id, title+"\n"+content, evidence, score, contracts.StageVector, "detail", 0))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result, queries, err
	}

	rows, err = tx.Query(ctx, `
		SELECT id, source_path, source_range, content, content_sha256,
		       LEAST(1.0, GREATEST(0.0, 1.0-(embedding <=> $2))) AS score
		FROM fornix.chunks
		WHERE workspace_id=$1 AND embedding IS NOT NULL
		ORDER BY embedding <=> $2, id LIMIT $3`, request.WorkspaceID, vector, limit)
	queries++
	if err != nil {
		return result, queries, err
	}
	for rows.Next() {
		var id int64
		var sourcePath, sourceRange, content, evidence string
		var score float64
		if err := rows.Scan(&id, &sourcePath, &sourceRange, &content, &evidence, &score); err != nil {
			rows.Close()
			return result, queries, err
		}
		result = append(result, sourceCandidate(request.WorkspaceID, "chunk", id, content, evidence, score, contracts.StageVector, "detail", 0))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result, queries, err
	}

	rows, err = tx.Query(ctx, symbolSelect+` ,
		       LEAST(1.0, GREATEST(0.0, 1.0-(embedding <=> $2))) AS score
		FROM fornix.symbols
		WHERE workspace_id=$1 AND deleted_at IS NULL AND embedding IS NOT NULL
		ORDER BY embedding <=> $2, id LIMIT $3`, request.WorkspaceID, vector, limit)
	queries++
	if err != nil {
		return result, queries, err
	}
	var appendErr error
	result, appendErr = appendSymbolRowsWithScore(result, rows, request.WorkspaceID, contracts.StageVector)
	if appendErr != nil {
		return result, queries, appendErr
	}
	return result, queries, nil
}

const symbolSelect = `SELECT id, repo, file_path, symbol_name, symbol_kind, language,
       line_start, line_end, COALESCE(signature,''), COALESCE(docstring,''), sha256`

func appendSymbolRows(result []candidate, rows pgx.Rows, workspaceID string, stage contracts.RetrievalStage, score float64) ([]candidate, error) {
	defer rows.Close()
	for rows.Next() {
		var id int64
		var repo, filePath, symbolName, symbolKind, language, signature, docstring, evidence string
		var lineStart, lineEnd int
		if err := rows.Scan(&id, &repo, &filePath, &symbolName, &symbolKind, &language, &lineStart, &lineEnd, &signature, &docstring, &evidence); err != nil {
			return result, err
		}
		result = append(result, sourceCandidate(workspaceID, "symbol", id, symbolText(repo, filePath, symbolName, symbolKind, language, lineStart, lineEnd, signature, docstring), evidence, score, stage, "detail", id))
	}
	return result, rows.Err()
}

func appendSymbolRowsWithScore(result []candidate, rows pgx.Rows, workspaceID string, stage contracts.RetrievalStage) ([]candidate, error) {
	defer rows.Close()
	for rows.Next() {
		var id int64
		var repo, filePath, symbolName, symbolKind, language, signature, docstring, evidence string
		var lineStart, lineEnd int
		var score float64
		if err := rows.Scan(&id, &repo, &filePath, &symbolName, &symbolKind, &language, &lineStart, &lineEnd, &signature, &docstring, &evidence, &score); err != nil {
			return result, err
		}
		result = append(result, sourceCandidate(workspaceID, "symbol", id, symbolText(repo, filePath, symbolName, symbolKind, language, lineStart, lineEnd, signature, docstring), evidence, score, stage, "detail", id))
	}
	return result, rows.Err()
}

func sourceCandidate(workspaceID, kind string, id int64, text, evidence string, score float64, stage contracts.RetrievalStage, representation string, symbolID int64) candidate {
	ref := kind + ":" + strconv.FormatInt(id, 10)
	return candidate{
		item: contracts.ContextItem{
			WorkspaceID: workspaceID, SourceReference: ref, Kind: kind,
			Representation: representation, Text: text, EvidenceHash: evidence,
			Score: clampScore(score), Stage: stage,
			Provenance: []contracts.Provenance{{SourceArtifactRefs: []string{ref}}},
		},
		symbolID: symbolID,
		stage:    stage,
	}
}

func eventCandidate(workspaceID, ref string, raw []byte, eventID string, score float64) candidate {
	digest := sha256.Sum256(raw)
	return candidate{
		item: contracts.ContextItem{
			WorkspaceID: workspaceID, SourceReference: ref, Kind: "event",
			Representation: "raw", Text: string(raw), EvidenceHash: hex.EncodeToString(digest[:]),
			Score: score, Stage: contracts.StageStructured,
			Provenance: []contracts.Provenance{{SourceEventIDs: []string{eventID}}},
		},
		stage: contracts.StageStructured,
	}
}

func symbolText(repo, filePath, symbolName, symbolKind, language string, lineStart, lineEnd int, signature, docstring string) string {
	return fmt.Sprintf("%s/%s:%s (%s, %s, lines %d-%d)\n%s\n%s", repo, filePath, symbolName, symbolKind, language, lineStart, lineEnd, signature, docstring)
}

func parseSourceRef(ref string) (string, int64, bool) {
	parts := strings.SplitN(strings.TrimSpace(ref), ":", 2)
	if len(parts) != 2 || (parts[0] != "memo" && parts[0] != "chunk" && parts[0] != "symbol") {
		return "", 0, false
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	return parts[0], id, err == nil && id > 0
}

func (set *candidateSet) add(values []candidate) (int, int) {
	accepted, duplicates := 0, 0
	for _, value := range values {
		key := value.item.EvidenceHash
		if key == "" {
			key = value.item.SourceReference
		}
		previous, exists := set.byKey[key]
		if !exists {
			set.byKey[key] = value
			accepted++
			continue
		}
		duplicates++
		if better(value, previous) {
			value.item.Provenance = mergeProvenance(value.item.Provenance, previous.item.Provenance)
			set.byKey[key] = value
		} else {
			previous.item.Provenance = mergeProvenance(previous.item.Provenance, value.item.Provenance)
			set.byKey[key] = previous
		}
	}
	return accepted, duplicates
}

func (set *candidateSet) symbolRoots() map[int64]candidate {
	result := make(map[int64]candidate)
	for _, value := range set.byKey {
		if value.symbolID == 0 {
			continue
		}
		previous, exists := result[value.symbolID]
		if !exists || better(value, previous) {
			result[value.symbolID] = value
		}
	}
	return result
}

func (set *candidateSet) satisfied(request contracts.RetrievalRequest) bool {
	if len(set.byKey) >= request.MaxItems {
		return true
	}
	if len(set.byKey) < request.MinResults {
		return false
	}
	best := 0.0
	for _, value := range set.byKey {
		if value.item.Score > best {
			best = value.item.Score
		}
	}
	return best >= request.MinScore
}

func better(a, b candidate) bool {
	if a.item.Score != b.item.Score {
		return a.item.Score > b.item.Score
	}
	if stagePriority(a.stage) != stagePriority(b.stage) {
		return stagePriority(a.stage) < stagePriority(b.stage)
	}
	if a.item.Kind != b.item.Kind {
		return a.item.Kind < b.item.Kind
	}
	return a.item.SourceReference < b.item.SourceReference
}

func stagePriority(stage contracts.RetrievalStage) int {
	switch stage {
	case contracts.StageStructured:
		return 0
	case contracts.StageLexical:
		return 1
	case contracts.StageGraph:
		return 2
	case contracts.StageVector:
		return 3
	default:
		return 99
	}
}

func mergeProvenance(values ...[]contracts.Provenance) []contracts.Provenance {
	all := make([]contracts.Provenance, 0)
	seen := make(map[string]struct{})
	for _, group := range values {
		for _, value := range group {
			b, _ := json.Marshal(value)
			key := string(b)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			all = append(all, value)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		left, _ := json.Marshal(all[i])
		right, _ := json.Marshal(all[j])
		return string(left) < string(right)
	})
	return all
}

func clampScore(score float64) float64 {
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}
