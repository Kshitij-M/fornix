package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrRetrievalSurfaceNotFound = errors.New("retrieval surface not found")
	ErrRetrievalSurfaceConflict = errors.New("retrieval surface conflict")
	ErrRetrievalSurfaceCursor   = errors.New("invalid retrieval surface cursor")
)

const (
	DefaultRetrievalSurfacePageSize = 50
	MaxRetrievalSurfacePageSize     = 100
)

// RetrievalSurfaceStore is the Postgres authority for redacted retrieval
// captures. Surface rows are append-only and never become retrieval truth.
type RetrievalSurfaceStore struct {
	pool        *pgxpool.Pool
	failureHook func(string) error
}

// NewRetrievalSurfaceStore constructs the append-only capture store.
func NewRetrievalSurfaceStore(pool *pgxpool.Pool) *RetrievalSurfaceStore {
	return &RetrievalSurfaceStore{pool: pool}
}

// SetFailureHook is used by crash-recovery tests to inject a failure at a
// transaction boundary. It is intentionally not exposed through the server.
func (s *RetrievalSurfaceStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

// Capture records one redacted retrieval surface idempotently. The capture is
// diagnostic/evaluation history; authoritative evidence and source records stay
// in their own stores.
func (s *RetrievalSurfaceStore) Capture(ctx context.Context, surface contracts.RetrievalSurface) (contracts.RetrievalSurface, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.RetrievalSurface{}, false, fmt.Errorf("retrieval surface store is not configured")
	}
	// Normalize trims and redacts nested slices. Copy them before normalization
	// so concurrent callers may safely retry the same request value.
	surface.Trace.Stages = append([]contracts.RetrievalStageTrace(nil), surface.Trace.Stages...)
	surface.References = append([]contracts.RetrievalSurfaceReference(nil), surface.References...)
	if err := surface.Normalize(); err != nil {
		return contracts.RetrievalSurface{}, false, err
	}
	budget, _ := json.Marshal(surface.Budget)
	trace, _ := json.Marshal(surface.Trace)
	references, _ := json.Marshal(surface.References)
	actor, _ := json.Marshal(surface.Actor)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.RetrievalSurface{}, false, fmt.Errorf("begin retrieval surface capture: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	commandTag, err := tx.Exec(ctx, `
		INSERT INTO fornix.retrieval_surfaces(
			id, workspace_id, request_id, idempotency_key, payload_hash,
			request_hash, plan_hash, context_hash, budget, trace, evidence_refs,
			duration_ms, sql_queries, cost_usd, cost_known, cost_estimated,
			actor, causation_id, correlation_id, captured_at
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb,
			$12,$13,$14,$15,$16,$17::jsonb,$18,$19,$20)
		ON CONFLICT (workspace_id, idempotency_key) DO NOTHING`,
		surface.ID, surface.WorkspaceID, surface.RequestID, surface.IdempotencyKey, surface.PayloadHash,
		surface.RequestHash, surface.PlanHash, surface.ContextHash, budget, trace, references,
		surface.DurationMS, surface.SQLQueries, surface.CostUSD, surface.CostKnown, surface.CostEstimated,
		actor, surface.CausationID, surface.CorrelationID, surface.CapturedAt)
	if err != nil {
		return contracts.RetrievalSurface{}, false, fmt.Errorf("insert retrieval surface: %w", err)
	}
	if s.failureHook != nil {
		if err := s.failureHook("inserted"); err != nil {
			return contracts.RetrievalSurface{}, false, err
		}
	}
	stored, err := readRetrievalSurfaceByKeyTx(ctx, tx, surface.WorkspaceID, surface.IdempotencyKey)
	if err != nil {
		return contracts.RetrievalSurface{}, false, err
	}
	if stored.PayloadHash != surface.PayloadHash {
		return contracts.RetrievalSurface{}, false, fmt.Errorf("%w: %s", ErrRetrievalSurfaceConflict, surface.IdempotencyKey)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.RetrievalSurface{}, false, fmt.Errorf("commit retrieval surface: %w", err)
	}
	return stored, commandTag.RowsAffected() == 1, nil
}

// Get reads one captured surface within workspaceID.
func (s *RetrievalSurfaceStore) Get(ctx context.Context, workspaceID, id string) (contracts.RetrievalSurface, error) {
	if s == nil || s.pool == nil {
		return contracts.RetrievalSurface{}, fmt.Errorf("retrieval surface store is not configured")
	}
	var surface contracts.RetrievalSurface
	var budget, trace, references, actor []byte
	err := s.pool.QueryRow(ctx, retrievalSurfaceSelect+` WHERE workspace_id=$1 AND id=$2`, strings.TrimSpace(workspaceID), strings.TrimSpace(id)).Scan(retrievalSurfaceArgs(&surface, &budget, &trace, &references, &actor)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.RetrievalSurface{}, ErrRetrievalSurfaceNotFound
	}
	if err != nil {
		return contracts.RetrievalSurface{}, err
	}
	if err := decodeRetrievalSurface(&surface, budget, trace, references, actor); err != nil {
		return contracts.RetrievalSurface{}, err
	}
	return surface, nil
}

// GetMany is used by bounded evaluations so one dataset batch performs one
// indexed surface read instead of one query per case.
func (s *RetrievalSurfaceStore) GetMany(ctx context.Context, workspaceID string, ids []string) (map[string]contracts.RetrievalSurface, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("retrieval surface store is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	ids = uniqueSorted(ids)
	if len(ids) == 0 {
		return map[string]contracts.RetrievalSurface{}, nil
	}
	if len(ids) > contracts.MaxEvalCases {
		return nil, fmt.Errorf("retrieval surface batch exceeds %d", contracts.MaxEvalCases)
	}
	rows, err := s.pool.Query(ctx, retrievalSurfaceSelect+` WHERE workspace_id=$1 AND id=ANY($2) ORDER BY captured_at,id`, workspaceID, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]contracts.RetrievalSurface, len(ids))
	for rows.Next() {
		var surface contracts.RetrievalSurface
		var budget, trace, references, actor []byte
		if err := rows.Scan(retrievalSurfaceArgs(&surface, &budget, &trace, &references, &actor)...); err != nil {
			return nil, err
		}
		if err := decodeRetrievalSurface(&surface, budget, trace, references, actor); err != nil {
			return nil, err
		}
		result[surface.ID] = surface
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) != len(ids) {
		return nil, ErrRetrievalSurfaceNotFound
	}
	return result, nil
}

// List returns a bounded page ordered by capture time and ID so evaluation and
// operator pagination are repeatable.
func (s *RetrievalSurfaceStore) List(ctx context.Context, workspaceID string, limit int, cursor string) (contracts.RetrievalSurfacePage, error) {
	if s == nil || s.pool == nil {
		return contracts.RetrievalSurfacePage{}, fmt.Errorf("retrieval surface store is not configured")
	}
	if limit <= 0 {
		limit = DefaultRetrievalSurfacePageSize
	}
	if limit > MaxRetrievalSurfacePageSize {
		limit = MaxRetrievalSurfacePageSize
	}
	workspaceID = strings.TrimSpace(workspaceID)
	var rows pgx.Rows
	var err error
	if cursor == "" {
		rows, err = s.pool.Query(ctx, retrievalSurfaceSelect+` WHERE workspace_id=$1 ORDER BY captured_at,id LIMIT $2`, workspaceID, limit+1)
	} else {
		value, parseErr := decodeRetrievalSurfaceCursor(cursor)
		if parseErr != nil {
			return contracts.RetrievalSurfacePage{}, parseErr
		}
		rows, err = s.pool.Query(ctx, retrievalSurfaceSelect+` WHERE workspace_id=$1 AND (captured_at,id) > ($2,$3) ORDER BY captured_at,id LIMIT $4`, workspaceID, value.CapturedAt, value.ID, limit+1)
	}
	if err != nil {
		return contracts.RetrievalSurfacePage{}, err
	}
	defer rows.Close()
	page := contracts.RetrievalSurfacePage{Items: make([]contracts.RetrievalSurface, 0, limit)}
	for rows.Next() {
		var surface contracts.RetrievalSurface
		var budget, trace, references, actor []byte
		if err := rows.Scan(retrievalSurfaceArgs(&surface, &budget, &trace, &references, &actor)...); err != nil {
			return contracts.RetrievalSurfacePage{}, err
		}
		if err := decodeRetrievalSurface(&surface, budget, trace, references, actor); err != nil {
			return contracts.RetrievalSurfacePage{}, err
		}
		page.Items = append(page.Items, surface)
	}
	if err := rows.Err(); err != nil {
		return contracts.RetrievalSurfacePage{}, err
	}
	if len(page.Items) > limit {
		last := page.Items[limit-1]
		page.Items = page.Items[:limit]
		page.NextCursor = encodeRetrievalSurfaceCursor(last.CapturedAt, last.ID)
	}
	return page, nil
}

const retrievalSurfaceSelect = `SELECT id,workspace_id,request_id,idempotency_key,payload_hash,request_hash,plan_hash,context_hash,budget,trace,evidence_refs,duration_ms,sql_queries,cost_usd,cost_known,cost_estimated,actor,causation_id,correlation_id,captured_at FROM fornix.retrieval_surfaces`

func retrievalSurfaceArgs(surface *contracts.RetrievalSurface, budget, trace, references, actor *[]byte) []any {
	return []any{&surface.ID, &surface.WorkspaceID, &surface.RequestID, &surface.IdempotencyKey, &surface.PayloadHash, &surface.RequestHash, &surface.PlanHash, &surface.ContextHash, budget, trace, references, &surface.DurationMS, &surface.SQLQueries, &surface.CostUSD, &surface.CostKnown, &surface.CostEstimated, actor, &surface.CausationID, &surface.CorrelationID, &surface.CapturedAt}
}

func readRetrievalSurfaceByKeyTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (contracts.RetrievalSurface, error) {
	var surface contracts.RetrievalSurface
	var budget, trace, references, actor []byte
	err := tx.QueryRow(ctx, retrievalSurfaceSelect+` WHERE workspace_id=$1 AND idempotency_key=$2`, workspaceID, key).Scan(retrievalSurfaceArgs(&surface, &budget, &trace, &references, &actor)...)
	if err != nil {
		return contracts.RetrievalSurface{}, err
	}
	if err := decodeRetrievalSurface(&surface, budget, trace, references, actor); err != nil {
		return contracts.RetrievalSurface{}, err
	}
	return surface, nil
}

func decodeRetrievalSurface(surface *contracts.RetrievalSurface, budget, trace, references, actor []byte) error {
	if err := json.Unmarshal(budget, &surface.Budget); err != nil {
		return fmt.Errorf("decode retrieval surface budget: %w", err)
	}
	if err := json.Unmarshal(trace, &surface.Trace); err != nil {
		return fmt.Errorf("decode retrieval surface trace: %w", err)
	}
	if err := json.Unmarshal(references, &surface.References); err != nil {
		return fmt.Errorf("decode retrieval surface references: %w", err)
	}
	if err := json.Unmarshal(actor, &surface.Actor); err != nil {
		return fmt.Errorf("decode retrieval surface actor: %w", err)
	}
	return surface.Normalize()
}

type retrievalSurfaceCursor struct {
	CapturedAt time.Time `json:"captured_at"`
	ID         string    `json:"id"`
}

func encodeRetrievalSurfaceCursor(capturedAt time.Time, id string) string {
	raw, _ := json.Marshal(retrievalSurfaceCursor{CapturedAt: capturedAt.UTC(), ID: id})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeRetrievalSurfaceCursor(value string) (retrievalSurfaceCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return retrievalSurfaceCursor{}, ErrRetrievalSurfaceCursor
	}
	var cursor retrievalSurfaceCursor
	if json.Unmarshal(raw, &cursor) != nil || cursor.ID == "" || cursor.CapturedAt.IsZero() {
		return retrievalSurfaceCursor{}, ErrRetrievalSurfaceCursor
	}
	return cursor, nil
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
