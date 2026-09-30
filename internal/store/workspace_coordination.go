package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrCoordinationMessageMissing = errors.New("workspace coordination message not found")
	ErrCoordinationConflict       = errors.New("workspace coordination idempotency conflict")
	ErrRouterObservationConflict  = errors.New("workspace router observation idempotency conflict")
)

const (
	defaultCoordinationReadLimit = 50
	maxCoordinationReadLimit     = 500
	maxRouterRecommendationLimit = 100
)

// WorkspaceCoordinationStore owns the workspace-scoped replacements for the
// historical global coordination and router-learning tables. It emits typed
// events in the same transaction as each authoritative row.
type WorkspaceCoordinationStore struct {
	pool        *pgxpool.Pool
	events      *EventStore
	failureMu   sync.Mutex
	failureHook func(string) error
}

func NewWorkspaceCoordinationStore(pool *pgxpool.Pool, events *EventStore) *WorkspaceCoordinationStore {
	return &WorkspaceCoordinationStore{pool: pool, events: events}
}

// SetFailureHook is a qualification seam used to prove rollback at a commit
// boundary. Production callers leave it nil.
func (s *WorkspaceCoordinationStore) SetFailureHook(hook func(string) error) {
	if s == nil {
		return
	}
	s.failureMu.Lock()
	s.failureHook = hook
	s.failureMu.Unlock()
}

func (s *WorkspaceCoordinationStore) failure(point string) error {
	if s == nil {
		return nil
	}
	s.failureMu.Lock()
	hook := s.failureHook
	s.failureMu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(point)
}

// AppendMessage inserts one workspace message and its event atomically.
func (s *WorkspaceCoordinationStore) AppendMessage(ctx context.Context, message contracts.CoordinationMessage) (contracts.CoordinationMessage, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.CoordinationMessage{}, false, fmt.Errorf("workspace coordination store is not configured")
	}
	if err := message.Normalize(); err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, message.WorkspaceID)
	if err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var sequence int64
	var createdAt time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.workspace_coordination_messages
			(id, schema_version, workspace_id, request_id, idempotency_key, request_hash,
			 sender, recipient, subject, body, actor, causation_id, correlation_id,
			 origin_host, occurred_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15)
		ON CONFLICT (workspace_id, idempotency_key) DO NOTHING
		RETURNING sequence, created_at`,
		message.ID, message.SchemaVersion, message.WorkspaceID, message.RequestID, message.IdempotencyKey, message.RequestHash,
		message.Sender, message.Recipient, message.Subject, message.Body, mustJSON(message.Actor), message.CausationID,
		message.CorrelationID, message.OriginHost, message.OccurredAt).Scan(&sequence, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var existing contracts.CoordinationMessage
		existing, err = readCoordinationByKeyTx(ctx, tx, message.WorkspaceID, message.IdempotencyKey)
		if err != nil {
			return contracts.CoordinationMessage{}, false, err
		}
		if existing.RequestHash != message.RequestHash {
			return contracts.CoordinationMessage{}, false, fmt.Errorf("%w: %s", ErrCoordinationConflict, message.IdempotencyKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.CoordinationMessage{}, false, err
		}
		return existing, true, nil
	}
	if err != nil {
		return contracts.CoordinationMessage{}, false, fmt.Errorf("append workspace coordination message: %w", err)
	}
	message.Sequence = sequence
	message.CreatedAt = createdAt
	payload, err := json.Marshal(message)
	if err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	event := contracts.EventEnvelope{
		EventID: contracts.NewID("evt-coordination"), EventType: contracts.CoordinationMessageEventType,
		SchemaVersion: contracts.EventSchemaVersion, OccurredAt: message.OccurredAt,
		Scope: contracts.Scope{WorkspaceID: message.WorkspaceID}, Actor: message.Actor,
		CausationID: message.CausationID, CorrelationID: message.CorrelationID,
		IdempotencyKey: "coordination:event:" + message.IdempotencyKey, Payload: payload,
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	if err := s.failure("workspace_coordination_before_commit"); err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	return message, false, nil
}

// appendMessageTx is the transaction-composable form used by federation
// imports. The caller owns commit/rollback so imported messages and the poll
// attempt can share one authority boundary.
func (s *WorkspaceCoordinationStore) appendMessageTx(ctx context.Context, tx pgx.Tx, message contracts.CoordinationMessage) (contracts.CoordinationMessage, bool, error) {
	if s == nil || tx == nil || s.events == nil {
		return contracts.CoordinationMessage{}, false, fmt.Errorf("workspace coordination transaction is not configured")
	}
	if err := message.Normalize(); err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	var sequence int64
	var createdAt time.Time
	err := tx.QueryRow(ctx, `
		INSERT INTO fornix.workspace_coordination_messages
			(id, schema_version, workspace_id, request_id, idempotency_key, request_hash,
			 sender, recipient, subject, body, actor, causation_id, correlation_id,
			 origin_host, occurred_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15)
		ON CONFLICT (workspace_id, idempotency_key) DO NOTHING
		RETURNING sequence, created_at`, message.ID, message.SchemaVersion, message.WorkspaceID, message.RequestID,
		message.IdempotencyKey, message.RequestHash, message.Sender, message.Recipient, message.Subject, message.Body,
		mustJSON(message.Actor), message.CausationID, message.CorrelationID, message.OriginHost, message.OccurredAt).Scan(&sequence, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, readErr := readCoordinationByKeyTx(ctx, tx, message.WorkspaceID, message.IdempotencyKey)
		if readErr != nil {
			return contracts.CoordinationMessage{}, false, readErr
		}
		if existing.RequestHash != message.RequestHash {
			return contracts.CoordinationMessage{}, false, fmt.Errorf("%w: %s", ErrCoordinationConflict, message.IdempotencyKey)
		}
		return existing, true, nil
	}
	if err != nil {
		return contracts.CoordinationMessage{}, false, fmt.Errorf("append workspace coordination message: %w", err)
	}
	message.Sequence, message.CreatedAt = sequence, createdAt
	payload, err := json.Marshal(message)
	if err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	event := contracts.EventEnvelope{EventID: contracts.NewID("evt-coordination"), EventType: contracts.CoordinationMessageEventType, SchemaVersion: contracts.EventSchemaVersion, OccurredAt: message.OccurredAt, Scope: contracts.Scope{WorkspaceID: message.WorkspaceID}, Actor: message.Actor, CausationID: message.CausationID, CorrelationID: message.CorrelationID, IdempotencyKey: "coordination:event:" + message.IdempotencyKey, Payload: payload}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	if err := s.failure("workspace_coordination_before_commit"); err != nil {
		return contracts.CoordinationMessage{}, false, err
	}
	return message, false, nil
}

// ReadMessages reads a bounded, stable page after the supplied workspace
// sequence. The sequence is a cursor, not a timestamp and may contain gaps.
func (s *WorkspaceCoordinationStore) ReadMessages(ctx context.Context, workspaceID string, afterSequence int64, recipient string, limit int) ([]contracts.CoordinationMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("workspace coordination store is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, fmt.Errorf("workspace_id is required")
	}
	if afterSequence < 0 {
		return nil, fmt.Errorf("after sequence cannot be negative")
	}
	if limit <= 0 {
		limit = defaultCoordinationReadLimit
	}
	if limit > maxCoordinationReadLimit {
		limit = maxCoordinationReadLimit
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	query := `SELECT sequence,id,schema_version,workspace_id,request_id,idempotency_key,request_hash,sender,recipient,subject,body,actor,causation_id,correlation_id,origin_host,occurred_at,created_at FROM fornix.workspace_coordination_messages WHERE workspace_id=$1 AND sequence>$2`
	args := []any{workspaceID, afterSequence}
	if recipient = strings.TrimSpace(recipient); recipient != "" {
		query += " AND recipient=$3"
		args = append(args, recipient)
	}
	query += fmt.Sprintf(" ORDER BY sequence ASC LIMIT $%d", len(args)+1)
	args = append(args, limit)
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]contracts.CoordinationMessage, 0)
	for rows.Next() {
		message, scanErr := scanCoordinationMessage(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return messages, nil
}

// AppendRouterObservation records one workspace telemetry point atomically
// with its event. It does not claim to replace provider/model-call authority.
func (s *WorkspaceCoordinationStore) AppendRouterObservation(ctx context.Context, observation contracts.RouterObservation) (contracts.RouterObservation, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.RouterObservation{}, false, fmt.Errorf("workspace coordination store is not configured")
	}
	if err := observation.Normalize(); err != nil {
		return contracts.RouterObservation{}, false, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, observation.WorkspaceID)
	if err != nil {
		return contracts.RouterObservation{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id int64
	var createdAt time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.workspace_router_observations
			(schema_version,workspace_id,request_id,idempotency_key,request_hash,task_category,model_id,
			 cost_usd,latency_ms,outcome,outcome_score,actor,causation_id,correlation_id,observed_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14,$15)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING
		RETURNING id, created_at`, observation.SchemaVersion, observation.WorkspaceID, observation.RequestID, observation.IdempotencyKey,
		observation.RequestHash, observation.TaskCategory, observation.ModelID, observation.CostUSD, observation.LatencyMS,
		observation.Outcome, observation.OutcomeScore, mustJSON(observation.Actor), observation.CausationID,
		observation.CorrelationID, observation.ObservedAt).Scan(&id, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var existing contracts.RouterObservation
		existing, err = readRouterObservationByKeyTx(ctx, tx, observation.WorkspaceID, observation.IdempotencyKey)
		if err != nil {
			return contracts.RouterObservation{}, false, err
		}
		if existing.RequestHash != observation.RequestHash {
			return contracts.RouterObservation{}, false, fmt.Errorf("%w: %s", ErrRouterObservationConflict, observation.IdempotencyKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.RouterObservation{}, false, err
		}
		return existing, true, nil
	}
	if err != nil {
		return contracts.RouterObservation{}, false, err
	}
	observation.ID = id
	observation.CreatedAt = createdAt
	payload, err := json.Marshal(observation)
	if err != nil {
		return contracts.RouterObservation{}, false, err
	}
	event := contracts.EventEnvelope{
		EventID: contracts.NewID("evt-router"), EventType: contracts.RouterObservationEventType,
		SchemaVersion: contracts.EventSchemaVersion, OccurredAt: observation.ObservedAt,
		Scope: contracts.Scope{WorkspaceID: observation.WorkspaceID}, Actor: observation.Actor,
		CausationID: observation.CausationID, CorrelationID: observation.CorrelationID,
		IdempotencyKey: "router:event:" + observation.IdempotencyKey, Payload: payload,
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.RouterObservation{}, false, err
	}
	if err := s.failure("workspace_router_before_commit"); err != nil {
		return contracts.RouterObservation{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.RouterObservation{}, false, err
	}
	return observation, false, nil
}

// Recommend derives a deterministic cost-aware recommendation from one
// workspace and bounded recent observations. Ties are resolved by model ID.
func (s *WorkspaceCoordinationStore) Recommend(ctx context.Context, workspaceID, category string, limit int) ([]contracts.RouterRecommendation, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("workspace coordination store is not configured")
	}
	workspaceID, category = strings.TrimSpace(workspaceID), strings.TrimSpace(category)
	if workspaceID == "" || category == "" {
		return nil, fmt.Errorf("workspace_id and category are required")
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > maxRouterRecommendationLimit {
		limit = maxRouterRecommendationLimit
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT model_id, AVG(cost_usd)::float8,
		       percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)::float8,
		       (SUM(CASE WHEN outcome='success' THEN 1 ELSE 0 END)::float8 / NULLIF(COUNT(*),0))::float8,
		       COUNT(*)::bigint
		FROM fornix.workspace_router_observations
		WHERE workspace_id=$1 AND task_category=$2 AND observed_at > clock_timestamp() - INTERVAL '30 days'
		GROUP BY model_id
		HAVING COUNT(*) >= 1`, workspaceID, category)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	recommendations := make([]contracts.RouterRecommendation, 0)
	for rows.Next() {
		var recommendation contracts.RouterRecommendation
		if err := rows.Scan(&recommendation.ModelID, &recommendation.CostUSDAvg, &recommendation.LatencyP50, &recommendation.SuccessRate, &recommendation.SampleSize); err != nil {
			return nil, err
		}
		recommendations = append(recommendations, recommendation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(recommendations, func(i, j int) bool {
		left := recommendations[i].SuccessRate / maxFloat(recommendations[i].CostUSDAvg, 1e-9)
		right := recommendations[j].SuccessRate / maxFloat(recommendations[j].CostUSDAvg, 1e-9)
		if left != right {
			return left > right
		}
		if recommendations[i].SuccessRate != recommendations[j].SuccessRate {
			return recommendations[i].SuccessRate > recommendations[j].SuccessRate
		}
		return recommendations[i].ModelID < recommendations[j].ModelID
	})
	if len(recommendations) > limit {
		recommendations = recommendations[:limit]
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return recommendations, nil
}

func readCoordinationByKeyTx(ctx context.Context, tx pgx.Tx, workspaceID, idempotencyKey string) (contracts.CoordinationMessage, error) {
	return scanCoordinationMessage(tx.QueryRow(ctx, `SELECT sequence,id,schema_version,workspace_id,request_id,idempotency_key,request_hash,sender,recipient,subject,body,actor,causation_id,correlation_id,origin_host,occurred_at,created_at FROM fornix.workspace_coordination_messages WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, workspaceID, idempotencyKey))
}

func readRouterObservationByKeyTx(ctx context.Context, tx pgx.Tx, workspaceID, idempotencyKey string) (contracts.RouterObservation, error) {
	return scanRouterObservation(tx.QueryRow(ctx, `SELECT id,schema_version,workspace_id,request_id,idempotency_key,request_hash,task_category,model_id,cost_usd,latency_ms,outcome,outcome_score,actor,causation_id,correlation_id,observed_at,created_at FROM fornix.workspace_router_observations WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, workspaceID, idempotencyKey))
}

func scanCoordinationMessage(row interface{ Scan(...any) error }) (contracts.CoordinationMessage, error) {
	var message contracts.CoordinationMessage
	var actorJSON []byte
	err := row.Scan(&message.Sequence, &message.ID, &message.SchemaVersion, &message.WorkspaceID, &message.RequestID, &message.IdempotencyKey, &message.RequestHash, &message.Sender, &message.Recipient, &message.Subject, &message.Body, &actorJSON, &message.CausationID, &message.CorrelationID, &message.OriginHost, &message.OccurredAt, &message.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.CoordinationMessage{}, ErrCoordinationMessageMissing
	}
	if err != nil {
		return contracts.CoordinationMessage{}, err
	}
	if err := json.Unmarshal(actorJSON, &message.Actor); err != nil {
		return contracts.CoordinationMessage{}, err
	}
	return message, nil
}

func scanRouterObservation(row interface{ Scan(...any) error }) (contracts.RouterObservation, error) {
	var observation contracts.RouterObservation
	var actorJSON []byte
	err := row.Scan(&observation.ID, &observation.SchemaVersion, &observation.WorkspaceID, &observation.RequestID, &observation.IdempotencyKey, &observation.RequestHash, &observation.TaskCategory, &observation.ModelID, &observation.CostUSD, &observation.LatencyMS, &observation.Outcome, &observation.OutcomeScore, &actorJSON, &observation.CausationID, &observation.CorrelationID, &observation.ObservedAt, &observation.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.RouterObservation{}, ErrRouterObservationConflict
	}
	if err != nil {
		return contracts.RouterObservation{}, err
	}
	if err := json.Unmarshal(actorJSON, &observation.Actor); err != nil {
		return contracts.RouterObservation{}, err
	}
	return observation, nil
}

func maxFloat(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}
