package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrIdempotencyConflict   = errors.New("idempotency key reused with a different request")
	ErrIncompleteIdempotency = errors.New("idempotency record has no committed event")
	ErrInvalidCheckpoint     = errors.New("checkpoint must reference an existing event")
	ErrCheckpointRegression  = errors.New("checkpoint cannot move backwards")
)

const (
	defaultEventReadLimit = 500
	maxEventReadLimit     = 5000
)

// EventStore is the durable control-plane event boundary. Postgres is the
// authority; delivery caches and future brokers must be built on this API.
type EventStore struct {
	pool *pgxpool.Pool
}

// NewEventStore creates the append-only event authority over a Postgres pool.
func NewEventStore(pool *pgxpool.Pool) *EventStore {
	return &EventStore{pool: pool}
}

// Begin exposes the store-owned transaction boundary to projection runners.
// Domain code should prefer AppendTx when it is already inside a mutation
// transaction; subscribers use this to make projection and checkpoint writes
// atomic.
func (s *EventStore) Begin(ctx context.Context) (pgx.Tx, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("event store is not configured")
	}
	return s.pool.Begin(ctx)
}

// AppendResult reports whether an event was appended or matched an existing
// idempotency record.
type AppendResult struct {
	Event     contracts.EventEnvelope
	Duplicate bool
}

// Append commits exactly one event and, when supplied, its idempotency record.
func (s *EventStore) Append(ctx context.Context, event contracts.EventEnvelope) (AppendResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AppendResult{}, fmt.Errorf("begin event append: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result, err := s.AppendTx(ctx, tx, event)
	if err != nil {
		return AppendResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AppendResult{}, fmt.Errorf("commit event append: %w", err)
	}
	return result, nil
}

// AppendTx lets a domain mutation and its event become one atomic commit.
// Callers own the transaction lifecycle and must commit only after all
// authoritative state writes and the event append succeed.
func (s *EventStore) AppendTx(ctx context.Context, tx pgx.Tx, event contracts.EventEnvelope) (AppendResult, error) {
	if tx == nil {
		return AppendResult{}, fmt.Errorf("event append transaction is nil")
	}
	event = event.Clone()
	if err := event.Normalize(); err != nil {
		return AppendResult{}, fmt.Errorf("normalize event: %w", err)
	}
	requestHash, err := contracts.RequestHash(event)
	if err != nil {
		return AppendResult{}, err
	}
	workspaceID := event.Scope.WorkspaceID

	if event.IdempotencyKey != "" {
		inserted, err := tx.Exec(ctx, `
			INSERT INTO fornix.idempotency_records(workspace_id, idempotency_key, request_hash)
			VALUES($1, $2, $3)
			ON CONFLICT (workspace_id, idempotency_key) DO NOTHING`,
			workspaceID, event.IdempotencyKey, requestHash)
		if err != nil {
			return AppendResult{}, fmt.Errorf("reserve idempotency key: %w", err)
		}
		if inserted.RowsAffected() == 0 {
			var existingHash, existingEventID string
			var existingSequence *int64
			if err := tx.QueryRow(ctx, `
				SELECT request_hash, event_sequence, event_id
				FROM fornix.idempotency_records
				WHERE workspace_id=$1 AND idempotency_key=$2
				FOR UPDATE`, workspaceID, event.IdempotencyKey).
				Scan(&existingHash, &existingSequence, &existingEventID); err != nil {
				return AppendResult{}, fmt.Errorf("read idempotency record: %w", err)
			}
			if existingHash != requestHash {
				return AppendResult{}, fmt.Errorf("%w: %s", ErrIdempotencyConflict, event.IdempotencyKey)
			}
			if existingSequence == nil || *existingSequence <= 0 {
				return AppendResult{}, ErrIncompleteIdempotency
			}
			stored, err := readEventBySequenceTx(ctx, tx, workspaceID, *existingSequence)
			if err != nil {
				return AppendResult{}, fmt.Errorf("read duplicate event %s: %w", existingEventID, err)
			}
			return AppendResult{Event: stored, Duplicate: true}, nil
		}
	}

	scopeJSON, err := jsonString(event.Scope)
	if err != nil {
		return AppendResult{}, err
	}
	actorJSON, err := jsonString(event.Actor)
	if err != nil {
		return AppendResult{}, err
	}
	taskJSON, err := jsonStringOrEmpty(event.Task)
	if err != nil {
		return AppendResult{}, err
	}
	sessionJSON, err := jsonStringOrEmpty(event.Session)
	if err != nil {
		return AppendResult{}, err
	}
	deltasJSON, err := jsonString(event.StateDeltas)
	if err != nil {
		return AppendResult{}, err
	}
	artifactsJSON, err := jsonString(event.Artifacts)
	if err != nil {
		return AppendResult{}, err
	}
	provenanceJSON, err := jsonString(event.Provenance)
	if err != nil {
		return AppendResult{}, err
	}
	// JSONB gives queryable structure; raw_payload preserves the exact input
	// bytes for evidence, replay diagnostics, and future migrations.
	payloadJSON := string(event.Payload)

	var sequence int64
	var recordedAtValue = event.RecordedAt
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.control_events(
			event_id, event_type, schema_version, workspace_id, scope, actor,
			task_ref, session_ref, causation_id, correlation_id, idempotency_key,
			state_deltas, artifacts, provenance, payload, raw_payload, request_hash,
			occurred_at
		) VALUES(
			$1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb,
			$9, $10, $11, $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb,
			$16, $17, $18
		) RETURNING sequence, recorded_at`,
		event.EventID, event.EventType, event.SchemaVersion, workspaceID,
		scopeJSON, actorJSON, taskJSON, sessionJSON, event.CausationID,
		event.CorrelationID, event.IdempotencyKey, deltasJSON, artifactsJSON,
		provenanceJSON, payloadJSON, []byte(event.Payload), requestHash,
		event.OccurredAt).Scan(&sequence, &recordedAtValue)
	if err != nil {
		return AppendResult{}, fmt.Errorf("insert control event: %w", err)
	}
	if sequence <= 0 {
		return AppendResult{}, fmt.Errorf("database returned invalid event sequence %d", sequence)
	}
	event.Sequence = uint64(sequence)
	event.RecordedAt = recordedAtValue.UTC()
	// Keep one attributable, integrity-checked evidence record beside every
	// committed event. This is part of the caller-owned transaction: a failure
	// to preserve evidence must roll back the authoritative event as well.
	if _, err := NewEvidenceStore(s.pool).PutTx(ctx, tx, EvidencePutInput{
		WorkspaceID:      workspaceID,
		SourceReference:  fmt.Sprintf("event:%d", sequence),
		DeduplicationKey: event.EventID,
		Kind:             "control_event",
		MediaType:        "application/json",
		Gist:             event.EventType,
		Detail:           string(event.Payload),
		RawPayload:       event.Payload,
	}); err != nil {
		return AppendResult{}, fmt.Errorf("append event evidence: %w", err)
	}
	if event.IdempotencyKey != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE fornix.idempotency_records
			SET event_sequence=$3, event_id=$4
			WHERE workspace_id=$1 AND idempotency_key=$2`,
			workspaceID, event.IdempotencyKey, sequence, event.EventID); err != nil {
			return AppendResult{}, fmt.Errorf("complete idempotency record: %w", err)
		}
	}
	return AppendResult{Event: event}, nil
}

// ReadRequest bounds a workspace-scoped event read after a sequence cursor.
type ReadRequest struct {
	WorkspaceID   string
	AfterSequence uint64
	ToSequence    uint64
	EventType     string
	RunID         string
	TaskID        string
	SessionID     string
	Limit         int
}

// ReadAfter reads ordered events after the requested cursor.
func (s *EventStore) ReadAfter(ctx context.Context, request ReadRequest) ([]contracts.EventEnvelope, error) {
	return readAfter(ctx, s.pool, request)
}

// ReadAfterTx reads from the caller's transaction snapshot. Projection
// runners use it with EnsureCheckpointTx so the cursor, source events, and
// derived writes share one consistent commit boundary.
func (s *EventStore) ReadAfterTx(ctx context.Context, tx pgx.Tx, request ReadRequest) ([]contracts.EventEnvelope, error) {
	if tx == nil {
		return nil, fmt.Errorf("event read transaction is nil")
	}
	return readAfter(ctx, tx, request)
}

type eventQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func readAfter(ctx context.Context, queryer eventQueryer, request ReadRequest) ([]contracts.EventEnvelope, error) {
	workspaceID := strings.TrimSpace(request.WorkspaceID)
	if workspaceID == "" {
		return nil, fmt.Errorf("workspace_id is required")
	}
	limit := request.Limit
	if limit <= 0 {
		limit = defaultEventReadLimit
	}
	if limit > maxEventReadLimit {
		limit = maxEventReadLimit
	}
	args := []any{workspaceID, request.AfterSequence}
	query := `
		SELECT sequence, event_id, event_type, schema_version, workspace_id,
		       scope, actor, task_ref, session_ref, causation_id, correlation_id,
		       idempotency_key, state_deltas, artifacts, provenance, payload,
		       raw_payload, occurred_at, recorded_at
		FROM fornix.control_events
		WHERE workspace_id=$1 AND sequence>$2`
	if request.ToSequence > 0 {
		args = append(args, request.ToSequence)
		query += fmt.Sprintf(" AND sequence <= $%d", len(args))
	}
	if eventType := strings.TrimSpace(request.EventType); eventType != "" {
		args = append(args, eventType)
		query += fmt.Sprintf(" AND event_type = $%d", len(args))
	}
	if runID := strings.TrimSpace(request.RunID); runID != "" {
		args = append(args, runID)
		query += fmt.Sprintf(" AND (payload->>'run_id') = $%d", len(args))
	}
	if taskID := strings.TrimSpace(request.TaskID); taskID != "" {
		args = append(args, taskID)
		query += fmt.Sprintf(" AND task_ref->>'id' = $%d", len(args))
	}
	if sessionID := strings.TrimSpace(request.SessionID); sessionID != "" {
		args = append(args, sessionID)
		query += fmt.Sprintf(" AND session_ref->>'id' = $%d", len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY sequence ASC LIMIT $%d", len(args))

	rows, err := queryer.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read control events: %w", err)
	}
	defer rows.Close()
	events := make([]contracts.EventEnvelope, 0, limit)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("decode control event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate control events: %w", err)
	}
	return events, nil
}

// ReadAfterSequence is the compact workspace-scoped event read used by
// replaying consumers.
func (s *EventStore) ReadAfterSequence(ctx context.Context, workspaceID string, after uint64, limit int) ([]contracts.EventEnvelope, error) {
	return s.ReadAfter(ctx, ReadRequest{WorkspaceID: workspaceID, AfterSequence: after, Limit: limit})
}

// Replay reads an immutable sequence range and does not advance any consumer.
func (s *EventStore) Replay(ctx context.Context, workspaceID string, from, to uint64, limit int) ([]contracts.EventEnvelope, error) {
	if to > 0 && to < from {
		return nil, fmt.Errorf("replay upper sequence must be >= lower sequence")
	}
	return s.ReadAfter(ctx, ReadRequest{
		WorkspaceID: workspaceID, AfterSequence: from, ToSequence: to, Limit: limit,
	})
}

// AdvanceCheckpoint moves a consumer cursor monotonically in its own
// transaction. Projection runners should use the lease-protected variant.
func (s *EventStore) AdvanceCheckpoint(ctx context.Context, workspaceID, consumerID string, sequence uint64) error {
	tx, err := s.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin checkpoint advance: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.AdvanceCheckpointTx(ctx, tx, workspaceID, consumerID, sequence); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit checkpoint advance: %w", err)
	}
	return nil
}

// EnsureCheckpointTx creates a zero cursor if needed and locks the cursor row
// for the duration of the caller's transaction. This is the serialization
// point for concurrent runs of one consumer.
func (s *EventStore) EnsureCheckpointTx(ctx context.Context, tx pgx.Tx, workspaceID, consumerID string) (uint64, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	consumerID = strings.TrimSpace(consumerID)
	if workspaceID == "" || consumerID == "" {
		return 0, fmt.Errorf("workspace_id and consumer_id are required")
	}
	if tx == nil {
		return 0, fmt.Errorf("checkpoint transaction is nil")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.control_checkpoints(workspace_id, consumer_id, sequence)
		VALUES($1, $2, 0)
		ON CONFLICT (workspace_id, consumer_id) DO NOTHING`, workspaceID, consumerID); err != nil {
		return 0, fmt.Errorf("ensure checkpoint: %w", err)
	}
	var sequence int64
	if err := tx.QueryRow(ctx, `
		SELECT sequence
		FROM fornix.control_checkpoints
		WHERE workspace_id=$1 AND consumer_id=$2
		FOR UPDATE`, workspaceID, consumerID).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("lock checkpoint: %w", err)
	}
	if sequence < 0 {
		return 0, fmt.Errorf("database returned negative checkpoint")
	}
	return uint64(sequence), nil
}

// AdvanceCheckpointTx validates and advances a locked checkpoint. It never
// lowers a cursor, even when called by a stale or replaying worker.
func (s *EventStore) AdvanceCheckpointTx(ctx context.Context, tx pgx.Tx, workspaceID, consumerID string, sequence uint64) error {
	current, err := s.EnsureCheckpointTx(ctx, tx, workspaceID, consumerID)
	if err != nil {
		return err
	}
	return s.AdvanceCheckpointAtTx(ctx, tx, workspaceID, consumerID, current, sequence)
}

// AdvanceCheckpointAtTx advances a checkpoint whose row the caller already
// locked with EnsureCheckpointTx. Keeping the expected current value explicit
// avoids a second lock/read round trip inside a projection batch.
func (s *EventStore) AdvanceCheckpointAtTx(ctx context.Context, tx pgx.Tx, workspaceID, consumerID string, current, sequence uint64) error {
	workspaceID = strings.TrimSpace(workspaceID)
	consumerID = strings.TrimSpace(consumerID)
	if tx == nil {
		return fmt.Errorf("checkpoint transaction is nil")
	}
	if workspaceID == "" || consumerID == "" {
		return fmt.Errorf("workspace_id and consumer_id are required")
	}
	if current > uint64(1<<63-1) || sequence > uint64(1<<63-1) {
		return ErrInvalidCheckpoint
	}
	if sequence < current {
		return ErrCheckpointRegression
	}
	if sequence == current {
		return nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM fornix.control_events WHERE workspace_id=$1 AND sequence=$2)`,
		workspaceID, int64(sequence)).Scan(&exists); err != nil {
		return fmt.Errorf("validate checkpoint: %w", err)
	}
	if !exists {
		return ErrInvalidCheckpoint
	}
	commandTag, err := tx.Exec(ctx, `
		UPDATE fornix.control_checkpoints
		SET sequence=$3, updated_at=now()
		WHERE workspace_id=$1 AND consumer_id=$2 AND sequence=$4`,
		workspaceID, consumerID, int64(sequence), int64(current))
	if err != nil {
		return fmt.Errorf("advance checkpoint: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrCheckpointRegression
	}
	return nil
}

// ResetCheckpointTx is used only while rebuilding a derived projection. It
// requires the caller to reset the projection in the same transaction.
func (s *EventStore) ResetCheckpointTx(ctx context.Context, tx pgx.Tx, workspaceID, consumerID string) error {
	if _, err := s.EnsureCheckpointTx(ctx, tx, workspaceID, consumerID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE fornix.control_checkpoints
		SET sequence=0, updated_at=now()
		WHERE workspace_id=$1 AND consumer_id=$2`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(consumerID))
	if err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}
	return nil
}

// Checkpoint returns the durable cursor for one workspace consumer.
func (s *EventStore) Checkpoint(ctx context.Context, workspaceID, consumerID string) (uint64, error) {
	var sequence int64
	err := s.pool.QueryRow(ctx, `
		SELECT sequence FROM fornix.control_checkpoints
		WHERE workspace_id=$1 AND consumer_id=$2`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(consumerID)).Scan(&sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read checkpoint: %w", err)
	}
	if sequence < 0 {
		return 0, fmt.Errorf("database returned negative checkpoint")
	}
	return uint64(sequence), nil
}

func jsonString(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal event metadata: %w", err)
	}
	return string(raw), nil
}

func jsonStringOrEmpty(value any) (string, error) {
	if value == nil {
		return "{}", nil
	}
	return jsonString(value)
}

type eventScanner interface {
	Scan(dest ...any) error
}

func readEventBySequenceTx(ctx context.Context, tx pgx.Tx, workspaceID string, sequence int64) (contracts.EventEnvelope, error) {
	return scanEvent(tx.QueryRow(ctx, `
		SELECT sequence, event_id, event_type, schema_version, workspace_id,
		       scope, actor, task_ref, session_ref, causation_id, correlation_id,
		       idempotency_key, state_deltas, artifacts, provenance, payload,
		       raw_payload, occurred_at, recorded_at
		FROM fornix.control_events
		WHERE workspace_id=$1 AND sequence=$2`, workspaceID, sequence))
}

func scanEvent(row eventScanner) (contracts.EventEnvelope, error) {
	var (
		sequence                                                         int64
		eventID, eventType, workspaceID, causationID, correlationID, key string
		schemaVersion                                                    int
		scopeJSON, actorJSON, taskJSON, sessionJSON, deltasJSON          []byte
		artifactsJSON, provenanceJSON, payloadJSON, rawPayload           []byte
	)
	var occurred, recorded time.Time
	if err := row.Scan(
		&sequence, &eventID, &eventType, &schemaVersion, &workspaceID,
		&scopeJSON, &actorJSON, &taskJSON, &sessionJSON, &causationID,
		&correlationID, &key, &deltasJSON, &artifactsJSON, &provenanceJSON,
		&payloadJSON, &rawPayload, &occurred, &recorded,
	); err != nil {
		return contracts.EventEnvelope{}, err
	}
	event := contracts.EventEnvelope{
		Sequence:       uint64(sequence),
		EventID:        eventID,
		EventType:      eventType,
		SchemaVersion:  schemaVersion,
		Scope:          contracts.Scope{WorkspaceID: workspaceID},
		CausationID:    causationID,
		CorrelationID:  correlationID,
		IdempotencyKey: key,
		OccurredAt:     occurred.UTC(),
		RecordedAt:     recorded.UTC(),
		Payload:        append(json.RawMessage(nil), rawPayload...),
	}
	if len(event.Payload) == 0 {
		event.Payload = append(json.RawMessage(nil), payloadJSON...)
	}
	if err := json.Unmarshal(scopeJSON, &event.Scope); err != nil {
		return contracts.EventEnvelope{}, fmt.Errorf("scope: %w", err)
	}
	if err := json.Unmarshal(actorJSON, &event.Actor); err != nil {
		return contracts.EventEnvelope{}, fmt.Errorf("actor: %w", err)
	}
	if err := unmarshalOptional(taskJSON, &event.Task); err != nil {
		return contracts.EventEnvelope{}, fmt.Errorf("task reference: %w", err)
	}
	if err := unmarshalOptional(sessionJSON, &event.Session); err != nil {
		return contracts.EventEnvelope{}, fmt.Errorf("session reference: %w", err)
	}
	if err := json.Unmarshal(deltasJSON, &event.StateDeltas); err != nil {
		return contracts.EventEnvelope{}, fmt.Errorf("state deltas: %w", err)
	}
	if err := json.Unmarshal(artifactsJSON, &event.Artifacts); err != nil {
		return contracts.EventEnvelope{}, fmt.Errorf("artifacts: %w", err)
	}
	if err := json.Unmarshal(provenanceJSON, &event.Provenance); err != nil {
		return contracts.EventEnvelope{}, fmt.Errorf("provenance: %w", err)
	}
	return event, nil
}

func unmarshalOptional(raw []byte, target any) error {
	if len(raw) == 0 || string(raw) == "null" || string(raw) == "{}" {
		return nil
	}
	return json.Unmarshal(raw, target)
}
