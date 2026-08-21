package projection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
)

const (
	TaskProjectionName             = "task-state-v1"
	DefaultTaskConsumerID          = "projection.task_state.v1"
	TaskProjectionStatusDone       = "done"
	TaskProjectionStatusFailed     = "failed"
	TaskProjectionStatusActive     = "in_progress"
	TaskProjectionStatusClaimed    = "claimed"
	TaskProjectionStatusPending    = "pending"
	TaskProjectionStatusCancelled  = "cancelled"
	TaskProjectionStatusDeadLetter = "deadletter"
)

// TaskProjection is a derived, workspace-scoped task view. The task table
// remains the compatibility/current-state authority for existing handlers;
// this view is rebuilt only from immutable control events.
type TaskProjection struct {
	consumerID string
	batchSize  int
}

// NewTaskProjection constructs the task-state subscriber with bounded batch
// processing.
func NewTaskProjection(consumerID string, batchSize int) *TaskProjection {
	consumerID = strings.TrimSpace(consumerID)
	if consumerID == "" {
		consumerID = DefaultTaskConsumerID
	}
	return &TaskProjection{
		consumerID: consumerID,
		batchSize:  boundedBatchSize(batchSize),
	}
}

// Name returns the stable projection identity used in operator and checkpoint
// records.
func (p *TaskProjection) Name() string {
	return TaskProjectionName
}

// ConsumerID returns the durable workspace-scoped checkpoint identity.
func (p *TaskProjection) ConsumerID() string {
	return p.consumerID
}

// BatchSize returns the bounded number of events processed per transaction.
func (p *TaskProjection) BatchSize() int {
	return p.batchSize
}

// Apply accepts only the task lifecycle events this projection owns. Other
// control-plane events are acknowledged without changing the derived view so
// one subscriber can safely consume the shared event stream.
func (p *TaskProjection) Apply(ctx context.Context, tx pgx.Tx, event contracts.EventEnvelope) (ApplyResult, error) {
	if event.EventType == "task.lease_renewed" {
		if tx == nil {
			return ApplyResult{}, errors.New("task projection transaction is nil")
		}
		return ApplyResult{Handled: true}, nil
	}
	status, supported := taskEventStatus(event.EventType)
	if !supported {
		return ApplyResult{}, nil
	}
	if tx == nil {
		return ApplyResult{}, errors.New("task projection transaction is nil")
	}
	if event.Sequence == 0 {
		return ApplyResult{}, errors.New("task projection event sequence is required")
	}
	if strings.TrimSpace(event.Scope.WorkspaceID) == "" {
		return ApplyResult{}, errors.New("task projection event workspace_id is required")
	}
	if event.Task == nil || strings.TrimSpace(event.Task.ID) == "" {
		return ApplyResult{}, fmt.Errorf("%s event requires a task reference", event.EventType)
	}
	if event.Task.Kind != "" && event.Task.Kind != "task" {
		return ApplyResult{}, errors.New("task projection task reference kind must be task")
	}
	workspaceID := strings.TrimSpace(event.Scope.WorkspaceID)
	taskID := strings.TrimSpace(event.Task.ID)
	if event.Task.WorkspaceID != "" && strings.TrimSpace(event.Task.WorkspaceID) != workspaceID {
		return ApplyResult{}, fmt.Errorf("task reference workspace does not match event workspace")
	}

	current, found, err := readTaskState(ctx, tx, workspaceID, taskID, true)
	if err != nil {
		return ApplyResult{}, err
	}
	// A different consumer may redeliver an event after another consumer has
	// already moved this task past it. Sequence is the immutable ordering
	// authority, so do not re-parse or rewrite the row in that case.
	if found && current.LastEventSequence >= event.Sequence {
		return ApplyResult{Handled: true, Duplicate: true}, nil
	}

	deltaState, err := taskDeltaState(event, status, taskID)
	if err != nil {
		return ApplyResult{}, err
	}
	if !found {
		current = TaskState{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			Status:      deltaState.Status,
			Result:      deltaState.Result,
		}
	} else {
		current.Status = deltaState.Status
		if deltaState.ResultPresent {
			current.Result = deltaState.Result
		}
	}
	if deltaState.AssignedSessionPresent {
		current.AssignedSession = deltaState.AssignedSession
	} else if event.Session != nil {
		if event.Session.Kind != "" && event.Session.Kind != "session" {
			return ApplyResult{}, fmt.Errorf("task projection session reference kind must be session")
		}
		if event.Session.WorkspaceID != "" && strings.TrimSpace(event.Session.WorkspaceID) != workspaceID {
			return ApplyResult{}, fmt.Errorf("session reference workspace does not match event workspace")
		}
		current.AssignedSession = strings.TrimSpace(event.Session.ID)
	}
	current.LastEventID = event.EventID
	current.LastEventSequence = event.Sequence
	if found {
		current.AppliedEventCount++
	} else {
		current.AppliedEventCount = 1
	}
	current.StateHash = taskStateHash(current)

	if !found {
		_, err = tx.Exec(ctx, `
			INSERT INTO fornix.task_state_projections(
				workspace_id, task_id, status, result, assigned_session,
				last_event_id, last_event_sequence, applied_event_count, state_hash
			) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			current.WorkspaceID, current.TaskID, current.Status, current.Result,
			current.AssignedSession, current.LastEventID, int64(current.LastEventSequence),
			int64(current.AppliedEventCount), current.StateHash)
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE fornix.task_state_projections
			SET status=$3, result=$4, assigned_session=$5, last_event_id=$6,
				last_event_sequence=$7, applied_event_count=$8, state_hash=$9,
				updated_at=now()
			WHERE workspace_id=$1 AND task_id=$2`,
			current.WorkspaceID, current.TaskID, current.Status, current.Result,
			current.AssignedSession, current.LastEventID, int64(current.LastEventSequence),
			int64(current.AppliedEventCount), current.StateHash)
	}
	if err != nil {
		return ApplyResult{}, fmt.Errorf("write task projection: %w", err)
	}
	return ApplyResult{Handled: true, Applied: true}, nil
}

// Reset clears the derived task rows for one workspace so a rebuild can replay
// immutable events from sequence zero without touching authoritative tasks.
func (p *TaskProjection) Reset(ctx context.Context, tx pgx.Tx, workspaceID string) error {
	if tx == nil {
		return errors.New("task projection transaction is nil")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM fornix.task_state_projections WHERE workspace_id=$1`, workspaceID); err != nil {
		return fmt.Errorf("delete task projection: %w", err)
	}
	return nil
}

// TaskState is the stable read contract for the derived task view.
type TaskState struct {
	WorkspaceID       string
	TaskID            string
	Status            string
	Result            string
	AssignedSession   string
	LastEventID       string
	LastEventSequence uint64
	AppliedEventCount uint64
	StateHash         string
}

// ReadTaskStateTx reads one derived task state inside the caller's transaction
// and enforces the workspace key.
func ReadTaskStateTx(ctx context.Context, tx pgx.Tx, workspaceID, taskID string) (TaskState, bool, error) {
	workspaceID, taskID, err := validateTaskKey(workspaceID, taskID)
	if err != nil {
		return TaskState{}, false, err
	}
	return readTaskState(ctx, tx, workspaceID, taskID, false)
}

// SnapshotHashTx returns a stable hash of all derived task states in a
// workspace, useful for incremental-versus-rebuild comparisons.
func SnapshotHashTx(ctx context.Context, tx pgx.Tx, workspaceID string) (string, error) {
	if tx == nil {
		return "", errors.New("task projection transaction is nil")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return "", errors.New("workspace_id is required")
	}
	rows, err := tx.Query(ctx, `
		SELECT task_id, status, result, assigned_session, last_event_id,
		       last_event_sequence, applied_event_count, state_hash
		FROM fornix.task_state_projections
		WHERE workspace_id=$1
		ORDER BY task_id ASC`, workspaceID)
	if err != nil {
		return "", fmt.Errorf("read task projection snapshot: %w", err)
	}
	defer rows.Close()
	states := make([]TaskState, 0)
	for rows.Next() {
		var state TaskState
		var sequence, count int64
		if err := rows.Scan(&state.TaskID, &state.Status, &state.Result, &state.AssignedSession,
			&state.LastEventID, &sequence, &count, &state.StateHash); err != nil {
			return "", fmt.Errorf("scan task projection snapshot: %w", err)
		}
		if sequence <= 0 || count <= 0 {
			return "", errors.New("task projection contains invalid counters")
		}
		state.WorkspaceID = workspaceID
		state.LastEventSequence = uint64(sequence)
		state.AppliedEventCount = uint64(count)
		if state.StateHash != taskStateHash(state) {
			return "", errors.New("task projection snapshot contains a state hash mismatch")
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate task projection snapshot: %w", err)
	}
	return hashJSON(states)
}

type taskDeltaResult struct {
	Status                 string
	Result                 string
	ResultPresent          bool
	AssignedSession        string
	AssignedSessionPresent bool
}

func taskDeltaState(event contracts.EventEnvelope, expectedStatus, taskID string) (taskDeltaResult, error) {
	result := taskDeltaResult{}
	statusFound := false
	statusPath := "/tasks/" + taskID + "/status"
	resultPath := "/tasks/" + taskID + "/result"
	assignedPath := "/tasks/" + taskID + "/assigned_session"
	for _, delta := range event.StateDeltas {
		if delta.Path != statusPath && delta.Path != resultPath && delta.Path != assignedPath {
			continue
		}
		if delta.Op != contracts.DeltaSet {
			return taskDeltaResult{}, fmt.Errorf("task projection only accepts set deltas for %s", delta.Path)
		}
		if delta.Path == statusPath {
			var status string
			if err := json.Unmarshal(delta.Value, &status); err != nil || strings.TrimSpace(status) == "" {
				return taskDeltaResult{}, fmt.Errorf("task status delta must be a JSON string")
			}
			status = strings.TrimSpace(status)
			if status != expectedStatus {
				return taskDeltaResult{}, fmt.Errorf("task status delta %q does not match event type status %q", status, expectedStatus)
			}
			if statusFound && result.Status != status {
				return taskDeltaResult{}, errors.New("task event contains conflicting status deltas")
			}
			result.Status = status
			statusFound = true
			continue
		}
		if delta.Path == assignedPath {
			decoded, err := decodeTaskResult(delta.Value)
			if err != nil {
				return taskDeltaResult{}, err
			}
			result.AssignedSession = decoded
			result.AssignedSessionPresent = true
			continue
		}
		decoded, err := decodeTaskResult(delta.Value)
		if err != nil {
			return taskDeltaResult{}, err
		}
		result.Result = decoded
		result.ResultPresent = true
	}
	if !statusFound {
		return taskDeltaResult{}, fmt.Errorf("%s event requires a task status delta", event.EventType)
	}
	return result, nil
}

func decodeTaskResult(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return "", errors.New("task result delta must be valid JSON")
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("decode task result delta: %w", err)
	}
	if value == nil {
		return "", nil
	}
	if text, ok := value.(string); ok {
		return text, nil
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("canonicalize task result delta: %w", err)
	}
	return string(canonical), nil
}

func taskEventStatus(eventType string) (string, bool) {
	switch strings.TrimSpace(eventType) {
	case "task.created":
		return TaskProjectionStatusPending, true
	case "task.claimed":
		return TaskProjectionStatusClaimed, true
	case "task.retry_scheduled":
		return TaskProjectionStatusPending, true
	case "task.completed":
		return TaskProjectionStatusDone, true
	case "task.failed":
		return TaskProjectionStatusFailed, true
	case "task.deadlettered":
		return TaskProjectionStatusDeadLetter, true
	case "task.cancelled":
		return TaskProjectionStatusCancelled, true
	case "task.progressed":
		return TaskProjectionStatusActive, true
	default:
		return "", false
	}
}

func validateTaskKey(workspaceID, taskID string) (string, string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if workspaceID == "" || taskID == "" {
		return "", "", errors.New("workspace_id and task_id are required")
	}
	return workspaceID, taskID, nil
}

func readTaskState(ctx context.Context, tx pgx.Tx, workspaceID, taskID string, lock bool) (TaskState, bool, error) {
	if tx == nil {
		return TaskState{}, false, errors.New("task projection transaction is nil")
	}
	var state TaskState
	var sequence, count int64
	query := `
		SELECT status, result, assigned_session, last_event_id,
		       last_event_sequence, applied_event_count, state_hash
		FROM fornix.task_state_projections
		WHERE workspace_id=$1 AND task_id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	err := tx.QueryRow(ctx, query, workspaceID, taskID).Scan(
		&state.Status, &state.Result, &state.AssignedSession, &state.LastEventID,
		&sequence, &count, &state.StateHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return TaskState{}, false, nil
	}
	if err != nil {
		return TaskState{}, false, fmt.Errorf("read task projection: %w", err)
	}
	if sequence <= 0 || count <= 0 {
		return TaskState{}, false, errors.New("task projection contains invalid counters")
	}
	state.WorkspaceID = workspaceID
	state.TaskID = taskID
	state.LastEventSequence = uint64(sequence)
	state.AppliedEventCount = uint64(count)
	if state.StateHash != taskStateHash(state) {
		return TaskState{}, false, errors.New("task projection state hash mismatch")
	}
	return state, true, nil
}

func taskStateHash(state TaskState) string {
	input := struct {
		TaskID          string `json:"task_id"`
		Status          string `json:"status"`
		Result          string `json:"result"`
		AssignedSession string `json:"assigned_session"`
	}{
		TaskID:          state.TaskID,
		Status:          state.Status,
		Result:          state.Result,
		AssignedSession: state.AssignedSession,
	}
	hash, _ := hashJSON(input)
	return hash
}

func hashJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal task projection hash: %w", err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
