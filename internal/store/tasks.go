package store

import (
	"context"
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
	ErrTaskNotFound             = errors.New("task not found")
	ErrTaskNoReady              = errors.New("no ready task")
	ErrTaskLeaseMissing         = errors.New("task execution lease not found")
	ErrTaskLeaseHeld            = errors.New("task execution lease is held by another owner")
	ErrTaskLeaseOwned           = errors.New("task execution lease is not owned by this worker")
	ErrTaskLeaseFenced          = errors.New("task execution lease fence is stale")
	ErrTaskLeaseExpired         = errors.New("task execution lease is expired")
	ErrTaskLeaseReleased        = errors.New("task execution lease is released")
	ErrTaskNotClaimed           = errors.New("task is not claimed by this worker")
	ErrTaskTerminal             = errors.New("task is already terminal")
	ErrTaskDependencyMissing    = errors.New("task dependency is missing")
	ErrTaskDependencyCycle      = errors.New("task dependency would create a cycle")
	ErrTaskInvalidStatus        = errors.New("invalid task status")
	ErrTaskRetryBudgetExhausted = errors.New("task retry budget exhausted")
	ErrTaskSessionBusy          = errors.New("session already owns an active task")
	ErrTaskFenceExhausted       = errors.New("task execution fence is exhausted")
)

const maxTaskFence = uint64(1<<63 - 1)

// Task is the compatibility read model returned by the task API. Its source
// of truth is the task row; lifecycle events are the immutable audit/replay
// history and the task projection is derived separately.
type Task struct {
	ID                   int64      `json:"id"`
	WorkspaceID          string     `json:"workspace_id"`
	Title                string     `json:"title"`
	Brief                string     `json:"brief"`
	RequiredCapabilities []string   `json:"required_capabilities"`
	AssignedSession      *string    `json:"assigned_session,omitempty"`
	Status               string     `json:"status"`
	Result               *string    `json:"result,omitempty"`
	CreatedBy            string     `json:"created_by"`
	OriginHost           string     `json:"origin_host"`
	Attempts             int        `json:"attempts"`
	MaxAttempts          int        `json:"max_attempts"`
	NextAttemptAt        time.Time  `json:"next_attempt_at"`
	LastError            string     `json:"last_error,omitempty"`
	FailureClass         string     `json:"failure_class,omitempty"`
	Retryable            *bool      `json:"retryable,omitempty"`
	ExecutionFence       uint64     `json:"execution_fence"`
	CreatedAt            time.Time  `json:"created_at"`
	ClaimedAt            *time.Time `json:"claimed_at,omitempty"`
	CompletedAt          *time.Time `json:"completed_at,omitempty"`
	CancelledAt          *time.Time `json:"cancelled_at,omitempty"`
}

type TaskCreateInput struct {
	WorkspaceID          string
	Title                string
	Brief                string
	RequiredCapabilities []string
	CreatedBy            string
	OriginHost           string
	MaxAttempts          int
	DependsOn            []int64
	Payload              json.RawMessage
}

type TaskClaimInput struct {
	WorkspaceID string
	SessionID   string
	ActorID     string
	LeaseTTL    time.Duration
}

type TaskClaimResult struct {
	Task     Task                         `json:"task"`
	Lease    contracts.TaskExecutionLease `json:"lease"`
	Event    contracts.EventEnvelope      `json:"event"`
	Takeover bool                         `json:"takeover"`
}

type TaskOutcomeInput struct {
	WorkspaceID    string
	TaskID         int64
	OwnerID        string
	Fence          uint64
	Result         string
	Status         string
	IdempotencyKey string
	ActorID        string
	Payload        json.RawMessage
}

type TaskFailureInput struct {
	WorkspaceID    string
	TaskID         int64
	OwnerID        string
	Fence          uint64
	Error          string
	FailureClass   string
	Retryable      *bool
	RetryAfter     *time.Duration
	IdempotencyKey string
	ActorID        string
	Payload        json.RawMessage
}

type TaskCancelInput struct {
	WorkspaceID    string
	TaskID         int64
	OwnerID        string
	Fence          uint64
	Reason         string
	IdempotencyKey string
	ActorID        string
	Payload        json.RawMessage
}

type TaskMutationResult struct {
	Task           Task                          `json:"task"`
	Lease          *contracts.TaskExecutionLease `json:"lease,omitempty"`
	Event          contracts.EventEnvelope       `json:"event"`
	Deduped        bool                          `json:"deduped"`
	RetryScheduled bool                          `json:"retry_scheduled,omitempty"`
}

type TaskRenewResult struct {
	Lease contracts.TaskExecutionLease `json:"lease"`
	Event contracts.EventEnvelope      `json:"event"`
}

// TaskStore is the only mutation boundary for task execution. It intentionally
// shares EventStore's transaction and idempotency implementation.
type TaskStore struct {
	pool         *pgxpool.Pool
	events       *EventStore
	beforeCommit func() error // test-only crash boundary; nil in production
}

func NewTaskStore(pool *pgxpool.Pool, events *EventStore) *TaskStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &TaskStore{pool: pool, events: events}
}

func (s *TaskStore) Create(ctx context.Context, input TaskCreateInput) (Task, contracts.EventEnvelope, error) {
	workspaceID := normalizeWorkspace(input.WorkspaceID)
	input.Title = strings.TrimSpace(input.Title)
	input.Brief = strings.TrimSpace(input.Brief)
	input.CreatedBy = strings.TrimSpace(input.CreatedBy)
	if input.Title == "" || input.Brief == "" || input.CreatedBy == "" {
		return Task{}, contracts.EventEnvelope{}, errors.New("title, brief, and created_by are required")
	}
	maxAttempts, err := normalizeMaxAttempts(input.MaxAttempts)
	if err != nil {
		return Task{}, contracts.EventEnvelope{}, err
	}
	dependencies := normalizeTaskIDs(input.DependsOn)
	if input.Payload == nil {
		input.Payload = json.RawMessage(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Task{}, contracts.EventEnvelope{}, fmt.Errorf("begin task create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var taskID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO fornix.tasks(
			workspace_id, title, brief, required_capabilities, created_by,
			origin_host, max_attempts, next_attempt_at
		) VALUES($1,$2,$3,$4,$5,$6,$7,clock_timestamp())
		RETURNING id`, workspaceID, input.Title, input.Brief, cleanStrings(input.RequiredCapabilities),
		input.CreatedBy, strings.TrimSpace(input.OriginHost), maxAttempts).Scan(&taskID); err != nil {
		return Task{}, contracts.EventEnvelope{}, fmt.Errorf("insert task: %w", err)
	}
	for _, dependencyID := range dependencies {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS(SELECT 1 FROM fornix.tasks WHERE workspace_id=$1 AND id=$2)`, workspaceID, dependencyID).Scan(&exists); err != nil {
			return Task{}, contracts.EventEnvelope{}, fmt.Errorf("check task dependency: %w", err)
		}
		if !exists {
			return Task{}, contracts.EventEnvelope{}, fmt.Errorf("%w: %d", ErrTaskDependencyMissing, dependencyID)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.task_dependencies(workspace_id, task_id, depends_on_task_id)
			VALUES($1,$2,$3)`, workspaceID, taskID, dependencyID); err != nil {
			return Task{}, contracts.EventEnvelope{}, fmt.Errorf("insert task dependency: %w", err)
		}
	}

	payload := input.Payload
	event, err := newTaskEvent("task.created", payload, workspaceID, taskID, "", "",
		[]contracts.StateDelta{taskStatusDelta(taskID, contracts.TaskStatusPending)})
	if err != nil {
		return Task{}, contracts.EventEnvelope{}, err
	}
	event.Actor = contracts.ActorRef{ID: input.CreatedBy, Kind: "principal", WorkspaceID: workspaceID}
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return Task{}, contracts.EventEnvelope{}, fmt.Errorf("append task.created: %w", err)
	}
	task, err := readTaskTx(ctx, tx, workspaceID, taskID, false)
	if err != nil {
		return Task{}, contracts.EventEnvelope{}, err
	}
	if err := s.commit(ctx, tx); err != nil {
		return Task{}, contracts.EventEnvelope{}, fmt.Errorf("commit task create: %w", err)
	}
	return task, appended.Event, nil
}

// ClaimNext selects the oldest due task whose direct dependencies are all
// successful. SKIP LOCKED keeps unrelated workers progressing concurrently;
// the task and lease rows remain locked until the event and session update
// commit.
func (s *TaskStore) ClaimNext(ctx context.Context, input TaskClaimInput) (TaskClaimResult, error) {
	workspaceID := normalizeWorkspace(input.WorkspaceID)
	sessionID := strings.TrimSpace(input.SessionID)
	if workspaceID == "" || sessionID == "" {
		return TaskClaimResult{}, errors.New("workspace_id and session_id are required")
	}
	ttl := boundedTaskLeaseTTL(input.LeaseTTL)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TaskClaimResult{}, fmt.Errorf("begin task claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var capabilities []string
	var sessionStatus string
	var currentTask *int64
	if err := tx.QueryRow(ctx, `
		SELECT capabilities, status, current_task_id
		FROM fornix.sessions WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, workspaceID, sessionID).
		Scan(&capabilities, &sessionStatus, &currentTask); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TaskClaimResult{}, fmt.Errorf("%w: session %s", ErrTaskNotFound, sessionID)
		}
		return TaskClaimResult{}, fmt.Errorf("read task session: %w", err)
	}
	if currentTask != nil && (sessionStatus == "busy" || sessionStatus == "working") {
		return TaskClaimResult{}, ErrTaskSessionBusy
	}

	for examined := 0; examined < 100; examined++ {
		task, err := selectClaimableTaskTx(ctx, tx, workspaceID, capabilities)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if err := s.commit(ctx, tx); err != nil {
					return TaskClaimResult{}, fmt.Errorf("commit empty task claim: %w", err)
				}
				return TaskClaimResult{}, ErrTaskNoReady
			}
			return TaskClaimResult{}, err
		}
		if task.Attempts >= task.MaxAttempts {
			if err := s.transitionDeadLetterTx(ctx, tx, task, "retry budget exhausted"); err != nil {
				return TaskClaimResult{}, err
			}
			continue
		}

		oldLease, leaseExists, active, err := readTaskLeaseTx(ctx, tx, workspaceID, task.ID, true)
		if err != nil {
			return TaskClaimResult{}, err
		}
		if active {
			return TaskClaimResult{}, ErrTaskLeaseHeld
		}
		fence := uint64(1)
		takeover := false
		if leaseExists {
			if oldLease.Fence >= maxTaskFence {
				return TaskClaimResult{}, ErrTaskFenceExhausted
			}
			fence = oldLease.Fence + 1
			takeover = true
			if _, err := tx.Exec(ctx, `
				UPDATE fornix.task_execution_leases
				SET owner_id=$3, fence=$4, lease_until=clock_timestamp() + ($5::double precision * interval '1 millisecond'),
				    acquired_at=clock_timestamp(), renewed_at=clock_timestamp(), released_at=NULL
				WHERE workspace_id=$1 AND task_id=$2 AND fence=$6`,
				workspaceID, task.ID, sessionID, int64(fence), ttl.Milliseconds(), int64(oldLease.Fence)); err != nil {
				return TaskClaimResult{}, fmt.Errorf("take over task lease: %w", err)
			}
		} else {
			if _, err := tx.Exec(ctx, `
				INSERT INTO fornix.task_execution_leases(workspace_id, task_id, owner_id, fence, lease_until)
				VALUES($1,$2,$3,1,clock_timestamp() + ($4::double precision * interval '1 millisecond'))`,
				workspaceID, task.ID, sessionID, ttl.Milliseconds()); err != nil {
				return TaskClaimResult{}, fmt.Errorf("create task lease: %w", err)
			}
		}
		lease, _, active, err := readTaskLeaseTx(ctx, tx, workspaceID, task.ID, true)
		if err != nil || !active {
			if err != nil {
				return TaskClaimResult{}, err
			}
			return TaskClaimResult{}, ErrTaskLeaseExpired
		}
		if _, err := tx.Exec(ctx, `
			UPDATE fornix.tasks
			SET status=$3, assigned_session=$4, claimed_at=clock_timestamp(), attempts=attempts+1,
			    execution_fence=$5, next_attempt_at=clock_timestamp(), completed_at=NULL,
			    cancelled_at=NULL
			WHERE workspace_id=$1 AND id=$2`, workspaceID, task.ID, contracts.TaskStatusClaimed,
			sessionID, int64(fence)); err != nil {
			return TaskClaimResult{}, fmt.Errorf("update claimed task: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE fornix.sessions SET status='busy', current_task_id=$3, last_heartbeat=clock_timestamp()
			WHERE workspace_id=$1 AND id=$2`, workspaceID, sessionID, task.ID); err != nil {
			return TaskClaimResult{}, fmt.Errorf("mark task session busy: %w", err)
		}
		actorID := strings.TrimSpace(input.ActorID)
		if actorID == "" {
			actorID = sessionID
		}
		event, err := newTaskEvent("task.claimed", map[string]any{
			"task_id": task.ID, "session_id": sessionID, "fence": fence, "takeover": takeover,
		}, workspaceID, task.ID, sessionID, actorID, []contracts.StateDelta{
			taskStatusDelta(task.ID, contracts.TaskStatusClaimed),
			taskStringDelta(task.ID, "assigned_session", sessionID),
		})
		if err != nil {
			return TaskClaimResult{}, err
		}
		appended, err := s.events.AppendTx(ctx, tx, event)
		if err != nil {
			return TaskClaimResult{}, fmt.Errorf("append task.claimed: %w", err)
		}
		claimedTask, err := readTaskTx(ctx, tx, workspaceID, task.ID, false)
		if err != nil {
			return TaskClaimResult{}, err
		}
		if err := s.commit(ctx, tx); err != nil {
			return TaskClaimResult{}, fmt.Errorf("commit task claim: %w", err)
		}
		return TaskClaimResult{Task: claimedTask, Lease: lease, Event: appended.Event, Takeover: takeover}, nil
	}
	if err := s.commit(ctx, tx); err != nil {
		return TaskClaimResult{}, fmt.Errorf("commit task claim scan: %w", err)
	}
	return TaskClaimResult{}, ErrTaskNoReady
}

func (s *TaskStore) Renew(ctx context.Context, workspaceID string, taskID int64, ownerID string, fence uint64, ttl time.Duration, actorID string) (TaskRenewResult, error) {
	workspaceID = normalizeWorkspace(workspaceID)
	if err := contracts.ValidateTaskLeaseIdentity(workspaceID, ownerID, taskID); err != nil || fence == 0 {
		if err != nil {
			return TaskRenewResult{}, err
		}
		return TaskRenewResult{}, ErrTaskLeaseFenced
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TaskRenewResult{}, fmt.Errorf("begin task lease renewal: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	task, err := readTaskTx(ctx, tx, workspaceID, taskID, true)
	if err != nil {
		return TaskRenewResult{}, err
	}
	lease, err := authorizeTaskLeaseTx(ctx, tx, task, ownerID, fence)
	if err != nil {
		return TaskRenewResult{}, err
	}
	ttl = boundedTaskLeaseTTL(ttl)
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.task_execution_leases
		SET lease_until=clock_timestamp() + ($5::double precision * interval '1 millisecond'), renewed_at=clock_timestamp()
		WHERE workspace_id=$1 AND task_id=$2 AND owner_id=$3 AND fence=$4`,
		workspaceID, taskID, ownerID, int64(fence), ttl.Milliseconds()); err != nil {
		return TaskRenewResult{}, fmt.Errorf("renew task lease: %w", err)
	}
	lease, _, active, err := readTaskLeaseTx(ctx, tx, workspaceID, taskID, true)
	if err != nil {
		return TaskRenewResult{}, err
	}
	if !active {
		return TaskRenewResult{}, ErrTaskLeaseExpired
	}
	if strings.TrimSpace(actorID) == "" {
		actorID = ownerID
	}
	event, err := newTaskEvent("task.lease_renewed", map[string]any{
		"task_id": taskID, "owner_id": ownerID, "fence": fence, "lease_until": lease.LeaseUntil,
	}, workspaceID, taskID, ownerID, actorID, nil)
	if err != nil {
		return TaskRenewResult{}, err
	}
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return TaskRenewResult{}, fmt.Errorf("append task.lease_renewed: %w", err)
	}
	if err := s.commit(ctx, tx); err != nil {
		return TaskRenewResult{}, fmt.Errorf("commit task lease renewal: %w", err)
	}
	return TaskRenewResult{Lease: lease, Event: appended.Event}, nil
}

func (s *TaskStore) Complete(ctx context.Context, input TaskOutcomeInput) (TaskMutationResult, error) {
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = contracts.TaskStatusDone
	}
	if status != contracts.TaskStatusDone && status != contracts.TaskStatusInProgress {
		return TaskMutationResult{}, fmt.Errorf("%w: %s", ErrTaskInvalidStatus, status)
	}
	return s.completeOrProgress(ctx, input, status)
}

func (s *TaskStore) completeOrProgress(ctx context.Context, input TaskOutcomeInput, status string) (TaskMutationResult, error) {
	workspaceID := normalizeWorkspace(input.WorkspaceID)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TaskMutationResult{}, fmt.Errorf("begin task completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	task, err := readTaskTx(ctx, tx, workspaceID, input.TaskID, true)
	if err != nil {
		return TaskMutationResult{}, err
	}
	assignedValue := ""
	if status == contracts.TaskStatusInProgress {
		assignedValue = strings.TrimSpace(input.OwnerID)
	}
	eventType := "task.progressed"
	if status == contracts.TaskStatusDone {
		eventType = "task.completed"
	}
	event, err := outcomeEvent(eventType, input.Payload, workspaceID, task.ID, input.OwnerID, input.ActorID,
		[]contracts.StateDelta{taskStatusDelta(task.ID, status), taskStringDelta(task.ID, "result", input.Result), taskStringDelta(task.ID, "assigned_session", assignedValue)})
	if err != nil {
		return TaskMutationResult{}, err
	}
	event.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	duplicate, err := s.resolveExistingIdempotencyTx(ctx, tx, workspaceID, input.IdempotencyKey, event)
	if err != nil {
		return TaskMutationResult{}, err
	}
	if duplicate != nil {
		if err := s.commit(ctx, tx); err != nil {
			return TaskMutationResult{}, fmt.Errorf("commit duplicate task completion: %w", err)
		}
		return TaskMutationResult{Task: task, Event: *duplicate, Deduped: true}, nil
	}
	if status == contracts.TaskStatusDone && contracts.IsTaskTerminal(task.Status) {
		return TaskMutationResult{}, ErrTaskTerminal
	}
	if err := authorizeTaskMutation(ctx, tx, task, input.OwnerID, input.Fence, status == contracts.TaskStatusDone); err != nil {
		return TaskMutationResult{}, err
	}
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return TaskMutationResult{}, fmt.Errorf("append %s: %w", eventType, err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.tasks
		SET status=$3, result=$4, completed_at=CASE WHEN $3='done' THEN clock_timestamp() ELSE NULL END,
		    assigned_session=CASE WHEN $3='done' THEN NULL ELSE $5 END, next_attempt_at=clock_timestamp(),
		    last_error='', failure_class='', retryable=NULL
		WHERE workspace_id=$1 AND id=$2`, workspaceID, input.TaskID, status, input.Result, assignedValue); err != nil {
		return TaskMutationResult{}, fmt.Errorf("update completed task: %w", err)
	}
	if status == contracts.TaskStatusDone {
		if err := releaseTaskLeaseAndSessionTx(ctx, tx, task, input.OwnerID); err != nil {
			return TaskMutationResult{}, err
		}
	}
	storedEvent, err := latestEventForIdempotencyOrEvent(ctx, tx, workspaceID, input.IdempotencyKey, appended.Event)
	if err != nil {
		return TaskMutationResult{}, err
	}
	updated, err := readTaskTx(ctx, tx, workspaceID, input.TaskID, false)
	if err != nil {
		return TaskMutationResult{}, err
	}
	if err := s.commit(ctx, tx); err != nil {
		return TaskMutationResult{}, fmt.Errorf("commit task completion: %w", err)
	}
	return TaskMutationResult{Task: updated, Event: storedEvent}, nil
}

func (s *TaskStore) Fail(ctx context.Context, input TaskFailureInput) (TaskMutationResult, error) {
	workspaceID := normalizeWorkspace(input.WorkspaceID)
	class := strings.TrimSpace(input.FailureClass)
	if class == "" {
		class = contracts.FailureUnknown
	}
	if !contracts.IsFailureClass(class) {
		return TaskMutationResult{}, fmt.Errorf("unknown failure_class %q", class)
	}
	retryable := input.Retryable != nil && *input.Retryable
	if input.Retryable == nil {
		retryable = class == contracts.FailureTransient || class == contracts.FailureTimeout || class == contracts.FailureRateLimited
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TaskMutationResult{}, fmt.Errorf("begin task failure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	task, err := readTaskTx(ctx, tx, workspaceID, input.TaskID, true)
	if err != nil {
		return TaskMutationResult{}, err
	}
	willRetry := retryable && task.Attempts < task.MaxAttempts
	eventType := "task.failed"
	if willRetry {
		eventType = "task.retry_scheduled"
	} else if retryable {
		eventType = "task.deadlettered"
	}
	if existing, err := existingIdempotencyEventTx(ctx, tx, workspaceID, input.IdempotencyKey); err != nil {
		return TaskMutationResult{}, err
	} else if existing != nil {
		// Retry delivery may arrive after another attempt has advanced the
		// task. Use the original lifecycle type for request-hash comparison.
		eventType = existing.EventType
	}
	eventStatus := contracts.TaskStatusFailed
	if eventType == "task.retry_scheduled" {
		eventStatus = contracts.TaskStatusPending
	} else if eventType == "task.deadlettered" {
		eventStatus = contracts.TaskStatusDeadLetter
	}
	payload := input.Payload
	if len(payload) == 0 {
		payload, _ = json.Marshal(map[string]any{"task_id": input.TaskID, "error": input.Error, "failure_class": class, "retryable": retryable})
	}
	event, err := outcomeEvent(eventType, payload, workspaceID, input.TaskID, input.OwnerID, input.ActorID,
		[]contracts.StateDelta{taskStatusDelta(input.TaskID, eventStatus), taskStringDelta(input.TaskID, "result", input.Error), taskStringDelta(input.TaskID, "assigned_session", "")})
	if err != nil {
		return TaskMutationResult{}, err
	}
	event.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if duplicate, err := s.resolveExistingIdempotencyTx(ctx, tx, workspaceID, input.IdempotencyKey, event); err != nil {
		return TaskMutationResult{}, err
	} else if duplicate != nil {
		if err := s.commit(ctx, tx); err != nil {
			return TaskMutationResult{}, fmt.Errorf("commit duplicate task failure: %w", err)
		}
		return TaskMutationResult{Task: task, Event: *duplicate, Deduped: true, RetryScheduled: duplicate.EventType == "task.retry_scheduled"}, nil
	}
	if contracts.IsTaskTerminal(task.Status) {
		return TaskMutationResult{}, ErrTaskTerminal
	}
	if err := authorizeTaskMutation(ctx, tx, task, input.OwnerID, input.Fence, true); err != nil {
		return TaskMutationResult{}, err
	}
	status := contracts.TaskStatusFailed
	if willRetry {
		status = contracts.TaskStatusPending
	} else if retryable {
		status = contracts.TaskStatusDeadLetter
	}
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return TaskMutationResult{}, fmt.Errorf("append %s: %w", eventType, err)
	}
	delay := retryDelay(task.Attempts, input.RetryAfter)
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.tasks
		SET status=$3, result=$4, assigned_session=NULL, completed_at=CASE WHEN $3 IN ('failed','deadletter') THEN clock_timestamp() ELSE NULL END,
		    next_attempt_at=CASE WHEN $3='pending' THEN clock_timestamp() + ($5::double precision * interval '1 millisecond') ELSE clock_timestamp() END,
		    last_error=$6, failure_class=$7, retryable=$8
		WHERE workspace_id=$1 AND id=$2`, workspaceID, input.TaskID, status, input.Error, delay.Milliseconds(), input.Error, class, retryable); err != nil {
		return TaskMutationResult{}, fmt.Errorf("update failed task: %w", err)
	}
	if err := releaseTaskLeaseAndSessionTx(ctx, tx, task, input.OwnerID); err != nil {
		return TaskMutationResult{}, err
	}
	storedEvent, err := latestEventForIdempotencyOrEvent(ctx, tx, workspaceID, input.IdempotencyKey, appended.Event)
	if err != nil {
		return TaskMutationResult{}, err
	}
	updated, err := readTaskTx(ctx, tx, workspaceID, input.TaskID, false)
	if err != nil {
		return TaskMutationResult{}, err
	}
	if err := s.commit(ctx, tx); err != nil {
		return TaskMutationResult{}, fmt.Errorf("commit task failure: %w", err)
	}
	return TaskMutationResult{Task: updated, Event: storedEvent, RetryScheduled: willRetry}, nil
}

func (s *TaskStore) Cancel(ctx context.Context, input TaskCancelInput) (TaskMutationResult, error) {
	workspaceID := normalizeWorkspace(input.WorkspaceID)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TaskMutationResult{}, fmt.Errorf("begin task cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	task, err := readTaskTx(ctx, tx, workspaceID, input.TaskID, true)
	if err != nil {
		return TaskMutationResult{}, err
	}
	payload := input.Payload
	if len(payload) == 0 {
		payload, _ = json.Marshal(map[string]any{"task_id": input.TaskID, "reason": input.Reason})
	}
	event, err := outcomeEvent("task.cancelled", payload, workspaceID, input.TaskID, input.OwnerID, input.ActorID,
		[]contracts.StateDelta{taskStatusDelta(input.TaskID, contracts.TaskStatusCancelled), taskStringDelta(input.TaskID, "result", input.Reason), taskStringDelta(input.TaskID, "assigned_session", "")})
	if err != nil {
		return TaskMutationResult{}, err
	}
	event.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if duplicate, err := s.resolveExistingIdempotencyTx(ctx, tx, workspaceID, input.IdempotencyKey, event); err != nil {
		return TaskMutationResult{}, err
	} else if duplicate != nil {
		if err := s.commit(ctx, tx); err != nil {
			return TaskMutationResult{}, fmt.Errorf("commit duplicate task cancellation: %w", err)
		}
		return TaskMutationResult{Task: task, Event: *duplicate, Deduped: true}, nil
	}
	if contracts.IsTaskTerminal(task.Status) {
		return TaskMutationResult{}, ErrTaskTerminal
	}
	if err := authorizeTaskMutation(ctx, tx, task, input.OwnerID, input.Fence, true); err != nil {
		return TaskMutationResult{}, err
	}
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return TaskMutationResult{}, fmt.Errorf("append task.cancelled: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.tasks SET status='cancelled', result=$3, assigned_session=NULL,
			completed_at=clock_timestamp(), cancelled_at=clock_timestamp(), next_attempt_at=clock_timestamp()
		WHERE workspace_id=$1 AND id=$2`, workspaceID, input.TaskID, input.Reason); err != nil {
		return TaskMutationResult{}, fmt.Errorf("update cancelled task: %w", err)
	}
	if err := releaseTaskLeaseAndSessionTx(ctx, tx, task, input.OwnerID); err != nil {
		return TaskMutationResult{}, err
	}
	storedEvent, err := latestEventForIdempotencyOrEvent(ctx, tx, workspaceID, input.IdempotencyKey, appended.Event)
	if err != nil {
		return TaskMutationResult{}, err
	}
	updated, err := readTaskTx(ctx, tx, workspaceID, input.TaskID, false)
	if err != nil {
		return TaskMutationResult{}, err
	}
	if err := s.commit(ctx, tx); err != nil {
		return TaskMutationResult{}, fmt.Errorf("commit task cancellation: %w", err)
	}
	return TaskMutationResult{Task: updated, Event: storedEvent}, nil
}

func (s *TaskStore) Get(ctx context.Context, workspaceID string, taskID int64) (Task, error) {
	task, err := readTask(ctx, s.pool, normalizeWorkspace(workspaceID), taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, ErrTaskNotFound
	}
	return task, err
}

func (s *TaskStore) List(ctx context.Context, workspaceID, status, assigned, since string, limit int) ([]Task, error) {
	workspaceID = normalizeWorkspace(workspaceID)
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	args := []any{workspaceID}
	query := taskSelectSQL + ` WHERE t.workspace_id=$1`
	if strings.TrimSpace(status) != "" {
		args = append(args, strings.TrimSpace(status))
		query += fmt.Sprintf(" AND t.status=$%d", len(args))
	}
	if strings.TrimSpace(assigned) != "" {
		args = append(args, strings.TrimSpace(assigned))
		query += fmt.Sprintf(" AND t.assigned_session=$%d", len(args))
	}
	if strings.TrimSpace(since) != "" {
		args = append(args, since)
		query += fmt.Sprintf(" AND t.created_at>$%d", len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY t.created_at DESC, t.id DESC LIMIT $%d", len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()
	result := make([]Task, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		result = append(result, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tasks: %w", err)
	}
	return result, nil
}

func (s *TaskStore) commit(ctx context.Context, tx pgx.Tx) error {
	if s.beforeCommit != nil {
		if err := s.beforeCommit(); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

const taskSelectSQL = `SELECT t.id, t.workspace_id, t.title, t.brief, t.required_capabilities,
	t.assigned_session, t.status, t.result, t.created_by, t.origin_host, t.attempts,
	t.max_attempts, t.next_attempt_at, t.last_error, t.failure_class, t.retryable,
	t.execution_fence, t.created_at, t.claimed_at, t.completed_at, t.cancelled_at
	FROM fornix.tasks t`

func selectClaimableTaskTx(ctx context.Context, tx pgx.Tx, workspaceID string, capabilities []string) (Task, error) {
	query := taskSelectSQL + `
	WHERE t.workspace_id=$1
	  AND (t.status='pending' AND t.next_attempt_at <= clock_timestamp()
	       OR t.status IN ('claimed','in_progress') AND EXISTS(
	          SELECT 1 FROM fornix.task_execution_leases l
	          WHERE l.workspace_id=t.workspace_id AND l.task_id=t.id
	            AND l.released_at IS NULL AND l.lease_until <= clock_timestamp()))
	  AND (t.required_capabilities='{}'::text[] OR t.required_capabilities <@ $2)
	  AND NOT EXISTS(
		SELECT 1 FROM fornix.task_dependencies d
		JOIN fornix.tasks dep ON dep.workspace_id=d.workspace_id AND dep.id=d.depends_on_task_id
		WHERE d.workspace_id=t.workspace_id AND d.task_id=t.id AND dep.status <> 'done')
	ORDER BY t.created_at ASC, t.id ASC
	FOR UPDATE OF t SKIP LOCKED LIMIT 1`
	rows, err := tx.Query(ctx, query, workspaceID, capabilities)
	if err != nil {
		return Task{}, fmt.Errorf("select claimable task: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Task{}, fmt.Errorf("iterate claimable task: %w", err)
		}
		return Task{}, pgx.ErrNoRows
	}
	return scanTask(rows)
}

func readTask(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID string, taskID int64) (Task, error) {
	return scanTaskRow(queryer.QueryRow(ctx, taskSelectSQL+` WHERE t.workspace_id=$1 AND t.id=$2`, workspaceID, taskID))
}

func readTaskTx(ctx context.Context, tx pgx.Tx, workspaceID string, taskID int64, lock bool) (Task, error) {
	query := taskSelectSQL + ` WHERE t.workspace_id=$1 AND t.id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	return scanTaskRow(tx.QueryRow(ctx, query, workspaceID, taskID))
}

type rowScanner interface{ Scan(...any) error }

func scanTask(rows pgx.Rows) (Task, error) { return scanTaskRow(rows) }

func scanTaskRow(row rowScanner) (Task, error) {
	var task Task
	var fence int64
	if err := row.Scan(&task.ID, &task.WorkspaceID, &task.Title, &task.Brief, &task.RequiredCapabilities,
		&task.AssignedSession, &task.Status, &task.Result, &task.CreatedBy, &task.OriginHost,
		&task.Attempts, &task.MaxAttempts, &task.NextAttemptAt, &task.LastError, &task.FailureClass,
		&task.Retryable, &fence, &task.CreatedAt, &task.ClaimedAt, &task.CompletedAt, &task.CancelledAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Task{}, ErrTaskNotFound
		}
		return Task{}, err
	}
	if fence < 0 {
		return Task{}, errors.New("task execution fence is negative")
	}
	task.ExecutionFence = uint64(fence)
	return task, nil
}

func readTaskLeaseTx(ctx context.Context, tx pgx.Tx, workspaceID string, taskID int64, lock bool) (contracts.TaskExecutionLease, bool, bool, error) {
	query := `SELECT workspace_id, task_id, owner_id, fence, lease_until, acquired_at, renewed_at, released_at,
		(released_at IS NULL AND lease_until > clock_timestamp()) AS active
		FROM fornix.task_execution_leases WHERE workspace_id=$1 AND task_id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	var lease contracts.TaskExecutionLease
	var fence int64
	var active bool
	if err := tx.QueryRow(ctx, query, workspaceID, taskID).Scan(&lease.WorkspaceID, &lease.TaskID, &lease.OwnerID,
		&fence, &lease.LeaseUntil, &lease.AcquiredAt, &lease.RenewedAt, &lease.ReleasedAt, &active); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.TaskExecutionLease{}, false, false, nil
		}
		return contracts.TaskExecutionLease{}, false, false, fmt.Errorf("read task lease: %w", err)
	}
	if fence <= 0 {
		return contracts.TaskExecutionLease{}, false, false, errors.New("task execution lease fence is invalid")
	}
	lease.Fence = uint64(fence)
	return lease, true, active, nil
}

func authorizeTaskLeaseTx(ctx context.Context, tx pgx.Tx, task Task, ownerID string, fence uint64) (contracts.TaskExecutionLease, error) {
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" || fence == 0 {
		return contracts.TaskExecutionLease{}, ErrTaskNotClaimed
	}
	lease, exists, active, err := readTaskLeaseTx(ctx, tx, task.WorkspaceID, task.ID, true)
	if err != nil {
		return contracts.TaskExecutionLease{}, err
	}
	if !exists {
		return contracts.TaskExecutionLease{}, ErrTaskLeaseMissing
	}
	if lease.Fence != fence {
		return lease, fmt.Errorf("%w: expected=%d current=%d", ErrTaskLeaseFenced, fence, lease.Fence)
	}
	if lease.OwnerID != ownerID {
		return lease, ErrTaskLeaseOwned
	}
	if task.AssignedSession == nil || strings.TrimSpace(*task.AssignedSession) != ownerID {
		return lease, ErrTaskLeaseOwned
	}
	if lease.ReleasedAt != nil {
		return lease, ErrTaskLeaseReleased
	}
	if !active {
		return lease, ErrTaskLeaseExpired
	}
	return lease, nil
}

func authorizeTaskMutation(ctx context.Context, tx pgx.Tx, task Task, ownerID string, fence uint64, allowUnclaimed bool) error {
	if allowUnclaimed && task.AssignedSession == nil && task.Status == contracts.TaskStatusPending && strings.TrimSpace(ownerID) == "" && fence == 0 {
		return nil
	}
	_, err := authorizeTaskLeaseTx(ctx, tx, task, ownerID, fence)
	return err
}

func releaseTaskLeaseAndSessionTx(ctx context.Context, tx pgx.Tx, task Task, ownerID string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.task_execution_leases
		SET lease_until=clock_timestamp(), released_at=clock_timestamp(), renewed_at=clock_timestamp()
		WHERE workspace_id=$1 AND task_id=$2 AND owner_id=$3 AND fence=$4 AND released_at IS NULL`,
		task.WorkspaceID, task.ID, strings.TrimSpace(ownerID), int64(task.ExecutionFence)); err != nil {
		return fmt.Errorf("release task lease: %w", err)
	}
	if task.AssignedSession != nil {
		if _, err := tx.Exec(ctx, `
			UPDATE fornix.sessions SET status='idle', current_task_id=NULL, last_heartbeat=clock_timestamp()
			WHERE workspace_id=$1 AND id=$2 AND current_task_id=$3`, task.WorkspaceID, *task.AssignedSession, task.ID); err != nil {
			return fmt.Errorf("idle task session: %w", err)
		}
	}
	return nil
}

func (s *TaskStore) transitionDeadLetterTx(ctx context.Context, tx pgx.Tx, task Task, reason string) error {
	event, err := outcomeEvent("task.deadlettered", map[string]any{"task_id": task.ID, "error": reason},
		task.WorkspaceID, task.ID, deref(task.AssignedSession), "system",
		[]contracts.StateDelta{taskStatusDelta(task.ID, contracts.TaskStatusDeadLetter), taskStringDelta(task.ID, "result", reason), taskStringDelta(task.ID, "assigned_session", "")})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.tasks SET status='deadletter', result=$3, completed_at=clock_timestamp(), assigned_session=NULL, next_attempt_at=clock_timestamp(), last_error=$3, failure_class='permanent', retryable=false WHERE workspace_id=$1 AND id=$2`, task.WorkspaceID, task.ID, reason); err != nil {
		return fmt.Errorf("dead-letter task: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.task_execution_leases SET lease_until=clock_timestamp(), released_at=clock_timestamp(), renewed_at=clock_timestamp() WHERE workspace_id=$1 AND task_id=$2 AND released_at IS NULL`, task.WorkspaceID, task.ID); err != nil {
		return fmt.Errorf("release dead-letter task lease: %w", err)
	}
	if task.AssignedSession != nil {
		if _, err := tx.Exec(ctx, `UPDATE fornix.sessions SET status='idle', current_task_id=NULL, last_heartbeat=clock_timestamp() WHERE workspace_id=$1 AND id=$2 AND current_task_id=$3`, task.WorkspaceID, *task.AssignedSession, task.ID); err != nil {
			return fmt.Errorf("idle dead-letter task session: %w", err)
		}
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return fmt.Errorf("append dead-letter event: %w", err)
	}
	return nil
}

func (s *TaskStore) resolveExistingIdempotencyTx(ctx context.Context, tx pgx.Tx, workspaceID, key string, candidate contracts.EventEnvelope) (*contracts.EventEnvelope, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, nil
	}
	var requestHash, eventID string
	var sequence *int64
	err := tx.QueryRow(ctx, `SELECT request_hash, event_id, event_sequence FROM fornix.idempotency_records WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, workspaceID, key).Scan(&requestHash, &eventID, &sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read task idempotency: %w", err)
	}
	if sequence == nil || *sequence <= 0 {
		return nil, ErrIncompleteIdempotency
	}
	hash, err := contracts.RequestHash(candidate)
	if err != nil {
		return nil, err
	}
	if hash != requestHash {
		return nil, fmt.Errorf("%w: %s", ErrIdempotencyConflict, key)
	}
	event, err := readEventBySequenceTx(ctx, tx, workspaceID, *sequence)
	if err != nil {
		return nil, fmt.Errorf("read duplicate task event %s: %w", eventID, err)
	}
	return &event, nil
}

func existingIdempotencyEventTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (*contracts.EventEnvelope, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, nil
	}
	var sequence *int64
	if err := tx.QueryRow(ctx, `SELECT event_sequence FROM fornix.idempotency_records WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, workspaceID, key).Scan(&sequence); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("read existing task idempotency: %w", err)
	} else if sequence == nil || *sequence <= 0 {
		return nil, ErrIncompleteIdempotency
	} else {
		event, err := readEventBySequenceTx(ctx, tx, workspaceID, *sequence)
		if err != nil {
			return nil, err
		}
		return &event, nil
	}
}

func latestEventForIdempotencyOrEvent(ctx context.Context, tx pgx.Tx, workspaceID, key string, event contracts.EventEnvelope) (contracts.EventEnvelope, error) {
	if strings.TrimSpace(key) != "" {
		var sequence *int64
		if err := tx.QueryRow(ctx, `SELECT event_sequence FROM fornix.idempotency_records WHERE workspace_id=$1 AND idempotency_key=$2`, workspaceID, key).Scan(&sequence); err != nil {
			return contracts.EventEnvelope{}, fmt.Errorf("read committed task event: %w", err)
		}
		if sequence != nil && *sequence > 0 {
			return readEventBySequenceTx(ctx, tx, workspaceID, *sequence)
		}
	}
	return event, nil
}

func outcomeEvent(eventType string, payload any, workspaceID string, taskID int64, ownerID, actorID string, deltas []contracts.StateDelta) (contracts.EventEnvelope, error) {
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	event, err := newTaskEvent(eventType, payload, workspaceID, taskID, ownerID, actorID, deltas)
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	return event, nil
}

func newTaskEvent(eventType string, payload any, workspaceID string, taskID int64, sessionID, actorID string, deltas []contracts.StateDelta) (contracts.EventEnvelope, error) {
	event, err := contracts.NewEvent(eventType, payload)
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	event.Scope.WorkspaceID = normalizeWorkspace(workspaceID)
	event.Task = &contracts.EntityRef{ID: fmt.Sprintf("%d", taskID), Kind: "task", WorkspaceID: event.Scope.WorkspaceID}
	if strings.TrimSpace(sessionID) != "" {
		event.Session = &contracts.EntityRef{ID: strings.TrimSpace(sessionID), Kind: "session", WorkspaceID: event.Scope.WorkspaceID}
	}
	if strings.TrimSpace(actorID) != "" {
		kind := "principal"
		if strings.TrimSpace(actorID) == strings.TrimSpace(sessionID) {
			kind = "session"
		}
		event.Actor = contracts.ActorRef{ID: strings.TrimSpace(actorID), Kind: kind, WorkspaceID: workspaceID}
	}
	event.StateDeltas = deltas
	return event, nil
}

func taskStatusDelta(taskID int64, status string) contracts.StateDelta {
	value, _ := json.Marshal(status)
	return contracts.StateDelta{Op: contracts.DeltaSet, Path: fmt.Sprintf("/tasks/%d/status", taskID), Value: value}
}

func taskStringDelta(taskID int64, field, value string) contracts.StateDelta {
	encoded, _ := json.Marshal(value)
	return contracts.StateDelta{Op: contracts.DeltaSet, Path: fmt.Sprintf("/tasks/%d/%s", taskID, field), Value: encoded}
}

func normalizeWorkspace(workspaceID string) string {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return contracts.DefaultWorkspaceID
	}
	return workspaceID
}

func normalizeMaxAttempts(value int) (int, error) {
	if value == 0 {
		return contracts.DefaultTaskAttempts, nil
	}
	if value < 1 || value > contracts.MaxTaskAttempts {
		return 0, fmt.Errorf("max_attempts must be between 1 and %d", contracts.MaxTaskAttempts)
	}
	return value, nil
}

func boundedTaskLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return contracts.DefaultTaskLeaseTTL
	}
	if ttl > contracts.MaxTaskLeaseTTL {
		return contracts.MaxTaskLeaseTTL
	}
	return ttl
}

func retryDelay(attempt int, requested *time.Duration) time.Duration {
	if requested != nil {
		if *requested < 0 {
			return 0
		}
		if *requested > contracts.MaxTaskRetryBackoff {
			return contracts.MaxTaskRetryBackoff
		}
		return *requested
	}
	if attempt < 1 {
		attempt = 1
	}
	delay := 100 * time.Millisecond
	for i := 1; i < attempt && delay < contracts.MaxTaskRetryBackoff; i++ {
		delay *= 2
	}
	if delay > contracts.MaxTaskRetryBackoff {
		return contracts.MaxTaskRetryBackoff
	}
	return delay
}

func normalizeTaskIDs(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	result := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func cleanStrings(values []string) []string {
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

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
