package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
)

var (
	ErrAgentRunMissing   = errors.New("agent run not found")
	ErrAgentRunConflict  = errors.New("agent run state version conflict")
	ErrAgentRunTerminal  = errors.New("agent run is already terminal")
	ErrAgentRunCancelled = errors.New("agent run is cancelled")
	ErrAgentRunStale     = errors.New("agent run task fence is stale")
)

// AgentRunStore is the Postgres checkpoint boundary for the bounded agent
// loop. The runtime constructs transitions; this store commits one transition
// and one typed event atomically.
type AgentRunStore struct {
	pool          *pgxpool.Pool
	events        *EventStore
	artifacts     *ArtifactStore
	observability *ObservabilityStore
	beforeCommit  func() error
}

func NewAgentRunStore(pool *pgxpool.Pool, events *EventStore) *AgentRunStore {
	return &AgentRunStore{pool: pool, events: events, artifacts: NewArtifactStore(pool)}
}

func (s *AgentRunStore) SetObservability(observer *ObservabilityStore) {
	if s != nil {
		s.observability = observer
	}
}

// SetArtifactFailureHook provides a deterministic rollback seam for agent
// output-link tests. Production callers should leave it unset.
func (s *AgentRunStore) SetArtifactFailureHook(hook func(string) error) {
	if s != nil && s.artifacts != nil {
		s.artifacts.SetFailureHook(hook)
	}
}

func (s *AgentRunStore) Reserve(ctx context.Context, request contracts.AgentRunRequest) (contracts.AgentRun, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.AgentRun{}, false, fmt.Errorf("agent run store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.AgentRun{}, false, err
	}
	requestHash, err := request.RequestHash()
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	taskJSON, err := jsonOrEmpty(request.Task)
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	sessionJSON, err := jsonOrEmpty(request.Session)
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	providerJSON, err := json.Marshal(request.Provider)
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	toolsJSON, err := json.Marshal(request.Tools)
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	budgetJSON, err := json.Marshal(request.Budget)
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	retrievalJSON, err := jsonOrEmpty(request.Retrieval)
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	historyJSON, err := json.Marshal([]contracts.ModelMessage{{Role: "user", Content: request.Goal}})
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	now := time.Now().UTC()
	run := contracts.AgentRun{
		ID: request.RunID, WorkspaceID: request.WorkspaceID, RequestID: request.RequestID,
		IdempotencyKey: request.IdempotencyKey, RequestHash: requestHash, SchemaVersion: request.SchemaVersion,
		CausationID: request.CausationID, CorrelationID: request.CorrelationID, Actor: request.Actor,
		Task: cloneEntityRefForRun(request.Task), Session: cloneEntityRefForRun(request.Session),
		TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Goal: request.Goal,
		Provider: request.Provider, Tools: append([]contracts.ModelToolDefinition(nil), request.Tools...), Retrieval: cloneRetrievalRequest(request.Retrieval), Budget: request.Budget,
		State: contracts.AgentRunPending, Phase: contracts.AgentPhaseModel, History: []contracts.ModelMessage{{Role: "user", Content: request.Goal}},
		StateVersion: 1, CreatedAt: now, UpdatedAt: now,
	}
	run.StateHash = run.ComputeStateHash()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.AgentRun{}, false, fmt.Errorf("begin agent run reserve: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.agent_runs(
			id, workspace_id, request_id, idempotency_key, request_hash, schema_version,
			causation_id, correlation_id, actor, task_ref, session_ref, task_owner_id,
			task_fence, goal, provider, tools, budget, retrieval_request, state, phase, history, state_hash
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb,$12,$13,$14,$15::jsonb,$16::jsonb,$17::jsonb,$18::jsonb,$19,$20,$21::jsonb,$22)
		ON CONFLICT (workspace_id, idempotency_key) DO NOTHING`,
		run.ID, run.WorkspaceID, run.RequestID, run.IdempotencyKey, run.RequestHash, run.SchemaVersion,
		run.CausationID, run.CorrelationID, actorJSON, taskJSON, sessionJSON, run.TaskOwnerID, int64(run.TaskFence), run.Goal,
		providerJSON, toolsJSON, budgetJSON, retrievalJSON, run.State, run.Phase, historyJSON, run.StateHash)
	if err != nil {
		return contracts.AgentRun{}, false, fmt.Errorf("reserve agent run: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		existing, readErr := readAgentRunByIdempotencyTx(ctx, tx, request.WorkspaceID, request.IdempotencyKey, false)
		if readErr != nil {
			return contracts.AgentRun{}, false, readErr
		}
		if existing.RequestHash != requestHash {
			return contracts.AgentRun{}, false, fmt.Errorf("%w: %s", ErrIdempotencyConflict, request.IdempotencyKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.AgentRun{}, false, fmt.Errorf("commit duplicate agent run read: %w", err)
		}
		return existing, true, nil
	}
	event, err := agentEvent(contracts.AgentEventCreated, run, map[string]any{"run_id": run.ID, "goal": run.Goal, "provider": run.Provider, "state": run.State})
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return contracts.AgentRun{}, false, fmt.Errorf("append agent.created: %w", err)
	}
	run.EventSequence = appended.Event.Sequence
	if _, err := tx.Exec(ctx, `UPDATE fornix.agent_runs SET event_sequence=$3, updated_at=now() WHERE workspace_id=$1 AND id=$2`, run.WorkspaceID, run.ID, int64(run.EventSequence)); err != nil {
		return contracts.AgentRun{}, false, fmt.Errorf("record agent creation sequence: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.AgentRun{}, false, fmt.Errorf("commit agent run reserve: %w", err)
	}
	return run, false, nil
}

func (s *AgentRunStore) Get(ctx context.Context, workspaceID, runID string) (contracts.AgentRun, error) {
	if s == nil || s.pool == nil {
		return contracts.AgentRun{}, fmt.Errorf("agent run store is not configured")
	}
	run, err := readAgentRun(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(runID), false)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.AgentRun{}, ErrAgentRunMissing
	}
	return run, err
}

func (s *AgentRunStore) Commit(ctx context.Context, current, next contracts.AgentRun, eventType string, payload any) (contracts.AgentRun, error) {
	return s.commit(ctx, current, next, eventType, payload, nil)
}

// CommitOwned is the scheduler-owned checkpoint boundary. It validates and
// locks the run lease, renews that same lease, appends the typed event, and
// updates the run checkpoint in one transaction. A stale or expired worker
// therefore cannot advance the run or keep its ownership alive.
func (s *AgentRunStore) CommitOwned(ctx context.Context, current, next contracts.AgentRun, eventType string, payload any, lease contracts.AgentRunLease) (contracts.AgentRun, error) {
	return s.commit(ctx, current, next, eventType, payload, &lease)
}

func (s *AgentRunStore) commit(ctx context.Context, current, next contracts.AgentRun, eventType string, payload any, ownedLease *contracts.AgentRunLease) (contracts.AgentRun, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.AgentRun{}, fmt.Errorf("agent run store is not configured")
	}
	if strings.TrimSpace(current.ID) == "" || strings.TrimSpace(current.WorkspaceID) == "" {
		return contracts.AgentRun{}, fmt.Errorf("current agent run identity is required")
	}
	if strings.TrimSpace(next.ID) != current.ID || strings.TrimSpace(next.WorkspaceID) != current.WorkspaceID {
		return contracts.AgentRun{}, fmt.Errorf("agent run transition crosses identity boundary")
	}
	if ownedLease != nil {
		if err := contracts.ValidateAgentRunLeaseIdentity(ownedLease.WorkspaceID, ownedLease.RunID, ownedLease.OwnerID); err != nil {
			return contracts.AgentRun{}, err
		}
		if ownedLease.WorkspaceID != current.WorkspaceID || ownedLease.RunID != current.ID {
			return contracts.AgentRun{}, ErrAgentRunLeaseFenced
		}
	}
	if contracts.IsAgentTerminal(current.State) && current.State != next.State {
		return contracts.AgentRun{}, ErrAgentRunTerminal
	}
	if err := s.ValidateTaskFence(ctx, current); err != nil {
		return contracts.AgentRun{}, err
	}
	if err := normalizeRunForCommit(&next); err != nil {
		return contracts.AgentRun{}, err
	}
	// The durable representation is always the redacted representation. This
	// keeps the state hash reproducible when a large history/output is hydrated
	// back from its artifact.
	if err := redactAgentOutput(&next); err != nil {
		return contracts.AgentRun{}, err
	}
	if current.LastOutput == next.LastOutput {
		next.LastOutputArtifact = current.LastOutputArtifact
	} else {
		next.LastOutputArtifact = nil
	}
	if sameAgentHistory(current.History, next.History) {
		next.HistoryArtifact = current.HistoryArtifact
	} else {
		next.HistoryArtifact = nil
	}
	next.StateVersion = current.StateVersion + 1
	next.EventSequence = current.EventSequence
	next.StateHash = next.ComputeStateHash()
	now := time.Now().UTC()
	next.UpdatedAt = now
	if next.StartedAt == nil && next.State != contracts.AgentRunPending {
		started := now
		next.StartedAt = &started
	}
	if contracts.IsAgentTerminal(next.State) {
		finished := now
		next.FinishedAt = &finished
	}
	actorJSON, err := json.Marshal(next.Actor)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	taskJSON, err := jsonOrEmpty(next.Task)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	sessionJSON, err := jsonOrEmpty(next.Session)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	providerJSON, err := json.Marshal(next.Provider)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	toolsJSON, err := json.Marshal(next.Tools)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	budgetJSON, err := json.Marshal(next.Budget)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	retrievalJSON, err := jsonOrEmpty(next.Retrieval)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	costJSON, err := json.Marshal(next.Cost)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	historyJSON, err := json.Marshal(next.History)
	if err != nil {
		return contracts.AgentRun{}, fmt.Errorf("agent history exceeds durable bound")
	}
	pendingJSON, err := json.Marshal(next.PendingTools)
	if err != nil || len(pendingJSON) > contracts.MaxAgentHistoryBytes {
		return contracts.AgentRun{}, fmt.Errorf("agent pending tools exceed durable bound")
	}
	var failureJSON []byte
	if next.LastFailure != nil {
		failureJSON, err = json.Marshal(next.LastFailure)
		if err != nil {
			return contracts.AgentRun{}, err
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.AgentRun{}, fmt.Errorf("begin agent run commit: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	locked, err := readAgentRunTx(ctx, tx, current.WorkspaceID, current.ID, true)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	if locked.StateVersion != current.StateVersion {
		return contracts.AgentRun{}, ErrAgentRunConflict
	}
	if locked.TaskOwnerID != next.TaskOwnerID || locked.TaskFence != next.TaskFence || !sameEntityRef(locked.Task, next.Task) {
		return contracts.AgentRun{}, ErrAgentRunStale
	}
	if err := validateRunTaskFenceTx(ctx, tx, locked); err != nil {
		return contracts.AgentRun{}, err
	}
	inlineHistory, inlineLastOutput, artifactIDs, err := s.artifactizeAgentOutputsTx(ctx, tx, next, historyJSON)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	// Return the same durable references that are written with the checkpoint.
	// This keeps the commit result observationally equivalent to a subsequent
	// Get, including on the first oversized-output transition.
	if id, ok := artifactIDs.lastOutput.(int64); ok {
		ref, refErr := readArtifactRefByArtifactID(ctx, tx, next.WorkspaceID, id, "agent_run", "last_output")
		if refErr != nil {
			return contracts.AgentRun{}, refErr
		}
		next.LastOutputArtifact = &ref
	}
	if id, ok := artifactIDs.history.(int64); ok {
		ref, refErr := readArtifactRefByArtifactID(ctx, tx, next.WorkspaceID, id, "agent_run", "history")
		if refErr != nil {
			return contracts.AgentRun{}, refErr
		}
		next.HistoryArtifact = &ref
	}
	if ownedLease != nil {
		if _, err := validateAgentRunLeaseTx(ctx, tx, *ownedLease); err != nil {
			return contracts.AgentRun{}, err
		}
		ttl := time.Duration(ownedLease.LeaseTTLMS) * time.Millisecond
		ttl = contracts.NormalizeAgentRunLeaseTTL(ttl)
		if _, err := tx.Exec(ctx, `
			UPDATE fornix.agent_run_worker_leases
			SET lease_until=clock_timestamp() + ($5::double precision * interval '1 millisecond'),
			    renewed_at=clock_timestamp(), updated_at=clock_timestamp()
			WHERE workspace_id=$1 AND run_id=$2 AND owner_id=$3 AND fence=$4`,
			ownedLease.WorkspaceID, ownedLease.RunID, ownedLease.OwnerID, int64(ownedLease.Fence), ttl.Milliseconds()); err != nil {
			return contracts.AgentRun{}, fmt.Errorf("renew agent run lease with checkpoint: %w", err)
		}
	}
	event, err := agentEvent(eventType, next, payload)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return contracts.AgentRun{}, fmt.Errorf("append agent transition: %w", err)
	}
	next.EventSequence = appended.Event.Sequence
	commandTag, err := tx.Exec(ctx, `
		UPDATE fornix.agent_runs SET
			actor=$3::jsonb, task_ref=$4::jsonb, session_ref=$5::jsonb, task_owner_id=$6, task_fence=$7,
			provider=$8::jsonb, tools=$9::jsonb, budget=$10::jsonb, retrieval_request=$11::jsonb, context_hash=$12, state=$13, phase=$14, turn=$15, step=$16,
			model_attempts=$17, model_calls=$18, tool_calls=$19, input_tokens=$20, output_tokens=$21, total_tokens=$22,
			context_bytes=$23, cost=$24::jsonb, history=$25::jsonb, pending_tools=$26::jsonb, last_output=$27,
			last_output_artifact_id=$28, history_artifact_id=$29, last_failure=$30::jsonb, termination=$31, next_retry_at=$32, state_version=$33, event_sequence=$34,
			state_hash=$35, updated_at=$36, started_at=$37, finished_at=$38
		WHERE workspace_id=$1 AND id=$2 AND state_version=$39`,
		next.WorkspaceID, next.ID, actorJSON, taskJSON, sessionJSON, next.TaskOwnerID, int64(next.TaskFence), providerJSON,
		toolsJSON, budgetJSON, retrievalJSON, next.ContextHash, next.State, next.Phase, next.Turn, next.Step, next.ModelAttempts, next.ModelCalls, next.ToolCalls,
		next.InputTokens, next.OutputTokens, next.TotalTokens, next.ContextBytes, costJSON, inlineHistory, pendingJSON, inlineLastOutput,
		artifactIDs.lastOutput, artifactIDs.history, nullJSON(failureJSON), next.Termination, next.NextRetryAt, next.StateVersion, int64(next.EventSequence), next.StateHash,
		next.UpdatedAt, next.StartedAt, next.FinishedAt, current.StateVersion)
	if err != nil {
		return contracts.AgentRun{}, fmt.Errorf("update agent run checkpoint: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return contracts.AgentRun{}, ErrAgentRunConflict
	}
	if s.observability != nil {
		if err := s.observability.recordObservationTx(ctx, tx, contracts.RunObservation{WorkspaceID: next.WorkspaceID, IdempotencyKey: fmt.Sprintf("agent-transition:%s:%d", next.ID, next.StateVersion), Kind: contracts.ObservationAgent, Component: "agent_loop", Operation: "transition", Outcome: next.State, Actor: next.Actor, Task: next.Task, Session: next.Session, CausationID: next.CausationID, CorrelationID: next.CorrelationID, SourceKind: "agent_run", SourceID: next.ID, StartedAt: next.UpdatedAt, FinishedAt: next.UpdatedAt, InputBytes: int64(next.ContextBytes), InputTokens: next.InputTokens, OutputTokens: next.OutputTokens, TotalTokens: next.TotalTokens, CostUSD: next.Cost.TotalCostUSD, CostKnown: strings.TrimSpace(next.Cost.Source) != "" || next.Cost.TotalCostUSD > 0}); err != nil {
			return contracts.AgentRun{}, err
		}
		if next.State == contracts.AgentRunAwaitingRetry || next.State == contracts.AgentRunAwaitingApproval {
			kind, operation := contracts.ObservationRetry, "schedule"
			if next.State == contracts.AgentRunAwaitingApproval {
				kind, operation = contracts.ObservationApproval, "wait"
			}
			if next.State == contracts.AgentRunAwaitingRetry {
				if err := s.observability.recordCostTx(ctx, tx, contracts.CostLedgerEntry{WorkspaceID: next.WorkspaceID, IdempotencyKey: fmt.Sprintf("agent-retry-cost:%s:%d", next.ID, next.StateVersion), Category: contracts.CostRetry, Basis: "retry_transition", SourceKind: "agent_run", SourceID: next.ID, Actor: next.Actor, Task: next.Task, Session: next.Session, Units: 1}); err != nil {
					return contracts.AgentRun{}, err
				}
			}
			if err := s.observability.recordObservationTx(ctx, tx, contracts.RunObservation{WorkspaceID: next.WorkspaceID, IdempotencyKey: fmt.Sprintf("agent-%s-observation:%s:%d", operation, next.ID, next.StateVersion), Kind: kind, Component: "agent_loop", Operation: operation, Outcome: contracts.OutcomeWaiting, Actor: next.Actor, Task: next.Task, Session: next.Session, CausationID: next.CausationID, CorrelationID: next.CorrelationID, SourceKind: "agent_run", SourceID: next.ID, StartedAt: next.UpdatedAt, Metadata: map[string]string{"state": next.State}}); err != nil {
				return contracts.AgentRun{}, err
			}
		}
	}
	if s.beforeCommit != nil {
		if err := s.beforeCommit(); err != nil {
			return contracts.AgentRun{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.AgentRun{}, fmt.Errorf("commit agent run transition: %w", err)
	}
	return next, nil
}

func (s *AgentRunStore) Cancel(ctx context.Context, run contracts.AgentRun, reason string) (contracts.AgentRun, error) {
	next := run
	next.State, next.Phase, next.Termination = contracts.AgentRunCancelled, contracts.AgentPhaseModel, contracts.AgentTerminationCancelled
	next.LastOutput = run.LastOutput
	next.LastFailure = &contracts.LoopFailure{Code: contracts.AgentFailureCancelled, Message: strings.TrimSpace(reason), Phase: run.Phase}
	if next.LastFailure.Message == "" {
		next.LastFailure.Message = "run cancelled"
	}
	return s.Commit(ctx, run, next, contracts.AgentEventCancelled, map[string]any{"run_id": run.ID, "reason": next.LastFailure.Message})
}

func (s *AgentRunStore) Replay(ctx context.Context, workspaceID, runID string, from uint64, limit int) ([]contracts.EventEnvelope, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("agent run store is not configured")
	}
	workspaceID, runID = strings.TrimSpace(workspaceID), strings.TrimSpace(runID)
	if workspaceID == "" || runID == "" {
		return nil, fmt.Errorf("workspace_id and run_id are required")
	}
	if _, err := s.Get(ctx, workspaceID, runID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	events, err := NewEventStore(s.pool).ReadAfter(ctx, ReadRequest{WorkspaceID: workspaceID, AfterSequence: from, RunID: runID, Limit: limit})
	if err != nil {
		return nil, err
	}
	filtered := make([]contracts.EventEnvelope, 0, len(events))
	for _, event := range events {
		if !strings.HasPrefix(event.EventType, "agent.") {
			continue
		}
		var payload struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.RunID != runID {
			continue
		}
		filtered = append(filtered, event)
	}
	return filtered, nil
}

// ReplayCheckpoint returns the last canonical checkpoint carried by the
// run's events after the requested sequence. This is a verification primitive:
// callers can compare StateHash/HistoryHash with an incremental checkpoint
// without treating the mutable agent_runs row as the replay source.
func (s *AgentRunStore) ReplayCheckpoint(ctx context.Context, workspaceID, runID string, from uint64, limit int) (contracts.LoopCheckpoint, error) {
	events, err := s.Replay(ctx, workspaceID, runID, from, limit)
	if err != nil {
		return contracts.LoopCheckpoint{}, err
	}
	var replayed contracts.LoopCheckpoint
	for _, event := range events {
		var envelope struct {
			RunID      string                   `json:"run_id"`
			Checkpoint contracts.LoopCheckpoint `json:"checkpoint"`
		}
		if err := json.Unmarshal(event.Payload, &envelope); err != nil {
			return contracts.LoopCheckpoint{}, fmt.Errorf("decode agent replay checkpoint: %w", err)
		}
		if envelope.RunID != runID || envelope.Checkpoint.RunID != runID || envelope.Checkpoint.StateHash == "" {
			return contracts.LoopCheckpoint{}, fmt.Errorf("agent replay event %d has no canonical checkpoint", event.Sequence)
		}
		replayed = envelope.Checkpoint
		replayed.EventSequence = event.Sequence
	}
	if replayed.RunID == "" {
		return contracts.LoopCheckpoint{}, fmt.Errorf("no agent checkpoint found after sequence %d", from)
	}
	return replayed, nil
}

// ValidateTaskFence is intentionally public so the orchestrator checks the
// lease immediately before an external model admission.
func (s *AgentRunStore) ValidateTaskFence(ctx context.Context, run contracts.AgentRun) error {
	if run.Task == nil {
		return nil
	}
	if strings.TrimSpace(run.TaskOwnerID) == "" || run.TaskFence == 0 {
		return ErrAgentRunStale
	}
	return validateTaskFence(ctx, s.pool, run.WorkspaceID, run.Task.ID, run.TaskOwnerID, run.TaskFence)
}

func validateTaskFence(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskID, owner string, fence uint64) error {
	parsed, err := strconv.ParseInt(strings.TrimSpace(taskID), 10, 64)
	if err != nil {
		return ErrAgentRunStale
	}
	var currentOwner string
	var currentFence int64
	var assigned *string
	if err := pool.QueryRow(ctx, `SELECT l.owner_id, l.fence, t.assigned_session FROM fornix.task_execution_leases l JOIN fornix.tasks t ON t.workspace_id=l.workspace_id AND t.id=l.task_id WHERE l.workspace_id=$1 AND l.task_id=$2 AND l.released_at IS NULL AND l.lease_until > clock_timestamp()`, workspaceID, parsed).Scan(&currentOwner, &currentFence, &assigned); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrAgentRunStale
		}
		return fmt.Errorf("validate agent task fence: %w", err)
	}
	if currentFence <= 0 || uint64(currentFence) != fence || currentOwner != strings.TrimSpace(owner) || assigned == nil || *assigned != strings.TrimSpace(owner) {
		return ErrAgentRunStale
	}
	return nil
}

func validateRunTaskFenceTx(ctx context.Context, tx pgx.Tx, run contracts.AgentRun) error {
	if run.Task == nil {
		return nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(run.Task.ID), 10, 64)
	if err != nil {
		return ErrAgentRunStale
	}
	var owner string
	var fence int64
	var assigned *string
	err = tx.QueryRow(ctx, `SELECT l.owner_id, l.fence, t.assigned_session FROM fornix.task_execution_leases l JOIN fornix.tasks t ON t.workspace_id=l.workspace_id AND t.id=l.task_id WHERE l.workspace_id=$1 AND l.task_id=$2 AND l.released_at IS NULL AND l.lease_until > clock_timestamp() FOR UPDATE OF l`, run.WorkspaceID, parsed).Scan(&owner, &fence, &assigned)
	if errors.Is(err, pgx.ErrNoRows) || err != nil {
		if err == nil {
			return ErrAgentRunStale
		}
		return fmt.Errorf("validate agent task fence: %w", err)
	}
	if uint64(fence) != run.TaskFence || owner != run.TaskOwnerID || assigned == nil || *assigned != run.TaskOwnerID {
		return ErrAgentRunStale
	}
	return nil
}

func normalizeRunForCommit(run *contracts.AgentRun) error {
	if run == nil || strings.TrimSpace(run.ID) == "" || strings.TrimSpace(run.WorkspaceID) == "" {
		return fmt.Errorf("agent run identity is required")
	}
	if !contracts.IsAgentTerminal(run.State) && run.State != contracts.AgentRunPending && run.State != contracts.AgentRunRunning && run.State != contracts.AgentRunAwaitingApproval && run.State != contracts.AgentRunAwaitingRetry && run.State != contracts.AgentRunAwaitingExternal {
		return fmt.Errorf("invalid agent run state %q", run.State)
	}
	if run.Phase != contracts.AgentPhaseModel && run.Phase != contracts.AgentPhaseTool {
		return fmt.Errorf("invalid agent run phase %q", run.Phase)
	}
	if err := run.Budget.Normalize(); err != nil {
		return err
	}
	if len(run.History) > contracts.MaxAgentHistoryMessages {
		return fmt.Errorf("agent history exceeds message budget")
	}
	if len(run.PendingTools) > contracts.MaxAgentPendingTools {
		return fmt.Errorf("agent pending tools exceed budget")
	}
	for i := range run.History {
		if run.History[i].Role == "" {
			return fmt.Errorf("agent history message role is required")
		}
	}
	return nil
}

func agentEvent(eventType string, run contracts.AgentRun, payload any) (contracts.EventEnvelope, error) {
	eventPayload := map[string]any{
		"run_id":     run.ID,
		"state":      run.State,
		"phase":      run.Phase,
		"checkpoint": run.Checkpoint(),
		"payload":    payload,
	}
	event, err := contracts.NewEvent(eventType, eventPayload)
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	event.Scope.WorkspaceID = run.WorkspaceID
	event.Actor = run.Actor
	event.Task = cloneEntityRefForRun(run.Task)
	event.Session = cloneEntityRefForRun(run.Session)
	event.CausationID, event.CorrelationID = run.CausationID, run.CorrelationID
	event.IdempotencyKey = fmt.Sprintf("agent:%s:v:%d", run.ID, run.StateVersion)
	event.StateDeltas = []contracts.StateDelta{{Op: contracts.DeltaSet, Path: "/agent_runs/" + run.ID + "/state", Value: json.RawMessage(fmt.Sprintf("%q", run.State))}, {Op: contracts.DeltaSet, Path: "/agent_runs/" + run.ID + "/phase", Value: json.RawMessage(fmt.Sprintf("%q", run.Phase))}}
	return event, nil
}

type agentArtifactIDs struct {
	lastOutput any
	history    any
}

func redactAgentOutput(run *contracts.AgentRun) error {
	if run == nil {
		return fmt.Errorf("agent run is nil")
	}
	historyJSON, err := json.Marshal(run.History)
	if err != nil {
		return err
	}
	redactedHistory := model.RedactUnboundedBytes(historyJSON)
	var history []contracts.ModelMessage
	if err := json.Unmarshal(redactedHistory, &history); err != nil {
		return fmt.Errorf("redact agent history: %w", err)
	}
	run.History = history
	run.LastOutput = string(model.RedactUnboundedBytes([]byte(run.LastOutput)))
	return nil
}

func sameAgentHistory(left, right []contracts.ModelMessage) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func (s *AgentRunStore) artifactizeAgentOutputsTx(ctx context.Context, tx pgx.Tx, run contracts.AgentRun, historyJSON []byte) ([]byte, string, agentArtifactIDs, error) {
	if s.artifacts == nil {
		return nil, "", agentArtifactIDs{}, fmt.Errorf("artifact store is not configured")
	}
	ids := agentArtifactIDs{}
	inlineHistory := append([]byte(nil), historyJSON...)
	inlineLastOutput := run.LastOutput
	versionedSource := run.ID + ":v:" + strconv.FormatInt(run.StateVersion, 10)
	put := func(role, kind, mediaType string, raw []byte) (*contracts.ArtifactRef, error) {
		artifact, err := s.artifacts.PutTx(ctx, tx, ArtifactPutInput{
			WorkspaceID: run.WorkspaceID, Kind: kind, MediaType: mediaType, Raw: raw,
			Manifest: contracts.ArtifactManifest{Gist: "redacted agent output", Metadata: map[string]string{
				"agent_run_id": run.ID, "role": role, "state_version": strconv.FormatInt(run.StateVersion, 10),
			}}, SourceKind: "agent_run", SourceID: versionedSource, Role: role,
			IdempotencyKey: "agent-output:" + versionedSource + ":" + role,
			CausationID:    run.CausationID, CorrelationID: run.CorrelationID, Actor: run.Actor,
		})
		if err != nil {
			return nil, fmt.Errorf("store agent %s artifact: %w", role, err)
		}
		return &artifact.Reference, nil
	}
	if len([]byte(run.LastOutput)) > contracts.MaxToolEvidenceBytes && run.LastOutputArtifact == nil {
		ref, err := put("last_output", "agent-output", "text/plain", model.RedactUnboundedBytes([]byte(run.LastOutput)))
		if err != nil {
			return nil, "", ids, err
		}
		ids.lastOutput = ref.ArtifactID
		inlineLastOutput = toolArtifactMarker(*ref)
	}
	if len(historyJSON) > contracts.MaxAgentHistoryBytes && run.HistoryArtifact == nil {
		ref, err := put("history", "agent-history", "application/json", model.RedactUnboundedBytes(historyJSON))
		if err != nil {
			return nil, "", ids, err
		}
		ids.history = ref.ArtifactID
		inlineHistory = []byte(fmt.Sprintf(`{"artifact_id":%d,"content_hash":%q,"byte_size":%d}`, ref.ArtifactID, ref.ContentHash, ref.ByteSize))
	}
	if run.LastOutputArtifact != nil && ids.lastOutput == nil {
		ids.lastOutput = run.LastOutputArtifact.ArtifactID
	}
	if run.HistoryArtifact != nil && ids.history == nil {
		ids.history = run.HistoryArtifact.ArtifactID
	}
	return inlineHistory, inlineLastOutput, ids, nil
}

func readAgentRun(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, workspaceID, runID string, lock bool) (contracts.AgentRun, error) {
	query := agentRunSelectSQL + ` WHERE workspace_id=$1 AND id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	run, err := scanAgentRun(queryer.QueryRow(ctx, query, workspaceID, runID))
	if err != nil {
		return contracts.AgentRun{}, err
	}
	return hydrateAgentRunArtifacts(ctx, queryer, &run)
}

func readAgentRunTx(ctx context.Context, tx pgx.Tx, workspaceID, runID string, lock bool) (contracts.AgentRun, error) {
	return readAgentRun(ctx, tx, workspaceID, runID, lock)
}

func readAgentRunByIdempotencyTx(ctx context.Context, tx pgx.Tx, workspaceID, idempotencyKey string, lock bool) (contracts.AgentRun, error) {
	query := agentRunSelectSQL + ` WHERE workspace_id=$1 AND idempotency_key=$2`
	if lock {
		query += " FOR UPDATE"
	}
	run, err := scanAgentRun(tx.QueryRow(ctx, query, workspaceID, idempotencyKey))
	if err != nil {
		return contracts.AgentRun{}, err
	}
	return hydrateAgentRunArtifacts(ctx, tx, &run)
}

const agentRunSelectSQL = `SELECT id, workspace_id, request_id, idempotency_key, request_hash, schema_version,
 causation_id, correlation_id, actor, task_ref, session_ref, task_owner_id, task_fence, goal, provider, tools, budget, retrieval_request, context_hash,
 state, phase, turn, step, model_attempts, model_calls, tool_calls, input_tokens, output_tokens, total_tokens, context_bytes,
 cost, history, pending_tools, last_output, last_output_artifact_id, history_artifact_id, last_failure, termination, next_retry_at, state_version, event_sequence,
 state_hash, created_at, updated_at, started_at, finished_at FROM fornix.agent_runs`

func scanAgentRun(row interface{ Scan(...any) error }) (contracts.AgentRun, error) {
	var run contracts.AgentRun
	var err error
	var actorJSON, taskJSON, sessionJSON, providerJSON, toolsJSON, budgetJSON, retrievalJSON, costJSON, historyJSON, pendingJSON, failureJSON []byte
	var lastOutputArtifactID, historyArtifactID *int64
	var fence, sequence int64
	if err = row.Scan(&run.ID, &run.WorkspaceID, &run.RequestID, &run.IdempotencyKey, &run.RequestHash, &run.SchemaVersion, &run.CausationID, &run.CorrelationID, &actorJSON, &taskJSON, &sessionJSON, &run.TaskOwnerID, &fence, &run.Goal, &providerJSON, &toolsJSON, &budgetJSON, &retrievalJSON, &run.ContextHash, &run.State, &run.Phase, &run.Turn, &run.Step, &run.ModelAttempts, &run.ModelCalls, &run.ToolCalls, &run.InputTokens, &run.OutputTokens, &run.TotalTokens, &run.ContextBytes, &costJSON, &historyJSON, &pendingJSON, &run.LastOutput, &lastOutputArtifactID, &historyArtifactID, &failureJSON, &run.Termination, &run.NextRetryAt, &run.StateVersion, &sequence, &run.StateHash, &run.CreatedAt, &run.UpdatedAt, &run.StartedAt, &run.FinishedAt); err != nil {
		return contracts.AgentRun{}, err
	}
	if fence < 0 || sequence < 0 {
		return contracts.AgentRun{}, fmt.Errorf("agent run numeric state is invalid")
	}
	run.TaskFence, run.EventSequence = uint64(fence), uint64(sequence)
	if err := json.Unmarshal(actorJSON, &run.Actor); err != nil {
		return contracts.AgentRun{}, err
	}
	if run.Task, err = decodeEntityRef(taskJSON); err != nil {
		return contracts.AgentRun{}, err
	}
	if run.Session, err = decodeEntityRef(sessionJSON); err != nil {
		return contracts.AgentRun{}, err
	}
	if err := json.Unmarshal(providerJSON, &run.Provider); err != nil {
		return contracts.AgentRun{}, err
	}
	if err := json.Unmarshal(toolsJSON, &run.Tools); err != nil {
		return contracts.AgentRun{}, err
	}
	if err := json.Unmarshal(budgetJSON, &run.Budget); err != nil {
		return contracts.AgentRun{}, err
	}
	if len(retrievalJSON) > 0 && string(retrievalJSON) != "null" && string(retrievalJSON) != "{}" {
		run.Retrieval = &contracts.RetrievalRequest{}
		if err := json.Unmarshal(retrievalJSON, run.Retrieval); err != nil {
			return contracts.AgentRun{}, err
		}
	}
	if err := json.Unmarshal(costJSON, &run.Cost); err != nil {
		return contracts.AgentRun{}, err
	}
	if len(historyJSON) > 0 && historyJSON[0] == '[' {
		if err := json.Unmarshal(historyJSON, &run.History); err != nil {
			return contracts.AgentRun{}, err
		}
	}
	if err := json.Unmarshal(pendingJSON, &run.PendingTools); err != nil {
		return contracts.AgentRun{}, err
	}
	if len(failureJSON) > 0 && string(failureJSON) != "null" {
		run.LastFailure = &contracts.LoopFailure{}
		if err := json.Unmarshal(failureJSON, run.LastFailure); err != nil {
			return contracts.AgentRun{}, err
		}
	}
	if lastOutputArtifactID != nil {
		// Hydration fills the complete output after the row has been decoded.
		run.LastOutputArtifact = &contracts.ArtifactRef{ArtifactID: *lastOutputArtifactID, WorkspaceID: run.WorkspaceID}
	}
	if historyArtifactID != nil {
		run.HistoryArtifact = &contracts.ArtifactRef{ArtifactID: *historyArtifactID, WorkspaceID: run.WorkspaceID}
	}
	return run, nil
}

type agentArtifactQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func hydrateAgentRunArtifacts(ctx context.Context, queryer agentArtifactQueryer, run *contracts.AgentRun) (contracts.AgentRun, error) {
	if run == nil {
		return contracts.AgentRun{}, fmt.Errorf("agent run is nil")
	}
	if run.LastOutputArtifact != nil {
		ref, err := readArtifactRefByArtifactID(ctx, queryer, run.WorkspaceID, run.LastOutputArtifact.ArtifactID, "agent_run", "last_output")
		if err != nil {
			return contracts.AgentRun{}, err
		}
		artifact, err := readArtifact(ctx, queryer, run.WorkspaceID, ref.ArtifactID, false)
		if err != nil {
			return contracts.AgentRun{}, err
		}
		raw, err := readArtifactRawWithQuery(ctx, queryer, artifact)
		if err != nil {
			return contracts.AgentRun{}, err
		}
		if err := verifyArtifactBytes(artifact, raw, nil); err != nil {
			return contracts.AgentRun{}, err
		}
		run.LastOutputArtifact = &ref
		run.LastOutput = string(raw)
	}
	if run.HistoryArtifact != nil {
		ref, err := readArtifactRefByArtifactID(ctx, queryer, run.WorkspaceID, run.HistoryArtifact.ArtifactID, "agent_run", "history")
		if err != nil {
			return contracts.AgentRun{}, err
		}
		artifact, err := readArtifact(ctx, queryer, run.WorkspaceID, ref.ArtifactID, false)
		if err != nil {
			return contracts.AgentRun{}, err
		}
		raw, err := readArtifactRawWithQuery(ctx, queryer, artifact)
		if err != nil {
			return contracts.AgentRun{}, err
		}
		if err := verifyArtifactBytes(artifact, raw, nil); err != nil {
			return contracts.AgentRun{}, err
		}
		if err := json.Unmarshal(raw, &run.History); err != nil {
			return contracts.AgentRun{}, fmt.Errorf("decode agent history artifact: %w", err)
		}
		run.HistoryArtifact = &ref
	}
	return *run, nil
}

func cloneEntityRefForRun(ref *contracts.EntityRef) *contracts.EntityRef {
	if ref == nil {
		return nil
	}
	copy := *ref
	return &copy
}

func cloneRetrievalRequest(request *contracts.RetrievalRequest) *contracts.RetrievalRequest {
	if request == nil {
		return nil
	}
	copy := *request
	copy.ExactSourceRefs = append([]string(nil), request.ExactSourceRefs...)
	copy.QueryEmbedding = append([]float32(nil), request.QueryEmbedding...)
	if request.EnableGraph != nil {
		value := *request.EnableGraph
		copy.EnableGraph = &value
	}
	if request.EnableVector != nil {
		value := *request.EnableVector
		copy.EnableVector = &value
	}
	return &copy
}

func sameEntityRef(left, right *contracts.EntityRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.ID == right.ID && left.Kind == right.Kind && left.WorkspaceID == right.WorkspaceID
}
