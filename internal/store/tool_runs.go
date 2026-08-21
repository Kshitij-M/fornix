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
	"github.com/omaveda/fornix/internal/model"
)

var (
	ErrToolRunMissing       = errors.New("tool run not found")
	ErrToolApprovalMissing  = errors.New("tool approval not found")
	ErrToolApprovalConflict = errors.New("tool approval is already decided")
	ErrToolRunTerminal      = errors.New("tool run is already terminal")
)

// ToolRunStore is the Postgres authority for durable tool reservations,
// approvals, results, and artifact links. External execution remains
// at-least-once; this store makes the durable effect idempotent and fenced.
type ToolRunStore struct {
	pool          *pgxpool.Pool
	events        *EventStore
	artifacts     *ArtifactStore
	observability *ObservabilityStore
}

// NewToolRunStore constructs the tool-run store and its artifact boundary.
func NewToolRunStore(pool *pgxpool.Pool, events *EventStore) *ToolRunStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &ToolRunStore{pool: pool, events: events, artifacts: NewArtifactStore(pool)}
}

// SetObservability attaches the optional transactional observation sink.
func (s *ToolRunStore) SetObservability(observer *ObservabilityStore) {
	if s != nil {
		s.observability = observer
	}
}

// SetArtifactFailureHook provides a deterministic rollback seam for output
// integration tests. Production callers should leave it unset.
func (s *ToolRunStore) SetArtifactFailureHook(hook func(string) error) {
	if s != nil && s.artifacts != nil {
		s.artifacts.SetFailureHook(hook)
	}
}

// Reserve creates or reuses a workspace-scoped tool run keyed by request
// identity. It records only redacted request evidence.
func (s *ToolRunStore) Reserve(ctx context.Context, req contracts.ToolRequest, mode string) (contracts.ToolRun, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.ToolRun{}, false, fmt.Errorf("tool run store is not configured")
	}
	if err := req.Normalize(); err != nil {
		return contracts.ToolRun{}, false, err
	}
	hash, err := req.RequestHash()
	if err != nil {
		return contracts.ToolRun{}, false, err
	}
	requestEvidence, err := req.RedactedEvidence()
	if err != nil {
		return contracts.ToolRun{}, false, err
	}
	requestEvidence = model.RedactBytes(requestEvidence)
	actorJSON, _ := json.Marshal(req.Actor)
	taskJSON, _ := jsonOrEmpty(req.Task)
	sessionJSON, _ := jsonOrEmpty(req.Session)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ToolRun{}, false, fmt.Errorf("begin tool reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id string
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.tool_runs(id, workspace_id, request_id, idempotency_key, request_hash, schema_version,
		  causation_id, correlation_id, tool_id, capability, mode, status, actor, task_ref, session_ref,
		  task_owner_id, task_fence, request_evidence)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending',$12::jsonb,$13::jsonb,$14::jsonb,$15,$16,$17::jsonb)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING RETURNING id`,
		contracts.NewID("toolrun"), req.WorkspaceID, req.RequestID, req.IdempotencyKey, hash, req.SchemaVersion,
		req.CausationID, req.CorrelationID, req.ToolID, req.Capability, mode, actorJSON, taskJSON, sessionJSON,
		req.TaskOwnerID, int64(req.TaskFence), requestEvidence).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		run, readErr := readToolRunTx(ctx, tx, req.WorkspaceID, req.IdempotencyKey)
		if readErr != nil {
			return contracts.ToolRun{}, false, readErr
		}
		if run.RequestHash != hash {
			return contracts.ToolRun{}, false, fmt.Errorf("%w: %s", ErrIdempotencyConflict, req.IdempotencyKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ToolRun{}, false, fmt.Errorf("commit duplicate tool reservation: %w", err)
		}
		return run, true, nil
	}
	if err != nil {
		return contracts.ToolRun{}, false, fmt.Errorf("reserve tool run: %w", err)
	}
	run, err := readToolRunTx(ctx, tx, req.WorkspaceID, req.IdempotencyKey)
	if err != nil {
		return contracts.ToolRun{}, false, err
	}
	event, err := toolEvent(contracts.ToolEventRequested, req.WorkspaceID, req, run.ID, map[string]any{"run_id": run.ID, "tool_id": req.ToolID, "mode": mode, "request_hash": hash})
	if err != nil {
		return contracts.ToolRun{}, false, err
	}
	event.IdempotencyKey = "tool-requested:" + run.ID
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ToolRun{}, false, fmt.Errorf("append tool.requested: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ToolRun{}, false, fmt.Errorf("commit tool reservation: %w", err)
	}
	return run, false, nil
}

// CreateApproval durably places a tool run into an approval wait state with a
// bounded expiry and auditable request identity.
func (s *ToolRunStore) CreateApproval(ctx context.Context, run contracts.ToolRun, req contracts.ToolRequest, ttl time.Duration) (contracts.ApprovalRequest, error) {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	if ttl > 24*time.Hour {
		ttl = 24 * time.Hour
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ApprovalRequest{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var approvalID string
	expiresAt := time.Now().UTC().Add(ttl)
	requestJSON, _ := json.Marshal(req.Actor)
	taskJSON, _ := jsonOrEmpty(req.Task)
	sessionJSON, _ := jsonOrEmpty(req.Session)
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.tool_approvals(id, workspace_id, request_id, run_id, request_hash, tool_id, actor, task_ref, session_ref, expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10)
		ON CONFLICT (run_id) DO NOTHING RETURNING id`, contracts.NewID("approval"), run.WorkspaceID, run.RequestID, run.ID, run.RequestHash, run.ToolID, requestJSON, taskJSON, sessionJSON, expiresAt).Scan(&approvalID)
	if errors.Is(err, pgx.ErrNoRows) {
		approval, readErr := readApprovalTx(ctx, tx, run.WorkspaceID, run.ID)
		if readErr != nil {
			return contracts.ApprovalRequest{}, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ApprovalRequest{}, err
		}
		return approval, nil
	}
	if err != nil {
		return contracts.ApprovalRequest{}, fmt.Errorf("reserve tool approval: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.tool_runs SET status='awaiting_approval', approval_id=$2 WHERE id=$1 AND workspace_id=$3`, run.ID, approvalID, run.WorkspaceID); err != nil {
		return contracts.ApprovalRequest{}, err
	}
	approval, err := readApprovalTx(ctx, tx, run.WorkspaceID, run.ID)
	if err != nil {
		return contracts.ApprovalRequest{}, err
	}
	event, err := toolEvent(contracts.ToolEventApprovalRequested, req.WorkspaceID, req, run.ID, map[string]any{"run_id": run.ID, "approval_id": approval.ID, "request_hash": run.RequestHash, "expires_at": approval.ExpiresAt})
	if err != nil {
		return contracts.ApprovalRequest{}, err
	}
	event.IdempotencyKey = "tool-approval-requested:" + approval.ID
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ApprovalRequest{}, err
	}
	if s.observability != nil {
		if err := s.observability.recordObservationTx(ctx, tx, contracts.RunObservation{
			WorkspaceID: run.WorkspaceID, IdempotencyKey: "approval-observation:" + approval.ID,
			Kind: contracts.ObservationApproval, Component: "tool_runtime", Operation: "request",
			Outcome: contracts.OutcomeWaiting, Actor: req.Actor, Task: req.Task, Session: req.Session,
			SourceKind: "tool_approval", SourceID: approval.ID, StartedAt: time.Now().UTC(),
			Metadata: map[string]string{"tool_id": run.ToolID},
		}); err != nil {
			return contracts.ApprovalRequest{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ApprovalRequest{}, err
	}
	return approval, nil
}

// SetAwaitingApproval returns the canonical approval-waiting tool run. The
// state transition is owned by CreateApproval and remains idempotent.
func (s *ToolRunStore) SetAwaitingApproval(ctx context.Context, run contracts.ToolRun, approval contracts.ApprovalRequest) (contracts.ToolRun, error) {
	return s.Get(ctx, run.WorkspaceID, run.IdempotencyKey)
}

// MarkStarted transitions a non-terminal run to execution after validating its
// task fence.
func (s *ToolRunStore) MarkStarted(ctx context.Context, run contracts.ToolRun) (contracts.ToolRun, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ToolRun{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, err := readToolRunTx(ctx, tx, run.WorkspaceID, run.IdempotencyKey)
	if err != nil {
		return contracts.ToolRun{}, err
	}
	if contracts.IsToolTerminal(current.Status) {
		return current, nil
	}
	if err := validateTaskFenceTx(ctx, tx, current); err != nil {
		return contracts.ToolRun{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.tool_runs SET status='running', attempt=attempt+1, started_at=COALESCE(started_at,clock_timestamp()) WHERE id=$1 AND workspace_id=$2 AND status IN ('pending','awaiting_approval')`, current.ID, current.WorkspaceID); err != nil {
		return contracts.ToolRun{}, err
	}
	request := toolRequestFromRun(current)
	event, err := toolEvent(contracts.ToolEventStarted, current.WorkspaceID, request, current.ID, map[string]any{"run_id": current.ID, "attempt": current.Attempt + 1})
	if err != nil {
		return contracts.ToolRun{}, err
	}
	event.IdempotencyKey = "tool-started:" + current.ID
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ToolRun{}, err
	}
	updated, err := readToolRunTx(ctx, tx, current.WorkspaceID, current.IdempotencyKey)
	if err != nil {
		return contracts.ToolRun{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ToolRun{}, err
	}
	return updated, nil
}

// Finish commits a tool result, redacted evidence, artifact links, lifecycle
// event, and optional observations atomically. A stale task worker is rejected
// before any authoritative effect is written.
func (s *ToolRunStore) Finish(ctx context.Context, run contracts.ToolRun, result contracts.ToolResult) (contracts.ToolRun, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ToolRun{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, err := readToolRunTx(ctx, tx, run.WorkspaceID, run.IdempotencyKey)
	if err != nil {
		return contracts.ToolRun{}, err
	}
	if contracts.IsToolTerminal(current.Status) {
		return current, nil
	}
	if err := validateTaskFenceTx(ctx, tx, current); err != nil {
		return contracts.ToolRun{}, err
	}
	status := result.Status
	if status == "" {
		status = contracts.ToolRunFailed
	}
	if status == contracts.ToolRunSucceeded && result.Failure != nil {
		status = contracts.ToolRunFailed
	}
	result.Status, result.RunID, result.RequestID, result.ToolID = status, current.ID, current.RequestID, current.ToolID
	if result.ContentHash == "" {
		result.ContentHash = result.Hash()
	}
	storedResult, artifactIDs, err := s.artifactizeToolResultTx(ctx, tx, current, result)
	if err != nil {
		return contracts.ToolRun{}, err
	}
	resultJSON, err := json.Marshal(storedResult)
	if err != nil {
		return contracts.ToolRun{}, fmt.Errorf("marshal stored tool result: %w", err)
	}
	failureJSON, _ := json.Marshal(result.Failure)
	evidence := model.RedactBytes(resultJSON)
	if len(evidence) == 0 {
		evidence = []byte(`{}`)
	}
	if len(evidence) > contracts.MaxToolEvidenceBytes {
		evidence = []byte(`{"redacted":true,"truncated":true}`)
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.tool_runs SET status=$2, result=$3::jsonb, failure=$4::jsonb, response_evidence=$5::jsonb, stdout_artifact_id=$6, stderr_artifact_id=$7, result_artifact_id=$8, finished_at=clock_timestamp(), duration_ms=GREATEST(0,FLOOR(EXTRACT(EPOCH FROM (clock_timestamp()-COALESCE(started_at,created_at)))*1000)::bigint) WHERE id=$1 AND workspace_id=$9`, current.ID, status, resultJSON, nullJSON(failureJSON), evidence, artifactIDs.stdout, artifactIDs.stderr, artifactIDs.result, current.WorkspaceID); err != nil {
		return contracts.ToolRun{}, err
	}
	request := toolRequestFromRun(current)
	eventType := contracts.ToolEventFailed
	if status == contracts.ToolRunSucceeded {
		eventType = contracts.ToolEventSucceeded
	}
	if status == contracts.ToolRunDenied {
		eventType = contracts.ToolEventDenied
	}
	event, err := toolEvent(eventType, current.WorkspaceID, request, current.ID, map[string]any{"run_id": current.ID, "status": status, "content_hash": result.ContentHash, "failure": result.Failure})
	if err != nil {
		return contracts.ToolRun{}, err
	}
	event.IdempotencyKey = "tool-result:" + current.ID
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ToolRun{}, err
	}
	if s.observability != nil {
		var durationMS int64
		if err := tx.QueryRow(ctx, `SELECT duration_ms FROM fornix.tool_runs WHERE workspace_id=$1 AND id=$2`, current.WorkspaceID, current.ID).Scan(&durationMS); err != nil {
			return contracts.ToolRun{}, fmt.Errorf("read tool run duration: %w", err)
		}
		startedAt := current.CreatedAt
		if current.StartedAt != nil {
			startedAt = *current.StartedAt
		}
		outputBytes := int64(len([]byte(result.Stdout)) + len([]byte(result.Stderr)))
		if err := s.observability.recordObservationTx(ctx, tx, contracts.RunObservation{WorkspaceID: current.WorkspaceID, IdempotencyKey: "tool-observation:" + current.ID, Kind: contracts.ObservationTool, Component: "tool_runtime", Operation: "execute", Outcome: status, Actor: current.Actor, Task: current.Task, Session: current.Session, CausationID: current.CausationID, CorrelationID: current.CorrelationID, SourceKind: "tool_run", SourceID: current.ID, StartedAt: startedAt, FinishedAt: time.Now().UTC(), DurationMS: durationMS, OutputBytes: outputBytes}); err != nil {
			return contracts.ToolRun{}, err
		}
		if err := s.observability.recordCostTx(ctx, tx, contracts.CostLedgerEntry{WorkspaceID: current.WorkspaceID, IdempotencyKey: "tool-cost:" + current.ID, Category: contracts.CostTool, Basis: "duration_ms", SourceKind: "tool_run", SourceID: current.ID, Actor: current.Actor, Task: current.Task, Session: current.Session, CausationID: current.CausationID, CorrelationID: current.CorrelationID, Units: float64(durationMS), DurationMS: durationMS, Bytes: outputBytes, Measured: true}); err != nil {
			return contracts.ToolRun{}, err
		}
	}
	updated, err := readToolRunTx(ctx, tx, current.WorkspaceID, current.IdempotencyKey)
	if err != nil {
		return contracts.ToolRun{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ToolRun{}, err
	}
	return updated, nil
}

type toolArtifactIDs struct {
	stdout any
	stderr any
	result any
}

// artifactizeToolResultTx keeps the hot tool row bounded while preserving the
// redacted output and result envelope in immutable artifacts. Separate output
// roles avoid storing stdout/stderr twice inside a result artifact.
func (s *ToolRunStore) artifactizeToolResultTx(ctx context.Context, tx pgx.Tx, run contracts.ToolRun, result contracts.ToolResult) (contracts.ToolResult, toolArtifactIDs, error) {
	stored := result
	ids := toolArtifactIDs{}
	refs := make([]contracts.ArtifactReference, 0, 2)
	put := func(role, kind, mediaType string, raw []byte) (*contracts.ArtifactRef, error) {
		if len(raw) == 0 {
			return nil, nil
		}
		if s.artifacts == nil {
			return nil, fmt.Errorf("artifact store is not configured")
		}
		result, err := s.artifacts.PutTx(ctx, tx, ArtifactPutInput{
			WorkspaceID: run.WorkspaceID, Kind: kind, MediaType: mediaType, Raw: raw,
			Manifest: contracts.ArtifactManifest{Gist: "redacted tool output", Metadata: map[string]string{
				"tool_run_id": run.ID, "tool_id": run.ToolID, "role": role,
			}}, SourceKind: "tool_run", SourceID: run.ID, Role: role,
			IdempotencyKey: "tool-output:" + run.ID + ":" + role,
			CausationID:    run.CausationID, CorrelationID: run.CorrelationID, Actor: run.Actor,
		})
		if err != nil {
			return nil, fmt.Errorf("store tool %s artifact: %w", role, err)
		}
		return &result.Reference, nil
	}
	if len(result.Stdout) > contracts.MaxToolEvidenceBytes {
		ref, err := put("stdout", "tool-stdout", "text/plain", model.RedactUnboundedBytes([]byte(result.Stdout)))
		if err != nil {
			return contracts.ToolResult{}, ids, err
		}
		stored.Stdout = toolArtifactMarker(*ref)
		ids.stdout = ref.ArtifactID
		refs = append(refs, artifactReferenceFromRef(*ref))
	} else {
		stored.Stdout = string(model.RedactUnboundedBytes([]byte(result.Stdout)))
	}
	if len(result.Stderr) > contracts.MaxToolEvidenceBytes {
		ref, err := put("stderr", "tool-stderr", "text/plain", model.RedactUnboundedBytes([]byte(result.Stderr)))
		if err != nil {
			return contracts.ToolResult{}, ids, err
		}
		stored.Stderr = toolArtifactMarker(*ref)
		ids.stderr = ref.ArtifactID
		refs = append(refs, artifactReferenceFromRef(*ref))
	} else {
		stored.Stderr = string(model.RedactUnboundedBytes([]byte(result.Stderr)))
	}
	stored.Artifacts = append(append([]contracts.ArtifactReference(nil), result.Artifacts...), refs...)
	fullRedacted, err := json.Marshal(struct {
		RequestID   string                        `json:"request_id"`
		RunID       string                        `json:"run_id,omitempty"`
		ToolID      string                        `json:"tool_id"`
		Status      string                        `json:"status"`
		ExitCode    int                           `json:"exit_code,omitempty"`
		Stdout      string                        `json:"stdout,omitempty"`
		Stderr      string                        `json:"stderr,omitempty"`
		StartedAt   time.Time                     `json:"started_at,omitempty"`
		FinishedAt  time.Time                     `json:"finished_at,omitempty"`
		DurationMS  int64                         `json:"duration_ms,omitempty"`
		ContentHash string                        `json:"content_hash,omitempty"`
		Artifacts   []contracts.ArtifactReference `json:"artifacts,omitempty"`
		Failure     *contracts.ToolFailure        `json:"failure,omitempty"`
	}{result.RequestID, result.RunID, result.ToolID, result.Status, result.ExitCode, result.Stdout, result.Stderr, result.StartedAt, result.FinishedAt, result.DurationMS, result.ContentHash, result.Artifacts, result.Failure})
	if err != nil {
		return contracts.ToolResult{}, ids, err
	}
	if len(model.RedactUnboundedBytes(fullRedacted)) > contracts.MaxToolEvidenceBytes {
		artifactPayload, err := json.Marshal(stored)
		if err != nil {
			return contracts.ToolResult{}, ids, err
		}
		ref, err := put("result", "tool-result", "application/json", model.RedactUnboundedBytes(artifactPayload))
		if err != nil {
			return contracts.ToolResult{}, ids, err
		}
		ids.result = ref.ArtifactID
	}
	return stored, ids, nil
}

func toolArtifactMarker(ref contracts.ArtifactRef) string {
	return fmt.Sprintf("[fornix-artifact id=%d sha256=%s bytes=%d]", ref.ArtifactID, ref.ContentHash, ref.ByteSize)
}

func artifactReferenceFromRef(ref contracts.ArtifactRef) contracts.ArtifactReference {
	return contracts.ArtifactReference{Ref: fmt.Sprintf("artifact:%d", ref.ArtifactID), Kind: ref.Role, SHA256: ref.ContentHash, MediaType: ref.MediaType, SizeBytes: ref.ByteSize}
}

// Get reads one tool run by workspace-scoped idempotency key.
func (s *ToolRunStore) Get(ctx context.Context, workspaceID, idempotencyKey string) (contracts.ToolRun, error) {
	return readToolRun(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(idempotencyKey))
}

// GetApproval reads one approval request within its workspace.
func (s *ToolRunStore) GetApproval(ctx context.Context, workspaceID, approvalID string) (contracts.ApprovalRequest, error) {
	return readApproval(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(approvalID))
}

// DecideApproval commits one approval decision idempotently and leaves the
// request auditable for replay.
func (s *ToolRunStore) DecideApproval(ctx context.Context, decision contracts.ApprovalDecision) (contracts.ApprovalRequest, error) {
	decision.WorkspaceID, decision.ApprovalID, decision.Decision = strings.TrimSpace(decision.WorkspaceID), strings.TrimSpace(decision.ApprovalID), strings.ToLower(strings.TrimSpace(decision.Decision))
	if decision.WorkspaceID == "" || decision.ApprovalID == "" {
		return contracts.ApprovalRequest{}, errors.New("workspace_id and approval_id are required")
	}
	if decision.Decision != contracts.ApprovalApproved && decision.Decision != contracts.ApprovalDenied {
		return contracts.ApprovalRequest{}, errors.New("decision must be approved or denied")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ApprovalRequest{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	approval, err := readApprovalByIDTx(ctx, tx, decision.WorkspaceID, decision.ApprovalID)
	if err != nil {
		return contracts.ApprovalRequest{}, err
	}
	if approval.Status == contracts.ApprovalPending && !approval.ExpiresAt.After(time.Now().UTC()) {
		_, _ = tx.Exec(ctx, `UPDATE fornix.tool_approvals SET status='expired', decided_at=clock_timestamp() WHERE id=$1`, approval.ID)
		approval.Status = contracts.ApprovalExpired
	}
	if approval.Status != contracts.ApprovalPending {
		if approval.Status == decision.Decision {
			if err := tx.Commit(ctx); err != nil {
				return contracts.ApprovalRequest{}, err
			}
			return approval, nil
		}
		return contracts.ApprovalRequest{}, ErrToolApprovalConflict
	}
	decidedAt := time.Now().UTC()
	reason := strings.TrimSpace(decision.Reason)
	actorJSON, _ := json.Marshal(decision.Actor)
	if _, err := tx.Exec(ctx, `UPDATE fornix.tool_approvals SET status=$2, decided_by=$3::jsonb, decision_reason=$4, decided_at=$5 WHERE workspace_id=$1 AND id=$6`, decision.WorkspaceID, decision.Decision, actorJSON, reason, decidedAt, decision.ApprovalID); err != nil {
		return contracts.ApprovalRequest{}, err
	}
	approval.Status, approval.DecidedAt = decision.Decision, &decidedAt
	event, err := toolEvent(contracts.ToolEventApprovalDecided, decision.WorkspaceID, toolRequestFromApproval(approval), approval.RunID, map[string]any{"approval_id": approval.ID, "decision": approval.Status, "reason": reason})
	if err != nil {
		return contracts.ApprovalRequest{}, err
	}
	event.IdempotencyKey = "tool-approval-decision:" + approval.ID
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ApprovalRequest{}, err
	}
	if s.observability != nil {
		if err := s.observability.recordObservationTx(ctx, tx, contracts.RunObservation{
			WorkspaceID: decision.WorkspaceID, IdempotencyKey: "approval-decision-observation:" + approval.ID,
			Kind: contracts.ObservationApproval, Component: "tool_runtime", Operation: "decide",
			Outcome: decision.Decision, Actor: decision.Actor, Task: approval.Task, Session: approval.Session,
			SourceKind: "tool_approval", SourceID: approval.ID, StartedAt: decidedAt,
		}); err != nil {
			return contracts.ApprovalRequest{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ApprovalRequest{}, err
	}
	return approval, nil
}

// ValidateTaskFence verifies that a task-bound tool request still owns the
// current workspace task fence.
func (s *ToolRunStore) ValidateTaskFence(ctx context.Context, req contracts.ToolRequest) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	run := contracts.ToolRun{WorkspaceID: req.WorkspaceID, Task: req.Task, TaskOwnerID: req.TaskOwnerID, TaskFence: req.TaskFence}
	if err := validateTaskFenceTx(ctx, tx, run); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func validateTaskFenceTx(ctx context.Context, tx pgx.Tx, run contracts.ToolRun) error {
	if run.Task == nil {
		return nil
	}
	if run.TaskFence == 0 || strings.TrimSpace(run.TaskOwnerID) == "" {
		return fmt.Errorf("%w: task owner and fence are required", ErrTaskLeaseFenced)
	}
	var owner string
	var fence int64
	var released *time.Time
	var active bool
	if err := tx.QueryRow(ctx, `SELECT l.owner_id,l.fence,l.released_at,(l.released_at IS NULL AND l.lease_until > clock_timestamp()) FROM fornix.task_execution_leases l WHERE l.workspace_id=$1 AND l.task_id=$2::bigint FOR UPDATE`, run.WorkspaceID, run.Task.ID).Scan(&owner, &fence, &released, &active); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: task lease missing", ErrTaskLeaseFenced)
		}
		return err
	}
	if owner != run.TaskOwnerID || uint64(fence) != run.TaskFence || released != nil || !active {
		return fmt.Errorf("%w: expected owner=%s fence=%d", ErrTaskLeaseFenced, run.TaskOwnerID, run.TaskFence)
	}
	return nil
}

func readToolRun(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, key string) (contracts.ToolRun, error) {
	return readToolRunWithQuery(ctx, queryer, workspaceID, key)
}
func readToolRunTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (contracts.ToolRun, error) {
	return readToolRunWithQuery(ctx, tx, workspaceID, key)
}
func readToolRunWithQuery(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, key string) (contracts.ToolRun, error) {
	var run contracts.ToolRun
	var actorJSON, taskJSON, sessionJSON, requestEvidence, responseEvidence, resultJSON, failureJSON []byte
	var taskOwner string
	var fence int64
	var started, finished *time.Time
	var stdoutArtifactID, stderrArtifactID, resultArtifactID *int64
	err := queryer.QueryRow(ctx, `SELECT id,workspace_id,request_id,idempotency_key,request_hash,schema_version,causation_id,correlation_id,tool_id,capability,mode,status,actor,task_ref,session_ref,task_owner_id,task_fence,approval_id,attempt,result,failure,request_evidence,response_evidence,stdout_artifact_id,stderr_artifact_id,result_artifact_id,created_at,started_at,finished_at,duration_ms FROM fornix.tool_runs WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, workspaceID, key).Scan(&run.ID, &run.WorkspaceID, &run.RequestID, &run.IdempotencyKey, &run.RequestHash, &run.SchemaVersion, &run.CausationID, &run.CorrelationID, &run.ToolID, &run.Capability, &run.Mode, &run.Status, &actorJSON, &taskJSON, &sessionJSON, &taskOwner, &fence, &run.ApprovalID, &run.Attempt, &resultJSON, &failureJSON, &requestEvidence, &responseEvidence, &stdoutArtifactID, &stderrArtifactID, &resultArtifactID, &run.CreatedAt, &started, &finished, &run.DurationMS)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ToolRun{}, ErrToolRunMissing
	}
	if err != nil {
		return contracts.ToolRun{}, err
	}
	if err := json.Unmarshal(actorJSON, &run.Actor); err != nil {
		return contracts.ToolRun{}, err
	}
	var decErr error
	run.Task, decErr = decodeEntityRef(taskJSON)
	if decErr != nil {
		return contracts.ToolRun{}, decErr
	}
	run.Session, decErr = decodeEntityRef(sessionJSON)
	if decErr != nil {
		return contracts.ToolRun{}, decErr
	}
	run.TaskOwnerID, run.TaskFence = taskOwner, uint64(fence)
	run.RequestEvidence = append([]byte(nil), requestEvidence...)
	run.ResponseEvidence = append([]byte(nil), responseEvidence...)
	run.StartedAt, run.FinishedAt = started, finished
	if len(resultJSON) > 0 && string(resultJSON) != "null" {
		run.Result = &contracts.ToolResult{}
		if err := json.Unmarshal(resultJSON, run.Result); err != nil {
			return contracts.ToolRun{}, err
		}
	}
	if len(failureJSON) > 0 && string(failureJSON) != "null" {
		run.Failure = &contracts.ToolFailure{}
		if err := json.Unmarshal(failureJSON, run.Failure); err != nil {
			return contracts.ToolRun{}, err
		}
	}
	for _, item := range []struct {
		id   *int64
		role string
		dst  **contracts.ArtifactRef
	}{{stdoutArtifactID, "stdout", &run.StdoutArtifact}, {stderrArtifactID, "stderr", &run.StderrArtifact}, {resultArtifactID, "result", &run.ResultArtifact}} {
		if item.id == nil {
			continue
		}
		ref, refErr := readArtifactRefBySource(ctx, queryer, workspaceID, "tool_run", run.ID, item.role)
		if refErr != nil {
			return contracts.ToolRun{}, refErr
		}
		*item.dst = &ref
	}
	return run, nil
}

func readApproval(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, id string) (contracts.ApprovalRequest, error) {
	return readApprovalWithQuery(ctx, queryer, workspaceID, id, false)
}
func readApprovalTx(ctx context.Context, tx pgx.Tx, workspaceID, runID string) (contracts.ApprovalRequest, error) {
	return readApprovalWithQuery(ctx, tx, workspaceID, runID, true)
}
func readApprovalByIDTx(ctx context.Context, tx pgx.Tx, workspaceID, id string) (contracts.ApprovalRequest, error) {
	return readApprovalByIDQuery(ctx, tx, workspaceID, id, true)
}
func readApprovalWithQuery(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, id string, lock bool) (contracts.ApprovalRequest, error) {
	q := `SELECT id,workspace_id,request_id,run_id,request_hash,tool_id,actor,task_ref,session_ref,status,reason,expires_at,created_at,decided_at FROM fornix.tool_approvals WHERE workspace_id=$1 AND run_id=$2`
	if lock {
		q += " FOR UPDATE"
	}
	return scanApproval(queryer.QueryRow(ctx, q, workspaceID, id))
}
func readApprovalByIDQuery(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, id string, lock bool) (contracts.ApprovalRequest, error) {
	q := `SELECT id,workspace_id,request_id,run_id,request_hash,tool_id,actor,task_ref,session_ref,status,reason,expires_at,created_at,decided_at FROM fornix.tool_approvals WHERE workspace_id=$1 AND id=$2`
	if lock {
		q += " FOR UPDATE"
	}
	return scanApproval(queryer.QueryRow(ctx, q, workspaceID, id))
}
func scanApproval(row interface{ Scan(...any) error }) (contracts.ApprovalRequest, error) {
	var a contracts.ApprovalRequest
	var actorJSON, taskJSON, sessionJSON []byte
	if err := row.Scan(&a.ID, &a.WorkspaceID, &a.RequestID, &a.RunID, &a.RequestHash, &a.ToolID, &actorJSON, &taskJSON, &sessionJSON, &a.Status, &a.Reason, &a.ExpiresAt, &a.CreatedAt, &a.DecidedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.ApprovalRequest{}, ErrToolApprovalMissing
		}
		return contracts.ApprovalRequest{}, err
	}
	if err := json.Unmarshal(actorJSON, &a.Actor); err != nil {
		return contracts.ApprovalRequest{}, err
	}
	var err error
	a.Task, err = decodeEntityRef(taskJSON)
	if err != nil {
		return contracts.ApprovalRequest{}, err
	}
	a.Session, err = decodeEntityRef(sessionJSON)
	return a, err
}

func toolEvent(eventType, workspace string, req contracts.ToolRequest, runID string, payload any) (contracts.EventEnvelope, error) {
	event, err := contracts.NewEvent(eventType, payload)
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	event.Scope.WorkspaceID = workspace
	event.Actor = req.Actor
	if req.Task != nil {
		copyRef := *req.Task
		copyRef.WorkspaceID = workspace
		event.Task = &copyRef
	}
	if req.Session != nil {
		copyRef := *req.Session
		copyRef.WorkspaceID = workspace
		event.Session = &copyRef
	}
	event.CausationID = req.CausationID
	event.CorrelationID = req.CorrelationID
	return event, nil
}
func toolRequestFromRun(run contracts.ToolRun) contracts.ToolRequest {
	return contracts.ToolRequest{SchemaVersion: run.SchemaVersion, RequestID: run.RequestID, IdempotencyKey: run.IdempotencyKey, CausationID: run.CausationID, CorrelationID: run.CorrelationID, WorkspaceID: run.WorkspaceID, Actor: run.Actor, Task: run.Task, Session: run.Session, TaskOwnerID: run.TaskOwnerID, TaskFence: run.TaskFence, ToolID: run.ToolID, Capability: run.Capability, Mode: run.Mode}
}
func toolRequestFromApproval(a contracts.ApprovalRequest) contracts.ToolRequest {
	return contracts.ToolRequest{SchemaVersion: contracts.ToolSchemaVersion, RequestID: a.RequestID, IdempotencyKey: a.RequestID, WorkspaceID: a.WorkspaceID, Actor: a.Actor, Task: a.Task, Session: a.Session, ToolID: a.ToolID}
}

// IsToolTerminal reports whether a tool run can no longer be advanced.
func IsToolTerminal(status string) bool { return contracts.IsToolTerminal(status) }
