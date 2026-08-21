// Package contracts contains the versioned data contracts shared by Fornix's
// verifiable AI work path: admission, execution, evidence, artifacts,
// accounting, replay, and operator surfaces. Contracts keep workspace scope,
// provenance, budgets, and replay identities explicit at every durable
// boundary.
package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	AgentSchemaVersion       = 1
	DefaultAgentMaxTurns     = 8
	DefaultAgentMaxSteps     = 32
	DefaultAgentMaxToolCalls = 64
	DefaultAgentMaxTokens    = 32_768
	DefaultAgentMaxContext   = 4 << 20
	DefaultAgentMaxWallTime  = 30 * time.Minute
	DefaultAgentMaxToolRetry = 3
	DefaultAgentMaxCostUSD   = 10.0
	MaxAgentGoalBytes        = 64 << 10
	MaxAgentHistoryMessages  = 256
	MaxAgentHistoryBytes     = 4 << 20
	MaxAgentPendingTools     = 64
)

const (
	AgentRunPending          = "pending"
	AgentRunRunning          = "running"
	AgentRunAwaitingApproval = "awaiting_approval"
	AgentRunAwaitingRetry    = "awaiting_retry"
	AgentRunAwaitingExternal = "awaiting_external"
	AgentRunSucceeded        = "succeeded"
	AgentRunFailed           = "failed"
	AgentRunCancelled        = "cancelled"
	AgentRunDeadLetter       = "deadletter"
)

const (
	AgentPhaseModel = "model"
	AgentPhaseTool  = "tool"
)

const (
	AgentActionAdvanced = "advanced"
	AgentActionWaiting  = "waiting"
	AgentActionTerminal = "terminal"
	AgentActionAborted  = "aborted"
)

const (
	AgentTerminationCompleted    = "completed"
	AgentTerminationBudget       = "budget_exceeded"
	AgentTerminationCancelled    = "cancelled"
	AgentTerminationModelFailure = "model_failure"
	AgentTerminationToolFailure  = "tool_failure"
	AgentTerminationAbstained    = "abstained"
	AgentTerminationStaleFence   = "stale_fence"
	AgentTerminationConflict     = "conflict"
)

const (
	AgentFailureInvalidRequest = "invalid_request"
	AgentFailureBudget         = "budget"
	AgentFailureModel          = "model_failure"
	AgentFailureTool           = "tool_failure"
	AgentFailureApproval       = "approval_required"
	AgentFailureRetry          = "retry_pending"
	AgentFailureCancelled      = "cancelled"
	AgentFailureStaleFence     = "stale_fence"
	AgentFailureConflict       = "conflict"
	AgentFailureWorkspace      = "workspace_isolation"
	AgentFailureAbstained      = "abstained"
	AgentFailureExternal       = "external_completion"
)

const (
	AgentEventCreated           = "agent.created"
	AgentEventContextCompiled   = "agent.context_compiled"
	AgentEventModelCompleted    = "agent.model_completed"
	AgentEventToolCompleted     = "agent.tool_completed"
	AgentEventApprovalWaiting   = "agent.approval_waiting"
	AgentEventRetryWaiting      = "agent.retry_waiting"
	AgentEventCompleted         = "agent.completed"
	AgentEventFailed            = "agent.failed"
	AgentEventCancelled         = "agent.cancelled"
	AgentEventCheckpointed      = "agent.checkpointed"
	AgentEventExternalWaiting   = "agent.external_waiting"
	AgentEventExternalCompleted = "agent.external_completed"
)

// AgentBudget bounds one agent run. The limits are admission controls, not
// estimates: an implementation must stop before it commits work beyond them.
type AgentBudget struct {
	MaxTurns        int     `json:"max_turns"`
	MaxModelSteps   int     `json:"max_model_steps"`
	MaxToolCalls    int     `json:"max_tool_calls"`
	MaxContextBytes int     `json:"max_context_bytes"`
	MaxOutputTokens int     `json:"max_output_tokens"`
	MaxWallTimeMS   int64   `json:"max_wall_time_ms"`
	MaxCostUSD      float64 `json:"max_cost_usd"`
	MaxToolAttempts int     `json:"max_tool_attempts"`
}

// DefaultAgentBudget returns the conservative offline defaults for a run.
func DefaultAgentBudget() AgentBudget {
	return AgentBudget{
		MaxTurns: DefaultAgentMaxTurns, MaxModelSteps: DefaultAgentMaxSteps,
		MaxToolCalls: DefaultAgentMaxToolCalls, MaxContextBytes: DefaultAgentMaxContext,
		MaxOutputTokens: DefaultAgentMaxTokens, MaxWallTimeMS: DefaultAgentMaxWallTime.Milliseconds(),
		MaxCostUSD: DefaultAgentMaxCostUSD, MaxToolAttempts: DefaultAgentMaxToolRetry,
	}
}

// Normalize fills omitted limits and rejects values outside the supported
// execution envelope.
func (b *AgentBudget) Normalize() error {
	defaults := DefaultAgentBudget()
	if b.MaxTurns == 0 {
		b.MaxTurns = defaults.MaxTurns
	}
	if b.MaxModelSteps == 0 {
		b.MaxModelSteps = defaults.MaxModelSteps
	}
	if b.MaxToolCalls == 0 {
		b.MaxToolCalls = defaults.MaxToolCalls
	}
	if b.MaxContextBytes == 0 {
		b.MaxContextBytes = defaults.MaxContextBytes
	}
	if b.MaxOutputTokens == 0 {
		b.MaxOutputTokens = defaults.MaxOutputTokens
	}
	if b.MaxWallTimeMS == 0 {
		b.MaxWallTimeMS = defaults.MaxWallTimeMS
	}
	if b.MaxCostUSD == 0 {
		b.MaxCostUSD = defaults.MaxCostUSD
	}
	if b.MaxToolAttempts == 0 {
		b.MaxToolAttempts = defaults.MaxToolAttempts
	}
	if b.MaxTurns < 1 || b.MaxTurns > 100 || b.MaxModelSteps < 1 || b.MaxModelSteps > 500 || b.MaxToolCalls < 1 || b.MaxToolCalls > 1000 || b.MaxContextBytes < 1 || b.MaxContextBytes > MaxAgentHistoryBytes || b.MaxOutputTokens < 1 || b.MaxOutputTokens > 1<<20 || b.MaxWallTimeMS < 1 || time.Duration(b.MaxWallTimeMS)*time.Millisecond > 24*time.Hour || b.MaxCostUSD < 0 || b.MaxCostUSD > 1_000_000 || b.MaxToolAttempts < 1 || b.MaxToolAttempts > 10 {
		return fmt.Errorf("agent budget is outside supported bounds")
	}
	return nil
}

// AgentRunRequest is the authenticated, idempotent input used to reserve an
// agent run. A task-bound request must carry the worker's current fence.
type AgentRunRequest struct {
	SchemaVersion  int                   `json:"schema_version"`
	RunID          string                `json:"run_id,omitempty"`
	RequestID      string                `json:"request_id,omitempty"`
	IdempotencyKey string                `json:"idempotency_key"`
	CausationID    string                `json:"causation_id,omitempty"`
	CorrelationID  string                `json:"correlation_id,omitempty"`
	WorkspaceID    string                `json:"workspace_id"`
	Actor          ActorRef              `json:"actor,omitempty"`
	Task           *EntityRef            `json:"task,omitempty"`
	Session        *EntityRef            `json:"session,omitempty"`
	TaskOwnerID    string                `json:"task_owner_id,omitempty"`
	TaskFence      uint64                `json:"task_fence,omitempty"`
	Goal           string                `json:"goal"`
	Provider       ProviderRef           `json:"provider"`
	Tools          []ModelToolDefinition `json:"tools,omitempty"`
	Retrieval      *RetrievalRequest     `json:"retrieval,omitempty"`
	Budget         AgentBudget           `json:"budget"`
	Metadata       map[string]string     `json:"metadata,omitempty"`
}

// Normalize assigns request identities, validates workspace references, and
// applies bounded defaults before the request can reach durable storage.
func (r *AgentRunRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("agent run request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = AgentSchemaVersion
	}
	if r.SchemaVersion != AgentSchemaVersion {
		return fmt.Errorf("unsupported agent schema_version %d", r.SchemaVersion)
	}
	r.RunID, r.RequestID, r.IdempotencyKey = strings.TrimSpace(r.RunID), strings.TrimSpace(r.RequestID), strings.TrimSpace(r.IdempotencyKey)
	if r.RunID == "" {
		r.RunID = NewID("run")
	}
	if r.RequestID == "" {
		r.RequestID = NewID("runreq")
	}
	if r.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if len(r.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("idempotency_key is too large")
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	if r.WorkspaceID == "" {
		return fmt.Errorf("workspace_id is required")
	}
	if len(r.Goal) == 0 || len(r.Goal) > MaxAgentGoalBytes {
		return fmt.Errorf("goal must be between 1 and %d bytes", MaxAgentGoalBytes)
	}
	r.Goal = strings.TrimSpace(r.Goal)
	if r.Goal == "" {
		return fmt.Errorf("goal is required")
	}
	if err := normalizeAgentProvider(&r.Provider); err != nil {
		return err
	}
	if r.Task != nil {
		if err := validateModelEntityRef(r.Task, "task", r.WorkspaceID); err != nil {
			return err
		}
	}
	if r.Session != nil {
		if err := validateModelEntityRef(r.Session, "session", r.WorkspaceID); err != nil {
			return err
		}
	}
	if r.Task != nil && (strings.TrimSpace(r.TaskOwnerID) == "" || r.TaskFence == 0) {
		return fmt.Errorf("task-bound runs require task_owner_id and task_fence")
	}
	if err := r.Budget.Normalize(); err != nil {
		return err
	}
	for i := range r.Tools {
		if err := r.Tools[i].Normalize(); err != nil {
			return fmt.Errorf("tools[%d]: %w", i, err)
		}
	}
	if len(r.Tools) > MaxAgentPendingTools {
		return fmt.Errorf("too many agent tools")
	}
	if r.Retrieval != nil {
		retrieval, err := r.Retrieval.Normalize()
		if err != nil {
			return fmt.Errorf("retrieval: %w", err)
		}
		if retrieval.WorkspaceID != r.WorkspaceID {
			return fmt.Errorf("retrieval workspace_id must match agent workspace_id")
		}
		r.Retrieval = &retrieval
	}
	if len(r.Metadata) > MaxModelMetadataEntries {
		return fmt.Errorf("metadata is too large")
	}
	return nil
}

func normalizeAgentProvider(p *ProviderRef) error {
	p.Provider, p.Endpoint, p.Model = strings.ToLower(strings.TrimSpace(p.Provider)), strings.TrimSpace(p.Endpoint), strings.TrimSpace(p.Model)
	if p.Provider == "" {
		return fmt.Errorf("provider.provider is required")
	}
	return nil
}

// RequestHash identifies logical run input while excluding transport and
// retry identities, so the same idempotent request has the same hash.
func (r AgentRunRequest) RequestHash() (string, error) {
	clone := r
	clone.RunID, clone.RequestID, clone.IdempotencyKey, clone.CausationID, clone.CorrelationID = "", "", "", "", ""
	b, err := json.Marshal(clone)
	if err != nil {
		return "", fmt.Errorf("hash agent request: %w", err)
	}
	d := sha256.Sum256(b)
	return hex.EncodeToString(d[:]), nil
}

// PendingToolCall records a tool request that must be resumed from the last
// committed checkpoint rather than inferred from an in-memory loop.
type PendingToolCall struct {
	ID          string          `json:"id"`
	ToolID      string          `json:"tool_id"`
	Arguments   json.RawMessage `json:"arguments"`
	Attempt     int             `json:"attempt"`
	ApprovalID  string          `json:"approval_id,omitempty"`
	LastFailure *LoopFailure    `json:"last_failure,omitempty"`
}

// LoopFailure is the redacted, deterministic failure shape persisted in run
// state. ContentEmitted prevents unsafe retry or provider fallback.
type LoopFailure struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	Phase          string `json:"phase,omitempty"`
	Retryable      bool   `json:"retryable"`
	ContentEmitted bool   `json:"content_emitted"`
	Attempt        int    `json:"attempt,omitempty"`
}

// Normalize validates the stable failure code and message fields.
func (f *LoopFailure) Normalize() error {
	if f == nil {
		return fmt.Errorf("loop failure is nil")
	}
	f.Code, f.Message, f.Phase = strings.TrimSpace(f.Code), strings.TrimSpace(f.Message), strings.TrimSpace(f.Phase)
	if f.Code == "" || f.Message == "" {
		return fmt.Errorf("loop failure code and message are required")
	}
	return nil
}

// ModelStep records one model attempt and its observed usage. Remote model
// execution remains at-least-once; this record makes that boundary explicit.
type ModelStep struct {
	ID             string         `json:"id"`
	RequestID      string         `json:"request_id"`
	IdempotencyKey string         `json:"idempotency_key"`
	Attempt        int            `json:"attempt"`
	Provider       ProviderRef    `json:"provider"`
	Response       *ModelResponse `json:"response,omitempty"`
	Usage          ModelUsage     `json:"usage"`
	Cost           ModelCost      `json:"cost"`
	ContentEmitted bool           `json:"content_emitted"`
	Failure        *LoopFailure   `json:"failure,omitempty"`
	StartedAt      time.Time      `json:"started_at"`
	FinishedAt     time.Time      `json:"finished_at,omitempty"`
}

// ToolStep records one durable tool attempt, including approval and failure
// state needed for crash recovery and duplicate delivery.
type ToolStep struct {
	ID             string       `json:"id"`
	ToolID         string       `json:"tool_id"`
	RequestID      string       `json:"request_id"`
	IdempotencyKey string       `json:"idempotency_key"`
	Attempt        int          `json:"attempt"`
	Status         string       `json:"status"`
	ApprovalID     string       `json:"approval_id,omitempty"`
	Result         *ToolResult  `json:"result,omitempty"`
	Failure        *LoopFailure `json:"failure,omitempty"`
	StartedAt      time.Time    `json:"started_at"`
	FinishedAt     time.Time    `json:"finished_at,omitempty"`
}

// AgentTurn is the bounded unit of model and tool work within an agent run.
type AgentTurn struct {
	Number     int         `json:"number"`
	ModelSteps []ModelStep `json:"model_steps,omitempty"`
	ToolSteps  []ToolStep  `json:"tool_steps,omitempty"`
	Output     string      `json:"output,omitempty"`
	Usage      ModelUsage  `json:"usage"`
	Cost       ModelCost   `json:"cost"`
}

// LoopState is the compact deterministic state-machine view used in a
// checkpoint. It intentionally excludes large history and raw outputs.
type LoopState struct {
	RunID        string            `json:"run_id"`
	WorkspaceID  string            `json:"workspace_id"`
	State        string            `json:"state"`
	Phase        string            `json:"phase"`
	Turn         int               `json:"turn"`
	Step         int               `json:"step"`
	ContextHash  string            `json:"context_hash,omitempty"`
	PendingTools []PendingToolCall `json:"pending_tools,omitempty"`
	Termination  string            `json:"termination,omitempty"`
	NextRetryAt  *time.Time        `json:"next_retry_at,omitempty"`
}

// LoopCheckpoint is the replay anchor committed with the corresponding event
// sequence and state hash.
type LoopCheckpoint struct {
	RunID         string    `json:"run_id"`
	Version       int64     `json:"version"`
	EventSequence uint64    `json:"event_sequence"`
	State         LoopState `json:"state"`
	StateHash     string    `json:"state_hash"`
	HistoryHash   string    `json:"history_hash"`
	CommittedAt   time.Time `json:"committed_at"`
}

// AgentRun is the authoritative durable state of one bounded agent workflow.
// Postgres owns state transitions; projections and replay views are derived.
type AgentRun struct {
	ID                 string                `json:"id"`
	WorkspaceID        string                `json:"workspace_id"`
	RequestID          string                `json:"request_id"`
	IdempotencyKey     string                `json:"idempotency_key"`
	RequestHash        string                `json:"request_hash"`
	SchemaVersion      int                   `json:"schema_version"`
	CausationID        string                `json:"causation_id,omitempty"`
	CorrelationID      string                `json:"correlation_id,omitempty"`
	Actor              ActorRef              `json:"actor,omitempty"`
	Task               *EntityRef            `json:"task,omitempty"`
	Session            *EntityRef            `json:"session,omitempty"`
	TaskOwnerID        string                `json:"task_owner_id,omitempty"`
	TaskFence          uint64                `json:"task_fence,omitempty"`
	Goal               string                `json:"goal"`
	Provider           ProviderRef           `json:"provider"`
	Tools              []ModelToolDefinition `json:"tools,omitempty"`
	Retrieval          *RetrievalRequest     `json:"retrieval,omitempty"`
	Budget             AgentBudget           `json:"budget"`
	ContextHash        string                `json:"context_hash,omitempty"`
	State              string                `json:"state"`
	Phase              string                `json:"phase"`
	Turn               int                   `json:"turn"`
	Step               int                   `json:"step"`
	ModelAttempts      int                   `json:"model_attempts"`
	ModelCalls         int                   `json:"model_calls"`
	ToolCalls          int                   `json:"tool_calls"`
	InputTokens        int                   `json:"input_tokens"`
	OutputTokens       int                   `json:"output_tokens"`
	TotalTokens        int                   `json:"total_tokens"`
	ContextBytes       int                   `json:"context_bytes"`
	Cost               ModelCost             `json:"cost"`
	History            []ModelMessage        `json:"history,omitempty"`
	PendingTools       []PendingToolCall     `json:"pending_tools,omitempty"`
	LastOutput         string                `json:"last_output,omitempty"`
	LastOutputArtifact *ArtifactRef          `json:"last_output_artifact,omitempty"`
	HistoryArtifact    *ArtifactRef          `json:"history_artifact,omitempty"`
	LastFailure        *LoopFailure          `json:"last_failure,omitempty"`
	Termination        string                `json:"termination,omitempty"`
	NextRetryAt        *time.Time            `json:"next_retry_at,omitempty"`
	StateVersion       int64                 `json:"state_version"`
	EventSequence      uint64                `json:"event_sequence"`
	StateHash          string                `json:"state_hash"`
	CreatedAt          time.Time             `json:"created_at"`
	UpdatedAt          time.Time             `json:"updated_at"`
	StartedAt          *time.Time            `json:"started_at,omitempty"`
	FinishedAt         *time.Time            `json:"finished_at,omitempty"`
}

// LoopDecision describes the next deterministic action selected from an
// AgentRun and its current checkpoint.
type LoopDecision struct {
	Action     string         `json:"action"`
	Run        AgentRun       `json:"run"`
	Checkpoint LoopCheckpoint `json:"checkpoint"`
	Model      *ModelStep     `json:"model,omitempty"`
	Tool       *ToolStep      `json:"tool,omitempty"`
	Failure    *LoopFailure   `json:"failure,omitempty"`
}

// StateSnapshot returns the compact state representation used for checkpoint
// hashes and replay comparisons.
func (r AgentRun) StateSnapshot() LoopState {
	return LoopState{RunID: r.ID, WorkspaceID: r.WorkspaceID, State: r.State, Phase: r.Phase, Turn: r.Turn, Step: r.Step, ContextHash: r.ContextHash, PendingTools: clonePendingTools(r.PendingTools), Termination: r.Termination, NextRetryAt: cloneTime(r.NextRetryAt)}
}

// Checkpoint builds the canonical checkpoint representation for the run's
// current committed state.
func (r AgentRun) Checkpoint() LoopCheckpoint {
	return LoopCheckpoint{RunID: r.ID, Version: r.StateVersion, EventSequence: r.EventSequence, State: r.StateSnapshot(), StateHash: r.StateHash, HistoryHash: historyHash(r.History), CommittedAt: r.UpdatedAt}
}

// ComputeStateHash returns the stable hash of state, history, output, usage,
// and cost fields that affect deterministic replay.
func (r AgentRun) ComputeStateHash() string {
	canonical, _ := json.Marshal(struct {
		State struct {
			RunID        string            `json:"run_id"`
			WorkspaceID  string            `json:"workspace_id"`
			State        string            `json:"state"`
			Phase        string            `json:"phase"`
			Turn         int               `json:"turn"`
			Step         int               `json:"step"`
			ContextHash  string            `json:"context_hash,omitempty"`
			PendingTools []PendingToolCall `json:"pending_tools,omitempty"`
			Termination  string            `json:"termination,omitempty"`
		} `json:"state"`
		History []ModelMessage `json:"history"`
		Output  string         `json:"output"`
		Tokens  int            `json:"tokens"`
		Cost    ModelCost      `json:"cost"`
	}{struct {
		RunID        string            `json:"run_id"`
		WorkspaceID  string            `json:"workspace_id"`
		State        string            `json:"state"`
		Phase        string            `json:"phase"`
		Turn         int               `json:"turn"`
		Step         int               `json:"step"`
		ContextHash  string            `json:"context_hash,omitempty"`
		PendingTools []PendingToolCall `json:"pending_tools,omitempty"`
		Termination  string            `json:"termination,omitempty"`
	}{r.ID, r.WorkspaceID, r.State, r.Phase, r.Turn, r.Step, r.ContextHash, clonePendingTools(r.PendingTools), r.Termination}, r.History, r.LastOutput, r.TotalTokens, r.Cost})
	d := sha256.Sum256(canonical)
	return hex.EncodeToString(d[:])
}

func historyHash(history []ModelMessage) string {
	b, _ := json.Marshal(history)
	d := sha256.Sum256(b)
	return hex.EncodeToString(d[:])
}

func clonePendingTools(in []PendingToolCall) []PendingToolCall {
	out := make([]PendingToolCall, len(in))
	for i, value := range in {
		out[i] = value
		out[i].Arguments = append(json.RawMessage(nil), value.Arguments...)
	}
	return out
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

// IsAgentTerminal reports whether no further loop transition is permitted.
func IsAgentTerminal(state string) bool {
	switch strings.TrimSpace(state) {
	case AgentRunSucceeded, AgentRunFailed, AgentRunCancelled, AgentRunDeadLetter:
		return true
	default:
		return false
	}
}
