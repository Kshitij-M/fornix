package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	WorkflowSchemaVersion = 1
	WorkflowDefaultFanOut = 2
	WorkflowMaxFanOut     = 16
	WorkflowMaxOutput     = 16 << 20
	WorkflowMaxTokens     = 1_000_000
	WorkflowMaxCostMicros = 10_000_000
	WorkflowMaxWallTimeMS = int64((24 * time.Hour) / time.Millisecond)
)

const (
	WorkflowStatusCreated          = "created"
	WorkflowStatusRunning          = "running"
	WorkflowStatusAwaitingApproval = "awaiting_approval"
	WorkflowStatusAwaitingHuman    = "awaiting_human_input"
	WorkflowStatusAwaitingCallback = "awaiting_callback"
	WorkflowStatusAwaitingRetry    = "awaiting_retry"
	WorkflowStatusAwaitingExternal = "awaiting_external"
	WorkflowStatusRecoveryRequired = "recovery_required"
	WorkflowStatusSucceeded        = "succeeded"
	WorkflowStatusFailed           = "failed"
	WorkflowStatusCancelled        = "cancelled"
	WorkflowStatusDeadLetter       = "dead_letter"
)

const (
	WorkflowStepModel        = "model_step"
	WorkflowStepTool         = "tool_step"
	WorkflowStepConnector    = "connector_step"
	WorkflowStepApproval     = "approval_step"
	WorkflowStepHumanInput   = "human_input_step"
	WorkflowStepCallback     = "wait_for_callback_step"
	WorkflowStepValidation   = "validation_step"
	WorkflowStepCompensation = "compensation_step"
)

const (
	WorkflowStepPlanned          = "planned"
	WorkflowStepReady            = "ready"
	WorkflowStepRunning          = "running"
	WorkflowStepAwaitingApproval = "awaiting_approval"
	WorkflowStepAwaitingHuman    = "awaiting_human_input"
	WorkflowStepAwaitingCallback = "awaiting_callback"
	WorkflowStepAwaitingRetry    = "awaiting_retry"
	WorkflowStepAwaitingExternal = "awaiting_external"
	WorkflowStepSucceeded        = "succeeded"
	WorkflowStepFailed           = "failed"
	WorkflowStepCancelled        = "cancelled"
	WorkflowStepRecoveryRequired = "recovery_required"
	WorkflowStepCompensating     = "compensating"
	WorkflowStepCompensated      = "compensated"
)

const (
	WorkflowWaitApproval = "approval"
	WorkflowWaitHuman    = "human_input"
	WorkflowWaitCallback = "callback"
	WorkflowWaitTimer    = "timer"
	WorkflowWaitExternal = "external"
	WorkflowWaitRetry    = "retry"
	WorkflowWaitRecovery = "recovery"
)

// IsTerminalWorkflowStatus reports whether a run cannot accept further work.
func IsTerminalWorkflowStatus(status string) bool {
	switch status {
	case WorkflowStatusSucceeded, WorkflowStatusFailed, WorkflowStatusCancelled, WorkflowStatusDeadLetter:
		return true
	default:
		return false
	}
}

// HashStrings provides a small canonical identity helper for idempotency keys.
// NUL separators prevent ambiguous concatenations such as ["ab", "c"] and
// ["a", "bc"] from sharing an identity.
func HashStrings(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// WorkflowBudget is the hard cumulative ceiling for one workflow. Zero cost
// means no monetary ceiling is configured; all other zero values receive safe
// defaults during normalization.
type WorkflowBudget struct {
	MaxSteps        int   `json:"max_steps"`
	MaxParallelRead int   `json:"max_parallel_read"`
	MaxRetries      int   `json:"max_retries"`
	MaxOutputBytes  int64 `json:"max_output_bytes"`
	MaxTokens       int64 `json:"max_tokens"`
	MaxWallTimeMS   int64 `json:"max_wall_time_ms"`
	MaxCostMicros   int64 `json:"max_cost_micros,omitempty"`
}

func DefaultWorkflowBudget() WorkflowBudget {
	return WorkflowBudget{MaxSteps: MaxOperationSteps, MaxParallelRead: WorkflowDefaultFanOut, MaxRetries: 2, MaxOutputBytes: WorkflowMaxOutput, MaxTokens: WorkflowMaxTokens, MaxWallTimeMS: WorkflowMaxWallTimeMS}
}

func (b *WorkflowBudget) Normalize() error {
	if b == nil {
		return fmt.Errorf("workflow budget is nil")
	}
	defaults := DefaultWorkflowBudget()
	if b.MaxSteps == 0 {
		b.MaxSteps = defaults.MaxSteps
	}
	if b.MaxParallelRead == 0 {
		b.MaxParallelRead = defaults.MaxParallelRead
	}
	if b.MaxRetries == 0 {
		b.MaxRetries = defaults.MaxRetries
	}
	if b.MaxOutputBytes == 0 {
		b.MaxOutputBytes = defaults.MaxOutputBytes
	}
	if b.MaxTokens == 0 {
		b.MaxTokens = defaults.MaxTokens
	}
	if b.MaxWallTimeMS == 0 {
		b.MaxWallTimeMS = defaults.MaxWallTimeMS
	}
	if b.MaxSteps < 1 || b.MaxSteps > MaxOperationSteps || b.MaxParallelRead < 1 || b.MaxParallelRead > WorkflowMaxFanOut || b.MaxRetries < 0 || b.MaxRetries > 1024 || b.MaxOutputBytes < 1 || b.MaxOutputBytes > WorkflowMaxOutput || b.MaxTokens < 1 || b.MaxTokens > WorkflowMaxTokens || b.MaxWallTimeMS < 1 || b.MaxWallTimeMS > WorkflowMaxWallTimeMS || b.MaxCostMicros < 0 || b.MaxCostMicros > WorkflowMaxCostMicros {
		return fmt.Errorf("workflow budget is outside bounds")
	}
	return nil
}

// WorkflowWait is the redacted durable reason a run cannot advance. Token is
// an opaque hash/idempotency identity; raw prompts, secrets, and user text do
// not belong in the workflow authority.
type WorkflowWait struct {
	Kind      string     `json:"kind"`
	Token     string     `json:"token"`
	Reason    string     `json:"reason,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func (w *WorkflowWait) Normalize(workspaceID string) error {
	if w == nil {
		return nil
	}
	w.Kind = strings.ToLower(strings.TrimSpace(w.Kind))
	switch w.Kind {
	case WorkflowWaitApproval, WorkflowWaitHuman, WorkflowWaitCallback, WorkflowWaitTimer, WorkflowWaitExternal, WorkflowWaitRetry, WorkflowWaitRecovery:
	default:
		return fmt.Errorf("unsupported workflow wait kind %q", w.Kind)
	}
	if w.Token == "" || len(w.Token) > MaxIdempotencyLength || strings.ContainsAny(w.Token, "\x00\r\n") {
		return fmt.Errorf("workflow wait token is invalid")
	}
	if len(w.Reason) > MaxOperationFailureText || strings.ContainsAny(w.Reason, "\x00\r\n") {
		return fmt.Errorf("workflow wait reason is invalid")
	}
	if w.ExpiresAt != nil && w.ExpiresAt.IsZero() {
		w.ExpiresAt = nil
	}
	_ = workspaceID
	return nil
}

// WorkflowFailure is a bounded, replay-safe classification. Detailed errors
// belong in redacted evidence or operator diagnostics, not state authority.
type WorkflowFailure struct {
	Code      string `json:"code"`
	Message   string `json:"message,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
	Attempt   int    `json:"attempt,omitempty"`
	External  bool   `json:"external_effect_started,omitempty"`
}

func (f *WorkflowFailure) Normalize() error {
	if f == nil {
		return nil
	}
	f.Code = strings.ToLower(strings.TrimSpace(f.Code))
	if f.Code == "" || len(f.Code) > MaxDomainNameLength || strings.ContainsAny(f.Code, "\x00\r\n") {
		return fmt.Errorf("workflow failure code is invalid")
	}
	if len(f.Message) > MaxOperationFailureText || strings.ContainsAny(f.Message, "\x00\r\n") {
		return fmt.Errorf("workflow failure message is invalid")
	}
	if f.Attempt < 0 || f.Attempt > 1024 {
		return fmt.Errorf("workflow failure attempt is invalid")
	}
	return nil
}

// WorkflowStepResult is the bounded output supplied by a step executor. It
// contains hashes and accounting only; raw output is stored by the existing
// artifact/evidence authorities when disclosure is explicitly requested.
type WorkflowStepResult struct {
	Status                string                 `json:"status"`
	OutputHash            string                 `json:"output_hash,omitempty"`
	Evidence              []OperationEvidenceRef `json:"evidence,omitempty"`
	Artifacts             []ArtifactRef          `json:"artifacts,omitempty"`
	Effect                *ExternalEffect        `json:"effect,omitempty"`
	Wait                  *WorkflowWait          `json:"wait,omitempty"`
	Failure               *WorkflowFailure       `json:"failure,omitempty"`
	OutputBytes           int64                  `json:"output_bytes,omitempty"`
	Tokens                int64                  `json:"tokens,omitempty"`
	CostMicros            int64                  `json:"cost_micros,omitempty"`
	ContentEmitted        bool                   `json:"content_emitted,omitempty"`
	ExternalEffectStarted bool                   `json:"external_effect_started,omitempty"`
}

func (r *WorkflowStepResult) Normalize(workspaceID string) error {
	if r == nil {
		return fmt.Errorf("workflow step result is nil")
	}
	r.Status = strings.ToLower(strings.TrimSpace(r.Status))
	if r.Status != WorkflowStepSucceeded && r.Status != WorkflowStepFailed && r.Status != WorkflowStepAwaitingApproval && r.Status != WorkflowStepAwaitingHuman && r.Status != WorkflowStepAwaitingCallback && r.Status != WorkflowStepAwaitingExternal && r.Status != WorkflowStepAwaitingRetry && r.Status != WorkflowStepRecoveryRequired {
		return fmt.Errorf("unsupported workflow step result status %q", r.Status)
	}
	if r.OutputHash != "" {
		if _, err := normalizeDomainHash(r.OutputHash, "workflow output_hash", false); err != nil {
			return err
		}
	}
	if len(r.Evidence) > MaxDomainReferences || len(r.Artifacts) > MaxDomainReferences {
		return fmt.Errorf("workflow step references exceed bounds")
	}
	for i := range r.Evidence {
		if err := r.Evidence[i].Normalize(); err != nil {
			return err
		}
		if r.Evidence[i].WorkspaceID != workspaceID {
			return fmt.Errorf("workflow evidence crosses workspace")
		}
	}
	for i := range r.Artifacts {
		if err := normalizeWorkflowArtifactRef(&r.Artifacts[i], workspaceID); err != nil {
			return err
		}
	}
	if r.Effect != nil {
		if err := r.Effect.Normalize(); err != nil {
			return err
		}
	}
	if err := r.Wait.Normalize(workspaceID); err != nil {
		return err
	}
	if r.Wait != nil {
		if r.Status == WorkflowStepAwaitingRetry && r.Wait.Kind != WorkflowWaitRetry {
			return fmt.Errorf("awaiting_retry result requires a retry wait")
		}
		if r.Status != WorkflowStepAwaitingRetry && r.Wait.Kind == WorkflowWaitRetry {
			return fmt.Errorf("retry wait requires awaiting_retry result")
		}
	}
	if err := r.Failure.Normalize(); err != nil {
		return err
	}
	if r.OutputBytes < 0 || r.OutputBytes > WorkflowMaxOutput || r.Tokens < 0 || r.Tokens > WorkflowMaxTokens || r.CostMicros < 0 || r.CostMicros > WorkflowMaxCostMicros {
		return fmt.Errorf("workflow step accounting is outside bounds")
	}
	return nil
}

// WorkflowStepState is the current bounded projection of a plan step.
type WorkflowStepState struct {
	WorkspaceID    string                 `json:"workspace_id"`
	RunID          string                 `json:"run_id"`
	StepID         string                 `json:"step_id"`
	Ordinal        int                    `json:"ordinal"`
	Kind           string                 `json:"kind"`
	Status         string                 `json:"status"`
	Attempt        int                    `json:"attempt"`
	IdempotencyKey string                 `json:"idempotency_key"`
	OutputHash     string                 `json:"output_hash,omitempty"`
	Evidence       []OperationEvidenceRef `json:"evidence,omitempty"`
	Artifacts      []ArtifactRef          `json:"artifacts,omitempty"`
	Effect         *ExternalEffect        `json:"effect,omitempty"`
	Wait           *WorkflowWait          `json:"wait,omitempty"`
	Failure        *WorkflowFailure       `json:"failure,omitempty"`
	NextRetryAt    *time.Time             `json:"next_retry_at,omitempty"`
	StateVersion   int64                  `json:"state_version"`
	StateHash      string                 `json:"state_hash"`
}

func (s *WorkflowStepState) Normalize(workspaceID string) error {
	if s == nil {
		return fmt.Errorf("workflow step state is nil")
	}
	if s.WorkspaceID != workspaceID || s.RunID == "" || s.StepID == "" || s.Kind == "" || s.Ordinal < 0 || s.Ordinal >= MaxOperationSteps || s.Attempt < 0 || s.Attempt > 1024 || s.StateVersion < 0 {
		return fmt.Errorf("workflow step state identity is invalid")
	}
	if !validWorkflowStepStatus(s.Status) || len(s.IdempotencyKey) > MaxIdempotencyLength || strings.ContainsAny(s.IdempotencyKey, "\x00\r\n") {
		return fmt.Errorf("workflow step state status or idempotency is invalid")
	}
	if s.OutputHash != "" {
		if _, err := normalizeDomainHash(s.OutputHash, "workflow step output_hash", false); err != nil {
			return err
		}
	}
	if len(s.Evidence) > MaxDomainReferences || len(s.Artifacts) > MaxDomainReferences {
		return fmt.Errorf("workflow step references exceed bounds")
	}
	for i := range s.Evidence {
		if err := s.Evidence[i].Normalize(); err != nil {
			return err
		}
		if s.Evidence[i].WorkspaceID != workspaceID {
			return fmt.Errorf("workflow step evidence crosses workspace")
		}
	}
	for i := range s.Artifacts {
		if err := normalizeWorkflowArtifactRef(&s.Artifacts[i], workspaceID); err != nil {
			return err
		}
	}
	if s.Effect != nil {
		if err := s.Effect.Normalize(); err != nil {
			return err
		}
	}
	if err := s.Wait.Normalize(workspaceID); err != nil {
		return err
	}
	if err := s.Failure.Normalize(); err != nil {
		return err
	}
	if s.StateHash != "" {
		if _, err := normalizeDomainHash(s.StateHash, "workflow step state_hash", true); err != nil {
			return err
		}
	}
	return nil
}

// WorkflowRun is the durable current workflow projection. Plan and operation
// history remain authoritative inputs; Steps is a bounded convenience view.
type WorkflowRun struct {
	SchemaVersion  int                 `json:"schema_version"`
	WorkspaceID    string              `json:"workspace_id"`
	ID             string              `json:"id"`
	Operation      OperationReference  `json:"operation"`
	Plan           OperationPlan       `json:"plan"`
	PlanHash       string              `json:"plan_hash"`
	Actor          ActorRef            `json:"actor"`
	Task           *EntityRef          `json:"task,omitempty"`
	Session        *EntityRef          `json:"session,omitempty"`
	Status         string              `json:"status"`
	Budget         WorkflowBudget      `json:"budget"`
	Steps          []WorkflowStepState `json:"steps"`
	Wait           *WorkflowWait       `json:"wait,omitempty"`
	Failure        *WorkflowFailure    `json:"failure,omitempty"`
	TerminalReason string              `json:"terminal_reason,omitempty"`
	StateVersion   int64               `json:"state_version"`
	StateHash      string              `json:"state_hash"`
	OutputBytes    int64               `json:"output_bytes"`
	Tokens         int64               `json:"tokens"`
	CostMicros     int64               `json:"cost_micros"`
	CreatedAt      time.Time           `json:"created_at"`
	UpdatedAt      time.Time           `json:"updated_at"`
}

func (r *WorkflowRun) Normalize() error {
	if r == nil {
		return fmt.Errorf("workflow run is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = WorkflowSchemaVersion
	}
	if r.SchemaVersion != WorkflowSchemaVersion {
		return fmt.Errorf("unsupported workflow schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	if r.ID == "" || len(r.ID) > MaxDomainIDLength {
		return fmt.Errorf("workflow run id is invalid")
	}
	if err := r.Operation.Normalize(); err != nil {
		return err
	}
	if r.Operation.WorkspaceID != workspace {
		return fmt.Errorf("workflow operation crosses workspace")
	}
	if err := r.Plan.Normalize(); err != nil {
		return err
	}
	if r.Plan.WorkspaceID != workspace || r.PlanHash != r.Plan.StableHash() {
		return fmt.Errorf("workflow plan hash is invalid")
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	if err := normalizeDomainEntity(r.Task, "task", workspace); err != nil {
		return err
	}
	if err := normalizeDomainEntity(r.Session, "session", workspace); err != nil {
		return err
	}
	if err := r.Budget.Normalize(); err != nil {
		return err
	}
	if !validWorkflowStatus(r.Status) || len(r.Steps) != len(r.Plan.Steps) || len(r.Steps) > r.Budget.MaxSteps {
		return fmt.Errorf("workflow run status or step count is invalid")
	}
	for i := range r.Steps {
		if err := r.Steps[i].Normalize(workspace); err != nil {
			return err
		}
		if r.Steps[i].StepID != r.Plan.Steps[i].ID || r.Steps[i].Ordinal != r.Plan.Steps[i].Ordinal {
			return fmt.Errorf("workflow step projection does not match plan")
		}
	}
	if err := r.Wait.Normalize(workspace); err != nil {
		return err
	}
	if err := r.Failure.Normalize(); err != nil {
		return err
	}
	if r.StateHash != "" {
		if _, err := normalizeDomainHash(r.StateHash, "workflow state_hash", true); err != nil {
			return err
		}
	}
	if r.OutputBytes < 0 || r.OutputBytes > r.Budget.MaxOutputBytes || r.Tokens < 0 || r.Tokens > r.Budget.MaxTokens || r.CostMicros < 0 || (r.Budget.MaxCostMicros > 0 && r.CostMicros > r.Budget.MaxCostMicros) {
		return fmt.Errorf("workflow accounting exceeds budget")
	}
	return nil
}

func (r WorkflowRun) StableHash() string {
	clone := r
	clone.StateVersion, clone.StateHash, clone.CreatedAt, clone.UpdatedAt = 0, "", time.Time{}, time.Time{}
	for i := range clone.Steps {
		clone.Steps[i].StateVersion, clone.Steps[i].StateHash = 0, ""
	}
	raw, err := json.Marshal(clone)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// WorkflowCheckpoint is the immutable hash input recorded at each commit.
type WorkflowCheckpoint struct {
	WorkspaceID    string              `json:"workspace_id"`
	RunID          string              `json:"run_id"`
	Version        int64               `json:"version"`
	PreviousHash   string              `json:"previous_hash"`
	StateHash      string              `json:"state_hash"`
	Status         string              `json:"status"`
	StepStates     []WorkflowStepState `json:"step_states"`
	Wait           *WorkflowWait       `json:"wait,omitempty"`
	Failure        *WorkflowFailure    `json:"failure,omitempty"`
	TerminalReason string              `json:"terminal_reason,omitempty"`
	OutputBytes    int64               `json:"output_bytes"`
	Tokens         int64               `json:"tokens"`
	CostMicros     int64               `json:"cost_micros"`
}

func (c WorkflowCheckpoint) StableHash() string {
	raw, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func validWorkflowStatus(status string) bool {
	switch status {
	case WorkflowStatusCreated, WorkflowStatusRunning, WorkflowStatusAwaitingApproval, WorkflowStatusAwaitingHuman, WorkflowStatusAwaitingCallback, WorkflowStatusAwaitingRetry, WorkflowStatusAwaitingExternal, WorkflowStatusRecoveryRequired, WorkflowStatusSucceeded, WorkflowStatusFailed, WorkflowStatusCancelled, WorkflowStatusDeadLetter:
		return true
	}
	return false
}
func validWorkflowStepStatus(status string) bool {
	switch status {
	case WorkflowStepPlanned, WorkflowStepReady, WorkflowStepRunning, WorkflowStepAwaitingApproval, WorkflowStepAwaitingHuman, WorkflowStepAwaitingCallback, WorkflowStepAwaitingRetry, WorkflowStepAwaitingExternal, WorkflowStepSucceeded, WorkflowStepFailed, WorkflowStepCancelled, WorkflowStepRecoveryRequired, WorkflowStepCompensating, WorkflowStepCompensated:
		return true
	}
	return false
}

func normalizeWorkflowArtifactRef(ref *ArtifactRef, workspaceID string) error {
	if ref == nil || ref.WorkspaceID != workspaceID || ref.ID < 1 || ref.ArtifactID < 1 || ref.ContentHash == "" || ref.SourceKind == "" || ref.SourceID == "" {
		return fmt.Errorf("workflow artifact reference is invalid or crosses workspace")
	}
	if _, err := normalizeDomainHash(ref.ContentHash, "workflow artifact content_hash", true); err != nil {
		return err
	}
	if ref.ByteSize < 0 || len(ref.SourceKind) > MaxDomainNameLength || len(ref.SourceID) > MaxDomainIDLength {
		return fmt.Errorf("workflow artifact reference is out of bounds")
	}
	return nil
}

// WorkflowRunnableSteps returns canonical ready steps. It never returns an
// effectful step alongside another step sharing its resource identity.
func (r WorkflowRun) WorkflowRunnableSteps() []WorkflowStepState {
	completed := map[string]bool{}
	for _, step := range r.Steps {
		completed[step.StepID] = step.Status == WorkflowStepSucceeded || step.Status == WorkflowStepCompensated
	}
	ready := make([]WorkflowStepState, 0, r.Budget.MaxParallelRead)
	for index, step := range r.Steps {
		if step.Status != WorkflowStepPlanned && step.Status != WorkflowStepReady {
			continue
		}
		planStep := r.Plan.Steps[index]
		ok := true
		for _, dep := range planStep.DependsOn {
			if !completed[dep] {
				ok = false
				break
			}
		}
		if ok {
			if len(ready) > 0 {
				planEffect := r.Plan.Steps[index].Effect
				if planEffect != EffectClassReadOnly && planEffect != EffectClassObservation {
					continue
				}
			}
			step.Status = WorkflowStepReady
			ready = append(ready, step)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Ordinal != ready[j].Ordinal {
			return ready[i].Ordinal < ready[j].Ordinal
		}
		return ready[i].StepID < ready[j].StepID
	})
	if len(ready) > r.Budget.MaxParallelRead {
		ready = ready[:r.Budget.MaxParallelRead]
	}
	return ready
}
