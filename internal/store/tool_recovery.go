package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
)

var (
	ErrToolRecoveryConflict = errors.New("tool recovery conflicts with durable state")
	ErrToolRecoveryPending  = errors.New("tool attempt has no completed recoverable outcome")
)

// ToolSandboxRecoveryFinalizeResult is the authoritative local result after
// one sandbox observation has been reconciled.
type ToolSandboxRecoveryFinalizeResult struct {
	Run       contracts.ToolRun
	Effect    EffectState
	Link      contracts.DomainEffectLink
	Duplicate bool
}

// ToolSandboxRecoveryPrepareResult is the canonical state after a recovery
// worker has fenced and normalized an interrupted dispatch for inspection.
type ToolSandboxRecoveryPrepareResult struct {
	Run       contracts.ToolRun
	Effect    EffectState
	Link      DomainEffectLinkCurrent
	Duplicate bool
}

// ToolSandboxRecoveryCoordinator commits a completed sandbox observation,
// generic effect/link transitions, output artifacts, and the terminal tool
// event in one Postgres transaction. It never contacts a sandbox provider.
type ToolSandboxRecoveryCoordinator struct {
	pool        *pgxpool.Pool
	toolRuns    *ToolRunStore
	admission   *AdmissionStore
	links       *DomainEffectLinkStore
	failureHook func(string) error
}

func NewToolSandboxRecoveryCoordinator(pool *pgxpool.Pool, toolRuns *ToolRunStore, admission *AdmissionStore, links *DomainEffectLinkStore) *ToolSandboxRecoveryCoordinator {
	return &ToolSandboxRecoveryCoordinator{pool: pool, toolRuns: toolRuns, admission: admission, links: links}
}

// SetFailureHook injects transaction failures for crash-recovery tests.
func (c *ToolSandboxRecoveryCoordinator) SetFailureHook(hook func(string) error) {
	if c != nil {
		c.failureHook = hook
	}
}

func (c *ToolSandboxRecoveryCoordinator) fail(point string) error {
	if c == nil || c.failureHook == nil {
		return nil
	}
	return c.failureHook(point)
}

// Prepare moves one interrupted, possibly-started sandbox attempt into the
// recovery-required triple under a fresh effect lease. It never invokes the
// sandbox. Known pre-finalization states are reconciled atomically so a crash
// between generic dispatch state and the specialized tool row is recoverable.
func (c *ToolSandboxRecoveryCoordinator) Prepare(ctx context.Context, workspaceID, toolRunID, requestID, ownerID string, fence uint64, expectedEffectVersion, expectedLinkVersion int64, actor contracts.ActorRef) (ToolSandboxRecoveryPrepareResult, error) {
	if c == nil || c.pool == nil || c.toolRuns == nil || c.admission == nil || c.links == nil {
		return ToolSandboxRecoveryPrepareResult{}, fmt.Errorf("tool sandbox recovery coordinator is not configured")
	}
	if strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(toolRunID) == "" || strings.TrimSpace(requestID) == "" || strings.TrimSpace(ownerID) == "" || fence == 0 || expectedEffectVersion < 1 || expectedLinkVersion < 1 {
		return ToolSandboxRecoveryPrepareResult{}, ErrToolRecoveryConflict
	}
	tx, err := beginWorkspaceTx(ctx, c.pool, workspaceID)
	if err != nil {
		return ToolSandboxRecoveryPrepareResult{}, fmt.Errorf("begin tool recovery preparation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var idempotencyKey string
	if err := tx.QueryRow(ctx, `SELECT idempotency_key FROM fornix.tool_runs WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, workspaceID, toolRunID).Scan(&idempotencyKey); errors.Is(err, pgx.ErrNoRows) {
		return ToolSandboxRecoveryPrepareResult{}, ErrToolRunMissing
	} else if err != nil {
		return ToolSandboxRecoveryPrepareResult{}, err
	}
	run, err := readToolRunTx(ctx, tx, workspaceID, idempotencyKey)
	if err != nil {
		return ToolSandboxRecoveryPrepareResult{}, err
	}
	if run.ID != toolRunID || run.WorkspaceID != workspaceID || (run.Status != contracts.ToolRunRunning && run.Status != contracts.ToolRunRecoveryRequired) {
		return ToolSandboxRecoveryPrepareResult{}, ErrToolRecoveryPending
	}
	link, err := readDomainEffectLinkByDomainTx(ctx, tx, workspaceID, contracts.DomainEffectKindToolRun, run.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		return ToolSandboxRecoveryPrepareResult{}, err
	}
	currentLink, err := c.links.CurrentTx(ctx, tx, workspaceID, link.ID)
	if err != nil {
		return ToolSandboxRecoveryPrepareResult{}, err
	}
	effect, err := readEffectState(ctx, tx, workspaceID, link.EffectID, true)
	if err != nil {
		return ToolSandboxRecoveryPrepareResult{}, err
	}
	if link.WorkspaceID != workspaceID || link.OperationID != effect.OperationID || link.EffectID != effect.EffectID || link.DomainKind != contracts.DomainEffectKindToolRun || link.DomainID != run.ID || link.DomainHash != run.RequestHash {
		return ToolSandboxRecoveryPrepareResult{}, ErrToolRecoveryConflict
	}
	if effect.Version != expectedEffectVersion || currentLink.Transition.Version != expectedLinkVersion {
		return ToolSandboxRecoveryPrepareResult{}, ErrEffectLeaseFenced
	}
	if run.Status == contracts.ToolRunRecoveryRequired && effect.State == contracts.ExternalEffectRecoveryRequired && currentLink.Transition.ToStatus == contracts.DomainEffectLinkStatusRecoveryRequired {
		if _, err := validateEffectLease(ctx, tx, EffectLease{WorkspaceID: workspaceID, EffectID: effect.EffectID, OwnerID: ownerID, Fence: fence}); err != nil {
			return ToolSandboxRecoveryPrepareResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ToolSandboxRecoveryPrepareResult{}, err
		}
		return ToolSandboxRecoveryPrepareResult{Run: run, Effect: effect, Link: currentLink, Duplicate: true}, nil
	}
	if (effect.State != contracts.ExternalEffectDispatching && effect.State != contracts.ExternalEffectDispatched && effect.State != contracts.ExternalEffectAcknowledged && effect.State != contracts.ExternalEffectRecoveryRequired) ||
		(currentLink.Transition.ToStatus != contracts.DomainEffectLinkStatusLinked && currentLink.Transition.ToStatus != contracts.DomainEffectLinkStatusRecoveryRequired) {
		return ToolSandboxRecoveryPrepareResult{}, ErrToolRecoveryPending
	}
	if _, err := validateEffectLease(ctx, tx, EffectLease{WorkspaceID: workspaceID, EffectID: effect.EffectID, OwnerID: ownerID, Fence: fence}); err != nil {
		return ToolSandboxRecoveryPrepareResult{}, err
	}
	if effect.State != contracts.ExternalEffectRecoveryRequired {
		updatedEffect, updateErr := c.admission.UpdateEffectTx(ctx, tx, contracts.ExternalEffectUpdate{
			WorkspaceID: workspaceID, OperationID: link.OperationID, EffectID: link.EffectID,
			OwnerID: ownerID, Fence: fence, LeaseKind: "effect", RequestID: requestID,
			IdempotencyKey: toolRecoveryKey("prepare:"+run.ID+":"+fmt.Sprint(effect.Version), "effect"),
			State:          contracts.ExternalEffectRecoveryRequired, ProviderRequestID: effect.ProviderRequestID,
			ResponseHash: effect.ResponseHash, FailureCode: "sandbox_recovery",
		})
		if updateErr != nil {
			return ToolSandboxRecoveryPrepareResult{}, updateErr
		}
		effect = updatedEffect.State
	}
	if err := c.fail("tool_recovery_prepare_after_effect"); err != nil {
		return ToolSandboxRecoveryPrepareResult{}, err
	}
	if currentLink.Transition.ToStatus == contracts.DomainEffectLinkStatusLinked {
		transition, transitionErr := c.links.TransitionTx(ctx, tx, contracts.DomainEffectLinkTransitionRequest{
			WorkspaceID: workspaceID, LinkID: link.ID, ExpectedVersion: currentLink.Transition.Version,
			FromStatus: contracts.DomainEffectLinkStatusLinked, ToStatus: contracts.DomainEffectLinkStatusRecoveryRequired,
			FailureCode: "sandbox_recovery", IdempotencyKey: toolRecoveryKey("prepare:"+run.ID+":"+fmt.Sprint(currentLink.Transition.Version), "link"), Actor: actor,
		})
		if transitionErr != nil {
			return ToolSandboxRecoveryPrepareResult{}, transitionErr
		}
		currentLink = DomainEffectLinkCurrent{Link: transition.Link, Transition: transition.Transition}
	}
	if err := c.fail("tool_recovery_prepare_after_link"); err != nil {
		return ToolSandboxRecoveryPrepareResult{}, err
	}
	if run.Status != contracts.ToolRunRecoveryRequired {
		recoveryResult := contracts.ToolResult{
			RequestID: run.RequestID, RunID: run.ID, ToolID: run.ToolID,
			Status:  contracts.ToolRunRecoveryRequired,
			Failure: &contracts.ToolFailure{Code: contracts.ToolFailureExternalUncertain, Message: "external tool outcome requires reconciliation"},
		}
		run, err = c.toolRuns.finishToolResultTx(ctx, tx, run, recoveryResult, &actor, "tool-recovery-required:"+run.ID, map[string]any{"recovery_prepared": true}, false)
		if err != nil {
			return ToolSandboxRecoveryPrepareResult{}, err
		}
	}
	if err := c.fail("tool_recovery_prepare_before_commit"); err != nil {
		return ToolSandboxRecoveryPrepareResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ToolSandboxRecoveryPrepareResult{}, fmt.Errorf("commit tool recovery preparation: %w", err)
	}
	return ToolSandboxRecoveryPrepareResult{Run: run, Effect: effect, Link: currentLink}, nil
}

// Finalize atomically publishes a completed attempt under the active effect
// recovery lease. Replayed requests are read-only and must match every
// already-committed result/effect/link hash.
func (c *ToolSandboxRecoveryCoordinator) Finalize(ctx context.Context, command contracts.ToolRecoveryFinalizeRequest) (ToolSandboxRecoveryFinalizeResult, error) {
	if c == nil || c.pool == nil || c.toolRuns == nil || c.admission == nil || c.links == nil {
		return ToolSandboxRecoveryFinalizeResult{}, fmt.Errorf("tool sandbox recovery coordinator is not configured")
	}
	if err := command.Normalize(); err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	result := *command.Observation.Result
	observedResultHash := result.Hash()
	if result.ContentHash != "" && result.ContentHash != observedResultHash {
		return ToolSandboxRecoveryFinalizeResult{}, ErrToolRecoveryConflict
	}
	if result.RequestID != "" && result.RequestID != command.ToolRequestID {
		return ToolSandboxRecoveryFinalizeResult{}, ErrToolRecoveryConflict
	}
	if result.RunID != "" && result.RunID != command.ToolRunID {
		return ToolSandboxRecoveryFinalizeResult{}, ErrToolRecoveryConflict
	}
	// Redact credential-shaped output before any result, artifact, or event
	// persistence. The durable content hash remains the provider-observed hash.
	rawResult, err := json.Marshal(result)
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	redacted := model.RedactUnboundedBytes(rawResult)
	if err := json.Unmarshal(redacted, &result); err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	result.ContentHash = observedResultHash
	result.RunID, result.RequestID = command.ToolRunID, command.ToolRequestID

	tx, err := beginWorkspaceTx(ctx, c.pool, command.WorkspaceID)
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, fmt.Errorf("begin tool sandbox recovery: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var idempotencyKey string
	if err := tx.QueryRow(ctx, `SELECT idempotency_key FROM fornix.tool_runs WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, command.WorkspaceID, command.ToolRunID).Scan(&idempotencyKey); errors.Is(err, pgx.ErrNoRows) {
		return ToolSandboxRecoveryFinalizeResult{}, ErrToolRunMissing
	} else if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	run, err := readToolRunTx(ctx, tx, command.WorkspaceID, idempotencyKey)
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	if run.ID != command.ToolRunID || run.WorkspaceID != command.WorkspaceID || run.RequestID != command.ToolRequestID || (result.ToolID != "" && result.ToolID != run.ToolID) {
		return ToolSandboxRecoveryFinalizeResult{}, ErrToolRecoveryConflict
	}
	link, err := readDomainEffectLinkByDomainTx(ctx, tx, command.WorkspaceID, contracts.DomainEffectKindToolRun, run.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	currentLink, err := c.links.CurrentTx(ctx, tx, command.WorkspaceID, link.ID)
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	effect, err := readEffectState(ctx, tx, command.WorkspaceID, link.EffectID, true)
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	if err := validateToolRecoveryIdentity(run, currentLink, effect, command); err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}

	if contracts.IsToolTerminal(run.Status) {
		if run.Result == nil || run.Result.Status != result.Status || run.Result.ContentHash != observedResultHash ||
			currentLink.Transition.ToStatus != contracts.DomainEffectLinkStatusReconciled || currentLink.Transition.IdempotencyKey != toolRecoveryKey(command.IdempotencyKey, "link") || currentLink.Transition.ResultHash != observedResultHash ||
			effect.State != contracts.ExternalEffectVerified || effect.ResponseHash != observedResultHash {
			return ToolSandboxRecoveryFinalizeResult{}, ErrToolRecoveryConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return ToolSandboxRecoveryFinalizeResult{}, err
		}
		return ToolSandboxRecoveryFinalizeResult{Run: run, Effect: effect, Link: currentLink.Link, Duplicate: true}, nil
	}
	if run.Status != contracts.ToolRunRecoveryRequired || effect.State != contracts.ExternalEffectRecoveryRequired || currentLink.Transition.ToStatus != contracts.DomainEffectLinkStatusRecoveryRequired {
		return ToolSandboxRecoveryFinalizeResult{}, ErrToolRecoveryPending
	}
	if effect.Version != command.ExpectedEffectVer || currentLink.Transition.Version != command.ExpectedLinkVersion {
		return ToolSandboxRecoveryFinalizeResult{}, ErrEffectLeaseFenced
	}
	if effect.ResponseHash != "" && effect.ResponseHash != observedResultHash {
		return ToolSandboxRecoveryFinalizeResult{}, ErrToolRecoveryConflict
	}

	_, err = c.admission.UpdateEffectTx(ctx, tx, contracts.ExternalEffectUpdate{
		WorkspaceID: command.WorkspaceID, OperationID: link.OperationID, EffectID: link.EffectID,
		OwnerID: command.OwnerID, Fence: command.Fence, LeaseKind: "effect",
		RequestID: command.RequestID, IdempotencyKey: toolRecoveryKey(command.IdempotencyKey, "pending"),
		State: contracts.ExternalEffectVerificationPending,
	})
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	if err := c.fail("tool_recovery_after_pending"); err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	verificationHash := contracts.HashStrings("fornix.sandbox.recovery.v1", command.Identity.StableHash(), command.Observation.IdentityHash, command.Observation.ResultHash, command.Observation.State)
	final, err := c.admission.UpdateEffectTx(ctx, tx, contracts.ExternalEffectUpdate{
		WorkspaceID: command.WorkspaceID, OperationID: link.OperationID, EffectID: link.EffectID,
		OwnerID: command.OwnerID, Fence: command.Fence, LeaseKind: "effect",
		RequestID: command.RequestID, IdempotencyKey: toolRecoveryKey(command.IdempotencyKey, "effect"),
		State: contracts.ExternalEffectVerified, ResponseHash: observedResultHash, VerificationHash: verificationHash,
	})
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	if err := c.fail("tool_recovery_after_effect"); err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	linkResult, err := c.links.TransitionTx(ctx, tx, contracts.DomainEffectLinkTransitionRequest{
		WorkspaceID: command.WorkspaceID, LinkID: link.ID, ExpectedVersion: currentLink.Transition.Version,
		FromStatus: contracts.DomainEffectLinkStatusRecoveryRequired, ToStatus: contracts.DomainEffectLinkStatusReconciled,
		ResultHash: observedResultHash, IdempotencyKey: toolRecoveryKey(command.IdempotencyKey, "link"), Actor: command.Actor,
	})
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	if err := c.fail("tool_recovery_after_link"); err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	identityHash := command.Identity.StableHash()
	eventKey := "tool-recovered-result:" + contracts.HashStrings(command.IdempotencyKey)[:40] + ":" + run.ID
	metadata := map[string]any{"recovered": true, "recovery_identity_hash": identityHash, "recovery_observation_hash": contracts.HashStrings(command.Observation.IdentityHash, command.Observation.State, observedResultHash), "recovery_actor_id": command.Actor.ID, "sandbox_backend": link.Boundary}
	recovered, err := c.toolRuns.finishToolResultTx(ctx, tx, run, result, &command.Actor, eventKey, metadata, false)
	if err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	if err := c.fail("tool_recovery_before_commit"); err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ToolSandboxRecoveryFinalizeResult{}, err
	}
	linkResult.Link.Status = contracts.DomainEffectLinkStatusReconciled
	return ToolSandboxRecoveryFinalizeResult{Run: recovered, Effect: final.State, Link: linkResult.Link}, nil
}

func validateToolRecoveryIdentity(run contracts.ToolRun, link DomainEffectLinkCurrent, effect EffectState, command contracts.ToolRecoveryFinalizeRequest) error {
	identity := command.Identity
	stored := link.Link
	if stored.WorkspaceID != run.WorkspaceID || stored.OperationID != effect.OperationID || stored.EffectID != effect.EffectID || stored.DomainKind != contracts.DomainEffectKindToolRun || stored.DomainID != run.ID || stored.DomainHash != run.RequestHash || stored.Boundary != string(identity.Backend) || identity.Backend == contracts.SandboxBackendLocalProcess ||
		identity.WorkspaceID != run.WorkspaceID || identity.ToolRunID != run.ID || identity.ToolAttempt != run.Attempt || identity.ToolRequestHash != run.RequestHash ||
		identity.OperationID != stored.OperationID || identity.OperationOwnerID != stored.OperationOwnerID || identity.OperationFence != stored.OperationFence || identity.OperationRequestHash != stored.OperationHash || identity.AttemptID != stored.AttemptID || identity.EffectID != stored.EffectID || identity.EffectReservationHash != stored.EffectReservationHash ||
		identity.TaskOwnerID != stored.TaskOwnerID || identity.TaskFence != stored.TaskFence || identity.AgentRunID != recoveryEntityRefID(run.AgentRun) || identity.AgentRunOwnerID != run.AgentRunOwnerID || identity.AgentRunFence != run.AgentRunFence {
		return ErrToolRecoveryConflict
	}
	if len(run.RequestEvidence) == 0 {
		return ErrToolRecoveryConflict
	}
	var request contracts.ToolRequest
	if json.Unmarshal(run.RequestEvidence, &request) != nil || request.ToolDefinitionHash != identity.ToolDefinitionHash || request.SandboxProfileHash != identity.SandboxProfileHash || request.ToolID != run.ToolID || request.WorkspaceID != run.WorkspaceID || request.RequestID != run.RequestID || request.IdempotencyKey != run.IdempotencyKey || request.Actor.ID != run.Actor.ID || request.Actor.WorkspaceID != run.WorkspaceID || request.Budget.Backend != stored.Boundary ||
		request.TaskOwnerID != run.TaskOwnerID || request.TaskFence != run.TaskFence || recoveryEntityRefID(request.Task) != recoveryEntityRefID(run.Task) ||
		request.AgentRunOwnerID != run.AgentRunOwnerID || request.AgentRunFence != run.AgentRunFence || recoveryEntityRefID(request.AgentRun) != recoveryEntityRefID(run.AgentRun) {
		return ErrToolRecoveryConflict
	}
	if command.ExpectedEffectVer < 1 {
		return ErrToolRecoveryConflict
	}
	return nil
}

func toolRecoveryKey(idempotencyKey, stage string) string {
	return "tool-recovery-" + contracts.HashStrings(idempotencyKey, stage)[:48]
}

func recoveryEntityRefID(reference *contracts.EntityRef) string {
	if reference == nil {
		return ""
	}
	return reference.ID
}
