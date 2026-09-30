package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

// EmbeddingRecoveryFinalizeResult is the complete local outcome of one
// recovery command. The vector remains available only through the embedding
// ledger; the generic effect, link, and event expose hashes and references.
type EmbeddingRecoveryFinalizeResult struct {
	Record    contracts.EmbeddingCallRecord
	Effect    EffectState
	Link      contracts.DomainEffectLink
	Event     contracts.EventEnvelope
	Duplicate bool
}

// EmbeddingRecoveryCoordinator is the single local transaction boundary for
// an ambiguous embedding outcome. It composes existing authorities; it does
// not call a provider and it does not create a second lease system.
type EmbeddingRecoveryCoordinator struct {
	pool        *pgxpool.Pool
	embeddings  *EmbeddingCallStore
	admission   *AdmissionStore
	links       *DomainEffectLinkStore
	events      *EventStore
	failureHook func(string) error
}

func NewEmbeddingRecoveryCoordinator(pool *pgxpool.Pool, embeddings *EmbeddingCallStore, admission *AdmissionStore, links *DomainEffectLinkStore, events *EventStore) *EmbeddingRecoveryCoordinator {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &EmbeddingRecoveryCoordinator{pool: pool, embeddings: embeddings, admission: admission, links: links, events: events}
}

// SetFailureHook provides deterministic crash points for transaction tests.
// Returning an error rolls the caller-owned transaction back.
func (c *EmbeddingRecoveryCoordinator) SetFailureHook(hook func(string) error) {
	if c != nil {
		c.failureHook = hook
	}
}

func (c *EmbeddingRecoveryCoordinator) fail(point string) error {
	if c == nil || c.failureHook == nil {
		return nil
	}
	return c.failureHook(point)
}

// Finalize commits the generic effect, domain-effect link, embedding ledger,
// reconciliation audit, and typed event together. The existing effect lease
// owner/fence is checked by UpdateEffectTx, so stale recovery workers fail
// closed before any part of the transaction can become authoritative.
func (c *EmbeddingRecoveryCoordinator) Finalize(ctx context.Context, command contracts.EmbeddingRecoveryFinalizeRequest) (EmbeddingRecoveryFinalizeResult, error) {
	if c == nil || c.pool == nil || c.embeddings == nil || c.admission == nil || c.links == nil || c.events == nil {
		return EmbeddingRecoveryFinalizeResult{}, fmt.Errorf("embedding recovery coordinator is not configured")
	}
	if err := command.Normalize(); err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	tx, err := beginWorkspaceTx(ctx, c.pool, command.WorkspaceID)
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, fmt.Errorf("begin embedding recovery coordination: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	record, err := readEmbeddingCallTx(ctx, tx, command.WorkspaceID, command.RequestID)
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	if record.WorkspaceID != command.WorkspaceID || record.RequestID != command.RequestID {
		return EmbeddingRecoveryFinalizeResult{}, ErrEmbeddingReconciliationConflict
	}
	if command.Result.SourceHash != record.SourceHash || strings.ToLower(command.Result.Provider.Provider) != strings.ToLower(record.Provider.Provider) || command.Result.Provider.Model != record.Provider.Model || command.Result.ProviderRequestID != record.ProviderRequestID {
		return EmbeddingRecoveryFinalizeResult{}, ErrEmbeddingReconciliationConflict
	}
	if command.Result.TaskOwnerID == "" {
		command.Result.TaskOwnerID = record.TaskOwnerID
	}
	if command.Result.TaskFence == 0 && record.TaskFence > 0 {
		command.Result.TaskFence = record.TaskFence
	}
	if command.Result.Actor.ID == "" {
		command.Result.Actor = command.Actor
	}

	link, err := readDomainEffectLinkByDomainTx(ctx, tx, command.WorkspaceID, contracts.DomainEffectKindEmbeddingCall, record.IdempotencyKey, contracts.DomainEffectLinkRolePrimary)
	if errors.Is(err, ErrDomainEffectLinkNotFound) {
		return EmbeddingRecoveryFinalizeResult{}, ErrDomainEffectLinkNotFound
	}
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	latest, err := readLatestDomainEffectLinkTransitionTx(ctx, tx, command.WorkspaceID, link.ID, true)
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	effect, err := readEffectState(ctx, tx, command.WorkspaceID, link.EffectID, true)
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	if effect.OperationID != link.OperationID {
		return EmbeddingRecoveryFinalizeResult{}, ErrDomainEffectLinkConflict
	}
	if effect.Version != command.ExpectedEffectVersion {
		var existingVersion int64
		err := tx.QueryRow(ctx, `SELECT version FROM fornix.operation_effect_transitions WHERE workspace_id=$1 AND effect_id=$2 AND idempotency_key=$3`, command.WorkspaceID, link.EffectID, command.IdempotencyKey).Scan(&existingVersion)
		if errors.Is(err, pgx.ErrNoRows) {
			return EmbeddingRecoveryFinalizeResult{}, ErrEffectLeaseFenced
		}
		if err != nil {
			return EmbeddingRecoveryFinalizeResult{}, err
		}
	}

	// A committed command can be replayed even after the caller's lease has
	// expired. The idempotency rows are checked before lease validation by the
	// underlying transition APIs, and no new authority is created here.
	duplicateLink := latest.IdempotencyKey == command.IdempotencyKey && latest.ToStatus == contracts.DomainEffectLinkStatusReconciled
	if !duplicateLink && latest.Version != command.ExpectedLinkVersion {
		return EmbeddingRecoveryFinalizeResult{}, ErrDomainEffectLinkFenced
	}
	resultHash := recoveryResultHash(record.RequestHash, command.Result)
	finalState := contracts.ExternalEffectCompensationPending
	failureCode := "embedding_reconciliation_failed"
	verificationHash := ""
	if command.Result.Status == contracts.EmbeddingCallSucceeded {
		finalState = contracts.ExternalEffectVerificationPending
		failureCode = ""
		verificationHash = resultHash
	}
	if duplicateLink {
		// A replay must prove that the already-committed link and effect carry
		// the same terminal result. It must not reacquire or mutate authority.
		if latest.ResultHash != resultHash || latest.ProviderRequestID != command.Result.ProviderRequestID || latest.FailureCode != failureCode || effect.State != finalState || effect.ResponseHash != resultHash {
			return EmbeddingRecoveryFinalizeResult{}, ErrDomainEffectLinkConflict
		}
		link.Status = latest.ToStatus
		return EmbeddingRecoveryFinalizeResult{Record: record, Effect: effect, Link: link, Duplicate: true}, nil
	}

	effectResult, err := c.admission.UpdateEffectTx(ctx, tx, contracts.ExternalEffectUpdate{
		WorkspaceID: command.WorkspaceID, OperationID: link.OperationID, EffectID: link.EffectID,
		OwnerID: command.OwnerID, Fence: command.Fence, LeaseKind: "effect",
		RequestID: "embedding-recovery:" + command.RequestID, IdempotencyKey: command.IdempotencyKey,
		State: finalState, ProviderRequestID: command.Result.ProviderRequestID,
		ResponseHash: resultHash, VerificationHash: verificationHash, FailureCode: failureCode,
	})
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	if err := c.fail("embedding_recovery_effect_transition"); err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}

	if latest.ToStatus == contracts.DomainEffectLinkStatusLinked {
		marked, markErr := c.links.TransitionTx(ctx, tx, contracts.DomainEffectLinkTransitionRequest{
			WorkspaceID: command.WorkspaceID, LinkID: link.ID, ExpectedVersion: latest.Version,
			FromStatus: contracts.DomainEffectLinkStatusLinked, ToStatus: contracts.DomainEffectLinkStatusRecoveryRequired,
			// recovery_required is an admission marker, not provider proof. The
			// result hash belongs only on the terminal reconciled transition.
			ProviderRequestID: command.Result.ProviderRequestID,
			IdempotencyKey:    "embedding-recovery-marker-" + contracts.HashStrings(command.IdempotencyKey)[:48], Actor: command.Actor,
		})
		if markErr != nil {
			return EmbeddingRecoveryFinalizeResult{}, markErr
		}
		latest.Version = marked.Transition.Version
		latest.ToStatus = marked.Transition.ToStatus
	}
	if latest.ToStatus != contracts.DomainEffectLinkStatusRecoveryRequired && !duplicateLink {
		return EmbeddingRecoveryFinalizeResult{}, ErrDomainEffectLinkTransition
	}
	linkResult, err := c.links.TransitionTx(ctx, tx, contracts.DomainEffectLinkTransitionRequest{
		WorkspaceID: command.WorkspaceID, LinkID: link.ID, ExpectedVersion: latest.Version,
		FromStatus: contracts.DomainEffectLinkStatusRecoveryRequired, ToStatus: contracts.DomainEffectLinkStatusReconciled,
		ProviderRequestID: command.Result.ProviderRequestID, ResultHash: resultHash,
		FailureCode: failureCode, IdempotencyKey: command.IdempotencyKey, Actor: command.Actor,
	})
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	if err := c.fail("embedding_recovery_link_transition"); err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}

	if err := c.embeddings.resolveRecoveryTx(ctx, tx, command.Result, false, EmbeddingRecoveryAuditFacts{
		OperationID: link.OperationID, EffectID: link.EffectID, LinkID: link.ID,
		RequestHash: record.RequestHash, RecoveryOwnerID: command.OwnerID, RecoveryFence: command.Fence,
		ExpectedEffectVersion: command.ExpectedEffectVersion, ExpectedLinkVersion: command.ExpectedLinkVersion,
		CausationID: command.Result.RequestID, CorrelationID: contracts.HashStrings(command.WorkspaceID, command.RequestID),
	}); err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	event, err := embeddingRecoveryEvent(command, link, resultHash, finalState, failureCode)
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	appendResult, err := c.events.AppendTx(ctx, tx, event)
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	if err := c.fail("embedding_recovery_before_commit"); err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}

	record, err = c.embeddings.Get(ctx, command.WorkspaceID, command.RequestID)
	if err != nil {
		return EmbeddingRecoveryFinalizeResult{}, err
	}
	linkResult.Link.Status = contracts.DomainEffectLinkStatusReconciled
	return EmbeddingRecoveryFinalizeResult{Record: record, Effect: effectResult.State, Link: linkResult.Link, Event: appendResult.Event, Duplicate: effectResult.Duplicate || linkResult.Duplicate}, nil
}

func recoveryResultHash(requestHash string, result contracts.EmbeddingReconciliationResult) string {
	vectorHash := ""
	if result.Status == contracts.EmbeddingCallSucceeded {
		vectorHash, _ = contracts.EmbeddingVectorHash(result.Vector)
	}
	failureCode := ""
	if result.Failure != nil {
		failureCode = result.Failure.Code
	}
	return contracts.HashStrings("embedding-recovery", requestHash, result.ProviderRequestID, result.SourceHash, vectorHash, result.Status, failureCode)
}

func embeddingRecoveryEvent(command contracts.EmbeddingRecoveryFinalizeRequest, link contracts.DomainEffectLink, resultHash, effectState, failureCode string) (contracts.EventEnvelope, error) {
	event, err := contracts.NewEvent("embedding.recovery_finalized", map[string]any{
		"request_id": command.RequestID, "effect_id": link.EffectID, "link_id": link.ID,
		"provider": command.Result.Provider.Provider, "model": command.Result.Provider.Model,
		"provider_request_id": command.Result.ProviderRequestID, "source_hash": command.Result.SourceHash,
		"result_hash": resultHash, "embedding_status": command.Result.Status,
		"effect_state": effectState, "failure_code": failureCode,
	})
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	event.Scope = contracts.Scope{WorkspaceID: command.WorkspaceID}
	event.Actor = command.Actor
	event.CausationID = command.RequestID
	event.CorrelationID = contracts.HashStrings(command.WorkspaceID, command.RequestID, command.IdempotencyKey)
	event.IdempotencyKey = "embedding-recovery-event-" + contracts.HashStrings(command.WorkspaceID, command.IdempotencyKey)[:48]
	return event, nil
}
