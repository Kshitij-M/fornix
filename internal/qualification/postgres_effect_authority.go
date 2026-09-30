package qualification

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/store"
)

const (
	defaultPostgresEffectProbeTimeout = 30 * time.Second
	maxPostgresEffectProbeTimeout     = 2 * time.Minute
	probeLeaseTTL                     = time.Minute
	probeStaleLeaseTTL                = 100 * time.Millisecond
	probeStaleWait                    = 150 * time.Millisecond
)

var ErrPostgresEffectAuthorityProbe = errors.New("postgres effect-authority qualification probe failed")

// PostgresEffectAuthorityProbeOptions is explicit by design. The DSN is used
// only to open the caller-selected disposable database and is never included
// in an observation, report, error detail, or log line.
type PostgresEffectAuthorityProbeOptions struct {
	DSN         string
	WorkspaceID string
	RunID       string
	Timeout     time.Duration
}

func (o PostgresEffectAuthorityProbeOptions) normalize() (PostgresEffectAuthorityProbeOptions, error) {
	o.DSN = strings.TrimSpace(o.DSN)
	o.WorkspaceID = strings.TrimSpace(o.WorkspaceID)
	o.RunID = strings.TrimSpace(o.RunID)
	if o.DSN == "" {
		return PostgresEffectAuthorityProbeOptions{}, fmt.Errorf("%w: explicit disposable DSN is required", ErrPostgresEffectAuthorityProbe)
	}
	if o.WorkspaceID == "" || len(o.WorkspaceID) > contracts.MaxDomainIDLength {
		return PostgresEffectAuthorityProbeOptions{}, fmt.Errorf("%w: workspace_id is invalid", ErrPostgresEffectAuthorityProbe)
	}
	if o.RunID == "" || len(o.RunID) > contracts.MaxDomainIDLength {
		return PostgresEffectAuthorityProbeOptions{}, fmt.Errorf("%w: run_id is invalid", ErrPostgresEffectAuthorityProbe)
	}
	if o.Timeout == 0 {
		o.Timeout = defaultPostgresEffectProbeTimeout
	}
	if o.Timeout <= 0 || o.Timeout > maxPostgresEffectProbeTimeout {
		return PostgresEffectAuthorityProbeOptions{}, fmt.Errorf("%w: timeout is outside bounds", ErrPostgresEffectAuthorityProbe)
	}
	return o, nil
}

// RunPostgresEffectAuthorityProbe runs one bounded, authority-backed probe.
// It performs no provider or network call beyond the explicit PostgreSQL DSN;
// the external invoker is a deterministic in-process fake. The returned
// observation is hash-only and suitable for the portable qualification
// runner. This function is intentionally not called by the default CLI.
func RunPostgresEffectAuthorityProbe(ctx context.Context, options PostgresEffectAuthorityProbeOptions) (observation contracts.EffectAuthorityObservation, err error) {
	if ctx == nil {
		return contracts.EffectAuthorityObservation{}, fmt.Errorf("%w: context is nil", ErrPostgresEffectAuthorityProbe)
	}
	options, err = options.normalize()
	if err != nil {
		return contracts.EffectAuthorityObservation{}, err
	}
	runCtx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	pool, err := pgxpool.New(runCtx, options.DSN)
	if err != nil {
		return contracts.EffectAuthorityObservation{}, fmt.Errorf("%w: database open", ErrPostgresEffectAuthorityProbe)
	}
	defer pool.Close()
	if err := pool.Ping(runCtx); err != nil {
		return contracts.EffectAuthorityObservation{}, fmt.Errorf("%w: database ping", ErrPostgresEffectAuthorityProbe)
	}
	if err := store.ApplyMigrations(runCtx, pool); err != nil {
		return contracts.EffectAuthorityObservation{}, fmt.Errorf("%w: migrations", ErrPostgresEffectAuthorityProbe)
	}
	operations := store.NewOperationStore(pool, store.NewEventStore(pool))
	admission := store.NewAdmissionStore(pool, store.NewEventStore(pool))
	links := store.NewDomainEffectLinkStore(pool)
	dispatcher := &effectdispatch.Dispatcher{Operations: operations, Admission: admission, Links: links}

	success, err := createProbeOperation(runCtx, operations, options.WorkspaceID, options.RunID+"-success", "success-worker", "operator-success", probeLeaseTTL)
	if err != nil {
		return contracts.EffectAuthorityObservation{}, probeFailure("create success operation")
	}
	var invocations atomic.Int32
	invoker := probeInvoker(options.WorkspaceID, success, &invocations)
	first, err := dispatcher.Dispatch(runCtx, dispatchRequest(success, invoker))
	if err != nil || first.OperationResult == nil || first.State.State != contracts.ExternalEffectVerified || first.DomainLink == nil || first.DomainLink.Status != contracts.DomainEffectLinkStatusReconciled {
		return contracts.EffectAuthorityObservation{}, probeFailure("successful dispatch")
	}
	duplicate, err := dispatcher.Dispatch(runCtx, dispatchRequest(success, invoker))
	if err != nil || !duplicate.Duplicate || duplicate.DomainLink == nil || duplicate.DomainLink.Status != contracts.DomainEffectLinkStatusReconciled || invocations.Load() != 1 {
		return contracts.EffectAuthorityObservation{}, probeFailure("duplicate dispatch")
	}

	replayHash := dispatchReplayHash(first, duplicate)
	receipts := store.NewWorkReceiptStore(pool)
	receiptRequest := probeReceiptRequest(options, success, first, replayHash, "receipt")
	crashedRequest := receiptRequest
	crashedRequest.ReceiptID += "-crash"
	crashedRequest.RequestID += "-crash"
	crashedRequest.IdempotencyKey += "-crash"
	receipts.SetFailureHook(func(stage string) error {
		if stage == "links_inserted" {
			return errors.New("qualification receipt rollback")
		}
		return nil
	})
	if _, _, crashErr := receipts.Finalize(runCtx, crashedRequest); crashErr == nil {
		return contracts.EffectAuthorityObservation{}, probeFailure("receipt rollback was not exercised")
	}
	receipts.SetFailureHook(nil)
	if _, getErr := receipts.Get(runCtx, options.WorkspaceID, crashedRequest.ReceiptID); !errors.Is(getErr, store.ErrWorkReceiptNotFound) {
		return contracts.EffectAuthorityObservation{}, probeFailure("receipt rollback left durable history")
	}
	receipt, inserted, err := receipts.Finalize(runCtx, receiptRequest)
	if err != nil || !inserted {
		return contracts.EffectAuthorityObservation{}, probeFailure("receipt finalization")
	}
	duplicateReceipt, duplicateInserted, err := receipts.Finalize(runCtx, receiptRequest)
	if err != nil || duplicateInserted || duplicateReceipt.CanonicalHash != receipt.CanonicalHash {
		return contracts.EffectAuthorityObservation{}, probeFailure("receipt duplicate")
	}
	authorityLinks, err := operations.AuthorityLinks(runCtx, options.WorkspaceID, success.operation.ID, 16)
	if err != nil {
		return contracts.EffectAuthorityObservation{}, probeFailure("receipt authority link read")
	}
	receiptLink, receiptLinkOK := findReceiptAuthorityLink(authorityLinks, receipt.ID, receipt.CanonicalHash)
	if !receiptLinkOK {
		return contracts.EffectAuthorityObservation{}, probeFailure("receipt authority link verification")
	}

	stale, err := createProbeOperation(runCtx, operations, options.WorkspaceID, options.RunID+"-stale", "stale-worker-a", "operator-stale", probeStaleLeaseTTL)
	if err != nil {
		return contracts.EffectAuthorityObservation{}, probeFailure("create stale operation")
	}
	oldLease := stale.lease
	time.Sleep(probeStaleWait)
	if _, err := operations.AcquireLease(runCtx, options.WorkspaceID, stale.operation.ID, "stale-worker-b", probeLeaseTTL); err != nil {
		return contracts.EffectAuthorityObservation{}, probeFailure("take over stale lease")
	}
	var staleInvocations atomic.Int32
	_, staleErr := dispatcher.Dispatch(runCtx, dispatchRequestWithLease(stale, oldLease, func(context.Context, contracts.EffectAuthority) (effectdispatch.InvocationResult, error) {
		staleInvocations.Add(1)
		return effectdispatch.InvocationResult{}, nil
	}))
	if staleErr == nil || staleInvocations.Load() != 0 {
		return contracts.EffectAuthorityObservation{}, probeFailure("stale fence was accepted")
	}

	_, foreignErr := links.Get(runCtx, options.WorkspaceID+"-foreign", first.DomainLink.ID)
	workspaceIsolationProven := errors.Is(foreignErr, store.ErrDomainEffectLinkNotFound)
	if !workspaceIsolationProven {
		return contracts.EffectAuthorityObservation{}, probeFailure("workspace isolation")
	}

	observation = contracts.EffectAuthorityObservation{
		WorkspaceID:              options.WorkspaceID,
		OperationID:              success.operation.ID,
		EffectID:                 first.Effect.EffectID,
		ReservationHash:          first.Authority.EffectReservationHash,
		DomainLinkID:             first.DomainLink.ID,
		DomainLinkHash:           first.DomainLink.LinkHash,
		ResultHash:               first.DomainLink.ResultHash,
		ReceiptHash:              receipt.CanonicalHash,
		ReceiptLinkHash:          receiptLink.LinkHash,
		ReplayHash:               replayHash,
		SuccessReconciled:        first.DomainLink.Status == contracts.DomainEffectLinkStatusReconciled,
		ReceiptLinkVerified:      receiptLink.ReceiptHash == receipt.CanonicalHash,
		DuplicateSuppressed:      duplicate.Duplicate && invocations.Load() == 1,
		StaleFenceRejected:       staleErr != nil && staleInvocations.Load() == 0,
		WorkspaceIsolationProven: workspaceIsolationProven,
		ReplayStable:             replayHash == dispatchReplayHash(first, duplicate),
		InvocationCount:          int(invocations.Load()),
	}
	if err := observation.Normalize(); err != nil {
		return contracts.EffectAuthorityObservation{}, probeFailure("observation validation")
	}
	return observation, nil
}

type probeOperation struct {
	operation  store.Operation
	definition contracts.CapabilityDefinition
	request    contracts.OperationRequest
	admission  contracts.AdmissionInput
	lease      store.OperationLease
}

func createProbeOperation(ctx context.Context, operations *store.OperationStore, workspaceID, operationID, ownerID, actorID string, leaseTTL time.Duration) (probeOperation, error) {
	definition := contracts.CapabilityDefinition{
		WorkspaceID:        workspaceID,
		Ref:                contracts.CapabilityRef{WorkspaceID: workspaceID, Connector: contracts.ConnectorRef{WorkspaceID: workspaceID, Name: "qualification", Version: "1"}, Name: "effect-authority", Version: "1"},
		Description:        "bounded effect authority qualification",
		InputSchemaVersion: 1, InputSchemaHash: contracts.HashStrings("qualification-input"),
		OutputSchemaVersion: 1, OutputSchemaHash: contracts.HashStrings("qualification-output"),
		Effect: contracts.EffectClassReversibleWrite, Profile: contracts.DefaultExecutionProfile(),
		RetryPolicy:   contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"},
		ResourceKinds: []string{"qualification-record"}, SupportsIdempotency: true, Enabled: true,
	}
	if err := definition.Normalize(); err != nil {
		return probeOperation{}, err
	}
	request := contracts.OperationRequest{
		ID: operationID, RequestID: operationID + "-request", IdempotencyKey: operationID + "-key", WorkspaceID: workspaceID,
		Actor: contracts.ActorRef{ID: actorID, Kind: "qualification", WorkspaceID: workspaceID}, Capability: definition.Ref,
		Target:    contracts.ResourceRef{WorkspaceID: workspaceID, System: contracts.SystemRef{WorkspaceID: workspaceID, Type: "qualification", ID: "authority", Version: "1"}, Kind: "qualification-record", ID: operationID, Version: "1"},
		InputType: "qualification.input", InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash,
		InputHash: contracts.HashStrings("qualification-input", operationID), Profile: definition.Profile,
	}
	if err := request.Normalize(); err != nil {
		return probeOperation{}, err
	}
	plan := contracts.OperationPlan{ID: operationID + "-plan", WorkspaceID: workspaceID, OperationID: operationID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: "effect-step", Ordinal: 0, Kind: "qualification.effect", Capability: definition.Ref, Target: request.Target, Effect: definition.Effect, Profile: definition.Profile, InputHash: request.InputHash}}}
	if err := plan.Normalize(); err != nil {
		return probeOperation{}, err
	}
	created, err := operations.Create(ctx, store.OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		return probeOperation{}, err
	}
	policy := contracts.AdmissionPolicy{WorkspaceID: workspaceID, PolicyID: "effect-authority-qualification", Version: "1", EffectRules: []contracts.AdmissionEffectRule{{Effect: definition.Effect, Mode: contracts.PolicyApprovalAutomatic}}, AllowedConnectors: []contracts.ConnectorRef{definition.Ref.Connector}, AllowedResourceKinds: []string{"qualification-record"}, AllowedActorIDs: []string{actorID}, MaxCostMicros: 100, MaxOperationsPerWindow: 4, MaxCostPerWindowMicros: 400}
	if err := policy.Normalize(); err != nil {
		return probeOperation{}, err
	}
	admission := contracts.AdmissionInput{WorkspaceID: workspaceID, OperationID: operationID, OperationHash: created.Operation.OperationHash, RequestID: request.RequestID, IdempotencyKey: operationID + "-admission", Actor: request.Actor, Capability: definition, Target: request.Target, Policy: policy, ConnectorAvailable: true, ResourceAllowed: true, EvidenceSatisfied: true, TaskFenceValid: true, RequestedCostMicros: 1}
	leaseResult, err := operations.AcquireLease(ctx, workspaceID, operationID, ownerID, leaseTTL)
	if err != nil {
		return probeOperation{}, err
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if _, err := operations.Transition(ctx, store.OperationTransitionInput{WorkspaceID: workspaceID, OperationID: operationID, OwnerID: leaseResult.Lease.OwnerID, Fence: leaseResult.Lease.Fence, Actor: request.Actor, RequestID: operationID + "-" + status, IdempotencyKey: operationID + "-" + status, ToStatus: status}); err != nil {
			return probeOperation{}, err
		}
	}
	return probeOperation{operation: created.Operation, definition: definition, request: request, admission: admission, lease: leaseResult.Lease}, nil
}

func probeInvoker(workspaceID string, fixture probeOperation, calls *atomic.Int32) effectdispatch.Invoker {
	return func(_ context.Context, authority contracts.EffectAuthority) (effectdispatch.InvocationResult, error) {
		calls.Add(1)
		outputHash := contracts.HashStrings("qualification-output", fixture.operation.ID)
		effect := contracts.ExternalEffect{SchemaVersion: contracts.DomainNeutralSchemaVersion, ID: authority.EffectID, WorkspaceID: workspaceID, Boundary: "qualification.fake-effect", Class: fixture.definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: authority.EffectID, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		result := contracts.OperationResult{ID: fixture.operation.ID + "-result", OperationID: authority.OperationID, OperationHash: authority.RequestHash, RequestID: fixture.request.RequestID, WorkspaceID: workspaceID, Actor: fixture.request.Actor, Status: contracts.OperationStatusSucceeded, OutputHash: outputHash, ExternalEffects: []contracts.ExternalEffect{effect}}
		return effectdispatch.InvocationResult{Result: result, ResponseHash: outputHash, ExternalEffectStarted: true}, nil
	}
}

func dispatchRequest(fixture probeOperation, invoker effectdispatch.Invoker) effectdispatch.Request {
	return dispatchRequestWithLease(fixture, fixture.lease, invoker)
}

func dispatchRequestWithLease(fixture probeOperation, lease store.OperationLease, invoker effectdispatch.Invoker) effectdispatch.Request {
	return effectdispatch.Request{
		Admission: fixture.admission, Operation: fixture.request, Definition: fixture.definition,
		OwnerID: lease.OwnerID, Fence: lease.Fence, StepID: "effect-step", AttemptKey: fixture.operation.ID + "-attempt",
		Effect:       contracts.ExternalEffect{WorkspaceID: fixture.operation.WorkspaceID, Boundary: "qualification.fake-effect", Class: fixture.definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable},
		DomainLink:   &contracts.DomainEffectLink{DomainKind: contracts.DomainEffectKindHTTPRequest, DomainID: fixture.operation.ID, DomainHash: fixture.request.StableHash(), LinkRole: contracts.DomainEffectLinkRolePrimary, IdempotencyKey: fixture.operation.ID + "-domain-link"},
		RecordResult: true, Invoker: invoker,
	}
}

func probeReceiptRequest(options PostgresEffectAuthorityProbeOptions, fixture probeOperation, result effectdispatch.Result, replayHash, suffix string) contracts.WorkReceiptFinalizeRequest {
	return contracts.WorkReceiptFinalizeRequest{
		ReceiptID: "effect-authority-receipt-" + suffix, RequestID: "effect-authority-receipt-request-" + suffix, IdempotencyKey: "effect-authority-receipt-key-" + suffix,
		WorkspaceID: options.WorkspaceID, Actor: fixture.request.Actor, WorkKind: contracts.WorkReceiptReferenceOperation, WorkID: fixture.operation.ID,
		Operation: &contracts.OperationReference{WorkspaceID: options.WorkspaceID, ID: fixture.operation.ID, Hash: fixture.operation.OperationHash}, ReplayHash: replayHash,
		Steps: []contracts.WorkReceiptStep{{Ordinal: 0, ID: "effect-dispatch", Name: "effect dispatch", Kind: "effect", Status: "succeeded", SourceKind: "domain_effect", SourceID: result.DomainLink.ID, SourceHash: result.DomainLink.LinkHash, InputHash: fixture.request.InputHash, OutputHash: result.DomainLink.ResultHash, Attempts: 1, ExternalEffect: true, ExternalBoundary: "qualification.fake-effect"}},
	}
}

func dispatchReplayHash(first, duplicate effectdispatch.Result) string {
	if first.DomainLink == nil || duplicate.DomainLink == nil {
		return ""
	}
	return contracts.HashStrings("effect-authority-replay", first.Effect.EffectID, first.DomainLink.LinkHash, first.DomainLink.ResultHash, duplicate.Effect.EffectID, duplicate.DomainLink.LinkHash, duplicate.DomainLink.ResultHash)
}

func findReceiptAuthorityLink(links []contracts.OperationAuthorityLink, receiptID, receiptHash string) (contracts.OperationAuthorityLink, bool) {
	for _, link := range links {
		if link.Stage == contracts.AuthorityStageReceipt && link.ReceiptID == receiptID && link.ReceiptHash == receiptHash && link.LinkHash != "" {
			return link, true
		}
	}
	return contracts.OperationAuthorityLink{}, false
}

func probeFailure(phase string) error {
	return fmt.Errorf("%w: %s", ErrPostgresEffectAuthorityProbe, phase)
}
