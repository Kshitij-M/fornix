package connector_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/omaveda/fornix/internal/adapters/repository"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

const testWorkspace = "workspace-a"

func TestRegistryRepositoryConformance(t *testing.T) {
	var calls atomic.Int32
	adapter, err := repository.NewConnector(testWorkspace, "1", func(_ context.Context, request contracts.OperationRequest) (repository.Inspection, error) {
		calls.Add(1)
		return repository.Inspection{
			OutputHash: repository.HashBytes([]byte(request.Target.ID + ":state")),
			Evidence: []contracts.OperationEvidenceRef{{
				WorkspaceID: testWorkspace, SourceReference: "repository:" + request.Target.ID,
				EvidenceHash: repository.HashBytes([]byte("evidence:" + request.Target.ID)), Role: "snapshot",
			}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	request := repositoryRequest(t, adapter.Capabilities()[0].Definition())
	failures := connector.RunConformanceSuite(context.Background(), registry, request, connector.AdmissionOptions{})
	for _, failure := range failures {
		t.Errorf("conformance case %s: %v", failure.Case, failure.Err)
	}
	if len(failures) != 0 {
		t.Fatalf("repository conformance failures: %d", len(failures))
	}
	if calls.Load() != 1 {
		t.Fatalf("conformance replay must use the recorded result, calls=%d", calls.Load())
	}
}

func TestRegistryRejectsDuplicateAndMismatchedRegistration(t *testing.T) {
	adapter, err := repository.NewConnector(testWorkspace, "1", readyInspection)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(adapter); !errors.Is(err, connector.ErrConnectorDuplicate) {
		t.Fatalf("duplicate connector error = %v, want %v", err, connector.ErrConnectorDuplicate)
	}
	bad, err := repository.NewConnector("workspace-b", "1", readyInspection)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(bad); err != nil {
		t.Fatal(err)
	}
	if got := len(registry.ConnectorRefs(testWorkspace)); got != 1 {
		t.Fatalf("workspace connector count = %d, want 1", got)
	}
}

func TestRegistryRejectsUnboundedCapabilityBudgets(t *testing.T) {
	definition := testDefinition(testWorkspace, "budget", contracts.EffectClassReadOnly)
	definition.MaxRows = contracts.MaxCapabilityMaxRows + 1
	definition.Ref.DefinitionHash = ""
	if err := definition.Normalize(); err == nil {
		t.Fatal("unbounded row budget was normalized")
	}

	definition = testDefinition(testWorkspace, "rate-budget", contracts.EffectClassReadOnly)
	definition.RateLimitPerMinute = contracts.MaxCapabilityRatePerMinute + 1
	definition.Ref.DefinitionHash = ""
	if err := definition.Normalize(); err == nil {
		t.Fatal("unbounded rate budget was normalized")
	}
}

func TestRegistryAdmissionFailsClosed(t *testing.T) {
	ready, err := repository.NewConnector(testWorkspace, "1", readyInspection)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(ready); err != nil {
		t.Fatal(err)
	}
	request := repositoryRequest(t, ready.Capabilities()[0].Definition())

	stale := request
	stale.Capability.DefinitionHash = repository.HashBytes([]byte("stale"))
	if _, err := registry.Admit(context.Background(), stale, connector.AdmissionOptions{}); !errors.Is(err, connector.ErrCapabilityNotFound) {
		t.Fatalf("stale definition error = %v, want capability-not-found", err)
	}

	crossWorkspace := request
	crossWorkspace.Target.WorkspaceID = "workspace-b"
	if _, err := registry.Admit(context.Background(), crossWorkspace, connector.AdmissionOptions{}); err == nil {
		t.Fatal("cross-workspace target was admitted")
	}

	unsupportedResource := request
	unsupportedResource.Target.Kind = "database"
	unsupportedResource.Target.System.Type = "database"
	if _, err := registry.Admit(context.Background(), unsupportedResource, connector.AdmissionOptions{}); err == nil {
		t.Fatal("unsupported resource kind was admitted")
	}

	unauthorized := request
	principal := contracts.Principal{ID: "different", WorkspaceID: testWorkspace, Kind: "operator", Authenticated: true}
	if _, err := registry.Admit(context.Background(), unauthorized, connector.AdmissionOptions{Principal: &principal}); !errors.Is(err, connector.ErrUnauthorized) {
		t.Fatalf("actor mismatch error = %v, want unauthorized", err)
	}

	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{
		Authorize: func(context.Context, contracts.Principal, contracts.OperationRequest, contracts.CapabilityDefinition) (bool, error) {
			return false, nil
		},
	}); !errors.Is(err, connector.ErrUnauthorized) {
		t.Fatalf("missing principal authorization error = %v, want unauthorized", err)
	}
}

func TestRegistryAdmissionRequiresApprovalAndCredentialAvailability(t *testing.T) {
	definition := testDefinition(testWorkspace, "governed", contracts.EffectClassReversibleWrite)
	definition.RequiresApproval = true
	definition.RequiredCredentialRefs = []string{"credential-1"}
	definition.Ref.DefinitionHash = ""
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	fake := &fakeCapability{definition: definition}
	registry := connector.NewRegistry()
	if err := registry.Register(&fakeConnector{ref: definition.Ref.Connector, capability: fake}); err != nil {
		t.Fatal(err)
	}
	request := fakeRequest(definition)
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{Credentials: map[string]bool{"credential-1": true}}); !errors.Is(err, connector.ErrApprovalRequired) {
		t.Fatalf("approval error = %v, want approval required", err)
	}
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{ApprovalGranted: true}); !errors.Is(err, connector.ErrCredentialUnavailable) {
		t.Fatalf("credential error = %v, want credential unavailable", err)
	}
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{ApprovalGranted: true, Credentials: map[string]bool{"credential-1": true}}); err != nil {
		t.Fatalf("available credential admission failed: %v", err)
	}
}

func TestRegistryDeterministicListingAndDefensiveCopies(t *testing.T) {
	registry := connector.NewRegistry()
	for _, name := range []string{"zeta", "alpha", "middle"} {
		definition := testDefinition(testWorkspace, name, contracts.EffectClassReadOnly)
		if err := registry.Register(&fakeConnector{ref: definition.Ref.Connector, capability: &fakeCapability{definition: definition}}); err != nil {
			t.Fatal(err)
		}
	}
	definitions := registry.Capabilities(testWorkspace)
	if len(definitions) != 3 || definitions[0].Ref.Name != "alpha" || definitions[1].Ref.Name != "middle" || definitions[2].Ref.Name != "zeta" {
		t.Fatalf("capability order is not stable: %+v", definitions)
	}
	definitions[0].ResourceKinds[0] = "mutated"
	again := registry.Capabilities(testWorkspace)
	if again[0].ResourceKinds[0] == "mutated" {
		t.Fatal("registry returned a mutable definition")
	}
}

func TestRegistryConcurrentLookupPreservesIntegrity(t *testing.T) {
	adapter, err := repository.NewConnector(testWorkspace, "1", readyInspection)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	request := repositoryRequest(t, adapter.Capabilities()[0].Definition())
	const workers = 32
	var group sync.WaitGroup
	group.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer group.Done()
			for j := 0; j < 100; j++ {
				capability, ok := registry.Lookup(request.Capability)
				if !ok || capability.Definition().Ref.DefinitionHash != request.Capability.DefinitionHash {
					t.Errorf("lookup lost registered capability")
					return
				}
			}
		}()
	}
	group.Wait()
}

func TestRegistryConcurrentRegistrationPreservesIntegrity(t *testing.T) {
	const registrations = 32
	registry := connector.NewRegistry()
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(registrations)
	for i := 0; i < registrations; i++ {
		go func(index int) {
			defer group.Done()
			<-start
			name := fmt.Sprintf("concurrent-%02d", index)
			definition := testDefinition(testWorkspace, name, contracts.EffectClassReadOnly)
			if err := registry.Register(&fakeConnector{ref: definition.Ref.Connector, capability: &fakeCapability{definition: definition}}); err != nil {
				t.Errorf("register %s: %v", name, err)
			}
		}(i)
	}
	close(start)
	group.Wait()
	if got := len(registry.ConnectorRefs(testWorkspace)); got != registrations {
		t.Fatalf("registered connector count = %d, want %d", got, registrations)
	}
	if got := len(registry.Capabilities(testWorkspace)); got != registrations {
		t.Fatalf("registered capability count = %d, want %d", got, registrations)
	}
}

func TestRegistryRejectsCapabilityDefinitionMutationAfterRegistration(t *testing.T) {
	definition := testDefinition(testWorkspace, "immutable", contracts.EffectClassReadOnly)
	fake := &fakeCapability{definition: definition}
	registry := connector.NewRegistry()
	if err := registry.Register(&fakeConnector{ref: definition.Ref.Connector, capability: fake}); err != nil {
		t.Fatal(err)
	}
	fake.definition.Description = "mutated after registration"
	if _, err := registry.Admit(context.Background(), fakeRequest(definition), connector.AdmissionOptions{}); !errors.Is(err, connector.ErrDefinitionConflict) {
		t.Fatalf("mutated definition error = %v, want definition conflict", err)
	}
}

func TestExecutorRetriesOnlyClassifiedFailures(t *testing.T) {
	definition := testDefinition(testWorkspace, "retry", contracts.EffectClassReadOnly)
	definition.RetryPolicy = contracts.CapabilityRetryPolicy{MaxAttempts: 2, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none", RetryableCodes: []string{"transport"}}
	definition.Ref.DefinitionHash = ""
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	fake := &fakeCapability{definition: definition, execute: func(_ context.Context, request contracts.OperationRequest, plan contracts.OperationPlan) (contracts.OperationResult, error) {
		if attempts.Add(1) == 1 {
			return contracts.OperationResult{}, &connector.FailureError{Code: "transport", Retryable: true}
		}
		return successResult(request, plan), nil
	}}
	registry := connector.NewRegistry()
	if err := registry.Register(&fakeConnector{ref: definition.Ref.Connector, capability: fake}); err != nil {
		t.Fatal(err)
	}
	outcome, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), fakeRequest(definition), connector.AdmissionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Attempts != 2 || attempts.Load() != 2 {
		t.Fatalf("attempts = %d/%d, want 2/2", outcome.Attempts, attempts.Load())
	}
}

func TestExecutorHonorsRequestRetryBudget(t *testing.T) {
	definition := testDefinition(testWorkspace, "retry-budget", contracts.EffectClassReadOnly)
	definition.RetryPolicy = contracts.CapabilityRetryPolicy{MaxAttempts: 3, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none", RetryableCodes: []string{"transport"}}
	definition.Ref.DefinitionHash = ""
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	fake := &fakeCapability{definition: definition, execute: func(_ context.Context, _ contracts.OperationRequest, _ contracts.OperationPlan) (contracts.OperationResult, error) {
		attempts.Add(1)
		return contracts.OperationResult{}, &connector.FailureError{Code: "transport", Retryable: true}
	}}
	registry := connector.NewRegistry()
	if err := registry.Register(&fakeConnector{ref: definition.Ref.Connector, capability: fake}); err != nil {
		t.Fatal(err)
	}
	request := fakeRequest(definition)
	request.Profile.MaxRetries = 1
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("retry budget failure unexpectedly succeeded")
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2 for one retry", got)
	}
}

func TestExecutorDoesNotRetryAfterExternalEffect(t *testing.T) {
	definition := testDefinition(testWorkspace, "external", contracts.EffectClassReversibleWrite)
	definition.RetryPolicy = contracts.CapabilityRetryPolicy{MaxAttempts: 3, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none", RetryableCodes: []string{"transport"}}
	definition.Ref.DefinitionHash = ""
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	fake := &fakeCapability{definition: definition, execute: func(_ context.Context, _ contracts.OperationRequest, _ contracts.OperationPlan) (contracts.OperationResult, error) {
		attempts.Add(1)
		return contracts.OperationResult{}, &connector.FailureError{Code: "transport", Retryable: true, ExternalEffectStarted: true}
	}}
	registry := connector.NewRegistry()
	if err := registry.Register(&fakeConnector{ref: definition.Ref.Connector, capability: fake}); err != nil {
		t.Fatal(err)
	}
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), fakeRequest(definition), connector.AdmissionOptions{}); err == nil {
		t.Fatal("external failure unexpectedly succeeded")
	}
	if attempts.Load() != 1 {
		t.Fatalf("external failure attempts = %d, want 1", attempts.Load())
	}
}

func TestExecutorHonorsCancellation(t *testing.T) {
	definition := testDefinition(testWorkspace, "cancel", contracts.EffectClassReadOnly)
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	fake := &fakeCapability{definition: definition, execute: func(ctx context.Context, _ contracts.OperationRequest, _ contracts.OperationPlan) (contracts.OperationResult, error) {
		<-ctx.Done()
		return contracts.OperationResult{}, ctx.Err()
	}}
	registry := connector.NewRegistry()
	if err := registry.Register(&fakeConnector{ref: definition.Ref.Connector, capability: fake}); err != nil {
		t.Fatal(err)
	}
	request := fakeRequest(definition)
	request.Profile.TimeoutMS = 10
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("cancelled capability unexpectedly succeeded")
	}
}

func TestExecutorRejectsOutputSchemaMismatch(t *testing.T) {
	definition := testDefinition(testWorkspace, "schema-output", contracts.EffectClassReadOnly)
	fake := &fakeCapability{definition: definition, execute: func(_ context.Context, request contracts.OperationRequest, plan contracts.OperationPlan) (contracts.OperationResult, error) {
		result := successResult(request, plan)
		result.OutputSchemaHash = repository.HashBytes([]byte("wrong-output-schema"))
		result.Steps[0].OutputSchemaHash = result.OutputSchemaHash
		return result, nil
	}}
	registry := connector.NewRegistry()
	if err := registry.Register(&fakeConnector{ref: definition.Ref.Connector, capability: fake}); err != nil {
		t.Fatal(err)
	}
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), fakeRequest(definition), connector.AdmissionOptions{}); err == nil {
		t.Fatal("output schema mismatch unexpectedly succeeded")
	}
}

func BenchmarkRegistryLookup(b *testing.B) {
	adapter, err := repository.NewConnector(testWorkspace, "1", readyInspection)
	if err != nil {
		b.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		b.Fatal(err)
	}
	request := benchmarkRequest(adapter.Capabilities()[0].Definition())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := registry.Lookup(request.Capability); !ok {
			b.Fatal("registered capability was not found")
		}
	}
}

func BenchmarkRegistryAdmissionAndPlan(b *testing.B) {
	adapter, err := repository.NewConnector(testWorkspace, "1", readyInspection)
	if err != nil {
		b.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		b.Fatal(err)
	}
	request := benchmarkRequest(adapter.Capabilities()[0].Definition())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		admission, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := admission.Capability.Plan(admission.Request); err != nil {
			b.Fatal(err)
		}
	}
}

func readyInspection(_ context.Context, request contracts.OperationRequest) (repository.Inspection, error) {
	return repository.Inspection{
		OutputHash: repository.HashBytes([]byte(request.Target.ID)),
		Evidence:   []contracts.OperationEvidenceRef{{WorkspaceID: request.WorkspaceID, SourceReference: "repo:" + request.Target.ID, EvidenceHash: repository.HashBytes([]byte("evidence")), Role: "snapshot"}},
	}, nil
}

func repositoryRequest(t *testing.T, definition contracts.CapabilityDefinition) contracts.OperationRequest {
	t.Helper()
	return contracts.OperationRequest{
		ID: "operation-repository-1", RequestID: "request-repository-1", IdempotencyKey: "idempotency-repository-1",
		WorkspaceID: testWorkspace, Actor: contracts.ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: testWorkspace},
		Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: testWorkspace, System: contracts.SystemRef{WorkspaceID: testWorkspace, Type: "repository", ID: "repo-1", Version: "1"}, Kind: "repository", ID: "repo-1", Version: "v1"},
		InputType: repository.CapabilityInputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: repository.HashBytes([]byte("input")), Profile: definition.Profile,
	}
}

func benchmarkRequest(definition contracts.CapabilityDefinition) contracts.OperationRequest {
	return contracts.OperationRequest{
		ID: "operation-benchmark", RequestID: "request-benchmark", IdempotencyKey: "idempotency-benchmark",
		WorkspaceID: testWorkspace, Actor: contracts.ActorRef{ID: "actor-benchmark", Kind: "operator", WorkspaceID: testWorkspace},
		Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: testWorkspace, System: contracts.SystemRef{WorkspaceID: testWorkspace, Type: "repository", ID: "repo-benchmark", Version: "1"}, Kind: "repository", ID: "repo-benchmark", Version: "1"},
		InputType: repository.CapabilityInputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: repository.HashBytes([]byte("benchmark-input")), Profile: definition.Profile,
	}
}

func testDefinition(workspace, name string, effect contracts.EffectClass) contracts.CapabilityDefinition {
	ref := contracts.ConnectorRef{WorkspaceID: workspace, Name: "fake-" + name, Version: "1"}
	definition := contracts.CapabilityDefinition{
		WorkspaceID: workspace, Ref: contracts.CapabilityRef{WorkspaceID: workspace, Connector: ref, Name: name, Version: "1"},
		InputSchemaVersion: 1, InputSchemaHash: repository.HashBytes([]byte(name + ":input")), OutputSchemaVersion: 1, OutputSchemaHash: repository.HashBytes([]byte(name + ":output")),
		Effect: effect, ResourceKinds: []string{"resource"}, Profile: contracts.DefaultExecutionProfile(), RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"}, Enabled: true,
	}
	if err := definition.Normalize(); err != nil {
		panic(err)
	}
	return definition
}

func fakeRequest(definition contracts.CapabilityDefinition) contracts.OperationRequest {
	return contracts.OperationRequest{
		ID: "operation-" + definition.Ref.Name, RequestID: "request-" + definition.Ref.Name, IdempotencyKey: "idempotency-" + definition.Ref.Name,
		WorkspaceID: definition.WorkspaceID, Actor: contracts.ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: definition.WorkspaceID}, Capability: definition.Ref,
		Target:    contracts.ResourceRef{WorkspaceID: definition.WorkspaceID, System: contracts.SystemRef{WorkspaceID: definition.WorkspaceID, Type: "fake", ID: "system-1", Version: "1"}, Kind: "resource", ID: "resource-1", Version: "1"},
		InputType: "fake.input", InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: repository.HashBytes([]byte("input")), Profile: definition.Profile,
	}
}

func successResult(request contracts.OperationRequest, plan contracts.OperationPlan) contracts.OperationResult {
	outputHash := repository.HashBytes([]byte("output:" + request.ID))
	definition := request.Capability
	return contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: 1, OutputSchemaHash: repository.HashBytes([]byte(definition.Name + ":output")), OutputHash: outputHash, Steps: []contracts.OperationStepResult{{StepID: plan.Steps[0].ID, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: 1, OutputSchemaHash: repository.HashBytes([]byte(definition.Name + ":output")), OutputHash: outputHash}}}
}

type fakeConnector struct {
	ref        contracts.ConnectorRef
	capability *fakeCapability
}

func (c *fakeConnector) Definition() contracts.ConnectorRef { return c.ref }
func (c *fakeConnector) Capabilities() []connector.Capability {
	return []connector.Capability{c.capability}
}
func (c *fakeConnector) Health(context.Context) connector.HealthStatus {
	return connector.HealthStatus{Status: connector.HealthReady}
}

type fakeCapability struct {
	definition contracts.CapabilityDefinition
	execute    func(context.Context, contracts.OperationRequest, contracts.OperationPlan) (contracts.OperationResult, error)
}

func (c *fakeCapability) Definition() contracts.CapabilityDefinition { return c.definition }
func (c *fakeCapability) Validate(request contracts.OperationRequest) error {
	return contracts.ValidateOperationRequest(request, c.definition)
}
func (c *fakeCapability) Plan(request contracts.OperationRequest) (contracts.OperationPlan, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationPlan{}, err
	}
	plan := contracts.OperationPlan{ID: request.ID + "-plan", OperationID: request.ID, OperationHash: request.StableHash(), WorkspaceID: request.WorkspaceID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: request.ID + "-step", Ordinal: 0, Kind: "fake", Capability: request.Capability, Target: request.Target, Effect: c.definition.Effect, Profile: request.Profile, InputHash: request.InputHash}}}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationPlan{}, err
	}
	return plan, nil
}
func (c *fakeCapability) Execute(ctx context.Context, request contracts.OperationRequest, plan contracts.OperationPlan) (contracts.OperationResult, error) {
	if c.execute != nil {
		return c.execute(ctx, request, plan)
	}
	return successResult(request, plan), nil
}
