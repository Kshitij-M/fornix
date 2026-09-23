package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func domainTestHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func domainTestCapability(t *testing.T, workspace string, effect EffectClass) CapabilityDefinition {
	t.Helper()
	definition := CapabilityDefinition{
		WorkspaceID: workspace,
		Ref: CapabilityRef{
			WorkspaceID: workspace,
			Connector:   ConnectorRef{WorkspaceID: workspace, Name: "repository", Version: "1"},
			Name:        "inspect", Version: "1",
		},
		InputSchemaVersion:   1,
		InputSchemaHash:      domainTestHash("repository.inspect.input"),
		OutputSchemaVersion:  1,
		OutputSchemaHash:     domainTestHash("repository.inspect.output"),
		Effect:               effect,
		ResourceKinds:        []string{"repository"},
		SupportsIdempotency:  true,
		SupportsVerification: true,
		Enabled:              true,
	}
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	return definition
}

func domainTestResource(workspace, systemType, systemID, kind, id string) ResourceRef {
	return ResourceRef{
		WorkspaceID: workspace,
		System:      SystemRef{WorkspaceID: workspace, Type: systemType, ID: systemID, Version: "1"},
		Kind:        kind, ID: id, Version: "v1", ContentHash: domainTestHash(id),
	}
}

func domainTestOperation(t *testing.T, workspace, requestID, idempotency string) OperationRequest {
	t.Helper()
	definition := domainTestCapability(t, workspace, EffectClassReadOnly)
	return OperationRequest{
		ID: idempotency + "-operation", RequestID: requestID, IdempotencyKey: idempotency,
		WorkspaceID: workspace, Actor: ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: workspace},
		Capability: definition.Ref, Target: domainTestResource(workspace, "repository", "repo-1", "repository", "repo-1"),
		InputType: "repository.inspect", InputSchemaVersion: 1, InputSchemaHash: definition.InputSchemaHash, InputHash: domainTestHash("input"),
		Profile: DefaultExecutionProfile(), Metadata: map[string]string{"region": "us-east-1"},
	}
}

func TestDomainNeutralHashesExcludeDeliveryIdentity(t *testing.T) {
	one := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	two := domainTestOperation(t, "workspace-a", "request-b", "idempotency-b")
	two.ID, two.CausationID, two.CorrelationID = "operation-b", "event-b", "trace-b"
	oneHash, err := one.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	twoHash, err := two.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	if oneHash == "" || oneHash != twoHash {
		t.Fatalf("delivery identity changed operation hash: %q != %q", oneHash, twoHash)
	}
	resource := domainTestResource("workspace-a", "repository", "repo-1", "repository", "repo-1")
	if resource.StableHash() != resource.StableHash() {
		t.Fatal("resource hash was not stable")
	}
}

func TestDomainNeutralDefinitionAndReferencesAreVersioned(t *testing.T) {
	definition := domainTestCapability(t, "workspace-a", EffectClassObservation)
	if definition.Ref.DefinitionHash == "" || definition.StableHash() != definition.Ref.DefinitionHash {
		t.Fatalf("definition hash was not derived consistently: %+v", definition)
	}
	if !definition.Matches(definition.Ref) {
		t.Fatal("definition did not match its own registered reference")
	}
	unknown := definition.Ref
	unknown.DefinitionHash = ""
	if err := unknown.Normalize(); err == nil {
		t.Fatal("unregistered capability reference was accepted")
	}
	badEffect := CapabilityDefinition{
		WorkspaceID: "workspace-a", Ref: definition.Ref, InputSchemaVersion: 1,
		InputSchemaHash: domainTestHash("in"), OutputSchemaVersion: 1,
		OutputSchemaHash: domainTestHash("out"), Effect: EffectClassUnknown,
	}
	if err := badEffect.Normalize(); err == nil {
		t.Fatal("unknown effect class was accepted")
	}
	badResource := domainTestResource("workspace-a", "repository", "repo-1", "unknown", "repo-1")
	if err := badResource.Normalize(); err == nil {
		t.Fatal("unknown resource kind was accepted")
	}
}

func TestDomainNeutralRejectsCrossWorkspaceReferences(t *testing.T) {
	request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Target.WorkspaceID = "workspace-b"
	if err := request.Normalize(); err == nil {
		t.Fatal("cross-workspace target was accepted")
	}
	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Actor.WorkspaceID = "workspace-b"
	if err := request.Normalize(); err == nil {
		t.Fatal("cross-workspace actor was accepted")
	}
	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Capability.WorkspaceID = "workspace-b"
	if err := request.Normalize(); err == nil {
		t.Fatal("cross-workspace capability was accepted")
	}
}

func TestDomainNeutralPlanCanonicalizesAndRejectsCycles(t *testing.T) {
	request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	stepA := OperationStep{ID: "step-a", Ordinal: 1, Kind: "inspect", Capability: request.Capability, Target: request.Target, Effect: EffectClassReadOnly, InputHash: request.InputHash}
	stepB := OperationStep{ID: "step-b", Ordinal: 0, Kind: "inspect", Capability: request.Capability, Target: request.Target, Effect: EffectClassReadOnly, InputHash: request.InputHash, DependsOn: []string{"step-a"}}
	plan := OperationPlan{ID: "plan-1", OperationID: request.ID, OperationHash: request.StableHash(), WorkspaceID: request.WorkspaceID, Actor: request.Actor, Steps: []OperationStep{stepA, stepB}}
	if err := plan.Normalize(); err != nil {
		t.Fatal(err)
	}
	if plan.Steps[0].ID != "step-b" || plan.StableHash() == "" {
		t.Fatalf("plan was not deterministically ordered: %+v", plan.Steps)
	}
	plan.Steps[0].DependsOn = []string{"step-a"}
	plan.Steps[1].DependsOn = []string{"step-b"}
	if err := plan.Normalize(); err == nil {
		t.Fatal("cyclic operation plan was accepted")
	}
}

func TestDomainNeutralExternalEffectsNeverClaimExactlyOnce(t *testing.T) {
	effect := ExternalEffect{
		WorkspaceID: "workspace-a", Boundary: "billing-api", Class: EffectClassReversibleWrite,
		DeliveryGuarantee: ExternalDeliveryAtLeastOnce, ExactlyOnceClaimed: true,
	}
	if err := effect.Normalize(); err == nil {
		t.Fatal("exactly-once external claim was accepted")
	}
	effect.ExactlyOnceClaimed = false
	if err := effect.Normalize(); err != nil {
		t.Fatal(err)
	}
	if effect.DeliveryGuarantee != ExternalDeliveryAtLeastOnce || effect.StableHash() == "" {
		t.Fatalf("at-least-once semantics were not preserved: %+v", effect)
	}
}

func TestOperationLifecycleIsExplicitAndTerminal(t *testing.T) {
	valid := [][2]string{
		{OperationStatusCreated, OperationStatusPlanned},
		{OperationStatusPlanned, OperationStatusAdmitted},
		{OperationStatusAdmitted, OperationStatusRunning},
		{OperationStatusRunning, OperationStatusAwaitingRetry},
		{OperationStatusAwaitingRetry, OperationStatusRunning},
		{OperationStatusRunning, OperationStatusVerifying},
		{OperationStatusVerifying, OperationStatusSucceeded},
		{OperationStatusRunning, OperationStatusRecoveryRequired},
		{OperationStatusRecoveryRequired, OperationStatusDeadLetter},
	}
	for _, transition := range valid {
		if !CanTransitionOperation(transition[0], transition[1]) {
			t.Fatalf("expected transition %q -> %q to be valid", transition[0], transition[1])
		}
	}
	invalid := [][2]string{
		{OperationStatusCreated, OperationStatusRunning},
		{OperationStatusPlanned, OperationStatusSucceeded},
		{OperationStatusAwaitingExternal, OperationStatusRunning},
		{OperationStatusSucceeded, OperationStatusRunning},
		{OperationStatusFailed, OperationStatusCancelled},
		{"unknown", OperationStatusRunning},
	}
	for _, transition := range invalid {
		if CanTransitionOperation(transition[0], transition[1]) {
			t.Fatalf("expected transition %q -> %q to be rejected", transition[0], transition[1])
		}
	}
	for _, status := range []string{OperationStatusSucceeded, OperationStatusFailed, OperationStatusCancelled, OperationStatusDeadLetter, OperationStatusAbstained} {
		if !IsTerminalOperationStatus(status) {
			t.Fatalf("expected %q to be terminal", status)
		}
	}
}

func TestDomainNeutralMetadataAndSchemaFailClosed(t *testing.T) {
	request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Metadata = map[string]string{"prompt": "do not persist this"}
	if err := request.Normalize(); err == nil {
		t.Fatal("prompt metadata was accepted")
	}
	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Metadata = map[string]string{"label": "raw prompt with spaces"}
	if err := request.Normalize(); err == nil {
		t.Fatal("arbitrary text metadata was accepted")
	}
	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.SchemaVersion = DomainNeutralSchemaVersion + 1
	if err := request.Normalize(); err == nil {
		t.Fatal("unsupported operation schema was accepted")
	}
	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.InputSchemaVersion = MaxDomainSchemaVersion + 1
	if err := request.Normalize(); err == nil {
		t.Fatal("unbounded input schema version was accepted")
	}
}

func TestDomainNeutralNormalizationCanonicalizesSafeMetadataAndReferences(t *testing.T) {
	request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Metadata = map[string]string{" Region ": " us-east-1 "}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	if request.Metadata["region"] != "us-east-1" || len(request.Metadata) != 1 {
		t.Fatalf("metadata was not canonicalized: %#v", request.Metadata)
	}
	canonical := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	canonical.Metadata = map[string]string{"region": "us-east-1"}
	if request.StableHash() != canonical.StableHash() {
		t.Fatal("equivalent normalized metadata changed the operation hash")
	}

	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Metadata = map[string]string{"region": "us-east-1", " Region ": "us-west-2"}
	if err := request.Normalize(); err == nil {
		t.Fatal("metadata key collision after normalization was accepted")
	}

	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Task = &EntityRef{ID: "task\n1", Kind: "task", WorkspaceID: request.WorkspaceID}
	if err := request.Normalize(); err == nil {
		t.Fatal("malformed task identity was accepted")
	}
}

func TestDomainNeutralRepositoryAndNonRepositoryRoundTrip(t *testing.T) {
	for _, resource := range []ResourceRef{
		domainTestResource("workspace-a", "repository", "repo-1", "repository", "repo-1"),
		domainTestResource("workspace-a", "billing", "billing-prod", "invoice", "invoice-42"),
	} {
		request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
		request.Target = resource
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var roundTrip OperationRequest
		if err := json.Unmarshal(raw, &roundTrip); err != nil {
			t.Fatal(err)
		}
		before, err := request.CanonicalHash()
		if err != nil {
			t.Fatal(err)
		}
		if err := roundTrip.Normalize(); err != nil {
			t.Fatal(err)
		}
		after, err := roundTrip.CanonicalHash()
		if err != nil || before != after {
			t.Fatalf("round-trip changed operation hash: %q != %q (err=%v)", before, after, err)
		}
	}
}

func TestDomainNeutralWorkReceiptCompatibilityAndOperationLink(t *testing.T) {
	base, err := testWorkReceiptRequest().ToReceipt(time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	withoutOperation := base.StableHash()
	linkedRequest := testWorkReceiptRequest()
	linkedRequest.Operation = &OperationReference{WorkspaceID: linkedRequest.WorkspaceID, ID: "operation-1", Hash: domainTestHash("operation")}
	linked, err := linkedRequest.ToReceipt(time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if linked.StableHash() == withoutOperation || linked.Operation == nil {
		t.Fatal("generic operation link did not affect receipt identity")
	}
	legacy, err := testWorkReceiptRequest().ToReceipt(time.Unix(9, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if legacy.StableHash() != base.StableHash() || legacy.RequestContentHash() != base.RequestContentHash() {
		t.Fatal("empty generic fields changed legacy receipt hashes")
	}
}

func TestDomainNeutralResultRequiresFailureForFailedStatus(t *testing.T) {
	request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	result := OperationResult{
		ID: "result-1", OperationID: request.ID, OperationHash: request.StableHash(),
		WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: OperationStatusFailed,
	}
	if err := result.Normalize(); err == nil {
		t.Fatal("failed result without failure was accepted")
	}
	result.Failure = &OperationFailure{WorkspaceID: request.WorkspaceID, Code: OperationFailureAdapter, Attempts: 1}
	if err := result.Normalize(); err != nil {
		t.Fatal(err)
	}
	if result.StableHash() == "" {
		t.Fatal("normalized result has no stable hash")
	}
}

func TestDomainNeutralResultHashCanonicalizesStepOrder(t *testing.T) {
	request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	first := OperationResult{
		ID: "result-1", OperationID: request.ID, OperationHash: request.StableHash(),
		WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: OperationStatusSucceeded,
		Steps: []OperationStepResult{{StepID: "step-b", Status: OperationStatusSucceeded}, {StepID: "step-a", Status: OperationStatusSucceeded}},
	}
	second := first
	second.Steps = []OperationStepResult{{StepID: "step-a", Status: OperationStatusSucceeded}, {StepID: "step-b", Status: OperationStatusSucceeded}}
	if first.StableHash() == "" || first.StableHash() != second.StableHash() {
		t.Fatal("result step order changed the canonical hash")
	}
}

func TestDomainNeutralAdapterAdmissionFailsClosedAndAllowsTightening(t *testing.T) {
	definition := domainTestCapability(t, "workspace-a", EffectClassReadOnly)
	request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Profile.MaxSteps = 8
	if err := ValidateOperationRequest(request, definition); err != nil {
		t.Fatal(err)
	}
	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Target = domainTestResource("workspace-a", "billing", "billing-prod", "invoice", "invoice-1")
	if err := ValidateOperationRequest(request, definition); err == nil {
		t.Fatal("unsupported resource kind was admitted")
	}
	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Capability.DefinitionHash = domainTestHash("not-this-definition")
	if err := ValidateOperationRequest(request, definition); err == nil {
		t.Fatal("unknown capability definition was admitted")
	}
	request = domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	request.Profile.MaxSteps = definition.Profile.MaxSteps + 1
	if err := ValidateOperationRequest(request, definition); err == nil {
		t.Fatal("request that widened capability budget was admitted")
	}
}

func TestDomainNeutralPublicJSONDoesNotContainRawInputField(t *testing.T) {
	request := domainTestOperation(t, "workspace-a", "request-a", "idempotency-a")
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "prompt") || strings.Contains(string(raw), "parameters") {
		t.Fatalf("operation contract exposed raw input vocabulary: %s", raw)
	}
}

func BenchmarkOperationRequestCanonicalHash(b *testing.B) {
	request := domainTestOperationForBenchmark()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := request.CanonicalHash(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOperationPlanStableHash(b *testing.B) {
	request := domainTestOperationForBenchmark()
	if err := request.Normalize(); err != nil {
		b.Fatal(err)
	}
	plan := OperationPlan{
		ID: "plan-1", OperationID: request.ID, OperationHash: request.StableHash(),
		WorkspaceID: request.WorkspaceID, Actor: request.Actor,
		Steps: []OperationStep{{ID: "step-1", Ordinal: 0, Kind: "inspect", Capability: request.Capability, Target: request.Target, Effect: EffectClassReadOnly, InputHash: request.InputHash}},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if plan.StableHash() == "" {
			b.Fatal("plan hash is empty")
		}
	}
}

func domainTestOperationForBenchmark() OperationRequest {
	definition := CapabilityDefinition{
		WorkspaceID: "workspace-a",
		Ref: CapabilityRef{
			WorkspaceID: "workspace-a",
			Connector:   ConnectorRef{WorkspaceID: "workspace-a", Name: "repository", Version: "1"},
			Name:        "inspect", Version: "1",
		},
		InputSchemaVersion: 1, InputSchemaHash: domainTestHash("in"),
		OutputSchemaVersion: 1, OutputSchemaHash: domainTestHash("out"),
		Effect: EffectClassReadOnly, ResourceKinds: []string{"repository"}, Enabled: true,
	}
	if err := definition.Normalize(); err != nil {
		panic(err)
	}
	return OperationRequest{
		ID: "operation-1", RequestID: "request-1", IdempotencyKey: "idempotency-1",
		WorkspaceID: "workspace-a", Actor: ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: "workspace-a"},
		Capability: definition.Ref, Target: domainTestResource("workspace-a", "repository", "repo-1", "repository", "repo-1"),
		InputType: "repository.inspect", InputSchemaVersion: 1, InputSchemaHash: definition.InputSchemaHash, InputHash: domainTestHash("input"),
		Profile: DefaultExecutionProfile(),
	}
}
