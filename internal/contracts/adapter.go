package contracts

import "fmt"

// OperationAdapter is the deterministic boundary owned by a domain adapter.
// It validates a typed request and produces a bounded plan; it does not execute
// external work, load plugins, resolve credentials, or persist state. Those
// responsibilities stay with the future admission/runtime layers.
type OperationAdapter interface {
	Definition() CapabilityDefinition
	Validate(request OperationRequest) error
	Plan(request OperationRequest) (OperationPlan, error)
}

// ValidateOperationRequest binds an operation request to one immutable
// capability definition before execution code can see it. A request may
// tighten the definition's profile, but it may never widen a budget or enable
// an external effect that the capability does not explicitly allow.
func ValidateOperationRequest(request OperationRequest, definition CapabilityDefinition) error {
	if err := definition.Normalize(); err != nil {
		return fmt.Errorf("capability definition: %w", err)
	}
	if err := request.Normalize(); err != nil {
		return fmt.Errorf("operation request: %w", err)
	}
	if !definition.Enabled {
		return fmt.Errorf("capability is disabled")
	}
	if !definition.Matches(request.Capability) {
		return fmt.Errorf("operation capability is unknown or does not match its definition")
	}
	if len(definition.ResourceKinds) == 0 {
		return fmt.Errorf("capability has no registered resource kinds")
	}
	allowedKind := false
	for _, kind := range definition.ResourceKinds {
		if request.Target.Kind == kind {
			allowedKind = true
			break
		}
	}
	if !allowedKind {
		return fmt.Errorf("resource kind %q is not supported by capability", request.Target.Kind)
	}
	if request.Profile.AllowExternalEffects && !definition.Profile.AllowExternalEffects {
		return fmt.Errorf("request cannot enable external effects for this capability")
	}
	if !executionProfileWithin(request.Profile, definition.Profile) {
		return fmt.Errorf("request execution profile exceeds capability limits")
	}
	return nil
}

func executionProfileWithin(request, maximum ExecutionProfile) bool {
	return request.MaxSteps <= maximum.MaxSteps &&
		request.MaxConcurrency <= maximum.MaxConcurrency &&
		request.TimeoutMS <= maximum.TimeoutMS &&
		request.MaxInputBytes <= maximum.MaxInputBytes &&
		request.MaxOutputBytes <= maximum.MaxOutputBytes &&
		request.MaxOutputTokens <= maximum.MaxOutputTokens &&
		request.MaxCostUSD <= maximum.MaxCostUSD &&
		request.MaxRetries <= maximum.MaxRetries &&
		request.MaxDisclosureBytes <= maximum.MaxDisclosureBytes &&
		request.MaxDisclosureItems <= maximum.MaxDisclosureItems &&
		request.MaxDisclosureTokens <= maximum.MaxDisclosureTokens &&
		request.MaxCancellationGraceMS <= maximum.MaxCancellationGraceMS
}
