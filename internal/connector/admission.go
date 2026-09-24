package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

// CredentialChecker proves availability of an opaque credential reference.
// The value itself must never be returned to or stored by the registry.
type CredentialChecker func(context.Context, string, string) (bool, error)

// Authorizer is the integration seam for the durable identity/RBAC and future
// universal policy layers. The registry enforces workspace and capability
// identity; the policy package owns actor-specific permissions and approvals.
type Authorizer func(context.Context, contracts.Principal, contracts.OperationRequest, contracts.CapabilityDefinition) (bool, error)

// AdmissionOptions contains non-secret facts supplied by the control plane.
// A missing fact fails closed when the registered capability requires it.
type AdmissionOptions struct {
	Principal       *contracts.Principal
	ApprovalGranted bool
	Credentials     map[string]bool
	CheckCredential CredentialChecker
	Authorize       Authorizer
}

// Admission is the normalized, immutable view passed to the execution
// wrapper. It contains metadata and references only, never raw input or
// credential material.
type Admission struct {
	Request    contracts.OperationRequest
	Definition contracts.CapabilityDefinition
	Capability Capability
	Health     HealthStatus
}

// Admit resolves and validates one capability invocation. It performs no
// database writes and no external work. Durable authorization, approval,
// idempotency, fencing, and lifecycle records belong to later control-plane
// layers, but callers can use the supplied seams here today.
func (r *Registry) Admit(ctx context.Context, request contracts.OperationRequest, options AdmissionOptions) (Admission, error) {
	if r == nil {
		return Admission{}, ErrRegistryNil
	}
	if err := ctx.Err(); err != nil {
		return Admission{}, err
	}
	if err := request.Normalize(); err != nil {
		return Admission{}, fmt.Errorf("normalize operation request: %w", err)
	}
	var principal contracts.Principal
	capability, ok := r.Lookup(request.Capability)
	if !ok {
		return Admission{}, fmt.Errorf("%w: %s", ErrCapabilityNotFound, capabilityIdentityKey(request.Capability))
	}
	definition := capability.Definition()
	if !definition.Enabled {
		return Admission{}, ErrCapabilityDisabled
	}
	if policy, trusted := r.TrustPolicy(request.WorkspaceID); trusted {
		if err := policy.Authorize(definition); err != nil {
			return Admission{}, fmt.Errorf("%w: %v", ErrCapabilityUntrusted, err)
		}
	} else if r.isTrustRequired() {
		return Admission{}, ErrTrustPolicyMissing
	}
	health := r.Health(ctx, request.Capability.Connector)
	if health.Status != HealthReady {
		reason := strings.TrimSpace(health.Reason)
		if reason == "" {
			reason = "connector is not ready"
		}
		return Admission{}, fmt.Errorf("%w: %s", ErrConnectorUnavailable, reason)
	}
	if options.Principal != nil {
		normalized, err := options.Principal.Normalize()
		if err != nil {
			return Admission{}, fmt.Errorf("principal: %w", err)
		}
		principal = normalized
		if !principal.Authenticated || principal.WorkspaceID != request.WorkspaceID || principal.ID != request.Actor.ID {
			return Admission{}, ErrUnauthorized
		}
	}
	if options.Authorize != nil {
		if options.Principal == nil {
			return Admission{}, fmt.Errorf("%w: authenticated principal is required", ErrUnauthorized)
		}
		allowed, err := options.Authorize(ctx, principal, request, definition)
		if err != nil {
			return Admission{}, fmt.Errorf("authorize capability: %w", err)
		}
		if !allowed {
			return Admission{}, ErrUnauthorized
		}
	}
	if definition.RequiresApproval && !options.ApprovalGranted {
		return Admission{}, ErrApprovalRequired
	}
	for _, reference := range definition.RequiredCredentialRefs {
		available, err := credentialAvailable(ctx, options, request.WorkspaceID, reference)
		if err != nil {
			return Admission{}, fmt.Errorf("check credential reference %q: %w", reference, err)
		}
		if !available {
			return Admission{}, fmt.Errorf("%w: %s", ErrCredentialUnavailable, reference)
		}
	}
	if err := capability.Validate(request); err != nil {
		return Admission{}, fmt.Errorf("validate capability input: %w", err)
	}
	return Admission{Request: request, Definition: definition, Capability: capability, Health: health}, nil
}

func credentialAvailable(ctx context.Context, options AdmissionOptions, workspaceID, reference string) (bool, error) {
	if options.CheckCredential != nil {
		return options.CheckCredential(ctx, workspaceID, reference)
	}
	if options.Credentials == nil {
		return false, nil
	}
	return options.Credentials[strings.TrimSpace(reference)], nil
}
