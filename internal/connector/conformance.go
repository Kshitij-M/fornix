package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

// ConformanceFailure identifies one reusable registry/adapter qualification
// failure. The suite intentionally reports bounded names and never includes
// request payloads, credentials, or connector errors verbatim.
type ConformanceFailure struct {
	Case string
	Err  error
}

// RunConformanceSuite exercises the common guarantees every built-in
// capability must satisfy. It is intentionally fake-first and side-effect
// free for read-only capabilities; external adapters should provide recorded
// responses rather than live calls when using this suite during replay.
func RunConformanceSuite(ctx context.Context, registry *Registry, request contracts.OperationRequest, options AdmissionOptions) []ConformanceFailure {
	failures := make([]ConformanceFailure, 0)
	add := func(name string, err error) {
		if err != nil {
			failures = append(failures, ConformanceFailure{Case: name, Err: err})
		}
	}
	if registry == nil {
		return []ConformanceFailure{{Case: "registry-configured", Err: ErrRegistryNil}}
	}
	capability, ok := registry.Lookup(request.Capability)
	if !ok {
		return []ConformanceFailure{{Case: "exact-capability-lookup", Err: ErrCapabilityNotFound}}
	}
	add("identity-lookup", func() error {
		identity, found := registry.LookupIdentity(request.WorkspaceID, request.Capability.Connector.Name, request.Capability.Connector.Version, request.Capability.Name, request.Capability.Version)
		if !found || identity.Definition().Ref.DefinitionHash != capability.Definition().Ref.DefinitionHash {
			return fmt.Errorf("identity lookup did not return the registered definition")
		}
		return nil
	}())
	add("admission", func() error {
		_, err := registry.Admit(ctx, request, options)
		return err
	}())
	first, err := (&Executor{Registry: registry}).Execute(ctx, request, options)
	add("execution", err)
	if err == nil {
		definition := capability.Definition()
		add("result-normalization", validateResult(request, definition, first.Result))
		if (definition.Effect == contracts.EffectClassReadOnly || definition.Effect == contracts.EffectClassObservation) && len(first.Result.ExternalEffects) > 0 {
			add("read-only-external-effects", fmt.Errorf("read-only capability returned external effects"))
		}
		if definition.Effect == contracts.EffectClassReadOnly || definition.Effect == contracts.EffectClassObservation {
			// Replay is deliberately evaluated from the recorded result. Calling
			// an external connector again would turn a conformance check into an
			// unbounded external effect. Durable duplicate suppression belongs to
			// the operation authority, while this check proves hash stability.
			add("duplicate-read-replay", validateResult(request, definition, first.Result))
			if first.Result.StableHash() == "" {
				add("duplicate-read-hash", fmt.Errorf("recorded read result has no stable hash"))
			}
		}
	}
	add("definition-fence", func() error {
		stale := request
		stale.Capability.DefinitionHash = strings.Repeat("0", 64)
		if _, err := registry.Admit(ctx, stale, options); err == nil {
			return fmt.Errorf("stale capability definition was admitted")
		}
		return nil
	}())
	add("workspace-fence", func() error {
		cross := request
		cross.Target.WorkspaceID = request.WorkspaceID + "-other"
		if _, err := registry.Admit(ctx, cross, options); err == nil {
			return fmt.Errorf("cross-workspace target was admitted")
		}
		return nil
	}())
	add("redacted-contract", func() error {
		if err := request.Normalize(); err != nil {
			return err
		}
		raw, err := json.Marshal(request)
		if err != nil {
			return err
		}
		lower := strings.ToLower(string(raw))
		for _, forbidden := range []string{"credential_value", "secret_value", "authorization", "prompt", "raw_input"} {
			if strings.Contains(lower, forbidden) {
				return fmt.Errorf("contract contains forbidden field %q", forbidden)
			}
		}
		return nil
	}())
	return failures
}
