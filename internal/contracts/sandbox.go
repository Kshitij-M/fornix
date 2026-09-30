package contracts

import (
	"errors"
	"sort"
	"strings"
)

var (
	errInvalidSandboxBackend            = errors.New("invalid sandbox backend")
	errUnknownSandboxCapability         = errors.New("unknown sandbox capability")
	errContradictorySandboxCapabilities = errors.New("sandbox capability is both enforced and not enforced")
)

// SandboxBackend identifies an execution boundary. A backend name is a
// selection, not proof that its advertised controls are available on a host.
type SandboxBackend string

const (
	SandboxBackendLocalProcess SandboxBackend = "local-process"
	SandboxBackendOCI          SandboxBackend = "oci-container"
	SandboxBackendGVisor       SandboxBackend = "gvisor"
	SandboxBackendMicroVM      SandboxBackend = "microvm"
)

// SandboxCapability names one independently enforceable process-boundary
// guarantee. Callers must not infer a capability from another capability.
type SandboxCapability string

const (
	SandboxCapabilityStructuredArgv          SandboxCapability = "structured-argv"
	SandboxCapabilityExplicitEnvironment     SandboxCapability = "explicit-environment"
	SandboxCapabilityWallTimeLimit           SandboxCapability = "wall-time-limit"
	SandboxCapabilityProcessGroupTermination SandboxCapability = "process-group-termination"
	SandboxCapabilityProcessTreeTermination  SandboxCapability = "process-tree-termination"
	SandboxCapabilityOutputByteLimit         SandboxCapability = "output-byte-limit"
	SandboxCapabilityArgumentBudget          SandboxCapability = "argument-budget"
	SandboxCapabilityEnvironmentBudget       SandboxCapability = "environment-budget"
	SandboxCapabilityWorkdirPathPreflight    SandboxCapability = "workdir-path-preflight"
	SandboxCapabilityReadOnlyMount           SandboxCapability = "read-only-mount"
	SandboxCapabilityReadOnlyRootFS          SandboxCapability = "read-only-root-filesystem"
	SandboxCapabilityFilesystemIsolation     SandboxCapability = "filesystem-isolation"
	SandboxCapabilityNetworkIsolation        SandboxCapability = "network-isolation"
	SandboxCapabilityCPULimit                SandboxCapability = "cpu-limit"
	SandboxCapabilityMemoryLimit             SandboxCapability = "memory-limit"
	SandboxCapabilityProcessLimit            SandboxCapability = "process-limit"
	SandboxCapabilityScratchLimit            SandboxCapability = "scratch-limit"
	SandboxCapabilityDurableAttemptIdentity  SandboxCapability = "durable-attempt-identity"
	SandboxCapabilityCrashReconciliation     SandboxCapability = "crash-reconciliation"
)

// SandboxBackendCapabilities reports controls the selected provider enforces
// and controls it explicitly does not enforce. Available means the provider
// is configured and qualified by the embedding application; it is not an
// isolation certification.
type SandboxBackendCapabilities struct {
	Backend     SandboxBackend      `json:"backend"`
	Available   bool                `json:"available"`
	Enforced    []SandboxCapability `json:"enforced"`
	NotEnforced []SandboxCapability `json:"not_enforced"`
	Limitations []string            `json:"limitations,omitempty"`
}

// Normalize sorts and validates a capability report before it is used for
// deterministic discovery or admission.
func (c *SandboxBackendCapabilities) Normalize() error {
	if c == nil || !knownSandboxBackend(c.Backend) {
		return errInvalidSandboxBackend
	}
	var err error
	c.Enforced, err = normalizeSandboxCapabilities(c.Enforced)
	if err != nil {
		return err
	}
	c.NotEnforced, err = normalizeSandboxCapabilities(c.NotEnforced)
	if err != nil {
		return err
	}
	for _, enforced := range c.Enforced {
		for _, absent := range c.NotEnforced {
			if enforced == absent {
				return errContradictorySandboxCapabilities
			}
		}
	}
	if len(c.Limitations) > 16 {
		return errors.New("sandbox capability report has too many limitations")
	}
	limitations := make([]string, 0, len(c.Limitations))
	seenLimitations := make(map[string]struct{}, len(c.Limitations))
	for _, limitation := range c.Limitations {
		limitation = strings.TrimSpace(limitation)
		if limitation == "" || len(limitation) > 512 {
			return errors.New("sandbox capability limitation is empty or oversized")
		}
		if _, exists := seenLimitations[limitation]; exists {
			continue
		}
		seenLimitations[limitation] = struct{}{}
		limitations = append(limitations, limitation)
	}
	sort.Strings(limitations)
	c.Limitations = limitations
	return nil
}

// ControlHash binds the independently reported enforcement controls and
// bounded limitations. Availability is excluded because it is a live
// readiness signal checked separately, not a qualification result.
func (c SandboxBackendCapabilities) ControlHash() string {
	if err := c.Normalize(); err != nil {
		return ""
	}
	parts := []string{"sandbox-control-report-v1", string(c.Backend)}
	parts = append(parts, "enforced")
	for _, capability := range c.Enforced {
		parts = append(parts, string(capability))
	}
	parts = append(parts, "not-enforced")
	for _, capability := range c.NotEnforced {
		parts = append(parts, string(capability))
	}
	parts = append(parts, "limitations")
	parts = append(parts, c.Limitations...)
	return HashStrings(parts...)
}

// KnownSandboxBackends returns the stable backend vocabulary accepted by
// durable tool profiles. It does not imply that every backend is installed.
func KnownSandboxBackends() []SandboxBackend {
	return []SandboxBackend{SandboxBackendLocalProcess, SandboxBackendOCI, SandboxBackendGVisor, SandboxBackendMicroVM}
}

func knownSandboxBackend(backend SandboxBackend) bool {
	switch backend {
	case SandboxBackendLocalProcess, SandboxBackendOCI, SandboxBackendGVisor, SandboxBackendMicroVM:
		return true
	default:
		return false
	}
}

func knownSandboxCapability(capability SandboxCapability) bool {
	switch capability {
	case SandboxCapabilityStructuredArgv, SandboxCapabilityExplicitEnvironment,
		SandboxCapabilityWallTimeLimit, SandboxCapabilityProcessGroupTermination,
		SandboxCapabilityProcessTreeTermination,
		SandboxCapabilityOutputByteLimit,
		SandboxCapabilityArgumentBudget, SandboxCapabilityEnvironmentBudget,
		SandboxCapabilityWorkdirPathPreflight, SandboxCapabilityReadOnlyMount,
		SandboxCapabilityReadOnlyRootFS,
		SandboxCapabilityFilesystemIsolation, SandboxCapabilityNetworkIsolation,
		SandboxCapabilityCPULimit, SandboxCapabilityMemoryLimit,
		SandboxCapabilityProcessLimit, SandboxCapabilityScratchLimit,
		SandboxCapabilityDurableAttemptIdentity, SandboxCapabilityCrashReconciliation:
		return true
	default:
		return false
	}
}

// RequiredEnforcements derives the controls a provider must advertise for this
// normalized profile. A provider that lacks even one control is not admissible.
func (p SandboxProfile) RequiredEnforcements() ([]SandboxCapability, error) {
	if err := p.Normalize(); err != nil {
		return nil, err
	}
	required := append([]SandboxCapability(nil), p.RequiredCapabilities...)
	required = append(required,
		SandboxCapabilityStructuredArgv,
		SandboxCapabilityExplicitEnvironment,
		SandboxCapabilityWallTimeLimit,
		SandboxCapabilityOutputByteLimit,
		SandboxCapabilityArgumentBudget,
		SandboxCapabilityEnvironmentBudget,
	)
	if p.Backend == string(SandboxBackendLocalProcess) {
		required = append(required, SandboxCapabilityProcessGroupTermination)
		if p.ReadOnlyWorkdir {
			required = append(required, SandboxCapabilityWorkdirPathPreflight)
		}
	} else {
		required = append(required,
			SandboxCapabilityProcessTreeTermination,
			SandboxCapabilityReadOnlyRootFS,
			SandboxCapabilityReadOnlyMount,
			SandboxCapabilityFilesystemIsolation,
			SandboxCapabilityNetworkIsolation,
			SandboxCapabilityDurableAttemptIdentity,
			SandboxCapabilityCrashReconciliation,
		)
	}
	if p.CPUQuotaMilli > 0 {
		required = append(required, SandboxCapabilityCPULimit)
	}
	if p.MemoryBytes > 0 {
		required = append(required, SandboxCapabilityMemoryLimit)
	}
	if p.PIDsLimit > 0 {
		required = append(required, SandboxCapabilityProcessLimit)
	}
	if p.ScratchBytes > 0 {
		required = append(required, SandboxCapabilityScratchLimit)
	}
	return normalizeSandboxCapabilities(required)
}

func normalizeSandboxCapabilities(values []SandboxCapability) ([]SandboxCapability, error) {
	seen := make(map[SandboxCapability]struct{}, len(values))
	result := make([]SandboxCapability, 0, len(values))
	for _, value := range values {
		if !knownSandboxCapability(value) {
			return nil, errUnknownSandboxCapability
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}
