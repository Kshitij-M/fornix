package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const SandboxExecutionIdentitySchemaVersion = 2

const maxSandboxAttemptResultJSONBytes = 2*MaxToolOutputBytes + (128 << 10)

const (
	SandboxAttemptAbsent    = "absent"
	SandboxAttemptRunning   = "running"
	SandboxAttemptCompleted = "completed"
	SandboxAttemptStopped   = "stopped"
	SandboxAttemptUnknown   = "unknown"
)

// SandboxExecutionIdentity binds a runtime object to one already-reserved
// Fornix tool effect. It carries identifiers and hashes only: never command
// arguments, host paths, environment values, credentials, or output.
type SandboxExecutionIdentity struct {
	SchemaVersion         int            `json:"schema_version"`
	WorkspaceID           string         `json:"workspace_id"`
	ToolRunID             string         `json:"tool_run_id"`
	ToolAttempt           int            `json:"tool_attempt"`
	Backend               SandboxBackend `json:"backend"`
	OperationID           string         `json:"operation_id"`
	OperationOwnerID      string         `json:"operation_owner_id"`
	OperationFence        uint64         `json:"operation_fence"`
	AttemptID             string         `json:"attempt_id"`
	EffectID              string         `json:"effect_id"`
	ToolRequestHash       string         `json:"tool_request_hash"`
	OperationRequestHash  string         `json:"operation_request_hash"`
	EffectReservationHash string         `json:"effect_reservation_hash"`
	TaskOwnerID           string         `json:"task_owner_id,omitempty"`
	TaskFence             uint64         `json:"task_fence,omitempty"`
	AgentRunID            string         `json:"agent_run_id,omitempty"`
	AgentRunOwnerID       string         `json:"agent_run_owner_id,omitempty"`
	AgentRunFence         uint64         `json:"agent_run_fence,omitempty"`
	ToolDefinitionHash    string         `json:"tool_definition_hash"`
	SandboxProfileHash    string         `json:"sandbox_profile_hash"`
	QualificationHash     string         `json:"qualification_hash,omitempty"`
}

// Normalize validates all authority facts required to identify one runtime
// attempt. It deliberately rejects incomplete identities rather than
// manufacturing identifiers from caller-controlled values.
func (i *SandboxExecutionIdentity) Normalize() error {
	if i == nil {
		return fmt.Errorf("sandbox execution identity is nil")
	}
	if i.SchemaVersion == 0 {
		i.SchemaVersion = SandboxExecutionIdentitySchemaVersion
	}
	if i.SchemaVersion != SandboxExecutionIdentitySchemaVersion {
		return fmt.Errorf("unsupported sandbox execution identity schema version")
	}
	workspace, err := normalizeDomainWorkspace(i.WorkspaceID)
	if err != nil {
		return fmt.Errorf("normalize sandbox workspace: %w", err)
	}
	for name, value := range map[string]*string{
		"sandbox tool_run_id":        &i.ToolRunID,
		"sandbox operation_id":       &i.OperationID,
		"sandbox operation_owner_id": &i.OperationOwnerID,
		"sandbox attempt_id":         &i.AttemptID,
		"sandbox effect_id":          &i.EffectID,
	} {
		*value, err = normalizeDomainIdentifier(*value, name, MaxDomainIDLength, true)
		if err != nil {
			return err
		}
	}
	if !knownSandboxBackend(i.Backend) {
		return errInvalidSandboxBackend
	}
	if i.Backend != SandboxBackendLocalProcess && i.QualificationHash == "" {
		return fmt.Errorf("non-local sandbox identity requires a qualification hash")
	}
	if i.ToolAttempt < 1 || i.OperationFence == 0 {
		return fmt.Errorf("sandbox tool attempt and operation fence must be positive")
	}
	if i.OperationFence > uint64(1<<63-1) {
		return fmt.Errorf("sandbox operation fence exceeds database range")
	}
	if i.TaskOwnerID == "" && i.TaskFence != 0 || i.TaskOwnerID != "" && i.TaskFence == 0 {
		return fmt.Errorf("sandbox task owner and fence must be supplied together")
	}
	if i.TaskFence > uint64(1<<63-1) {
		return fmt.Errorf("sandbox task fence exceeds database range")
	}
	if i.TaskOwnerID != "" {
		i.TaskOwnerID, err = normalizeDomainIdentifier(i.TaskOwnerID, "sandbox task_owner_id", MaxDomainIDLength, true)
		if err != nil {
			return err
		}
	}
	if i.AgentRunID == "" && (i.AgentRunOwnerID != "" || i.AgentRunFence != 0) || i.AgentRunID != "" && (i.AgentRunOwnerID == "" || i.AgentRunFence == 0) {
		return fmt.Errorf("sandbox agent run identity and owner fence must be supplied together")
	}
	if i.AgentRunFence > uint64(1<<63-1) {
		return fmt.Errorf("sandbox agent run fence exceeds database range")
	}
	if i.AgentRunID != "" {
		for name, value := range map[string]*string{
			"sandbox agent_run_id":       &i.AgentRunID,
			"sandbox agent_run_owner_id": &i.AgentRunOwnerID,
		} {
			*value, err = normalizeDomainIdentifier(*value, name, MaxDomainIDLength, true)
			if err != nil {
				return err
			}
		}
	}
	for name, value := range map[string]*string{
		"sandbox tool_request_hash":       &i.ToolRequestHash,
		"sandbox operation_request_hash":  &i.OperationRequestHash,
		"sandbox effect_reservation_hash": &i.EffectReservationHash,
		"sandbox tool_definition_hash":    &i.ToolDefinitionHash,
		"sandbox profile_hash":            &i.SandboxProfileHash,
	} {
		*value, err = normalizeDomainHash(*value, name, true)
		if err != nil {
			return err
		}
	}
	if i.QualificationHash != "" {
		i.QualificationHash, err = normalizeDomainHash(i.QualificationHash, "sandbox qualification_hash", true)
		if err != nil {
			return err
		}
	}
	i.WorkspaceID = workspace
	return nil
}

// StableHash returns the canonical hash of the fenced runtime attempt.
func (i SandboxExecutionIdentity) StableHash() string {
	if err := i.Normalize(); err != nil {
		return ""
	}
	raw, _ := json.Marshal(i)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// RuntimeName returns a bounded opaque name suitable for exact-label runtime
// lookup. The name discloses neither workspace nor tool identifiers.
func (i SandboxExecutionIdentity) RuntimeName() string {
	hash := i.StableHash()
	return SandboxRuntimeNameFromHash(hash)
}

// SandboxRuntimeNameFromHash returns the one canonical opaque Engine-object
// name for a normalized execution-identity hash. Invalid hashes produce an
// empty name so callers cannot derive a runtime name from unchecked input.
func SandboxRuntimeNameFromHash(identityHash string) string {
	identityHash, err := normalizeDomainHash(identityHash, "sandbox execution identity hash", true)
	if err != nil {
		return ""
	}
	return "fornix-" + identityHash[:40]
}

// SandboxAttemptObservation is the bounded result of looking up a runtime
// attempt by its exact execution identity. A provider must not report a
// successful result unless it can recover and hash-bind that result.
type SandboxAttemptObservation struct {
	IdentityHash string      `json:"identity_hash"`
	State        string      `json:"state"`
	RuntimeID    string      `json:"runtime_id,omitempty"`
	ResultHash   string      `json:"result_hash,omitempty"`
	Result       *ToolResult `json:"result,omitempty"`
}

// Normalize validates a reconciliation observation without treating unknown
// runtime state as absence. The caller must route unknown state to recovery.
func (o *SandboxAttemptObservation) Normalize(identity SandboxExecutionIdentity) error {
	if o == nil {
		return fmt.Errorf("sandbox attempt observation is nil")
	}
	identityHash := identity.StableHash()
	if identityHash == "" || !strings.EqualFold(o.IdentityHash, identityHash) {
		return fmt.Errorf("sandbox observation does not match execution identity")
	}
	o.IdentityHash = identityHash
	switch o.State {
	case SandboxAttemptAbsent, SandboxAttemptRunning, SandboxAttemptCompleted, SandboxAttemptStopped, SandboxAttemptUnknown:
	default:
		return fmt.Errorf("invalid sandbox attempt state")
	}
	o.RuntimeID = strings.TrimSpace(o.RuntimeID)
	if len(o.RuntimeID) > 256 {
		return fmt.Errorf("sandbox runtime identifier is too large")
	}
	if o.ResultHash != "" {
		normalized, err := normalizeDomainHash(o.ResultHash, "sandbox result hash", true)
		if err != nil {
			return err
		}
		o.ResultHash = normalized
	}
	if o.State == SandboxAttemptCompleted {
		if o.Result == nil || o.ResultHash == "" || o.Result.Hash() != o.ResultHash {
			return fmt.Errorf("completed sandbox observation requires a matching result")
		}
		if len(o.Result.Stdout) > MaxToolOutputBytes || len(o.Result.Stderr) > MaxToolOutputBytes || len(o.Result.Artifacts) > MaxDomainReferences {
			return fmt.Errorf("sandbox attempt result exceeds disclosure bounds")
		}
		if o.Result.Failure != nil && (len(o.Result.Failure.Code) > 128 || len(o.Result.Failure.Message) > 4096 || len(o.Result.Failure.Detail) > 4096) {
			return fmt.Errorf("sandbox attempt failure metadata exceeds disclosure bounds")
		}
		encoded, err := json.Marshal(o.Result)
		if err != nil || len(encoded) > maxSandboxAttemptResultJSONBytes {
			return fmt.Errorf("sandbox attempt result exceeds encoded size bound")
		}
	} else if o.Result != nil || o.ResultHash != "" {
		return fmt.Errorf("non-completed sandbox observation cannot contain a result")
	}
	if o.State == SandboxAttemptAbsent && o.RuntimeID != "" {
		return fmt.Errorf("absent sandbox observation cannot contain a runtime identifier")
	}
	return nil
}
