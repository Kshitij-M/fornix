package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// CapabilityRef points to a registered operation definition. DefinitionHash
// is required on an executable reference so a name alone can never select an
// unknown or silently changed capability.
type CapabilityRef struct {
	SchemaVersion  int          `json:"schema_version"`
	WorkspaceID    string       `json:"workspace_id"`
	Connector      ConnectorRef `json:"connector"`
	Name           string       `json:"name"`
	Version        string       `json:"version"`
	DefinitionHash string       `json:"definition_hash"`
}

// Normalize validates a capability reference intended for execution.
func (r *CapabilityRef) Normalize() error {
	return r.normalize(true)
}

func (r *CapabilityRef) normalize(requireDefinitionHash bool) error {
	if r == nil {
		return fmt.Errorf("capability reference is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DomainNeutralSchemaVersion
	}
	if r.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported capability schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	if err := r.Connector.Normalize(); err != nil {
		return fmt.Errorf("capability connector: %w", err)
	}
	if r.Connector.WorkspaceID != workspace {
		return fmt.Errorf("capability connector crosses workspace boundary")
	}
	name, err := normalizeDomainName(r.Name, "capability name", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	version, err := normalizeDomainVersion(r.Version, "capability version", true)
	if err != nil {
		return err
	}
	hash, err := normalizeDomainHash(r.DefinitionHash, "capability definition_hash", requireDefinitionHash)
	if err != nil {
		return err
	}
	r.WorkspaceID, r.Name, r.Version, r.DefinitionHash = workspace, name, version, hash
	return nil
}

// StableHash identifies the fully-qualified registered capability.
func (r CapabilityRef) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	raw, _ := json.Marshal(r)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// CapabilityDefinition is immutable registry metadata. The definition hash
// is computed from the definition with Ref.DefinitionHash omitted, preventing
// a self-referential hash.
type CapabilityDefinition struct {
	SchemaVersion        int                   `json:"schema_version"`
	WorkspaceID          string                `json:"workspace_id"`
	Ref                  CapabilityRef         `json:"ref"`
	Description          string                `json:"description,omitempty"`
	InputSchemaVersion   int                   `json:"input_schema_version"`
	InputSchemaHash      string                `json:"input_schema_hash"`
	OutputSchemaVersion  int                   `json:"output_schema_version"`
	OutputSchemaHash     string                `json:"output_schema_hash"`
	Effect               EffectClass           `json:"effect"`
	Profile              ExecutionProfile      `json:"profile"`
	Evidence             []EvidenceRequirement `json:"evidence,omitempty"`
	ResourceKinds        []string              `json:"resource_kinds,omitempty"`
	SupportsIdempotency  bool                  `json:"supports_idempotency"`
	SupportsVerification bool                  `json:"supports_verification"`
	Enabled              bool                  `json:"enabled"`
}

// Normalize validates a definition and assigns Ref.DefinitionHash from its
// canonical logical payload. A supplied hash must match exactly.
func (d *CapabilityDefinition) Normalize() error {
	if d == nil {
		return fmt.Errorf("capability definition is nil")
	}
	if d.SchemaVersion == 0 {
		d.SchemaVersion = DomainNeutralSchemaVersion
	}
	if d.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported capability definition schema_version %d", d.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(d.WorkspaceID)
	if err != nil {
		return err
	}
	if err := d.Ref.normalize(false); err != nil {
		return err
	}
	suppliedDefinitionHash, err := normalizeDomainHash(d.Ref.DefinitionHash, "capability definition_hash", false)
	if err != nil {
		return err
	}
	if d.Ref.WorkspaceID != workspace {
		return fmt.Errorf("capability reference crosses workspace boundary")
	}
	if len(d.Description) > 1024 || strings.ContainsAny(d.Description, "\r\n") {
		return fmt.Errorf("capability description is invalid or too large")
	}
	if d.InputSchemaVersion < 1 || d.OutputSchemaVersion < 1 {
		return fmt.Errorf("input and output schema versions are required")
	}
	inputHash, err := normalizeDomainHash(d.InputSchemaHash, "input_schema_hash", true)
	if err != nil {
		return err
	}
	outputHash, err := normalizeDomainHash(d.OutputSchemaHash, "output_schema_hash", true)
	if err != nil {
		return err
	}
	effect := EffectClass(strings.ToLower(strings.TrimSpace(string(d.Effect))))
	if !effect.valid() {
		return fmt.Errorf("unknown capability effect %q", d.Effect)
	}
	if err := d.Profile.Normalize(); err != nil {
		return fmt.Errorf("capability profile: %w", err)
	}
	if len(d.Evidence) > MaxDomainReferences {
		return fmt.Errorf("capability evidence exceeds %d entries", MaxDomainReferences)
	}
	for i := range d.Evidence {
		if err := d.Evidence[i].Normalize(); err != nil {
			return fmt.Errorf("evidence[%d]: %w", i, err)
		}
		if d.Evidence[i].WorkspaceID != workspace {
			return fmt.Errorf("capability evidence crosses workspace boundary")
		}
	}
	kinds, err := normalizeDomainStrings(d.ResourceKinds, "capability resource_kinds", MaxDomainReferences)
	if err != nil {
		return err
	}
	for i := range kinds {
		kinds[i] = strings.ToLower(kinds[i])
	}
	sort.Strings(kinds)
	d.WorkspaceID, d.InputSchemaHash, d.OutputSchemaHash, d.Effect, d.ResourceKinds = workspace, inputHash, outputHash, effect, kinds
	d.Ref.DefinitionHash = ""
	hash := d.StableHash()
	if hash == "" {
		return fmt.Errorf("capability definition hash could not be computed")
	}
	if suppliedDefinitionHash != "" && suppliedDefinitionHash != hash {
		return fmt.Errorf("capability definition_hash does not match definition")
	}
	d.Ref.DefinitionHash = hash
	return nil
}

// StableHash returns the definition identity without the derived self-hash.
func (d CapabilityDefinition) StableHash() string {
	d = cloneCapabilityDefinition(d)
	if err := d.normalizeForHash(); err != nil {
		return ""
	}
	type logicalDefinition struct {
		SchemaVersion        int                   `json:"schema_version"`
		WorkspaceID          string                `json:"workspace_id"`
		Ref                  CapabilityRef         `json:"ref"`
		Description          string                `json:"description,omitempty"`
		InputSchemaVersion   int                   `json:"input_schema_version"`
		InputSchemaHash      string                `json:"input_schema_hash"`
		OutputSchemaVersion  int                   `json:"output_schema_version"`
		OutputSchemaHash     string                `json:"output_schema_hash"`
		Effect               EffectClass           `json:"effect"`
		Profile              ExecutionProfile      `json:"profile"`
		Evidence             []EvidenceRequirement `json:"evidence,omitempty"`
		ResourceKinds        []string              `json:"resource_kinds,omitempty"`
		SupportsIdempotency  bool                  `json:"supports_idempotency"`
		SupportsVerification bool                  `json:"supports_verification"`
		Enabled              bool                  `json:"enabled"`
	}
	d.Ref.DefinitionHash = ""
	raw, _ := json.Marshal(logicalDefinition{
		SchemaVersion: d.SchemaVersion, WorkspaceID: d.WorkspaceID, Ref: d.Ref,
		Description: d.Description, InputSchemaVersion: d.InputSchemaVersion,
		InputSchemaHash: d.InputSchemaHash, OutputSchemaVersion: d.OutputSchemaVersion,
		OutputSchemaHash: d.OutputSchemaHash, Effect: d.Effect, Profile: d.Profile,
		Evidence: d.Evidence, ResourceKinds: d.ResourceKinds,
		SupportsIdempotency: d.SupportsIdempotency, SupportsVerification: d.SupportsVerification,
		Enabled: d.Enabled,
	})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func cloneCapabilityDefinition(d CapabilityDefinition) CapabilityDefinition {
	d.Evidence = append([]EvidenceRequirement(nil), d.Evidence...)
	for i := range d.Evidence {
		d.Evidence[i].RequiredFields = append([]string(nil), d.Evidence[i].RequiredFields...)
	}
	d.ResourceKinds = append([]string(nil), d.ResourceKinds...)
	return d
}

func (d *CapabilityDefinition) normalizeForHash() error {
	if d == nil {
		return fmt.Errorf("capability definition is nil")
	}
	if d.SchemaVersion == 0 {
		d.SchemaVersion = DomainNeutralSchemaVersion
	}
	if d.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported capability definition schema_version %d", d.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(d.WorkspaceID)
	if err != nil {
		return err
	}
	if err := d.Ref.normalize(false); err != nil {
		return err
	}
	if d.Ref.WorkspaceID != workspace {
		return fmt.Errorf("capability reference crosses workspace boundary")
	}
	if len(d.Description) > 1024 || strings.ContainsAny(d.Description, "\r\n") {
		return fmt.Errorf("capability description is invalid or too large")
	}
	if d.InputSchemaVersion < 1 || d.OutputSchemaVersion < 1 {
		return fmt.Errorf("input and output schema versions are required")
	}
	inputHash, err := normalizeDomainHash(d.InputSchemaHash, "input_schema_hash", true)
	if err != nil {
		return err
	}
	outputHash, err := normalizeDomainHash(d.OutputSchemaHash, "output_schema_hash", true)
	if err != nil {
		return err
	}
	effect := EffectClass(strings.ToLower(strings.TrimSpace(string(d.Effect))))
	if !effect.valid() {
		return fmt.Errorf("unknown capability effect %q", d.Effect)
	}
	if err := d.Profile.Normalize(); err != nil {
		return err
	}
	if len(d.Evidence) > MaxDomainReferences {
		return fmt.Errorf("capability evidence exceeds %d entries", MaxDomainReferences)
	}
	for i := range d.Evidence {
		if err := d.Evidence[i].Normalize(); err != nil {
			return err
		}
		if d.Evidence[i].WorkspaceID != workspace {
			return fmt.Errorf("capability evidence crosses workspace boundary")
		}
	}
	kinds, err := normalizeDomainStrings(d.ResourceKinds, "capability resource_kinds", MaxDomainReferences)
	if err != nil {
		return err
	}
	for i := range kinds {
		kinds[i] = strings.ToLower(kinds[i])
	}
	sort.Strings(kinds)
	d.WorkspaceID, d.InputSchemaHash, d.OutputSchemaHash, d.Effect, d.ResourceKinds = workspace, inputHash, outputHash, effect, kinds
	return nil
}

// Matches reports whether a reference points to this exact registered
// definition in the same workspace.
func (d CapabilityDefinition) Matches(ref CapabilityRef) bool {
	d = cloneCapabilityDefinition(d)
	if err := d.Normalize(); err != nil || ref.Normalize() != nil {
		return false
	}
	return d.Ref == ref && d.StableHash() == ref.DefinitionHash
}
