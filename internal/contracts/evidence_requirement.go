package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// EvidenceRequirement declares the proof an adapter must provide for one
// operation. It describes references and integrity metadata, never evidence
// bodies.
type EvidenceRequirement struct {
	SchemaVersion     int      `json:"schema_version"`
	WorkspaceID       string   `json:"workspace_id"`
	Kind              string   `json:"kind"`
	RequiredFields    []string `json:"required_fields,omitempty"`
	MinItems          int      `json:"min_items"`
	MaxItems          int      `json:"max_items"`
	MaxBytes          int64    `json:"max_bytes"`
	RequireHash       bool     `json:"require_hash"`
	RequireProvenance bool     `json:"require_provenance"`
	AllowSuperseded   bool     `json:"allow_superseded"`
}

// Normalize makes the requirement deterministic and bounded.
func (r *EvidenceRequirement) Normalize() error {
	if r == nil {
		return fmt.Errorf("evidence requirement is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DomainNeutralSchemaVersion
	}
	if r.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported evidence requirement schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	kind, err := normalizeDomainName(r.Kind, "evidence kind", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	fields, err := normalizeDomainStrings(r.RequiredFields, "evidence required_fields", MaxDomainReferences)
	if err != nil {
		return err
	}
	for i := range fields {
		fields[i] = strings.ToLower(fields[i])
	}
	sort.Strings(fields)
	if r.MinItems == 0 {
		r.MinItems = 1
	}
	if r.MaxItems == 0 {
		r.MaxItems = MaxExecutionDisclosureItems
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = MaxExecutionDisclosureBytes
	}
	if r.MinItems < 0 || r.MaxItems < 1 || r.MinItems > r.MaxItems || r.MaxItems > MaxExecutionDisclosureItems || r.MaxBytes < 1 || r.MaxBytes > MaxExecutionDisclosureBytes {
		return fmt.Errorf("evidence requirement bounds are invalid")
	}
	r.WorkspaceID, r.Kind, r.RequiredFields = workspace, kind, fields
	return nil
}

// StableHash is the canonical identity of a normalized evidence requirement.
func (r EvidenceRequirement) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	raw, _ := json.Marshal(r)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
