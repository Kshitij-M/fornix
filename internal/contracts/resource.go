package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ResourceRef identifies a typed version of a resource within one system.
// ContentHash is optional because some systems expose a version without a
// content digest; adapters should provide it whenever the source supports it.
type ResourceRef struct {
	SchemaVersion int       `json:"schema_version"`
	WorkspaceID   string    `json:"workspace_id"`
	System        SystemRef `json:"system"`
	Kind          string    `json:"kind"`
	ID            string    `json:"id"`
	Version       string    `json:"version"`
	ContentHash   string    `json:"content_hash,omitempty"`
}

// Normalize validates the resource and its containing system. The explicit
// workspace comparison prevents a nested reference from bypassing isolation.
func (r *ResourceRef) Normalize() error {
	if r == nil {
		return fmt.Errorf("resource reference is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DomainNeutralSchemaVersion
	}
	if r.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported resource schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	if err := r.System.Normalize(); err != nil {
		return fmt.Errorf("resource system: %w", err)
	}
	if r.System.WorkspaceID != workspace {
		return fmt.Errorf("resource system crosses workspace boundary")
	}
	kind, err := normalizeDomainName(r.Kind, "resource kind", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(r.ID, "resource id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	version, err := normalizeDomainVersion(r.Version, "resource version", true)
	if err != nil {
		return err
	}
	hash, err := normalizeDomainHash(r.ContentHash, "resource content_hash", false)
	if err != nil {
		return err
	}
	r.WorkspaceID, r.Kind, r.ID, r.Version, r.ContentHash = workspace, kind, id, version, hash
	return nil
}

// StableHash is the content identity of a normalized resource reference.
func (r ResourceRef) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	raw, _ := json.Marshal(r)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// ValidateWorkspace makes the common nested-reference check available to
// adapter packages without exposing the normalization implementation.
func (r ResourceRef) ValidateWorkspace(workspaceID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if err := r.Normalize(); err != nil {
		return err
	}
	if r.WorkspaceID != workspaceID {
		return fmt.Errorf("resource crosses workspace boundary")
	}
	return nil
}
