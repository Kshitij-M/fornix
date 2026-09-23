package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// ConnectorRef identifies the adapter implementation selected for a system.
// Version is mandatory so a replay can distinguish connector behavior.
type ConnectorRef struct {
	SchemaVersion int    `json:"schema_version"`
	WorkspaceID   string `json:"workspace_id"`
	Name          string `json:"name"`
	Version       string `json:"version"`
}

// Normalize validates a connector identity. Registration and availability are
// intentionally outside the contract package and must fail closed in the
// future admission registry.
func (r *ConnectorRef) Normalize() error {
	if r == nil {
		return fmt.Errorf("connector reference is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DomainNeutralSchemaVersion
	}
	if r.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported connector schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	name, err := normalizeDomainName(r.Name, "connector name", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	version, err := normalizeDomainVersion(r.Version, "connector version", true)
	if err != nil {
		return err
	}
	r.WorkspaceID, r.Name, r.Version = workspace, name, version
	return nil
}

// StableHash is the content identity of a normalized connector reference.
func (r ConnectorRef) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	raw, _ := json.Marshal(r)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
