package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// SystemRef identifies one external system instance inside a workspace. The
// system type is descriptive; admission still requires a registered connector
// before an operation can execute.
type SystemRef struct {
	SchemaVersion int    `json:"schema_version"`
	WorkspaceID   string `json:"workspace_id"`
	Type          string `json:"type"`
	ID            string `json:"id"`
	Version       string `json:"version"`
}

// Normalize validates a system reference without performing discovery or
// connector I/O.
func (r *SystemRef) Normalize() error {
	if r == nil {
		return fmt.Errorf("system reference is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DomainNeutralSchemaVersion
	}
	if r.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported system schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	typ, err := normalizeDomainName(r.Type, "system type", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(r.ID, "system id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	version, err := normalizeDomainVersion(r.Version, "system version", true)
	if err != nil {
		return err
	}
	r.WorkspaceID, r.Type, r.ID, r.Version = workspace, typ, id, version
	return nil
}

// StableHash is the content identity of a normalized system reference.
func (r SystemRef) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	raw, _ := json.Marshal(r)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
