package contracts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	ConnectorBindingSchemaVersion  = 1
	ConnectorBindingHTTPAPI        = "http_api"
	ConnectorBindingSQLReadonly    = "sql_readonly"
	ConnectorBindingActive         = "active"
	ConnectorBindingDisabled       = "disabled"
	MaxConnectorBindingConfigBytes = 64 << 10
)

// ConnectorBinding is the durable, redacted identity of one workspace's
// connector configuration. Configuration contains only non-secret, bounded
// allowlists and logical references. Runtime connector instances remain
// process-local and must be constructed explicitly from this record.
type ConnectorBinding struct {
	SchemaVersion  int             `json:"schema_version"`
	ID             string          `json:"id"`
	WorkspaceID    string          `json:"workspace_id"`
	Connector      ConnectorRef    `json:"connector"`
	Kind           string          `json:"kind"`
	Version        int             `json:"version"`
	ConfigHash     string          `json:"config_hash"`
	Configuration  json.RawMessage `json:"configuration"`
	CredentialRefs []string        `json:"credential_refs,omitempty"`
	Status         string          `json:"status"`
	CreatedBy      ActorRef        `json:"created_by"`
	CreatedAt      time.Time       `json:"created_at"`
}

// ConnectorBindingRequest is the idempotent command for registering one
// immutable connector binding. A new configuration is a new binding version;
// existing admitted operations retain their original hash.
type ConnectorBindingRequest struct {
	SchemaVersion  int              `json:"schema_version"`
	RequestID      string           `json:"request_id"`
	IdempotencyKey string           `json:"idempotency_key"`
	WorkspaceID    string           `json:"workspace_id"`
	Actor          ActorRef         `json:"actor"`
	Binding        ConnectorBinding `json:"binding"`
}

func (b *ConnectorBinding) Normalize() error {
	if b == nil {
		return fmt.Errorf("connector binding is nil")
	}
	if b.SchemaVersion == 0 {
		b.SchemaVersion = ConnectorBindingSchemaVersion
	}
	if b.SchemaVersion != ConnectorBindingSchemaVersion {
		return fmt.Errorf("unsupported connector binding schema_version %d", b.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(b.WorkspaceID)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(b.ID, "connector binding id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	if err := b.Connector.Normalize(); err != nil {
		return fmt.Errorf("connector binding connector: %w", err)
	}
	if b.Connector.WorkspaceID != workspace {
		return fmt.Errorf("connector binding connector crosses workspace boundary")
	}
	kind := strings.ToLower(strings.TrimSpace(b.Kind))
	if kind != ConnectorBindingHTTPAPI && kind != ConnectorBindingSQLReadonly {
		return fmt.Errorf("unsupported connector binding kind %q", b.Kind)
	}
	if b.Version < 1 || b.Version > MaxDomainSchemaVersion {
		return fmt.Errorf("connector binding version is out of bounds")
	}
	if len(b.Configuration) == 0 || len(b.Configuration) > MaxConnectorBindingConfigBytes || !json.Valid(b.Configuration) {
		return fmt.Errorf("connector binding configuration is missing, invalid, or too large")
	}
	trimmed := bytes.TrimSpace(b.Configuration)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("connector binding configuration must be a JSON object")
	}
	var configurationValue map[string]any
	if err := json.Unmarshal(trimmed, &configurationValue); err != nil {
		return fmt.Errorf("connector binding configuration must be a JSON object")
	}
	canonicalConfiguration, err := json.Marshal(configurationValue)
	if err != nil {
		return fmt.Errorf("canonicalize connector binding configuration: %w", err)
	}
	lower := strings.ToLower(string(canonicalConfiguration))
	for _, forbidden := range []string{"password", "secret", "token", "authorization", "connection_string", "dsn"} {
		if strings.Contains(lower, "\""+forbidden+"\"") {
			return fmt.Errorf("connector binding configuration contains forbidden secret field %q", forbidden)
		}
	}
	configHash := sha256.Sum256(canonicalConfiguration)
	hash := hex.EncodeToString(configHash[:])
	if b.ConfigHash != "" && strings.ToLower(strings.TrimSpace(b.ConfigHash)) != hash {
		return fmt.Errorf("connector binding config_hash does not match configuration")
	}
	credentials, err := normalizeDomainStrings(b.CredentialRefs, "connector binding credential_refs", MaxDomainReferences)
	if err != nil {
		return err
	}
	status := strings.ToLower(strings.TrimSpace(b.Status))
	if status == "" {
		status = ConnectorBindingActive
	}
	if status != ConnectorBindingActive && status != ConnectorBindingDisabled {
		return fmt.Errorf("unsupported connector binding status %q", b.Status)
	}
	if err := normalizeDomainActor(&b.CreatedBy, workspace); err != nil {
		return err
	}
	b.WorkspaceID, b.ID, b.Kind, b.ConfigHash, b.CredentialRefs, b.Status = workspace, id, kind, hash, credentials, status
	b.Configuration = append(json.RawMessage(nil), canonicalConfiguration...)
	return nil
}

func (b ConnectorBinding) StableHash() string {
	if err := b.Normalize(); err != nil {
		return ""
	}
	b.CreatedAt = time.Time{}
	raw, _ := json.Marshal(b)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (r *ConnectorBindingRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("connector binding request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ConnectorBindingSchemaVersion
	}
	if r.SchemaVersion != ConnectorBindingSchemaVersion {
		return fmt.Errorf("unsupported connector binding request schema_version %d", r.SchemaVersion)
	}
	for field, value := range map[string]string{"request_id": r.RequestID, "idempotency_key": r.IdempotencyKey} {
		if _, err := normalizeDomainIdentifier(value, "connector binding "+field, MaxIdempotencyLength, true); err != nil {
			return err
		}
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	if r.Binding.WorkspaceID != workspace {
		return fmt.Errorf("connector binding request crosses workspace boundary")
	}
	if err := r.Binding.Normalize(); err != nil {
		return err
	}
	r.WorkspaceID, r.RequestID, r.IdempotencyKey = workspace, strings.TrimSpace(r.RequestID), strings.TrimSpace(r.IdempotencyKey)
	return nil
}

func (r ConnectorBindingRequest) RequestHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	r.RequestID, r.IdempotencyKey = "", ""
	raw, _ := json.Marshal(r)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
