package connector

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrSchemaCatalogMissing = errors.New("connector schema catalog is missing")
	ErrSchemaUntrusted      = errors.New("connector schema is not trusted")
	ErrSchemaSignature      = errors.New("connector schema catalog signature is invalid")
	ErrSchemaExpired        = errors.New("connector schema catalog is expired")
	ErrSchemaDowngrade      = errors.New("connector schema catalog revision is not monotonic")
	ErrSchemaMismatch       = errors.New("connector schema does not match the admitted request")
)

const MaxSchemaCatalogEntries = contracts.MaxDomainReferences

// SchemaEntry is a bounded fingerprint of the schemas attached to one exact
// capability definition. Raw schema documents are intentionally not part of
// the catalog authority.
type SchemaEntry struct {
	ConnectorHash       string `json:"connector_hash"`
	CapabilityHash      string `json:"capability_hash"`
	InputSchemaVersion  int    `json:"input_schema_version"`
	InputSchemaHash     string `json:"input_schema_hash"`
	OutputSchemaVersion int    `json:"output_schema_version"`
	OutputSchemaHash    string `json:"output_schema_hash"`
}

func (e *SchemaEntry) Normalize() error {
	if e == nil || !validHash(strings.ToLower(strings.TrimSpace(e.ConnectorHash))) || !validHash(strings.ToLower(strings.TrimSpace(e.CapabilityHash))) {
		return ErrSchemaSignature
	}
	e.ConnectorHash = strings.ToLower(strings.TrimSpace(e.ConnectorHash))
	e.CapabilityHash = strings.ToLower(strings.TrimSpace(e.CapabilityHash))
	inputHash, err := normalizeSchemaHash(e.InputSchemaHash)
	if err != nil {
		return err
	}
	outputHash, err := normalizeSchemaHash(e.OutputSchemaHash)
	if err != nil {
		return err
	}
	e.InputSchemaHash, e.OutputSchemaHash = inputHash, outputHash
	if e.InputSchemaVersion < 1 || e.InputSchemaVersion > contracts.MaxDomainSchemaVersion || e.OutputSchemaVersion < 1 || e.OutputSchemaVersion > contracts.MaxDomainSchemaVersion {
		return ErrSchemaSignature
	}
	return nil
}

func normalizeSchemaHash(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if !validHash(value) {
		return "", ErrSchemaSignature
	}
	return value, nil
}

func schemaEntryKey(e SchemaEntry) string {
	return e.ConnectorHash + "\x00" + e.CapabilityHash
}

// SchemaCatalog is an immutable signed snapshot of schema fingerprints. It
// is deliberately separate from TrustPolicy so deployments can rotate schema
// compatibility without accidentally broadening the capability allowlist.
type SchemaCatalog struct {
	WorkspaceID     string        `json:"workspace_id"`
	Revision        string        `json:"revision"`
	Entries         []SchemaEntry `json:"entries"`
	CatalogHash     string        `json:"catalog_hash"`
	SignatureScheme string        `json:"signature_scheme,omitempty"`
	SignerID        string        `json:"signer_id,omitempty"`
	Signature       string        `json:"signature,omitempty"`
	IssuedAt        time.Time     `json:"issued_at,omitempty"`
	ExpiresAt       time.Time     `json:"expires_at,omitempty"`
}

func NewSchemaCatalog(workspaceID, revision string, definitions []contracts.CapabilityDefinition) (SchemaCatalog, error) {
	catalog := SchemaCatalog{WorkspaceID: strings.TrimSpace(workspaceID), Revision: strings.TrimSpace(revision)}
	if catalog.WorkspaceID == "" || catalog.Revision == "" || len(definitions) == 0 || len(definitions) > MaxSchemaCatalogEntries {
		return SchemaCatalog{}, ErrSchemaSignature
	}
	for i := range definitions {
		definition := definitions[i]
		if err := definition.Normalize(); err != nil || definition.WorkspaceID != catalog.WorkspaceID {
			return SchemaCatalog{}, ErrSchemaSignature
		}
		entry := SchemaEntry{
			ConnectorHash:       definition.Ref.Connector.StableHash(),
			CapabilityHash:      definition.Ref.DefinitionHash,
			InputSchemaVersion:  definition.InputSchemaVersion,
			InputSchemaHash:     definition.InputSchemaHash,
			OutputSchemaVersion: definition.OutputSchemaVersion,
			OutputSchemaHash:    definition.OutputSchemaHash,
		}
		if err := entry.Normalize(); err != nil {
			return SchemaCatalog{}, err
		}
		catalog.Entries = append(catalog.Entries, entry)
	}
	if err := catalog.Normalize(); err != nil {
		return SchemaCatalog{}, err
	}
	return catalog, nil
}

func (c *SchemaCatalog) Normalize() error {
	if c == nil || strings.TrimSpace(c.WorkspaceID) == "" || strings.TrimSpace(c.Revision) == "" || len(c.Entries) == 0 || len(c.Entries) > MaxSchemaCatalogEntries {
		return ErrSchemaSignature
	}
	c.WorkspaceID, c.Revision = strings.TrimSpace(c.WorkspaceID), strings.TrimSpace(c.Revision)
	seen := make(map[string]struct{}, len(c.Entries))
	for i := range c.Entries {
		if err := c.Entries[i].Normalize(); err != nil {
			return err
		}
		key := schemaEntryKey(c.Entries[i])
		if _, exists := seen[key]; exists {
			return ErrSchemaSignature
		}
		seen[key] = struct{}{}
	}
	sort.Slice(c.Entries, func(i, j int) bool { return schemaEntryKey(c.Entries[i]) < schemaEntryKey(c.Entries[j]) })
	expected := schemaCatalogHash(*c)
	if c.CatalogHash != "" && strings.ToLower(strings.TrimSpace(c.CatalogHash)) != expected {
		return ErrSchemaSignature
	}
	c.CatalogHash = expected
	return nil
}

func (c SchemaCatalog) StableHash() string {
	if c.CatalogHash != "" {
		return strings.ToLower(strings.TrimSpace(c.CatalogHash))
	}
	return schemaCatalogHash(c)
}

// Authorize proves both the registered definition identity and request input
// schema are present in the exact catalog snapshot.
func (c SchemaCatalog) Authorize(request contracts.OperationRequest, definition contracts.CapabilityDefinition) error {
	clone := c
	clone.Entries = append([]SchemaEntry(nil), c.Entries...)
	if err := clone.Normalize(); err != nil || clone.WorkspaceID != request.WorkspaceID {
		return ErrSchemaUntrusted
	}
	if err := definition.Normalize(); err != nil || definition.WorkspaceID != clone.WorkspaceID {
		return ErrSchemaUntrusted
	}
	for _, entry := range clone.Entries {
		if entry.ConnectorHash == definition.Ref.Connector.StableHash() && entry.CapabilityHash == definition.Ref.DefinitionHash {
			if entry.InputSchemaVersion != definition.InputSchemaVersion || entry.InputSchemaHash != definition.InputSchemaHash || entry.OutputSchemaVersion != definition.OutputSchemaVersion || entry.OutputSchemaHash != definition.OutputSchemaHash {
				return ErrSchemaMismatch
			}
			if request.InputSchemaVersion != entry.InputSchemaVersion || request.InputSchemaHash != entry.InputSchemaHash {
				return ErrSchemaMismatch
			}
			return nil
		}
	}
	return ErrSchemaUntrusted
}

func (c *SchemaCatalog) Sign(signerID string, privateKey ed25519.PrivateKey, issuedAt, expiresAt time.Time) error {
	if c == nil || len(privateKey) != ed25519.PrivateKeySize || strings.TrimSpace(signerID) == "" || issuedAt.IsZero() || expiresAt.IsZero() || !expiresAt.After(issuedAt) {
		return ErrSchemaSignature
	}
	if err := c.Normalize(); err != nil {
		return err
	}
	c.SignatureScheme, c.SignerID, c.IssuedAt, c.ExpiresAt = "ed25519", strings.TrimSpace(signerID), issuedAt.UTC(), expiresAt.UTC()
	c.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, schemaSigningBytes(*c)))
	return nil
}

func (c SchemaCatalog) Verify(publicKeys map[string]ed25519.PublicKey, now time.Time) error {
	clone := c
	clone.Entries = append([]SchemaEntry(nil), c.Entries...)
	if err := clone.Normalize(); err != nil || c.SignatureScheme != "ed25519" || strings.TrimSpace(c.SignerID) == "" || strings.TrimSpace(c.Signature) == "" {
		return ErrSchemaSignature
	}
	if c.CatalogHash != clone.CatalogHash {
		return ErrSchemaSignature
	}
	if c.IssuedAt.IsZero() || c.ExpiresAt.IsZero() || !c.ExpiresAt.After(c.IssuedAt) {
		return ErrSchemaSignature
	}
	now = now.UTC()
	if now.Before(c.IssuedAt) || !now.Before(c.ExpiresAt) {
		return ErrSchemaExpired
	}
	key, ok := publicKeys[c.SignerID]
	if !ok || len(key) != ed25519.PublicKeySize {
		return ErrSchemaSignature
	}
	signature, err := base64.RawURLEncoding.DecodeString(c.Signature)
	if err != nil || !ed25519.Verify(key, schemaSigningBytes(c), signature) {
		return ErrSchemaSignature
	}
	return nil
}

func schemaCatalogHash(c SchemaCatalog) string {
	entries := append([]SchemaEntry(nil), c.Entries...)
	sort.Slice(entries, func(i, j int) bool { return schemaEntryKey(entries[i]) < schemaEntryKey(entries[j]) })
	canonical := struct {
		WorkspaceID string        `json:"workspace_id"`
		Revision    string        `json:"revision"`
		Entries     []SchemaEntry `json:"entries"`
	}{c.WorkspaceID, c.Revision, entries}
	raw, _ := json.Marshal(canonical)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func schemaSigningBytes(c SchemaCatalog) []byte {
	return []byte("fornix-schema-catalog-v1\x00" + c.CatalogHash + "\x00" + c.WorkspaceID + "\x00" + c.Revision + "\x00" + c.IssuedAt.UTC().Format(time.RFC3339Nano) + "\x00" + c.ExpiresAt.UTC().Format(time.RFC3339Nano) + "\x00" + c.SignerID)
}

func SchemaCatalogJSON(c SchemaCatalog) ([]byte, error) {
	clone := c
	clone.Entries = append([]SchemaEntry(nil), c.Entries...)
	if err := clone.Normalize(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(clone)
	if err != nil {
		return nil, fmt.Errorf("marshal schema catalog: %w", err)
	}
	return raw, nil
}
