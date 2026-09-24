package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrTrustPolicyMissing  = errors.New("connector trust policy is missing")
	ErrCapabilityUntrusted = errors.New("connector capability is not trusted")
)

// TrustEntry pins one exact connector version and capability definition hash.
// A name/version without the definition hash is not sufficient: a process
// must not silently execute a changed schema or effect declaration.
type TrustEntry struct {
	ConnectorHash  string `json:"connector_hash"`
	CapabilityHash string `json:"capability_hash"`
}

// TrustPolicy is an immutable, workspace-scoped allowlist for executable
// capability definitions. It is intentionally a process configuration seam
// in this slice; a future signed catalog may produce the same normalized
// policy, but registration never expands it implicitly.
type TrustPolicy struct {
	WorkspaceID string       `json:"workspace_id"`
	Revision    string       `json:"revision"`
	Entries     []TrustEntry `json:"entries"`
	PolicyHash  string       `json:"policy_hash"`
}

// NewTrustPolicy snapshots the supplied normalized definitions in stable
// order. The input is copied so later registry changes cannot mutate trust.
func NewTrustPolicy(workspaceID, revision string, definitions []contracts.CapabilityDefinition) (TrustPolicy, error) {
	policy := TrustPolicy{WorkspaceID: strings.TrimSpace(workspaceID), Revision: strings.TrimSpace(revision)}
	if policy.WorkspaceID == "" {
		return TrustPolicy{}, fmt.Errorf("trust policy workspace_id is required")
	}
	if policy.Revision == "" {
		return TrustPolicy{}, fmt.Errorf("trust policy revision is required")
	}
	for index := range definitions {
		definition := definitions[index]
		if err := definition.Normalize(); err != nil {
			return TrustPolicy{}, fmt.Errorf("trust definition %d: %w", index, err)
		}
		if definition.WorkspaceID != policy.WorkspaceID || definition.Ref.WorkspaceID != policy.WorkspaceID {
			return TrustPolicy{}, fmt.Errorf("trust definition %d crosses workspace boundary", index)
		}
		if definition.Ref.DefinitionHash == "" {
			return TrustPolicy{}, fmt.Errorf("trust definition %d has no definition hash", index)
		}
		policy.Entries = append(policy.Entries, TrustEntry{
			ConnectorHash:  definition.Ref.Connector.StableHash(),
			CapabilityHash: definition.Ref.DefinitionHash,
		})
	}
	sort.Slice(policy.Entries, func(i, j int) bool {
		if policy.Entries[i].ConnectorHash != policy.Entries[j].ConnectorHash {
			return policy.Entries[i].ConnectorHash < policy.Entries[j].ConnectorHash
		}
		return policy.Entries[i].CapabilityHash < policy.Entries[j].CapabilityHash
	})
	if err := normalizeTrustEntries(policy.Entries); err != nil {
		return TrustPolicy{}, err
	}
	policy.PolicyHash = trustPolicyHash(policy)
	return policy, nil
}

func normalizeTrustEntries(entries []TrustEntry) error {
	if len(entries) == 0 {
		return fmt.Errorf("trust policy must contain at least one capability")
	}
	for index := range entries {
		entries[index].ConnectorHash = strings.ToLower(strings.TrimSpace(entries[index].ConnectorHash))
		entries[index].CapabilityHash = strings.ToLower(strings.TrimSpace(entries[index].CapabilityHash))
		if !validHash(entries[index].ConnectorHash) || !validHash(entries[index].CapabilityHash) {
			return fmt.Errorf("trust entry %d has invalid hashes", index)
		}
		if index > 0 && entries[index-1] == entries[index] {
			return fmt.Errorf("trust policy contains duplicate entry")
		}
	}
	return nil
}

// Authorize returns nil only for an exact connector identity and capability
// definition hash. The caller must still perform workspace authorization and
// operation admission separately.
func (p TrustPolicy) Authorize(definition contracts.CapabilityDefinition) error {
	if strings.TrimSpace(p.WorkspaceID) == "" || strings.TrimSpace(p.PolicyHash) == "" {
		return ErrTrustPolicyMissing
	}
	if err := definition.Normalize(); err != nil {
		return fmt.Errorf("normalize trusted capability: %w", err)
	}
	if definition.WorkspaceID != p.WorkspaceID {
		return ErrCapabilityUntrusted
	}
	connectorHash := definition.Ref.Connector.StableHash()
	for _, entry := range p.Entries {
		if entry.ConnectorHash == connectorHash && entry.CapabilityHash == definition.Ref.DefinitionHash {
			return nil
		}
	}
	return ErrCapabilityUntrusted
}

// StableHash is the normalized trust snapshot identity.
func (p TrustPolicy) StableHash() string {
	if p.PolicyHash != "" {
		return p.PolicyHash
	}
	return trustPolicyHash(p)
}

func trustPolicyHash(policy TrustPolicy) string {
	clone := struct {
		WorkspaceID string       `json:"workspace_id"`
		Revision    string       `json:"revision"`
		Entries     []TrustEntry `json:"entries"`
	}{policy.WorkspaceID, policy.Revision, append([]TrustEntry(nil), policy.Entries...)}
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func validHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
