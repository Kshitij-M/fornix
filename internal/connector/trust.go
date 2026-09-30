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
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrTrustPolicyMissing  = errors.New("connector trust policy is missing")
	ErrCapabilityUntrusted = errors.New("connector capability is not trusted")
	ErrTrustSignature      = errors.New("connector trust signature is invalid")
	ErrTrustExpired        = errors.New("connector trust policy is expired")
	ErrTrustDowngrade      = errors.New("connector trust policy revision is not monotonic")
)

// TrustEntry pins one exact connector version and capability definition hash.
// A name/version without the definition hash is not sufficient: a process
// must not silently execute a changed schema or effect declaration.
type TrustEntry struct {
	ConnectorHash  string `json:"connector_hash"`
	CapabilityHash string `json:"capability_hash"`
}

// TrustPolicy is an immutable, workspace-scoped allowlist for executable
// capability definitions. Development may install an unsigned process
// snapshot; production composition can require the detached Ed25519 fields
// and monotonic revision checks below.
type TrustPolicy struct {
	WorkspaceID     string       `json:"workspace_id"`
	Revision        string       `json:"revision"`
	Entries         []TrustEntry `json:"entries"`
	PolicyHash      string       `json:"policy_hash"`
	SignatureScheme string       `json:"signature_scheme,omitempty"`
	SignerID        string       `json:"signer_id,omitempty"`
	Signature       string       `json:"signature,omitempty"`
	IssuedAt        time.Time    `json:"issued_at,omitempty"`
	ExpiresAt       time.Time    `json:"expires_at,omitempty"`
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

// Sign creates a detached Ed25519 signature over the normalized policy hash.
// The private key is consumed only at composition time and is never part of
// the policy or registry state.
func (p *TrustPolicy) Sign(signerID string, privateKey ed25519.PrivateKey, issuedAt, expiresAt time.Time) error {
	if p == nil || len(privateKey) != ed25519.PrivateKeySize || strings.TrimSpace(signerID) == "" {
		return ErrTrustSignature
	}
	if issuedAt.IsZero() || expiresAt.IsZero() || !expiresAt.After(issuedAt) {
		return fmt.Errorf("trust policy signature window is invalid")
	}
	if p.PolicyHash != trustPolicyHash(*p) {
		return fmt.Errorf("trust policy hash does not match normalized entries")
	}
	p.SignatureScheme = "ed25519"
	p.SignerID = strings.TrimSpace(signerID)
	p.IssuedAt = issuedAt.UTC()
	p.ExpiresAt = expiresAt.UTC()
	signature := ed25519.Sign(privateKey, trustSigningBytes(*p))
	p.Signature = base64.RawURLEncoding.EncodeToString(signature)
	return nil
}

// Verify checks signer identity, detached signature, validity window, and the
// policy hash. It is intentionally independent of connector registration so a
// caller can verify a catalog before installing or admitting it.
func (p TrustPolicy) Verify(publicKeys map[string]ed25519.PublicKey, now time.Time) error {
	if p.SignatureScheme != "ed25519" || strings.TrimSpace(p.SignerID) == "" || strings.TrimSpace(p.Signature) == "" {
		return ErrTrustSignature
	}
	if p.PolicyHash == "" || p.PolicyHash != trustPolicyHash(p) {
		return ErrTrustSignature
	}
	if p.IssuedAt.IsZero() || p.ExpiresAt.IsZero() || !p.ExpiresAt.After(p.IssuedAt) {
		return ErrTrustSignature
	}
	now = now.UTC()
	if now.Before(p.IssuedAt) || !now.Before(p.ExpiresAt) {
		return ErrTrustExpired
	}
	key, ok := publicKeys[p.SignerID]
	if !ok || len(key) != ed25519.PublicKeySize {
		return ErrTrustSignature
	}
	signature, err := base64.RawURLEncoding.DecodeString(p.Signature)
	if err != nil || !ed25519.Verify(key, trustSigningBytes(p), signature) {
		return ErrTrustSignature
	}
	return nil
}

func trustSigningBytes(policy TrustPolicy) []byte {
	return []byte("fornix-trust-v1\x00" + policy.PolicyHash + "\x00" + policy.WorkspaceID + "\x00" + policy.Revision + "\x00" + policy.IssuedAt.UTC().Format(time.RFC3339Nano) + "\x00" + policy.ExpiresAt.UTC().Format(time.RFC3339Nano) + "\x00" + policy.SignerID)
}

func trustRevisionNumber(value string) (uint64, error) {
	n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("signed trust revision must be an unsigned integer")
	}
	return n, nil
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
