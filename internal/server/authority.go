package server

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/store"
)

type authorityGeneration struct {
	WorkspaceID       string    `json:"workspace_id"`
	TrustPolicyHash   string    `json:"trust_policy_hash,omitempty"`
	TrustRevision     string    `json:"trust_revision,omitempty"`
	TrustSignerID     string    `json:"trust_signer_id,omitempty"`
	TrustExpiresAt    time.Time `json:"trust_expires_at,omitempty"`
	SchemaCatalogHash string    `json:"schema_catalog_hash,omitempty"`
	SchemaRevision    string    `json:"schema_revision,omitempty"`
	SchemaSignerID    string    `json:"schema_signer_id,omitempty"`
	SchemaExpiresAt   time.Time `json:"schema_expires_at,omitempty"`
	LoadedAt          time.Time `json:"loaded_at"`
}

const maxAuthorityWorkspaces = 10000

// reloadWorkspaceAuthority loads one complete signed authority generation
// from Postgres before it can authorize new work. Loading policy and schema
// together prevents a process from combining a new allowlist with an old
// schema snapshot. Missing catalogs are tolerated only in explicitly
// development-compatible mode; malformed or partially present catalogs never
// fall back to unsigned trust.
func (s *server) reloadWorkspaceAuthority(ctx context.Context, workspaceID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return fmt.Errorf("workspace authority workspace_id is required")
	}
	if s.trustCatalog == nil || s.connectorRegistry == nil {
		if s.requireSignedAuthority {
			return fmt.Errorf("signed workspace authority is not configured")
		}
		return nil
	}
	now := time.Now().UTC()
	policy, policyKey, policyErr := s.trustCatalog.CurrentSignedPolicy(ctx, workspaceID, now)
	catalog, catalogKey, catalogErr := s.trustCatalog.CurrentSignedSchemaCatalog(ctx, workspaceID, now)
	policyMissing := errors.Is(policyErr, store.ErrTrustPolicyNotFound)
	catalogMissing := errors.Is(catalogErr, store.ErrSchemaCatalogNotFound)
	if policyMissing && catalogMissing {
		if s.requireSignedAuthority {
			return fmt.Errorf("signed trust policy and schema catalog are required")
		}
		s.recordAuthorityGeneration(authorityGeneration{WorkspaceID: workspaceID, LoadedAt: now})
		s.setAuthorityStatus(true, "", time.Time{}, 0)
		return nil
	}
	if policyErr != nil {
		return fmt.Errorf("load signed trust policy: %w", policyErr)
	}
	if catalogErr != nil {
		return fmt.Errorf("load signed schema catalog: %w", catalogErr)
	}
	if policy.WorkspaceID != workspaceID || catalog.WorkspaceID != workspaceID {
		return fmt.Errorf("signed authority crosses workspace boundary")
	}
	if err := installSignedAuthority(s.connectorRegistry, workspaceID, policy, policyKey, catalog, catalogKey, now); err != nil {
		return err
	}
	if err := s.connectorRegistry.ValidateEffectAuthorityConformance(workspaceID); err != nil {
		return err
	}
	s.recordAuthorityGeneration(authorityGeneration{
		WorkspaceID: workspaceID, TrustPolicyHash: policy.PolicyHash, TrustRevision: policy.Revision,
		TrustSignerID: policy.SignerID, TrustExpiresAt: policy.ExpiresAt,
		SchemaCatalogHash: catalog.CatalogHash, SchemaRevision: catalog.Revision,
		SchemaSignerID: catalog.SignerID, SchemaExpiresAt: catalog.ExpiresAt, LoadedAt: now,
	})
	s.setAuthorityStatus(true, "", now, 1)
	return nil
}

// installSignedAuthority publishes one internally consistent registry
// generation. The trust and schema APIs each enforce monotonic revisions;
// equality is an idempotent reload, while a downgrade is rejected.
func installSignedAuthority(registry *connectorruntime.Registry, workspaceID string, policy connectorruntime.TrustPolicy, policyKey ed25519.PublicKey, catalog connectorruntime.SchemaCatalog, catalogKey ed25519.PublicKey, now time.Time) error {
	if registry == nil {
		return connectorruntime.ErrRegistryNil
	}
	if policy.WorkspaceID != workspaceID || catalog.WorkspaceID != workspaceID || policy.SignerID == "" || catalog.SignerID == "" || !equalPublicKey(policyKey, catalogKey) && policy.SignerID == catalog.SignerID {
		return fmt.Errorf("signed authority generation is inconsistent")
	}
	if err := registry.SetTrustSigner(policy.SignerID, policyKey); err != nil {
		return err
	}
	if catalog.SignerID != policy.SignerID {
		if err := registry.SetTrustSigner(catalog.SignerID, catalogKey); err != nil {
			return err
		}
	}
	registry.RequireTrustPolicy(true)
	registry.RequireSignedTrustPolicy(true)
	registry.RequireSignedSchemaCatalog(true)
	if existing, ok := registry.TrustPolicy(workspaceID); !ok || existing.StableHash() != policy.StableHash() || existing.Revision != policy.Revision || strings.TrimSpace(existing.Signature) == "" {
		if err := registry.SetSignedTrustPolicy(policy, now); err != nil {
			return err
		}
	}
	if err := registry.SetSignedSchemaCatalog(catalog, now); err != nil {
		return err
	}
	return nil
}

func equalPublicKey(left, right ed25519.PublicKey) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (s *server) setAuthorityStatus(ready bool, failure string, loadedAt time.Time, workspaceCount int) {
	s.authorityMu.Lock()
	s.authorityReady = ready
	s.authorityError = failure
	if !loadedAt.IsZero() {
		s.authorityLoadedAt = loadedAt
	}
	if workspaceCount > 0 {
		s.authorityWorkspaces = workspaceCount
	}
	s.authorityMu.Unlock()
}

func (s *server) authorityStatus() map[string]any {
	s.authorityMu.RLock()
	defer s.authorityMu.RUnlock()
	status := map[string]any{
		"required":   s.requireSignedAuthority,
		"ready":      s.authorityReady,
		"workspaces": s.authorityWorkspaces,
	}
	status["qualification"] = s.qualificationAuthorityStatus()
	status["qualification_release"] = s.qualificationReleaseStatus()
	if s.effectConformanceHash != "" {
		status["effect_conformance_manifest_hash"] = s.effectConformanceHash
	}
	if s.authorityLoadedAt.IsZero() == false {
		status["loaded_at"] = s.authorityLoadedAt.UTC().Format(time.RFC3339Nano)
	}
	if s.authorityError != "" {
		status["error"] = s.authorityError
	}
	generations := make([]authorityGeneration, 0, len(s.authorityGenerations))
	for _, generation := range s.authorityGenerations {
		generations = append(generations, generation)
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i].WorkspaceID < generations[j].WorkspaceID })
	if len(generations) > 0 {
		status["generations"] = generations
	}
	return status
}

func (s *server) recordAuthorityGeneration(generation authorityGeneration) {
	s.authorityMu.Lock()
	if s.authorityGenerations == nil {
		s.authorityGenerations = make(map[string]authorityGeneration)
	}
	s.authorityGenerations[generation.WorkspaceID] = generation
	s.authorityMu.Unlock()
}
