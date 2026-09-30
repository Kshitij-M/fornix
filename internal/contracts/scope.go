package contracts

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// DomainNeutralSchemaVersion is the first version of the domain-neutral
// operation vocabulary. It is independent from the event, task, and receipt
// schemas because those contracts have their own compatibility lifecycles.
const DomainNeutralSchemaVersion = 1

const (
	MaxDomainWorkspaceIDLength = 256
	MaxDomainIDLength          = 128
	MaxDomainNameLength        = 128
	MaxDomainVersionLength     = 64
	// MaxCredentialSourceVersionLength bounds opaque managed-secret version
	// metadata without pretending that it is a domain version identifier.
	MaxCredentialSourceVersionLength = 128
	MaxDomainSchemaVersion           = 1024
	MaxDomainHashLength              = 64
	MaxDomainMetadataEntries         = 32
	MaxDomainMetadataKeyLength       = 64
	MaxDomainMetadataValueLen        = 128
	MaxDomainReferences              = 128
)

var (
	domainIdentifierPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/@+-]*$`)
	domainMetadataKeyPattern   = regexp.MustCompile(`^[a-z][a-z0-9_.-]*$`)
	domainMetadataValuePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/@+\-]*$`)
)

// normalizeDomainIdentifier validates a bounded opaque identifier. IDs remain
// case-sensitive; names and kinds are normalized by their owning contract.
func normalizeDomainIdentifier(value, field string, max int, required bool) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		if required {
			return "", fmt.Errorf("%s is required", field)
		}
		return "", nil
	}
	if len(value) > max || !domainIdentifierPattern.MatchString(value) {
		return "", fmt.Errorf("%s is invalid or exceeds %d characters", field, max)
	}
	if strings.EqualFold(value, "unknown") || strings.EqualFold(value, "unspecified") {
		return "", fmt.Errorf("%s cannot be unknown or unspecified", field)
	}
	return value, nil
}

func normalizeDomainName(value, field string, max int, required bool) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	return normalizeDomainIdentifier(value, field, max, required)
}

func normalizeDomainVersion(value, field string, required bool) (string, error) {
	value = strings.TrimSpace(value)
	return normalizeDomainIdentifier(value, field, MaxDomainVersionLength, required)
}

func normalizeDomainWorkspace(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("workspace_id is required")
	}
	if len(value) > MaxDomainWorkspaceIDLength || strings.ContainsAny(value, "\r\n\t") {
		return "", fmt.Errorf("workspace_id is invalid or exceeds %d characters", MaxDomainWorkspaceIDLength)
	}
	return value, nil
}

func normalizeDomainHash(value, field string, required bool) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		if required {
			return "", fmt.Errorf("%s is required", field)
		}
		return "", nil
	}
	if len(value) != MaxDomainHashLength || !canonicalSHA256(value) {
		return "", fmt.Errorf("%s must be a lowercase sha256", field)
	}
	return value, nil
}

func normalizeDomainActor(actor *ActorRef, workspaceID string) error {
	if actor == nil {
		return fmt.Errorf("actor is required")
	}
	actor.ID = strings.TrimSpace(actor.ID)
	actor.Kind = strings.ToLower(strings.TrimSpace(actor.Kind))
	actor.Name = strings.TrimSpace(actor.Name)
	actor.WorkspaceID = strings.TrimSpace(actor.WorkspaceID)
	if _, err := normalizeDomainIdentifier(actor.ID, "actor id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if _, err := normalizeDomainName(actor.Kind, "actor kind", MaxDomainNameLength, true); err != nil {
		return err
	}
	if len(actor.Name) > 256 || strings.ContainsAny(actor.Name, "\x00\r\n\t") {
		return fmt.Errorf("actor name is invalid or too large")
	}
	if actor.WorkspaceID != workspaceID {
		return fmt.Errorf("actor crosses workspace boundary")
	}
	return nil
}

func normalizeDomainEntity(ref *EntityRef, expectedKind, workspaceID string) error {
	if ref == nil {
		return nil
	}
	value := ref
	id, err := normalizeDomainIdentifier(value.ID, expectedKind+" id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	kind, err := normalizeDomainName(value.Kind, expectedKind+" kind", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	value.WorkspaceID = strings.TrimSpace(value.WorkspaceID)
	if kind != expectedKind || value.WorkspaceID != workspaceID {
		return fmt.Errorf("%s reference must be bounded and workspace-scoped", expectedKind)
	}
	value.ID, value.Kind = id, kind
	return nil
}

func normalizeDomainMetadata(metadata map[string]string) error {
	if len(metadata) > MaxDomainMetadataEntries {
		return fmt.Errorf("metadata exceeds %d entries", MaxDomainMetadataEntries)
	}
	normalized := make(map[string]string, len(metadata))
	for key, value := range metadata {
		key = strings.ToLower(strings.TrimSpace(key))
		if len(key) > MaxDomainMetadataKeyLength || !domainMetadataKeyPattern.MatchString(key) {
			return fmt.Errorf("metadata key is invalid")
		}
		for _, forbidden := range []string{"prompt", "secret", "credential", "token", "password", "authorization", "body", "content", "output", "input", "environment"} {
			if strings.Contains(key, forbidden) {
				return fmt.Errorf("metadata key %q is not permitted", key)
			}
		}
		value = strings.TrimSpace(value)
		if value == "" || len(value) > MaxDomainMetadataValueLen || !domainMetadataValuePattern.MatchString(value) || looksLikeSecret(value) {
			return fmt.Errorf("metadata value for %q is invalid or unsafe", key)
		}
		if _, exists := normalized[key]; exists {
			return fmt.Errorf("metadata contains duplicate normalized key %q", key)
		}
		normalized[key] = value
	}
	for key := range metadata {
		delete(metadata, key)
	}
	for key, value := range normalized {
		metadata[key] = value
	}
	return nil
}

func normalizeDomainStrings(values []string, field string, max int) ([]string, error) {
	if len(values) > max {
		return nil, fmt.Errorf("%s exceeds %d values", field, max)
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("%s contains an empty value", field)
		}
		normalized, err := normalizeDomainIdentifier(value, field, MaxDomainIDLength, true)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	// Stable sort gives deterministic hashes without changing semantic order in
	// contracts where this helper is used for set-like fields.
	sort.Strings(result)
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}
