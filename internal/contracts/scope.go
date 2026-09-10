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
	MaxDomainHashLength        = 64
	MaxDomainMetadataEntries   = 32
	MaxDomainMetadataKeyLength = 64
	MaxDomainMetadataValueLen  = 128
	MaxDomainReferences        = 128
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
	if actor.ID == "" || len(actor.ID) > MaxDomainIDLength {
		return fmt.Errorf("actor id is required and bounded")
	}
	if actor.Kind == "" || len(actor.Kind) > MaxDomainNameLength {
		return fmt.Errorf("actor kind is required and bounded")
	}
	if len(actor.Name) > 256 || strings.ContainsAny(actor.Name, "\r\n") {
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
	value.ID = strings.TrimSpace(value.ID)
	value.Kind = strings.ToLower(strings.TrimSpace(value.Kind))
	value.WorkspaceID = strings.TrimSpace(value.WorkspaceID)
	if value.ID == "" || len(value.ID) > MaxDomainIDLength || value.Kind != expectedKind || value.WorkspaceID != workspaceID {
		return fmt.Errorf("%s reference must be bounded and workspace-scoped", expectedKind)
	}
	return nil
}

func normalizeDomainMetadata(metadata map[string]string) error {
	if len(metadata) > MaxDomainMetadataEntries {
		return fmt.Errorf("metadata exceeds %d entries", MaxDomainMetadataEntries)
	}
	for key, value := range metadata {
		if len(key) > MaxDomainMetadataKeyLength || !domainMetadataKeyPattern.MatchString(strings.ToLower(strings.TrimSpace(key))) {
			return fmt.Errorf("metadata key is invalid")
		}
		lowerKey := strings.ToLower(strings.TrimSpace(key))
		for _, forbidden := range []string{"prompt", "secret", "credential", "token", "password", "authorization", "body", "content", "output", "input", "environment"} {
			if strings.Contains(lowerKey, forbidden) {
				return fmt.Errorf("metadata key %q is not permitted", key)
			}
		}
		value = strings.TrimSpace(value)
		if value == "" || len(value) > MaxDomainMetadataValueLen || !domainMetadataValuePattern.MatchString(value) || looksLikeSecret(value) {
			return fmt.Errorf("metadata value for %q is invalid or unsafe", key)
		}
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
			continue
		}
		if len(value) > MaxDomainIDLength {
			return nil, fmt.Errorf("%s contains an oversized value", field)
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	// Stable sort gives deterministic hashes without changing semantic order in
	// contracts where this helper is used for set-like fields.
	sort.Strings(result)
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}
