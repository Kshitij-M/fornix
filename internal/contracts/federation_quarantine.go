package contracts

import (
	"fmt"
	"strings"
	"time"
)

// FederationQuarantineSchemaVersion versions the redacted disposition
// records for historical global federation rows. These records describe an
// audit action; they never establish ownership of the old peer.
const FederationQuarantineSchemaVersion = 1

const (
	FederationLegacyQuarantined        = "quarantined"
	MaxFederationQuarantineReasonBytes = 512
	MaxFederationQuarantinePageSize    = 500
)

// FederationLegacyQuarantineRequest explicitly chooses the workspace that
// owns the audit record. AuditWorkspaceID is not inferred ownership of the
// historical peer.
type FederationLegacyQuarantineRequest struct {
	SchemaVersion    int      `json:"schema_version"`
	RequestID        string   `json:"request_id"`
	IdempotencyKey   string   `json:"idempotency_key"`
	AuditWorkspaceID string   `json:"audit_workspace_id"`
	Reason           string   `json:"reason"`
	Limit            int      `json:"limit"`
	Actor            ActorRef `json:"actor"`
}

// FederationLegacyQuarantineRecord is safe to disclose. It contains hashes
// of historical source values rather than URLs, bearer tokens, or bodies.
type FederationLegacyQuarantineRecord struct {
	SchemaVersion    int       `json:"schema_version"`
	ID               string    `json:"id"`
	AuditWorkspaceID string    `json:"audit_workspace_id"`
	LegacyTable      string    `json:"legacy_table"`
	LegacyPeerID     string    `json:"legacy_peer_id"`
	SourceURLHash    string    `json:"source_url_hash"`
	RowHash          string    `json:"row_hash"`
	Disposition      string    `json:"disposition"`
	Reason           string    `json:"reason"`
	RequestID        string    `json:"request_id"`
	IdempotencyKey   string    `json:"idempotency_key"`
	Actor            ActorRef  `json:"actor"`
	CreatedAt        time.Time `json:"created_at"`
}

type FederationLegacyQuarantinePage struct {
	Items      []FederationLegacyQuarantineRecord `json:"items"`
	NextCursor string                             `json:"next_cursor,omitempty"`
}

func (r *FederationLegacyQuarantineRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("federation quarantine request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = FederationQuarantineSchemaVersion
	}
	if r.SchemaVersion != FederationQuarantineSchemaVersion {
		return fmt.Errorf("unsupported federation quarantine schema_version %d", r.SchemaVersion)
	}
	var err error
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "federation quarantine request_id", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "federation quarantine idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	workspace, err := normalizeDomainWorkspace(r.AuditWorkspaceID)
	if err != nil {
		return err
	}
	r.AuditWorkspaceID = workspace
	r.Reason = strings.TrimSpace(r.Reason)
	if r.Reason == "" || len([]byte(r.Reason)) > MaxFederationQuarantineReasonBytes {
		return fmt.Errorf("federation quarantine reason is required and bounded")
	}
	if r.Limit == 0 {
		r.Limit = MaxFederationQuarantinePageSize
	}
	if r.Limit < 1 || r.Limit > MaxFederationQuarantinePageSize {
		return fmt.Errorf("federation quarantine limit is outside bounds")
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	return nil
}

func (r *FederationLegacyQuarantineRecord) Normalize() error {
	if r == nil {
		return fmt.Errorf("federation quarantine record is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = FederationQuarantineSchemaVersion
	}
	if r.SchemaVersion != FederationQuarantineSchemaVersion {
		return fmt.Errorf("unsupported federation quarantine schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.AuditWorkspaceID)
	if err != nil {
		return err
	}
	for name, value := range map[string]*string{
		"quarantine id":   &r.ID,
		"legacy table":    &r.LegacyTable,
		"legacy peer id":  &r.LegacyPeerID,
		"source url hash": &r.SourceURLHash,
		"row hash":        &r.RowHash,
		"disposition":     &r.Disposition,
		"reason":          &r.Reason,
		"request id":      &r.RequestID,
		"idempotency key": &r.IdempotencyKey,
	} {
		*value = strings.TrimSpace(*value)
		if *value == "" {
			return fmt.Errorf("federation quarantine %s is required", name)
		}
	}
	if len(r.ID) > MaxEventIDLength || len(r.LegacyTable) > 128 || len(r.LegacyPeerID) > MaxIdempotencyLength || len(r.RequestID) > MaxIdempotencyLength || len(r.IdempotencyKey) > MaxIdempotencyLength || len([]byte(r.Reason)) > MaxFederationQuarantineReasonBytes {
		return fmt.Errorf("federation quarantine identity is too large")
	}
	if !isLowerHexHash(r.SourceURLHash) || !isLowerHexHash(r.RowHash) {
		return fmt.Errorf("federation quarantine hashes are invalid")
	}
	if r.Disposition != FederationLegacyQuarantined {
		return fmt.Errorf("invalid federation quarantine disposition %q", r.Disposition)
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	r.AuditWorkspaceID = workspace
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now().UTC()
	}
	r.CreatedAt = r.CreatedAt.UTC()
	return nil
}

func isLowerHexHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}
