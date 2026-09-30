package contracts

import (
	"fmt"
	"strings"
	"time"
)

const (
	FederationRetentionSchemaVersion = 1
	// FederationRetentionConsumerID is the durable consumer lease identity used
	// by scheduled retention owners. It is intentionally stable across process
	// restarts so takeover fencing remains authoritative in Postgres.
	FederationRetentionConsumerID    = "federation.retention"
	FederationRetentionActive        = "active"
	FederationRetentionExpired       = "expired"
	FederationRetentionAuthoritative = "authoritative"
	FederationRetentionOperational   = "operational"
	MaxFederationRetentionBatchSize  = 500
)

// FederationRetentionRequest controls one bounded workspace-local retention
// pass. Authoritative peer-command and control-event history is never selected
// by this API.
type FederationRetentionRequest struct {
	SchemaVersion  int       `json:"schema_version"`
	RequestID      string    `json:"request_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	WorkspaceID    string    `json:"workspace_id"`
	Before         time.Time `json:"before"`
	BatchSize      int       `json:"batch_size"`
	DryRun         bool      `json:"dry_run"`
	OwnerID        string    `json:"owner_id,omitempty"`
	Fence          uint64    `json:"fence,omitempty"`
	Actor          ActorRef  `json:"actor"`
	CausationID    string    `json:"causation_id,omitempty"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
}

func (r *FederationRetentionRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("federation retention request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = FederationRetentionSchemaVersion
	}
	if r.SchemaVersion != FederationRetentionSchemaVersion {
		return fmt.Errorf("unsupported federation retention schema_version %d", r.SchemaVersion)
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	if r.WorkspaceID == "" || len(r.WorkspaceID) > MaxDomainIDLength {
		return fmt.Errorf("workspace_id is invalid")
	}
	if r.Before.IsZero() {
		r.Before = time.Now().UTC()
	}
	r.Before = r.Before.UTC()
	r.RequestID = strings.TrimSpace(r.RequestID)
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	if r.RequestID == "" {
		r.RequestID = "federation-retention-" + HashStrings(r.WorkspaceID, r.Before.Format(time.RFC3339Nano))[:48]
	}
	if r.IdempotencyKey == "" {
		r.IdempotencyKey = r.RequestID
	}
	if len(r.RequestID) > MaxIdempotencyLength || len(r.IdempotencyKey) > MaxIdempotencyLength || strings.ContainsAny(r.RequestID, " \t\r\n") || strings.ContainsAny(r.IdempotencyKey, " \t\r\n") {
		return fmt.Errorf("federation retention request identity is invalid")
	}
	r.CausationID = strings.TrimSpace(r.CausationID)
	r.CorrelationID = strings.TrimSpace(r.CorrelationID)
	r.OwnerID = strings.TrimSpace(r.OwnerID)
	if (r.OwnerID == "") != (r.Fence == 0) {
		return fmt.Errorf("owner_id and fence must be supplied together")
	}
	if r.OwnerID != "" && len(r.OwnerID) > MaxDomainIDLength {
		return fmt.Errorf("owner_id is too large")
	}
	if r.BatchSize <= 0 {
		r.BatchSize = 100
	}
	if r.BatchSize > MaxFederationRetentionBatchSize {
		r.BatchSize = MaxFederationRetentionBatchSize
	}
	if len(r.CausationID) > MaxIdempotencyLength || len(r.CorrelationID) > MaxIdempotencyLength {
		return fmt.Errorf("federation retention correlation fields are too large")
	}
	if err := normalizeDomainActor(&r.Actor, r.WorkspaceID); err != nil {
		return err
	}
	return nil
}

// FederationRetentionResult is a bounded, deterministic operational report.
type FederationRetentionResult struct {
	WorkspaceID          string   `json:"workspace_id"`
	DryRun               bool     `json:"dry_run"`
	BatchSize            int      `json:"batch_size"`
	PollCandidates       int      `json:"poll_candidates"`
	PollExpired          int      `json:"poll_expired"`
	QuarantineCandidates int      `json:"quarantine_candidates"`
	QuarantineExpired    int      `json:"quarantine_expired"`
	ProtectedRecovery    int      `json:"protected_recovery"`
	ProtectedLease       int      `json:"protected_lease"`
	TombstoneHashes      []string `json:"tombstone_hashes,omitempty"`
}
