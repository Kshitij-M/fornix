package contracts

import (
	"fmt"
	"strings"
	"time"
)

const (
	SandboxCleanupProtocolVersion = 1

	SandboxCleanupPending    = "pending"
	SandboxCleanupLeased     = "leased"
	SandboxCleanupRetryWait  = "retry_wait"
	SandboxCleanupCompleted  = "completed"
	SandboxCleanupDeadLetter = "dead_letter"

	SandboxCleanupRemoved          = "removed"
	SandboxCleanupAlreadyAbsent    = "already_absent"
	SandboxCleanupUnknown          = "unknown"
	SandboxCleanupIdentityMismatch = "identity_mismatch"

	MaxSandboxCleanupFailureCodeLength = 64
	MaxSandboxCleanupAttempts          = 12
)

// SandboxCleanupIntent binds post-finalization cleanup to the exact durable
// workspace-scoped tool attempt. Identity contains identifiers and hashes
// only; it must never contain commands, paths, credentials, or output.
type SandboxCleanupIntent struct {
	WorkspaceID  string                   `json:"workspace_id"`
	ToolRunID    string                   `json:"tool_run_id"`
	ToolAttempt  int                      `json:"tool_attempt"`
	DomainLinkID string                   `json:"domain_link_id"`
	Identity     SandboxExecutionIdentity `json:"identity"`
	ResultHash   string                   `json:"result_hash"`
	Actor        ActorRef                 `json:"actor"`
}

// Normalize validates all durable cleanup bindings and rejects local-process
// identities, which do not own an externally managed runtime object.
func (i *SandboxCleanupIntent) Normalize() error {
	if i == nil {
		return fmt.Errorf("sandbox cleanup intent is nil")
	}
	workspace, err := normalizeDomainWorkspace(i.WorkspaceID)
	if err != nil {
		return fmt.Errorf("sandbox cleanup workspace is invalid")
	}
	for name, value := range map[string]*string{
		"sandbox cleanup tool_run_id":    &i.ToolRunID,
		"sandbox cleanup domain_link_id": &i.DomainLinkID,
	} {
		*value, err = normalizeDomainIdentifier(*value, name, MaxDomainIDLength, true)
		if err != nil {
			return err
		}
	}
	if i.ToolAttempt < 1 || i.ToolAttempt > 1_000_000 {
		return fmt.Errorf("sandbox cleanup tool attempt is invalid")
	}
	if err := i.Identity.Normalize(); err != nil {
		return fmt.Errorf("sandbox cleanup identity is invalid: %w", err)
	}
	if i.Identity.Backend == SandboxBackendLocalProcess || i.Identity.WorkspaceID != workspace ||
		i.Identity.ToolRunID != i.ToolRunID || i.Identity.ToolAttempt != i.ToolAttempt {
		return fmt.Errorf("sandbox cleanup identity scope or backend does not match")
	}
	i.ResultHash, err = normalizeDomainHash(i.ResultHash, "sandbox cleanup result hash", true)
	if err != nil {
		return err
	}
	i.Actor.ID = strings.TrimSpace(i.Actor.ID)
	i.Actor.Kind = strings.TrimSpace(i.Actor.Kind)
	i.Actor.Name = ""
	i.Actor.WorkspaceID = strings.TrimSpace(i.Actor.WorkspaceID)
	if i.Actor.WorkspaceID != workspace || i.Actor.ID == "" || len(i.Actor.ID) > MaxDomainIDLength {
		return fmt.Errorf("sandbox cleanup actor is invalid")
	}
	i.WorkspaceID = workspace
	return nil
}

// StableHash returns the content identity of a cleanup intent, excluding
// creation time and queue lease state.
func (i SandboxCleanupIntent) StableHash() string {
	if err := i.Normalize(); err != nil {
		return ""
	}
	return HashStrings("fornix.sandbox.cleanup.intent.v1", i.WorkspaceID, i.ToolRunID,
		fmt.Sprint(i.ToolAttempt), i.DomainLinkID, i.Identity.StableHash(), i.ResultHash)
}

// SandboxCleanupLease is the fencing token required for every worker mutation.
type SandboxCleanupLease struct {
	WorkspaceID string    `json:"workspace_id"`
	JobID       string    `json:"job_id"`
	OwnerID     string    `json:"owner_id"`
	Fence       uint64    `json:"fence"`
	LeaseUntil  time.Time `json:"lease_until"`
}

// SandboxCleanupJob is the mutable queue projection for one immutable intent.
type SandboxCleanupJob struct {
	ID                string               `json:"id"`
	Intent            SandboxCleanupIntent `json:"intent"`
	IntentHash        string               `json:"intent_hash"`
	Status            string               `json:"status"`
	OwnerID           string               `json:"owner_id,omitempty"`
	Fence             uint64               `json:"fence"`
	Attempts          int                  `json:"attempts"`
	RetryAt           time.Time            `json:"retry_at"`
	LeaseUntil        *time.Time           `json:"lease_until,omitempty"`
	FailureCode       string               `json:"failure_code,omitempty"`
	CompletionState   string               `json:"completion_state,omitempty"`
	CompletionOwnerID string               `json:"completion_owner_id,omitempty"`
	Version           int64                `json:"version"`
	CreatedAt         time.Time            `json:"created_at"`
	UpdatedAt         time.Time            `json:"updated_at"`
	CompletedAt       *time.Time           `json:"completed_at,omitempty"`
}

// SandboxCleanupCommand authorizes the host runner to inspect and remove one
// exact runtime object. It contains identifiers and hashes only; PostgreSQL
// still decides whether the owner/fence is current when the result is stored.
type SandboxCleanupCommand struct {
	SchemaVersion int                  `json:"schema_version"`
	JobID         string               `json:"job_id"`
	OwnerID       string               `json:"owner_id"`
	Fence         uint64               `json:"fence"`
	IntentHash    string               `json:"intent_hash"`
	Intent        SandboxCleanupIntent `json:"intent"`
}

// Normalize validates the private runner command against its immutable
// cleanup intent. It does not assert that a Postgres lease is still current.
func (c *SandboxCleanupCommand) Normalize() error {
	if c == nil {
		return fmt.Errorf("sandbox cleanup command is nil")
	}
	if c.SchemaVersion == 0 {
		c.SchemaVersion = SandboxCleanupProtocolVersion
	}
	if c.SchemaVersion != SandboxCleanupProtocolVersion {
		return fmt.Errorf("sandbox cleanup command schema version is unsupported")
	}
	jobID, err := normalizeDomainIdentifier(c.JobID, "sandbox cleanup job_id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	ownerID, err := normalizeDomainIdentifier(c.OwnerID, "sandbox cleanup owner_id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	if c.Fence == 0 || c.Fence > uint64(1<<63-1) {
		return fmt.Errorf("sandbox cleanup command fence is invalid")
	}
	if err := c.Intent.Normalize(); err != nil {
		return fmt.Errorf("sandbox cleanup command intent is invalid")
	}
	intentHash, err := normalizeDomainHash(c.IntentHash, "sandbox cleanup intent hash", true)
	if err != nil || intentHash != c.Intent.StableHash() {
		return fmt.Errorf("sandbox cleanup command intent hash does not match")
	}
	c.JobID, c.OwnerID, c.IntentHash = jobID, ownerID, intentHash
	return nil
}

// JobAndLease returns the bounded synthetic leased view needed to validate a
// runner observation. It is not a database lease check; callers must still
// commit through SandboxCleanupStore with the same fence.
func (c SandboxCleanupCommand) JobAndLease() (SandboxCleanupJob, SandboxCleanupLease, error) {
	if err := c.Normalize(); err != nil {
		return SandboxCleanupJob{}, SandboxCleanupLease{}, err
	}
	job := SandboxCleanupJob{ID: c.JobID, Intent: c.Intent, IntentHash: c.IntentHash, Status: SandboxCleanupLeased, OwnerID: c.OwnerID, Fence: c.Fence}
	lease := SandboxCleanupLease{WorkspaceID: c.Intent.WorkspaceID, JobID: c.JobID, OwnerID: c.OwnerID, Fence: c.Fence}
	return job, lease, nil
}

// SandboxCleanupObservation is a bounded runner statement about one exact
// cleanup intent. Only removed and already_absent can complete a job.
type SandboxCleanupObservation struct {
	WorkspaceID  string `json:"workspace_id"`
	JobID        string `json:"job_id"`
	OwnerID      string `json:"owner_id"`
	Fence        uint64 `json:"fence"`
	IdentityHash string `json:"identity_hash"`
	RequestHash  string `json:"request_hash"`
	State        string `json:"state"`
	FailureCode  string `json:"failure_code,omitempty"`
}

// Normalize validates a runner observation against its currently leased job.
func (o *SandboxCleanupObservation) Normalize(job SandboxCleanupJob, lease SandboxCleanupLease) error {
	if o == nil || job.Status != SandboxCleanupLeased || lease.WorkspaceID != job.Intent.WorkspaceID ||
		lease.JobID != job.ID || lease.OwnerID == "" || lease.Fence == 0 ||
		o.WorkspaceID != lease.WorkspaceID || o.JobID != lease.JobID || o.OwnerID != lease.OwnerID || o.Fence != lease.Fence {
		return fmt.Errorf("sandbox cleanup observation lease does not match")
	}
	identityHash := job.Intent.Identity.StableHash()
	if identityHash == "" || o.IdentityHash != identityHash || o.RequestHash != job.Intent.Identity.ToolRequestHash {
		return fmt.Errorf("sandbox cleanup observation identity does not match")
	}
	switch o.State {
	case SandboxCleanupRemoved, SandboxCleanupAlreadyAbsent:
		if o.FailureCode != "" {
			return fmt.Errorf("successful sandbox cleanup observation has a failure code")
		}
	case SandboxCleanupUnknown, SandboxCleanupIdentityMismatch:
		o.FailureCode = strings.ToLower(strings.TrimSpace(o.FailureCode))
		if o.FailureCode == "" || len(o.FailureCode) > MaxSandboxCleanupFailureCodeLength {
			return fmt.Errorf("sandbox cleanup failure code is invalid")
		}
		for _, char := range o.FailureCode {
			if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_') {
				return fmt.Errorf("sandbox cleanup failure code is invalid")
			}
		}
	default:
		return fmt.Errorf("sandbox cleanup observation state is invalid")
	}
	return nil
}

// NormalizeForCommand validates the returned observation against the exact
// cleanup command that crossed the private runner boundary.
func (o *SandboxCleanupObservation) NormalizeForCommand(command SandboxCleanupCommand) error {
	job, lease, err := command.JobAndLease()
	if err != nil {
		return err
	}
	return o.Normalize(job, lease)
}
