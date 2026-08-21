package contracts

import (
	"fmt"
	"strings"
	"time"
)

const (
	DefaultAgentRunLeaseTTL = 30 * time.Second
	MaxAgentRunLeaseTTL     = 10 * time.Minute
	DefaultAgentRunPoll     = 500 * time.Millisecond
	MaxAgentRunPoll         = 30 * time.Second
)

// AgentRunLease is the durable worker ownership token for one
// (workspace_id, run_id). Fence is a monotonic epoch: a takeover or release
// followed by a new acquisition always receives a higher fence.
// LeaseTTLMS is an in-process renewal hint and is not an authority field.
type AgentRunLease struct {
	WorkspaceID string     `json:"workspace_id"`
	RunID       string     `json:"run_id"`
	OwnerID     string     `json:"owner_id"`
	Fence       uint64     `json:"fence"`
	LeaseUntil  time.Time  `json:"lease_until"`
	AcquiredAt  time.Time  `json:"acquired_at"`
	RenewedAt   time.Time  `json:"renewed_at"`
	ReleasedAt  *time.Time `json:"released_at,omitempty"`
	LeaseTTLMS  int64      `json:"lease_ttl_ms,omitempty"`
}

// AgentRunLeaseResult reports whether a durable run lease was acquired,
// reused, or taken over.
type AgentRunLeaseResult struct {
	Lease    AgentRunLease `json:"lease"`
	Acquired bool          `json:"acquired"`
	Takeover bool          `json:"takeover"`
	Reused   bool          `json:"reused"`
}

// AgentRunClaim combines a runnable run with its current fencing lease.
type AgentRunClaim struct {
	Run      AgentRun      `json:"run"`
	Lease    AgentRunLease `json:"lease"`
	Takeover bool          `json:"takeover"`
	Reused   bool          `json:"reused"`
}

// ValidateAgentRunLeaseIdentity rejects incomplete ownership identities before
// they can be used in a lease or fenced mutation.
func ValidateAgentRunLeaseIdentity(workspaceID, runID, ownerID string) error {
	if strings.TrimSpace(workspaceID) == "" {
		return fmt.Errorf("workspace_id is required")
	}
	if strings.TrimSpace(runID) == "" {
		return fmt.Errorf("run_id is required")
	}
	if strings.TrimSpace(ownerID) == "" {
		return fmt.Errorf("owner_id is required")
	}
	return nil
}

// ActiveAt reports whether this exact lease is live at the supplied authority
// time. Callers must still validate the token transactionally in Postgres.
func (l AgentRunLease) ActiveAt(now time.Time) bool {
	return ValidateAgentRunLeaseIdentity(l.WorkspaceID, l.RunID, l.OwnerID) == nil &&
		l.Fence > 0 && l.ReleasedAt == nil && l.LeaseUntil.After(now)
}

// NormalizeAgentRunLeaseTTL clamps a worker renewal hint to safe bounds.
func NormalizeAgentRunLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = DefaultAgentRunLeaseTTL
	}
	if ttl > MaxAgentRunLeaseTTL {
		ttl = MaxAgentRunLeaseTTL
	}
	if ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	return ttl
}
