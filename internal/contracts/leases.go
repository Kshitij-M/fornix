package contracts

import (
	"fmt"
	"strings"
	"time"
)

const (
	// DefaultConsumerLeaseTTL bounds how long an owner can remain authoritative
	// without renewal. The database clock remains authoritative for expiry.
	DefaultConsumerLeaseTTL = 30 * time.Second
	MaxConsumerLeaseTTL     = 10 * time.Minute
)

// ConsumerLease is the current durable ownership contract for one
// workspace-scoped consumer. Fence is a monotonic epoch: every takeover gets
// a larger value, so a stale worker cannot be mistaken for the new owner.
type ConsumerLease struct {
	WorkspaceID string     `json:"workspace_id"`
	ConsumerID  string     `json:"consumer_id"`
	OwnerID     string     `json:"owner_id"`
	Fence       uint64     `json:"fence"`
	LeaseUntil  time.Time  `json:"lease_until"`
	AcquiredAt  time.Time  `json:"acquired_at"`
	RenewedAt   time.Time  `json:"renewed_at"`
	ReleasedAt  *time.Time `json:"released_at,omitempty"`
}

// ConsumerLeaseResult makes idempotent acquisition and takeover observable
// without requiring callers to inspect timestamps or compare fence values.
type ConsumerLeaseResult struct {
	Lease    ConsumerLease `json:"lease"`
	Acquired bool          `json:"acquired"`
	Takeover bool          `json:"takeover"`
	Reused   bool          `json:"reused"`
}

// ActiveAt reports whether this lease can authorize a transaction at the
// supplied instant. Production validation uses PostgreSQL's clock instead.
func (l ConsumerLease) ActiveAt(now time.Time) bool {
	return strings.TrimSpace(l.WorkspaceID) != "" &&
		strings.TrimSpace(l.ConsumerID) != "" &&
		strings.TrimSpace(l.OwnerID) != "" &&
		l.Fence > 0 &&
		l.ReleasedAt == nil &&
		l.LeaseUntil.After(now)
}

// ValidateConsumerLeaseIdentity keeps all store entry points aligned on the
// explicit workspace/consumer/owner boundary.
func ValidateConsumerLeaseIdentity(workspaceID, consumerID, ownerID string) error {
	if strings.TrimSpace(workspaceID) == "" {
		return fmt.Errorf("workspace_id is required")
	}
	if strings.TrimSpace(consumerID) == "" {
		return fmt.Errorf("consumer_id is required")
	}
	if strings.TrimSpace(ownerID) == "" {
		return fmt.Errorf("owner_id is required")
	}
	return nil
}
