package contracts

import (
	"fmt"
	"strings"
	"time"
)

const (
	TaskStatusPending    = "pending"
	TaskStatusClaimed    = "claimed"
	TaskStatusInProgress = "in_progress"
	TaskStatusDone       = "done"
	TaskStatusFailed     = "failed"
	TaskStatusCancelled  = "cancelled"
	TaskStatusDeadLetter = "deadletter"
)

const (
	FailureTransient    = "transient"
	FailureTimeout      = "timeout"
	FailureRateLimited  = "rate_limited"
	FailurePermanent    = "permanent"
	FailureDependency   = "dependency"
	FailureUnknown      = "unknown"
	DefaultTaskLeaseTTL = 30 * time.Second
	MaxTaskLeaseTTL     = 10 * time.Minute
	DefaultTaskAttempts = 3
	MaxTaskAttempts     = 20
	MaxTaskRetryBackoff = 5 * time.Minute
)

// TaskExecutionLease authorizes one worker to mutate one task. Fence is a
// monotonically increasing epoch and must be checked by the database while
// the lease row is locked.
type TaskExecutionLease struct {
	WorkspaceID string     `json:"workspace_id"`
	TaskID      int64      `json:"task_id"`
	OwnerID     string     `json:"owner_id"`
	Fence       uint64     `json:"fence"`
	LeaseUntil  time.Time  `json:"lease_until"`
	AcquiredAt  time.Time  `json:"acquired_at"`
	RenewedAt   time.Time  `json:"renewed_at"`
	ReleasedAt  *time.Time `json:"released_at,omitempty"`
}

type TaskExecutionResult struct {
	Lease    TaskExecutionLease `json:"lease"`
	Takeover bool               `json:"takeover"`
}

func ValidateTaskLeaseIdentity(workspaceID, ownerID string, taskID int64) error {
	if strings.TrimSpace(workspaceID) == "" {
		return fmt.Errorf("workspace_id is required")
	}
	if taskID <= 0 {
		return fmt.Errorf("task_id must be positive")
	}
	if strings.TrimSpace(ownerID) == "" {
		return fmt.Errorf("owner_id is required")
	}
	return nil
}

func IsTaskTerminal(status string) bool {
	switch strings.TrimSpace(status) {
	case TaskStatusDone, TaskStatusFailed, TaskStatusCancelled, TaskStatusDeadLetter:
		return true
	default:
		return false
	}
}

func IsFailureClass(value string) bool {
	switch strings.TrimSpace(value) {
	case FailureTransient, FailureTimeout, FailureRateLimited, FailurePermanent, FailureDependency, FailureUnknown:
		return true
	default:
		return false
	}
}
