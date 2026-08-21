package tool

import (
	"errors"
	"fmt"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrUnauthorized     = errors.New("tool unauthorized")
	ErrApprovalRequired = errors.New("tool approval required")
	ErrApprovalDenied   = errors.New("tool approval denied")
	ErrRunInProgress    = errors.New("tool run is already in progress")
	ErrRunConflict      = errors.New("tool run request conflicts with existing idempotency record")
	ErrStaleTaskFence   = errors.New("tool task fence is stale")
)

// FailureError carries the stable redacted failure classification returned by
// policy, approval, execution, and durable lifecycle boundaries.
type FailureError struct{ Failure contracts.ToolFailure }

// Error returns a concise failure summary without output or secret material.
func (e *FailureError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("tool failure code=%s message=%s", e.Failure.Code, e.Failure.Message)
}

// Unwrap maps stable tool failure codes to sentinel errors for callers that
// need category checks without parsing strings.
func (e *FailureError) Unwrap() error {
	switch e.Failure.Code {
	case contracts.ToolFailureUnauthorized:
		return ErrUnauthorized
	case contracts.ToolFailureApprovalRequired:
		return ErrApprovalRequired
	case contracts.ToolFailureApprovalDenied, contracts.ToolFailureApprovalExpired:
		return ErrApprovalDenied
	case contracts.ToolFailureInProgress:
		return ErrRunInProgress
	case contracts.ToolFailureStaleFence:
		return ErrStaleTaskFence
	default:
		return nil
	}
}

func failure(code, message string, retryable bool) *FailureError {
	return &FailureError{Failure: contracts.ToolFailure{Code: code, Message: message, Retryable: retryable}}
}
