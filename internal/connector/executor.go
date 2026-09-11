package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

// FailureError is the redacted failure classification used by the bounded
// executor. The optional wrapped error is for the immediate caller only; it
// must not be persisted without redaction and hashing.
type FailureError struct {
	Code                  string
	Retryable             bool
	ContentEmitted        bool
	ExternalEffectStarted bool
	Err                   error
}

func (e *FailureError) Error() string {
	if e == nil {
		return "connector capability failure"
	}
	code := strings.TrimSpace(e.Code)
	if code == "" {
		code = "unknown"
	}
	return "connector capability failure: " + code
}

func (e *FailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ExecutionOutcome contains the admission and deterministic plan alongside
// the adapter result. The operation store introduced by Issue #39 will persist
// these values and provide durable duplicate suppression.
type ExecutionOutcome struct {
	Admission Admission
	Plan      contracts.OperationPlan
	Result    contracts.OperationResult
	Attempts  int
}

// Executor invokes registered capabilities with bounded timeout and explicit
// retry classification. It never retries after an adapter reports emitted
// content or a started external effect, and it never claims exactly-once.
type Executor struct {
	Registry *Registry
}

// Execute admits, plans, and executes one capability. Every retry reuses the
// same normalized request and plan; it is therefore safe only when the
// capability's declared idempotency and external-effect semantics permit it.
func (e *Executor) Execute(ctx context.Context, request contracts.OperationRequest, options AdmissionOptions) (ExecutionOutcome, error) {
	if e == nil || e.Registry == nil {
		return ExecutionOutcome{}, ErrRegistryNil
	}
	admission, err := e.Registry.Admit(ctx, request, options)
	if err != nil {
		return ExecutionOutcome{}, err
	}
	plan, err := admission.Capability.Plan(admission.Request)
	if err != nil {
		return ExecutionOutcome{}, fmt.Errorf("plan capability: %w", err)
	}
	profile := admission.Request.Profile
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(profile.TimeoutMS)*time.Millisecond)
	defer cancel()

	policy := admission.Definition.RetryPolicy
	// MaxRetries is a request-level tightening budget. Capability policy owns
	// the upper bound and failure classes; the caller may still request fewer
	// retries for a particular operation.
	requestMaxAttempts := admission.Request.Profile.MaxRetries + 1
	if requestMaxAttempts < policy.MaxAttempts {
		policy.MaxAttempts = requestMaxAttempts
	}
	attempts := 0
	for attempts < policy.MaxAttempts {
		attempts++
		result, executeErr := admission.Capability.Execute(runCtx, admission.Request, plan)
		if executeErr == nil {
			if err := validateResult(admission.Request, admission.Definition, result); err != nil {
				return ExecutionOutcome{}, err
			}
			return ExecutionOutcome{Admission: admission, Plan: plan, Result: result, Attempts: attempts}, nil
		}
		failure := failureFromError(executeErr)
		if !shouldRetry(runCtx, failure, policy, attempts, result) {
			return ExecutionOutcome{}, fmt.Errorf("execute capability after %d attempt(s): %w", attempts, executeErr)
		}
		if err := waitBackoff(runCtx, policy, attempts); err != nil {
			return ExecutionOutcome{}, err
		}
	}
	return ExecutionOutcome{}, fmt.Errorf("execute capability exhausted after %d attempt(s)", attempts)
}

func failureFromError(err error) *FailureError {
	var failure *FailureError
	if errors.As(err, &failure) {
		return failure
	}
	return nil
}

func shouldRetry(ctx context.Context, failure *FailureError, policy contracts.CapabilityRetryPolicy, attempt int, result contracts.OperationResult) bool {
	if failure == nil || !failure.Retryable || failure.ContentEmitted || failure.ExternalEffectStarted || len(result.ExternalEffects) > 0 || attempt >= policy.MaxAttempts {
		return false
	}
	if err := ctx.Err(); err != nil {
		return false
	}
	code := strings.ToLower(strings.TrimSpace(failure.Code))
	for _, allowed := range policy.RetryableCodes {
		if code == strings.ToLower(strings.TrimSpace(allowed)) {
			return true
		}
	}
	return false
}

func waitBackoff(ctx context.Context, policy contracts.CapabilityRetryPolicy, attempt int) error {
	delay := policy.BackoffMS
	for index := 1; index < attempt; index++ {
		if delay >= policy.MaxBackoffMS/2 {
			delay = policy.MaxBackoffMS
			break
		}
		delay *= 2
	}
	if delay > policy.MaxBackoffMS {
		delay = policy.MaxBackoffMS
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func validateResult(request contracts.OperationRequest, definition contracts.CapabilityDefinition, result contracts.OperationResult) error {
	if err := result.Normalize(); err != nil {
		return fmt.Errorf("normalize operation result: %w", err)
	}
	if result.WorkspaceID != request.WorkspaceID || result.OperationID != request.ID || result.OperationHash != request.StableHash() || result.Actor.ID != request.Actor.ID {
		return fmt.Errorf("operation result is not bound to the admitted request")
	}
	if err := validateResultSchema(result, definition); err != nil {
		return err
	}
	return nil
}
