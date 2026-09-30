package generic

import (
	"context"
	"errors"
	"testing"
	"time"

	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestReadOnlyConnectorStepRequiresDurableAdmission(t *testing.T) {
	registry := connectorruntime.NewRegistry()
	executor := &ConnectorStepExecutor{
		Registry: registry,
		Executor: &connectorruntime.Executor{Registry: registry},
	}
	step := contracts.OperationStep{Effect: contracts.EffectClassReadOnly}
	if _, err := executor.Execute(context.Background(), contracts.WorkflowRun{}, contracts.WorkflowStepState{}, step); !errors.Is(err, ErrAdmissionAuthorityNeeded) {
		t.Fatalf("read-only step without durable admission error=%v, want ErrAdmissionAuthorityNeeded", err)
	}
}

func TestWorkflowAdmissionRateLimitRequiresDurableRetryDeadline(t *testing.T) {
	decision := contracts.AdmissionDecision{
		WorkspaceID: "workspace-a", OperationID: "operation-a", IdempotencyKey: "admission-a",
		DecisionHash: contracts.HashStrings("rate-denial"), Status: contracts.AdmissionDenied,
		ReasonCode: contracts.AdmissionReasonRateLimited,
	}
	withoutDeadline := workflowAdmissionResult(decision)
	if withoutDeadline.Status != contracts.WorkflowStepFailed || withoutDeadline.Failure == nil || withoutDeadline.Failure.Retryable {
		t.Fatalf("legacy denial without deadline must fail closed: %+v", withoutDeadline)
	}
	retryAt := time.Date(2030, time.January, 2, 3, 4, 5, 0, time.UTC)
	decision.RetryAt = &retryAt
	withDeadline := workflowAdmissionResult(decision)
	if withDeadline.Status != contracts.WorkflowStepAwaitingRetry || withDeadline.Wait == nil || withDeadline.Wait.Kind != contracts.WorkflowWaitRetry ||
		withDeadline.Wait.ExpiresAt == nil || !withDeadline.Wait.ExpiresAt.Equal(retryAt) || withDeadline.Failure == nil || !withDeadline.Failure.Retryable {
		t.Fatalf("rate denial was not converted to exact durable retry wait: %+v", withDeadline)
	}
}
