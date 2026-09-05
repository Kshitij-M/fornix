package validation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/change"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/ingest"
	"github.com/omaveda/fornix/internal/store"
)

// HandoffSubmitter is the only live boundary after validation. The server
// implementation discovers the configured source and submits an IngestStore
// job; validators themselves remain read-only.
type HandoffSubmitter func(context.Context, contracts.ReindexHandoff) (contracts.IngestJob, error)

// Service composes deterministic validators with the Postgres validation
// authority. No model, shell, broker, or external tool is reachable here.
type Service struct {
	Registry      *Registry
	Runs          *store.ValidationStore
	SubmitHandoff HandoffSubmitter
	Discovery     func(context.Context, contracts.RepositorySource) (ingest.DiscoveryResult, error)
	Now           func() time.Time
}

// Result is the operator-visible outcome of a validation request.
type Result struct {
	Run     contracts.ValidationRun   `json:"run"`
	Handoff *contracts.ReindexHandoff `json:"handoff,omitempty"`
	DryRun  bool                      `json:"dry_run,omitempty"`
}

// Validate performs one bounded post-change validation. The source and mount
// roots are supplied by authenticated server configuration, never trusted from
// an unscoped request.
func (s *Service) Validate(ctx context.Context, request contracts.ValidationRequest, sourceRoot, mountRoot string) (Result, error) {
	if s == nil || s.Registry == nil || s.Runs == nil {
		return Result{}, fmt.Errorf("validation service is not configured")
	}
	if strings.TrimSpace(request.Source.SourceRoot) == "" {
		request.Source.SourceRoot = sourceRoot
	}
	request.Source.SourceRoot = sourceRoot
	request.Source.MountRoot = mountRoot
	request.Source.Repository = request.Repository
	if request.Repository == "" {
		request.Repository = request.Source.Repository
		request.Source.Repository = request.Repository
	}
	if request.ChangeApplicationID == "" || request.ProposalID == "" || request.PacketHash == "" || request.ExpectedTreeHash == "" {
		application, proposal, err := s.Runs.ChangeAuthority(ctx, request.WorkspaceID, request.ChangeApplicationID, request.ProposalID)
		if err != nil {
			return Result{}, err
		}
		if request.ChangeApplicationID == "" {
			request.ChangeApplicationID = application.ID
		}
		if request.ProposalID == "" {
			request.ProposalID = proposal.ID
		}
		if request.PacketHash == "" {
			request.PacketHash = application.PacketHash
		}
		if request.ExpectedTreeHash == "" {
			request.ExpectedTreeHash = application.ExpectedTreeHash
			if request.ExpectedTreeHash == "" {
				request.ExpectedTreeHash = proposal.ExpectedTreeHash
			}
		}
		if request.SourceManifestHash == "" {
			request.SourceManifestHash = proposal.Source.ManifestHash
		}
	}
	if err := request.Normalize(); err != nil {
		return Result{}, err
	}
	plan, err := request.Plan()
	if err != nil {
		return Result{}, err
	}
	for _, reference := range plan.Validators {
		if _, ok := s.Registry.Lookup(reference); !ok {
			return Result{}, fmt.Errorf("validator %s@%s is not registered", reference.ID, reference.Version)
		}
	}
	application, proposal, err := s.Runs.ChangeAuthority(ctx, request.WorkspaceID, request.ChangeApplicationID, request.ProposalID)
	if err != nil {
		return Result{}, err
	}
	// The authoritative proposal is the source of truth when the caller used
	// the compact request form. Rebuild the plan so the persisted request hash
	// covers the resolved manifest as well as the other authority fields.
	if request.SourceManifestHash == "" && proposal.Source.ManifestHash != "" {
		request.SourceManifestHash = proposal.Source.ManifestHash
		plan, err = request.Plan()
		if err != nil {
			return Result{}, err
		}
	}
	if request.DryRun {
		dry := s.execute(ctx, request, plan, application, proposal, "dry-run")
		return Result{Run: dry.Run, DryRun: true}, nil
	}
	run, _, err := s.Runs.Start(ctx, store.StartValidationInput{Request: request, Plan: plan})
	if err != nil {
		return Result{}, err
	}
	request.ID = run.ID
	if run.Status == contracts.ValidationPassed || run.Status == contracts.ValidationFailed || run.Status == contracts.ValidationAbstained || run.Status == contracts.ValidationCancelled {
		var handoff *contracts.ReindexHandoff
		if value, handoffErr := s.Runs.GetHandoff(ctx, run.WorkspaceID, "reindex-"+run.ID); handoffErr == nil {
			handoff = &value
		}
		return Result{Run: run, Handoff: handoff}, nil
	}
	result := s.execute(ctx, request, plan, application, proposal, run.ID)
	if result.Run.Report == nil {
		return Result{}, fmt.Errorf("validation report is missing")
	}
	if len(result.Run.Report.Results) == 0 {
		result.Run.Report.Results = failureResults(run.ID, run.WorkspaceID, plan, result.Run.Report.LastError)
	}
	committed, handoff, _, err := s.Runs.Commit(ctx, store.ValidationCommitInput{WorkspaceID: run.WorkspaceID, RunID: run.ID, Actor: request.Actor, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Results: result.Run.Report.Results, Report: *result.Run.Report, ObservedTree: result.Run.Report.ObservedTreeHash, Discovery: store.ManifestSummary{ManifestHash: result.discovery.ManifestHash, FileCount: len(result.discovery.Files), TotalBytes: result.discovery.TotalBytes}})
	if err != nil {
		return Result{}, err
	}
	out := Result{Run: committed, Handoff: handoff}
	if handoff != nil && s.SubmitHandoff != nil {
		job, submitErr := s.SubmitHandoff(ctx, *handoff)
		if submitErr != nil {
			failed, failErr := s.Runs.MarkHandoffFailed(ctx, handoff.WorkspaceID, handoff.ID, contracts.ValidationFailure{Code: contracts.ValidationFailureRecovery, Message: "re-index handoff submission failed"})
			if failErr != nil {
				return Result{}, failErr
			}
			out.Handoff = &failed
			return out, fmt.Errorf("re-index handoff submission failed")
		}
		submitted, markErr := s.Runs.MarkHandoffSubmitted(ctx, handoff.ID, handoff.WorkspaceID, job)
		if markErr != nil {
			return Result{}, markErr
		}
		out.Handoff = &submitted
	}
	return out, nil
}

// Resume reconstructs a durable request and reruns only the deterministic
// local validation boundary. Terminal runs are returned without work.
func (s *Service) Resume(ctx context.Context, workspaceID, runID, sourceRoot, mountRoot string) (Result, error) {
	run, err := s.Runs.Get(ctx, workspaceID, runID)
	if err != nil {
		return Result{}, err
	}
	request := contracts.ValidationRequest{SchemaVersion: contracts.ValidationSchemaVersion, ID: run.ID, RequestID: run.RequestID, IdempotencyKey: run.IdempotencyKey, WorkspaceID: run.WorkspaceID, Actor: run.Actor, Task: run.Task, Session: run.Session, AgentRun: run.AgentRun, TaskOwnerID: run.TaskOwnerID, TaskFence: run.TaskFence, ChangeApplicationID: run.ChangeApplicationID, ProposalID: run.ProposalID, PacketHash: run.PacketHash, ExpectedTreeHash: run.ExpectedTreeHash, Repository: run.Repository, Source: contracts.RepositorySource{Repository: run.Repository, SourceRoot: sourceRoot, MountRoot: mountRoot}, SourceManifestHash: run.SourceManifestHash, Validators: run.Plan.Validators, Budget: run.Budget}
	return s.Validate(ctx, request, sourceRoot, mountRoot)
}

type executionResult struct {
	Run       contracts.ValidationRun
	discovery ingest.DiscoveryResult
}

func (s *Service) execute(ctx context.Context, request contracts.ValidationRequest, plan contracts.ValidationPlan, application contracts.ChangeApplication, proposal contracts.ChangeProposal, runID string) executionResult {
	started := time.Now()
	if s.Now != nil {
		started = s.Now()
	}
	observation, err := change.ObserveAppliedPacketState(ctx, request.Source.SourceRoot, contracts.ChangePacket{SchemaVersion: contracts.ChangeSchemaVersion, WorkspaceID: request.WorkspaceID, Repository: request.Repository, Source: proposal.Source, Operations: proposal.Operations, Budgets: proposal.Budgets, ExpectedTreeHash: request.ExpectedTreeHash})
	if err != nil && observation.Conflict == nil {
		return executionResult{Run: failedExecutionRun(request, runID, contracts.ValidationFailure{Code: contracts.ValidationFailureRecovery, Message: "filesystem state could not be observed"})}
	}
	var discovery ingest.DiscoveryResult
	for _, reference := range plan.Validators {
		if reference.ID != contracts.ValidationValidatorReindex {
			continue
		}
		discover := s.Discovery
		if discover == nil {
			discover = ingest.Discover
		}
		discovery, err = discover(ctx, request.Source)
		if err != nil {
			return executionResult{Run: failedExecutionRun(request, runID, contracts.ValidationFailure{Code: contracts.ValidationFailureSourceConflict, Message: "re-index discovery failed"})}
		}
		break
	}
	input := Input{Request: request, Plan: plan, Proposal: proposal, Application: application, Root: request.Source.SourceRoot, MountRoot: request.Source.MountRoot, Observation: observation, Discovery: &discovery, SourceHash: discovery.ManifestHash}
	results, err := s.Registry.ValidatePlan(ctx, input)
	if err != nil {
		return executionResult{Run: failedExecutionRun(request, runID, contracts.ValidationFailure{Code: contracts.ValidationFailureValidator, Message: "validator execution failed"})}
	}
	report := contracts.ValidationReport{SchemaVersion: contracts.ValidationSchemaVersion, RunID: runID, WorkspaceID: request.WorkspaceID, PacketHash: request.PacketHash, ExpectedTreeHash: request.ExpectedTreeHash, ObservedTreeHash: observation.ResultTreeHash, Results: results, Files: observation.Files, Bytes: observation.Bytes, DurationMS: time.Since(started).Milliseconds()}
	runStatus, runOutcome := contracts.ValidationPending, contracts.ValidationOutcomeSkipped
	if request.DryRun {
		report.Status = "dry_run"
		report.Outcome = contracts.ValidationOutcomeSkipped
	} else {
		report.Status = contracts.ValidationPending
		report.Outcome = contracts.ValidationOutcomeSkipped
	}
	if request.DryRun {
		runStatus = contracts.ValidationAbstained
	}
	return executionResult{Run: contracts.ValidationRun{ID: runID, WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, RequestHash: plan.RequestHash, ChangeApplicationID: request.ChangeApplicationID, ProposalID: request.ProposalID, PacketHash: request.PacketHash, ExpectedTreeHash: request.ExpectedTreeHash, SourceManifestHash: request.SourceManifestHash, Repository: request.Repository, SourceRoot: request.Source.SourceRoot, Actor: request.Actor, Task: request.Task, Session: request.Session, AgentRun: request.AgentRun, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Plan: plan, Budget: request.Budget, Status: runStatus, Outcome: runOutcome, Report: &report}, discovery: discovery}
}

func failedExecutionRun(request contracts.ValidationRequest, runID string, failure contracts.ValidationFailure) contracts.ValidationRun {
	report := contracts.ValidationReport{SchemaVersion: contracts.ValidationSchemaVersion, RunID: runID, WorkspaceID: request.WorkspaceID, Status: contracts.ValidationFailed, Outcome: contracts.ValidationOutcomeFailed, PacketHash: request.PacketHash, ExpectedTreeHash: request.ExpectedTreeHash, LastError: failure.Message}
	return contracts.ValidationRun{ID: runID, WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, RequestHash: request.RequestHash(), ChangeApplicationID: request.ChangeApplicationID, ProposalID: request.ProposalID, PacketHash: request.PacketHash, ExpectedTreeHash: request.ExpectedTreeHash, SourceManifestHash: request.SourceManifestHash, Repository: request.Repository, SourceRoot: request.Source.SourceRoot, Actor: request.Actor, Task: request.Task, Session: request.Session, AgentRun: request.AgentRun, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Plan: contracts.ValidationPlan{SchemaVersion: contracts.ValidationSchemaVersion, WorkspaceID: request.WorkspaceID, RequestHash: request.RequestHash(), Validators: request.Validators, Budget: request.Budget}, Budget: request.Budget, Report: &report}
}

func failureResults(runID, workspaceID string, plan contracts.ValidationPlan, message string) []contracts.ValidationResult {
	results := make([]contracts.ValidationResult, len(plan.Validators))
	for ordinal, validator := range plan.Validators {
		result := contracts.ValidationResult{SchemaVersion: contracts.ValidationSchemaVersion, ID: fmt.Sprintf("%s-runtime-failure-%03d", runID, ordinal), RunID: runID, WorkspaceID: workspaceID, Ordinal: ordinal, Validator: validator, Attempt: 1, Status: contracts.ValidationOutcomeSkipped, Outcome: contracts.ValidationOutcomeSkipped, InputHash: plan.RequestHash, Summary: "validator not executed"}
		if ordinal == 0 {
			result.Status = contracts.ValidationFailed
			result.Outcome = contracts.ValidationOutcomeFailed
			result.Summary = "validation runtime failed"
			result.Failure = &contracts.ValidationFailure{Code: contracts.ValidationFailureValidator, Message: boundedFailureMessage(message)}
		}
		results[ordinal] = result
	}
	return results
}

func boundedFailureMessage(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return "validation runtime failed"
	}
	if len(message) > 512 {
		return message[:512]
	}
	return message
}
