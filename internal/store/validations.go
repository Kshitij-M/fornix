package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrValidationNotFound         = errors.New("validation run not found")
	ErrValidationConflict         = errors.New("validation run identity conflict")
	ErrValidationStaleFence       = errors.New("validation task fence is stale")
	ErrValidationTerminal         = errors.New("validation run is terminal")
	ErrValidationAuthority        = errors.New("validation authority is not verified")
	ErrValidationResultConflict   = errors.New("validation result identity conflict")
	ErrValidationResultCount      = errors.New("validation result count does not match plan")
	ErrValidationEvidence         = errors.New("validation evidence is not authoritative")
	ErrValidationDisclosureBudget = errors.New("validation disclosure exceeds budget")
	ErrHandoffNotFound            = errors.New("re-index handoff not found")
	ErrHandoffConflict            = errors.New("re-index handoff identity conflict")
)

// ValidationStore is the Postgres authority for validation identities,
// immutable check results, report links, and re-index handoff state. It has no
// worker or network behavior; callers provide already-observed bounded results.
type ValidationStore struct {
	pool          *pgxpool.Pool
	events        *EventStore
	evidence      *EvidenceStore
	artifacts     *ArtifactStore
	receipts      *WorkReceiptStore
	observability *ObservabilityStore
	policies      *PolicyStore
	failureHook   func(string) error
}

// NewValidationStore creates the validation authority over the shared pool.
func NewValidationStore(pool *pgxpool.Pool, events *EventStore, evidence *EvidenceStore, artifacts *ArtifactStore, observability *ObservabilityStore) *ValidationStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	if evidence == nil {
		evidence = NewEvidenceStore(pool)
	}
	if artifacts == nil {
		artifacts = NewArtifactStore(pool)
	}
	return &ValidationStore{pool: pool, events: events, evidence: evidence, artifacts: artifacts, observability: observability}
}

// SetReceiptStore attaches the shared immutable receipt authority. Validation
// receipts are staged in the same transaction as the terminal result.
func (s *ValidationStore) SetReceiptStore(receipts *WorkReceiptStore) {
	if s != nil {
		s.receipts = receipts
	}
}

// SetPolicyStore attaches the immutable policy resolver used for new
// validation admission. Existing runs keep their persisted policy snapshot.
func (s *ValidationStore) SetPolicyStore(policies *PolicyStore) {
	if s != nil {
		s.policies = policies
	}
}

// ResolvePolicy exposes the store-owned admission resolver to the validation
// service without allowing callers to bypass the Postgres policy snapshot.
func (s *ValidationStore) ResolvePolicy(ctx context.Context, request contracts.PolicyEvaluationRequest) (contracts.PolicyResolution, error) {
	if s == nil || s.policies == nil {
		return contracts.PolicyResolution{}, fmt.Errorf("policy store is not configured")
	}
	return s.policies.Resolve(ctx, request)
}

// PolicyStoreConfigured reports whether new validation admissions are policy
// aware. It lets compatibility-mode unit tests keep their historical setup.
func (s *ValidationStore) PolicyStoreConfigured() bool {
	return s != nil && s.policies != nil
}

// SetFailureHook installs deterministic crash points for transaction tests.
func (s *ValidationStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

// ChangeAuthority returns the applied application and its immutable proposal
// inside the requested workspace. It is used by the read-only validator
// runtime to avoid accepting caller-supplied packet contents as authority.
func (s *ValidationStore) ChangeAuthority(ctx context.Context, workspaceID, applicationID, proposalID string) (contracts.ChangeApplication, contracts.ChangeProposal, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var application contracts.ChangeApplication
	if strings.TrimSpace(applicationID) != "" {
		application, err = readChangeApplicationTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(applicationID), false)
		if err != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
		}
	} else {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, fmt.Errorf("change_application_id is required")
	}
	proposal, err := readChangeProposalTx(ctx, tx, application.WorkspaceID, application.ProposalID, false)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	if proposalID != "" && proposal.ID != strings.TrimSpace(proposalID) {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, ErrValidationAuthority
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	return application, proposal, nil
}

func (s *ValidationStore) fail(stage string) error {
	if s != nil && s.failureHook != nil {
		return s.failureHook(stage)
	}
	return nil
}

// StartValidationInput is the normalized durable admission request. Source
// roots must have been resolved from authenticated workspace configuration by
// the caller; they are not trusted merely because they are absolute.
type StartValidationInput struct {
	Request contracts.ValidationRequest
	Plan    contracts.ValidationPlan
}

// Start creates or reuses a validation run after verifying that the referenced
// change application is applied and that any task fence is still live.
func (s *ValidationStore) Start(ctx context.Context, input StartValidationInput) (contracts.ValidationRun, bool, error) {
	if s == nil || s.pool == nil || s.events == nil || s.evidence == nil {
		return contracts.ValidationRun{}, false, fmt.Errorf("validation store is not configured")
	}
	request := input.Request
	requestedValidationBudget := request.Budget
	requestedValidators := append([]contracts.ValidatorRef(nil), request.Validators...)
	if err := request.Normalize(); err != nil {
		return contracts.ValidationRun{}, false, err
	}
	policySelected := false
	policyRequiresReindex := false
	if s.policies != nil {
		resolution, resolveErr := s.policies.Resolve(ctx, contracts.PolicyEvaluationRequest{
			WorkspaceID: request.WorkspaceID, Policy: request.Policy,
			RequestedValidators: requestedValidators,
			RequestedBudget:     contracts.PolicyBudget{Validation: requestedValidationBudget},
			Operation:           "validation",
		})
		if resolveErr != nil {
			return contracts.ValidationRun{}, false, resolveErr
		}
		if resolution.Selected {
			policySelected = true
			policyRequiresReindex = resolution.RequireReindex
			request.Policy = contracts.ClonePolicyReference(resolution.Ref)
			request.Validators = append([]contracts.ValidatorRef(nil), resolution.Validators...)
			request.Budget = resolution.Budget.Validation
		}
	}
	plan := input.Plan
	if plan.SchemaVersion == 0 {
		var err error
		plan, err = request.Plan()
		if err != nil {
			return contracts.ValidationRun{}, false, err
		}
	}
	if policySelected {
		// The caller-supplied plan is not trusted for policy-controlled
		// behavior. Keep its structural identity check above, then overwrite
		// this operational decision from the exact Postgres policy version.
		plan.RequireReindex = policyRequiresReindex
	}
	if plan.WorkspaceID != request.WorkspaceID || plan.RequestHash != request.RequestHash() {
		return contracts.ValidationRun{}, false, fmt.Errorf("validation plan does not match request")
	}
	if !filepath.IsAbs(request.Source.SourceRoot) {
		return contracts.ValidationRun{}, false, fmt.Errorf("validation source_root must be absolute")
	}
	actorJSON, _ := json.Marshal(request.Actor)
	taskJSON, err := jsonOrEmpty(request.Task)
	if err != nil {
		return contracts.ValidationRun{}, false, err
	}
	sessionJSON, err := jsonOrEmpty(request.Session)
	if err != nil {
		return contracts.ValidationRun{}, false, err
	}
	agentJSON, err := jsonOrEmpty(request.AgentRun)
	if err != nil {
		return contracts.ValidationRun{}, false, err
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return contracts.ValidationRun{}, false, err
	}
	budgetJSON, err := json.Marshal(request.Budget)
	if err != nil {
		return contracts.ValidationRun{}, false, err
	}
	requestHash := request.RequestHash()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ValidationRun{}, false, fmt.Errorf("begin validation start: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if s.policies != nil {
		if err := s.policies.LockActiveTx(ctx, tx, request.Policy); err != nil {
			return contracts.ValidationRun{}, false, err
		}
	}
	if err := s.validateChangeAuthorityTx(ctx, tx, request); err != nil {
		return contracts.ValidationRun{}, false, err
	}
	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.validation_runs(
			id,workspace_id,request_id,idempotency_key,request_hash,
			change_application_id,proposal_id,packet_hash,expected_tree_hash,
			source_manifest_hash,repository,source_root,actor,task_ref,session_ref,
			agent_run_ref,task_owner_id,task_fence,plan,budget,status,dry_run,policy_id,policy_version,policy_hash)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,
			$15::jsonb,$16::jsonb,$17,$18,$19::jsonb,$20::jsonb,'pending',$21,$22,$23,$24)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING
		RETURNING id`, request.ID, request.WorkspaceID, request.RequestID, request.IdempotencyKey,
		requestHash, request.ChangeApplicationID, request.ProposalID, request.PacketHash,
		request.ExpectedTreeHash, request.SourceManifestHash, request.Repository,
		request.Source.SourceRoot, actorJSON, taskJSON, sessionJSON, agentJSON,
		request.TaskOwnerID, int64(request.TaskFence), planJSON, budgetJSON, request.DryRun, policyID(request.Policy), policyVersion(request.Policy), policyHash(request.Policy)).Scan(&insertedID)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, readErr := readValidationRunByKeyTx(ctx, tx, request.WorkspaceID, request.IdempotencyKey, true)
		if readErr != nil {
			return contracts.ValidationRun{}, false, readErr
		}
		if existing.RequestHash != requestHash || existing.PacketHash != request.PacketHash || existing.ChangeApplicationID != request.ChangeApplicationID {
			return contracts.ValidationRun{}, false, fmt.Errorf("%w: idempotency key", ErrValidationConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ValidationRun{}, false, err
		}
		return existing, false, nil
	}
	if err != nil {
		return contracts.ValidationRun{}, false, fmt.Errorf("insert validation run: %w", err)
	}
	if err := s.fail("validation_run_inserted"); err != nil {
		return contracts.ValidationRun{}, false, err
	}
	event, err := validationEvent("validation.requested", request, map[string]any{
		"validation_run_id": insertedID, "change_application_id": request.ChangeApplicationID,
		"proposal_id": request.ProposalID, "packet_hash": request.PacketHash,
		"request_hash": requestHash,
	})
	if err != nil {
		return contracts.ValidationRun{}, false, err
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ValidationRun{}, false, fmt.Errorf("append validation request: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_transitions(workspace_id,validation_run_id,from_status,to_status,actor,request_id,reason) VALUES($1,$2,'','pending',$3::jsonb,$4,$5)`, request.WorkspaceID, insertedID, actorJSON, request.RequestID, "validation admitted"); err != nil {
		return contracts.ValidationRun{}, false, fmt.Errorf("record validation admission: %w", err)
	}
	run, err := readValidationRunTx(ctx, tx, request.WorkspaceID, insertedID, false)
	if err != nil {
		return contracts.ValidationRun{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ValidationRun{}, false, fmt.Errorf("commit validation start: %w", err)
	}
	return run, true, nil
}

// ValidationCommitInput supplies one complete bounded validation result set.
// All results, evidence, report artifacts, handoff state, event, and current
// run view commit in one transaction.
type ValidationCommitInput struct {
	WorkspaceID  string
	RunID        string
	Actor        contracts.ActorRef
	TaskOwnerID  string
	TaskFence    uint64
	Results      []contracts.ValidationResult
	Report       contracts.ValidationReport
	ObservedTree string
	Discovery    ManifestSummary
}

// ManifestSummary is the hash/count/byte view needed to form a re-index
// handoff without copying repository content into validation history.
type ManifestSummary struct {
	ManifestHash string
	FileCount    int
	TotalBytes   int64
}

// Commit records validator results and completes a run. Repeating the same
// terminal delivery is safe; a changed terminal report is rejected.
func (s *ValidationStore) Commit(ctx context.Context, input ValidationCommitInput) (contracts.ValidationRun, *contracts.ReindexHandoff, bool, error) {
	if s == nil || s.pool == nil || s.events == nil || s.evidence == nil {
		return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ValidationRun{}, nil, false, fmt.Errorf("begin validation commit: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	run, err := readValidationRunTx(ctx, tx, strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.RunID), true)
	if errors.Is(err, ErrValidationNotFound) {
		return contracts.ValidationRun{}, nil, false, err
	}
	if err != nil {
		return contracts.ValidationRun{}, nil, false, err
	}
	if run.Status == contracts.ValidationPassed || run.Status == contracts.ValidationFailed || run.Status == contracts.ValidationAbstained || run.Status == contracts.ValidationCancelled {
		if input.Report.ReportHash != "" && input.Report.ReportHash != run.ReportHash {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("%w: terminal report differs", ErrValidationConflict)
		}
		var handoff *contracts.ReindexHandoff
		if value, getErr := readHandoffByRunTx(ctx, tx, run.WorkspaceID, run.ID, false); getErr == nil {
			handoff = &value
		} else if !errors.Is(getErr, ErrHandoffNotFound) {
			return contracts.ValidationRun{}, nil, false, getErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ValidationRun{}, nil, false, err
		}
		return run, handoff, false, nil
	}
	if run.Task != nil {
		if input.TaskOwnerID == "" || input.TaskFence == 0 || run.TaskOwnerID != input.TaskOwnerID || run.TaskFence != input.TaskFence {
			return contracts.ValidationRun{}, nil, false, ErrValidationStaleFence
		}
		if err := validateChangeTaskFenceTx(ctx, tx, run.WorkspaceID, run.Task.ID, input.TaskOwnerID, input.TaskFence); err != nil {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("%w: %v", ErrValidationStaleFence, err)
		}
	}
	if input.Actor.WorkspaceID == "" {
		input.Actor.WorkspaceID = run.WorkspaceID
	}
	if input.Actor.WorkspaceID != run.WorkspaceID {
		return contracts.ValidationRun{}, nil, false, fmt.Errorf("workspace isolation violation")
	}
	if len(input.Results) != len(run.Plan.Validators) || len(input.Results) > run.Plan.Budget.MaxValidators || len(input.Results) > contracts.MaxValidationChecks {
		return contracts.ValidationRun{}, nil, false, ErrValidationResultCount
	}
	results := append([]contracts.ValidationResult(nil), input.Results...)
	sort.Slice(results, func(i, j int) bool { return results[i].Ordinal < results[j].Ordinal })
	seen := make(map[int]struct{}, len(results))
	for ordinal := range results {
		result := &results[ordinal]
		if result.Ordinal != ordinal || result.WorkspaceID != "" && result.WorkspaceID != run.WorkspaceID || result.RunID != "" && result.RunID != run.ID {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation result identity is inconsistent")
		}
		if _, exists := seen[result.Ordinal]; exists {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("duplicate validation result ordinal %d", result.Ordinal)
		}
		seen[result.Ordinal] = struct{}{}
		result.WorkspaceID, result.RunID = run.WorkspaceID, run.ID
		result.Attempt = 1
		result.ID = fmt.Sprintf("%s-result-%03d", run.ID, result.Ordinal)
		if result.Validator.ID == "" || result.Validator.Version == "" || result.InputHash == "" {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation result %d is missing identity", result.Ordinal)
		}
		if ordinal >= len(run.Plan.Validators) || result.Validator != run.Plan.Validators[ordinal] {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation result %d does not match the durable validator plan", result.Ordinal)
		}
		if result.Status != contracts.ValidationPassed && result.Status != contracts.ValidationFailed && result.Status != contracts.ValidationAbstained && result.Status != contracts.ValidationOutcomeSkipped {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation result %d has invalid status", result.Ordinal)
		}
		if result.Outcome != contracts.ValidationOutcomePassed && result.Outcome != contracts.ValidationOutcomeFailed && result.Outcome != contracts.ValidationOutcomeAbstained && result.Outcome != contracts.ValidationOutcomeSkipped {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation result %d has invalid outcome", result.Ordinal)
		}
		if result.Failure != nil {
			if err := result.Failure.Normalize(); err != nil {
				return contracts.ValidationRun{}, nil, false, err
			}
		}
		if len(result.Summary) > 8192 || result.Files < 0 || result.Bytes < 0 || result.SQLQueries < 0 || result.DurationMS < 0 {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation result %d is outside bounds", result.Ordinal)
		}
		result.Evidence = append([]contracts.ValidationEvidence(nil), result.Evidence...)
		if len(result.Evidence) > contracts.MaxValidationEvidence {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation result %d has too many evidence references", result.Ordinal)
		}
		for evidenceIndex := range result.Evidence {
			evidence := &result.Evidence[evidenceIndex]
			evidence.Kind = strings.TrimSpace(evidence.Kind)
			evidence.SourceReference = strings.TrimSpace(evidence.SourceReference)
			evidence.Hash = strings.ToLower(strings.TrimSpace(evidence.Hash))
			evidence.Role = strings.TrimSpace(evidence.Role)
			if evidence.Kind == "" || evidence.SourceReference == "" || !isEvidenceHash(evidence.Hash) {
				return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation result %d has invalid evidence identity", result.Ordinal)
			}
			if err := validateValidationEvidenceTx(ctx, tx, run, *evidence); err != nil {
				return contracts.ValidationRun{}, nil, false, fmt.Errorf("validation result %d evidence: %w", result.Ordinal, err)
			}
		}
		payload := struct {
			RunID      string `json:"run_id"`
			Ordinal    int    `json:"ordinal"`
			Validator  string `json:"validator"`
			Outcome    string `json:"outcome"`
			PacketHash string `json:"packet_hash"`
		}{run.ID, result.Ordinal, result.Validator.ID + "@" + result.Validator.Version, result.Outcome, run.PacketHash}
		rawEvidence, _ := json.Marshal(payload)
		evidenceResult, evidenceErr := s.evidence.PutTx(ctx, tx, EvidencePutInput{
			WorkspaceID: run.WorkspaceID, SourceReference: fmt.Sprintf("validation-result:%s:%03d", run.ID, result.Ordinal),
			DeduplicationKey: result.ID, Kind: "validation_result", MediaType: "application/json",
			Gist: "post-change validation result", Detail: "bounded validator result", RawPayload: rawEvidence,
			Actor: input.Actor, CausationID: run.ID, CorrelationID: run.RequestID,
		})
		if evidenceErr != nil {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("persist validation evidence: %w", evidenceErr)
		}
		result.Evidence = append(result.Evidence, contracts.ValidationEvidence{Kind: "evidence", SourceReference: strconv.FormatInt(evidenceResult.Record.ID, 10), Hash: evidenceResult.Record.EvidenceHash, Role: "validator_result"})
		result.Policy = contracts.ClonePolicyReference(run.Policy)
		result.ResultHash = result.StableHash()
		failureJSON, _ := jsonOrEmpty(result.Failure)
		evidenceJSON, _ := json.Marshal(result.Evidence)
		createdAt := result.CreatedAt
		if createdAt.IsZero() {
			createdAt = time.Now().UTC()
		}
		result.CreatedAt = createdAt
		_, insertErr := tx.Exec(ctx, `
			INSERT INTO fornix.validation_check_results(
				id,workspace_id,validation_run_id,ordinal,validator_id,validator_version,
				attempt,status,outcome,input_hash,result_hash,summary,failure,evidence,
				output_artifact_id,files,bytes,sql_queries,duration_ms,created_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15,$16,$17,$18,$19,$20)
			ON CONFLICT (workspace_id,id) DO NOTHING`, result.ID, run.WorkspaceID, run.ID,
			result.Ordinal, result.Validator.ID, result.Validator.Version, result.Attempt,
			result.Status, result.Outcome, result.InputHash, result.ResultHash, result.Summary,
			failureJSON, evidenceJSON, artifactID(result.OutputArtifact), result.Files, result.Bytes, result.SQLQueries, result.DurationMS, result.CreatedAt)
		if insertErr != nil {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("persist validation result: %w", insertErr)
		}
		var storedHash string
		if err := tx.QueryRow(ctx, `SELECT result_hash FROM fornix.validation_check_results WHERE workspace_id=$1 AND id=$2`, run.WorkspaceID, result.ID).Scan(&storedHash); err != nil {
			return contracts.ValidationRun{}, nil, false, err
		}
		if storedHash != result.ResultHash {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("%w: result %s", ErrValidationResultConflict, result.ID)
		}
		if result.OutputArtifact != nil {
			if err := validateArtifactRefTx(ctx, tx, run.WorkspaceID, *result.OutputArtifact); err != nil {
				return contracts.ValidationRun{}, nil, false, err
			}
			role := strings.TrimSpace(result.OutputArtifact.Role)
			if role == "" {
				role = "validator-output"
			}
			if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_artifact_links(workspace_id,validation_run_id,result_id,artifact_id,role) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, run.WorkspaceID, run.ID, result.ID, result.OutputArtifact.ArtifactID, role); err != nil {
				return contracts.ValidationRun{}, nil, false, fmt.Errorf("link validation result artifact: %w", err)
			}
		}
	}
	if err := s.fail("validation_results_inserted"); err != nil {
		return contracts.ValidationRun{}, nil, false, err
	}
	passed, failed, abstained := 0, 0, 0
	for _, result := range results {
		switch result.Outcome {
		case contracts.ValidationOutcomePassed:
			passed++
		case contracts.ValidationOutcomeFailed:
			failed++
		case contracts.ValidationOutcomeAbstained:
			abstained++
		}
	}
	status, outcome := contracts.ValidationPassed, contracts.ValidationOutcomePassed
	if failed > 0 {
		status, outcome = contracts.ValidationFailed, contracts.ValidationOutcomeFailed
	} else if abstained > 0 {
		status, outcome = contracts.ValidationAbstained, contracts.ValidationOutcomeAbstained
	}
	report := input.Report
	report.SchemaVersion, report.RunID, report.WorkspaceID = contracts.ValidationSchemaVersion, run.ID, run.WorkspaceID
	report.Status, report.Outcome = status, outcome
	report.PacketHash, report.ExpectedTreeHash = run.PacketHash, run.ExpectedTreeHash
	report.Policy = contracts.ClonePolicyReference(run.Policy)
	report.ObservedTreeHash = strings.ToLower(strings.TrimSpace(input.ObservedTree))
	if report.ObservedTreeHash == "" {
		report.ObservedTreeHash = run.ObservedTreeHash
	}
	report.Results = results
	report.ResultCount, report.PassedCount, report.FailedCount, report.AbstainedCount = len(results), passed, failed, abstained
	report.ReportHash = report.StableHash()
	fullReportJSON, err := json.Marshal(report)
	if err != nil {
		return contracts.ValidationRun{}, nil, false, err
	}
	var reportArtifact *contracts.ArtifactRef
	inlineReport := report
	if len(fullReportJSON) > run.Budget.MaxReportBytes {
		stored, putErr := s.artifacts.PutTx(ctx, tx, ArtifactPutInput{
			WorkspaceID: run.WorkspaceID, Kind: "validation-report", MediaType: "application/json", Raw: fullReportJSON,
			Manifest:   contracts.ArtifactManifest{Gist: "bounded post-change validation report", Metadata: map[string]string{"run_id": run.ID, "report_hash": report.ReportHash}},
			SourceKind: "validation_run", SourceID: run.ID, Role: "report", IdempotencyKey: "validation-report:" + run.ID,
			CausationID: run.ID, CorrelationID: run.RequestID, Actor: input.Actor,
		})
		if putErr != nil {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("store validation report artifact: %w", putErr)
		}
		reportArtifact = &stored.Reference
		inlineReport.Results = nil
	}
	reportJSON, err := json.Marshal(inlineReport)
	if err != nil {
		return contracts.ValidationRun{}, nil, false, err
	}
	replayHash := validationReplayHash(run.RequestHash, report.ReportHash, results)
	actorJSON, _ := json.Marshal(input.Actor)
	if _, err := tx.Exec(ctx, `UPDATE fornix.validation_runs SET observed_tree_hash=$3,status=$4,outcome=$5,result_count=$6,passed_count=$7,failed_count=$8,abstained_count=$9,report=$10::jsonb,report_hash=$11,replay_hash=$12,last_error='',updated_at=clock_timestamp(),started_at=COALESCE(started_at,clock_timestamp()),finished_at=clock_timestamp(),report_artifact_id=$13 WHERE workspace_id=$1 AND id=$2`, run.WorkspaceID, run.ID, report.ObservedTreeHash, status, outcome, len(results), passed, failed, abstained, reportJSON, report.ReportHash, replayHash, artifactID(reportArtifact)); err != nil {
		return contracts.ValidationRun{}, nil, false, fmt.Errorf("complete validation run: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_transitions(workspace_id,validation_run_id,from_status,to_status,outcome,actor,request_id,reason) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8)`, run.WorkspaceID, run.ID, run.Status, status, outcome, actorJSON, run.RequestID, "validation checks completed"); err != nil {
		return contracts.ValidationRun{}, nil, false, err
	}
	var handoff *contracts.ReindexHandoff
	if status == contracts.ValidationPassed && (run.Policy == nil || run.Plan.RequireReindex) {
		value := contracts.ReindexHandoff{SchemaVersion: contracts.ValidationSchemaVersion, ID: "reindex-" + run.ID, WorkspaceID: run.WorkspaceID, RequestID: run.RequestID, IdempotencyKey: "reindex:" + run.ID, RequestHash: run.RequestHash, ValidationRunID: run.ID, ChangeApplicationID: run.ChangeApplicationID, Repository: run.Repository, SourceRoot: run.SourceRoot, PreviousManifestHash: run.SourceManifestHash, ExpectedTreeHash: run.ExpectedTreeHash, ObservedTreeHash: report.ObservedTreeHash, ManifestHash: input.Discovery.ManifestHash, Status: contracts.ReindexHandoffPending, Actor: input.Actor, Task: run.Task, Session: run.Session, TaskOwnerID: run.TaskOwnerID, TaskFence: run.TaskFence, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
		taskJSON, _ := jsonOrEmpty(value.Task)
		sessionJSON, _ := jsonOrEmpty(value.Session)
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.reindex_handoffs(id,workspace_id,request_id,idempotency_key,request_hash,validation_run_id,change_application_id,repository,source_root,previous_manifest_hash,expected_tree_hash,observed_tree_hash,manifest_hash,status,actor,task_ref,session_ref,task_owner_id,task_fence,policy_id,policy_version,policy_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16::jsonb,$17::jsonb,$18,$19,$20,$21,$22) ON CONFLICT (workspace_id,validation_run_id) DO NOTHING`, value.ID, value.WorkspaceID, value.RequestID, value.IdempotencyKey, value.RequestHash, value.ValidationRunID, value.ChangeApplicationID, value.Repository, value.SourceRoot, value.PreviousManifestHash, value.ExpectedTreeHash, value.ObservedTreeHash, value.ManifestHash, value.Status, actorJSON, taskJSON, sessionJSON, value.TaskOwnerID, int64(value.TaskFence), policyID(run.Policy), policyVersion(run.Policy), policyHash(run.Policy)); err != nil {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("create re-index handoff: %w", err)
		}
		loaded, loadErr := readHandoffByRunTx(ctx, tx, run.WorkspaceID, run.ID, false)
		if loadErr != nil {
			return contracts.ValidationRun{}, nil, false, loadErr
		}
		handoff = &loaded
	}
	if s.receipts != nil && status == contracts.ValidationPassed {
		if _, _, err := s.receipts.FinalizeTx(ctx, tx, validationReceiptRequest(run, report, replayHash, reportArtifact, results, input)); err != nil {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("finalize validation work receipt: %w", err)
		}
	}
	if err := s.fail("validation_before_event"); err != nil {
		return contracts.ValidationRun{}, nil, false, err
	}
	payload := map[string]any{"validation_run_id": run.ID, "packet_hash": run.PacketHash, "status": status, "outcome": outcome, "report_hash": report.ReportHash, "replay_hash": replayHash}
	if handoff != nil {
		payload["reindex_handoff_id"] = handoff.ID
	}
	event, err := validationEvent("validation.completed", contracts.ValidationRequest{WorkspaceID: run.WorkspaceID, RequestID: run.RequestID, IdempotencyKey: "validation-completed:" + run.ID, Actor: input.Actor, Policy: run.Policy, CausationID: run.ID, CorrelationID: run.RequestID, Repository: run.Repository}, payload)
	if err != nil {
		return contracts.ValidationRun{}, nil, false, err
	}
	if reportArtifact != nil {
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_artifact_links(workspace_id,validation_run_id,result_id,artifact_id,role) VALUES($1,$2,'',$3,$4) ON CONFLICT DO NOTHING`, run.WorkspaceID, run.ID, reportArtifact.ArtifactID, "report"); err != nil {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("link validation report artifact: %w", err)
		}
		event.Artifacts = []contracts.ArtifactReference{{Ref: strconv.FormatInt(reportArtifact.ArtifactID, 10), Kind: "validation-report", SHA256: reportArtifact.ContentHash, MediaType: reportArtifact.MediaType, SizeBytes: reportArtifact.ByteSize}}
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ValidationRun{}, nil, false, fmt.Errorf("append validation completion: %w", err)
	}
	if s.observability != nil {
		observation := contracts.RunObservation{WorkspaceID: run.WorkspaceID, IdempotencyKey: "validation-observation:" + run.ID, Kind: contracts.ObservationValidation, Component: "validation", Operation: "post_change", Outcome: validationObservationOutcome(status), Actor: input.Actor, Task: run.Task, Session: run.Session, CausationID: run.ID, CorrelationID: run.RequestID, SourceKind: "validation_run", SourceID: run.ID, StartedAt: run.CreatedAt, FinishedAt: time.Now().UTC(), OutputBytes: int64(len(fullReportJSON)), Metadata: map[string]string{"validator_count": strconv.Itoa(len(results)), "report_hash": report.ReportHash}}
		if err := s.observability.recordObservationTx(ctx, tx, observation); err != nil {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("record validation observation: %w", err)
		}
		if err := s.observability.recordCostTx(ctx, tx, contracts.CostLedgerEntry{WorkspaceID: run.WorkspaceID, IdempotencyKey: "validation-cost:" + run.ID, Category: contracts.CostRetrieval, Basis: "validator_run", SourceKind: "validation_run", SourceID: run.ID, Actor: input.Actor, Task: run.Task, Session: run.Session, DurationMS: report.DurationMS, Bytes: int64(len(fullReportJSON)), Measured: true, Metadata: map[string]string{"report_hash": report.ReportHash}}); err != nil {
			return contracts.ValidationRun{}, nil, false, fmt.Errorf("record validation cost: %w", err)
		}
	}
	if err := s.fail("validation_before_commit"); err != nil {
		return contracts.ValidationRun{}, nil, false, err
	}
	stored, err := readValidationRunTx(ctx, tx, run.WorkspaceID, run.ID, false)
	if err != nil {
		return contracts.ValidationRun{}, nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ValidationRun{}, nil, false, fmt.Errorf("commit validation results: %w", err)
	}
	return stored, handoff, true, nil
}

func validationObservationOutcome(status string) string {
	switch status {
	case contracts.ValidationPassed:
		return contracts.OutcomeSucceeded
	case contracts.ValidationAbstained:
		return contracts.OutcomeSkipped
	case contracts.ValidationCancelled:
		return contracts.OutcomeCancelled
	default:
		return contracts.OutcomeFailed
	}
}

// Cancel durably stops a non-terminal validation run. It is idempotent.
func (s *ValidationStore) Cancel(ctx context.Context, workspaceID, runID string, actor contracts.ActorRef) (contracts.ValidationRun, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ValidationRun{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	run, err := readValidationRunTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(runID), true)
	if err != nil {
		return contracts.ValidationRun{}, err
	}
	if run.Status == contracts.ValidationPassed || run.Status == contracts.ValidationFailed || run.Status == contracts.ValidationAbstained || run.Status == contracts.ValidationCancelled {
		if err := tx.Commit(ctx); err != nil {
			return contracts.ValidationRun{}, err
		}
		return run, nil
	}
	if actor.WorkspaceID == "" {
		actor.WorkspaceID = run.WorkspaceID
	}
	if actor.WorkspaceID != run.WorkspaceID {
		return contracts.ValidationRun{}, fmt.Errorf("workspace isolation violation")
	}
	actorJSON, _ := json.Marshal(actor)
	if _, err := tx.Exec(ctx, `UPDATE fornix.validation_runs SET status='cancelled',outcome='skipped',last_error='cancelled by actor',updated_at=clock_timestamp(),finished_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, run.WorkspaceID, run.ID); err != nil {
		return contracts.ValidationRun{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_transitions(workspace_id,validation_run_id,from_status,to_status,outcome,actor,request_id,reason) VALUES($1,$2,$3,'cancelled','skipped',$4::jsonb,$5,'operator cancellation')`, run.WorkspaceID, run.ID, run.Status, actorJSON, run.RequestID); err != nil {
		return contracts.ValidationRun{}, err
	}
	event, err := validationEvent("validation.cancelled", contracts.ValidationRequest{WorkspaceID: run.WorkspaceID, RequestID: run.RequestID, IdempotencyKey: "validation-cancelled:" + run.ID, Actor: actor, Policy: run.Policy, CausationID: run.ID, CorrelationID: run.RequestID, Repository: run.Repository}, map[string]any{"validation_run_id": run.ID})
	if err != nil {
		return contracts.ValidationRun{}, err
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ValidationRun{}, err
	}
	updated, err := readValidationRunTx(ctx, tx, run.WorkspaceID, run.ID, false)
	if err != nil {
		return contracts.ValidationRun{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ValidationRun{}, err
	}
	return updated, nil
}

// Get reads one workspace-scoped validation run.
func (s *ValidationStore) Get(ctx context.Context, workspaceID, runID string) (contracts.ValidationRun, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ValidationRun{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	run, err := readValidationRunTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(runID), false)
	if err != nil {
		return contracts.ValidationRun{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ValidationRun{}, err
	}
	return run, nil
}

// ListResults returns immutable validator results in ordinal order.
func (s *ValidationStore) ListResults(ctx context.Context, workspaceID, runID string, limit, offset int) ([]contracts.ValidationResult, error) {
	workspaceID, runID = strings.TrimSpace(workspaceID), strings.TrimSpace(runID)
	if workspaceID == "" || runID == "" {
		return nil, fmt.Errorf("workspace_id and validation_run_id are required")
	}
	// Result rows intentionally remain compatible with the pre-policy schema:
	// the run is the authoritative policy pin. Reattach that pin while reading
	// results so replay hashes the same semantic values that Commit hashed.
	run, err := s.Get(ctx, workspaceID, runID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > contracts.MaxValidationChecks {
		limit = contracts.MaxValidationChecks
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.pool.Query(ctx, `SELECT id,workspace_id,validation_run_id,ordinal,validator_id,validator_version,attempt,status,outcome,input_hash,result_hash,summary,failure,evidence,files,bytes,sql_queries,duration_ms,created_at FROM fornix.validation_check_results WHERE workspace_id=$1 AND validation_run_id=$2 ORDER BY ordinal,attempt LIMIT $3 OFFSET $4`, workspaceID, runID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Keep allocation capacity independent of the request. SQL still applies
	// the normalized hard limit above, and append grows only for rows returned.
	results := make([]contracts.ValidationResult, 0)
	for rows.Next() {
		result, scanErr := scanValidationResult(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result.Policy = contracts.ClonePolicyReference(run.Policy)
		results = append(results, result)
	}
	return results, rows.Err()
}

// Replay reconstructs a run from durable result rows and validation events.
// It performs no filesystem, model, tool, or ingest work.
func (s *ValidationStore) Replay(ctx context.Context, request contracts.ValidationReplayRequest) (contracts.ValidationReplay, error) {
	if strings.TrimSpace(request.WorkspaceID) == "" || strings.TrimSpace(request.ValidationRunID) == "" {
		return contracts.ValidationReplay{}, fmt.Errorf("workspace_id and validation_run_id are required")
	}
	run, err := s.Get(ctx, request.WorkspaceID, request.ValidationRunID)
	if err != nil {
		return contracts.ValidationReplay{}, err
	}
	results, err := s.ListResults(ctx, request.WorkspaceID, request.ValidationRunID, request.Limit, 0)
	if err != nil {
		return contracts.ValidationReplay{}, err
	}
	limit := request.Limit
	if limit <= 0 {
		limit = 500
	}
	events, err := s.events.ReadAfter(ctx, ReadRequest{WorkspaceID: request.WorkspaceID, AfterSequence: request.FromSequence, RunID: request.ValidationRunID, Limit: limit})
	if err != nil {
		return contracts.ValidationReplay{}, err
	}
	replayHash := validationReplayHash(run.RequestHash, run.ReportHash, results)
	return contracts.ValidationReplay{SchemaVersion: contracts.ValidationSchemaVersion, WorkspaceID: request.WorkspaceID, ValidationRunID: request.ValidationRunID, Run: run, Results: results, Events: events, ReplayHash: replayHash}, nil
}

// Disclose returns a bounded report view without exposing source files or
// arbitrary validator output.
func (s *ValidationStore) Disclose(ctx context.Context, request contracts.ValidationDisclosureRequest) (contracts.ValidationDisclosureResult, error) {
	if err := request.Normalize(); err != nil {
		return contracts.ValidationDisclosureResult{}, err
	}
	run, err := s.Get(ctx, request.WorkspaceID, request.ValidationRunID)
	if err != nil {
		return contracts.ValidationDisclosureResult{}, err
	}
	result := contracts.ValidationDisclosureResult{SchemaVersion: contracts.ValidationSchemaVersion, WorkspaceID: run.WorkspaceID, ValidationRunID: run.ID, Level: request.Level, Status: run.Status, Outcome: run.Outcome, PacketHash: run.PacketHash, ReportHash: run.ReportHash, ReplayHash: run.ReplayHash, ReportArtifact: run.ReportArtifact}
	if request.Level == string(contracts.DisclosureGist) {
		result.Report = nil
	} else {
		if run.Report != nil {
			report := *run.Report
			report.Results = append([]contracts.ValidationResult(nil), run.Report.Results...)
			result.Report = &report
		}
		if result.Report != nil && request.Level == string(contracts.DisclosureRaw) && result.Report.Results == nil {
			loaded, loadErr := s.ListResults(ctx, run.WorkspaceID, run.ID, request.MaxItems, 0)
			if loadErr != nil {
				return contracts.ValidationDisclosureResult{}, loadErr
			}
			result.Report.Results = loaded
		}
	}
	if result.Report != nil && len(result.Report.Results) > request.MaxItems {
		result.Report.Results = result.Report.Results[:request.MaxItems]
		result.Truncated = true
	}
	result.TotalItems = len(disclosureReportResults(result))
	raw, err := marshalValidationDisclosure(&result)
	if err != nil {
		return contracts.ValidationDisclosureResult{}, err
	}
	if len(raw) > request.MaxBytes && result.Report != nil {
		result.Report = nil
		result.Truncated = true
		result.TotalItems = 0
		raw, err = marshalValidationDisclosure(&result)
		if err != nil {
			return contracts.ValidationDisclosureResult{}, err
		}
	}
	if len(raw) > request.MaxBytes {
		return contracts.ValidationDisclosureResult{}, ErrValidationDisclosureBudget
	}
	raw, err = finalizeValidationDisclosure(&result)
	if err != nil {
		return contracts.ValidationDisclosureResult{}, err
	}
	if len(raw) > request.MaxBytes {
		return contracts.ValidationDisclosureResult{}, ErrValidationDisclosureBudget
	}
	return result, nil
}

// finalizeValidationDisclosure reaches the small fixed point caused by
// serializing TotalBytes inside the bounded envelope. The loop is bounded
// because only the decimal width of the byte count can change between passes.
func finalizeValidationDisclosure(result *contracts.ValidationDisclosureResult) ([]byte, error) {
	for attempt := 0; attempt < 8; attempt++ {
		raw, err := marshalValidationDisclosure(result)
		if err != nil {
			return nil, err
		}
		if result.TotalBytes == len(raw) {
			return raw, nil
		}
		result.TotalBytes = len(raw)
	}
	raw, err := marshalValidationDisclosure(result)
	if err != nil {
		return nil, err
	}
	if result.TotalBytes != len(raw) {
		return nil, ErrValidationDisclosureBudget
	}
	return raw, nil
}

// marshalValidationDisclosure computes a stable logical-view hash while
// excluding envelope accounting fields. This avoids self-referential hashes
// and ensures the returned JSON is always valid and within MaxBytes.
func marshalValidationDisclosure(result *contracts.ValidationDisclosureResult) ([]byte, error) {
	canonical := struct {
		SchemaVersion   int                         `json:"schema_version"`
		WorkspaceID     string                      `json:"workspace_id"`
		ValidationRunID string                      `json:"validation_run_id"`
		Level           string                      `json:"level"`
		Status          string                      `json:"status"`
		Outcome         string                      `json:"outcome,omitempty"`
		PacketHash      string                      `json:"packet_hash"`
		ReportHash      string                      `json:"report_hash,omitempty"`
		ReplayHash      string                      `json:"replay_hash,omitempty"`
		Report          *contracts.ValidationReport `json:"report,omitempty"`
		ReportArtifact  *contracts.ArtifactRef      `json:"report_artifact,omitempty"`
		Truncated       bool                        `json:"truncated"`
	}{
		SchemaVersion: result.SchemaVersion, WorkspaceID: result.WorkspaceID,
		ValidationRunID: result.ValidationRunID, Level: result.Level, Status: result.Status,
		Outcome: result.Outcome, PacketHash: result.PacketHash, ReportHash: result.ReportHash,
		ReplayHash: result.ReplayHash, Report: result.Report, ReportArtifact: result.ReportArtifact,
		Truncated: result.Truncated,
	}
	canonicalRaw, err := json.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	result.ContentViewHash = hashBytes(canonicalRaw)
	return json.Marshal(result)
}

// disclosureReportResults is a small helper for bounded disclosure accounting.
func disclosureReportResults(r contracts.ValidationDisclosureResult) []contracts.ValidationResult {
	if r.Report == nil {
		return nil
	}
	return r.Report.Results
}

// GetHandoff reads a durable handoff. MountRoot is intentionally not stored;
// an authenticated caller must resolve it again from workspace configuration.
func (s *ValidationStore) GetHandoff(ctx context.Context, workspaceID, handoffID string) (contracts.ReindexHandoff, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	handoff, err := readHandoffTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(handoffID), false)
	if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ReindexHandoff{}, err
	}
	return handoff, nil
}

// MarkHandoffSubmitted records the idempotent ingest identity created by the
// caller after it performs bounded discovery and submits to IngestStore.
func (s *ValidationStore) MarkHandoffSubmitted(ctx context.Context, handoffID, workspaceID string, job contracts.IngestJob) (contracts.ReindexHandoff, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	handoff, err := readHandoffTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(handoffID), true)
	if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	if handoff.Status == contracts.ReindexHandoffSucceeded || handoff.Status == contracts.ReindexHandoffCancelled {
		return handoff, nil
	}
	if handoff.Status == contracts.ReindexHandoffSubmitted && handoff.IngestJobID != "" && handoff.IngestJobID != job.ID {
		return contracts.ReindexHandoff{}, ErrHandoffConflict
	}
	if job.WorkspaceID != handoff.WorkspaceID || job.Repository != handoff.Repository || job.ManifestHash == "" {
		return contracts.ReindexHandoff{}, fmt.Errorf("%w: ingest job does not match handoff", ErrHandoffConflict)
	}
	if handoff.ManifestHash != "" && job.ManifestHash != handoff.ManifestHash {
		return contracts.ReindexHandoff{}, fmt.Errorf("%w: ingest manifest does not match validated handoff", ErrHandoffConflict)
	}
	if handoff.Status == contracts.ReindexHandoffSubmitted {
		if handoff.Task != nil {
			if err := validateChangeTaskFenceTx(ctx, tx, handoff.WorkspaceID, handoff.Task.ID, handoff.TaskOwnerID, handoff.TaskFence); err != nil {
				return contracts.ReindexHandoff{}, fmt.Errorf("%w: %v", ErrValidationStaleFence, err)
			}
		}
		return handoff, nil
	}
	if handoff.Task != nil {
		if err := validateChangeTaskFenceTx(ctx, tx, handoff.WorkspaceID, handoff.Task.ID, handoff.TaskOwnerID, handoff.TaskFence); err != nil {
			return contracts.ReindexHandoff{}, fmt.Errorf("%w: %v", ErrValidationStaleFence, err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.reindex_handoffs SET ingest_job_id=$3,manifest_hash=$4,status='submitted',updated_at=clock_timestamp(),submitted_at=COALESCE(submitted_at,clock_timestamp()) WHERE workspace_id=$1 AND id=$2`, handoff.WorkspaceID, handoff.ID, job.ID, job.ManifestHash); err != nil {
		return contracts.ReindexHandoff{}, err
	}
	updated, err := readHandoffTx(ctx, tx, handoff.WorkspaceID, handoff.ID, false)
	if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ReindexHandoff{}, err
	}
	return updated, nil
}

// MarkHandoffFailed preserves a redacted failure and makes retries explicit.
func (s *ValidationStore) MarkHandoffFailed(ctx context.Context, workspaceID, handoffID string, failure contracts.ValidationFailure) (contracts.ReindexHandoff, error) {
	if err := failure.Normalize(); err != nil {
		return contracts.ReindexHandoff{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	handoff, err := readHandoffTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(handoffID), true)
	if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	if handoff.Status == contracts.ReindexHandoffSubmitted || handoff.Status == contracts.ReindexHandoffSucceeded || handoff.Status == contracts.ReindexHandoffCancelled || handoff.Status == contracts.ReindexHandoffFailed {
		if err := tx.Commit(ctx); err != nil {
			return contracts.ReindexHandoff{}, err
		}
		return handoff, nil
	}
	if handoff.Task != nil {
		if err := validateChangeTaskFenceTx(ctx, tx, handoff.WorkspaceID, handoff.Task.ID, handoff.TaskOwnerID, handoff.TaskFence); err != nil {
			return contracts.ReindexHandoff{}, fmt.Errorf("%w: %v", ErrValidationStaleFence, err)
		}
	}
	failureJSON, _ := json.Marshal(failure)
	if _, err := tx.Exec(ctx, `UPDATE fornix.reindex_handoffs SET status='failed',failure=$3::jsonb,updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, handoff.WorkspaceID, handoff.ID, failureJSON); err != nil {
		return contracts.ReindexHandoff{}, err
	}
	updated, err := readHandoffTx(ctx, tx, handoff.WorkspaceID, handoff.ID, false)
	if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ReindexHandoff{}, err
	}
	return updated, nil
}

func (s *ValidationStore) validateChangeAuthorityTx(ctx context.Context, tx pgx.Tx, request contracts.ValidationRequest) error {
	var status, proposalID, packetHash, expectedTree, owner string
	var fence int64
	err := tx.QueryRow(ctx, `SELECT status,proposal_id,packet_hash,expected_tree_hash,task_owner_id,task_fence FROM fornix.change_applications WHERE workspace_id=$1 AND id=$2 FOR SHARE`, request.WorkspaceID, request.ChangeApplicationID).Scan(&status, &proposalID, &packetHash, &expectedTree, &owner, &fence)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: change application missing", ErrValidationAuthority)
	}
	if err != nil {
		return err
	}
	if status != contracts.ChangeApplied || proposalID != request.ProposalID || packetHash != request.PacketHash || (expectedTree != "" && expectedTree != request.ExpectedTreeHash) {
		return fmt.Errorf("%w: change application is not the requested applied packet", ErrValidationAuthority)
	}
	proposal, err := readChangeProposalTx(ctx, tx, request.WorkspaceID, proposalID, false)
	if err != nil {
		return err
	}
	if proposal.PacketHash != request.PacketHash || proposal.ExpectedTreeHash != request.ExpectedTreeHash || strings.TrimSpace(proposal.Repository) != strings.TrimSpace(request.Repository) {
		return fmt.Errorf("%w: proposal hashes do not match", ErrValidationAuthority)
	}
	if !samePolicyReference(request.Policy, proposal.Policy) {
		return fmt.Errorf("%w: validation policy does not match proposal", ErrValidationAuthority)
	}
	proposalRoot, requestRoot := filepath.Clean(strings.TrimSpace(proposal.Source.SourceRoot)), filepath.Clean(strings.TrimSpace(request.Source.SourceRoot))
	if !filepath.IsAbs(proposalRoot) || !filepath.IsAbs(requestRoot) || proposalRoot != requestRoot {
		return fmt.Errorf("%w: proposal source root does not match request", ErrValidationAuthority)
	}
	if proposal.Source.ManifestHash != "" && proposal.Source.ManifestHash != request.SourceManifestHash {
		return fmt.Errorf("%w: proposal source manifest does not match request", ErrValidationAuthority)
	}
	if proposal.Task != nil {
		if request.TaskOwnerID == "" || request.TaskFence == 0 || owner != request.TaskOwnerID || fence <= 0 || uint64(fence) != request.TaskFence {
			return ErrValidationStaleFence
		}
		if err := validateChangeTaskFenceTx(ctx, tx, request.WorkspaceID, proposal.Task.ID, request.TaskOwnerID, request.TaskFence); err != nil {
			return ErrValidationStaleFence
		}
	}
	return nil
}

// validateValidationEvidenceTx accepts only source references that can be
// resolved to the same workspace and the same authoritative hashes used by
// this validation run. Unknown reference kinds fail closed instead of being
// treated as evidence merely because a caller supplied a hash.
func validateValidationEvidenceTx(ctx context.Context, tx pgx.Tx, run contracts.ValidationRun, evidence contracts.ValidationEvidence) error {
	var storedHash string
	switch evidence.Kind {
	case "change_application":
		if evidence.SourceReference != run.ChangeApplicationID {
			return ErrValidationEvidence
		}
		var status string
		if err := tx.QueryRow(ctx, `SELECT status,packet_hash FROM fornix.change_applications WHERE workspace_id=$1 AND id=$2`, run.WorkspaceID, evidence.SourceReference).Scan(&status, &storedHash); err != nil {
			return fmt.Errorf("%w: change application is missing", ErrValidationEvidence)
		}
		if status != contracts.ChangeApplied || !strings.EqualFold(storedHash, evidence.Hash) {
			return ErrValidationEvidence
		}
	case "change_proposal":
		if evidence.SourceReference != run.ProposalID {
			return ErrValidationEvidence
		}
		if err := tx.QueryRow(ctx, `SELECT packet_hash FROM fornix.change_proposals WHERE workspace_id=$1 AND id=$2`, run.WorkspaceID, evidence.SourceReference).Scan(&storedHash); err != nil {
			return fmt.Errorf("%w: change proposal is missing", ErrValidationEvidence)
		}
		if !strings.EqualFold(storedHash, evidence.Hash) {
			return ErrValidationEvidence
		}
	case "evidence":
		evidenceID, err := strconv.ParseInt(evidence.SourceReference, 10, 64)
		if err != nil || evidenceID <= 0 {
			return ErrValidationEvidence
		}
		if err := tx.QueryRow(ctx, `SELECT evidence_hash FROM fornix.evidence_records WHERE workspace_id=$1 AND id=$2`, run.WorkspaceID, evidenceID).Scan(&storedHash); err != nil {
			return fmt.Errorf("%w: evidence record is missing", ErrValidationEvidence)
		}
		if !strings.EqualFold(storedHash, evidence.Hash) {
			return ErrValidationEvidence
		}
	case "validation_result":
		if err := tx.QueryRow(ctx, `SELECT result_hash FROM fornix.validation_check_results WHERE workspace_id=$1 AND id=$2`, run.WorkspaceID, evidence.SourceReference).Scan(&storedHash); err != nil {
			return fmt.Errorf("%w: validation result is missing", ErrValidationEvidence)
		}
		if !strings.EqualFold(storedHash, evidence.Hash) {
			return ErrValidationEvidence
		}
	default:
		return ErrValidationEvidence
	}
	return nil
}

func validationEvent(eventType string, request contracts.ValidationRequest, payload map[string]any) (contracts.EventEnvelope, error) {
	if payload == nil {
		payload = make(map[string]any)
	}
	if request.Policy != nil {
		payload["policy_id"], payload["policy_version"], payload["policy_hash"] = request.Policy.PolicyID, request.Policy.Version, request.Policy.PolicyHash
	}
	if runID, ok := payload["validation_run_id"].(string); ok && strings.TrimSpace(runID) != "" {
		// EventStore's bounded RunID filter uses this generic field so replay
		// can read both request and completion events without event-specific SQL.
		payload["run_id"] = strings.TrimSpace(runID)
	}
	event, err := contracts.NewEvent(eventType, payload)
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	event.Scope = contracts.Scope{WorkspaceID: request.WorkspaceID, Subject: request.Repository}
	event.Actor, event.Task, event.Session = request.Actor, request.Task, request.Session
	event.CausationID, event.CorrelationID, event.IdempotencyKey = request.CausationID, request.CorrelationID, request.IdempotencyKey
	event.Policy = contracts.ClonePolicyReference(request.Policy)
	event.Provenance = contracts.Provenance{SourcePaths: []string{"post-change-validation"}}
	return event, nil
}

func validateArtifactRefTx(ctx context.Context, tx pgx.Tx, workspaceID string, ref contracts.ArtifactRef) error {
	var refWorkspace string
	var artifactID int64
	if err := tx.QueryRow(ctx, `SELECT workspace_id,artifact_id FROM fornix.artifact_refs WHERE workspace_id=$1 AND id=$2`, workspaceID, ref.ID).Scan(&refWorkspace, &artifactID); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("validation artifact reference %d is missing", ref.ID)
	} else if err != nil {
		return fmt.Errorf("validate validation artifact reference: %w", err)
	}
	if refWorkspace != workspaceID || artifactID != ref.ArtifactID {
		return fmt.Errorf("validation artifact reference %d crosses workspace or artifact identity", ref.ID)
	}
	var contentHash string
	if err := tx.QueryRow(ctx, `SELECT content_hash FROM fornix.artifacts WHERE workspace_id=$1 AND id=$2`, workspaceID, artifactID).Scan(&contentHash); err != nil {
		return fmt.Errorf("validate validation artifact %d: %w", artifactID, err)
	}
	if strings.ToLower(strings.TrimSpace(contentHash)) != strings.ToLower(strings.TrimSpace(ref.ContentHash)) {
		return fmt.Errorf("validation artifact %d hash mismatch", artifactID)
	}
	return nil
}

func validationReceiptRequest(run contracts.ValidationRun, report contracts.ValidationReport, replayHash string, reportArtifact *contracts.ArtifactRef, results []contracts.ValidationResult, input ValidationCommitInput) contracts.WorkReceiptFinalizeRequest {
	receipt := contracts.WorkReceiptFinalizeRequest{
		ReceiptID: "validation-receipt-" + run.ID, RequestID: "validation-receipt-request-" + run.ID,
		IdempotencyKey: "validation-receipt:" + run.WorkspaceID + ":" + run.ID,
		WorkspaceID:    run.WorkspaceID, Actor: input.Actor, WorkKind: contracts.WorkReceiptReferenceValidation,
		WorkID: run.ID, Task: run.Task, Session: run.Session, TaskOwnerID: run.TaskOwnerID, TaskFence: run.TaskFence,
		Policy:             run.Policy,
		SourceManifestHash: run.SourceManifestHash, ReplayHash: replayHash,
		Steps: []contracts.WorkReceiptStep{{Ordinal: 0, ID: "post-change-validation", Name: "verified post-change validation", Kind: "validation", Status: "succeeded", SourceKind: contracts.WorkReceiptReferenceValidation, SourceID: run.ID, SourceHash: report.ReportHash, InputHash: run.RequestHash, OutputHash: replayHash, ReferenceRoles: []string{"validation", "change_application", "replay"}, DurationMS: report.DurationMS}},
		References: []contracts.WorkReceiptReference{
			{WorkspaceID: run.WorkspaceID, Kind: contracts.WorkReceiptReferenceValidation, SourceID: run.ID, Role: "validation", Hash: report.ReportHash},
			{WorkspaceID: run.WorkspaceID, Kind: contracts.WorkReceiptReferenceChangeApplication, SourceID: run.ChangeApplicationID, Role: "applied-change", Hash: run.PacketHash},
			{WorkspaceID: run.WorkspaceID, Kind: contracts.WorkReceiptReferenceChangeProposal, SourceID: run.ProposalID, Role: "change-packet", Hash: run.PacketHash},
			{WorkspaceID: run.WorkspaceID, Kind: contracts.WorkReceiptReferenceReplay, SourceID: run.ID, Role: "replay", Hash: replayHash},
		},
	}
	seenEvidence := make(map[int64]struct{})
	for _, result := range results {
		for _, evidence := range result.Evidence {
			if evidence.Kind != "evidence" {
				continue
			}
			id, err := strconv.ParseInt(strings.TrimSpace(evidence.SourceReference), 10, 64)
			if err != nil || id <= 0 {
				continue
			}
			if _, exists := seenEvidence[id]; exists {
				continue
			}
			seenEvidence[id] = struct{}{}
			receipt.Evidence = append(receipt.Evidence, contracts.WorkReceiptEvidence{ID: id, WorkspaceID: run.WorkspaceID, EvidenceHash: evidence.Hash, SourceReference: evidence.SourceReference, Role: evidence.Role})
		}
	}
	if reportArtifact != nil {
		receipt.Artifacts = append(receipt.Artifacts, contracts.WorkReceiptArtifact{ID: reportArtifact.ID, ArtifactID: reportArtifact.ArtifactID, WorkspaceID: reportArtifact.WorkspaceID, ContentHash: reportArtifact.ContentHash, SourceKind: reportArtifact.SourceKind, SourceID: reportArtifact.SourceID, Role: reportArtifact.Role})
		receipt.References = append(receipt.References, contracts.WorkReceiptReference{WorkspaceID: run.WorkspaceID, Kind: contracts.WorkReceiptReferenceArtifact, SourceID: strconv.FormatInt(reportArtifact.ArtifactID, 10), Role: "report", Hash: reportArtifact.ContentHash})
	}
	return receipt
}

func validationReplayHash(requestHash, reportHash string, results []contracts.ValidationResult) string {
	ordered := append([]contracts.ValidationResult(nil), results...)
	for i := range ordered {
		// Database timestamps are truncated to microseconds and IDs are delivery
		// identities. Neither belongs in a replay identity; the immutable result
		// hash already covers the semantic validator output.
		ordered[i].ID = ""
		ordered[i].CreatedAt = time.Time{}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Ordinal < ordered[j].Ordinal })
	value := struct {
		RequestHash string                       `json:"request_hash"`
		ReportHash  string                       `json:"report_hash"`
		Results     []contracts.ValidationResult `json:"results"`
	}{requestHash, reportHash, ordered}
	raw, _ := json.Marshal(value)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func readValidationRunByKeyTx(ctx context.Context, tx pgx.Tx, workspaceID, key string, lock bool) (contracts.ValidationRun, error) {
	query := `SELECT id FROM fornix.validation_runs WHERE workspace_id=$1 AND idempotency_key=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	var id string
	if err := tx.QueryRow(ctx, query, workspaceID, key).Scan(&id); errors.Is(err, pgx.ErrNoRows) {
		return contracts.ValidationRun{}, ErrValidationNotFound
	} else if err != nil {
		return contracts.ValidationRun{}, err
	}
	return readValidationRunTx(ctx, tx, workspaceID, id, false)
}

func readValidationRunTx(ctx context.Context, tx pgx.Tx, workspaceID, runID string, lock bool) (contracts.ValidationRun, error) {
	query := `SELECT id,workspace_id,request_id,idempotency_key,request_hash,change_application_id,proposal_id,packet_hash,expected_tree_hash,observed_tree_hash,source_manifest_hash,repository,source_root,actor,task_ref,session_ref,agent_run_ref,task_owner_id,task_fence,plan,budget,status,outcome,dry_run,result_count,passed_count,failed_count,abstained_count,report,report_hash,report_artifact_id,replay_hash,last_error,policy_id,policy_version,policy_hash,created_at,updated_at,started_at,finished_at FROM fornix.validation_runs WHERE workspace_id=$1 AND id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	var run contracts.ValidationRun
	var actor, task, session, agent, planJSON, budgetJSON, reportJSON []byte
	var fence int64
	var reportArtifactID *int64
	var policyID, policyVersion, policyHash *string
	err := tx.QueryRow(ctx, query, workspaceID, runID).Scan(&run.ID, &run.WorkspaceID, &run.RequestID, &run.IdempotencyKey, &run.RequestHash, &run.ChangeApplicationID, &run.ProposalID, &run.PacketHash, &run.ExpectedTreeHash, &run.ObservedTreeHash, &run.SourceManifestHash, &run.Repository, &run.SourceRoot, &actor, &task, &session, &agent, &run.TaskOwnerID, &fence, &planJSON, &budgetJSON, &run.Status, &run.Outcome, &run.DryRun, &run.ResultCount, &run.PassedCount, &run.FailedCount, &run.AbstainedCount, &reportJSON, &run.ReportHash, &reportArtifactID, &run.ReplayHash, &run.LastError, &policyID, &policyVersion, &policyHash, &run.CreatedAt, &run.UpdatedAt, &run.StartedAt, &run.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ValidationRun{}, ErrValidationNotFound
	}
	if err != nil {
		return contracts.ValidationRun{}, err
	}
	if fence < 0 {
		return contracts.ValidationRun{}, ErrValidationConflict
	}
	run.SchemaVersion = contracts.ValidationSchemaVersion
	run.TaskFence = uint64(fence)
	run.Policy = policyReference(policyID, policyVersion, policyHash, run.WorkspaceID)
	if err := json.Unmarshal(actor, &run.Actor); err != nil {
		return contracts.ValidationRun{}, err
	}
	run.Task, _ = decodeEntityRef(task)
	run.Session, _ = decodeEntityRef(session)
	run.AgentRun, _ = decodeEntityRef(agent)
	if err := json.Unmarshal(planJSON, &run.Plan); err != nil {
		return contracts.ValidationRun{}, err
	}
	if run.Policy == nil && run.Plan.Policy != nil {
		run.Policy = contracts.ClonePolicyReference(run.Plan.Policy)
	}
	if err := json.Unmarshal(budgetJSON, &run.Budget); err != nil {
		return contracts.ValidationRun{}, err
	}
	if len(reportJSON) > 0 && string(reportJSON) != "{}" {
		var report contracts.ValidationReport
		if err := json.Unmarshal(reportJSON, &report); err != nil {
			return contracts.ValidationRun{}, err
		}
		run.Report = &report
	}
	if reportArtifactID != nil {
		ref, err := readArtifactRefByIdentityTx(ctx, tx, run.WorkspaceID, "validation_run", run.ID, "report")
		if err != nil {
			return contracts.ValidationRun{}, err
		}
		run.ReportArtifact = &ref
	}
	return run, nil
}

func scanValidationResult(row interface{ Scan(...any) error }) (contracts.ValidationResult, error) {
	var result contracts.ValidationResult
	var failure, evidence []byte
	var attempt int
	if err := row.Scan(&result.ID, &result.WorkspaceID, &result.RunID, &result.Ordinal, &result.Validator.ID, &result.Validator.Version, &attempt, &result.Status, &result.Outcome, &result.InputHash, &result.ResultHash, &result.Summary, &failure, &evidence, &result.Files, &result.Bytes, &result.SQLQueries, &result.DurationMS, &result.CreatedAt); err != nil {
		return contracts.ValidationResult{}, err
	}
	result.SchemaVersion, result.Attempt = contracts.ValidationSchemaVersion, attempt
	if len(failure) > 0 && string(failure) != "{}" && string(failure) != "null" {
		result.Failure = &contracts.ValidationFailure{}
		if err := json.Unmarshal(failure, result.Failure); err != nil {
			return contracts.ValidationResult{}, err
		}
	}
	if len(evidence) > 0 {
		if err := json.Unmarshal(evidence, &result.Evidence); err != nil {
			return contracts.ValidationResult{}, err
		}
	}
	return result, nil
}

func readHandoffByRunTx(ctx context.Context, tx pgx.Tx, workspaceID, runID string, lock bool) (contracts.ReindexHandoff, error) {
	query := `SELECT id FROM fornix.reindex_handoffs WHERE workspace_id=$1 AND validation_run_id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	var id string
	if err := tx.QueryRow(ctx, query, workspaceID, runID).Scan(&id); errors.Is(err, pgx.ErrNoRows) {
		return contracts.ReindexHandoff{}, ErrHandoffNotFound
	} else if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	return readHandoffTx(ctx, tx, workspaceID, id, false)
}

func readHandoffTx(ctx context.Context, tx pgx.Tx, workspaceID, id string, lock bool) (contracts.ReindexHandoff, error) {
	query := `SELECT id,workspace_id,request_id,idempotency_key,request_hash,validation_run_id,change_application_id,repository,source_root,previous_manifest_hash,expected_tree_hash,observed_tree_hash,manifest_hash,ingest_job_id,status,actor,task_ref,session_ref,task_owner_id,task_fence,failure,policy_id,policy_version,policy_hash,created_at,updated_at,submitted_at,completed_at FROM fornix.reindex_handoffs WHERE workspace_id=$1 AND id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	var handoff contracts.ReindexHandoff
	var actor, task, session, failure []byte
	var policyID, policyVersion, policyHash *string
	var fence int64
	err := tx.QueryRow(ctx, query, workspaceID, id).Scan(&handoff.ID, &handoff.WorkspaceID, &handoff.RequestID, &handoff.IdempotencyKey, &handoff.RequestHash, &handoff.ValidationRunID, &handoff.ChangeApplicationID, &handoff.Repository, &handoff.SourceRoot, &handoff.PreviousManifestHash, &handoff.ExpectedTreeHash, &handoff.ObservedTreeHash, &handoff.ManifestHash, &handoff.IngestJobID, &handoff.Status, &actor, &task, &session, &handoff.TaskOwnerID, &fence, &failure, &policyID, &policyVersion, &policyHash, &handoff.CreatedAt, &handoff.UpdatedAt, &handoff.SubmittedAt, &handoff.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ReindexHandoff{}, ErrHandoffNotFound
	}
	if err != nil {
		return contracts.ReindexHandoff{}, err
	}
	handoff.SchemaVersion, handoff.TaskFence = contracts.ValidationSchemaVersion, uint64(fence)
	handoff.Policy = policyReference(policyID, policyVersion, policyHash, handoff.WorkspaceID)
	if err := json.Unmarshal(actor, &handoff.Actor); err != nil {
		return contracts.ReindexHandoff{}, err
	}
	handoff.Task, _ = decodeEntityRef(task)
	handoff.Session, _ = decodeEntityRef(session)
	if len(failure) > 0 && string(failure) != "{}" {
		handoff.Failure = &contracts.ValidationFailure{}
		if err := json.Unmarshal(failure, handoff.Failure); err != nil {
			return contracts.ReindexHandoff{}, err
		}
	}
	return handoff, nil
}

func artifactID(ref *contracts.ArtifactRef) any {
	if ref == nil {
		return nil
	}
	return ref.ArtifactID
}
