package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrSandboxCleanupMissing  = errors.New("sandbox cleanup job not found")
	ErrSandboxCleanupFence    = errors.New("sandbox cleanup lease is stale or expired")
	ErrSandboxCleanupConflict = errors.New("sandbox cleanup intent conflicts with existing authority")
)

const (
	maxSandboxCleanupBatch   = 100
	minSandboxCleanupLease   = time.Second
	maxSandboxCleanupLease   = 10 * time.Minute
	maxSandboxCleanupBackoff = 15 * time.Minute
)

// SandboxCleanupStore owns durable, workspace-scoped cleanup intent and
// fenced retry state. It does not call a runtime; only a separately trusted
// runner may remove an object after checking this authority.
type SandboxCleanupStore struct {
	pool *pgxpool.Pool
}

// NewSandboxCleanupStore constructs the PostgreSQL cleanup authority.
func NewSandboxCleanupStore(pool *pgxpool.Pool) *SandboxCleanupStore {
	return &SandboxCleanupStore{pool: pool}
}

// EnqueueTx creates one immutable cleanup intent inside the caller's
// authoritative result transaction. Duplicate exact intents are idempotent;
// conflicting identities for the same attempt fail closed.
func (s *SandboxCleanupStore) EnqueueTx(ctx context.Context, tx pgx.Tx, intent contracts.SandboxCleanupIntent) (contracts.SandboxCleanupJob, bool, error) {
	if s == nil || tx == nil {
		return contracts.SandboxCleanupJob{}, false, fmt.Errorf("sandbox cleanup transaction is not configured")
	}
	if err := intent.Normalize(); err != nil {
		return contracts.SandboxCleanupJob{}, false, err
	}
	if err := setWorkspaceContext(ctx, tx, intent.WorkspaceID); err != nil {
		return contracts.SandboxCleanupJob{}, false, err
	}
	identityJSON, err := json.Marshal(intent.Identity)
	if err != nil {
		return contracts.SandboxCleanupJob{}, false, fmt.Errorf("marshal sandbox cleanup identity: %w", err)
	}
	actorJSON, err := json.Marshal(intent.Actor)
	if err != nil {
		return contracts.SandboxCleanupJob{}, false, fmt.Errorf("marshal sandbox cleanup actor: %w", err)
	}
	identityHash := intent.Identity.StableHash()
	intentHash := intent.StableHash()
	if identityHash == "" || intentHash == "" {
		return contracts.SandboxCleanupJob{}, false, fmt.Errorf("sandbox cleanup hashes are invalid")
	}
	id := contracts.NewID("sbcleanup")
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.sandbox_cleanup_jobs(
		 workspace_id,id,tool_run_id,tool_attempt,domain_link_id,domain_kind,domain_id,backend,execution_identity,
		 execution_identity_hash,intent_hash,tool_request_hash,result_hash,actor
		) VALUES($1,$2,$3,$4,$5,'tool_run',$3,$6,$7::jsonb,$8,$9,$10,$11,$12::jsonb)
		ON CONFLICT (workspace_id,tool_run_id,tool_attempt) DO NOTHING
		RETURNING id`,
		intent.WorkspaceID, id, intent.ToolRunID, intent.ToolAttempt, intent.DomainLinkID,
		string(intent.Identity.Backend), identityJSON, identityHash, intentHash,
		intent.Identity.ToolRequestHash, intent.ResultHash, actorJSON).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var existingID string
		if err := tx.QueryRow(ctx, `SELECT id FROM fornix.sandbox_cleanup_jobs WHERE workspace_id=$1 AND tool_run_id=$2 AND tool_attempt=$3`, intent.WorkspaceID, intent.ToolRunID, intent.ToolAttempt).Scan(&existingID); err != nil {
			return contracts.SandboxCleanupJob{}, false, fmt.Errorf("read duplicate sandbox cleanup intent: %w", err)
		}
		job, err := readSandboxCleanupJobTx(ctx, tx, intent.WorkspaceID, existingID, false)
		if err != nil {
			return contracts.SandboxCleanupJob{}, false, err
		}
		if job.IntentHash != intentHash || job.Intent.Identity.StableHash() != identityHash || job.Intent.Actor != intent.Actor {
			return contracts.SandboxCleanupJob{}, false, ErrSandboxCleanupConflict
		}
		return job, true, nil
	}
	if err != nil {
		return contracts.SandboxCleanupJob{}, false, fmt.Errorf("insert sandbox cleanup intent: %w", err)
	}
	job, err := readSandboxCleanupJobTx(ctx, tx, intent.WorkspaceID, id, false)
	if err != nil {
		return contracts.SandboxCleanupJob{}, false, err
	}
	if err := appendSandboxCleanupEventTx(ctx, tx, job, "created", "", intent.Actor, "", ""); err != nil {
		return contracts.SandboxCleanupJob{}, false, err
	}
	return job, false, nil
}

// Claim takes a bounded deterministic batch of due cleanup work. PostgreSQL
// time, row locks, and monotonically increasing fences decide ownership.
func (s *SandboxCleanupStore) Claim(ctx context.Context, workspaceID, ownerID string, limit int, leaseDuration time.Duration) ([]contracts.SandboxCleanupJob, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox cleanup store is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	ownerID = strings.TrimSpace(ownerID)
	if workspaceID == "" || ownerID == "" || len(ownerID) > contracts.MaxDomainIDLength || limit < 1 || limit > maxSandboxCleanupBatch || leaseDuration < minSandboxCleanupLease || leaseDuration > maxSandboxCleanupLease {
		return nil, fmt.Errorf("sandbox cleanup claim parameters are invalid")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := deadLetterExhaustedSandboxCleanupTx(ctx, tx, workspaceID, ownerID, maxSandboxCleanupBatch); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT id FROM fornix.sandbox_cleanup_jobs
		WHERE workspace_id=$1 AND fence < 9223372036854775807 AND attempt_count < $3 AND (
		  (status IN ('pending','retry_wait') AND retry_at <= clock_timestamp()) OR
		  (status='leased' AND lease_until <= clock_timestamp())
		)
		ORDER BY retry_at,created_at,id
		FOR UPDATE SKIP LOCKED LIMIT $2`, workspaceID, limit, contracts.MaxSandboxCleanupAttempts)
	if err != nil {
		return nil, fmt.Errorf("select due sandbox cleanup jobs: %w", err)
	}
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	claimed := make([]contracts.SandboxCleanupJob, 0, len(ids))
	actor := contracts.ActorRef{ID: ownerID, Kind: "service", WorkspaceID: workspaceID}
	for _, id := range ids {
		var fence int64
		if err := tx.QueryRow(ctx, `
			UPDATE fornix.sandbox_cleanup_jobs
			SET status='leased',owner_id=$3,fence=fence+1,attempt_count=attempt_count+1,
			    lease_until=clock_timestamp()+($4 * interval '1 microsecond'),
			    version=version+1,updated_at=clock_timestamp(),failure_code=''
			WHERE workspace_id=$1 AND id=$2
		RETURNING fence`, workspaceID, id, ownerID, leaseDuration.Microseconds()).Scan(&fence); err != nil {
			return nil, fmt.Errorf("claim sandbox cleanup job: %w", err)
		}
		job, err := readSandboxCleanupJobTx(ctx, tx, workspaceID, id, false)
		if err != nil {
			return nil, err
		}
		if err := appendSandboxCleanupEventTx(ctx, tx, job, "claimed", ownerID, actor, "", ""); err != nil {
			return nil, err
		}
		claimed = append(claimed, job)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit sandbox cleanup claims: %w", err)
	}
	return claimed, nil
}

func deadLetterExhaustedSandboxCleanupTx(ctx context.Context, tx pgx.Tx, workspaceID, actorID string, limit int) error {
	rows, err := tx.Query(ctx, `
		SELECT id FROM fornix.sandbox_cleanup_jobs
		WHERE workspace_id=$1 AND attempt_count >= $2 AND (
		  (status IN ('pending','retry_wait') AND retry_at <= clock_timestamp()) OR
		  (status='leased' AND lease_until <= clock_timestamp())
		)
		ORDER BY retry_at,created_at,id
		FOR UPDATE SKIP LOCKED LIMIT $3`, workspaceID, contracts.MaxSandboxCleanupAttempts, limit)
	if err != nil {
		return fmt.Errorf("select exhausted sandbox cleanup jobs: %w", err)
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	actor := contracts.ActorRef{ID: actorID, Kind: "service", WorkspaceID: workspaceID}
	for _, id := range ids {
		if _, err := tx.Exec(ctx, `
			UPDATE fornix.sandbox_cleanup_jobs
			SET status='dead_letter',owner_id='',lease_until=NULL,failure_code='attempt_budget_exhausted',
			    updated_at=clock_timestamp(),version=version+1
			WHERE workspace_id=$1 AND id=$2`, workspaceID, id); err != nil {
			return fmt.Errorf("dead-letter exhausted sandbox cleanup job: %w", err)
		}
		job, err := readSandboxCleanupJobTx(ctx, tx, workspaceID, id, false)
		if err != nil {
			return err
		}
		if err := appendSandboxCleanupEventTx(ctx, tx, job, "dead_letter", actorID, actor, "attempt_budget_exhausted", ""); err != nil {
			return err
		}
	}
	return nil
}

// Renew extends an active worker lease only when owner and fence still match.
func (s *SandboxCleanupStore) Renew(ctx context.Context, lease contracts.SandboxCleanupLease, duration time.Duration) (contracts.SandboxCleanupLease, error) {
	if s == nil || s.pool == nil || !validSandboxCleanupLease(lease) || duration < minSandboxCleanupLease || duration > maxSandboxCleanupLease {
		return contracts.SandboxCleanupLease{}, fmt.Errorf("sandbox cleanup lease renewal is invalid")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, lease.WorkspaceID)
	if err != nil {
		return contracts.SandboxCleanupLease{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var version int64
	if err := tx.QueryRow(ctx, `
		UPDATE fornix.sandbox_cleanup_jobs
		SET lease_until=clock_timestamp()+($5 * interval '1 microsecond'),version=version+1,updated_at=clock_timestamp()
		WHERE workspace_id=$1 AND id=$2 AND status='leased' AND owner_id=$3 AND fence=$4 AND lease_until > clock_timestamp()
		RETURNING version`, lease.WorkspaceID, lease.JobID, lease.OwnerID, int64(lease.Fence), duration.Microseconds()).Scan(&version); errors.Is(err, pgx.ErrNoRows) {
		return contracts.SandboxCleanupLease{}, ErrSandboxCleanupFence
	} else if err != nil {
		return contracts.SandboxCleanupLease{}, fmt.Errorf("renew sandbox cleanup lease: %w", err)
	}
	job, err := readSandboxCleanupJobTx(ctx, tx, lease.WorkspaceID, lease.JobID, false)
	if err != nil {
		return contracts.SandboxCleanupLease{}, err
	}
	if err := appendSandboxCleanupEventTx(ctx, tx, job, "renewed", lease.OwnerID, contracts.ActorRef{ID: lease.OwnerID, Kind: "service", WorkspaceID: lease.WorkspaceID}, "", ""); err != nil {
		return contracts.SandboxCleanupLease{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.SandboxCleanupLease{}, err
	}
	return contracts.SandboxCleanupLease{WorkspaceID: lease.WorkspaceID, JobID: lease.JobID, OwnerID: lease.OwnerID, Fence: lease.Fence, LeaseUntil: *job.LeaseUntil}, nil
}

// Complete marks cleanup complete only for an exact identity-bound successful
// runner observation. Repeating the same committed observation is idempotent.
func (s *SandboxCleanupStore) Complete(ctx context.Context, lease contracts.SandboxCleanupLease, observation contracts.SandboxCleanupObservation) (contracts.SandboxCleanupJob, bool, error) {
	if s == nil || s.pool == nil || !validSandboxCleanupLease(lease) {
		return contracts.SandboxCleanupJob{}, false, fmt.Errorf("sandbox cleanup store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, lease.WorkspaceID)
	if err != nil {
		return contracts.SandboxCleanupJob{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	job, err := readSandboxCleanupJobTx(ctx, tx, lease.WorkspaceID, lease.JobID, true)
	if err != nil {
		return contracts.SandboxCleanupJob{}, false, err
	}
	if job.Status == contracts.SandboxCleanupCompleted {
		if job.Fence != lease.Fence || job.CompletionOwnerID != lease.OwnerID {
			return contracts.SandboxCleanupJob{}, false, ErrSandboxCleanupFence
		}
		validationJob := job
		validationJob.Status = contracts.SandboxCleanupLeased
		if validateErr := observation.Normalize(validationJob, lease); validateErr == nil && job.CompletionState == observation.State {
			return job, true, nil
		}
		return contracts.SandboxCleanupJob{}, false, ErrSandboxCleanupConflict
	}
	if err := observation.Normalize(job, lease); err != nil || observation.State != contracts.SandboxCleanupRemoved && observation.State != contracts.SandboxCleanupAlreadyAbsent {
		return contracts.SandboxCleanupJob{}, false, ErrSandboxCleanupConflict
	}
	var version int64
	err = tx.QueryRow(ctx, `
		UPDATE fornix.sandbox_cleanup_jobs
		SET status='completed',owner_id='',lease_until=NULL,completion_state=$5,completion_owner_id=$6,
		    completed_at=clock_timestamp(),updated_at=clock_timestamp(),version=version+1
		WHERE workspace_id=$1 AND id=$2 AND status='leased' AND owner_id=$3 AND fence=$4 AND lease_until > clock_timestamp()
		RETURNING version`, lease.WorkspaceID, lease.JobID, lease.OwnerID, int64(lease.Fence), observation.State, lease.OwnerID).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.SandboxCleanupJob{}, false, ErrSandboxCleanupFence
	}
	if err != nil {
		return contracts.SandboxCleanupJob{}, false, fmt.Errorf("complete sandbox cleanup job: %w", err)
	}
	job, err = readSandboxCleanupJobTx(ctx, tx, lease.WorkspaceID, lease.JobID, false)
	if err != nil {
		return contracts.SandboxCleanupJob{}, false, err
	}
	if err := appendSandboxCleanupEventTx(ctx, tx, job, "completed", lease.OwnerID, contracts.ActorRef{ID: lease.OwnerID, Kind: "service", WorkspaceID: lease.WorkspaceID}, "", observation.State); err != nil {
		return contracts.SandboxCleanupJob{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.SandboxCleanupJob{}, false, err
	}
	return job, false, nil
}

// Retry schedules bounded exponential backoff or dead-letters the job after
// the fixed maximum number of attempts. Only the active owner/fence can retry.
func (s *SandboxCleanupStore) Retry(ctx context.Context, lease contracts.SandboxCleanupLease, observation contracts.SandboxCleanupObservation) (contracts.SandboxCleanupJob, error) {
	if s == nil || s.pool == nil || !validSandboxCleanupLease(lease) {
		return contracts.SandboxCleanupJob{}, fmt.Errorf("sandbox cleanup store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, lease.WorkspaceID)
	if err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	job, err := readSandboxCleanupJobTx(ctx, tx, lease.WorkspaceID, lease.JobID, true)
	if err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	if err := observation.Normalize(job, lease); err != nil || observation.State == contracts.SandboxCleanupRemoved || observation.State == contracts.SandboxCleanupAlreadyAbsent {
		return contracts.SandboxCleanupJob{}, ErrSandboxCleanupConflict
	}
	if !validSandboxCleanupFailureCode(observation.FailureCode) {
		return contracts.SandboxCleanupJob{}, ErrSandboxCleanupConflict
	}
	status := contracts.SandboxCleanupRetryWait
	var retryDelay time.Duration
	if job.Attempts >= contracts.MaxSandboxCleanupAttempts {
		status = contracts.SandboxCleanupDeadLetter
	} else {
		exponent := max(0, min(job.Attempts-1, 10))
		retryDelay = time.Second << exponent
		if retryDelay > maxSandboxCleanupBackoff {
			retryDelay = maxSandboxCleanupBackoff
		}
	}
	var version int64
	err = tx.QueryRow(ctx, `
		UPDATE fornix.sandbox_cleanup_jobs
		SET status=$5,owner_id='',lease_until=NULL,
		    retry_at=CASE WHEN $6::bigint=0 THEN retry_at ELSE clock_timestamp()+($6 * interval '1 microsecond') END,
		    failure_code=$7,updated_at=clock_timestamp(),version=version+1
		WHERE workspace_id=$1 AND id=$2 AND status='leased' AND owner_id=$3 AND fence=$4 AND lease_until > clock_timestamp()
		RETURNING version`, lease.WorkspaceID, lease.JobID, lease.OwnerID, int64(lease.Fence), status, retryDelay.Microseconds(), observation.FailureCode).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.SandboxCleanupJob{}, ErrSandboxCleanupFence
	}
	if err != nil {
		return contracts.SandboxCleanupJob{}, fmt.Errorf("schedule sandbox cleanup retry: %w", err)
	}
	job, err = readSandboxCleanupJobTx(ctx, tx, lease.WorkspaceID, lease.JobID, false)
	if err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	kind := "retried"
	if status == contracts.SandboxCleanupDeadLetter {
		kind = "dead_letter"
	}
	if err := appendSandboxCleanupEventTx(ctx, tx, job, kind, lease.OwnerID, contracts.ActorRef{ID: lease.OwnerID, Kind: "service", WorkspaceID: lease.WorkspaceID}, observation.FailureCode, observation.State); err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	return job, nil
}

// Get reads a single workspace-scoped cleanup job without exposing event
// payloads, raw runner diagnostics, or host paths.
func (s *SandboxCleanupStore) Get(ctx context.Context, workspaceID, jobID string) (contracts.SandboxCleanupJob, error) {
	if s == nil || s.pool == nil {
		return contracts.SandboxCleanupJob{}, fmt.Errorf("sandbox cleanup store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	job, err := readSandboxCleanupJobTx(ctx, tx, workspaceID, jobID, false)
	if err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	return job, nil
}

// GetForAttempt reads the unique cleanup intent attached to one exact
// workspace-scoped tool attempt.
func (s *SandboxCleanupStore) GetForAttempt(ctx context.Context, workspaceID, toolRunID string, attempt int) (contracts.SandboxCleanupJob, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(toolRunID) == "" || attempt < 1 {
		return contracts.SandboxCleanupJob{}, fmt.Errorf("sandbox cleanup attempt lookup is invalid")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id string
	if err := tx.QueryRow(ctx, `SELECT id FROM fornix.sandbox_cleanup_jobs WHERE workspace_id=$1 AND tool_run_id=$2 AND tool_attempt=$3`, workspaceID, toolRunID, attempt).Scan(&id); errors.Is(err, pgx.ErrNoRows) {
		return contracts.SandboxCleanupJob{}, ErrSandboxCleanupMissing
	} else if err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	job, err := readSandboxCleanupJobTx(ctx, tx, workspaceID, id, false)
	if err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.SandboxCleanupJob{}, err
	}
	return job, nil
}

// enqueueTerminalSandboxCleanupTx creates cleanup authority only after the
// exact tool attempt is terminal and its generic effect and domain link agree
// on a verified result. It is called by both finalization orderings.
func enqueueTerminalSandboxCleanupTx(ctx context.Context, tx pgx.Tx, workspaceID, toolRunID string) error {
	var idempotencyKey string
	err := tx.QueryRow(ctx, `SELECT idempotency_key FROM fornix.tool_runs WHERE workspace_id=$1 AND id=$2`, workspaceID, toolRunID).Scan(&idempotencyKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrToolRunMissing
	}
	if err != nil {
		return err
	}
	run, err := readToolRunTx(ctx, tx, workspaceID, idempotencyKey)
	if err != nil {
		return err
	}
	if !contracts.IsToolTerminal(run.Status) || run.Status == contracts.ToolRunRecoveryRequired || run.Attempt < 1 || run.Result == nil {
		return nil
	}
	var request contracts.ToolRequest
	if len(run.RequestEvidence) == 0 || json.Unmarshal(run.RequestEvidence, &request) != nil {
		return ErrSandboxCleanupConflict
	}
	if request.Budget.Backend == string(contracts.SandboxBackendLocalProcess) {
		return nil
	}
	link, err := readDomainEffectLinkByDomainTx(ctx, tx, workspaceID, contracts.DomainEffectKindToolRun, run.ID, contracts.DomainEffectLinkRolePrimary)
	if errors.Is(err, ErrDomainEffectLinkNotFound) {
		// No effect link means no durable external-effect authority exists, so no
		// runtime cleanup is authorized. Still allow the terminal result to commit.
		return nil
	}
	if err != nil {
		return fmt.Errorf("read sandbox cleanup effect link: %w", err)
	}
	transition, err := readLatestDomainEffectLinkTransitionTx(ctx, tx, workspaceID, link.ID, false)
	if err != nil {
		return err
	}
	effect, err := readEffectState(ctx, tx, workspaceID, link.EffectID, false)
	if err != nil {
		return err
	}
	if effect.State != contracts.ExternalEffectVerified {
		return nil
	}
	if transition.ToStatus != contracts.DomainEffectLinkStatusReconciled || transition.ResultHash == "" ||
		transition.ResultHash != effect.ResponseHash || run.Result.ContentHash == "" || run.Result.ContentHash != effect.ResponseHash {
		return ErrSandboxCleanupConflict
	}
	identity := contracts.SandboxExecutionIdentity{
		SchemaVersion: contracts.SandboxExecutionIdentitySchemaVersion,
		WorkspaceID:   run.WorkspaceID, ToolRunID: run.ID, ToolAttempt: run.Attempt,
		Backend: contracts.SandboxBackend(link.Boundary), OperationID: link.OperationID,
		OperationOwnerID: link.OperationOwnerID, OperationFence: link.OperationFence,
		AttemptID: link.AttemptID, EffectID: link.EffectID, ToolRequestHash: run.RequestHash,
		OperationRequestHash: link.OperationHash, EffectReservationHash: link.EffectReservationHash,
		TaskOwnerID: link.TaskOwnerID, TaskFence: link.TaskFence,
		AgentRunID: agentRunID(run.AgentRun), AgentRunOwnerID: run.AgentRunOwnerID, AgentRunFence: run.AgentRunFence,
		ToolDefinitionHash: request.ToolDefinitionHash, SandboxProfileHash: request.SandboxProfileHash,
		QualificationHash: request.SandboxQualificationHash,
	}
	intent := contracts.SandboxCleanupIntent{
		WorkspaceID: run.WorkspaceID, ToolRunID: run.ID, ToolAttempt: run.Attempt,
		DomainLinkID: link.ID, Identity: identity, ResultHash: run.Result.ContentHash,
		Actor: contracts.ActorRef{ID: run.Actor.ID, Kind: run.Actor.Kind, WorkspaceID: run.WorkspaceID},
	}
	if err := intent.Normalize(); err != nil {
		return fmt.Errorf("normalize sandbox cleanup intent: %w", err)
	}
	_, _, err = NewSandboxCleanupStore(nil).EnqueueTx(ctx, tx, intent)
	return err
}

const sandboxCleanupJobSelect = `SELECT workspace_id,id,tool_run_id,tool_attempt,domain_link_id,domain_kind,domain_id,backend,execution_identity,execution_identity_hash,intent_hash,tool_request_hash,result_hash,actor,status,owner_id,fence,attempt_count,retry_at,lease_until,failure_code,completion_state,completion_owner_id,version,created_at,updated_at,completed_at FROM fornix.sandbox_cleanup_jobs`

func readSandboxCleanupJobTx(ctx context.Context, tx pgx.Tx, workspaceID, jobID string, forUpdate bool) (contracts.SandboxCleanupJob, error) {
	query := sandboxCleanupJobSelect + ` WHERE workspace_id=$1 AND id=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var job contracts.SandboxCleanupJob
	var identityJSON, actorJSON []byte
	var backend, workspace, runID, linkID, domainKind, domainID, identityHash, requestHash, resultHash string
	var attempt, fence, attempts int64
	var retryAt time.Time
	var leaseUntil, completedAt *time.Time
	if err := tx.QueryRow(ctx, query, workspaceID, jobID).Scan(
		&workspace, &job.ID, &runID, &attempt, &linkID, &domainKind, &domainID, &backend, &identityJSON, &identityHash,
		&job.IntentHash, &requestHash, &resultHash, &actorJSON, &job.Status, &job.OwnerID, &fence,
		&attempts, &retryAt, &leaseUntil, &job.FailureCode, &job.CompletionState, &job.CompletionOwnerID, &job.Version,
		&job.CreatedAt, &job.UpdatedAt, &completedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.SandboxCleanupJob{}, ErrSandboxCleanupMissing
		}
		return contracts.SandboxCleanupJob{}, err
	}
	var identity contracts.SandboxExecutionIdentity
	var actor contracts.ActorRef
	if err := json.Unmarshal(identityJSON, &identity); err != nil {
		return contracts.SandboxCleanupJob{}, fmt.Errorf("decode sandbox cleanup identity: %w", err)
	}
	if err := json.Unmarshal(actorJSON, &actor); err != nil {
		return contracts.SandboxCleanupJob{}, fmt.Errorf("decode sandbox cleanup actor: %w", err)
	}
	job.Intent = contracts.SandboxCleanupIntent{
		WorkspaceID: workspace, ToolRunID: runID, ToolAttempt: int(attempt), DomainLinkID: linkID,
		Identity: identity, ResultHash: resultHash, Actor: actor,
	}
	job.IntentHash = strings.TrimSpace(job.IntentHash)
	job.Fence, job.Attempts = uint64(fence), int(attempts)
	job.RetryAt, job.LeaseUntil, job.CompletedAt = retryAt, leaseUntil, completedAt
	if domainKind != contracts.DomainEffectKindToolRun || domainID != runID || identity.Normalize() != nil || string(identity.Backend) != backend || identity.StableHash() != identityHash || identity.ToolRequestHash != requestHash || job.Intent.Normalize() != nil || job.Intent.StableHash() != job.IntentHash {
		return contracts.SandboxCleanupJob{}, ErrSandboxCleanupConflict
	}
	return job, nil
}

func appendSandboxCleanupEventTx(ctx context.Context, tx pgx.Tx, job contracts.SandboxCleanupJob, kind, workerID string, actor contracts.ActorRef, failureCode, observationState string) error {
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	var retryAt any
	if !job.RetryAt.IsZero() && job.Status == contracts.SandboxCleanupRetryWait {
		retryAt = job.RetryAt
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO fornix.sandbox_cleanup_events(workspace_id,job_id,version,kind,status,worker_id,fence,failure_code,observation_state,retry_at,actor)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)`, job.Intent.WorkspaceID, job.ID, job.Version,
		kind, job.Status, workerID, int64(job.Fence), failureCode, observationState, retryAt, actorJSON)
	if err != nil {
		return fmt.Errorf("append sandbox cleanup event: %w", err)
	}
	return nil
}

func validSandboxCleanupFailureCode(code string) bool {
	code = strings.TrimSpace(code)
	if code == "" || len(code) > contracts.MaxSandboxCleanupFailureCodeLength {
		return false
	}
	for _, char := range code {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_') {
			return false
		}
	}
	return true
}

func validSandboxCleanupLease(lease contracts.SandboxCleanupLease) bool {
	return strings.TrimSpace(lease.WorkspaceID) != "" && strings.TrimSpace(lease.JobID) != "" &&
		strings.TrimSpace(lease.OwnerID) != "" && len(lease.OwnerID) <= contracts.MaxDomainIDLength &&
		lease.Fence > 0 && lease.Fence <= uint64(1<<63-1)
}
