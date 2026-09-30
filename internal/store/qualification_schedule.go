package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrQualificationScheduleNotFound       = errors.New("qualification refresh schedule not found")
	ErrQualificationScheduleConflict       = errors.New("qualification refresh schedule conflicts with existing state")
	ErrQualificationScheduleCursor         = errors.New("invalid qualification refresh schedule cursor")
	ErrQualificationScheduleHeld           = errors.New("qualification refresh schedule is owned by another worker")
	ErrQualificationScheduleOwned          = errors.New("qualification refresh schedule is not owned by this worker")
	ErrQualificationScheduleFenced         = errors.New("qualification refresh schedule fence is stale")
	ErrQualificationScheduleExpired        = errors.New("qualification refresh schedule lease is expired")
	ErrQualificationScheduleReleased       = errors.New("qualification refresh schedule lease is released")
	ErrQualificationScheduleFenceExhausted = errors.New("qualification refresh schedule fence is exhausted")
	ErrQualificationScheduleNotDue         = errors.New("qualification refresh schedule is not due")
	ErrQualificationScheduleTerminal       = errors.New("qualification refresh schedule is terminal")
	ErrQualificationScheduleAttempt        = errors.New("qualification refresh schedule attempt is invalid")
	ErrQualificationScheduleRecovery       = errors.New("qualification refresh schedule recovery evidence is not satisfied")
)

const maxQualificationScheduleFence = uint64(1<<63 - 1)

// RegisterQualificationRefreshSchedule creates one immutable schedule per
// workspace/deployment/release. The projection is mutable only through the
// fenced methods in this file; all meaningful transitions are events.
func (s *DeploymentEvidenceStore) RegisterQualificationRefreshSchedule(ctx context.Context, request contracts.QualificationRefreshScheduleRequest, now time.Time) (contracts.QualificationRefreshSchedule, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshSchedule{}, false, fmt.Errorf("deployment evidence store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.QualificationRefreshSchedule{}, false, err
	}
	now = scheduleNow(now)
	configHash := request.StableHash()
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-qualification-refresh-schedule:' || $1 || ':' || $2 || ':' || $3, 0))`, request.WorkspaceID, request.DeploymentID, request.ReleaseID); err != nil {
		return contracts.QualificationRefreshSchedule{}, false, err
	}
	if existing, err := queryQualificationScheduleByScopeTx(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ReleaseID, true); err == nil {
		if existing.ConfigHash != configHash {
			return contracts.QualificationRefreshSchedule{}, false, ErrQualificationScheduleConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationRefreshSchedule{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(err, ErrQualificationScheduleNotFound) {
		return contracts.QualificationRefreshSchedule{}, false, err
	}
	if existing, err := queryQualificationScheduleByIdempotencyTx(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true); err == nil {
		if existing.ConfigHash != configHash {
			return contracts.QualificationRefreshSchedule{}, false, ErrQualificationScheduleConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationRefreshSchedule{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(err, ErrQualificationScheduleNotFound) {
		return contracts.QualificationRefreshSchedule{}, false, err
	}
	evidenceJSON, _ := json.Marshal(request.RequiredEvidenceKinds)
	drillsJSON, _ := json.Marshal(request.RequiredRecoveryDrills)
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, false, err
	}
	schedule := contracts.QualificationRefreshSchedule{
		SchemaVersion: contracts.QualificationRefreshScheduleSchemaVersion, ID: contracts.NewID("qualification-refresh-schedule"),
		WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, ReleaseID: request.ReleaseID, ConfigHash: configHash,
		RequiredEvidenceKinds: request.RequiredEvidenceKinds, RequiredRecoveryDrills: request.RequiredRecoveryDrills,
		IntervalMS: request.IntervalMS, FreshnessMS: request.FreshnessMS, MaxAttempts: request.MaxAttempts,
		BackoffBaseMS: request.BackoffBaseMS, BackoffMaxMS: request.BackoffMaxMS, Status: contracts.QualificationRefreshScheduleActive,
		NextDueAt: timePtr(request.FirstDueAt), CreatedAt: now, UpdatedAt: now,
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_refresh_schedules
		(id,workspace_id,deployment_id,release_id,config_hash,required_evidence_kinds,required_recovery_drills,interval_ms,freshness_ms,max_attempts,backoff_base_ms,backoff_max_ms,status,next_due_at,request_id,idempotency_key,causation_id,correlation_id,actor,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10,$11,$12,'active',$13,$14,$15,$16,$17,$18::jsonb,$19,$19)`,
		schedule.ID, schedule.WorkspaceID, schedule.DeploymentID, schedule.ReleaseID, schedule.ConfigHash, evidenceJSON, drillsJSON,
		schedule.IntervalMS, schedule.FreshnessMS, schedule.MaxAttempts, schedule.BackoffBaseMS, schedule.BackoffMaxMS, schedule.NextDueAt,
		nullableString(request.RequestID), request.IdempotencyKey, nullableString(request.CausationID), nullableString(request.CorrelationID), actorJSON, now); err != nil {
		if isUniqueViolation(err) {
			return contracts.QualificationRefreshSchedule{}, false, ErrQualificationScheduleConflict
		}
		return contracts.QualificationRefreshSchedule{}, false, fmt.Errorf("insert qualification refresh schedule: %w", err)
	}
	if err := appendQualificationScheduleEventTx(ctx, tx, schedule, "registered", "", 0, "", request.Actor, map[string]string{"config_hash": schedule.ConfigHash}, request.RequestID, request.IdempotencyKey, request.CausationID, request.CorrelationID); err != nil {
		return contracts.QualificationRefreshSchedule{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRefreshSchedule{}, false, err
	}
	return schedule, true, nil
}

func (s *DeploymentEvidenceStore) GetQualificationRefreshSchedule(ctx context.Context, workspaceID, scheduleID string) (contracts.QualificationRefreshSchedule, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshSchedule{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, scheduleID = strings.TrimSpace(workspaceID), strings.TrimSpace(scheduleID)
	if workspaceID == "" || scheduleID == "" {
		return contracts.QualificationRefreshSchedule{}, ErrQualificationScheduleNotFound
	}
	var schedule contracts.QualificationRefreshSchedule
	err := workspaceQueryRow(ctx, s.pool, workspaceID, qualificationScheduleSelect()+` WHERE workspace_id=$1 AND id=$2`, []any{workspaceID, scheduleID}, func(row pgx.Row) error { var err error; schedule, err = scanQualificationSchedule(row); return err })
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationRefreshSchedule{}, ErrQualificationScheduleNotFound
	}
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	return schedule, nil
}

func (s *DeploymentEvidenceStore) ListQualificationRefreshSchedules(ctx context.Context, workspaceID, deploymentID, releaseID string, limit int, cursor string) (contracts.QualificationRefreshSchedulePage, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshSchedulePage{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, deploymentID, releaseID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" {
		return contracts.QualificationRefreshSchedulePage{}, ErrQualificationScheduleNotFound
	}
	limit = boundedQualificationPageLimit(limit)
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.QualificationRefreshSchedulePage{}, ErrQualificationScheduleCursor
	}
	query := qualificationScheduleSelect() + ` WHERE workspace_id=$1 AND deployment_id=$2 AND id>$3`
	args := []any{workspaceID, deploymentID, cursor}
	if releaseID != "" {
		query += ` AND release_id=$4`
		args = append(args, releaseID)
	}
	query += ` ORDER BY id LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit+1)
	items := make([]contracts.QualificationRefreshSchedule, 0, limit+1)
	err := workspaceQueryRows(ctx, s.pool, workspaceID, query, args, func(rows pgx.Rows) error {
		for rows.Next() {
			item, err := scanQualificationScheduleRows(rows)
			if err != nil {
				return err
			}
			items = append(items, item)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.QualificationRefreshSchedulePage{}, err
	}
	page := contracts.QualificationRefreshSchedulePage{Items: items}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// ListQualificationRefreshSchedulePlan returns only active, due, currently
// unleased schedules. It is a read-only deterministic planning view; claim is
// still the only authority that grants ownership.
func (s *DeploymentEvidenceStore) ListQualificationRefreshSchedulePlan(ctx context.Context, workspaceID, deploymentID, releaseID string, limit int, cursor string, now time.Time) (contracts.QualificationRefreshSchedulePage, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshSchedulePage{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, deploymentID, releaseID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" {
		return contracts.QualificationRefreshSchedulePage{}, ErrQualificationScheduleNotFound
	}
	limit = boundedQualificationPageLimit(limit)
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.QualificationRefreshSchedulePage{}, ErrQualificationScheduleCursor
	}
	now = scheduleNow(now)
	query := qualificationScheduleSelect() + ` WHERE workspace_id=$1 AND deployment_id=$2 AND status='active' AND next_due_at IS NOT NULL AND next_due_at <= $3 AND (lease_until IS NULL OR lease_until <= $3) AND id>$4`
	args := []any{workspaceID, deploymentID, now, cursor}
	if releaseID != "" {
		query += ` AND release_id=$5`
		args = append(args, releaseID)
	}
	// Cursor pagination is ID-based, so the disclosure order intentionally
	// remains ID order. Claiming still uses due-time priority transactionally.
	query += ` ORDER BY id LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit+1)
	items := make([]contracts.QualificationRefreshSchedule, 0, limit+1)
	err := workspaceQueryRows(ctx, s.pool, workspaceID, query, args, func(rows pgx.Rows) error {
		for rows.Next() {
			item, err := scanQualificationScheduleRows(rows)
			if err != nil {
				return err
			}
			items = append(items, item)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.QualificationRefreshSchedulePage{}, err
	}
	page := contracts.QualificationRefreshSchedulePage{Items: items}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// ClaimQualificationRefreshSchedule atomically selects the oldest due active
// schedule in a workspace. Expired owners are taken over with a higher fence.
func (s *DeploymentEvidenceStore) ClaimQualificationRefreshSchedule(ctx context.Context, workspaceID, ownerID string, ttl time.Duration, now time.Time) (contracts.QualificationRefreshScheduleClaim, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshScheduleClaim{}, false, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, ownerID = strings.TrimSpace(workspaceID), strings.TrimSpace(ownerID)
	if workspaceID == "" || ownerID == "" {
		return contracts.QualificationRefreshScheduleClaim{}, false, fmt.Errorf("workspace_id and owner_id are required")
	}
	now = scheduleNow(now)
	ttl = normalizeScheduleLeaseTTL(ttl)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.QualificationRefreshScheduleClaim{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var schedule contracts.QualificationRefreshSchedule
	schedule, err = scanQualificationSchedule(tx.QueryRow(ctx, qualificationScheduleSelect()+` WHERE workspace_id=$1 AND status='active' AND next_due_at IS NOT NULL AND next_due_at <= $2 AND (lease_until IS NULL OR lease_until <= $2) ORDER BY next_due_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1`, workspaceID, now))
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationRefreshScheduleClaim{}, false, err
		}
		return contracts.QualificationRefreshScheduleClaim{}, false, nil
	}
	if err != nil {
		return contracts.QualificationRefreshScheduleClaim{}, false, err
	}
	takeover := schedule.LeaseOwnerID != "" && schedule.LeaseUntil != nil && !schedule.LeaseUntil.After(now)
	if schedule.LeaseFence >= maxQualificationScheduleFence {
		return contracts.QualificationRefreshScheduleClaim{}, false, ErrQualificationScheduleFenceExhausted
	}
	fence := schedule.LeaseFence + 1
	attemptNumber := schedule.AttemptCount + 1
	retryCount := schedule.RetryCount + 1
	attemptID := qualificationScheduleAttemptID(schedule.ID, attemptNumber)
	leaseUntil := now.Add(ttl)
	planHash := contracts.QualificationRefreshSchedulePlanHash(schedule, attemptID, ownerID, fence, now, attemptNumber)
	if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_refresh_schedules SET lease_owner_id=$3,lease_fence=$4,lease_until=$5,attempt_count=$6,retry_count=$7,updated_at=$8 WHERE workspace_id=$1 AND id=$2`, workspaceID, schedule.ID, ownerID, fence, leaseUntil, attemptNumber, retryCount, now); err != nil {
		return contracts.QualificationRefreshScheduleClaim{}, false, err
	}
	schedule.LeaseOwnerID, schedule.LeaseFence, schedule.LeaseUntil, schedule.AttemptCount, schedule.RetryCount, schedule.UpdatedAt = ownerID, fence, &leaseUntil, attemptNumber, retryCount, now
	event := "claimed"
	if takeover {
		event = "taken_over"
	}
	if err := appendQualificationScheduleEventTx(ctx, tx, schedule, event, ownerID, fence, attemptID, contracts.AuditActor{ID: ownerID, Kind: "scheduler_worker", WorkspaceID: workspaceID}, map[string]string{"plan_hash": planHash, "attempt_number": strconv.Itoa(attemptNumber)}, "", "qualification-schedule-claim:"+schedule.ID+":"+strconv.FormatUint(fence, 10), "", ""); err != nil {
		return contracts.QualificationRefreshScheduleClaim{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRefreshScheduleClaim{}, false, err
	}
	return contracts.QualificationRefreshScheduleClaim{Schedule: schedule, AttemptID: attemptID, AttemptNumber: attemptNumber, OwnerID: ownerID, Fence: fence, LeaseUntil: leaseUntil, AsOf: now, PlanHash: planHash, Takeover: takeover}, true, nil
}

func (s *DeploymentEvidenceStore) RenewQualificationRefreshSchedule(ctx context.Context, workspaceID, scheduleID, ownerID string, fence uint64, ttl time.Duration, now time.Time) (contracts.QualificationRefreshSchedule, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshSchedule{}, fmt.Errorf("deployment evidence store is not configured")
	}
	now = scheduleNow(now)
	ttl = normalizeScheduleLeaseTTL(ttl)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schedule, err := lockQualificationScheduleTx(ctx, tx, workspaceID, scheduleID)
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	if err := validateQualificationScheduleLease(schedule, ownerID, fence, now); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	leaseUntil := now.Add(ttl)
	if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_refresh_schedules SET lease_until=$4,updated_at=$5 WHERE workspace_id=$1 AND id=$2 AND lease_owner_id=$3 AND lease_fence=$6`, workspaceID, scheduleID, ownerID, leaseUntil, now, int64(fence)); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	schedule.LeaseUntil, schedule.UpdatedAt = &leaseUntil, now
	if err := appendQualificationScheduleEventTx(ctx, tx, schedule, "renewed", ownerID, fence, "", contracts.AuditActor{ID: ownerID, Kind: "scheduler_worker", WorkspaceID: workspaceID}, map[string]string{"lease_until": leaseUntil.Format(time.RFC3339Nano)}, "", "qualification-schedule-renew:"+scheduleID+":"+strconv.FormatUint(fence, 10), "", ""); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	return schedule, nil
}

func (s *DeploymentEvidenceStore) ReleaseQualificationRefreshSchedule(ctx context.Context, workspaceID, scheduleID, ownerID string, fence uint64, now time.Time) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("deployment evidence store is not configured")
	}
	now = scheduleNow(now)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schedule, err := lockQualificationScheduleTx(ctx, tx, workspaceID, scheduleID)
	if err != nil {
		return err
	}
	if err := validateQualificationScheduleLease(schedule, ownerID, fence, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_refresh_schedules SET lease_owner_id=NULL,lease_until=NULL,updated_at=$3 WHERE workspace_id=$1 AND id=$2 AND lease_owner_id=$4 AND lease_fence=$5`, workspaceID, scheduleID, now, ownerID, int64(fence)); err != nil {
		return err
	}
	schedule.LeaseOwnerID, schedule.LeaseUntil, schedule.UpdatedAt = "", nil, now
	if err := appendQualificationScheduleEventTx(ctx, tx, schedule, "released", ownerID, fence, "", contracts.AuditActor{ID: ownerID, Kind: "scheduler_worker", WorkspaceID: workspaceID}, nil, "", "qualification-schedule-release:"+scheduleID+":"+strconv.FormatUint(fence, 10), "", ""); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CompleteQualificationRefreshSchedule records a fenced attempt and advances
// the projection in the same transaction. A successful completion must point
// to an existing Task 92 report and its signed imports; callers cannot assert
// recovery success by supplying a hash alone.
func (s *DeploymentEvidenceStore) CompleteQualificationRefreshSchedule(ctx context.Context, request contracts.QualificationRefreshScheduleCompletionRequest, now time.Time) (contracts.QualificationRefreshSchedule, contracts.QualificationRefreshScheduleAttempt, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, fmt.Errorf("deployment evidence store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
	}
	now = scheduleNow(now)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schedule, err := lockQualificationScheduleTx(ctx, tx, request.WorkspaceID, request.ScheduleID)
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
	}
	if existing, existingErr := queryQualificationScheduleAttemptByIdempotencyTx(ctx, tx, schedule.ID, request.IdempotencyKey, true); existingErr == nil {
		if existing.ID != request.AttemptID || existing.OwnerID != request.OwnerID || existing.Fence != request.Fence || existing.Outcome != request.Outcome || existing.RefreshID != request.RefreshID || existing.RefreshHash != request.RefreshHash || existing.PlanHash != request.PlanHash || !existing.AsOf.Equal(request.AsOf.UTC()) {
			return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, ErrQualificationScheduleConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
		}
		return schedule, existing, true, nil
	} else if !errors.Is(existingErr, ErrQualificationScheduleAttempt) {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, existingErr
	}
	if err := validateQualificationScheduleLease(schedule, request.OwnerID, request.Fence, now); err != nil {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
	}
	attemptNumber := schedule.AttemptCount
	attemptID := qualificationScheduleAttemptID(schedule.ID, attemptNumber)
	if request.AttemptID != attemptID || request.PlanHash != contracts.QualificationRefreshSchedulePlanHash(schedule, attemptID, request.OwnerID, request.Fence, request.AsOf, attemptNumber) {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, ErrQualificationScheduleAttempt
	}
	if request.Outcome == contracts.QualificationRefreshScheduleOutcomeSucceeded {
		if err := validateQualificationScheduleRefreshTx(ctx, tx, schedule, request, now); err != nil {
			return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
		}
	}
	var retryAt *time.Time
	nextStatus := contracts.QualificationRefreshScheduleActive
	nextRetryCount := schedule.RetryCount
	nextDue := timePtr(now.Add(time.Duration(schedule.IntervalMS) * time.Millisecond))
	event := "completed"
	if request.Outcome == contracts.QualificationRefreshScheduleOutcomeRetryable {
		if schedule.RetryCount >= schedule.MaxAttempts {
			nextStatus, nextDue, event = contracts.QualificationRefreshScheduleDeadLetter, nil, "dead_lettered"
		} else {
			delay := contracts.QualificationRefreshScheduleBackoffMS(schedule.BackoffBaseMS, schedule.BackoffMaxMS, schedule.RetryCount)
			value := now.Add(time.Duration(delay) * time.Millisecond)
			retryAt, nextDue, event = &value, &value, "retry_scheduled"
		}
	} else if request.Outcome == contracts.QualificationRefreshScheduleOutcomeSucceeded {
		nextRetryCount = 0
	} else if request.Outcome == contracts.QualificationRefreshScheduleOutcomeFailed {
		nextStatus, nextDue, event = contracts.QualificationRefreshSchedulePaused, nil, "paused"
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
	}
	attempt := contracts.QualificationRefreshScheduleAttempt{SchemaVersion: contracts.QualificationRefreshScheduleSchemaVersion, ID: attemptID, ScheduleID: schedule.ID, WorkspaceID: schedule.WorkspaceID, DeploymentID: schedule.DeploymentID, ReleaseID: schedule.ReleaseID, AttemptNumber: attemptNumber, OwnerID: request.OwnerID, Fence: request.Fence, AsOf: request.AsOf, PlanHash: request.PlanHash, RefreshID: request.RefreshID, RefreshHash: request.RefreshHash, Outcome: request.Outcome, Retryable: request.Retryable, ErrorCode: request.ErrorCode, RetryAt: retryAt, IdempotencyKey: request.IdempotencyKey, CreatedAt: now}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_refresh_schedule_attempts(id,schedule_id,workspace_id,deployment_id,release_id,attempt_number,owner_id,fence,as_of,plan_hash,refresh_id,refresh_hash,outcome,retryable,error_code,retry_at,idempotency_key,actor,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19)`, attempt.ID, attempt.ScheduleID, attempt.WorkspaceID, attempt.DeploymentID, attempt.ReleaseID, attempt.AttemptNumber, attempt.OwnerID, int64(attempt.Fence), attempt.AsOf, attempt.PlanHash, nullableString(attempt.RefreshID), nullableString(attempt.RefreshHash), attempt.Outcome, attempt.Retryable, nullableString(attempt.ErrorCode), attempt.RetryAt, attempt.IdempotencyKey, actorJSON, now); err != nil {
		if isUniqueViolation(err) {
			return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, ErrQualificationScheduleConflict
		}
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_refresh_schedules SET status=$3,next_due_at=$4,last_as_of=$5,last_attempt_id=$6,last_report_id=$7,last_report_hash=$8,last_outcome=$9,last_error_code=$10,retry_count=$11,lease_owner_id=NULL,lease_until=NULL,updated_at=$12 WHERE workspace_id=$1 AND id=$2 AND lease_owner_id=$13 AND lease_fence=$14`, schedule.WorkspaceID, schedule.ID, nextStatus, nextDue, request.AsOf, attempt.ID, nullableString(request.RefreshID), nullableString(request.RefreshHash), request.Outcome, nullableString(request.ErrorCode), nextRetryCount, now, request.OwnerID, int64(request.Fence)); err != nil {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
	}
	schedule.Status, schedule.NextDueAt, schedule.LastAsOf, schedule.LastAttemptID, schedule.LastReportID, schedule.LastReportHash, schedule.LastOutcome, schedule.LastErrorCode, schedule.RetryCount, schedule.LeaseOwnerID, schedule.LeaseUntil, schedule.UpdatedAt = nextStatus, nextDue, timePtr(request.AsOf), attempt.ID, request.RefreshID, request.RefreshHash, request.Outcome, request.ErrorCode, nextRetryCount, "", nil, now
	if err := appendQualificationScheduleEventTx(ctx, tx, schedule, event, request.OwnerID, request.Fence, attempt.ID, request.Actor, map[string]string{"outcome": request.Outcome, "attempt_number": strconv.Itoa(attemptNumber)}, request.RequestID, request.IdempotencyKey, request.CausationID, request.CorrelationID); err != nil {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRefreshSchedule{}, contracts.QualificationRefreshScheduleAttempt{}, false, err
	}
	return schedule, attempt, false, nil
}

func (s *DeploymentEvidenceStore) SetQualificationRefreshScheduleState(ctx context.Context, workspaceID, scheduleID, state, ownerID string, fence uint64, actor contracts.AuditActor, reason string, now time.Time) (contracts.QualificationRefreshSchedule, error) {
	state = strings.ToLower(strings.TrimSpace(state))
	if err := contracts.ValidateQualificationRefreshScheduleStatus(state); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	now = scheduleNow(now)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schedule, err := lockQualificationScheduleTx(ctx, tx, workspaceID, scheduleID)
	if err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	if state == contracts.QualificationRefreshScheduleActive {
		if schedule.Status == contracts.QualificationRefreshScheduleCancelled {
			return contracts.QualificationRefreshSchedule{}, ErrQualificationScheduleTerminal
		}
		if schedule.Status == contracts.QualificationRefreshScheduleDeadLetter {
			return contracts.QualificationRefreshSchedule{}, ErrQualificationScheduleTerminal
		}
		if schedule.Status == contracts.QualificationRefreshScheduleActive {
			if err := tx.Commit(ctx); err != nil {
				return contracts.QualificationRefreshSchedule{}, err
			}
			return schedule, nil
		}
	} else {
		if err := validateQualificationScheduleLease(schedule, ownerID, fence, now); err != nil {
			return contracts.QualificationRefreshSchedule{}, err
		}
		if state == contracts.QualificationRefreshScheduleCancelled && schedule.Status == contracts.QualificationRefreshScheduleCancelled {
			return schedule, tx.Commit(ctx)
		}
	}
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(actor.WorkspaceID) != strings.TrimSpace(workspaceID) || len(actor.ID) > contracts.MaxDomainIDLength {
		return contracts.QualificationRefreshSchedule{}, fmt.Errorf("qualification schedule actor crosses workspace boundary or is invalid")
	}
	if state == contracts.QualificationRefreshScheduleActive && schedule.NextDueAt == nil {
		schedule.NextDueAt = timePtr(now)
	}
	if state != contracts.QualificationRefreshScheduleActive {
		schedule.NextDueAt = nil
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_refresh_schedules SET status=$3,next_due_at=$4,lease_owner_id=NULL,lease_until=NULL,updated_at=$5,last_error_code=$6 WHERE workspace_id=$1 AND id=$2`, workspaceID, scheduleID, state, schedule.NextDueAt, now, nullableString(reason)); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	schedule.Status, schedule.UpdatedAt, schedule.LeaseOwnerID, schedule.LeaseUntil, schedule.LastErrorCode = state, now, "", nil, reason
	event := map[string]string{"paused": "paused", "cancelled": "cancelled", "active": "resumed", "dead_letter": "dead_lettered"}[state]
	if err := appendQualificationScheduleEventTx(ctx, tx, schedule, event, ownerID, fence, "", actor, map[string]string{"reason": reason}, "", "qualification-schedule-state:"+schedule.ID+":"+state, "", ""); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	return schedule, nil
}

func (s *DeploymentEvidenceStore) ListQualificationRefreshScheduleAttempts(ctx context.Context, workspaceID, scheduleID string, limit int, cursor string) (contracts.QualificationRefreshScheduleAttemptPage, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshScheduleAttemptPage{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, scheduleID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(scheduleID), strings.TrimSpace(cursor)
	if workspaceID == "" || scheduleID == "" {
		return contracts.QualificationRefreshScheduleAttemptPage{}, ErrQualificationScheduleNotFound
	}
	limit = boundedQualificationPageLimit(limit)
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.QualificationRefreshScheduleAttemptPage{}, ErrQualificationScheduleCursor
	}
	items := make([]contracts.QualificationRefreshScheduleAttempt, 0, limit+1)
	err := workspaceQueryRows(ctx, s.pool, workspaceID, qualificationScheduleAttemptSelect()+` WHERE workspace_id=$1 AND schedule_id=$2 AND id>$3 ORDER BY id LIMIT $4`, []any{workspaceID, scheduleID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			item, err := scanQualificationScheduleAttemptRows(rows)
			if err != nil {
				return err
			}
			items = append(items, item)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.QualificationRefreshScheduleAttemptPage{}, err
	}
	page := contracts.QualificationRefreshScheduleAttemptPage{Items: items}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

func validateQualificationScheduleLease(schedule contracts.QualificationRefreshSchedule, ownerID string, fence uint64, now time.Time) error {
	if schedule.LeaseOwnerID == "" || schedule.LeaseUntil == nil {
		return ErrQualificationScheduleReleased
	}
	if schedule.LeaseOwnerID != strings.TrimSpace(ownerID) {
		return ErrQualificationScheduleOwned
	}
	if schedule.LeaseFence != fence {
		return ErrQualificationScheduleFenced
	}
	if !schedule.LeaseUntil.After(now) {
		return ErrQualificationScheduleExpired
	}
	return nil
}

func validateQualificationRefreshScheduleAuthorizationTx(ctx context.Context, tx pgx.Tx, request contracts.QualificationRefreshRequest, now time.Time) error {
	authorization := request.ScheduleAuthorization
	if authorization == nil {
		return nil
	}
	schedule, err := lockQualificationScheduleTx(ctx, tx, request.WorkspaceID, authorization.ScheduleID)
	if err != nil {
		return err
	}
	if schedule.DeploymentID != request.DeploymentID || schedule.ReleaseID != request.ReleaseID {
		return ErrQualificationScheduleFenced
	}
	if err := validateQualificationScheduleLease(schedule, authorization.OwnerID, authorization.Fence, now); err != nil {
		return err
	}
	attemptNumber := schedule.AttemptCount
	attemptID := qualificationScheduleAttemptID(schedule.ID, attemptNumber)
	if attemptID != authorization.AttemptID || authorization.PlanHash != contracts.QualificationRefreshSchedulePlanHash(schedule, attemptID, authorization.OwnerID, authorization.Fence, request.AsOf, attemptNumber) {
		return ErrQualificationScheduleAttempt
	}
	return nil
}

func validateQualificationScheduleRefreshTx(ctx context.Context, tx pgx.Tx, schedule contracts.QualificationRefreshSchedule, request contracts.QualificationRefreshScheduleCompletionRequest, now time.Time) error {
	report, err := scanQualificationRefreshRun(tx.QueryRow(ctx, refreshRunSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3`, schedule.WorkspaceID, schedule.DeploymentID, request.RefreshID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrQualificationRefreshNotFound
	}
	if err != nil {
		return err
	}
	report, err = loadQualificationRefreshItemsTx(ctx, tx, report)
	if err != nil {
		return err
	}
	if report.WorkspaceID != schedule.WorkspaceID || report.DeploymentID != schedule.DeploymentID || report.ReleaseID != schedule.ReleaseID || report.ReportHash != request.RefreshHash || !report.AsOf.Equal(request.AsOf.UTC()) || report.AsOf.After(now) || now.Sub(report.AsOf) > time.Duration(schedule.FreshnessMS)*time.Millisecond || report.Outcome != contracts.QualificationOutcomePassed {
		return ErrQualificationScheduleRecovery
	}
	items := make(map[string]contracts.QualificationRefreshItemResult, len(report.Items))
	for _, item := range report.Items {
		items[item.Kind] = item
	}
	for _, kind := range schedule.RequiredEvidenceKinds {
		if _, ok := items[kind]; !ok {
			return ErrQualificationScheduleRecovery
		}
	}
	release, err := queryDeploymentRelease(ctx, tx, schedule.WorkspaceID, schedule.DeploymentID, schedule.ReleaseID, true)
	if err != nil {
		return err
	}
	for _, required := range schedule.RequiredRecoveryDrills {
		found := false
		for _, item := range report.Items {
			_, signed, importErr := queryQualificationImportEvidence(ctx, tx, schedule.WorkspaceID, schedule.DeploymentID, item.ImportID, true)
			if importErr != nil {
				return importErr
			}
			if signed.Bundle.Report.TargetHash != release.TargetHash {
				return ErrQualificationScheduleRecovery
			}
			for _, drill := range signed.Bundle.Report.RecoveryDrills {
				if drill.Kind == required && drill.Outcome == contracts.QualificationOutcomePassed && drill.TargetHash == release.TargetHash && !drill.ObservedAt.After(report.AsOf) && report.AsOf.Sub(drill.ObservedAt) <= time.Duration(schedule.FreshnessMS)*time.Millisecond {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return ErrQualificationScheduleRecovery
		}
	}
	return nil
}

func queryQualificationScheduleByScopeTx(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, releaseID string, lock bool) (contracts.QualificationRefreshSchedule, error) {
	clause := ""
	if lock {
		clause = " FOR UPDATE"
	}
	schedule, err := scanQualificationSchedule(tx.QueryRow(ctx, qualificationScheduleSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3`+clause, workspaceID, deploymentID, releaseID))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationRefreshSchedule{}, ErrQualificationScheduleNotFound
	}
	return schedule, err
}
func queryQualificationScheduleByIdempotencyTx(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, key string, lock bool) (contracts.QualificationRefreshSchedule, error) {
	clause := ""
	if lock {
		clause = " FOR UPDATE"
	}
	schedule, err := scanQualificationSchedule(tx.QueryRow(ctx, qualificationScheduleSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`+clause, workspaceID, deploymentID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationRefreshSchedule{}, ErrQualificationScheduleNotFound
	}
	return schedule, err
}
func lockQualificationScheduleTx(ctx context.Context, tx pgx.Tx, workspaceID, scheduleID string) (contracts.QualificationRefreshSchedule, error) {
	if strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(scheduleID) == "" {
		return contracts.QualificationRefreshSchedule{}, ErrQualificationScheduleNotFound
	}
	return scanQualificationSchedule(tx.QueryRow(ctx, qualificationScheduleSelect()+` WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, workspaceID, scheduleID))
}
func queryQualificationScheduleAttemptByIdempotencyTx(ctx context.Context, tx pgx.Tx, scheduleID, key string, lock bool) (contracts.QualificationRefreshScheduleAttempt, error) {
	clause := ""
	if lock {
		clause = " FOR UPDATE"
	}
	attempt, err := scanQualificationScheduleAttempt(tx.QueryRow(ctx, qualificationScheduleAttemptSelect()+` WHERE schedule_id=$1 AND idempotency_key=$2`+clause, scheduleID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationRefreshScheduleAttempt{}, ErrQualificationScheduleAttempt
	}
	return attempt, err
}

func appendQualificationScheduleEventTx(ctx context.Context, tx pgx.Tx, schedule contracts.QualificationRefreshSchedule, event, ownerID string, fence uint64, attemptID string, actor contracts.AuditActor, metadata map[string]string, requestID, idempotency, causation, correlation string) error {
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if len(metadataJSON) > contracts.MaxQualificationRefreshScheduleEventsMetadata {
		return fmt.Errorf("qualification schedule event metadata is too large")
	}
	_, err = tx.Exec(ctx, `INSERT INTO fornix.qualification_refresh_schedule_events(workspace_id,schedule_id,deployment_id,release_id,attempt_id,event,owner_id,fence,actor,metadata,request_id,idempotency_key,causation_id,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11,$12,$13,$14)`, schedule.WorkspaceID, schedule.ID, schedule.DeploymentID, schedule.ReleaseID, nullableString(attemptID), event, nullableString(ownerID), nullableUint64(fence), actorJSON, metadataJSON, nullableString(requestID), nullableString(idempotency), nullableString(causation), nullableString(correlation))
	return err
}

func qualificationScheduleSelect() string {
	return `SELECT id,workspace_id,deployment_id,release_id,config_hash,required_evidence_kinds,required_recovery_drills,interval_ms,freshness_ms,max_attempts,backoff_base_ms,backoff_max_ms,status,next_due_at,attempt_count,retry_count,last_as_of,COALESCE(last_attempt_id,''),COALESCE(last_report_id,''),COALESCE(last_report_hash,''),COALESCE(last_outcome,''),COALESCE(last_error_code,''),COALESCE(lease_owner_id,''),lease_fence,lease_until,created_at,updated_at FROM fornix.qualification_refresh_schedules`
}
func qualificationScheduleAttemptSelect() string {
	return `SELECT id,schedule_id,workspace_id,deployment_id,release_id,attempt_number,owner_id,fence,as_of,plan_hash,COALESCE(refresh_id,''),COALESCE(refresh_hash,''),outcome,retryable,COALESCE(error_code,''),retry_at,idempotency_key,created_at FROM fornix.qualification_refresh_schedule_attempts`
}

func scanQualificationSchedule(row pgx.Row) (contracts.QualificationRefreshSchedule, error) {
	var schedule contracts.QualificationRefreshSchedule
	var evidenceRaw, drillsRaw []byte
	var nextDue, lastAsOf, leaseUntil *time.Time
	err := row.Scan(&schedule.ID, &schedule.WorkspaceID, &schedule.DeploymentID, &schedule.ReleaseID, &schedule.ConfigHash, &evidenceRaw, &drillsRaw, &schedule.IntervalMS, &schedule.FreshnessMS, &schedule.MaxAttempts, &schedule.BackoffBaseMS, &schedule.BackoffMaxMS, &schedule.Status, &nextDue, &schedule.AttemptCount, &schedule.RetryCount, &lastAsOf, &schedule.LastAttemptID, &schedule.LastReportID, &schedule.LastReportHash, &schedule.LastOutcome, &schedule.LastErrorCode, &schedule.LeaseOwnerID, &schedule.LeaseFence, &leaseUntil, &schedule.CreatedAt, &schedule.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.QualificationRefreshSchedule{}, pgx.ErrNoRows
		}
		return contracts.QualificationRefreshSchedule{}, err
	}
	if err := json.Unmarshal(evidenceRaw, &schedule.RequiredEvidenceKinds); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	if err := json.Unmarshal(drillsRaw, &schedule.RequiredRecoveryDrills); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	schedule.NextDueAt, schedule.LastAsOf, schedule.LeaseUntil = nextDue, lastAsOf, leaseUntil
	schedule.SchemaVersion = contracts.QualificationRefreshScheduleSchemaVersion
	if err := contracts.ValidateQualificationRefreshScheduleStatus(schedule.Status); err != nil {
		return contracts.QualificationRefreshSchedule{}, err
	}
	if len(schedule.RequiredEvidenceKinds) == 0 {
		return contracts.QualificationRefreshSchedule{}, fmt.Errorf("stored qualification schedule has no evidence kinds")
	}
	return schedule, nil
}
func scanQualificationScheduleRows(rows pgx.Rows) (contracts.QualificationRefreshSchedule, error) {
	var schedule contracts.QualificationRefreshSchedule
	var evidenceRaw, drillsRaw []byte
	var nextDue, lastAsOf, leaseUntil *time.Time
	err := rows.Scan(&schedule.ID, &schedule.WorkspaceID, &schedule.DeploymentID, &schedule.ReleaseID, &schedule.ConfigHash, &evidenceRaw, &drillsRaw, &schedule.IntervalMS, &schedule.FreshnessMS, &schedule.MaxAttempts, &schedule.BackoffBaseMS, &schedule.BackoffMaxMS, &schedule.Status, &nextDue, &schedule.AttemptCount, &schedule.RetryCount, &lastAsOf, &schedule.LastAttemptID, &schedule.LastReportID, &schedule.LastReportHash, &schedule.LastOutcome, &schedule.LastErrorCode, &schedule.LeaseOwnerID, &schedule.LeaseFence, &leaseUntil, &schedule.CreatedAt, &schedule.UpdatedAt)
	if err != nil {
		return schedule, err
	}
	if err := json.Unmarshal(evidenceRaw, &schedule.RequiredEvidenceKinds); err != nil {
		return schedule, err
	}
	if err := json.Unmarshal(drillsRaw, &schedule.RequiredRecoveryDrills); err != nil {
		return schedule, err
	}
	schedule.NextDueAt, schedule.LastAsOf, schedule.LeaseUntil = nextDue, lastAsOf, leaseUntil
	schedule.SchemaVersion = contracts.QualificationRefreshScheduleSchemaVersion
	return schedule, nil
}
func scanQualificationScheduleAttempt(row pgx.Row) (contracts.QualificationRefreshScheduleAttempt, error) {
	var attempt contracts.QualificationRefreshScheduleAttempt
	var retryAt *time.Time
	err := row.Scan(&attempt.ID, &attempt.ScheduleID, &attempt.WorkspaceID, &attempt.DeploymentID, &attempt.ReleaseID, &attempt.AttemptNumber, &attempt.OwnerID, &attempt.Fence, &attempt.AsOf, &attempt.PlanHash, &attempt.RefreshID, &attempt.RefreshHash, &attempt.Outcome, &attempt.Retryable, &attempt.ErrorCode, &retryAt, &attempt.IdempotencyKey, &attempt.CreatedAt)
	attempt.RetryAt = retryAt
	attempt.SchemaVersion = contracts.QualificationRefreshScheduleSchemaVersion
	return attempt, err
}
func scanQualificationScheduleAttemptRows(rows pgx.Rows) (contracts.QualificationRefreshScheduleAttempt, error) {
	var attempt contracts.QualificationRefreshScheduleAttempt
	var retryAt *time.Time
	err := rows.Scan(&attempt.ID, &attempt.ScheduleID, &attempt.WorkspaceID, &attempt.DeploymentID, &attempt.ReleaseID, &attempt.AttemptNumber, &attempt.OwnerID, &attempt.Fence, &attempt.AsOf, &attempt.PlanHash, &attempt.RefreshID, &attempt.RefreshHash, &attempt.Outcome, &attempt.Retryable, &attempt.ErrorCode, &retryAt, &attempt.IdempotencyKey, &attempt.CreatedAt)
	attempt.RetryAt = retryAt
	attempt.SchemaVersion = contracts.QualificationRefreshScheduleSchemaVersion
	return attempt, err
}
func qualificationScheduleAttemptID(scheduleID string, number int) string {
	return "qualification-attempt-" + contracts.HashStrings(scheduleID, strconv.Itoa(number))[:32]
}
func scheduleNow(now time.Time) time.Time {
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now.UTC()
}
func normalizeScheduleLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = time.Duration(contracts.DefaultQualificationRefreshScheduleLeaseMS) * time.Millisecond
	}
	max := time.Duration(contracts.MaxQualificationRefreshScheduleLeaseMS) * time.Millisecond
	if ttl > max {
		ttl = max
	}
	if ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	return ttl
}
func timePtr(value time.Time) *time.Time { value = value.UTC(); return &value }
func nullableUint64(value uint64) any {
	if value == 0 {
		return nil
	}
	return int64(value)
}
