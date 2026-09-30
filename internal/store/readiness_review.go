package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrReadinessPolicyNotFound = errors.New("readiness freshness policy not found")
	ErrReadinessPolicyConflict = errors.New("readiness freshness policy conflicts with existing state")
	ErrReadinessPolicyCursor   = errors.New("invalid readiness freshness policy cursor")
)

// PublishFreshnessPolicy adds one immutable version to the review-policy
// history. It is deliberately separate from release admission configuration.
func (s *ReadinessStore) PublishFreshnessPolicy(ctx context.Context, request contracts.ReadinessFreshnessPolicyRequest, now time.Time) (contracts.ReadinessFreshnessPolicy, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.ReadinessFreshnessPolicy{}, false, fmt.Errorf("readiness store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := request.Normalize(); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, false, err
	}
	requestHash := freshnessPolicyRequestHash(request)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.ReadinessFreshnessPolicy{}, false, fmt.Errorf("begin freshness policy publication: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-readiness-policy:' || $1 || ':' || $2, 0))`, request.WorkspaceID, request.DeploymentID); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, false, fmt.Errorf("lock freshness policy scope: %w", err)
	}
	if existing, existingErr := queryFreshnessPolicyByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true); existingErr == nil {
		if existingRequestHashForPolicy(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey) != requestHash {
			return contracts.ReadinessFreshnessPolicy{}, false, ErrReadinessPolicyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ReadinessFreshnessPolicy{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrReadinessPolicyNotFound) {
		return contracts.ReadinessFreshnessPolicy{}, false, existingErr
	}
	var revision int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(revision),0) FROM fornix.qualification_readiness_policies WHERE workspace_id=$1 AND deployment_id=$2`, request.WorkspaceID, request.DeploymentID).Scan(&revision); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, false, fmt.Errorf("read freshness policy revision: %w", err)
	}
	policy := contracts.ReadinessFreshnessPolicy{
		SchemaVersion: contracts.ReadinessFreshnessSchemaVersion, ID: contracts.NewID("readiness-policy"),
		WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, Revision: revision + 1,
		MaxAgeSeconds: request.MaxAgeSeconds, RequireReady: request.RequireReady, Actor: request.Actor,
		CreatedAt: now.UTC(), RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey,
		CausationID: request.CausationID, CorrelationID: request.CorrelationID,
	}
	policy.PolicyHash = policy.StableHash()
	if err := policy.Normalize(); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, false, err
	}
	if existing, existingErr := queryFreshnessPolicyByHash(ctx, tx, policy.WorkspaceID, policy.DeploymentID, policy.PolicyHash, true); existingErr == nil {
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return existing, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ReadinessFreshnessPolicy{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrReadinessPolicyNotFound) {
		return contracts.ReadinessFreshnessPolicy{}, false, existingErr
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		policy.ID = "dry-run-" + policy.PolicyHash[:16]
		return policy, false, nil
	}
	actorJSON, err := json.Marshal(policy.Actor)
	if err != nil {
		return contracts.ReadinessFreshnessPolicy{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_readiness_policies(id,workspace_id,deployment_id,revision,max_age_seconds,require_ready,policy_hash,actor,request_id,idempotency_key,request_hash,causation_id,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13)`, policy.ID, policy.WorkspaceID, policy.DeploymentID, policy.Revision, policy.MaxAgeSeconds, policy.RequireReady, policy.PolicyHash, actorJSON, nullableString(policy.RequestID), policy.IdempotencyKey, requestHash, nullableString(policy.CausationID), nullableString(policy.CorrelationID)); err != nil {
		if isUniqueViolation(err) {
			return contracts.ReadinessFreshnessPolicy{}, false, ErrReadinessPolicyConflict
		}
		return contracts.ReadinessFreshnessPolicy{}, false, fmt.Errorf("insert freshness policy: %w", err)
	}
	metadata, _ := json.Marshal(map[string]any{"policy_hash": policy.PolicyHash, "revision": policy.Revision, "max_age_seconds": policy.MaxAgeSeconds, "require_ready": policy.RequireReady})
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_readiness_policy_events(workspace_id,deployment_id,policy_id,revision,event,actor,metadata,request_id,idempotency_key,causation_id,correlation_id) VALUES($1,$2,$3,$4,'published',$5::jsonb,$6::jsonb,$7,$8,$9,$10)`, policy.WorkspaceID, policy.DeploymentID, policy.ID, policy.Revision, actorJSON, metadata, nullableString(policy.RequestID), policy.IdempotencyKey, nullableString(policy.CausationID), nullableString(policy.CorrelationID)); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, false, fmt.Errorf("record freshness policy publication: %w", err)
	}
	if err := s.registerQualificationRetentionMetadata(ctx, tx, policy.WorkspaceID, policy.DeploymentID, contracts.QualificationRecordPolicy, policy.ID, policy.PolicyHash, policy.CreatedAt, policy.Actor, policy.RequestID, policy.CausationID, policy.CorrelationID); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, false, fmt.Errorf("register freshness policy retention metadata: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, false, fmt.Errorf("commit freshness policy: %w", err)
	}
	return policy, true, nil
}

func (s *ReadinessStore) CurrentFreshnessPolicy(ctx context.Context, workspaceID, deploymentID string) (contracts.ReadinessFreshnessPolicy, error) {
	if s == nil || s.pool == nil {
		return contracts.ReadinessFreshnessPolicy{}, fmt.Errorf("readiness store is not configured")
	}
	workspaceID, deploymentID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID)
	if workspaceID == "" || deploymentID == "" {
		return contracts.ReadinessFreshnessPolicy{}, ErrReadinessPolicyNotFound
	}
	return queryFreshnessPolicyWithPool(ctx, s.pool, workspaceID, freshnessPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 ORDER BY revision DESC LIMIT 1`, []any{workspaceID, deploymentID})
}

func (s *ReadinessStore) ListFreshnessPolicies(ctx context.Context, workspaceID, deploymentID string, limit int, cursor string) (contracts.ReadinessFreshnessPolicyPage, error) {
	if s == nil || s.pool == nil {
		return contracts.ReadinessFreshnessPolicyPage{}, fmt.Errorf("readiness store is not configured")
	}
	limit = boundedQualificationPageLimit(limit)
	workspaceID, deploymentID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" {
		return contracts.ReadinessFreshnessPolicyPage{}, ErrReadinessPolicyNotFound
	}
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.ReadinessFreshnessPolicyPage{}, ErrReadinessPolicyCursor
	}
	page := contracts.ReadinessFreshnessPolicyPage{Items: make([]contracts.ReadinessFreshnessPolicy, 0, limit)}
	err := workspaceQueryRows(ctx, s.pool, workspaceID, freshnessPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id>$3 ORDER BY id LIMIT $4`, []any{workspaceID, deploymentID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			policy, err := scanFreshnessPolicy(rows)
			if err != nil {
				return err
			}
			page.Items = append(page.Items, policy)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.ReadinessFreshnessPolicyPage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// Review compares two existing snapshots with the current policy. It is a
// read-only transaction and intentionally does not create an audit row.
func (s *ReadinessStore) Review(ctx context.Context, request contracts.ReadinessReviewRequest, asOf time.Time) (contracts.ReadinessReview, error) {
	if s == nil || s.pool == nil {
		return contracts.ReadinessReview{}, fmt.Errorf("readiness store is not configured")
	}
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}
	if err := request.Normalize(); err != nil {
		return contracts.ReadinessReview{}, err
	}
	if !request.AsOf.IsZero() {
		asOf = request.AsOf
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.ReadinessReview{}, fmt.Errorf("begin readiness review: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	policy, err := queryCurrentFreshnessPolicy(ctx, tx, request.WorkspaceID, request.DeploymentID)
	if err != nil {
		return contracts.ReadinessReview{}, err
	}
	left, err := queryReadinessSnapshot(ctx, tx, request.WorkspaceID, request.DeploymentID, request.LeftSnapshotID, false)
	if err != nil {
		return contracts.ReadinessReview{}, err
	}
	right, err := queryReadinessSnapshot(ctx, tx, request.WorkspaceID, request.DeploymentID, request.RightSnapshotID, false)
	if err != nil {
		return contracts.ReadinessReview{}, err
	}
	if left.ReleaseID != request.ReleaseID || right.ReleaseID != request.ReleaseID {
		return contracts.ReadinessReview{}, ErrReadinessSnapshotConflict
	}
	leftAge, leftFresh, leftReasons := freshnessAt(left, policy, asOf, "left")
	rightAge, rightFresh, rightReasons := freshnessAt(right, policy, asOf, "right")
	staleReasons := append(leftReasons, rightReasons...)
	if policy.RequireReady {
		if !left.Ready {
			staleReasons = append(staleReasons, "left_snapshot_not_ready")
		}
		if !right.Ready {
			staleReasons = append(staleReasons, "right_snapshot_not_ready")
		}
	}
	sort.Strings(staleReasons)
	review := contracts.ReadinessReview{
		SchemaVersion: contracts.ReadinessFreshnessSchemaVersion, WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, ReleaseID: request.ReleaseID,
		LeftSnapshotID: left.ID, RightSnapshotID: right.ID, LeftSnapshotHash: left.SnapshotHash, RightSnapshotHash: right.SnapshotHash,
		PolicyID: policy.ID, PolicyRevision: policy.Revision, PolicyHash: policy.PolicyHash, MaxAgeSeconds: policy.MaxAgeSeconds,
		AsOf: asOf.UTC(), LeftEvaluatedAt: left.EvaluatedAt, RightEvaluatedAt: right.EvaluatedAt, LeftAgeSeconds: leftAge, RightAgeSeconds: rightAge,
		LeftFresh: leftFresh, RightFresh: rightFresh, LeftReady: left.Ready, RightReady: right.Ready, GateChanged: left.GateHash != right.GateHash,
		EvidenceAdded: difference(right.ActiveEvidenceIDs, left.ActiveEvidenceIDs), EvidenceRemoved: difference(left.ActiveEvidenceIDs, right.ActiveEvidenceIDs),
		BlockedAdded: difference(right.BlockedReasons, left.BlockedReasons), BlockedResolved: difference(left.BlockedReasons, right.BlockedReasons), StaleReasons: staleReasons,
		Outcome: readinessReviewOutcome(left, right, leftFresh, rightFresh, staleReasons), EvaluatedBy: request.Actor,
	}
	review.ReviewHash = review.StableHash()
	if err := review.Normalize(); err != nil {
		return contracts.ReadinessReview{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ReadinessReview{}, fmt.Errorf("commit readiness review read: %w", err)
	}
	return review, nil
}

func freshnessAt(snapshot contracts.ReadinessSnapshot, policy contracts.ReadinessFreshnessPolicy, asOf time.Time, side string) (int64, bool, []string) {
	asOf = asOf.UTC()
	if asOf.Before(snapshot.EvaluatedAt.UTC()) {
		return 0, false, []string{side + "_snapshot_in_future"}
	}
	age := int64(asOf.Sub(snapshot.EvaluatedAt.UTC()) / time.Second)
	if age > policy.MaxAgeSeconds {
		return age, false, []string{side + "_snapshot_stale"}
	}
	return age, true, nil
}

func readinessReviewOutcome(left, right contracts.ReadinessSnapshot, leftFresh, rightFresh bool, staleReasons []string) string {
	if len(staleReasons) > 0 || !leftFresh || !rightFresh {
		return contracts.ReadinessReviewStale
	}
	if left.Ready == right.Ready && left.GateHash == right.GateHash && len(difference(left.ActiveEvidenceIDs, right.ActiveEvidenceIDs)) == 0 && len(difference(left.BlockedReasons, right.BlockedReasons)) == 0 {
		return contracts.ReadinessReviewUnchanged
	}
	if !left.Ready && right.Ready {
		return contracts.ReadinessReviewImproved
	}
	if left.Ready && !right.Ready {
		return contracts.ReadinessReviewDegraded
	}
	return contracts.ReadinessReviewChanged
}

func difference(left, right []string) []string {
	rightSet := make(map[string]struct{}, len(right))
	for _, value := range right {
		rightSet[value] = struct{}{}
	}
	result := make([]string, 0)
	for _, value := range left {
		if _, exists := rightSet[value]; !exists {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func freshnessPolicyRequestHash(request contracts.ReadinessFreshnessPolicyRequest) string {
	return contracts.HashStrings("readiness-freshness-policy-request", request.WorkspaceID, request.DeploymentID, fmt.Sprint(request.MaxAgeSeconds), fmt.Sprint(request.RequireReady))
}

func existingRequestHashForPolicy(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, idempotency string) string {
	var value string
	_ = tx.QueryRow(ctx, `SELECT request_hash FROM fornix.qualification_readiness_policies WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`, workspaceID, deploymentID, idempotency).Scan(&value)
	return value
}

func freshnessPolicySelect() string {
	return `SELECT id,workspace_id,deployment_id,revision,max_age_seconds,require_ready,policy_hash,actor,COALESCE(request_id,''),idempotency_key,causation_id,correlation_id,created_at FROM fornix.qualification_readiness_policies`
}

func queryFreshnessPolicyByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, idempotency string, lock bool) (contracts.ReadinessFreshnessPolicy, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	return scanFreshnessPolicyOrNotFound(tx.QueryRow(ctx, freshnessPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`+lockClause, workspaceID, deploymentID, idempotency))
}

func queryFreshnessPolicyByHash(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, hash string, lock bool) (contracts.ReadinessFreshnessPolicy, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	return scanFreshnessPolicyOrNotFound(tx.QueryRow(ctx, freshnessPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND policy_hash=$3`+lockClause, workspaceID, deploymentID, hash))
}

func queryCurrentFreshnessPolicy(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID string) (contracts.ReadinessFreshnessPolicy, error) {
	return scanFreshnessPolicyOrNotFound(tx.QueryRow(ctx, freshnessPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 ORDER BY revision DESC LIMIT 1`, workspaceID, deploymentID))
}

func queryFreshnessPolicyWithPool(ctx context.Context, pool *pgxpool.Pool, workspaceID, query string, args []any) (contracts.ReadinessFreshnessPolicy, error) {
	var result contracts.ReadinessFreshnessPolicy
	err := workspaceQueryRow(ctx, pool, workspaceID, query, args, func(row pgx.Row) error { var err error; result, err = scanFreshnessPolicy(row); return err })
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ReadinessFreshnessPolicy{}, ErrReadinessPolicyNotFound
	}
	return result, err
}

func scanFreshnessPolicyOrNotFound(row pgx.Row) (contracts.ReadinessFreshnessPolicy, error) {
	policy, err := scanFreshnessPolicy(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ReadinessFreshnessPolicy{}, ErrReadinessPolicyNotFound
	}
	return policy, err
}

func scanFreshnessPolicy(row interface{ Scan(...any) error }) (contracts.ReadinessFreshnessPolicy, error) {
	var policy contracts.ReadinessFreshnessPolicy
	var actorRaw []byte
	if err := row.Scan(&policy.ID, &policy.WorkspaceID, &policy.DeploymentID, &policy.Revision, &policy.MaxAgeSeconds, &policy.RequireReady, &policy.PolicyHash, &actorRaw, &policy.RequestID, &policy.IdempotencyKey, &policy.CausationID, &policy.CorrelationID, &policy.CreatedAt); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, err
	}
	if err := json.Unmarshal(actorRaw, &policy.Actor); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, fmt.Errorf("decode freshness policy actor: %w", err)
	}
	if err := policy.Normalize(); err != nil {
		return contracts.ReadinessFreshnessPolicy{}, err
	}
	return policy, nil
}
