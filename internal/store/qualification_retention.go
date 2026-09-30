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
	ErrQualificationRetentionPolicyNotFound   = errors.New("qualification retention policy not found")
	ErrQualificationRetentionMetadataNotFound = errors.New("qualification retention metadata not found")
	ErrQualificationRetentionConflict         = errors.New("qualification retention state conflicts with existing state")
	ErrQualificationRetentionCursor           = errors.New("invalid qualification retention cursor")
)

type retentionSourceRow struct {
	kind, id, hash string
	createdAt      time.Time
}

// PublishQualificationRetentionPolicy adds one immutable retention revision.
// It controls external archival planning only; it cannot delete history or
// participate in release admission.
func (s *ReadinessStore) PublishQualificationRetentionPolicy(ctx context.Context, request contracts.QualificationRetentionPolicyRequest, now time.Time) (contracts.QualificationRetentionPolicy, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRetentionPolicy{}, false, fmt.Errorf("readiness store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := request.Normalize(); err != nil {
		return contracts.QualificationRetentionPolicy{}, false, err
	}
	requestHash := qualificationRetentionPolicyRequestHash(request)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.QualificationRetentionPolicy{}, false, fmt.Errorf("begin qualification retention policy: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-qualification-retention:' || $1 || ':' || $2, 0))`, request.WorkspaceID, request.DeploymentID); err != nil {
		return contracts.QualificationRetentionPolicy{}, false, fmt.Errorf("lock qualification retention policy: %w", err)
	}
	if existing, existingErr := queryQualificationRetentionPolicyByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true); existingErr == nil {
		if existingQualificationRetentionRequestHash(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey) != requestHash {
			return contracts.QualificationRetentionPolicy{}, false, ErrQualificationRetentionConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationRetentionPolicy{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrQualificationRetentionPolicyNotFound) {
		return contracts.QualificationRetentionPolicy{}, false, existingErr
	}
	var revision int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(revision),0) FROM fornix.qualification_retention_policies WHERE workspace_id=$1 AND deployment_id=$2`, request.WorkspaceID, request.DeploymentID).Scan(&revision); err != nil {
		return contracts.QualificationRetentionPolicy{}, false, fmt.Errorf("read qualification retention policy revision: %w", err)
	}
	policy := contracts.QualificationRetentionPolicy{
		SchemaVersion: contracts.QualificationRetentionSchemaVersion, ID: contracts.NewID("qualification-retention-policy"),
		WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, Revision: revision + 1,
		SnapshotRetentionSeconds: request.SnapshotRetentionSeconds, IncidentRetentionSeconds: request.IncidentRetentionSeconds,
		PolicyRetentionSeconds: request.PolicyRetentionSeconds, KeepLatestSnapshots: request.KeepLatestSnapshots,
		KeepLatestIncidents: request.KeepLatestIncidents, ProtectIncidents: request.ProtectIncidents,
		Actor: request.Actor, CreatedAt: now.UTC(), RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey,
		CausationID: request.CausationID, CorrelationID: request.CorrelationID,
	}
	policy.PolicyHash = policy.StableHash()
	if err := policy.Normalize(); err != nil {
		return contracts.QualificationRetentionPolicy{}, false, err
	}
	if existing, existingErr := queryQualificationRetentionPolicyByHash(ctx, tx, policy.WorkspaceID, policy.DeploymentID, policy.PolicyHash, true); existingErr == nil {
		if request.DryRun {
			return existing, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationRetentionPolicy{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrQualificationRetentionPolicyNotFound) {
		return contracts.QualificationRetentionPolicy{}, false, existingErr
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		policy.ID = "dry-run-" + policy.PolicyHash[:16]
		return policy, false, nil
	}
	actorJSON, err := json.Marshal(policy.Actor)
	if err != nil {
		return contracts.QualificationRetentionPolicy{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_retention_policies(id,workspace_id,deployment_id,revision,snapshot_retention_seconds,incident_retention_seconds,policy_retention_seconds,keep_latest_snapshots,keep_latest_incidents,protect_incidents,policy_hash,actor,request_id,idempotency_key,request_hash,causation_id,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14,$15,$16,$17)`, policy.ID, policy.WorkspaceID, policy.DeploymentID, policy.Revision, policy.SnapshotRetentionSeconds, policy.IncidentRetentionSeconds, policy.PolicyRetentionSeconds, policy.KeepLatestSnapshots, policy.KeepLatestIncidents, policy.ProtectIncidents, policy.PolicyHash, actorJSON, nullableString(policy.RequestID), policy.IdempotencyKey, requestHash, nullableString(policy.CausationID), nullableString(policy.CorrelationID)); err != nil {
		if isUniqueViolation(err) {
			return contracts.QualificationRetentionPolicy{}, false, ErrQualificationRetentionConflict
		}
		return contracts.QualificationRetentionPolicy{}, false, fmt.Errorf("insert qualification retention policy: %w", err)
	}
	metadata, _ := json.Marshal(map[string]any{"policy_hash": policy.PolicyHash, "revision": policy.Revision, "snapshot_retention_seconds": policy.SnapshotRetentionSeconds, "incident_retention_seconds": policy.IncidentRetentionSeconds, "policy_retention_seconds": policy.PolicyRetentionSeconds})
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_retention_events(workspace_id,deployment_id,record_kind,record_id,event,actor,metadata,request_id,idempotency_key,causation_id,correlation_id) VALUES($1,$2,'retention_policy',$3,'policy_published',$4::jsonb,$5::jsonb,$6,$7,$8,$9)`, policy.WorkspaceID, policy.DeploymentID, policy.ID, actorJSON, metadata, nullableString(policy.RequestID), policy.IdempotencyKey, nullableString(policy.CausationID), nullableString(policy.CorrelationID)); err != nil {
		return contracts.QualificationRetentionPolicy{}, false, fmt.Errorf("record qualification retention policy publication: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRetentionPolicy{}, false, fmt.Errorf("commit qualification retention policy: %w", err)
	}
	return policy, true, nil
}

func (s *ReadinessStore) CurrentQualificationRetentionPolicy(ctx context.Context, workspaceID, deploymentID string) (contracts.QualificationRetentionPolicy, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRetentionPolicy{}, fmt.Errorf("readiness store is not configured")
	}
	workspaceID, deploymentID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID)
	if workspaceID == "" || deploymentID == "" {
		return contracts.QualificationRetentionPolicy{}, ErrQualificationRetentionPolicyNotFound
	}
	return queryQualificationRetentionPolicyWithPool(ctx, s.pool, workspaceID, qualificationRetentionPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 ORDER BY revision DESC LIMIT 1`, []any{workspaceID, deploymentID})
}

func (s *ReadinessStore) ListQualificationRetentionPolicies(ctx context.Context, workspaceID, deploymentID string, limit int, cursor string) (contracts.QualificationRetentionPolicyPage, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRetentionPolicyPage{}, fmt.Errorf("readiness store is not configured")
	}
	limit = normalizeQualificationRetentionBatch(limit)
	workspaceID, deploymentID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" {
		return contracts.QualificationRetentionPolicyPage{}, ErrQualificationRetentionPolicyNotFound
	}
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.QualificationRetentionPolicyPage{}, ErrQualificationRetentionCursor
	}
	page := contracts.QualificationRetentionPolicyPage{Items: make([]contracts.QualificationRetentionPolicy, 0, limit)}
	err := workspaceQueryRows(ctx, s.pool, workspaceID, qualificationRetentionPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id>$3 ORDER BY id LIMIT $4`, []any{workspaceID, deploymentID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			policy, err := scanQualificationRetentionPolicy(rows)
			if err != nil {
				return err
			}
			page.Items = append(page.Items, policy)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.QualificationRetentionPolicyPage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// SyncQualificationRetentionMetadata fills only missing overlay rows. It is
// bounded, cursor-based, and safe to replay concurrently.
func (s *ReadinessStore) SyncQualificationRetentionMetadata(ctx context.Context, request contracts.QualificationRetentionSyncRequest, now time.Time) (contracts.QualificationRetentionSyncResult, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRetentionSyncResult{}, fmt.Errorf("readiness store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := request.Normalize(); err != nil {
		return contracts.QualificationRetentionSyncResult{}, err
	}
	batch := normalizeQualificationRetentionBatch(request.BatchSize)
	result := contracts.QualificationRetentionSyncResult{WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, Cursor: request.Cursor, BatchSize: batch, DryRun: request.DryRun}
	cursorKind, cursorID, err := parseQualificationRetentionCursor(request.Cursor)
	if err != nil {
		return contracts.QualificationRetentionSyncResult{}, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.QualificationRetentionSyncResult{}, fmt.Errorf("begin qualification retention metadata sync: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	policy, _, err := currentQualificationRetentionPolicyOrDefault(ctx, tx, request.WorkspaceID, request.DeploymentID)
	if err != nil {
		return contracts.QualificationRetentionSyncResult{}, err
	}
	rows, err := tx.Query(ctx, `SELECT record_kind,record_id,record_hash,created_at FROM (
SELECT 'readiness_snapshot'::text AS record_kind,id AS record_id,snapshot_hash AS record_hash,created_at FROM fornix.qualification_readiness_snapshots WHERE workspace_id=$1 AND deployment_id=$2
UNION ALL
SELECT 'incident_annotation'::text,id,annotation_hash,created_at FROM fornix.qualification_incident_annotations WHERE workspace_id=$1 AND deployment_id=$2
UNION ALL
SELECT 'freshness_policy'::text,id,policy_hash,created_at FROM fornix.qualification_readiness_policies WHERE workspace_id=$1 AND deployment_id=$2
) source WHERE (record_kind>$3 OR (record_kind=$3 AND record_id>$4)) ORDER BY record_kind,record_id LIMIT $5`, request.WorkspaceID, request.DeploymentID, cursorKind, cursorID, batch+1)
	if err != nil {
		return contracts.QualificationRetentionSyncResult{}, fmt.Errorf("select qualification retention metadata candidates: %w", err)
	}
	var sources []retentionSourceRow
	for rows.Next() {
		var source retentionSourceRow
		if err := rows.Scan(&source.kind, &source.id, &source.hash, &source.createdAt); err != nil {
			rows.Close()
			return contracts.QualificationRetentionSyncResult{}, err
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return contracts.QualificationRetentionSyncResult{}, err
	}
	rows.Close()
	for _, source := range sources {
		result.Examined++
		result.NextCursor = qualificationRetentionCursor(source.kind, source.id)
		metadata, metadataErr := queryQualificationRetentionMetadataByIdentity(ctx, tx, request.WorkspaceID, request.DeploymentID, source.kind, source.id, false)
		if metadataErr == nil {
			if metadata.RecordHash != source.hash {
				return contracts.QualificationRetentionSyncResult{}, ErrQualificationRetentionConflict
			}
			result.Skipped++
			continue
		}
		if !errors.Is(metadataErr, ErrQualificationRetentionMetadataNotFound) {
			return contracts.QualificationRetentionSyncResult{}, metadataErr
		}
		result.Missing++
		if request.DryRun {
			continue
		}
		if _, _, err := registerQualificationRetentionMetadataTx(ctx, tx, request.WorkspaceID, request.DeploymentID, source.kind, source.id, source.hash, source.createdAt, policy, request.Actor, request.RequestID, request.CausationID, request.CorrelationID); err != nil {
			return contracts.QualificationRetentionSyncResult{}, err
		}
		result.Registered++
	}
	if request.DryRun {
		result.ResultHash = qualificationRetentionSyncHash(result)
		return result, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRetentionSyncResult{}, fmt.Errorf("commit qualification retention metadata sync: %w", err)
	}
	result.ResultHash = qualificationRetentionSyncHash(result)
	return result, nil
}

// PlanQualificationRetention is a read-only, hash-only archival plan. The
// source rows and metadata remain untouched.
func (s *ReadinessStore) PlanQualificationRetention(ctx context.Context, request contracts.QualificationRetentionPlanRequest, now time.Time) (contracts.QualificationRetentionPlan, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRetentionPlan{}, fmt.Errorf("readiness store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := request.Normalize(); err != nil {
		return contracts.QualificationRetentionPlan{}, err
	}
	if request.AsOf.IsZero() {
		request.AsOf = now.UTC()
	}
	batch := normalizeQualificationRetentionPlanBatch(request.BatchSize)
	request.AsOf = request.AsOf.UTC()
	cursorKind, cursorID, err := parseQualificationRetentionCursor(request.Cursor)
	if err != nil {
		return contracts.QualificationRetentionPlan{}, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.QualificationRetentionPlan{}, fmt.Errorf("begin qualification retention plan: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	policy, persisted, err := currentQualificationRetentionPolicyOrDefault(ctx, tx, request.WorkspaceID, request.DeploymentID)
	if err != nil {
		return contracts.QualificationRetentionPlan{}, err
	}
	policyID, policyRevision := policy.ID, policy.Revision
	if !persisted {
		policyID, policyRevision = "default-retention-policy", 0
	}
	rows, err := tx.Query(ctx, `SELECT q.record_kind,q.record_id,q.record_hash,q.created_at,q.latest_rank,COALESCE(m.retain_until,q.created_at + CASE q.record_kind WHEN 'readiness_snapshot' THEN ($5::bigint * interval '1 second') WHEN 'incident_annotation' THEN ($6::bigint * interval '1 second') ELSE ($7::bigint * interval '1 second') END),m.id IS NULL
FROM (
  SELECT record_kind,record_id,record_hash,created_at,ROW_NUMBER() OVER (PARTITION BY record_kind ORDER BY created_at DESC,record_id DESC) AS latest_rank FROM (
    SELECT 'readiness_snapshot'::text AS record_kind,id AS record_id,snapshot_hash AS record_hash,created_at FROM fornix.qualification_readiness_snapshots WHERE workspace_id=$1 AND deployment_id=$2
    UNION ALL
    SELECT 'incident_annotation'::text,id,annotation_hash,created_at FROM fornix.qualification_incident_annotations WHERE workspace_id=$1 AND deployment_id=$2
    UNION ALL
    SELECT 'freshness_policy'::text,id,policy_hash,created_at FROM fornix.qualification_readiness_policies WHERE workspace_id=$1 AND deployment_id=$2
  ) source
) q LEFT JOIN fornix.qualification_retention_metadata m ON m.workspace_id=$1 AND m.deployment_id=$2 AND m.record_kind=q.record_kind AND m.record_id=q.record_id
WHERE (q.record_kind>$3 OR (q.record_kind=$3 AND q.record_id>$4)) ORDER BY q.record_kind,q.record_id LIMIT $8`, request.WorkspaceID, request.DeploymentID, cursorKind, cursorID, policy.SnapshotRetentionSeconds, policy.IncidentRetentionSeconds, policy.PolicyRetentionSeconds, batch+1)
	if err != nil {
		return contracts.QualificationRetentionPlan{}, fmt.Errorf("select qualification retention plan: %w", err)
	}
	plan := contracts.QualificationRetentionPlan{SchemaVersion: contracts.QualificationRetentionSchemaVersion, WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, PolicyID: policyID, PolicyRevision: policyRevision, PolicyHash: policy.PolicyHash, AsOf: request.AsOf, Cursor: request.Cursor, BatchSize: batch, Candidates: make([]contracts.QualificationRetentionCandidate, 0, batch), EvaluatedBy: request.Actor}
	var lastKind, lastID string
	for rows.Next() {
		var source retentionSourceRow
		var latestRank int64
		var retainUntil time.Time
		var metadataMissing bool
		if err := rows.Scan(&source.kind, &source.id, &source.hash, &source.createdAt, &latestRank, &retainUntil, &metadataMissing); err != nil {
			rows.Close()
			return contracts.QualificationRetentionPlan{}, err
		}
		plan.Examined++
		lastKind, lastID = source.kind, source.id
		reason := contracts.RetentionCandidateNotDue
		if metadataMissing {
			reason = contracts.RetentionCandidateMetadataMissing
			plan.MetadataMissing++
		} else if source.kind == contracts.QualificationRecordIncident && policy.ProtectIncidents {
			reason = contracts.RetentionCandidateProtectedIncident
			plan.ProtectedIncident++
		} else if (source.kind == contracts.QualificationRecordSnapshot && latestRank <= int64(policy.KeepLatestSnapshots)) || (source.kind == contracts.QualificationRecordIncident && latestRank <= int64(policy.KeepLatestIncidents)) || (source.kind == contracts.QualificationRecordPolicy && latestRank <= 1) {
			reason = contracts.RetentionCandidateProtectedLatest
			plan.ProtectedLatest++
		} else if !retainUntil.After(request.AsOf) {
			reason = contracts.RetentionCandidateEligible
			plan.Eligible++
		} else {
			plan.NotDue++
		}
		plan.Candidates = append(plan.Candidates, contracts.QualificationRetentionCandidate{RecordKind: source.kind, RecordID: source.id, RecordHash: source.hash, CreatedAt: source.createdAt, RetainUntil: retainUntil, Reason: reason})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return contracts.QualificationRetentionPlan{}, err
	}
	rows.Close()
	if plan.Examined > batch {
		plan.NextCursor = qualificationRetentionCursor(lastKind, lastID)
		plan.Candidates = plan.Candidates[:batch]
	}
	plan.PlanHash = plan.StableHash()
	if err := plan.Normalize(); err != nil {
		return contracts.QualificationRetentionPlan{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRetentionPlan{}, fmt.Errorf("commit qualification retention plan read: %w", err)
	}
	return plan, nil
}

// QualificationRecoveryReport is a bounded read-only integrity check over
// qualification authority links and their retention overlay.
func (s *ReadinessStore) QualificationRecoveryReport(ctx context.Context, request contracts.QualificationRecoveryReportRequest, now time.Time) (contracts.QualificationRecoveryReport, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRecoveryReport{}, fmt.Errorf("readiness store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := request.Normalize(); err != nil {
		return contracts.QualificationRecoveryReport{}, err
	}
	if request.AsOf.IsZero() {
		request.AsOf = now.UTC()
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.QualificationRecoveryReport{}, fmt.Errorf("begin qualification recovery report: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	report := contracts.QualificationRecoveryReport{SchemaVersion: contracts.QualificationRetentionSchemaVersion, WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, AsOf: request.AsOf.UTC(), EvaluatedBy: request.Actor}
	for query, target := range map[string]*int64{
		`SELECT count(*) FROM fornix.qualification_readiness_snapshots WHERE workspace_id=$1 AND deployment_id=$2`:  &report.SnapshotCount,
		`SELECT count(*) FROM fornix.qualification_incident_annotations WHERE workspace_id=$1 AND deployment_id=$2`: &report.IncidentCount,
		`SELECT count(*) FROM fornix.qualification_readiness_policies WHERE workspace_id=$1 AND deployment_id=$2`:   &report.PolicyCount,
		`SELECT count(*) FROM fornix.qualification_retention_metadata WHERE workspace_id=$1 AND deployment_id=$2`:   &report.MetadataCount,
	} {
		if err := tx.QueryRow(ctx, query, request.WorkspaceID, request.DeploymentID).Scan(target); err != nil {
			return contracts.QualificationRecoveryReport{}, err
		}
	}
	queries := []struct {
		target *int64
		query  string
	}{
		{&report.MetadataMissingCount, `SELECT count(*) FROM (SELECT 'readiness_snapshot'::text AS record_kind,id AS record_id FROM fornix.qualification_readiness_snapshots WHERE workspace_id=$1 AND deployment_id=$2 UNION ALL SELECT 'incident_annotation'::text,id FROM fornix.qualification_incident_annotations WHERE workspace_id=$1 AND deployment_id=$2 UNION ALL SELECT 'freshness_policy'::text,id FROM fornix.qualification_readiness_policies WHERE workspace_id=$1 AND deployment_id=$2) source LEFT JOIN fornix.qualification_retention_metadata m ON m.workspace_id=$1 AND m.deployment_id=$2 AND m.record_kind=source.record_kind AND m.record_id=source.record_id WHERE m.id IS NULL`},
		{&report.DanglingMetadataCount, `SELECT count(*) FROM fornix.qualification_retention_metadata m WHERE m.workspace_id=$1 AND m.deployment_id=$2 AND NOT ((m.record_kind='readiness_snapshot' AND EXISTS (SELECT 1 FROM fornix.qualification_readiness_snapshots s WHERE s.workspace_id=m.workspace_id AND s.deployment_id=m.deployment_id AND s.id=m.record_id)) OR (m.record_kind='incident_annotation' AND EXISTS (SELECT 1 FROM fornix.qualification_incident_annotations a WHERE a.workspace_id=m.workspace_id AND a.deployment_id=m.deployment_id AND a.id=m.record_id)) OR (m.record_kind='freshness_policy' AND EXISTS (SELECT 1 FROM fornix.qualification_readiness_policies p WHERE p.workspace_id=m.workspace_id AND p.deployment_id=m.deployment_id AND p.id=m.record_id)))`},
		{&report.HashMismatchCount, `SELECT count(*) FROM (SELECT m.record_hash, s.snapshot_hash AS source_hash FROM fornix.qualification_retention_metadata m JOIN fornix.qualification_readiness_snapshots s ON s.workspace_id=m.workspace_id AND s.deployment_id=m.deployment_id AND s.id=m.record_id WHERE m.workspace_id=$1 AND m.deployment_id=$2 AND m.record_kind='readiness_snapshot' UNION ALL SELECT m.record_hash, a.annotation_hash FROM fornix.qualification_retention_metadata m JOIN fornix.qualification_incident_annotations a ON a.workspace_id=m.workspace_id AND a.deployment_id=m.deployment_id AND a.id=m.record_id WHERE m.workspace_id=$1 AND m.deployment_id=$2 AND m.record_kind='incident_annotation' UNION ALL SELECT m.record_hash, p.policy_hash FROM fornix.qualification_retention_metadata m JOIN fornix.qualification_readiness_policies p ON p.workspace_id=m.workspace_id AND p.deployment_id=m.deployment_id AND p.id=m.record_id WHERE m.workspace_id=$1 AND m.deployment_id=$2 AND m.record_kind='freshness_policy') hashes WHERE record_hash<>source_hash`},
		{&report.DanglingAnnotationCount, `SELECT count(*) FROM fornix.qualification_incident_annotations a WHERE a.workspace_id=$1 AND a.deployment_id=$2 AND NOT EXISTS (SELECT 1 FROM fornix.qualification_readiness_snapshots s WHERE s.workspace_id=a.workspace_id AND s.deployment_id=a.deployment_id AND s.id=a.snapshot_id AND s.snapshot_hash=a.snapshot_hash)`},
		{&report.EventReferenceMismatchCount, `SELECT count(*) FROM fornix.qualification_readiness_snapshot_events e WHERE e.workspace_id=$1 AND e.deployment_id=$2 AND ((e.event='captured' AND NOT EXISTS (SELECT 1 FROM fornix.qualification_readiness_snapshots s WHERE s.workspace_id=e.workspace_id AND s.deployment_id=e.deployment_id AND s.id=e.snapshot_id)) OR (e.event='annotated' AND (NOT EXISTS (SELECT 1 FROM fornix.qualification_incident_annotations a WHERE a.workspace_id=e.workspace_id AND a.deployment_id=e.deployment_id AND a.id=e.annotation_id) OR NOT EXISTS (SELECT 1 FROM fornix.qualification_incident_annotations a WHERE a.workspace_id=e.workspace_id AND a.deployment_id=e.deployment_id AND a.id=e.annotation_id AND a.snapshot_id=e.snapshot_id)))`},
	}
	for _, item := range queries {
		if err := tx.QueryRow(ctx, item.query, request.WorkspaceID, request.DeploymentID).Scan(item.target); err != nil {
			return contracts.QualificationRecoveryReport{}, err
		}
	}
	issueCounts := []struct {
		code  string
		count int64
	}{
		{"retention_metadata_missing", report.MetadataMissingCount}, {"retention_metadata_dangling", report.DanglingMetadataCount}, {"retention_metadata_hash_mismatch", report.HashMismatchCount}, {"retention_annotation_dangling_snapshot", report.DanglingAnnotationCount}, {"retention_event_reference_mismatch", report.EventReferenceMismatchCount},
	}
	for _, issue := range issueCounts {
		if issue.count > 0 {
			report.Issues = append(report.Issues, issue.code)
		}
	}
	sort.Strings(report.Issues)
	report.Healthy = len(report.Issues) == 0
	report.ReportHash = report.StableHash()
	if err := report.Normalize(); err != nil {
		return contracts.QualificationRecoveryReport{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRecoveryReport{}, fmt.Errorf("commit qualification recovery report read: %w", err)
	}
	return report, nil
}

func registerQualificationRetentionMetadataTx(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, recordKind, recordID, recordHash string, createdAt time.Time, policy contracts.QualificationRetentionPolicy, actor contracts.AuditActor, requestID, causationID, correlationID string) (contracts.QualificationRetentionMetadata, bool, error) {
	if existing, err := queryQualificationRetentionMetadataByIdentity(ctx, tx, workspaceID, deploymentID, recordKind, recordID, true); err == nil {
		if existing.RecordHash != recordHash {
			return contracts.QualificationRetentionMetadata{}, false, ErrQualificationRetentionConflict
		}
		return existing, false, nil
	} else if !errors.Is(err, ErrQualificationRetentionMetadataNotFound) {
		return contracts.QualificationRetentionMetadata{}, false, err
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	retentionSeconds := policyRetentionSeconds(policy, recordKind)
	metadata := contracts.QualificationRetentionMetadata{SchemaVersion: contracts.QualificationRetentionSchemaVersion, ID: contracts.NewID("qualification-retention-metadata"), WorkspaceID: workspaceID, DeploymentID: deploymentID, RecordKind: recordKind, RecordID: recordID, RecordHash: recordHash, RetainUntil: createdAt.UTC().Add(time.Duration(retentionSeconds) * time.Second), Actor: actor, CreatedAt: time.Now().UTC(), RequestID: requestID, IdempotencyKey: "qualification-retention-metadata:" + recordKind + ":" + recordID, CausationID: causationID, CorrelationID: correlationID}
	if policy.Revision > 0 {
		metadata.PolicyID, metadata.PolicyRevision = policy.ID, policy.Revision
	}
	metadata.MetadataHash = metadata.StableHash()
	if err := metadata.Normalize(); err != nil {
		return contracts.QualificationRetentionMetadata{}, false, err
	}
	actorJSON, err := json.Marshal(metadata.Actor)
	if err != nil {
		return contracts.QualificationRetentionMetadata{}, false, err
	}
	command, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_retention_metadata(id,workspace_id,deployment_id,record_kind,record_id,record_hash,policy_id,policy_revision,retain_until,metadata_hash,actor,request_id,idempotency_key,causation_id,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15) ON CONFLICT (workspace_id,deployment_id,record_kind,record_id) DO NOTHING`, metadata.ID, metadata.WorkspaceID, metadata.DeploymentID, metadata.RecordKind, metadata.RecordID, metadata.RecordHash, nullableString(metadata.PolicyID), metadata.PolicyRevision, metadata.RetainUntil, metadata.MetadataHash, actorJSON, nullableString(metadata.RequestID), metadata.IdempotencyKey, nullableString(metadata.CausationID), nullableString(metadata.CorrelationID))
	if err != nil {
		return contracts.QualificationRetentionMetadata{}, false, fmt.Errorf("insert qualification retention metadata: %w", err)
	}
	if command.RowsAffected() == 0 {
		existing, err := queryQualificationRetentionMetadataByIdentity(ctx, tx, workspaceID, deploymentID, recordKind, recordID, true)
		if err != nil {
			return contracts.QualificationRetentionMetadata{}, false, err
		}
		if existing.RecordHash != recordHash {
			return contracts.QualificationRetentionMetadata{}, false, ErrQualificationRetentionConflict
		}
		return existing, false, nil
	}
	eventMetadata, _ := json.Marshal(map[string]any{"record_hash": metadata.RecordHash, "metadata_hash": metadata.MetadataHash, "retain_until": metadata.RetainUntil})
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_retention_events(workspace_id,deployment_id,record_kind,record_id,event,actor,metadata,request_id,idempotency_key,causation_id,correlation_id) VALUES($1,$2,$3,$4,'metadata_registered',$5::jsonb,$6::jsonb,$7,$8,$9,$10)`, metadata.WorkspaceID, metadata.DeploymentID, metadata.RecordKind, metadata.RecordID, actorJSON, eventMetadata, nullableString(metadata.RequestID), metadata.IdempotencyKey, nullableString(metadata.CausationID), nullableString(metadata.CorrelationID)); err != nil {
		return contracts.QualificationRetentionMetadata{}, false, fmt.Errorf("record qualification retention metadata event: %w", err)
	}
	return metadata, true, nil
}

func (s *ReadinessStore) registerQualificationRetentionMetadata(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, recordKind, recordID, recordHash string, createdAt time.Time, actor contracts.AuditActor, requestID, causationID, correlationID string) error {
	policy, _, err := currentQualificationRetentionPolicyOrDefault(ctx, tx, workspaceID, deploymentID)
	if err != nil {
		return err
	}
	_, _, err = registerQualificationRetentionMetadataTx(ctx, tx, workspaceID, deploymentID, recordKind, recordID, recordHash, createdAt, policy, actor, requestID, causationID, correlationID)
	return err
}

func currentQualificationRetentionPolicyOrDefault(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID string) (contracts.QualificationRetentionPolicy, bool, error) {
	policy, err := queryCurrentQualificationRetentionPolicy(ctx, tx, workspaceID, deploymentID)
	if err == nil {
		return policy, true, nil
	}
	if !errors.Is(err, ErrQualificationRetentionPolicyNotFound) {
		return contracts.QualificationRetentionPolicy{}, false, err
	}
	seconds := int64(contracts.DefaultQualificationRetentionDays * 24 * 60 * 60)
	return contracts.QualificationRetentionPolicy{WorkspaceID: workspaceID, DeploymentID: deploymentID, SnapshotRetentionSeconds: seconds, IncidentRetentionSeconds: seconds, PolicyRetentionSeconds: seconds, KeepLatestSnapshots: 10, KeepLatestIncidents: 10, ProtectIncidents: true, PolicyHash: contracts.HashStrings("qualification-retention-policy", workspaceID, deploymentID, fmt.Sprint(seconds), fmt.Sprint(seconds), fmt.Sprint(seconds), "10", "10", "true")}, false, nil
}

func policyRetentionSeconds(policy contracts.QualificationRetentionPolicy, kind string) int64 {
	switch kind {
	case contracts.QualificationRecordSnapshot:
		return policy.SnapshotRetentionSeconds
	case contracts.QualificationRecordIncident:
		return policy.IncidentRetentionSeconds
	default:
		return policy.PolicyRetentionSeconds
	}
}

func qualificationRetentionPolicyRequestHash(request contracts.QualificationRetentionPolicyRequest) string {
	return contracts.HashStrings("qualification-retention-policy-request", request.WorkspaceID, request.DeploymentID, fmt.Sprint(request.SnapshotRetentionSeconds), fmt.Sprint(request.IncidentRetentionSeconds), fmt.Sprint(request.PolicyRetentionSeconds), fmt.Sprint(request.KeepLatestSnapshots), fmt.Sprint(request.KeepLatestIncidents), fmt.Sprint(request.ProtectIncidents))
}

func qualificationRetentionSyncHash(result contracts.QualificationRetentionSyncResult) string {
	return contracts.HashStrings("qualification-retention-sync", result.WorkspaceID, result.DeploymentID, result.Cursor, result.NextCursor, fmt.Sprint(result.BatchSize), fmt.Sprint(result.DryRun), fmt.Sprint(result.Examined), fmt.Sprint(result.Missing), fmt.Sprint(result.Registered), fmt.Sprint(result.Skipped))
}

func qualificationRetentionPolicySelect() string {
	return `SELECT id,workspace_id,deployment_id,revision,snapshot_retention_seconds,incident_retention_seconds,policy_retention_seconds,keep_latest_snapshots,keep_latest_incidents,protect_incidents,policy_hash,actor,COALESCE(request_id,''),idempotency_key,causation_id,correlation_id,created_at FROM fornix.qualification_retention_policies`
}

func queryQualificationRetentionPolicyByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, key string, lock bool) (contracts.QualificationRetentionPolicy, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	return scanQualificationRetentionPolicyOrNotFound(tx.QueryRow(ctx, qualificationRetentionPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`+lockClause, workspaceID, deploymentID, key))
}

func queryQualificationRetentionPolicyByHash(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, hash string, lock bool) (contracts.QualificationRetentionPolicy, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	return scanQualificationRetentionPolicyOrNotFound(tx.QueryRow(ctx, qualificationRetentionPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND policy_hash=$3`+lockClause, workspaceID, deploymentID, hash))
}

func queryCurrentQualificationRetentionPolicy(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID string) (contracts.QualificationRetentionPolicy, error) {
	return scanQualificationRetentionPolicyOrNotFound(tx.QueryRow(ctx, qualificationRetentionPolicySelect()+` WHERE workspace_id=$1 AND deployment_id=$2 ORDER BY revision DESC LIMIT 1`, workspaceID, deploymentID))
}

func queryQualificationRetentionPolicyWithPool(ctx context.Context, pool *pgxpool.Pool, workspaceID, query string, args []any) (contracts.QualificationRetentionPolicy, error) {
	var policy contracts.QualificationRetentionPolicy
	err := workspaceQueryRow(ctx, pool, workspaceID, query, args, func(row pgx.Row) error {
		var err error
		policy, err = scanQualificationRetentionPolicy(row)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationRetentionPolicy{}, ErrQualificationRetentionPolicyNotFound
	}
	return policy, err
}

func scanQualificationRetentionPolicyOrNotFound(row pgx.Row) (contracts.QualificationRetentionPolicy, error) {
	policy, err := scanQualificationRetentionPolicy(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationRetentionPolicy{}, ErrQualificationRetentionPolicyNotFound
	}
	return policy, err
}

func scanQualificationRetentionPolicy(row interface{ Scan(...any) error }) (contracts.QualificationRetentionPolicy, error) {
	var policy contracts.QualificationRetentionPolicy
	var actorRaw []byte
	if err := row.Scan(&policy.ID, &policy.WorkspaceID, &policy.DeploymentID, &policy.Revision, &policy.SnapshotRetentionSeconds, &policy.IncidentRetentionSeconds, &policy.PolicyRetentionSeconds, &policy.KeepLatestSnapshots, &policy.KeepLatestIncidents, &policy.ProtectIncidents, &policy.PolicyHash, &actorRaw, &policy.RequestID, &policy.IdempotencyKey, &policy.CausationID, &policy.CorrelationID, &policy.CreatedAt); err != nil {
		return contracts.QualificationRetentionPolicy{}, err
	}
	if err := json.Unmarshal(actorRaw, &policy.Actor); err != nil {
		return contracts.QualificationRetentionPolicy{}, fmt.Errorf("decode qualification retention policy actor: %w", err)
	}
	if err := policy.Normalize(); err != nil {
		return contracts.QualificationRetentionPolicy{}, err
	}
	return policy, nil
}

func existingQualificationRetentionRequestHash(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, key string) string {
	var value string
	_ = tx.QueryRow(ctx, `SELECT request_hash FROM fornix.qualification_retention_policies WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`, workspaceID, deploymentID, key).Scan(&value)
	return value
}

func queryQualificationRetentionMetadataByIdentity(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, kind, id string, lock bool) (contracts.QualificationRetentionMetadata, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	var metadata contracts.QualificationRetentionMetadata
	var actorRaw []byte
	err := tx.QueryRow(ctx, `SELECT id,workspace_id,deployment_id,record_kind,record_id,record_hash,COALESCE(policy_id,''),policy_revision,retain_until,metadata_hash,actor,COALESCE(request_id,''),idempotency_key,causation_id,correlation_id,created_at FROM fornix.qualification_retention_metadata WHERE workspace_id=$1 AND deployment_id=$2 AND record_kind=$3 AND record_id=$4`+lockClause, workspaceID, deploymentID, kind, id).Scan(&metadata.ID, &metadata.WorkspaceID, &metadata.DeploymentID, &metadata.RecordKind, &metadata.RecordID, &metadata.RecordHash, &metadata.PolicyID, &metadata.PolicyRevision, &metadata.RetainUntil, &metadata.MetadataHash, &actorRaw, &metadata.RequestID, &metadata.IdempotencyKey, &metadata.CausationID, &metadata.CorrelationID, &metadata.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationRetentionMetadata{}, ErrQualificationRetentionMetadataNotFound
	}
	if err != nil {
		return contracts.QualificationRetentionMetadata{}, err
	}
	if err := json.Unmarshal(actorRaw, &metadata.Actor); err != nil {
		return contracts.QualificationRetentionMetadata{}, fmt.Errorf("decode qualification retention metadata actor: %w", err)
	}
	if err := metadata.Normalize(); err != nil {
		return contracts.QualificationRetentionMetadata{}, err
	}
	return metadata, nil
}

func normalizeQualificationRetentionBatch(size int) int {
	if size <= 0 {
		return contracts.DefaultQualificationRetentionBatch
	}
	if size > contracts.MaxQualificationRetentionBatch {
		return contracts.MaxQualificationRetentionBatch
	}
	return size
}

func normalizeQualificationRetentionPlanBatch(size int) int {
	if size <= 0 {
		return contracts.DefaultQualificationRetentionBatch
	}
	if size > contracts.MaxQualificationRetentionCandidates {
		return contracts.MaxQualificationRetentionCandidates
	}
	return size
}

func qualificationRetentionCursor(kind, id string) string {
	return kind + ":" + id
}

func parseQualificationRetentionCursor(cursor string) (string, string, error) {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return "", "", nil
	}
	parts := strings.SplitN(cursor, ":", 2)
	if len(parts) != 2 || !validQualificationRetentionRecordKind(parts[0]) || strings.TrimSpace(parts[1]) == "" || len(parts[1]) > contracts.MaxDomainIDLength {
		return "", "", ErrQualificationRetentionCursor
	}
	return parts[0], parts[1], nil
}

func validQualificationRetentionRecordKind(kind string) bool {
	switch kind {
	case contracts.QualificationRecordSnapshot, contracts.QualificationRecordIncident, contracts.QualificationRecordPolicy:
		return true
	default:
		return false
	}
}
