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
	ErrReadinessSnapshotNotFound  = errors.New("readiness snapshot not found")
	ErrReadinessSnapshotConflict  = errors.New("readiness snapshot conflicts with existing state")
	ErrReadinessSnapshotCursor    = errors.New("invalid readiness snapshot cursor")
	ErrIncidentAnnotationNotFound = errors.New("incident annotation not found")
	ErrIncidentAnnotationConflict = errors.New("incident annotation conflicts with existing state")
	ErrIncidentAnnotationCursor   = errors.New("invalid incident annotation cursor")
)

// ReadinessStore persists advisory gate observations and bounded operator
// annotations. The DeploymentEvidenceStore remains the admission authority;
// this store only records its transactionally read result.
type ReadinessStore struct {
	pool       *pgxpool.Pool
	deployment *DeploymentEvidenceStore
}

func NewReadinessStore(pool *pgxpool.Pool, deployment *DeploymentEvidenceStore) *ReadinessStore {
	return &ReadinessStore{pool: pool, deployment: deployment}
}

// Capture evaluates the authoritative release gate and records its current
// hash-only facts. A repeated idempotency key returns the original result;
// unchanged authority facts also deduplicate by snapshot hash.
func (s *ReadinessStore) Capture(ctx context.Context, request contracts.ReadinessSnapshotRequest, now time.Time) (contracts.ReadinessSnapshot, bool, error) {
	if s == nil || s.pool == nil || s.deployment == nil {
		return contracts.ReadinessSnapshot{}, false, fmt.Errorf("readiness store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := request.Normalize(); err != nil {
		return contracts.ReadinessSnapshot{}, false, err
	}
	if len(request.RequiredKinds) == 0 {
		request.RequiredKinds = append([]string(nil), contracts.DeploymentDefaultRequiredEvidenceKinds...)
	}
	requestHash := readinessRequestHash(request)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.ReadinessSnapshot{}, false, fmt.Errorf("begin readiness capture: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-readiness:' || $1 || ':' || $2 || ':' || $3, 0))`, request.WorkspaceID, request.DeploymentID, request.ReleaseID); err != nil {
		return contracts.ReadinessSnapshot{}, false, fmt.Errorf("lock readiness scope: %w", err)
	}
	if existing, existingErr := queryReadinessByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true); existingErr == nil {
		if existingRequestHash(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey) != requestHash {
			return contracts.ReadinessSnapshot{}, false, ErrReadinessSnapshotConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ReadinessSnapshot{}, false, fmt.Errorf("commit readiness idempotency replay: %w", err)
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrReadinessSnapshotNotFound) {
		return contracts.ReadinessSnapshot{}, false, existingErr
	}
	release, err := queryDeploymentRelease(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ReleaseID, true)
	if err != nil {
		return contracts.ReadinessSnapshot{}, false, err
	}
	gate, err := evaluateDeploymentGateTx(ctx, tx, release, request.RequiredKinds, now.UTC())
	if err != nil {
		return contracts.ReadinessSnapshot{}, false, err
	}
	activeIDs := make([]string, 0, len(gate.Links))
	for _, link := range gate.Links {
		if link.Status == contracts.DeploymentEvidenceLinkActive {
			activeIDs = append(activeIDs, link.ID)
		}
	}
	sort.Strings(activeIDs)
	snapshot := contracts.ReadinessSnapshot{
		SchemaVersion: contracts.ReadinessSnapshotSchemaVersion, ID: contracts.NewID("readiness-snapshot"),
		WorkspaceID: release.WorkspaceID, DeploymentID: release.DeploymentID, ReleaseID: release.ID,
		ReleaseHash: release.ReleaseHash, TrustSnapshotRevision: release.TrustSnapshotRevision,
		TrustSnapshotHash: release.TrustSnapshotHash, RequiredKinds: append([]string(nil), gate.RequiredKinds...),
		ActiveEvidenceIDs: activeIDs, MissingKinds: append([]string(nil), gate.MissingKinds...),
		BlockedReasons: append([]string(nil), gate.BlockedReasons...), GateHash: gate.GateHash, Ready: gate.Ready,
		EvaluatedAt: now.UTC(), CreatedAt: now.UTC(), Actor: request.Actor, RequestID: request.RequestID,
		IdempotencyKey: request.IdempotencyKey, CausationID: request.CausationID, CorrelationID: request.CorrelationID,
	}
	snapshot.SetDerivedHashes()
	if err := snapshot.Normalize(); err != nil {
		return contracts.ReadinessSnapshot{}, false, err
	}
	if existing, existingErr := queryReadinessByHash(ctx, tx, snapshot.WorkspaceID, snapshot.DeploymentID, snapshot.ReleaseID, snapshot.SnapshotHash, true); existingErr == nil {
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return existing, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ReadinessSnapshot{}, false, fmt.Errorf("commit readiness hash replay: %w", err)
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrReadinessSnapshotNotFound) {
		return contracts.ReadinessSnapshot{}, false, existingErr
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		snapshot.ID = "dry-run-" + snapshot.SnapshotHash[:16]
		return snapshot, false, nil
	}
	actorJSON, err := json.Marshal(snapshot.Actor)
	if err != nil {
		return contracts.ReadinessSnapshot{}, false, err
	}
	arrays := make([][]byte, 4)
	for i, value := range []any{snapshot.RequiredKinds, snapshot.ActiveEvidenceIDs, snapshot.MissingKinds, snapshot.BlockedReasons} {
		arrays[i], err = json.Marshal(value)
		if err != nil {
			return contracts.ReadinessSnapshot{}, false, fmt.Errorf("encode readiness snapshot array: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_readiness_snapshots
		  (id,workspace_id,deployment_id,release_id,release_hash,trust_snapshot_revision,trust_snapshot_hash,required_kinds,active_evidence_ids,missing_kinds,blocked_reasons,gate_hash,ready,snapshot_hash,evaluated_at,actor,request_id,idempotency_key,request_hash,causation_id,correlation_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,$12,$13,$14,$15,$16::jsonb,$17,$18,$19,$20,$21)`,
		snapshot.ID, snapshot.WorkspaceID, snapshot.DeploymentID, snapshot.ReleaseID, snapshot.ReleaseHash,
		snapshot.TrustSnapshotRevision, snapshot.TrustSnapshotHash, arrays[0], arrays[1], arrays[2], arrays[3],
		snapshot.GateHash, snapshot.Ready, snapshot.SnapshotHash, snapshot.EvaluatedAt, actorJSON,
		nullableString(snapshot.RequestID), snapshot.IdempotencyKey, requestHash, nullableString(snapshot.CausationID), nullableString(snapshot.CorrelationID)); err != nil {
		if isUniqueViolation(err) {
			return contracts.ReadinessSnapshot{}, false, ErrReadinessSnapshotConflict
		}
		return contracts.ReadinessSnapshot{}, false, fmt.Errorf("insert readiness snapshot: %w", err)
	}
	metadata, _ := json.Marshal(map[string]any{"snapshot_hash": snapshot.SnapshotHash, "gate_hash": snapshot.GateHash, "ready": snapshot.Ready})
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_readiness_snapshot_events(workspace_id,deployment_id,release_id,snapshot_id,event,actor,metadata,request_id,idempotency_key,causation_id,correlation_id) VALUES($1,$2,$3,$4,'captured',$5::jsonb,$6::jsonb,$7,$8,$9,$10)`, snapshot.WorkspaceID, snapshot.DeploymentID, snapshot.ReleaseID, snapshot.ID, actorJSON, metadata, nullableString(snapshot.RequestID), snapshot.IdempotencyKey, nullableString(snapshot.CausationID), nullableString(snapshot.CorrelationID)); err != nil {
		return contracts.ReadinessSnapshot{}, false, fmt.Errorf("record readiness snapshot capture: %w", err)
	}
	if err := s.registerQualificationRetentionMetadata(ctx, tx, snapshot.WorkspaceID, snapshot.DeploymentID, contracts.QualificationRecordSnapshot, snapshot.ID, snapshot.SnapshotHash, snapshot.CreatedAt, snapshot.Actor, snapshot.RequestID, snapshot.CausationID, snapshot.CorrelationID); err != nil {
		return contracts.ReadinessSnapshot{}, false, fmt.Errorf("register readiness snapshot retention metadata: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ReadinessSnapshot{}, false, fmt.Errorf("commit readiness snapshot: %w", err)
	}
	return snapshot, true, nil
}

func (s *ReadinessStore) Get(ctx context.Context, workspaceID, deploymentID, snapshotID string) (contracts.ReadinessSnapshot, error) {
	if s == nil || s.pool == nil {
		return contracts.ReadinessSnapshot{}, fmt.Errorf("readiness store is not configured")
	}
	workspaceID, deploymentID, snapshotID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(snapshotID)
	if workspaceID == "" || deploymentID == "" || snapshotID == "" {
		return contracts.ReadinessSnapshot{}, ErrReadinessSnapshotNotFound
	}
	return queryReadinessSnapshotWithPool(ctx, s.pool, workspaceID, readinessSnapshotSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3`, []any{workspaceID, deploymentID, snapshotID})
}

func (s *ReadinessStore) List(ctx context.Context, workspaceID, deploymentID, releaseID string, limit int, cursor string) (contracts.ReadinessSnapshotPage, error) {
	if s == nil || s.pool == nil {
		return contracts.ReadinessSnapshotPage{}, fmt.Errorf("readiness store is not configured")
	}
	limit = boundedQualificationPageLimit(limit)
	workspaceID, deploymentID, releaseID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" || releaseID == "" {
		return contracts.ReadinessSnapshotPage{}, ErrReadinessSnapshotNotFound
	}
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.ReadinessSnapshotPage{}, ErrReadinessSnapshotCursor
	}
	page := contracts.ReadinessSnapshotPage{Items: make([]contracts.ReadinessSnapshot, 0, limit)}
	err := workspaceQueryRows(ctx, s.pool, workspaceID, readinessSnapshotSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 AND id>$4 ORDER BY id LIMIT $5`, []any{workspaceID, deploymentID, releaseID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			snapshot, err := scanReadinessSnapshot(rows)
			if err != nil {
				return err
			}
			page.Items = append(page.Items, snapshot)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.ReadinessSnapshotPage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// Annotate stores one structured operator observation against an existing
// snapshot. The snapshot itself remains immutable and admission-independent.
func (s *ReadinessStore) Annotate(ctx context.Context, request contracts.IncidentAnnotationRequest, now time.Time) (contracts.IncidentAnnotation, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.IncidentAnnotation{}, false, fmt.Errorf("readiness store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := request.Normalize(); err != nil {
		return contracts.IncidentAnnotation{}, false, err
	}
	requestHash := incidentRequestHash(request)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.IncidentAnnotation{}, false, fmt.Errorf("begin incident annotation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-readiness:' || $1 || ':' || $2 || ':' || $3, 0))`, request.WorkspaceID, request.DeploymentID, request.ReleaseID); err != nil {
		return contracts.IncidentAnnotation{}, false, err
	}
	if existing, existingErr := queryIncidentByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true); existingErr == nil {
		if existingRequestHashForAnnotation(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey) != requestHash {
			return contracts.IncidentAnnotation{}, false, ErrIncidentAnnotationConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.IncidentAnnotation{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrIncidentAnnotationNotFound) {
		return contracts.IncidentAnnotation{}, false, existingErr
	}
	snapshot, err := queryReadinessSnapshot(ctx, tx, request.WorkspaceID, request.DeploymentID, request.SnapshotID, true)
	if err != nil {
		return contracts.IncidentAnnotation{}, false, err
	}
	if snapshot.ReleaseID != request.ReleaseID {
		return contracts.IncidentAnnotation{}, false, ErrIncidentAnnotationConflict
	}
	annotation := contracts.IncidentAnnotation{
		SchemaVersion: contracts.ReadinessSnapshotSchemaVersion, ID: contracts.NewID("incident-annotation"),
		WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, ReleaseID: request.ReleaseID,
		SnapshotID: snapshot.ID, SnapshotHash: snapshot.SnapshotHash, Code: request.Code, Disposition: request.Disposition,
		ReferenceHash: request.ReferenceHash, Actor: request.Actor, CreatedAt: now.UTC(), RequestID: request.RequestID,
		IdempotencyKey: request.IdempotencyKey, CausationID: request.CausationID, CorrelationID: request.CorrelationID,
	}
	annotation.SetDerivedHash()
	if err := annotation.Normalize(); err != nil {
		return contracts.IncidentAnnotation{}, false, err
	}
	if existing, existingErr := queryIncidentByHash(ctx, tx, annotation.WorkspaceID, annotation.DeploymentID, annotation.ReleaseID, annotation.AnnotationHash, true); existingErr == nil {
		if err := tx.Commit(ctx); err != nil {
			return contracts.IncidentAnnotation{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrIncidentAnnotationNotFound) {
		return contracts.IncidentAnnotation{}, false, existingErr
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		annotation.ID = "dry-run-" + annotation.AnnotationHash[:16]
		return annotation, false, nil
	}
	actorJSON, err := json.Marshal(annotation.Actor)
	if err != nil {
		return contracts.IncidentAnnotation{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_incident_annotations(id,workspace_id,deployment_id,release_id,snapshot_id,snapshot_hash,code,disposition,reference_hash,annotation_hash,actor,request_id,idempotency_key,request_hash,causation_id,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15,$16)`, annotation.ID, annotation.WorkspaceID, annotation.DeploymentID, annotation.ReleaseID, annotation.SnapshotID, annotation.SnapshotHash, annotation.Code, annotation.Disposition, nullableString(annotation.ReferenceHash), annotation.AnnotationHash, actorJSON, nullableString(annotation.RequestID), annotation.IdempotencyKey, requestHash, nullableString(annotation.CausationID), nullableString(annotation.CorrelationID)); err != nil {
		if isUniqueViolation(err) {
			return contracts.IncidentAnnotation{}, false, ErrIncidentAnnotationConflict
		}
		return contracts.IncidentAnnotation{}, false, fmt.Errorf("insert incident annotation: %w", err)
	}
	metadata, _ := json.Marshal(map[string]any{"annotation_hash": annotation.AnnotationHash, "code": annotation.Code, "disposition": annotation.Disposition, "reference_hash": annotation.ReferenceHash})
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_readiness_snapshot_events(workspace_id,deployment_id,release_id,snapshot_id,annotation_id,event,actor,metadata,request_id,idempotency_key,causation_id,correlation_id) VALUES($1,$2,$3,$4,$5,'annotated',$6::jsonb,$7::jsonb,$8,$9,$10,$11)`, annotation.WorkspaceID, annotation.DeploymentID, annotation.ReleaseID, annotation.SnapshotID, annotation.ID, actorJSON, metadata, nullableString(annotation.RequestID), annotation.IdempotencyKey, nullableString(annotation.CausationID), nullableString(annotation.CorrelationID)); err != nil {
		return contracts.IncidentAnnotation{}, false, fmt.Errorf("record incident annotation: %w", err)
	}
	if err := s.registerQualificationRetentionMetadata(ctx, tx, annotation.WorkspaceID, annotation.DeploymentID, contracts.QualificationRecordIncident, annotation.ID, annotation.AnnotationHash, annotation.CreatedAt, annotation.Actor, annotation.RequestID, annotation.CausationID, annotation.CorrelationID); err != nil {
		return contracts.IncidentAnnotation{}, false, fmt.Errorf("register incident annotation retention metadata: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.IncidentAnnotation{}, false, fmt.Errorf("commit incident annotation: %w", err)
	}
	return annotation, true, nil
}

func (s *ReadinessStore) ListAnnotations(ctx context.Context, workspaceID, deploymentID, releaseID, snapshotID string, limit int, cursor string) (contracts.IncidentAnnotationPage, error) {
	if s == nil || s.pool == nil {
		return contracts.IncidentAnnotationPage{}, fmt.Errorf("readiness store is not configured")
	}
	limit = boundedQualificationPageLimit(limit)
	workspaceID, deploymentID, releaseID, snapshotID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID), strings.TrimSpace(snapshotID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" || releaseID == "" {
		return contracts.IncidentAnnotationPage{}, ErrIncidentAnnotationNotFound
	}
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.IncidentAnnotationPage{}, ErrIncidentAnnotationCursor
	}
	query := annotationSelect() + ` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 AND id>$4`
	args := []any{workspaceID, deploymentID, releaseID, cursor, limit + 1}
	if snapshotID != "" {
		query = annotationSelect() + ` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 AND snapshot_id=$4 AND id>$5`
		args = []any{workspaceID, deploymentID, releaseID, snapshotID, cursor, limit + 1}
	}
	query += ` ORDER BY id LIMIT $` + fmt.Sprint(len(args))
	page := contracts.IncidentAnnotationPage{Items: make([]contracts.IncidentAnnotation, 0, limit)}
	err := workspaceQueryRows(ctx, s.pool, workspaceID, query, args, func(rows pgx.Rows) error {
		for rows.Next() {
			annotation, err := scanIncidentAnnotation(rows)
			if err != nil {
				return err
			}
			page.Items = append(page.Items, annotation)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.IncidentAnnotationPage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

func readinessRequestHash(request contracts.ReadinessSnapshotRequest) string {
	parts := []string{"readiness-request", request.WorkspaceID, request.DeploymentID, request.ReleaseID}
	return contracts.HashStrings(append(parts, request.RequiredKinds...)...)
}

func incidentRequestHash(request contracts.IncidentAnnotationRequest) string {
	return contracts.HashStrings("incident-request", request.WorkspaceID, request.DeploymentID, request.ReleaseID, request.SnapshotID, request.Code, request.Disposition, request.ReferenceHash)
}

func existingRequestHash(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, idempotency string) string {
	var value string
	_ = tx.QueryRow(ctx, `SELECT request_hash FROM fornix.qualification_readiness_snapshots WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`, workspaceID, deploymentID, idempotency).Scan(&value)
	return value
}

func existingRequestHashForAnnotation(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, idempotency string) string {
	var value string
	_ = tx.QueryRow(ctx, `SELECT request_hash FROM fornix.qualification_incident_annotations WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`, workspaceID, deploymentID, idempotency).Scan(&value)
	return value
}

func readinessSnapshotSelect() string {
	return `SELECT id,workspace_id,deployment_id,release_id,release_hash,trust_snapshot_revision,trust_snapshot_hash,required_kinds,active_evidence_ids,missing_kinds,blocked_reasons,gate_hash,ready,snapshot_hash,evaluated_at,actor,COALESCE(request_id,''),idempotency_key,causation_id,correlation_id,created_at FROM fornix.qualification_readiness_snapshots`
}

func annotationSelect() string {
	return `SELECT id,workspace_id,deployment_id,release_id,snapshot_id,snapshot_hash,code,disposition,COALESCE(reference_hash,''),annotation_hash,actor,COALESCE(request_id,''),idempotency_key,causation_id,correlation_id,created_at FROM fornix.qualification_incident_annotations`
}

func queryReadinessByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, idempotency string, lock bool) (contracts.ReadinessSnapshot, error) {
	return queryReadinessWithExtra(ctx, tx, workspaceID, deploymentID, "", lock, ` AND idempotency_key=$3`, idempotency)
}

func queryReadinessByHash(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, releaseID, hash string, lock bool) (contracts.ReadinessSnapshot, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	return scanReadinessSnapshotOrNotFound(tx.QueryRow(ctx, readinessSnapshotSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 AND snapshot_hash=$4`+lockClause, workspaceID, deploymentID, releaseID, hash))
}

func queryReadinessWithExtra(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, snapshotID string, lock bool, extra string, value string) (contracts.ReadinessSnapshot, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	args := []any{workspaceID, deploymentID, value}
	query := readinessSnapshotSelect() + ` WHERE workspace_id=$1 AND deployment_id=$2` + extra + lockClause
	if snapshotID != "" {
		query = readinessSnapshotSelect() + ` WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3` + lockClause
		args = []any{workspaceID, deploymentID, snapshotID}
	}
	return scanReadinessSnapshotOrNotFound(tx.QueryRow(ctx, query, args...))
}

func queryReadinessSnapshot(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, snapshotID string, lock bool) (contracts.ReadinessSnapshot, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	return scanReadinessSnapshotOrNotFound(tx.QueryRow(ctx, readinessSnapshotSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3`+lockClause, workspaceID, deploymentID, snapshotID))
}

func queryReadinessSnapshotWithPool(ctx context.Context, pool *pgxpool.Pool, workspaceID, query string, args []any) (contracts.ReadinessSnapshot, error) {
	var result contracts.ReadinessSnapshot
	err := workspaceQueryRow(ctx, pool, workspaceID, query, args, func(row pgx.Row) error { var err error; result, err = scanReadinessSnapshot(row); return err })
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ReadinessSnapshot{}, ErrReadinessSnapshotNotFound
	}
	return result, err
}

func scanReadinessSnapshotOrNotFound(row pgx.Row) (contracts.ReadinessSnapshot, error) {
	snapshot, err := scanReadinessSnapshot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ReadinessSnapshot{}, ErrReadinessSnapshotNotFound
	}
	return snapshot, err
}

func scanReadinessSnapshot(row interface{ Scan(...any) error }) (contracts.ReadinessSnapshot, error) {
	var snapshot contracts.ReadinessSnapshot
	var requiredRaw, activeRaw, missingRaw, blockedRaw, actorRaw []byte
	if err := row.Scan(&snapshot.ID, &snapshot.WorkspaceID, &snapshot.DeploymentID, &snapshot.ReleaseID, &snapshot.ReleaseHash, &snapshot.TrustSnapshotRevision, &snapshot.TrustSnapshotHash, &requiredRaw, &activeRaw, &missingRaw, &blockedRaw, &snapshot.GateHash, &snapshot.Ready, &snapshot.SnapshotHash, &snapshot.EvaluatedAt, &actorRaw, &snapshot.RequestID, &snapshot.IdempotencyKey, &snapshot.CausationID, &snapshot.CorrelationID, &snapshot.CreatedAt); err != nil {
		return contracts.ReadinessSnapshot{}, err
	}
	for raw, target := range map[string]any{"required_kinds": &snapshot.RequiredKinds, "active_evidence_ids": &snapshot.ActiveEvidenceIDs, "missing_kinds": &snapshot.MissingKinds, "blocked_reasons": &snapshot.BlockedReasons} {
		var bytes []byte
		switch raw {
		case "required_kinds":
			bytes = requiredRaw
		case "active_evidence_ids":
			bytes = activeRaw
		case "missing_kinds":
			bytes = missingRaw
		default:
			bytes = blockedRaw
		}
		if err := json.Unmarshal(bytes, target); err != nil {
			return contracts.ReadinessSnapshot{}, fmt.Errorf("decode readiness snapshot %s: %w", raw, err)
		}
	}
	if err := json.Unmarshal(actorRaw, &snapshot.Actor); err != nil {
		return contracts.ReadinessSnapshot{}, fmt.Errorf("decode readiness snapshot actor: %w", err)
	}
	if err := snapshot.Normalize(); err != nil {
		return contracts.ReadinessSnapshot{}, err
	}
	return snapshot, nil
}

func queryIncidentByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, idempotency string, lock bool) (contracts.IncidentAnnotation, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	return scanIncidentOrNotFound(tx.QueryRow(ctx, annotationSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`+lockClause, workspaceID, deploymentID, idempotency))
}

func queryIncidentByHash(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, releaseID, hash string, lock bool) (contracts.IncidentAnnotation, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	return scanIncidentOrNotFound(tx.QueryRow(ctx, annotationSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 AND annotation_hash=$4`+lockClause, workspaceID, deploymentID, releaseID, hash))
}

func scanIncidentOrNotFound(row pgx.Row) (contracts.IncidentAnnotation, error) {
	annotation, err := scanIncidentAnnotation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.IncidentAnnotation{}, ErrIncidentAnnotationNotFound
	}
	return annotation, err
}

func scanIncidentAnnotation(row interface{ Scan(...any) error }) (contracts.IncidentAnnotation, error) {
	var annotation contracts.IncidentAnnotation
	var actorRaw []byte
	if err := row.Scan(&annotation.ID, &annotation.WorkspaceID, &annotation.DeploymentID, &annotation.ReleaseID, &annotation.SnapshotID, &annotation.SnapshotHash, &annotation.Code, &annotation.Disposition, &annotation.ReferenceHash, &annotation.AnnotationHash, &actorRaw, &annotation.RequestID, &annotation.IdempotencyKey, &annotation.CausationID, &annotation.CorrelationID, &annotation.CreatedAt); err != nil {
		return contracts.IncidentAnnotation{}, err
	}
	if err := json.Unmarshal(actorRaw, &annotation.Actor); err != nil {
		return contracts.IncidentAnnotation{}, fmt.Errorf("decode incident annotation actor: %w", err)
	}
	if err := annotation.Normalize(); err != nil {
		return contracts.IncidentAnnotation{}, err
	}
	return annotation, nil
}
