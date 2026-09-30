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
	ErrQualificationRefreshNotFound = errors.New("qualification refresh not found")
	ErrQualificationRefreshConflict = errors.New("qualification refresh conflicts with existing state")
	ErrQualificationRefreshStale    = errors.New("qualification refresh evidence is stale")
	ErrQualificationRefreshCursor   = errors.New("invalid qualification refresh cursor")
)

// RefreshQualificationEvidence validates and atomically replaces the selected
// active evidence links for one release. It never contacts the deployment or
// copies the signed import bytes.
func (s *DeploymentEvidenceStore) RefreshQualificationEvidence(ctx context.Context, request contracts.QualificationRefreshRequest, now time.Time) (contracts.QualificationRefreshResult, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshResult{}, fmt.Errorf("deployment evidence store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	requestHash := request.StableHash()
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-deployment-evidence:' || $1 || ':' || $2, 0))`, request.WorkspaceID, request.DeploymentID); err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	if existing, existingErr := queryQualificationRefreshByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey); existingErr == nil {
		if existing.RequestHash != requestHash {
			return contracts.QualificationRefreshResult{}, ErrQualificationRefreshConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationRefreshResult{}, err
		}
		return contracts.QualificationRefreshResult{Report: existing, Deduplicated: true, DryRun: request.DryRun}, nil
	} else if !errors.Is(existingErr, ErrQualificationRefreshNotFound) {
		return contracts.QualificationRefreshResult{}, existingErr
	}
	if request.ScheduleAuthorization != nil {
		if err := validateQualificationRefreshScheduleAuthorizationTx(ctx, tx, request, now); err != nil {
			return contracts.QualificationRefreshResult{}, err
		}
	}

	release, err := queryDeploymentRelease(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ReleaseID, true)
	if err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	beforeGate, err := evaluateDeploymentGateTx(ctx, tx, release, nil, request.AsOf)
	if err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	items := make([]contracts.QualificationRefreshItemResult, 0, len(request.Items))
	for index, item := range request.Items {
		predecessor, predecessorErr := queryDeploymentEvidenceByKind(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ReleaseID, item.Kind, true)
		if predecessorErr != nil && !errors.Is(predecessorErr, ErrDeploymentEvidenceNotFound) {
			return contracts.QualificationRefreshResult{}, predecessorErr
		}
		if errors.Is(predecessorErr, ErrDeploymentEvidenceNotFound) {
			if item.SupersedesLinkID != "" {
				return contracts.QualificationRefreshResult{}, ErrQualificationRefreshConflict
			}
		} else if item.SupersedesLinkID != "" && item.SupersedesLinkID != predecessor.ID {
			return contracts.QualificationRefreshResult{}, ErrQualificationRefreshConflict
		} else {
			item.SupersedesLinkID = predecessor.ID
		}
		linkRequest := contracts.DeploymentEvidenceLinkRequest{
			WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, ReleaseID: request.ReleaseID,
			Kind: item.Kind, ImportID: item.ImportID, SupersedesLinkID: item.SupersedesLinkID,
			RequestID: request.RequestID, IdempotencyKey: refreshLinkIdempotency(request.IdempotencyKey, item.Kind),
			CausationID: request.CausationID, CorrelationID: request.CorrelationID, Actor: request.Actor, DryRun: request.DryRun,
		}
		link, _, err := s.linkEvidenceTx(ctx, tx, linkRequest, request.AsOf)
		if err != nil {
			return contracts.QualificationRefreshResult{}, err
		}
		result := contracts.QualificationRefreshItemResult{
			Kind: item.Kind, ImportID: link.ImportID, LinkID: link.ID, SignedHash: link.SignedHash,
			ObservationHash: link.ObservationHash, ReportHash: link.ReportHash, ManifestHash: link.ManifestHash,
			SourceHash: link.SourceHash, BoundaryEvidenceHash: link.BoundaryEvidenceHash,
			BoundaryEvidenceUntil: link.BoundaryEvidenceExpiresAt, Outcome: link.Outcome,
		}
		if predecessor.ID != "" {
			result.PreviousLinkID = predecessor.ID
		}
		if err := result.Normalize(index); err != nil {
			return contracts.QualificationRefreshResult{}, err
		}
		items = append(items, result)
	}
	afterGate, err := evaluateDeploymentGateTx(ctx, tx, release, nil, request.AsOf)
	if err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	outcome := contracts.QualificationOutcomeBlocked
	if afterGate.Ready {
		outcome = contracts.QualificationOutcomePassed
	}
	report := contracts.QualificationRefreshReport{
		SchemaVersion: contracts.QualificationRefreshSchemaVersion,
		ID:            contracts.NewID("qualification-refresh"), WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID,
		ReleaseID: request.ReleaseID, AsOf: request.AsOf, RequestHash: requestHash,
		BeforeGateHash: beforeGate.GateHash, AfterGateHash: afterGate.GateHash, Items: items,
		Outcome: outcome, DryRun: request.DryRun, CreatedAt: now,
	}
	if request.DryRun {
		report.ID = "dry-run-" + requestHash[:16]
		if err := report.Normalize(); err != nil {
			return contracts.QualificationRefreshResult{}, err
		}
		_ = tx.Rollback(ctx)
		return contracts.QualificationRefreshResult{Report: report, DryRun: true}, nil
	}
	if err := report.Normalize(); err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_refresh_runs
		  (id,workspace_id,deployment_id,release_id,as_of,request_hash,before_gate_hash,after_gate_hash,outcome,actor,request_id,idempotency_key,causation_id,correlation_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14)`,
		report.ID, report.WorkspaceID, report.DeploymentID, report.ReleaseID, report.AsOf, report.RequestHash,
		report.BeforeGateHash, report.AfterGateHash, report.Outcome, actorJSON, nullableString(request.RequestID), request.IdempotencyKey,
		nullableString(request.CausationID), nullableString(request.CorrelationID)); err != nil {
		if isUniqueViolation(err) {
			return contracts.QualificationRefreshResult{}, ErrQualificationRefreshConflict
		}
		return contracts.QualificationRefreshResult{}, fmt.Errorf("insert qualification refresh: %w", err)
	}
	for index, item := range report.Items {
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.qualification_refresh_items
			  (run_id,workspace_id,deployment_id,release_id,item_index,kind,import_id,previous_link_id,link_id,signed_hash,observation_hash,report_hash,manifest_hash,source_hash,boundary_evidence_hash,boundary_evidence_until,outcome,reason)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
			report.ID, report.WorkspaceID, report.DeploymentID, report.ReleaseID, index, item.Kind, item.ImportID, nullableString(item.PreviousLinkID), item.LinkID,
			item.SignedHash, item.ObservationHash, item.ReportHash, item.ManifestHash, item.SourceHash, nullableString(item.BoundaryEvidenceHash), item.BoundaryEvidenceUntil,
			item.Outcome, nullableString(item.Reason)); err != nil {
			return contracts.QualificationRefreshResult{}, fmt.Errorf("insert qualification refresh item: %w", err)
		}
	}
	metadata, err := json.Marshal(map[string]string{"request_hash": report.RequestHash, "before_gate_hash": report.BeforeGateHash, "after_gate_hash": report.AfterGateHash, "report_hash": report.ReportHash})
	if err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_refresh_events(workspace_id,deployment_id,release_id,run_id,event,actor,metadata,request_id,idempotency_key,causation_id,correlation_id) VALUES($1,$2,$3,$4,'refreshed',$5::jsonb,$6::jsonb,$7,$8,$9,$10)`, report.WorkspaceID, report.DeploymentID, report.ReleaseID, report.ID, actorJSON, metadata, nullableString(request.RequestID), request.IdempotencyKey, nullableString(request.CausationID), nullableString(request.CorrelationID)); err != nil {
		return contracts.QualificationRefreshResult{}, fmt.Errorf("record qualification refresh event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRefreshResult{}, err
	}
	return contracts.QualificationRefreshResult{Report: report, Created: true}, nil
}

func refreshLinkIdempotency(refreshKey, kind string) string {
	return "qualification-refresh:" + refreshKey + ":" + kind
}

func (s *DeploymentEvidenceStore) GetQualificationRefresh(ctx context.Context, workspaceID, deploymentID, refreshID string) (contracts.QualificationRefreshReport, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshReport{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, deploymentID, refreshID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(refreshID)
	if workspaceID == "" || deploymentID == "" || refreshID == "" {
		return contracts.QualificationRefreshReport{}, ErrQualificationRefreshNotFound
	}
	var report contracts.QualificationRefreshReport
	err := workspaceQueryRow(ctx, s.pool, workspaceID, refreshRunSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3`, []any{workspaceID, deploymentID, refreshID}, func(row pgx.Row) error {
		var err error
		report, err = scanQualificationRefreshRun(row)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationRefreshReport{}, ErrQualificationRefreshNotFound
	}
	if err != nil {
		return contracts.QualificationRefreshReport{}, err
	}
	return loadQualificationRefreshItems(ctx, s.pool, report)
}

func (s *DeploymentEvidenceStore) ListQualificationRefreshes(ctx context.Context, workspaceID, deploymentID, releaseID string, limit int, cursor string) (contracts.QualificationRefreshPage, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationRefreshPage{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, deploymentID, releaseID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" || releaseID == "" {
		return contracts.QualificationRefreshPage{}, ErrQualificationRefreshNotFound
	}
	limit = boundedQualificationPageLimit(limit)
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.QualificationRefreshPage{}, ErrQualificationRefreshCursor
	}
	ids := make([]string, 0, limit+1)
	err := workspaceQueryRows(ctx, s.pool, workspaceID, refreshRunSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 AND id>$4 ORDER BY id LIMIT $5`, []any{workspaceID, deploymentID, releaseID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			var run contracts.QualificationRefreshReport
			if err := scanQualificationRefreshRows(rows, &run); err != nil {
				return err
			}
			ids = append(ids, run.ID)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.QualificationRefreshPage{}, err
	}
	page := contracts.QualificationRefreshPage{Items: make([]contracts.QualificationRefreshReport, 0, limit)}
	if len(ids) > limit {
		page.NextCursor = ids[limit-1]
		ids = ids[:limit]
	}
	for _, id := range ids {
		report, err := s.GetQualificationRefresh(ctx, workspaceID, deploymentID, id)
		if err != nil {
			return contracts.QualificationRefreshPage{}, err
		}
		page.Items = append(page.Items, report)
	}
	return page, nil
}

func queryQualificationRefreshByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, key string) (contracts.QualificationRefreshReport, error) {
	report, err := scanQualificationRefreshRun(tx.QueryRow(ctx, refreshRunSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`, workspaceID, deploymentID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationRefreshReport{}, ErrQualificationRefreshNotFound
	}
	if err != nil {
		return contracts.QualificationRefreshReport{}, err
	}
	return loadQualificationRefreshItemsTx(ctx, tx, report)
}

func loadQualificationRefreshItems(ctx context.Context, pool *pgxpool.Pool, report contracts.QualificationRefreshReport) (contracts.QualificationRefreshReport, error) {
	tx, err := beginWorkspaceTx(ctx, pool, report.WorkspaceID)
	if err != nil {
		return contracts.QualificationRefreshReport{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	report, err = loadQualificationRefreshItemsTx(ctx, tx, report)
	if err != nil {
		return contracts.QualificationRefreshReport{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationRefreshReport{}, err
	}
	return report, nil
}

func loadQualificationRefreshItemsTx(ctx context.Context, tx pgx.Tx, report contracts.QualificationRefreshReport) (contracts.QualificationRefreshReport, error) {
	rows, err := tx.Query(ctx, `SELECT kind,import_id,COALESCE(previous_link_id,''),link_id,signed_hash,observation_hash,report_hash,manifest_hash,source_hash,COALESCE(boundary_evidence_hash,''),boundary_evidence_until,outcome,COALESCE(reason,'') FROM fornix.qualification_refresh_items WHERE run_id=$1 ORDER BY item_index`, report.ID)
	if err != nil {
		return contracts.QualificationRefreshReport{}, err
	}
	defer rows.Close()
	report.Items = make([]contracts.QualificationRefreshItemResult, 0, contracts.MaxQualificationRefreshItems)
	for rows.Next() {
		var item contracts.QualificationRefreshItemResult
		if err := rows.Scan(&item.Kind, &item.ImportID, &item.PreviousLinkID, &item.LinkID, &item.SignedHash, &item.ObservationHash, &item.ReportHash, &item.ManifestHash, &item.SourceHash, &item.BoundaryEvidenceHash, &item.BoundaryEvidenceUntil, &item.Outcome, &item.Reason); err != nil {
			return contracts.QualificationRefreshReport{}, err
		}
		if err := item.Normalize(len(report.Items)); err != nil {
			return contracts.QualificationRefreshReport{}, err
		}
		report.Items = append(report.Items, item)
	}
	if err := rows.Err(); err != nil {
		return contracts.QualificationRefreshReport{}, err
	}
	if err := report.Normalize(); err != nil {
		return contracts.QualificationRefreshReport{}, err
	}
	return report, nil
}

func refreshRunSelect() string {
	return `SELECT id,workspace_id,deployment_id,release_id,as_of,request_hash,before_gate_hash,after_gate_hash,outcome,actor,created_at FROM fornix.qualification_refresh_runs`
}

func scanQualificationRefreshRun(row pgx.Row) (contracts.QualificationRefreshReport, error) {
	var report contracts.QualificationRefreshReport
	var actorRaw []byte
	if err := row.Scan(&report.ID, &report.WorkspaceID, &report.DeploymentID, &report.ReleaseID, &report.AsOf, &report.RequestHash, &report.BeforeGateHash, &report.AfterGateHash, &report.Outcome, &actorRaw, &report.CreatedAt); err != nil {
		return contracts.QualificationRefreshReport{}, err
	}
	if !json.Valid(actorRaw) {
		return contracts.QualificationRefreshReport{}, fmt.Errorf("decode qualification refresh actor")
	}
	report.SchemaVersion = contracts.QualificationRefreshSchemaVersion
	return report, nil
}

func scanQualificationRefreshRows(rows pgx.Rows, report *contracts.QualificationRefreshReport) error {
	var actorRaw []byte
	if err := rows.Scan(&report.ID, &report.WorkspaceID, &report.DeploymentID, &report.ReleaseID, &report.AsOf, &report.RequestHash, &report.BeforeGateHash, &report.AfterGateHash, &report.Outcome, &actorRaw, &report.CreatedAt); err != nil {
		return err
	}
	report.SchemaVersion = contracts.QualificationRefreshSchemaVersion
	return nil
}
