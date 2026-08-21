package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
)

// Backfill moves only oversized historical inline values. It is deliberately
// source-kind specific so each batch has a stable cursor and bounded SQL work.
func (s *ArtifactStore) Backfill(ctx context.Context, request contracts.ArtifactBackfillRequest) (contracts.ArtifactBackfillResult, error) {
	if s == nil || s.pool == nil {
		return contracts.ArtifactBackfillResult{}, fmt.Errorf("artifact store is not configured")
	}
	request.WorkspaceID = strings.TrimSpace(request.WorkspaceID)
	request.SourceKind = strings.TrimSpace(request.SourceKind)
	if request.WorkspaceID == "" {
		return contracts.ArtifactBackfillResult{}, fmt.Errorf("workspace_id is required")
	}
	if request.SourceKind != "tool_run" && request.SourceKind != "evidence" && request.SourceKind != "agent_run" {
		return contracts.ArtifactBackfillResult{}, fmt.Errorf("unsupported backfill source_kind %q", request.SourceKind)
	}
	batch := normalizeArtifactBatch(request.BatchSize)
	result := contracts.ArtifactBackfillResult{WorkspaceID: request.WorkspaceID, SourceKind: request.SourceKind, Cursor: request.Cursor, BatchSize: batch, DryRun: request.DryRun}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ArtifactBackfillResult{}, fmt.Errorf("begin artifact backfill: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	lock := ""
	if !request.DryRun {
		lock = " FOR UPDATE"
	}
	switch request.SourceKind {
	case "tool_run":
		err = s.backfillToolRunsTx(ctx, tx, request, batch, lock, &result)
	case "evidence":
		err = s.backfillEvidenceTx(ctx, tx, request, batch, lock, &result)
	case "agent_run":
		err = s.backfillAgentRunsTx(ctx, tx, request, batch, lock, &result)
	}
	if err != nil {
		return contracts.ArtifactBackfillResult{}, err
	}
	if request.DryRun {
		return result, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ArtifactBackfillResult{}, fmt.Errorf("commit artifact backfill: %w", err)
	}
	return result, nil
}

func normalizeArtifactBatch(size int) int {
	if size <= 0 {
		return contracts.DefaultArtifactOperationBatch
	}
	if size > contracts.MaxArtifactOperationBatch {
		return contracts.MaxArtifactOperationBatch
	}
	return size
}

func (s *ArtifactStore) backfillToolRunsTx(ctx context.Context, tx pgx.Tx, request contracts.ArtifactBackfillRequest, batch int, lock string, result *contracts.ArtifactBackfillResult) error {
	rows, err := tx.Query(ctx, `SELECT id,idempotency_key FROM fornix.tool_runs WHERE workspace_id=$1 AND id>$2 AND result IS NOT NULL AND (
		(stdout_artifact_id IS NULL AND octet_length(COALESCE(result->>'stdout',''))>$3) OR
		(stderr_artifact_id IS NULL AND octet_length(COALESCE(result->>'stderr',''))>$3) OR
		(result_artifact_id IS NULL AND octet_length(result::text)>$3)
	) ORDER BY id LIMIT $4`+lock, request.WorkspaceID, request.Cursor, contracts.MaxToolEvidenceBytes, batch)
	if err != nil {
		return fmt.Errorf("select tool backfill: %w", err)
	}
	var items []struct{ id, key string }
	for rows.Next() {
		var item struct{ id, key string }
		if err := rows.Scan(&item.id, &item.key); err != nil {
			rows.Close()
			return err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	toolStore := &ToolRunStore{artifacts: s}
	for _, item := range items {
		id, key := item.id, item.key
		result.Examined++
		result.NextCursor = id
		run, err := readToolRunTx(ctx, tx, request.WorkspaceID, key)
		if err != nil {
			return err
		}
		if run.Result == nil {
			result.Skipped++
			continue
		}
		original, _ := json.Marshal(run.Result)
		eligible := len(run.Result.Stdout) > contracts.MaxToolEvidenceBytes || len(run.Result.Stderr) > contracts.MaxToolEvidenceBytes || len(original) > contracts.MaxToolEvidenceBytes
		if !eligible {
			result.Skipped++
			continue
		}
		result.Eligible++
		if request.DryRun {
			continue
		}
		stored, ids, err := toolStore.artifactizeToolResultTx(ctx, tx, run, *run.Result)
		if err != nil {
			return err
		}
		storedJSON, err := json.Marshal(stored)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE fornix.tool_runs SET result=$3::jsonb, response_evidence=$4::jsonb, stdout_artifact_id=$5, stderr_artifact_id=$6, result_artifact_id=$7 WHERE workspace_id=$1 AND id=$2`, request.WorkspaceID, run.ID, storedJSON, validJSON(modelRedactBounded(storedJSON)), ids.stdout, ids.stderr, ids.result); err != nil {
			return fmt.Errorf("link tool backfill: %w", err)
		}
		result.Linked++
	}
	return nil
}

func (s *ArtifactStore) backfillEvidenceTx(ctx context.Context, tx pgx.Tx, request contracts.ArtifactBackfillRequest, batch int, lock string, result *contracts.ArtifactBackfillResult) error {
	rows, err := tx.Query(ctx, `SELECT id,source_reference,deduplication_key,kind,media_type,gist,raw_payload,raw_size_bytes FROM fornix.evidence_records WHERE workspace_id=$1 AND id>$2::bigint AND raw_artifact_id IS NULL AND raw_size_bytes>$3 ORDER BY id LIMIT $4`+lock, request.WorkspaceID, cursorInt64(request.Cursor), contracts.MaxEvidenceRawBytes, batch)
	if err != nil {
		return fmt.Errorf("select evidence backfill: %w", err)
	}
	var items []struct {
		id                                             int64
		sourceReference, dedupe, kind, mediaType, gist string
		raw                                            []byte
		rawSize                                        int64
	}
	for rows.Next() {
		var item struct {
			id                                             int64
			sourceReference, dedupe, kind, mediaType, gist string
			raw                                            []byte
			rawSize                                        int64
		}
		if err := rows.Scan(&item.id, &item.sourceReference, &item.dedupe, &item.kind, &item.mediaType, &item.gist, &item.raw, &item.rawSize); err != nil {
			rows.Close()
			return err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, item := range items {
		var id int64
		id = item.id
		sourceReference, dedupe, mediaType, gist := item.sourceReference, item.dedupe, item.mediaType, item.gist
		raw, rawSize := item.raw, item.rawSize
		result.Examined++
		result.NextCursor = strconv.FormatInt(id, 10)
		if len(raw) <= contracts.MaxEvidenceRawBytes {
			result.Skipped++
			continue
		}
		result.Eligible++
		if request.DryRun {
			continue
		}
		identity := EvidencePutInput{WorkspaceID: request.WorkspaceID, SourceReference: sourceReference, DeduplicationKey: dedupe}
		artifact, err := s.PutTx(ctx, tx, ArtifactPutInput{
			WorkspaceID: request.WorkspaceID, Kind: "evidence-raw", MediaType: mediaType, Raw: raw,
			Manifest: contracts.ArtifactManifest{Gist: gist}, SourceKind: "evidence", SourceID: evidenceArtifactSourceID(identity), Role: "raw",
			IdempotencyKey: "evidence-raw:" + evidenceArtifactSourceID(identity), Actor: request.Actor,
			CausationID: request.CausationID, CorrelationID: request.CorrelationID,
		})
		if err != nil {
			return err
		}
		marker := []byte(evidenceArtifactMarker(artifact.Reference))
		if _, err := tx.Exec(ctx, `UPDATE fornix.evidence_records SET raw_payload=$3, raw_artifact_id=$4 WHERE workspace_id=$1 AND id=$2 AND raw_artifact_id IS NULL`, request.WorkspaceID, id, marker, artifact.Artifact.ID); err != nil {
			return fmt.Errorf("link evidence backfill: %w", err)
		}
		_ = rawSize
		result.Linked++
	}
	return nil
}

func (s *ArtifactStore) backfillAgentRunsTx(ctx context.Context, tx pgx.Tx, request contracts.ArtifactBackfillRequest, batch int, lock string, result *contracts.ArtifactBackfillResult) error {
	rows, err := tx.Query(ctx, `SELECT id FROM fornix.agent_runs WHERE workspace_id=$1 AND id>$2 AND (
		(last_output_artifact_id IS NULL AND octet_length(last_output)>$3) OR
		(history_artifact_id IS NULL AND octet_length(history::text)>$4)
	) ORDER BY id LIMIT $5`+lock, request.WorkspaceID, request.Cursor, contracts.MaxToolEvidenceBytes, contracts.MaxAgentHistoryBytes, batch)
	if err != nil {
		return fmt.Errorf("select agent backfill: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, id := range ids {
		result.Examined++
		result.NextCursor = id
		run, err := readAgentRunTx(ctx, tx, request.WorkspaceID, id, true)
		if err != nil {
			return err
		}
		historyJSON, err := json.Marshal(run.History)
		if err != nil {
			return err
		}
		if len(run.LastOutput) <= contracts.MaxToolEvidenceBytes && len(historyJSON) <= contracts.MaxAgentHistoryBytes {
			result.Skipped++
			continue
		}
		result.Eligible++
		if request.DryRun {
			continue
		}
		inlineHistory, inlineLastOutput, artifactIDs, err := (&AgentRunStore{artifacts: s}).artifactizeAgentOutputsTx(ctx, tx, run, historyJSON)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE fornix.agent_runs SET history=$3::jsonb,last_output=$4,last_output_artifact_id=$5,history_artifact_id=$6 WHERE workspace_id=$1 AND id=$2`, request.WorkspaceID, run.ID, inlineHistory, inlineLastOutput, artifactIDs.lastOutput, artifactIDs.history); err != nil {
			return fmt.Errorf("link agent backfill: %w", err)
		}
		result.Linked++
	}
	return nil
}

func cursorInt64(value string) int64 {
	if value == "" {
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// RetentionSweep performs archive/delete transitions in deterministic artifact
// ID order. Delete is a tombstone operation: chunks are removed, but the
// artifact identity and all append-only links remain.
func (s *ArtifactStore) RetentionSweep(ctx context.Context, request contracts.ArtifactRetentionSweepRequest) (contracts.ArtifactRetentionSweepResult, error) {
	if s == nil || s.pool == nil {
		return contracts.ArtifactRetentionSweepResult{}, fmt.Errorf("artifact store is not configured")
	}
	request.WorkspaceID = strings.TrimSpace(request.WorkspaceID)
	if request.WorkspaceID == "" {
		return contracts.ArtifactRetentionSweepResult{}, fmt.Errorf("workspace_id is required")
	}
	batch := normalizeArtifactBatch(request.BatchSize)
	now := request.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result := contracts.ArtifactRetentionSweepResult{WorkspaceID: request.WorkspaceID, BatchSize: batch, DryRun: request.DryRun}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ArtifactRetentionSweepResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.retentionArchiveTx(ctx, tx, request, now, batch, &result); err != nil {
		return contracts.ArtifactRetentionSweepResult{}, err
	}
	// Keep archive and delete as separate deterministic phases. A sweep that
	// archives a candidate must not immediately remove the same candidate in
	// the same call, which makes dry-runs and resumable operators predictable.
	if result.Examined == 0 {
		if err := s.retentionDeleteTx(ctx, tx, request, now, batch, &result); err != nil {
			return contracts.ArtifactRetentionSweepResult{}, err
		}
	}
	if request.DryRun {
		return result, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ArtifactRetentionSweepResult{}, fmt.Errorf("commit retention sweep: %w", err)
	}
	return result, nil
}

func (s *ArtifactStore) retentionArchiveTx(ctx context.Context, tx pgx.Tx, request contracts.ArtifactRetentionSweepRequest, now time.Time, batch int, result *contracts.ArtifactRetentionSweepResult) error {
	lock := ""
	if !request.DryRun {
		lock = " FOR UPDATE"
	}
	rows, err := tx.Query(ctx, `SELECT id,status,integrity_state FROM fornix.artifacts WHERE workspace_id=$1 AND status='active' AND archive_after IS NOT NULL AND archive_after<=$2 AND (retain_until IS NULL OR retain_until<=$2) ORDER BY id LIMIT $3`+lock, request.WorkspaceID, now, batch)
	if err != nil {
		return err
	}
	var items []struct {
		id                int64
		status, integrity string
	}
	for rows.Next() {
		var item struct {
			id                int64
			status, integrity string
		}
		if err := rows.Scan(&item.id, &item.status, &item.integrity); err != nil {
			rows.Close()
			return err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, item := range items {
		id, status, integrity := item.id, item.status, item.integrity
		result.Examined++
		result.NextCursor = strconv.FormatInt(id, 10)
		if integrity == contracts.ArtifactIntegrityCorrupt {
			result.Corrupt++
			continue
		}
		if !request.DryRun {
			if _, err := tx.Exec(ctx, `UPDATE fornix.artifacts SET status='archived',archived_at=COALESCE(archived_at,clock_timestamp()) WHERE workspace_id=$1 AND id=$2 AND status='active'`, request.WorkspaceID, id); err != nil {
				return err
			}
			if err := recordArtifactLifecycleTx(ctx, tx, request.WorkspaceID, id, "archive", status, contracts.ArtifactArchived, request.Actor, request.CausationID, request.CorrelationID); err != nil {
				return err
			}
		}
		result.Archived++
	}
	return nil
}

func (s *ArtifactStore) retentionDeleteTx(ctx context.Context, tx pgx.Tx, request contracts.ArtifactRetentionSweepRequest, now time.Time, batch int, result *contracts.ArtifactRetentionSweepResult) error {
	lock := ""
	if !request.DryRun {
		lock = " FOR UPDATE"
	}
	rows, err := tx.Query(ctx, `SELECT a.id,a.status,a.integrity_state,EXISTS (SELECT 1 FROM fornix.artifact_refs r WHERE r.workspace_id=a.workspace_id AND r.artifact_id=a.id AND r.authoritative) FROM fornix.artifacts a WHERE a.workspace_id=$1 AND a.status='archived' AND a.allow_delete AND a.delete_after IS NOT NULL AND a.delete_after<=$2 AND (a.retain_until IS NULL OR a.retain_until<=$2) ORDER BY a.id LIMIT $3`+lock, request.WorkspaceID, now, batch)
	if err != nil {
		return err
	}
	var items []struct {
		id                int64
		status, integrity string
		authoritative     bool
	}
	for rows.Next() {
		var item struct {
			id                int64
			status, integrity string
			authoritative     bool
		}
		if err := rows.Scan(&item.id, &item.status, &item.integrity, &item.authoritative); err != nil {
			rows.Close()
			return err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, item := range items {
		id, status, integrity := item.id, item.status, item.integrity
		result.Examined++
		result.NextCursor = strconv.FormatInt(id, 10)
		if item.authoritative {
			result.Blocked++
			continue
		}
		if integrity == contracts.ArtifactIntegrityCorrupt {
			result.Corrupt++
			continue
		}
		if !request.DryRun {
			if _, err := tx.Exec(ctx, `DELETE FROM fornix.artifact_chunks WHERE workspace_id=$1 AND artifact_id=$2`, request.WorkspaceID, id); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE fornix.artifacts SET status='deleted',deleted_at=COALESCE(deleted_at,clock_timestamp()),integrity_state='unknown' WHERE workspace_id=$1 AND id=$2 AND status='archived'`, request.WorkspaceID, id); err != nil {
				return err
			}
			if err := recordArtifactLifecycleTx(ctx, tx, request.WorkspaceID, id, "delete", status, contracts.ArtifactDeleted, request.Actor, request.CausationID, request.CorrelationID); err != nil {
				return err
			}
		}
		result.Deleted++
	}
	return nil
}

func recordArtifactLifecycleTx(ctx context.Context, tx pgx.Tx, workspaceID string, artifactID int64, action, previous, next string, actor contracts.ActorRef, causationID, correlationID string) error {
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO fornix.artifact_lifecycle_events(workspace_id,artifact_id,action,previous_status,new_status,actor,causation_id,correlation_id) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8)`, workspaceID, artifactID, action, previous, next, actorJSON, causationID, correlationID)
	return err
}

// VerifyBatch checks a bounded page of artifacts and returns a resumable
// integrity cursor.
func (s *ArtifactStore) VerifyBatch(ctx context.Context, request contracts.ArtifactIntegrityRequest) (contracts.ArtifactIntegrityReport, error) {
	if s == nil || s.pool == nil {
		return contracts.ArtifactIntegrityReport{}, fmt.Errorf("artifact store is not configured")
	}
	request.WorkspaceID = strings.TrimSpace(request.WorkspaceID)
	if request.WorkspaceID == "" {
		return contracts.ArtifactIntegrityReport{}, fmt.Errorf("workspace_id is required")
	}
	batch := normalizeArtifactBatch(request.BatchSize)
	report := contracts.ArtifactIntegrityReport{WorkspaceID: request.WorkspaceID, Cursor: request.Cursor, BatchSize: batch, DryRun: request.DryRun}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return report, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	lock := ""
	if !request.DryRun {
		lock = " FOR UPDATE"
	}
	rows, err := tx.Query(ctx, `SELECT id,status,integrity_state FROM fornix.artifacts WHERE workspace_id=$1 AND id>$2 ORDER BY id LIMIT $3`+lock, request.WorkspaceID, request.Cursor, batch)
	if err != nil {
		return report, err
	}
	var items []struct {
		id            int64
		status, state string
	}
	for rows.Next() {
		var item struct {
			id            int64
			status, state string
		}
		if err := rows.Scan(&item.id, &item.status, &item.state); err != nil {
			rows.Close()
			return report, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return report, err
	}
	rows.Close()
	for _, item := range items {
		id, status, state := item.id, item.status, item.state
		report.Examined++
		report.NextCursor = id
		if status == contracts.ArtifactDeleted {
			continue
		}
		artifact, err := readArtifactTx(ctx, tx, request.WorkspaceID, id, false)
		if err != nil {
			return report, err
		}
		raw, rawErr := readArtifactRawTx(ctx, tx, artifact)
		valid := rawErr == nil && verifyArtifactBytes(artifact, raw, nil) == nil
		if valid {
			report.Valid++
		} else {
			report.Corrupt++
			report.CorruptIDs = append(report.CorruptIDs, id)
		}
		if !request.DryRun {
			newState := contracts.ArtifactIntegrityCorrupt
			if valid {
				newState = contracts.ArtifactIntegrityValid
			}
			if _, err := tx.Exec(ctx, `UPDATE fornix.artifacts SET integrity_state=$3,integrity_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, request.WorkspaceID, id, newState); err != nil {
				return report, err
			}
			if err := recordArtifactLifecycleTx(ctx, tx, request.WorkspaceID, id, "verify", state, newState, contracts.ActorRef{ID: "artifact-integrity", Kind: "system", WorkspaceID: request.WorkspaceID}, "", ""); err != nil {
				return report, err
			}
		}
	}
	if request.DryRun {
		return report, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return report, err
	}
	return report, nil
}

// Metrics returns workspace-scoped logical, physical, reference, and
// deduplication storage measurements.
func (s *ArtifactStore) Metrics(ctx context.Context, workspaceID string) (contracts.ArtifactStorageMetrics, error) {
	if s == nil || s.pool == nil {
		return contracts.ArtifactStorageMetrics{}, fmt.Errorf("artifact store is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return contracts.ArtifactStorageMetrics{}, fmt.Errorf("workspace_id is required")
	}
	var metrics contracts.ArtifactStorageMetrics
	metrics.WorkspaceID = workspaceID
	err := s.pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE status='active'),count(*) FILTER (WHERE status='archived'),count(*) FILTER (WHERE status='deleted'),COALESCE(sum(byte_size),0),COALESCE(sum(byte_size) FILTER (WHERE status<>'deleted'),0) FROM fornix.artifacts WHERE workspace_id=$1`, workspaceID).Scan(&metrics.Artifacts, &metrics.ActiveArtifacts, &metrics.ArchivedArtifacts, &metrics.DeletedArtifacts, &metrics.ArtifactBytes, &metrics.UniqueContentBytes)
	if err != nil {
		return contracts.ArtifactStorageMetrics{}, err
	}
	metrics.LogicalBytes = metrics.UniqueContentBytes
	if err := s.pool.QueryRow(ctx, `SELECT COALESCE(sum(a.byte_size),0) FROM fornix.artifact_refs r JOIN fornix.artifacts a ON a.workspace_id=r.workspace_id AND a.id=r.artifact_id WHERE r.workspace_id=$1`, workspaceID).Scan(&metrics.LogicalBytes); err != nil {
		return contracts.ArtifactStorageMetrics{}, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT COALESCE(sum(byte_size),0) FROM fornix.artifact_chunks WHERE workspace_id=$1`, workspaceID).Scan(&metrics.ChunkBytes); err != nil {
		return contracts.ArtifactStorageMetrics{}, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE authoritative) FROM fornix.artifact_refs WHERE workspace_id=$1`, workspaceID).Scan(&metrics.References, &metrics.AuthoritativeRefs); err != nil {
		return contracts.ArtifactStorageMetrics{}, err
	}
	if metrics.LogicalBytes > 0 {
		metrics.DedupRatio = float64(metrics.UniqueContentBytes) / float64(metrics.LogicalBytes)
	}
	return metrics, nil
}

func validJSON(value []byte) []byte {
	if len(value) == 0 || !json.Valid(value) {
		return []byte(`{}`)
	}
	return value
}

func modelRedactBounded(value []byte) []byte {
	// Keep this helper local to backfill so old rows receive the same bounded
	// compatibility evidence shape as live tool completion.
	return validJSON(model.RedactBytes(value))
}
