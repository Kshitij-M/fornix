package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrQualificationSnapshotNotFound        = errors.New("qualification trust snapshot not found")
	ErrQualificationSnapshotConflict        = errors.New("qualification trust snapshot conflicts with existing state")
	ErrQualificationSnapshotDowngrade       = errors.New("qualification trust snapshot revision is not monotonic")
	ErrQualificationSnapshotRevoked         = errors.New("qualification trust snapshot is revoked")
	ErrQualificationSnapshotExpired         = errors.New("qualification trust snapshot is expired")
	ErrQualificationSnapshotSignerUntrusted = errors.New("qualification trust snapshot signer is not trusted")
	ErrQualificationSnapshotCursor          = errors.New("invalid qualification trust snapshot cursor")
)

// PublishSnapshot verifies and durably publishes a deployment trust snapshot.
// The Task 79 signer catalog authorizes the publisher; the snapshot itself is
// the bounded signer set used by new qualification imports.
func (s *QualificationTrustStore) PublishSnapshot(ctx context.Context, request contracts.QualificationTrustSnapshotPublishRequest, now time.Time) (contracts.QualificationTrustSnapshotRecord, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, fmt.Errorf("qualification trust store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if err := request.Normalize(); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, err
	}
	raw, signed, err := qualificationSnapshotInputBytes(request)
	if err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, err
	}
	if signed.WorkspaceID != request.WorkspaceID || signed.DeploymentID != request.DeploymentID {
		return contracts.QualificationTrustSnapshotRecord{}, false, ErrQualificationSnapshotConflict
	}
	if err := signed.VerifyWithKey("", nil, now); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, ErrQualificationSnapshotExpired
	}
	sourceHash := sha256Hex(raw)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, fmt.Errorf("begin qualification snapshot publish: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-qualification-trust-distribution:' || $1 || ':' || $2, 0))`, request.WorkspaceID, request.DeploymentID); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, fmt.Errorf("lock qualification snapshot scope: %w", err)
	}
	if existing, existingErr := queryQualificationSnapshotByHash(ctx, tx, request.WorkspaceID, request.DeploymentID, signed.SnapshotHash, true); existingErr == nil {
		if existing.SourceHash != sourceHash || existing.IdempotencyKey != request.IdempotencyKey {
			return contracts.QualificationTrustSnapshotRecord{}, false, ErrQualificationSnapshotConflict
		}
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return existing, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationTrustSnapshotRecord{}, false, fmt.Errorf("commit qualification snapshot replay: %w", err)
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrQualificationSnapshotNotFound) {
		return contracts.QualificationTrustSnapshotRecord{}, false, existingErr
	}
	if existing, existingErr := queryQualificationSnapshotByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true); existingErr == nil {
		if existing.SourceHash != sourceHash || existing.Snapshot.SnapshotHash != signed.SnapshotHash {
			return contracts.QualificationTrustSnapshotRecord{}, false, ErrQualificationSnapshotConflict
		}
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return existing, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationTrustSnapshotRecord{}, false, fmt.Errorf("commit qualification snapshot idempotency replay: %w", err)
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrQualificationSnapshotNotFound) {
		return contracts.QualificationTrustSnapshotRecord{}, false, existingErr
	}
	_, publisherKey, err := queryQualificationSignerForImport(ctx, tx, request.WorkspaceID, request.DeploymentID, signed.SignerKeyID, now)
	if err != nil {
		if errors.Is(err, ErrQualificationSignerNotFound) || errors.Is(err, ErrQualificationSignerRevoked) || errors.Is(err, ErrQualificationSignerSuperseded) || errors.Is(err, ErrQualificationSignerNotYetValid) || errors.Is(err, ErrQualificationSignerExpired) {
			return contracts.QualificationTrustSnapshotRecord{}, false, ErrQualificationSnapshotSignerUntrusted
		}
		return contracts.QualificationTrustSnapshotRecord{}, false, err
	}
	if err := signed.VerifyWithKey(signed.SignerKeyID, publisherKey, now); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, ErrQualificationSnapshotSignerUntrusted
	}
	var currentRevision int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(revision),0) FROM fornix.qualification_trust_snapshots WHERE workspace_id=$1 AND deployment_id=$2`, request.WorkspaceID, request.DeploymentID).Scan(&currentRevision); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, fmt.Errorf("read qualification snapshot revision: %w", err)
	}
	if signed.Revision <= currentRevision {
		return contracts.QualificationTrustSnapshotRecord{}, false, ErrQualificationSnapshotDowngrade
	}
	record := contracts.QualificationTrustSnapshotRecord{
		ID: contracts.NewID("qualification-snapshot"), Snapshot: signed, SourceHash: sourceHash,
		SourceReference: request.SourceReference, RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey,
		CausationID: request.CausationID, CorrelationID: request.CorrelationID, Actor: request.Actor, CreatedAt: now,
	}
	if err := record.Normalize(); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, err
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		record.ID = "dry-run-" + record.Snapshot.SnapshotHash[:16]
		return record, false, nil
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_trust_snapshots
		  (id,workspace_id,deployment_id,revision,snapshot_hash,source_hash,source_reference,signer_key_id,signature_scheme,signer_public_key,signature,issued_at,expires_at,status,signed_bytes,actor,request_id,idempotency_key,causation_id,correlation_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,'ed25519',$9,$10,$11,$12,'active',$13,$14::jsonb,$15,$16,$17,$18)`,
		record.ID, record.Snapshot.WorkspaceID, record.Snapshot.DeploymentID, record.Snapshot.Revision, record.Snapshot.SnapshotHash, record.SourceHash, nullableString(record.SourceReference), record.Snapshot.SignerKeyID, []byte(signed.SignerPublicKey), signed.Signature, record.Snapshot.IssuedAt, record.Snapshot.ExpiresAt, raw, actorJSON, nullableString(record.RequestID), record.IdempotencyKey, nullableString(record.CausationID), nullableString(record.CorrelationID)); err != nil {
		if isUniqueViolation(err) {
			return contracts.QualificationTrustSnapshotRecord{}, false, ErrQualificationSnapshotConflict
		}
		return contracts.QualificationTrustSnapshotRecord{}, false, fmt.Errorf("insert qualification trust snapshot: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_trust_snapshot_events(workspace_id,deployment_id,snapshot_id,revision,event,actor,metadata)
		VALUES($1,$2,$3,$4,'published',$5::jsonb,$6::jsonb)`, record.Snapshot.WorkspaceID, record.Snapshot.DeploymentID, record.ID, record.Snapshot.Revision, actorJSON, `{"snapshot_hash":"`+record.Snapshot.SnapshotHash+`","source_hash":"`+record.SourceHash+`"}`); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, fmt.Errorf("record qualification snapshot publication: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, false, fmt.Errorf("commit qualification trust snapshot: %w", err)
	}
	return record, true, nil
}

// CurrentSnapshot loads and re-verifies the current snapshot against the
// Task 79 publisher catalog. Revocation or expiry therefore takes effect
// without waiting for a process restart.
func (s *QualificationTrustStore) CurrentSnapshot(ctx context.Context, workspaceID, deploymentID string, now time.Time) (contracts.QualificationTrustSnapshotRecord, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationTrustSnapshotRecord{}, fmt.Errorf("qualification trust store is not configured")
	}
	workspaceID, deploymentID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID)
	if workspaceID == "" || deploymentID == "" {
		return contracts.QualificationTrustSnapshotRecord{}, ErrQualificationSnapshotNotFound
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := queryCurrentQualificationSnapshotTx(ctx, tx, workspaceID, deploymentID, now.UTC())
	if err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, err
	}
	return record, nil
}

// RevokeSnapshot prevents future imports from using a snapshot while keeping
// its signed bytes and publication event available for audit.
func (s *QualificationTrustStore) RevokeSnapshot(ctx context.Context, workspaceID, deploymentID, snapshotID string, actor contracts.AuditActor) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("qualification trust store is not configured")
	}
	workspaceID, deploymentID, snapshotID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(snapshotID)
	if workspaceID == "" || deploymentID == "" || snapshotID == "" {
		return ErrQualificationSnapshotNotFound
	}
	if err := validateQualificationActor(actor, workspaceID); err != nil {
		return err
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-qualification-trust-distribution:' || $1 || ':' || $2, 0))`, workspaceID, deploymentID); err != nil {
		return err
	}
	var revision int64
	var status string
	if err := tx.QueryRow(ctx, `SELECT revision,status FROM fornix.qualification_trust_snapshots WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3 FOR UPDATE`, workspaceID, deploymentID, snapshotID).Scan(&revision, &status); errors.Is(err, pgx.ErrNoRows) {
		return ErrQualificationSnapshotNotFound
	} else if err != nil {
		return err
	}
	if status == contracts.QualificationSnapshotRevoked {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_trust_snapshots SET status='revoked' WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3 AND status='active'`, workspaceID, deploymentID, snapshotID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_trust_snapshot_events(workspace_id,deployment_id,snapshot_id,revision,event,actor) VALUES($1,$2,$3,$4,'revoked',$5::jsonb)`, workspaceID, deploymentID, snapshotID, revision, actorJSON); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListSnapshots returns bounded metadata ordered by stable durable ID.
func (s *QualificationTrustStore) ListSnapshots(ctx context.Context, workspaceID, deploymentID string, limit int, cursor string) (contracts.QualificationTrustSnapshotPage, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationTrustSnapshotPage{}, fmt.Errorf("qualification trust store is not configured")
	}
	limit = boundedQualificationPageLimit(limit)
	workspaceID, deploymentID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" {
		return contracts.QualificationTrustSnapshotPage{}, ErrQualificationSnapshotNotFound
	}
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.QualificationTrustSnapshotPage{}, ErrQualificationSnapshotCursor
	}
	page := contracts.QualificationTrustSnapshotPage{Items: make([]contracts.QualificationTrustSnapshotRecord, 0, limit)}
	err := workspaceQueryRows(ctx, s.pool, workspaceID, qualificationSnapshotSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id>$3 ORDER BY id LIMIT $4`, []any{workspaceID, deploymentID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			record, err := scanQualificationSnapshot(rows)
			if err != nil {
				return err
			}
			page.Items = append(page.Items, record)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.QualificationTrustSnapshotPage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

func (s *QualificationTrustStore) DiscloseSnapshot(ctx context.Context, workspaceID, deploymentID, snapshotID string) (contracts.QualificationTrustSnapshotDisclosure, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationTrustSnapshotDisclosure{}, fmt.Errorf("qualification trust store is not configured")
	}
	workspaceID, deploymentID, snapshotID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(snapshotID)
	if workspaceID == "" || deploymentID == "" || snapshotID == "" {
		return contracts.QualificationTrustSnapshotDisclosure{}, ErrQualificationSnapshotNotFound
	}
	var disclosure contracts.QualificationTrustSnapshotDisclosure
	err := workspaceQueryRow(ctx, s.pool, workspaceID, qualificationSnapshotSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3`, []any{workspaceID, deploymentID, snapshotID}, func(row pgx.Row) error {
		record, raw, err := scanQualificationSnapshotWithBytes(row)
		if err != nil {
			return err
		}
		disclosure = contracts.QualificationTrustSnapshotDisclosure{Record: record, SignedSnapshot: record.Snapshot, SourceBytes: raw}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationTrustSnapshotDisclosure{}, ErrQualificationSnapshotNotFound
	}
	if err != nil {
		return contracts.QualificationTrustSnapshotDisclosure{}, err
	}
	return disclosure, nil
}

func qualificationSnapshotSelect() string {
	return `SELECT id,workspace_id,deployment_id,revision,snapshot_hash,source_hash,COALESCE(source_reference,''),signer_key_id,signature_scheme,encode(signer_public_key,'hex'),signature,issued_at,expires_at,status,signed_bytes,actor,COALESCE(request_id,''),idempotency_key,COALESCE(causation_id,''),COALESCE(correlation_id,''),created_at FROM fornix.qualification_trust_snapshots`
}

func queryCurrentQualificationSnapshotTx(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID string, now time.Time) (contracts.QualificationTrustSnapshotRecord, error) {
	row := tx.QueryRow(ctx, qualificationSnapshotSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND status='active' AND expires_at>$3 ORDER BY revision DESC LIMIT 1`, workspaceID, deploymentID, now)
	record, raw, err := scanQualificationSnapshotWithBytes(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationTrustSnapshotRecord{}, ErrQualificationSnapshotNotFound
	}
	if err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, err
	}
	_, publisherKey, err := queryQualificationSignerForImport(ctx, tx, workspaceID, deploymentID, record.Snapshot.SignerKeyID, now)
	if err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, ErrQualificationSnapshotSignerUntrusted
	}
	if err := record.Snapshot.VerifyWithKey(record.Snapshot.SignerKeyID, publisherKey, now); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, ErrQualificationSnapshotSignerUntrusted
	}
	if len(raw) == 0 {
		return contracts.QualificationTrustSnapshotRecord{}, fmt.Errorf("qualification trust snapshot has no source bytes")
	}
	return record, nil
}

func queryQualificationSnapshotByHash(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, snapshotHash string, lock bool) (contracts.QualificationTrustSnapshotRecord, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	row := tx.QueryRow(ctx, qualificationSnapshotSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND snapshot_hash=$3`+lockClause, workspaceID, deploymentID, snapshotHash)
	return scanQualificationSnapshotOrNotFound(row)
}

func queryQualificationSnapshotByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, idempotency string, lock bool) (contracts.QualificationTrustSnapshotRecord, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	row := tx.QueryRow(ctx, qualificationSnapshotSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`+lockClause, workspaceID, deploymentID, idempotency)
	return scanQualificationSnapshotOrNotFound(row)
}

func scanQualificationSnapshotOrNotFound(row pgx.Row) (contracts.QualificationTrustSnapshotRecord, error) {
	record, _, err := scanQualificationSnapshotWithBytes(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationTrustSnapshotRecord{}, ErrQualificationSnapshotNotFound
	}
	return record, err
}

func scanQualificationSnapshot(row pgx.Row) (contracts.QualificationTrustSnapshotRecord, error) {
	record, _, err := scanQualificationSnapshotWithBytes(row)
	return record, err
}

func scanQualificationSnapshotWithBytes(row pgx.Row) (contracts.QualificationTrustSnapshotRecord, []byte, error) {
	var record contracts.QualificationTrustSnapshotRecord
	var actorRaw, signedBytes []byte
	var revision int64
	var signatureScheme, signerPublicKey, signature, status string
	if err := row.Scan(&record.ID, &record.Snapshot.WorkspaceID, &record.Snapshot.DeploymentID, &revision, &record.Snapshot.SnapshotHash, &record.SourceHash, &record.SourceReference, &record.Snapshot.SignerKeyID, &signatureScheme, &signerPublicKey, &signature, &record.Snapshot.IssuedAt, &record.Snapshot.ExpiresAt, &status, &signedBytes, &actorRaw, &record.RequestID, &record.IdempotencyKey, &record.CausationID, &record.CorrelationID, &record.CreatedAt); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, nil, err
	}
	if err := json.Unmarshal(actorRaw, &record.Actor); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, nil, fmt.Errorf("decode qualification snapshot actor: %w", err)
	}
	var signed contracts.QualificationTrustSnapshot
	if err := decodeQualificationSnapshotBytes(signedBytes, &signed); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, nil, err
	}
	if signed.WorkspaceID != record.Snapshot.WorkspaceID || signed.DeploymentID != record.Snapshot.DeploymentID || signed.Revision != revision || signed.SnapshotHash != record.Snapshot.SnapshotHash || signed.SignerKeyID != record.Snapshot.SignerKeyID || signed.SignerPublicKey != signerPublicKey || signed.SignatureScheme != signatureScheme || signed.Signature != signature {
		return contracts.QualificationTrustSnapshotRecord{}, nil, fmt.Errorf("qualification snapshot source hash identity mismatch")
	}
	if record.SourceHash != sha256Hex(signedBytes) {
		return contracts.QualificationTrustSnapshotRecord{}, nil, fmt.Errorf("qualification snapshot source bytes hash mismatch")
	}
	signed.Status = status
	record.Snapshot = signed
	if err := record.Normalize(); err != nil {
		return contracts.QualificationTrustSnapshotRecord{}, nil, err
	}
	return record, append([]byte(nil), signedBytes...), nil
}

func qualificationSnapshotInputBytes(request contracts.QualificationTrustSnapshotPublishRequest) ([]byte, contracts.QualificationTrustSnapshot, error) {
	raw := append([]byte(nil), request.SignedBytes...)
	if len(raw) == 0 {
		var err error
		raw, err = json.Marshal(request.SignedSnapshot)
		if err != nil {
			return nil, contracts.QualificationTrustSnapshot{}, err
		}
	}
	if len(raw) == 0 || len(raw) > contracts.MaxQualificationTrustSnapshotBytes {
		return nil, contracts.QualificationTrustSnapshot{}, fmt.Errorf("qualification trust snapshot exceeds %d bytes", contracts.MaxQualificationTrustSnapshotBytes)
	}
	var signed contracts.QualificationTrustSnapshot
	if err := decodeQualificationSnapshotBytes(raw, &signed); err != nil {
		return nil, contracts.QualificationTrustSnapshot{}, err
	}
	return raw, signed, nil
}

func decodeQualificationSnapshotBytes(raw []byte, target *contracts.QualificationTrustSnapshot) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("qualification trust snapshot is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("qualification trust snapshot contains trailing data")
	}
	return target.Normalize()
}
