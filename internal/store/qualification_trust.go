package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrQualificationSignerNotFound    = errors.New("qualification trusted signer not found")
	ErrQualificationSignerConflict    = errors.New("qualification trusted signer conflicts with existing state")
	ErrQualificationSignerRevoked     = errors.New("qualification trusted signer is revoked")
	ErrQualificationSignerSuperseded  = errors.New("qualification trusted signer is superseded")
	ErrQualificationSignerNotYetValid = errors.New("qualification trusted signer is not yet valid")
	ErrQualificationSignerExpired     = errors.New("qualification trusted signer is expired")
	ErrQualificationImportNotFound    = errors.New("qualification import not found")
	ErrQualificationImportConflict    = errors.New("qualification import conflicts with existing state")
	ErrQualificationImportStale       = errors.New("qualification import is stale")
	ErrQualificationImportCursor      = errors.New("invalid qualification import cursor")
	ErrQualificationSignerCursor      = errors.New("invalid qualification signer cursor")
)

// QualificationTrustStore is the PostgreSQL authority for deployment-owned
// qualification signer lifecycle and authorized signed-evidence imports. It
// stores public keys and bounded signed bytes, never private keys or secrets.
type QualificationTrustStore struct {
	pool *pgxpool.Pool
}

// NewQualificationTrustStore constructs a qualification trust authority over
// the supplied PostgreSQL pool.
func NewQualificationTrustStore(pool *pgxpool.Pool) *QualificationTrustStore {
	return &QualificationTrustStore{pool: pool}
}

// RegisterSigner installs one immutable public key for a workspace and
// deployment. Supplying SupersedesKeyID atomically retires the predecessor
// for new imports and records both lifecycle events. Repeating the same
// immutable registration is idempotent.
func (s *QualificationTrustStore) RegisterSigner(ctx context.Context, input contracts.QualificationTrustedSignerInput) (contracts.QualificationTrustedSigner, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("qualification trust store is not configured")
	}
	if input.ValidFrom.IsZero() || input.ValidUntil.IsZero() {
		return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("qualification signer validity window is required")
	}
	publicKey, err := normalizeQualificationPublicKey(input.PublicKey)
	if err != nil {
		return contracts.QualificationTrustedSigner{}, false, err
	}
	now := time.Now().UTC()
	signer := contracts.QualificationTrustedSigner{
		SchemaVersion:   contracts.QualificationTrustSchemaVersion,
		ID:              contracts.NewID("qualification-signer"),
		WorkspaceID:     input.WorkspaceID,
		DeploymentID:    input.DeploymentID,
		KeyID:           input.KeyID,
		Algorithm:       contracts.QualificationSignatureAlgorithmEd25519,
		PublicKey:       hex.EncodeToString(publicKey),
		Status:          contracts.QualificationSignerActive,
		SupersedesKeyID: input.SupersedesKeyID,
		ValidFrom:       input.ValidFrom,
		ValidUntil:      input.ValidUntil,
		CreatedAt:       now,
		Actor:           input.Actor,
	}
	if err := signer.Normalize(); err != nil {
		return contracts.QualificationTrustedSigner{}, false, err
	}
	if signer.SupersedesKeyID == signer.KeyID {
		return contracts.QualificationTrustedSigner{}, false, ErrQualificationSignerConflict
	}
	actorJSON, err := json.Marshal(signer.Actor)
	if err != nil {
		return contracts.QualificationTrustedSigner{}, false, err
	}

	tx, err := beginWorkspaceTx(ctx, s.pool, signer.WorkspaceID)
	if err != nil {
		return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("begin qualification signer registration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-qualification-trust:' || $1 || ':' || $2, 0))`, signer.WorkspaceID, signer.DeploymentID); err != nil {
		return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("lock qualification signer scope: %w", err)
	}

	existing, err := queryQualificationSigner(ctx, tx, signer.WorkspaceID, signer.DeploymentID, signer.KeyID, true)
	if err == nil {
		if qualificationSignerEquivalent(existing, signer) && existing.Status == contracts.QualificationSignerActive {
			if err := tx.Commit(ctx); err != nil {
				return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("commit idempotent qualification signer registration: %w", err)
			}
			return existing, false, nil
		}
		if existing.Status == contracts.QualificationSignerRevoked {
			return contracts.QualificationTrustedSigner{}, false, ErrQualificationSignerRevoked
		}
		if existing.Status == contracts.QualificationSignerSuperseded {
			return contracts.QualificationTrustedSigner{}, false, ErrQualificationSignerSuperseded
		}
		return contracts.QualificationTrustedSigner{}, false, ErrQualificationSignerConflict
	}
	if !errors.Is(err, ErrQualificationSignerNotFound) {
		return contracts.QualificationTrustedSigner{}, false, err
	}

	var predecessor contracts.QualificationTrustedSigner
	if signer.SupersedesKeyID != "" {
		predecessor, err = queryQualificationSigner(ctx, tx, signer.WorkspaceID, signer.DeploymentID, signer.SupersedesKeyID, true)
		if err != nil {
			if errors.Is(err, ErrQualificationSignerNotFound) {
				return contracts.QualificationTrustedSigner{}, false, ErrQualificationSignerConflict
			}
			return contracts.QualificationTrustedSigner{}, false, err
		}
		if predecessor.Status != contracts.QualificationSignerActive {
			return contracts.QualificationTrustedSigner{}, false, ErrQualificationSignerConflict
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_trusted_signers
		  (id,workspace_id,deployment_id,key_id,signature_scheme,public_key,public_key_hash,status,supersedes_key_id,valid_from,valid_until,actor)
		VALUES($1,$2,$3,$4,'ed25519',$5,$6,'active',$7,$8,$9,$10::jsonb)`,
		signer.ID, signer.WorkspaceID, signer.DeploymentID, signer.KeyID, publicKey, signer.PublicKeyHash, nullableString(signer.SupersedesKeyID), signer.ValidFrom, signer.ValidUntil, actorJSON); err != nil {
		if isUniqueViolation(err) {
			return contracts.QualificationTrustedSigner{}, false, ErrQualificationSignerConflict
		}
		return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("insert qualification trusted signer: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_trusted_signer_events(workspace_id,deployment_id,signer_id,key_id,event,actor,metadata)
		VALUES($1,$2,$3,$4,'registered',$5::jsonb,$6::jsonb)`, signer.WorkspaceID, signer.DeploymentID, signer.ID, signer.KeyID, actorJSON, qualificationSignerMetadata(signer)); err != nil {
		return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("record qualification signer registration: %w", err)
	}
	if signer.SupersedesKeyID != "" {
		if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_trusted_signers SET status='superseded',superseded_by_key_id=$1,superseded_at=clock_timestamp() WHERE workspace_id=$2 AND deployment_id=$3 AND key_id=$4 AND status='active'`, signer.KeyID, signer.WorkspaceID, signer.DeploymentID, predecessor.KeyID); err != nil {
			return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("supersede qualification signer: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.qualification_trusted_signer_events(workspace_id,deployment_id,signer_id,key_id,event,actor,metadata)
			VALUES($1,$2,$3,$4,'superseded',$5::jsonb,$6::jsonb)`, predecessor.WorkspaceID, predecessor.DeploymentID, predecessor.ID, predecessor.KeyID, actorJSON, `{"superseded_by_key_id":"`+signer.KeyID+`"}`); err != nil {
			return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("record qualification signer supersession: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationTrustedSigner{}, false, fmt.Errorf("commit qualification signer registration: %w", err)
	}
	return signer, true, nil
}

// RevokeSigner immediately prevents the signer from authorizing new imports.
// Repeating revocation is idempotent; the existing append-only event remains
// the authoritative record of the first transition.
func (s *QualificationTrustStore) RevokeSigner(ctx context.Context, workspaceID, deploymentID, keyID string, actor contracts.AuditActor) error {
	workspaceID, deploymentID, keyID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(keyID)
	if s == nil || s.pool == nil {
		return fmt.Errorf("qualification trust store is not configured")
	}
	if workspaceID == "" || deploymentID == "" || keyID == "" {
		return ErrQualificationSignerNotFound
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
		return fmt.Errorf("begin qualification signer revocation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-qualification-trust:' || $1 || ':' || $2, 0))`, workspaceID, deploymentID); err != nil {
		return fmt.Errorf("lock qualification signer revocation: %w", err)
	}
	signer, err := queryQualificationSigner(ctx, tx, workspaceID, deploymentID, keyID, true)
	if err != nil {
		return err
	}
	if signer.Status == contracts.QualificationSignerRevoked {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit idempotent qualification signer revocation: %w", err)
		}
		return nil
	}
	if signer.Status == contracts.QualificationSignerSuperseded {
		return ErrQualificationSignerSuperseded
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_trusted_signers SET status='revoked',revoked_at=clock_timestamp() WHERE workspace_id=$1 AND deployment_id=$2 AND key_id=$3 AND status='active'`, workspaceID, deploymentID, keyID); err != nil {
		return fmt.Errorf("revoke qualification signer: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_trusted_signer_events(workspace_id,deployment_id,signer_id,key_id,event,actor) VALUES($1,$2,$3,$4,'revoked',$5::jsonb)`, workspaceID, deploymentID, signer.ID, keyID, actorJSON); err != nil {
		return fmt.Errorf("record qualification signer revocation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit qualification signer revocation: %w", err)
	}
	return nil
}

// ListSigners returns deterministic bounded signer metadata ordered by ID.
func (s *QualificationTrustStore) ListSigners(ctx context.Context, workspaceID, deploymentID string, limit int, cursor string) (contracts.QualificationSignerPage, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationSignerPage{}, fmt.Errorf("qualification trust store is not configured")
	}
	limit = boundedQualificationPageLimit(limit)
	workspaceID, deploymentID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" {
		return contracts.QualificationSignerPage{}, ErrQualificationSignerNotFound
	}
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.QualificationSignerPage{}, ErrQualificationSignerCursor
	}
	page := contracts.QualificationSignerPage{Items: make([]contracts.QualificationTrustedSigner, 0, limit)}
	err := workspaceQueryRows(ctx, s.pool, workspaceID, `
		SELECT id,workspace_id,deployment_id,key_id,signature_scheme,encode(public_key,'hex'),public_key_hash,status,
		       COALESCE(supersedes_key_id,''),COALESCE(superseded_by_key_id,''),valid_from,valid_until,revoked_at,superseded_at,created_at,actor
		FROM fornix.qualification_trusted_signers
		WHERE workspace_id=$1 AND deployment_id=$2 AND id>$3
		ORDER BY id LIMIT $4`, []any{workspaceID, deploymentID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			signer, err := scanQualificationSignerRows(rows)
			if err != nil {
				return err
			}
			page.Items = append(page.Items, signer)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.QualificationSignerPage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// ImportAuthorized verifies one signed bundle against the active trusted
// signer and appends its immutable evidence row. The returned booleans are
// deterministic: created is true only for the first committed import, while
// deduplicated is true for an exact idempotent replay. DryRun never mutates.
func (s *QualificationTrustStore) ImportAuthorized(ctx context.Context, request contracts.QualificationImportRequest, now time.Time) (contracts.QualificationImportResult, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationImportResult{}, fmt.Errorf("qualification trust store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if err := request.Normalize(); err != nil {
		return contracts.QualificationImportResult{}, err
	}
	raw, signed, err := qualificationSignedInputBytes(request)
	if err != nil {
		return contracts.QualificationImportResult{}, err
	}
	if signed.Bundle.Report.WorkspaceID != request.WorkspaceID || signed.Bundle.Report.TargetHash != request.TargetHash {
		return contracts.QualificationImportResult{}, ErrQualificationImportStale
	}
	sourceHash := sha256Hex(raw)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.QualificationImportResult{}, fmt.Errorf("begin qualification import: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-qualification-trust:' || $1 || ':' || $2, 0))`, request.WorkspaceID, request.DeploymentID); err != nil {
		return contracts.QualificationImportResult{}, fmt.Errorf("lock qualification import scope: %w", err)
	}
	// Resolve exact identity replays before requiring the current snapshot.
	// Historical Task 79 imports remain readable and idempotent even after a
	// publisher or predecessor snapshot is revoked; a new effect never does.
	existing, existingErr := queryQualificationImport(ctx, tx, request.WorkspaceID, request.DeploymentID, signed.Signature.SignedHash, true)
	if existingErr == nil {
		if existing.SourceHash != sourceHash || existing.IdempotencyKey != request.IdempotencyKey || existing.TargetHash != request.TargetHash {
			return contracts.QualificationImportResult{}, ErrQualificationImportConflict
		}
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return contracts.QualificationImportResult{Record: existing, Validated: true, Deduplicated: true, DryRun: true}, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationImportResult{}, fmt.Errorf("commit qualification import replay: %w", err)
		}
		return contracts.QualificationImportResult{Record: existing, Validated: true, Deduplicated: true}, nil
	}
	if !errors.Is(existingErr, ErrQualificationImportNotFound) {
		return contracts.QualificationImportResult{}, existingErr
	}
	var byIdempotency contracts.QualificationImportRecord
	byIdempotency, existingErr = queryQualificationImportByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true)
	if existingErr == nil {
		if byIdempotency.SignedHash != signed.Signature.SignedHash || byIdempotency.SourceHash != sourceHash {
			return contracts.QualificationImportResult{}, ErrQualificationImportConflict
		}
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return contracts.QualificationImportResult{Record: byIdempotency, Validated: true, Deduplicated: true, DryRun: true}, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.QualificationImportResult{}, fmt.Errorf("commit qualification import idempotency replay: %w", err)
		}
		return contracts.QualificationImportResult{Record: byIdempotency, Validated: true, Deduplicated: true}, nil
	}
	if !errors.Is(existingErr, ErrQualificationImportNotFound) {
		return contracts.QualificationImportResult{}, existingErr
	}
	snapshot, err := queryCurrentQualificationSnapshotTx(ctx, tx, request.WorkspaceID, request.DeploymentID, now)
	if err != nil {
		return contracts.QualificationImportResult{}, err
	}
	publicKey, ok := qualificationSnapshotSignerPublicKey(snapshot.Snapshot, signed.Signature.KeyID, signed.Signature.PublicKey, now)
	if !ok {
		return contracts.QualificationImportResult{}, ErrQualificationSnapshotSignerUntrusted
	}
	if err := signed.VerifyWithKey(signed.Signature.KeyID, publicKey); err != nil {
		return contracts.QualificationImportResult{}, ErrQualificationImportStale
	}
	// A snapshot can deliberately contain a signer that is not currently
	// active in the local catalog. Preserve a catalog ID when it is present;
	// otherwise the snapshot ID is the durable authority reference.
	signerRecordID := snapshot.ID
	if catalogSigner, catalogErr := queryQualificationSigner(ctx, tx, request.WorkspaceID, request.DeploymentID, signed.Signature.KeyID, false); catalogErr == nil && catalogSigner.PublicKey == signed.Signature.PublicKey {
		signerRecordID = catalogSigner.ID
	} else if catalogErr != nil && !errors.Is(catalogErr, ErrQualificationSignerNotFound) {
		return contracts.QualificationImportResult{}, catalogErr
	}
	expectedRecord := contracts.QualificationImportRecord{
		SchemaVersion:         contracts.QualificationTrustSchemaVersion,
		ID:                    contracts.NewID("qualification-import"),
		WorkspaceID:           request.WorkspaceID,
		DeploymentID:          request.DeploymentID,
		TargetHash:            request.TargetHash,
		KeyID:                 signed.Signature.KeyID,
		SignerRecordID:        signerRecordID,
		SignedHash:            signed.Signature.SignedHash,
		ObservationHash:       signed.ObservationHash,
		SourceHash:            sourceHash,
		SourceReference:       request.SourceReference,
		RequestID:             request.RequestID,
		IdempotencyKey:        request.IdempotencyKey,
		CausationID:           request.CausationID,
		CorrelationID:         request.CorrelationID,
		TrustSnapshotRevision: snapshot.Snapshot.Revision,
		TrustSnapshotHash:     snapshot.Snapshot.SnapshotHash,
		Status:                contracts.QualificationImportAccepted,
		Actor:                 request.Actor,
		CreatedAt:             now,
	}
	if err := expectedRecord.Normalize(); err != nil {
		return contracts.QualificationImportResult{}, err
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		expectedRecord.ID = "dry-run-" + expectedRecord.SignedHash[:16]
		return contracts.QualificationImportResult{Record: expectedRecord, Validated: true, DryRun: true}, nil
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.QualificationImportResult{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_imports
		  (id,workspace_id,deployment_id,target_hash,key_id,signer_record_id,signed_hash,observation_hash,source_hash,source_reference,request_id,idempotency_key,causation_id,correlation_id,trust_snapshot_revision,trust_snapshot_hash,status,signed_bytes,actor)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'accepted',$17,$18::jsonb)`,
		expectedRecord.ID, expectedRecord.WorkspaceID, expectedRecord.DeploymentID, expectedRecord.TargetHash, expectedRecord.KeyID, expectedRecord.SignerRecordID, expectedRecord.SignedHash, expectedRecord.ObservationHash, expectedRecord.SourceHash, nullableString(expectedRecord.SourceReference), nullableString(expectedRecord.RequestID), expectedRecord.IdempotencyKey, nullableString(expectedRecord.CausationID), nullableString(expectedRecord.CorrelationID), expectedRecord.TrustSnapshotRevision, expectedRecord.TrustSnapshotHash, raw, actorJSON); err != nil {
		if isUniqueViolation(err) {
			return contracts.QualificationImportResult{}, ErrQualificationImportConflict
		}
		return contracts.QualificationImportResult{}, fmt.Errorf("insert qualification import: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_import_events(workspace_id,deployment_id,import_id,event,actor,metadata) VALUES($1,$2,$3,'accepted',$4::jsonb,$5::jsonb)`, expectedRecord.WorkspaceID, expectedRecord.DeploymentID, expectedRecord.ID, actorJSON, qualificationImportMetadata(expectedRecord)); err != nil {
		return contracts.QualificationImportResult{}, fmt.Errorf("record qualification import event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.QualificationImportResult{}, fmt.Errorf("commit qualification import: %w", err)
	}
	return contracts.QualificationImportResult{Record: expectedRecord, Validated: true, Created: true}, nil
}

// ListImports returns bounded hash-only import records ordered by durable ID.
func (s *QualificationTrustStore) ListImports(ctx context.Context, workspaceID, deploymentID string, limit int, cursor string) (contracts.QualificationImportPage, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationImportPage{}, fmt.Errorf("qualification trust store is not configured")
	}
	limit = boundedQualificationPageLimit(limit)
	workspaceID, deploymentID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" {
		return contracts.QualificationImportPage{}, ErrQualificationImportNotFound
	}
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.QualificationImportPage{}, ErrQualificationImportCursor
	}
	page := contracts.QualificationImportPage{Items: make([]contracts.QualificationImportRecord, 0, limit)}
	err := workspaceQueryRows(ctx, s.pool, workspaceID, `
		SELECT id,workspace_id,deployment_id,target_hash,key_id,signer_record_id,signed_hash,observation_hash,source_hash,
		       COALESCE(source_reference,''),COALESCE(request_id,''),idempotency_key,COALESCE(causation_id,''),COALESCE(correlation_id,''),COALESCE(trust_snapshot_revision,0),COALESCE(trust_snapshot_hash,''),status,actor,created_at
		FROM fornix.qualification_imports
		WHERE workspace_id=$1 AND deployment_id=$2 AND id>$3
		ORDER BY id LIMIT $4`, []any{workspaceID, deploymentID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			record, err := scanQualificationImport(rows)
			if err != nil {
				return err
			}
			page.Items = append(page.Items, record)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.QualificationImportPage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// DiscloseImport returns one bounded signed envelope and its exact submitted
// bytes for an explicitly authorized workspace/deployment disclosure.
func (s *QualificationTrustStore) DiscloseImport(ctx context.Context, workspaceID, deploymentID, importID string) (contracts.QualificationImportDisclosure, error) {
	if s == nil || s.pool == nil {
		return contracts.QualificationImportDisclosure{}, fmt.Errorf("qualification trust store is not configured")
	}
	workspaceID, deploymentID, importID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(importID)
	if workspaceID == "" || deploymentID == "" || importID == "" {
		return contracts.QualificationImportDisclosure{}, ErrQualificationImportNotFound
	}
	var disclosure contracts.QualificationImportDisclosure
	err := workspaceQueryRow(ctx, s.pool, workspaceID, `
		SELECT id,workspace_id,deployment_id,target_hash,key_id,signer_record_id,signed_hash,observation_hash,source_hash,
		       COALESCE(source_reference,''),COALESCE(request_id,''),idempotency_key,COALESCE(causation_id,''),COALESCE(correlation_id,''),COALESCE(trust_snapshot_revision,0),COALESCE(trust_snapshot_hash,''),status,actor,created_at,signed_bytes
		FROM fornix.qualification_imports WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3`, []any{workspaceID, deploymentID, importID}, func(row pgx.Row) error {
		record, raw, err := scanQualificationImportWithBytes(row)
		if err != nil {
			return err
		}
		signed, err := decodeQualificationSignedBytes(raw)
		if err != nil {
			return err
		}
		disclosure = contracts.QualificationImportDisclosure{Record: record, SignedBundle: signed, SourceBytes: append([]byte(nil), raw...)}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationImportDisclosure{}, ErrQualificationImportNotFound
	}
	if err != nil {
		return contracts.QualificationImportDisclosure{}, err
	}
	return disclosure, nil
}

func queryQualificationSignerForImport(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, keyID string, now time.Time) (contracts.QualificationTrustedSigner, []byte, error) {
	signer, err := queryQualificationSigner(ctx, tx, workspaceID, deploymentID, keyID, true)
	if err != nil {
		return contracts.QualificationTrustedSigner{}, nil, err
	}
	switch signer.Status {
	case contracts.QualificationSignerRevoked:
		return contracts.QualificationTrustedSigner{}, nil, ErrQualificationSignerRevoked
	case contracts.QualificationSignerSuperseded:
		return contracts.QualificationTrustedSigner{}, nil, ErrQualificationSignerSuperseded
	}
	if now.Before(signer.ValidFrom) {
		return contracts.QualificationTrustedSigner{}, nil, ErrQualificationSignerNotYetValid
	}
	if !now.Before(signer.ValidUntil) {
		return contracts.QualificationTrustedSigner{}, nil, ErrQualificationSignerExpired
	}
	publicKey, err := qualificationPublicKeyBytes(signer.PublicKey)
	if err != nil {
		return contracts.QualificationTrustedSigner{}, nil, err
	}
	return signer, publicKey, nil
}

func queryQualificationSigner(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, keyID string, lock bool) (contracts.QualificationTrustedSigner, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	row := tx.QueryRow(ctx, `
		SELECT id,workspace_id,deployment_id,key_id,signature_scheme,encode(public_key,'hex'),public_key_hash,status,
		       COALESCE(supersedes_key_id,''),COALESCE(superseded_by_key_id,''),valid_from,valid_until,revoked_at,superseded_at,created_at,actor
		FROM fornix.qualification_trusted_signers
		WHERE workspace_id=$1 AND deployment_id=$2 AND key_id=$3`+lockClause, workspaceID, deploymentID, keyID)
	signer, err := scanQualificationSignerRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationTrustedSigner{}, ErrQualificationSignerNotFound
	}
	return signer, err
}

func queryQualificationImport(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, signedHash string, lock bool) (contracts.QualificationImportRecord, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	row := tx.QueryRow(ctx, `
		SELECT id,workspace_id,deployment_id,target_hash,key_id,signer_record_id,signed_hash,observation_hash,source_hash,
		       COALESCE(source_reference,''),COALESCE(request_id,''),idempotency_key,COALESCE(causation_id,''),COALESCE(correlation_id,''),COALESCE(trust_snapshot_revision,0),COALESCE(trust_snapshot_hash,''),status,actor,created_at
		FROM fornix.qualification_imports WHERE workspace_id=$1 AND deployment_id=$2 AND signed_hash=$3`+lockClause, workspaceID, deploymentID, signedHash)
	record, err := scanQualificationImport(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationImportRecord{}, ErrQualificationImportNotFound
	}
	return record, err
}

func queryQualificationImportByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, idempotency string, lock bool) (contracts.QualificationImportRecord, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	row := tx.QueryRow(ctx, `
		SELECT id,workspace_id,deployment_id,target_hash,key_id,signer_record_id,signed_hash,observation_hash,source_hash,
		       COALESCE(source_reference,''),COALESCE(request_id,''),idempotency_key,COALESCE(causation_id,''),COALESCE(correlation_id,''),COALESCE(trust_snapshot_revision,0),COALESCE(trust_snapshot_hash,''),status,actor,created_at
		FROM fornix.qualification_imports WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`+lockClause, workspaceID, deploymentID, idempotency)
	record, err := scanQualificationImport(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationImportRecord{}, ErrQualificationImportNotFound
	}
	return record, err
}

// qualificationSnapshotSignerPublicKey resolves the exact signer material
// authorized by the currently loaded snapshot. Snapshot entry public keys are
// compared in normalized form and converted only after the scope/window check.
func qualificationSnapshotSignerPublicKey(snapshot contracts.QualificationTrustSnapshot, keyID, publicKey string, now time.Time) (ed25519.PublicKey, bool) {
	if !snapshot.AuthorizesSigner(keyID, publicKey, now) {
		return nil, false
	}
	for _, entry := range snapshot.Entries {
		if entry.KeyID != strings.TrimSpace(keyID) || entry.PublicKey != strings.ToLower(strings.TrimSpace(publicKey)) {
			continue
		}
		decoded, err := qualificationPublicKeyBytes(entry.PublicKey)
		if err != nil {
			return nil, false
		}
		return ed25519.PublicKey(decoded), true
	}
	return nil, false
}

func scanQualificationSignerRow(row pgx.Row) (contracts.QualificationTrustedSigner, error) {
	var signer contracts.QualificationTrustedSigner
	var actorRaw []byte
	if err := row.Scan(&signer.ID, &signer.WorkspaceID, &signer.DeploymentID, &signer.KeyID, &signer.Algorithm, &signer.PublicKey, &signer.PublicKeyHash, &signer.Status, &signer.SupersedesKeyID, &signer.SupersededByKey, &signer.ValidFrom, &signer.ValidUntil, &signer.RevokedAt, &signer.SupersededAt, &signer.CreatedAt, &actorRaw); err != nil {
		return contracts.QualificationTrustedSigner{}, err
	}
	if err := json.Unmarshal(actorRaw, &signer.Actor); err != nil {
		return contracts.QualificationTrustedSigner{}, fmt.Errorf("decode qualification signer actor: %w", err)
	}
	signer.SchemaVersion = contracts.QualificationTrustSchemaVersion
	if err := signer.Normalize(); err != nil {
		return contracts.QualificationTrustedSigner{}, err
	}
	return signer, nil
}

func scanQualificationSignerRows(rows pgx.Rows) (contracts.QualificationTrustedSigner, error) {
	var signer contracts.QualificationTrustedSigner
	var actorRaw []byte
	if err := rows.Scan(&signer.ID, &signer.WorkspaceID, &signer.DeploymentID, &signer.KeyID, &signer.Algorithm, &signer.PublicKey, &signer.PublicKeyHash, &signer.Status, &signer.SupersedesKeyID, &signer.SupersededByKey, &signer.ValidFrom, &signer.ValidUntil, &signer.RevokedAt, &signer.SupersededAt, &signer.CreatedAt, &actorRaw); err != nil {
		return contracts.QualificationTrustedSigner{}, err
	}
	if err := json.Unmarshal(actorRaw, &signer.Actor); err != nil {
		return contracts.QualificationTrustedSigner{}, fmt.Errorf("decode qualification signer actor: %w", err)
	}
	signer.SchemaVersion = contracts.QualificationTrustSchemaVersion
	if err := signer.Normalize(); err != nil {
		return contracts.QualificationTrustedSigner{}, err
	}
	return signer, nil
}

func scanQualificationImport(row pgx.Row) (contracts.QualificationImportRecord, error) {
	var record contracts.QualificationImportRecord
	var actorRaw []byte
	if err := row.Scan(&record.ID, &record.WorkspaceID, &record.DeploymentID, &record.TargetHash, &record.KeyID, &record.SignerRecordID, &record.SignedHash, &record.ObservationHash, &record.SourceHash, &record.SourceReference, &record.RequestID, &record.IdempotencyKey, &record.CausationID, &record.CorrelationID, &record.TrustSnapshotRevision, &record.TrustSnapshotHash, &record.Status, &actorRaw, &record.CreatedAt); err != nil {
		return contracts.QualificationImportRecord{}, err
	}
	if err := json.Unmarshal(actorRaw, &record.Actor); err != nil {
		return contracts.QualificationImportRecord{}, fmt.Errorf("decode qualification import actor: %w", err)
	}
	record.SchemaVersion = contracts.QualificationTrustSchemaVersion
	if err := record.Normalize(); err != nil {
		return contracts.QualificationImportRecord{}, err
	}
	return record, nil
}

func scanQualificationImportWithBytes(row pgx.Row) (contracts.QualificationImportRecord, []byte, error) {
	var record contracts.QualificationImportRecord
	var actorRaw, raw []byte
	if err := row.Scan(&record.ID, &record.WorkspaceID, &record.DeploymentID, &record.TargetHash, &record.KeyID, &record.SignerRecordID, &record.SignedHash, &record.ObservationHash, &record.SourceHash, &record.SourceReference, &record.RequestID, &record.IdempotencyKey, &record.CausationID, &record.CorrelationID, &record.TrustSnapshotRevision, &record.TrustSnapshotHash, &record.Status, &actorRaw, &record.CreatedAt, &raw); err != nil {
		return contracts.QualificationImportRecord{}, nil, err
	}
	if err := json.Unmarshal(actorRaw, &record.Actor); err != nil {
		return contracts.QualificationImportRecord{}, nil, fmt.Errorf("decode qualification import actor: %w", err)
	}
	record.SchemaVersion = contracts.QualificationTrustSchemaVersion
	if err := record.Normalize(); err != nil {
		return contracts.QualificationImportRecord{}, nil, err
	}
	return record, append([]byte(nil), raw...), nil
}

func qualificationSignedInputBytes(request contracts.QualificationImportRequest) ([]byte, contracts.SignedQualificationBundle, error) {
	raw := append([]byte(nil), request.SignedBytes...)
	if len(raw) == 0 {
		var err error
		raw, err = json.Marshal(request.SignedBundle)
		if err != nil {
			return nil, contracts.SignedQualificationBundle{}, fmt.Errorf("marshal qualification signed bundle: %w", err)
		}
	}
	if len(raw) == 0 || len(raw) > contracts.MaxQualificationReportBytes {
		return nil, contracts.SignedQualificationBundle{}, fmt.Errorf("qualification signed bundle exceeds %d bytes", contracts.MaxQualificationReportBytes)
	}
	signed, err := decodeQualificationSignedBytes(raw)
	if err != nil {
		return nil, contracts.SignedQualificationBundle{}, err
	}
	return raw, signed, nil
}

func decodeQualificationSignedBytes(raw []byte) (contracts.SignedQualificationBundle, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var signed contracts.SignedQualificationBundle
	if err := decoder.Decode(&signed); err != nil {
		return contracts.SignedQualificationBundle{}, fmt.Errorf("signed qualification bundle is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return contracts.SignedQualificationBundle{}, fmt.Errorf("signed qualification bundle contains trailing data")
	}
	if err := signed.Verify(); err != nil {
		return contracts.SignedQualificationBundle{}, fmt.Errorf("signed qualification bundle failed verification")
	}
	return signed, nil
}

func normalizeQualificationPublicKey(value string) ([]byte, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 64 {
		return nil, fmt.Errorf("qualification public key is invalid")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return nil, fmt.Errorf("qualification public key is invalid")
	}
	return decoded, nil
}

func qualificationPublicKeyBytes(value string) (ed25519.PublicKey, error) {
	decoded, err := normalizeQualificationPublicKey(value)
	if err != nil {
		return nil, err
	}
	return ed25519.PublicKey(decoded), nil
}

func qualificationSignerEquivalent(left, right contracts.QualificationTrustedSigner) bool {
	// Actor and creation time describe the first durable registration; they are
	// not signer identity. A replay from a different authenticated operator
	// must return the original row rather than mutate its audit provenance.
	return left.WorkspaceID == right.WorkspaceID && left.DeploymentID == right.DeploymentID && left.KeyID == right.KeyID && left.Algorithm == right.Algorithm && left.PublicKey == right.PublicKey && left.PublicKeyHash == right.PublicKeyHash && left.SupersedesKeyID == right.SupersedesKeyID && left.ValidFrom.Equal(right.ValidFrom) && left.ValidUntil.Equal(right.ValidUntil)
}

func qualificationSignerMetadata(signer contracts.QualificationTrustedSigner) string {
	return `{"public_key_hash":"` + signer.PublicKeyHash + `","valid_from":"` + signer.ValidFrom.Format(time.RFC3339Nano) + `","valid_until":"` + signer.ValidUntil.Format(time.RFC3339Nano) + `"}`
}

func qualificationImportMetadata(record contracts.QualificationImportRecord) string {
	return `{"signed_hash":"` + record.SignedHash + `","observation_hash":"` + record.ObservationHash + `","source_hash":"` + record.SourceHash + `"}`
}

func validateQualificationActor(actor contracts.AuditActor, workspaceID string) error {
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(actor.WorkspaceID) != strings.TrimSpace(workspaceID) || len(actor.ID) > contracts.MaxDomainIDLength || len(actor.APIKeyID) > contracts.MaxDomainIDLength {
		return fmt.Errorf("qualification audit actor is invalid")
	}
	return nil
}

func boundedQualificationPageLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > contracts.MaxQualificationPageSize {
		return contracts.MaxQualificationPageSize
	}
	return limit
}

func validQualificationCursor(value string) bool {
	if value == "" || len(value) > contracts.MaxDomainIDLength {
		return false
	}
	return value != "" && len(value) <= contracts.MaxDomainIDLength && !strings.ContainsAny(value, " \t\r\n")
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func sha256Hex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
