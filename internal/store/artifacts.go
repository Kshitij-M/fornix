package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrArtifactNotFound    = errors.New("artifact not found")
	ErrArtifactConflict    = errors.New("artifact identity conflict")
	ErrArtifactIntegrity   = errors.New("artifact integrity check failed")
	ErrArtifactDeleted     = errors.New("artifact raw content has been deleted")
	ErrArtifactReferenced  = errors.New("artifact has authoritative references")
	ErrArtifactRetention   = errors.New("artifact retention transition is not allowed")
	ErrArtifactInvalid     = errors.New("invalid artifact")
	ErrArtifactRefConflict = errors.New("artifact reference conflict")
)

// ArtifactStore is the Postgres authority for immutable content-addressed
// bytes, derived disclosure manifests, references, and provenance links.
type ArtifactStore struct {
	pool        *pgxpool.Pool
	failureHook func(string) error
}

type ArtifactPutInput struct {
	WorkspaceID      string
	Kind             string
	MediaType        string
	Raw              []byte
	Manifest         contracts.ArtifactManifest
	Retention        contracts.RetentionPolicy
	SourceKind       string
	SourceID         string
	Role             string
	IdempotencyKey   string
	CausationID      string
	CorrelationID    string
	Actor            contracts.ActorRef
	NonAuthoritative bool
}

type ArtifactPutResult struct {
	Artifact   contracts.Artifact    `json:"artifact"`
	Reference  contracts.ArtifactRef `json:"reference"`
	Created    bool                  `json:"created"`
	RefCreated bool                  `json:"reference_created"`
}

type ArtifactProvenanceInput struct {
	WorkspaceID  string
	FromArtifact int64
	ToArtifact   int64
	Relation     string
	Metadata     map[string]string
}

func NewArtifactStore(pool *pgxpool.Pool) *ArtifactStore {
	return &ArtifactStore{pool: pool}
}

// SetFailureHook is intentionally small and deterministic so integration
// tests can force a rollback at a named transaction boundary. Production
// callers should leave it nil.
func (s *ArtifactStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

func (s *ArtifactStore) Put(ctx context.Context, input ArtifactPutInput) (ArtifactPutResult, error) {
	if s == nil || s.pool == nil {
		return ArtifactPutResult{}, fmt.Errorf("artifact store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ArtifactPutResult{}, fmt.Errorf("begin artifact write: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.PutTx(ctx, tx, input)
	if err != nil {
		return ArtifactPutResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactPutResult{}, fmt.Errorf("commit artifact write: %w", err)
	}
	return result, nil
}

// PutTx creates or reuses one canonical artifact and appends its source
// reference in the caller's transaction. No raw identity or existing chunk
// can be overwritten.
func (s *ArtifactStore) PutTx(ctx context.Context, tx pgx.Tx, input ArtifactPutInput) (ArtifactPutResult, error) {
	normalized, contentHash, manifestJSON, requestHash, err := normalizeArtifactInput(input)
	if err != nil {
		return ArtifactPutResult{}, err
	}
	if err := s.fail("normalized"); err != nil {
		return ArtifactPutResult{}, err
	}

	var artifactID int64
	var created bool
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.artifacts(
			workspace_id, content_hash, kind, media_type, byte_size, chunk_size,
			chunk_count, manifest, retain_until, archive_after, delete_after, allow_delete
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12)
		ON CONFLICT (workspace_id, content_hash) DO NOTHING
		RETURNING id`, normalized.WorkspaceID, contentHash, normalized.Kind,
		normalized.MediaType, len(normalized.Raw), contracts.DefaultArtifactChunkBytes,
		chunkCount(len(normalized.Raw)), manifestJSON, normalized.Retention.RetainUntil,
		normalized.Retention.ArchiveAfter, normalized.Retention.DeleteAfter,
		normalized.Retention.AllowDelete).Scan(&artifactID)
	if err == nil {
		created = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return ArtifactPutResult{}, fmt.Errorf("insert artifact: %w", err)
	} else {
		var existing contracts.Artifact
		existing, err = readArtifactByHashTx(ctx, tx, normalized.WorkspaceID, contentHash, true)
		if err != nil {
			return ArtifactPutResult{}, err
		}
		if existing.Status == contracts.ArtifactDeleted {
			return ArtifactPutResult{}, ErrArtifactDeleted
		}
		existingManifest, manifestErr := existing.Manifest.Normalize()
		if manifestErr != nil {
			return ArtifactPutResult{}, fmt.Errorf("normalize existing artifact manifest: %w", manifestErr)
		}
		if existing.Kind != normalized.Kind || existing.MediaType != normalized.MediaType ||
			existing.ByteSize != int64(len(normalized.Raw)) || !reflect.DeepEqual(existingManifest, normalized.Manifest) {
			return ArtifactPutResult{}, fmt.Errorf("%w for workspace=%q hash=%q", ErrArtifactConflict, normalized.WorkspaceID, contentHash)
		}
		artifactID = existing.ID
		if err := verifyArtifactBytes(existing, normalized.Raw, nil); err != nil {
			return ArtifactPutResult{}, err
		}
	}
	if created {
		if err := s.fail("artifact_inserted"); err != nil {
			return ArtifactPutResult{}, err
		}
		for index, offset := 0, 0; offset < len(normalized.Raw); index, offset = index+1, offset+contracts.DefaultArtifactChunkBytes {
			end := offset + contracts.DefaultArtifactChunkBytes
			if end > len(normalized.Raw) {
				end = len(normalized.Raw)
			}
			chunk := normalized.Raw[offset:end]
			if _, err := tx.Exec(ctx, `
				INSERT INTO fornix.artifact_chunks(
					workspace_id, artifact_id, chunk_index, content_hash, byte_size, raw_bytes
				) VALUES($1,$2,$3,$4,$5,$6)`, normalized.WorkspaceID, artifactID, index,
				contracts.ArtifactContentHash(chunk), len(chunk), chunk); err != nil {
				return ArtifactPutResult{}, fmt.Errorf("insert artifact chunk %d: %w", index, err)
			}
		}
		if err := s.fail("chunks_inserted"); err != nil {
			return ArtifactPutResult{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE fornix.artifacts SET integrity_state='valid', integrity_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, normalized.WorkspaceID, artifactID); err != nil {
			return ArtifactPutResult{}, fmt.Errorf("record new artifact integrity: %w", err)
		}
	}

	actorJSON, err := json.Marshal(normalized.Actor)
	if err != nil {
		return ArtifactPutResult{}, fmt.Errorf("marshal artifact actor: %w", err)
	}
	authoritative := !normalized.NonAuthoritative
	var ref contracts.ArtifactRef
	var refCreated bool
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.artifact_refs(
			workspace_id, artifact_id, source_kind, source_id, role,
			idempotency_key, request_hash, actor, authoritative, causation_id, correlation_id
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11)
		ON CONFLICT (workspace_id, idempotency_key) DO NOTHING
		RETURNING id, artifact_id, workspace_id, source_kind, source_id, role,
			idempotency_key, causation_id, correlation_id, created_at`, normalized.WorkspaceID, artifactID,
		normalized.SourceKind, normalized.SourceID, normalized.Role,
		normalized.IdempotencyKey, requestHash, actorJSON, authoritative, normalized.CausationID, normalized.CorrelationID).Scan(
		&ref.ID, &ref.ArtifactID, &ref.WorkspaceID, &ref.SourceKind,
		&ref.SourceID, &ref.Role, &ref.IdempotencyKey, &ref.CausationID, &ref.CorrelationID, &ref.CreatedAt)
	if err == nil {
		refCreated = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return ArtifactPutResult{}, fmt.Errorf("insert artifact reference: %w", err)
	} else {
		ref, err = readArtifactRefByIdempotencyTx(ctx, tx, normalized.WorkspaceID, normalized.IdempotencyKey)
		if errors.Is(err, ErrArtifactNotFound) {
			ref, err = readArtifactRefByIdentityTx(ctx, tx, normalized.WorkspaceID, normalized.SourceKind, normalized.SourceID, normalized.Role)
		}
		if err != nil {
			return ArtifactPutResult{}, err
		}
		var existingRequestHash string
		if err := tx.QueryRow(ctx, `SELECT request_hash FROM fornix.artifact_refs WHERE workspace_id=$1 AND id=$2`, normalized.WorkspaceID, ref.ID).Scan(&existingRequestHash); err != nil {
			return ArtifactPutResult{}, fmt.Errorf("read artifact reference identity: %w", err)
		}
		if existingRequestHash != requestHash || ref.ArtifactID != artifactID {
			return ArtifactPutResult{}, fmt.Errorf("%w for workspace=%q source=%q/%q", ErrArtifactRefConflict, normalized.WorkspaceID, normalized.SourceKind, normalized.SourceID)
		}
	}
	if err := s.fail("reference_inserted"); err != nil {
		return ArtifactPutResult{}, err
	}
	artifact, err := readArtifactTx(ctx, tx, normalized.WorkspaceID, artifactID, false)
	if err != nil {
		return ArtifactPutResult{}, err
	}
	ref.MediaType = artifact.MediaType
	ref.ContentHash = artifact.ContentHash
	ref.ByteSize = artifact.ByteSize
	ref.SchemaVersion = contracts.ArtifactSchemaVersion
	return ArtifactPutResult{Artifact: artifact, Reference: ref, Created: created, RefCreated: refCreated}, nil
}

func (s *ArtifactStore) Get(ctx context.Context, workspaceID string, artifactID int64) (contracts.Artifact, error) {
	if s == nil || s.pool == nil {
		return contracts.Artifact{}, fmt.Errorf("artifact store is not configured")
	}
	return readArtifact(ctx, s.pool, strings.TrimSpace(workspaceID), artifactID, false)
}

func (s *ArtifactStore) GetByHash(ctx context.Context, workspaceID, contentHash string) (contracts.Artifact, error) {
	if s == nil || s.pool == nil {
		return contracts.Artifact{}, fmt.Errorf("artifact store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.Artifact{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	artifact, err := readArtifactByHashTx(ctx, tx, strings.TrimSpace(workspaceID), strings.ToLower(strings.TrimSpace(contentHash)), false)
	if err != nil {
		return contracts.Artifact{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.Artifact{}, err
	}
	return artifact, nil
}

func (s *ArtifactStore) Disclose(ctx context.Context, request contracts.ArtifactDisclosureRequest) (contracts.ArtifactDisclosureResult, error) {
	if s == nil || s.pool == nil {
		return contracts.ArtifactDisclosureResult{}, fmt.Errorf("artifact store is not configured")
	}
	normalized, err := request.Normalize()
	if err != nil {
		return contracts.ArtifactDisclosureResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ArtifactDisclosureResult{}, fmt.Errorf("begin artifact disclosure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var artifact contracts.Artifact
	if normalized.ArtifactID > 0 {
		artifact, err = readArtifactTx(ctx, tx, normalized.WorkspaceID, normalized.ArtifactID, false)
	} else {
		artifact, err = readArtifactByHashTx(ctx, tx, normalized.WorkspaceID, normalized.ContentHash, false)
	}
	if err != nil {
		return contracts.ArtifactDisclosureResult{}, err
	}
	if artifact.Status == contracts.ArtifactDeleted {
		return contracts.ArtifactDisclosureResult{}, ErrArtifactDeleted
	}
	raw, err := readArtifactRawTx(ctx, tx, artifact)
	if err != nil {
		return contracts.ArtifactDisclosureResult{}, err
	}
	if err := verifyArtifactBytes(artifact, raw, nil); err != nil {
		return contracts.ArtifactDisclosureResult{}, err
	}
	refs, err := readArtifactRefsTx(ctx, tx, artifact.WorkspaceID, artifact.ID, normalized.MaxItems+1)
	if err != nil {
		return contracts.ArtifactDisclosureResult{}, err
	}
	result := contracts.ArtifactDisclosureResult{
		SchemaVersion: contracts.ArtifactSchemaVersion, WorkspaceID: artifact.WorkspaceID,
		ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, Status: artifact.Status,
		Level: normalized.Level, MediaType: artifact.MediaType, ByteSize: artifact.ByteSize,
		References:        refs,
		IntegrityVerified: true,
	}
	if len(result.References) > normalized.MaxItems {
		result.References = result.References[:normalized.MaxItems]
		result.Truncated = true
	}
	if normalized.IncludeProvenance && normalized.MaxDepth > 0 {
		budget := normalized.MaxItems - len(result.References)
		if budget < 1 {
			result.Truncated = true
		} else {
			result.Provenance, err = readArtifactProvenanceTx(ctx, tx, artifact.WorkspaceID, artifact.ID, normalized.MaxDepth, budget)
			if err != nil {
				return contracts.ArtifactDisclosureResult{}, err
			}
		}
	}
	remainingBytes, remainingTokens := normalized.MaxBytes, normalized.MaxTokens
	result.Gist, result.Truncated = fitArtifactText(artifact.Manifest.Gist, remainingBytes, remainingTokens, result.Truncated)
	result.TotalBytes += len(result.Gist)
	result.TotalTokens += contracts.EstimateTokens(result.Gist)
	remainingBytes -= len(result.Gist)
	remainingTokens -= contracts.EstimateTokens(result.Gist)
	if normalized.Level == contracts.ArtifactDisclosureDetail || normalized.Level == contracts.ArtifactDisclosureRaw {
		result.Detail, result.Truncated = fitArtifactText(artifact.Manifest.Detail, remainingBytes, remainingTokens, result.Truncated)
		result.TotalBytes += len(result.Detail)
		result.TotalTokens += contracts.EstimateTokens(result.Detail)
		remainingBytes -= len(result.Detail)
		remainingTokens -= contracts.EstimateTokens(result.Detail)
	}
	if normalized.Level == contracts.ArtifactDisclosureRaw {
		rawTokens := contracts.EstimateTokens(string(raw))
		if len(raw) <= remainingBytes && rawTokens <= remainingTokens {
			result.Raw = append([]byte(nil), raw...)
			result.TotalBytes += len(raw)
			result.TotalTokens += rawTokens
		} else {
			result.Truncated = true
		}
	}
	result.ContentViewHash, err = artifactDisclosureHash(result)
	if err != nil {
		return contracts.ArtifactDisclosureResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ArtifactDisclosureResult{}, fmt.Errorf("commit artifact disclosure: %w", err)
	}
	return result, nil
}

func (s *ArtifactStore) Verify(ctx context.Context, workspaceID string, artifactID int64) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("artifact store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	artifact, err := readArtifactTx(ctx, tx, strings.TrimSpace(workspaceID), artifactID, true)
	if err != nil {
		return err
	}
	if artifact.Status == contracts.ArtifactDeleted {
		return ErrArtifactDeleted
	}
	raw, err := readArtifactRawTx(ctx, tx, artifact)
	if err != nil {
		_, _ = tx.Exec(ctx, `UPDATE fornix.artifacts SET integrity_state='corrupt', integrity_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, workspaceID, artifactID)
		_ = tx.Commit(ctx)
		return err
	}
	if err := verifyArtifactBytes(artifact, raw, nil); err != nil {
		_, _ = tx.Exec(ctx, `UPDATE fornix.artifacts SET integrity_state='corrupt', integrity_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, workspaceID, artifactID)
		_ = tx.Commit(ctx)
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.artifacts SET integrity_state='valid', integrity_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, workspaceID, artifactID); err != nil {
		return fmt.Errorf("record artifact integrity: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit artifact integrity: %w", err)
	}
	return nil
}

func (s *ArtifactStore) Archive(ctx context.Context, workspaceID string, artifactID int64) (contracts.Artifact, error) {
	if s == nil || s.pool == nil {
		return contracts.Artifact{}, fmt.Errorf("artifact store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.Artifact{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	artifact, err := readArtifactTx(ctx, tx, strings.TrimSpace(workspaceID), artifactID, true)
	if err != nil {
		return contracts.Artifact{}, err
	}
	if artifact.Status == contracts.ArtifactDeleted {
		return contracts.Artifact{}, ErrArtifactDeleted
	}
	if artifact.Status == contracts.ArtifactActive {
		if _, err := tx.Exec(ctx, `UPDATE fornix.artifacts SET status='archived', archived_at=COALESCE(archived_at,clock_timestamp()) WHERE workspace_id=$1 AND id=$2`, workspaceID, artifactID); err != nil {
			return contracts.Artifact{}, fmt.Errorf("archive artifact: %w", err)
		}
		if err := recordArtifactLifecycleTx(ctx, tx, workspaceID, artifactID, "archive", contracts.ArtifactActive, contracts.ArtifactArchived, contracts.ActorRef{}, "", ""); err != nil {
			return contracts.Artifact{}, fmt.Errorf("record artifact archive: %w", err)
		}
	}
	artifact, err = readArtifactTx(ctx, tx, workspaceID, artifactID, false)
	if err != nil {
		return contracts.Artifact{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.Artifact{}, fmt.Errorf("commit artifact archive: %w", err)
	}
	return artifact, nil
}

func (s *ArtifactStore) Delete(ctx context.Context, workspaceID string, artifactID int64) (contracts.Artifact, error) {
	if s == nil || s.pool == nil {
		return contracts.Artifact{}, fmt.Errorf("artifact store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.Artifact{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	workspaceID = strings.TrimSpace(workspaceID)
	artifact, err := readArtifactTx(ctx, tx, workspaceID, artifactID, true)
	if err != nil {
		return contracts.Artifact{}, err
	}
	if artifact.Status == contracts.ArtifactDeleted {
		return artifact, nil
	}
	if artifact.Status != contracts.ArtifactArchived || !artifact.Retention.AllowDelete || artifact.Retention.DeleteAfter == nil || artifact.Retention.DeleteAfter.After(time.Now().UTC()) {
		return contracts.Artifact{}, ErrArtifactRetention
	}
	var authoritative int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM fornix.artifact_refs WHERE workspace_id=$1 AND artifact_id=$2 AND authoritative`, workspaceID, artifactID).Scan(&authoritative); err != nil {
		return contracts.Artifact{}, fmt.Errorf("count artifact references: %w", err)
	}
	if authoritative > 0 {
		return contracts.Artifact{}, ErrArtifactReferenced
	}
	if _, err := tx.Exec(ctx, `DELETE FROM fornix.artifact_chunks WHERE workspace_id=$1 AND artifact_id=$2`, workspaceID, artifactID); err != nil {
		return contracts.Artifact{}, fmt.Errorf("delete artifact chunks: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.artifacts SET status='deleted', deleted_at=COALESCE(deleted_at,clock_timestamp()), integrity_state='unknown' WHERE workspace_id=$1 AND id=$2`, workspaceID, artifactID); err != nil {
		return contracts.Artifact{}, fmt.Errorf("tombstone artifact: %w", err)
	}
	if err := recordArtifactLifecycleTx(ctx, tx, workspaceID, artifactID, "delete", contracts.ArtifactArchived, contracts.ArtifactDeleted, contracts.ActorRef{}, "", ""); err != nil {
		return contracts.Artifact{}, fmt.Errorf("record artifact deletion: %w", err)
	}
	artifact, err = readArtifactTx(ctx, tx, workspaceID, artifactID, false)
	if err != nil {
		return contracts.Artifact{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.Artifact{}, fmt.Errorf("commit artifact deletion: %w", err)
	}
	return artifact, nil
}

func (s *ArtifactStore) AddProvenance(ctx context.Context, input ArtifactProvenanceInput) (contracts.ArtifactProvenanceLink, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.ArtifactProvenanceLink{}, false, fmt.Errorf("artifact store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ArtifactProvenanceLink{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	link, created, err := addArtifactProvenanceTx(ctx, tx, input)
	if err != nil {
		return contracts.ArtifactProvenanceLink{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ArtifactProvenanceLink{}, false, fmt.Errorf("commit artifact provenance: %w", err)
	}
	return link, created, nil
}

func addArtifactProvenanceTx(ctx context.Context, tx pgx.Tx, input ArtifactProvenanceInput) (contracts.ArtifactProvenanceLink, bool, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.Relation = strings.TrimSpace(input.Relation)
	if input.WorkspaceID == "" || input.FromArtifact <= 0 || input.ToArtifact <= 0 || input.FromArtifact == input.ToArtifact || input.Relation == "" {
		return contracts.ArtifactProvenanceLink{}, false, fmt.Errorf("%w: provenance identity is invalid", ErrArtifactInvalid)
	}
	metadataJSON, err := json.Marshal(input.Metadata)
	if err != nil {
		return contracts.ArtifactProvenanceLink{}, false, err
	}
	for _, id := range []int64{input.FromArtifact, input.ToArtifact} {
		if _, err := readArtifactTx(ctx, tx, input.WorkspaceID, id, false); err != nil {
			return contracts.ArtifactProvenanceLink{}, false, err
		}
	}
	var link contracts.ArtifactProvenanceLink
	var storedMetadata []byte
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.artifact_provenance(workspace_id,from_artifact_id,to_artifact_id,relation,metadata)
		VALUES($1,$2,$3,$4,$5::jsonb)
		ON CONFLICT (workspace_id,from_artifact_id,to_artifact_id,relation) DO NOTHING
		RETURNING id, workspace_id, from_artifact_id, to_artifact_id, relation, metadata, created_at`,
		input.WorkspaceID, input.FromArtifact, input.ToArtifact, input.Relation, metadataJSON).Scan(
		&link.ID, &link.WorkspaceID, &link.FromArtifact, &link.ToArtifact, &link.Relation, &storedMetadata, &link.CreatedAt)
	created := true
	if errors.Is(err, pgx.ErrNoRows) {
		created = false
		err = tx.QueryRow(ctx, `SELECT id,workspace_id,from_artifact_id,to_artifact_id,relation,metadata,created_at FROM fornix.artifact_provenance WHERE workspace_id=$1 AND from_artifact_id=$2 AND to_artifact_id=$3 AND relation=$4`, input.WorkspaceID, input.FromArtifact, input.ToArtifact, input.Relation).Scan(&link.ID, &link.WorkspaceID, &link.FromArtifact, &link.ToArtifact, &link.Relation, &storedMetadata, &link.CreatedAt)
	}
	if err != nil {
		return contracts.ArtifactProvenanceLink{}, false, fmt.Errorf("write artifact provenance: %w", err)
	}
	if len(storedMetadata) > 0 && string(storedMetadata) != "null" {
		if err := json.Unmarshal(storedMetadata, &link.Metadata); err != nil {
			return contracts.ArtifactProvenanceLink{}, false, err
		}
	}
	link.SchemaVersion = contracts.ArtifactSchemaVersion
	return link, created, nil
}

func normalizeArtifactInput(input ArtifactPutInput) (ArtifactPutInput, string, []byte, string, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.Kind = strings.TrimSpace(input.Kind)
	input.MediaType = strings.TrimSpace(input.MediaType)
	input.SourceKind = strings.TrimSpace(input.SourceKind)
	input.SourceID = strings.TrimSpace(input.SourceID)
	input.Role = strings.TrimSpace(input.Role)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	input.CausationID = strings.TrimSpace(input.CausationID)
	input.CorrelationID = strings.TrimSpace(input.CorrelationID)
	input.Raw = append([]byte(nil), input.Raw...)
	if input.MediaType == "" {
		input.MediaType = "application/octet-stream"
	}
	if input.Kind == "" {
		input.Kind = "output"
	}
	if input.WorkspaceID == "" || input.Kind == "" || input.SourceKind == "" || input.SourceID == "" || input.Role == "" || input.IdempotencyKey == "" {
		return ArtifactPutInput{}, "", nil, "", fmt.Errorf("%w: workspace, kind, source, role, and idempotency are required", ErrArtifactInvalid)
	}
	if len(input.Raw) == 0 || len(input.Raw) > contracts.MaxArtifactBytes {
		return ArtifactPutInput{}, "", nil, "", fmt.Errorf("%w: raw content must be between 1 and %d bytes", ErrArtifactInvalid, contracts.MaxArtifactBytes)
	}
	if len(input.WorkspaceID) > 256 || len(input.Kind) > 128 || len(input.MediaType) > 256 || len(input.SourceKind) > 128 || len(input.SourceID) > 2048 || len(input.Role) > 128 || len(input.IdempotencyKey) > contracts.MaxIdempotencyLength {
		return ArtifactPutInput{}, "", nil, "", fmt.Errorf("%w: artifact identity field is too large", ErrArtifactInvalid)
	}
	manifest, err := input.Manifest.Normalize()
	if err != nil {
		return ArtifactPutInput{}, "", nil, "", fmt.Errorf("%w: manifest: %v", ErrArtifactInvalid, err)
	}
	retention, err := input.Retention.Normalize()
	if err != nil {
		return ArtifactPutInput{}, "", nil, "", fmt.Errorf("%w: retention: %v", ErrArtifactInvalid, err)
	}
	input.Manifest, input.Retention = manifest, retention
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return ArtifactPutInput{}, "", nil, "", err
	}
	contentHash := contracts.ArtifactContentHash(input.Raw)
	identity, err := json.Marshal(struct {
		WorkspaceID string
		ContentHash string
		Kind        string
		MediaType   string
		SourceKind  string
		SourceID    string
		Role        string
		Manifest    contracts.ArtifactManifest
	}{input.WorkspaceID, contentHash, input.Kind, input.MediaType, input.SourceKind, input.SourceID, input.Role, input.Manifest})
	if err != nil {
		return ArtifactPutInput{}, "", nil, "", err
	}
	return input, contentHash, manifestJSON, contracts.ArtifactContentHash(identity), nil
}

func (s *ArtifactStore) fail(stage string) error {
	if s != nil && s.failureHook != nil {
		return s.failureHook(stage)
	}
	return nil
}

func readArtifact(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID string, artifactID int64, forUpdate bool) (contracts.Artifact, error) {
	if strings.TrimSpace(workspaceID) == "" || artifactID <= 0 {
		return contracts.Artifact{}, ErrArtifactNotFound
	}
	query := artifactSelect + ` WHERE workspace_id=$1 AND id=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	} else {
		query += ` FOR SHARE`
	}
	artifact, err := scanArtifact(queryer.QueryRow(ctx, query, workspaceID, artifactID))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.Artifact{}, ErrArtifactNotFound
	}
	return artifact, err
}

func readArtifactTx(ctx context.Context, tx pgx.Tx, workspaceID string, artifactID int64, forUpdate bool) (contracts.Artifact, error) {
	artifact, err := readArtifact(ctx, tx, workspaceID, artifactID, forUpdate)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.Artifact{}, ErrArtifactNotFound
	}
	return artifact, err
}

func readArtifactByHashTx(ctx context.Context, tx pgx.Tx, workspaceID, contentHash string, forUpdate bool) (contracts.Artifact, error) {
	if workspaceID == "" || contentHash == "" {
		return contracts.Artifact{}, ErrArtifactNotFound
	}
	query := artifactSelect + ` WHERE workspace_id=$1 AND content_hash=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	} else {
		query += ` FOR SHARE`
	}
	artifact, err := scanArtifact(tx.QueryRow(ctx, query, workspaceID, contentHash))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.Artifact{}, ErrArtifactNotFound
	}
	return artifact, err
}

const artifactSelect = `
	SELECT id,workspace_id,content_hash,kind,media_type,byte_size,chunk_size,
		chunk_count,manifest,status,integrity_state,retain_until,archive_after,
		delete_after,allow_delete,created_at,archived_at,deleted_at,integrity_at
	FROM fornix.artifacts`

func scanArtifact(row pgx.Row) (contracts.Artifact, error) {
	var artifact contracts.Artifact
	var manifestJSON []byte
	if err := row.Scan(&artifact.ID, &artifact.WorkspaceID, &artifact.ContentHash, &artifact.Kind,
		&artifact.MediaType, &artifact.ByteSize, &artifact.ChunkSize, &artifact.ChunkCount,
		&manifestJSON, &artifact.Status, &artifact.IntegrityState, &artifact.Retention.RetainUntil,
		&artifact.Retention.ArchiveAfter, &artifact.Retention.DeleteAfter, &artifact.Retention.AllowDelete,
		&artifact.CreatedAt, &artifact.ArchivedAt, &artifact.DeletedAt, &artifact.IntegrityAt); err != nil {
		return contracts.Artifact{}, err
	}
	artifact.SchemaVersion = contracts.ArtifactSchemaVersion
	artifact.Retention.SchemaVersion = contracts.ArtifactSchemaVersion
	if err := json.Unmarshal(manifestJSON, &artifact.Manifest); err != nil {
		return contracts.Artifact{}, fmt.Errorf("decode artifact manifest: %w", err)
	}
	return artifact, nil
}

func readArtifactRefByIdempotencyTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (contracts.ArtifactRef, error) {
	return readArtifactRef(ctx, tx, `WHERE r.workspace_id=$1 AND r.idempotency_key=$2`, workspaceID, key)
}

func readArtifactRefByIdentityTx(ctx context.Context, tx pgx.Tx, workspaceID, kind, sourceID, role string) (contracts.ArtifactRef, error) {
	return readArtifactRef(ctx, tx, `WHERE r.workspace_id=$1 AND r.source_kind=$2 AND r.source_id=$3 AND r.role=$4`, workspaceID, kind, sourceID, role)
}

func readArtifactRefBySource(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, sourceKind, sourceID, role string) (contracts.ArtifactRef, error) {
	var ref contracts.ArtifactRef
	err := queryer.QueryRow(ctx, `
		SELECT r.id,r.artifact_id,r.workspace_id,r.source_kind,r.source_id,r.role,
			r.idempotency_key,r.causation_id,r.correlation_id,
			a.content_hash,a.media_type,a.byte_size,r.created_at
		FROM fornix.artifact_refs r
		JOIN fornix.artifacts a ON a.workspace_id=r.workspace_id AND a.id=r.artifact_id
		WHERE r.workspace_id=$1 AND r.source_kind=$2 AND r.source_id=$3 AND r.role=$4
		FOR SHARE`, workspaceID, sourceKind, sourceID, role).Scan(
		&ref.ID, &ref.ArtifactID, &ref.WorkspaceID, &ref.SourceKind, &ref.SourceID,
		&ref.Role, &ref.IdempotencyKey, &ref.CausationID, &ref.CorrelationID,
		&ref.ContentHash, &ref.MediaType, &ref.ByteSize, &ref.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ArtifactRef{}, ErrArtifactNotFound
	}
	if err != nil {
		return contracts.ArtifactRef{}, fmt.Errorf("read artifact source reference: %w", err)
	}
	ref.SchemaVersion = contracts.ArtifactSchemaVersion
	return ref, nil
}

func readArtifactRefByArtifactID(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID string, artifactID int64, sourceKind, role string) (contracts.ArtifactRef, error) {
	var ref contracts.ArtifactRef
	err := queryer.QueryRow(ctx, `
		SELECT r.id,r.artifact_id,r.workspace_id,r.source_kind,r.source_id,r.role,
			r.idempotency_key,r.causation_id,r.correlation_id,
			a.content_hash,a.media_type,a.byte_size,r.created_at
		FROM fornix.artifact_refs r
		JOIN fornix.artifacts a ON a.workspace_id=r.workspace_id AND a.id=r.artifact_id
		WHERE r.workspace_id=$1 AND r.artifact_id=$2 AND r.source_kind=$3 AND r.role=$4
		ORDER BY r.id DESC LIMIT 1`, workspaceID, artifactID, sourceKind, role).Scan(
		&ref.ID, &ref.ArtifactID, &ref.WorkspaceID, &ref.SourceKind, &ref.SourceID,
		&ref.Role, &ref.IdempotencyKey, &ref.CausationID, &ref.CorrelationID,
		&ref.ContentHash, &ref.MediaType, &ref.ByteSize, &ref.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ArtifactRef{}, ErrArtifactNotFound
	}
	if err != nil {
		return contracts.ArtifactRef{}, fmt.Errorf("read artifact id reference: %w", err)
	}
	ref.SchemaVersion = contracts.ArtifactSchemaVersion
	return ref, nil
}

func readArtifactRef(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, where string, args ...any) (contracts.ArtifactRef, error) {
	var ref contracts.ArtifactRef
	err := queryer.QueryRow(ctx, `SELECT r.id,r.artifact_id,r.workspace_id,r.source_kind,r.source_id,r.role,
		r.idempotency_key,r.causation_id,r.correlation_id,a.content_hash,a.media_type,a.byte_size,r.created_at
		FROM fornix.artifact_refs r
		JOIN fornix.artifacts a ON a.workspace_id=r.workspace_id AND a.id=r.artifact_id `+where+` FOR SHARE`, args...).Scan(
		&ref.ID, &ref.ArtifactID, &ref.WorkspaceID, &ref.SourceKind, &ref.SourceID, &ref.Role,
		&ref.IdempotencyKey, &ref.CausationID, &ref.CorrelationID, &ref.ContentHash, &ref.MediaType, &ref.ByteSize, &ref.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ArtifactRef{}, ErrArtifactNotFound
	}
	if err != nil {
		return contracts.ArtifactRef{}, fmt.Errorf("read artifact reference: %w", err)
	}
	ref.SchemaVersion = contracts.ArtifactSchemaVersion
	return ref, nil
}

func readArtifactRefsTx(ctx context.Context, tx pgx.Tx, workspaceID string, artifactID int64, limit int) ([]contracts.ArtifactRef, error) {
	rows, err := tx.Query(ctx, `SELECT r.id,r.artifact_id,r.workspace_id,r.source_kind,r.source_id,r.role,r.idempotency_key,r.causation_id,r.correlation_id,a.content_hash,a.media_type,a.byte_size,r.created_at FROM fornix.artifact_refs r JOIN fornix.artifacts a ON a.workspace_id=r.workspace_id AND a.id=r.artifact_id WHERE r.workspace_id=$1 AND r.artifact_id=$2 ORDER BY r.id LIMIT $3`, workspaceID, artifactID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []contracts.ArtifactRef
	for rows.Next() {
		var ref contracts.ArtifactRef
		if err := rows.Scan(&ref.ID, &ref.ArtifactID, &ref.WorkspaceID, &ref.SourceKind, &ref.SourceID, &ref.Role, &ref.IdempotencyKey, &ref.CausationID, &ref.CorrelationID, &ref.ContentHash, &ref.MediaType, &ref.ByteSize, &ref.CreatedAt); err != nil {
			return nil, err
		}
		ref.SchemaVersion = contracts.ArtifactSchemaVersion
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

func readArtifactRawTx(ctx context.Context, tx pgx.Tx, artifact contracts.Artifact) ([]byte, error) {
	return readArtifactRawWithQuery(ctx, tx, artifact)
}

func readArtifactRawWithQuery(ctx context.Context, queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, artifact contracts.Artifact) ([]byte, error) {
	rows, err := queryer.Query(ctx, `SELECT chunk_index,content_hash,byte_size,raw_bytes FROM fornix.artifact_chunks WHERE workspace_id=$1 AND artifact_id=$2 ORDER BY chunk_index`, artifact.WorkspaceID, artifact.ID)
	if err != nil {
		return nil, fmt.Errorf("read artifact chunks: %w", err)
	}
	defer rows.Close()
	raw := make([]byte, 0, artifact.ByteSize)
	expectedIndex := 0
	for rows.Next() {
		var index int
		var hash string
		var byteSize int
		var chunk []byte
		if err := rows.Scan(&index, &hash, &byteSize, &chunk); err != nil {
			return nil, fmt.Errorf("scan artifact chunk: %w", err)
		}
		if index != expectedIndex {
			return nil, fmt.Errorf("%w: unexpected chunk index %d, wanted %d", ErrArtifactIntegrity, index, expectedIndex)
		}
		if byteSize != len(chunk) || contracts.ArtifactContentHash(chunk) != hash {
			return nil, fmt.Errorf("%w: chunk %d digest or size mismatch", ErrArtifactIntegrity, index)
		}
		raw = append(raw, chunk...)
		expectedIndex++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if expectedIndex != artifact.ChunkCount || int64(len(raw)) != artifact.ByteSize {
		return nil, fmt.Errorf("%w: chunk count or byte size mismatch", ErrArtifactIntegrity)
	}
	return raw, nil
}

func verifyArtifactBytes(artifact contracts.Artifact, raw []byte, expected []byte) error {
	if expected != nil && !bytes.Equal(raw, expected) {
		return fmt.Errorf("%w: duplicate raw bytes differ", ErrArtifactConflict)
	}
	if int64(len(raw)) != artifact.ByteSize || contracts.ArtifactContentHash(raw) != artifact.ContentHash {
		return fmt.Errorf("%w: artifact content hash or size mismatch", ErrArtifactIntegrity)
	}
	return nil
}

func fitArtifactText(value string, maxBytes, maxTokens int, truncated bool) (string, bool) {
	if maxBytes <= 0 || maxTokens <= 0 || value == "" {
		if value != "" {
			truncated = true
		}
		return "", truncated
	}
	if len(value) <= maxBytes && contracts.EstimateTokens(value) <= maxTokens {
		return value, truncated
	}
	limit := maxBytes
	if maxTokens*4 < limit {
		limit = maxTokens * 4
	}
	if limit <= 0 {
		return "", true
	}
	value = value[:limit]
	for len(value) > 0 && (value[len(value)-1]&0xc0) == 0x80 {
		value = value[:len(value)-1]
	}
	return value, true
}

func readArtifactProvenanceTx(ctx context.Context, tx pgx.Tx, workspaceID string, startID int64, maxDepth, maxNodes int) ([]contracts.ArtifactProvenanceLink, error) {
	frontier := []int64{startID}
	visited := map[int64]bool{startID: true}
	seenEdges := map[int64]bool{}
	var output []contracts.ArtifactProvenanceLink
	for depth := 0; depth < maxDepth && len(frontier) > 0 && len(output) < maxNodes; depth++ {
		rows, err := tx.Query(ctx, `SELECT id,workspace_id,from_artifact_id,to_artifact_id,relation,metadata,created_at FROM fornix.artifact_provenance WHERE workspace_id=$1 AND (from_artifact_id=ANY($2::bigint[]) OR to_artifact_id=ANY($2::bigint[])) ORDER BY relation,from_artifact_id,to_artifact_id,id`, workspaceID, frontier)
		if err != nil {
			return nil, err
		}
		next := make([]int64, 0)
		for rows.Next() {
			var link contracts.ArtifactProvenanceLink
			var metadata []byte
			if err := rows.Scan(&link.ID, &link.WorkspaceID, &link.FromArtifact, &link.ToArtifact, &link.Relation, &metadata, &link.CreatedAt); err != nil {
				rows.Close()
				return nil, err
			}
			if seenEdges[link.ID] {
				continue
			}
			seenEdges[link.ID] = true
			link.SchemaVersion = contracts.ArtifactSchemaVersion
			if len(metadata) > 0 && string(metadata) != "null" {
				if err := json.Unmarshal(metadata, &link.Metadata); err != nil {
					rows.Close()
					return nil, err
				}
			}
			output = append(output, link)
			neighbor := link.FromArtifact
			if visited[link.FromArtifact] {
				neighbor = link.ToArtifact
			} else if visited[link.ToArtifact] {
				neighbor = link.FromArtifact
			}
			if !visited[neighbor] && len(visited) < maxNodes {
				visited[neighbor] = true
				next = append(next, neighbor)
			}
			if len(output) >= maxNodes {
				break
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		frontier = next
	}
	return output, nil
}

func artifactDisclosureHash(result contracts.ArtifactDisclosureResult) (string, error) {
	canonical := struct {
		WorkspaceID string
		ArtifactID  int64
		ContentHash string
		Status      string
		Level       string
		Gist        string
		Detail      string
		Raw         []byte
		Provenance  []contracts.ArtifactProvenanceLink
		References  []contracts.ArtifactRef
		Truncated   bool
	}{result.WorkspaceID, result.ArtifactID, result.ContentHash, result.Status, result.Level, result.Gist, result.Detail, result.Raw, result.Provenance, result.References, result.Truncated}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func chunkCount(size int) int {
	return (size + contracts.DefaultArtifactChunkBytes - 1) / contracts.DefaultArtifactChunkBytes
}
