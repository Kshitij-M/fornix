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
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrEvidenceNotFound  = errors.New("evidence not found")
	ErrEvidenceConflict  = errors.New("evidence identity conflict")
	ErrEvidenceIntegrity = errors.New("evidence integrity check failed")
	ErrEvidenceCycle     = errors.New("supersession cycle rejected")
	ErrInvalidEvidence   = errors.New("invalid evidence")
)

// EvidenceStore is the Postgres authority for immutable source records and
// typed provenance. It deliberately has no cache or asynchronous delivery
// path; callers can compose its *Tx methods with event/task mutations.
type EvidenceStore struct {
	pool *pgxpool.Pool
}

func NewEvidenceStore(pool *pgxpool.Pool) *EvidenceStore {
	return &EvidenceStore{pool: pool}
}

type EvidencePutInput struct {
	WorkspaceID      string
	SourceReference  string
	DeduplicationKey string
	Kind             string
	MediaType        string
	Gist             string
	Detail           string
	RawPayload       []byte
	SupersedesID     *int64
	Contradicts      []int64
}

type EvidencePutResult struct {
	Record  contracts.SourceRecord `json:"record"`
	Created bool                   `json:"created"`
}

type ProvenanceEdgeResult struct {
	Edge    contracts.ProvenanceEdge `json:"edge"`
	Created bool                     `json:"created"`
}

const (
	defaultEvidenceKind      = "observation"
	defaultEvidenceMediaType = "application/octet-stream"
)

func (s *EvidenceStore) Put(ctx context.Context, input EvidencePutInput) (EvidencePutResult, error) {
	if s == nil || s.pool == nil {
		return EvidencePutResult{}, fmt.Errorf("evidence store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EvidencePutResult{}, fmt.Errorf("begin evidence write: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.PutTx(ctx, tx, input)
	if err != nil {
		return EvidencePutResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EvidencePutResult{}, fmt.Errorf("commit evidence write: %w", err)
	}
	return result, nil
}

// PutTx inserts one immutable source record and its declared relationships.
// The caller owns the transaction and may compose this with an event/task
// mutation. An exact identity repeat returns the existing row without writes.
func (s *EvidenceStore) PutTx(ctx context.Context, tx pgx.Tx, input EvidencePutInput) (EvidencePutResult, error) {
	normalized, rawHash, err := normalizeEvidenceInput(input)
	if err != nil {
		return EvidencePutResult{}, err
	}

	if normalized.SupersedesID != nil {
		if _, _, err := getEvidenceTx(ctx, tx, normalized.WorkspaceID, *normalized.SupersedesID); err != nil {
			return EvidencePutResult{}, fmt.Errorf("supersedes predecessor: %w", err)
		}
	}

	var record contracts.SourceRecord
	var raw []byte
	var inserted bool
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.evidence_records(
			workspace_id, source_reference, deduplication_key, kind, media_type,
			gist, detail, raw_payload, raw_size_bytes, evidence_hash, supersedes_id
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (workspace_id, source_reference, deduplication_key) DO NOTHING
		RETURNING id, workspace_id, source_reference, deduplication_key, kind,
			media_type, gist, detail, raw_payload, raw_size_bytes, evidence_hash,
			supersedes_id, created_at`,
		normalized.WorkspaceID, normalized.SourceReference, normalized.DeduplicationKey,
		normalized.Kind, normalized.MediaType, normalized.Gist, normalized.Detail,
		normalized.RawPayload, len(normalized.RawPayload), rawHash, normalized.SupersedesID,
	).Scan(&record.ID, &record.WorkspaceID, &record.SourceReference,
		&record.DeduplicationKey, &record.Kind, &record.MediaType, &record.Gist,
		&record.Detail, &raw, &record.RawSizeBytes, &record.EvidenceHash,
		&record.SupersedesID, &record.CreatedAt)
	if err == nil {
		inserted = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return EvidencePutResult{}, fmt.Errorf("insert evidence record: %w", err)
	} else {
		if err := tx.QueryRow(ctx, `
			SELECT id, workspace_id, source_reference, deduplication_key, kind,
				media_type, gist, detail, raw_payload, raw_size_bytes, evidence_hash,
				supersedes_id, created_at
			FROM fornix.evidence_records
			WHERE workspace_id=$1 AND source_reference=$2 AND deduplication_key=$3
			FOR SHARE`, normalized.WorkspaceID, normalized.SourceReference,
			normalized.DeduplicationKey).Scan(&record.ID, &record.WorkspaceID,
			&record.SourceReference, &record.DeduplicationKey, &record.Kind,
			&record.MediaType, &record.Gist, &record.Detail, &raw,
			&record.RawSizeBytes, &record.EvidenceHash, &record.SupersedesID,
			&record.CreatedAt); err != nil {
			return EvidencePutResult{}, fmt.Errorf("read duplicate evidence: %w", err)
		}
		if err := verifyEvidence(record.EvidenceHash, raw, record.RawSizeBytes); err != nil {
			return EvidencePutResult{}, err
		}
		if record.EvidenceHash != rawHash || record.Kind != normalized.Kind ||
			record.MediaType != normalized.MediaType || record.Gist != normalized.Gist ||
			record.Detail != normalized.Detail || !sameOptionalID(record.SupersedesID, normalized.SupersedesID) ||
			!bytes.Equal(raw, normalized.RawPayload) {
			return EvidencePutResult{}, fmt.Errorf("%w for workspace=%q source_reference=%q", ErrEvidenceConflict, normalized.WorkspaceID, normalized.SourceReference)
		}
		record.IntegrityVerified = true
		return EvidencePutResult{Record: record, Created: false}, nil
	}

	if err := verifyEvidence(record.EvidenceHash, raw, record.RawSizeBytes); err != nil {
		return EvidencePutResult{}, err
	}
	record.IntegrityVerified = true
	if normalized.SupersedesID != nil {
		if _, err := s.addEdgeTx(ctx, tx, contracts.ProvenanceEdgeInput{
			WorkspaceID: normalized.WorkspaceID, FromEvidenceID: record.ID,
			ToEvidenceID: *normalized.SupersedesID, Relation: contracts.RelationSupersedes,
		}); err != nil {
			return EvidencePutResult{}, fmt.Errorf("record supersession edge: %w", err)
		}
	}
	contradicts := uniquePositiveIDs(normalized.Contradicts)
	for _, target := range contradicts {
		if _, err := s.addEdgeTx(ctx, tx, contracts.ProvenanceEdgeInput{
			WorkspaceID: normalized.WorkspaceID, FromEvidenceID: record.ID,
			ToEvidenceID: target, Relation: contracts.RelationContradicts,
		}); err != nil {
			return EvidencePutResult{}, fmt.Errorf("record contradiction edge: %w", err)
		}
	}
	return EvidencePutResult{Record: record, Created: inserted}, nil
}

func (s *EvidenceStore) AddEdge(ctx context.Context, input contracts.ProvenanceEdgeInput) (ProvenanceEdgeResult, error) {
	if s == nil || s.pool == nil {
		return ProvenanceEdgeResult{}, fmt.Errorf("evidence store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ProvenanceEdgeResult{}, fmt.Errorf("begin provenance edge: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.addEdgeTx(ctx, tx, input)
	if err != nil {
		return ProvenanceEdgeResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProvenanceEdgeResult{}, fmt.Errorf("commit provenance edge: %w", err)
	}
	return result, nil
}

func (s *EvidenceStore) addEdgeTx(ctx context.Context, tx pgx.Tx, input contracts.ProvenanceEdgeInput) (ProvenanceEdgeResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return ProvenanceEdgeResult{}, fmt.Errorf("%w: workspace_id is required", ErrInvalidEvidence)
	}
	if input.FromEvidenceID <= 0 || input.ToEvidenceID <= 0 || input.FromEvidenceID == input.ToEvidenceID {
		return ProvenanceEdgeResult{}, fmt.Errorf("%w: edge endpoints must be distinct positive IDs", ErrInvalidEvidence)
	}
	if !validRelation(input.Relation) {
		return ProvenanceEdgeResult{}, fmt.Errorf("%w: unsupported relation %q", ErrInvalidEvidence, input.Relation)
	}
	metadata, err := normalizeJSON(input.Metadata)
	if err != nil {
		return ProvenanceEdgeResult{}, fmt.Errorf("%w: metadata: %v", ErrInvalidEvidence, err)
	}
	for _, id := range []int64{input.FromEvidenceID, input.ToEvidenceID} {
		if _, _, err := getEvidenceTx(ctx, tx, workspaceID, id); err != nil {
			return ProvenanceEdgeResult{}, fmt.Errorf("edge endpoint %d: %w", id, err)
		}
	}

	var edge contracts.ProvenanceEdge
	var storedMetadata []byte
	err = tx.QueryRow(ctx, `
		SELECT id, workspace_id, from_evidence_id, to_evidence_id, relation,
			metadata, created_at
		FROM fornix.provenance_edges
		WHERE workspace_id=$1 AND from_evidence_id=$2 AND to_evidence_id=$3 AND relation=$4
		FOR SHARE`, workspaceID, input.FromEvidenceID, input.ToEvidenceID,
		string(input.Relation)).Scan(&edge.ID, &edge.WorkspaceID, &edge.FromEvidenceID,
		&edge.ToEvidenceID, &edge.Relation, &storedMetadata, &edge.CreatedAt)
	if err == nil {
		if !jsonEqual(storedMetadata, metadata) {
			return ProvenanceEdgeResult{}, fmt.Errorf("%w for edge %d", ErrEvidenceConflict, edge.ID)
		}
		edge.Metadata = storedMetadata
		return ProvenanceEdgeResult{Edge: edge, Created: false}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ProvenanceEdgeResult{}, fmt.Errorf("read provenance edge: %w", err)
	}
	if input.Relation == contracts.RelationSupersedes {
		// Serialize supersession graph mutations per workspace. The recursive
		// cycle check is then evaluated against every committed predecessor,
		// including a concurrent writer that would otherwise be invisible under
		// READ COMMITTED.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, $2))`, workspaceID, int64(7_301_947)); err != nil {
			return ProvenanceEdgeResult{}, fmt.Errorf("lock supersession workspace: %w", err)
		}
		cycle, err := supersessionCycleTx(ctx, tx, workspaceID, input.FromEvidenceID, input.ToEvidenceID)
		if err != nil {
			return ProvenanceEdgeResult{}, fmt.Errorf("check supersession cycle: %w", err)
		}
		if cycle {
			return ProvenanceEdgeResult{}, ErrEvidenceCycle
		}
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.provenance_edges(
			workspace_id, from_evidence_id, to_evidence_id, relation, metadata
		) VALUES($1,$2,$3,$4,$5::jsonb)
		RETURNING id, workspace_id, from_evidence_id, to_evidence_id, relation,
			metadata, created_at`, workspaceID, input.FromEvidenceID, input.ToEvidenceID,
		string(input.Relation), metadata).Scan(&edge.ID, &edge.WorkspaceID,
		&edge.FromEvidenceID, &edge.ToEvidenceID, &edge.Relation, &edge.Metadata,
		&edge.CreatedAt)
	if err != nil {
		return ProvenanceEdgeResult{}, fmt.Errorf("insert provenance edge: %w", err)
	}
	return ProvenanceEdgeResult{Edge: edge, Created: true}, nil
}

// Traverse returns both incoming and outgoing provenance edges. It performs
// one bounded indexed query per hop instead of loading the graph into memory.
func (s *EvidenceStore) Traverse(ctx context.Context, request contracts.ProvenanceTraversalRequest) ([]contracts.ProvenanceEdge, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("evidence store is not configured")
	}
	workspaceID, maxDepth, maxNodes, err := normalizeTraversal(request)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin provenance traversal: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, _, err := getEvidenceTx(ctx, tx, workspaceID, request.EvidenceID); err != nil {
		return nil, err
	}
	edges, _, _, err := traverseTx(ctx, tx, workspaceID, request.EvidenceID, maxDepth, maxNodes)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit provenance traversal: %w", err)
	}
	return edges, nil
}

func (s *EvidenceStore) Disclose(ctx context.Context, request contracts.DisclosureRequest) (contracts.DisclosureResult, error) {
	if s == nil || s.pool == nil {
		return contracts.DisclosureResult{}, fmt.Errorf("evidence store is not configured")
	}
	normalized, err := request.Normalize()
	if err != nil {
		return contracts.DisclosureResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.DisclosureResult{}, fmt.Errorf("begin disclosure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	record, raw, err := getEvidenceForDisclosureTx(ctx, tx, normalized)
	if err != nil {
		return contracts.DisclosureResult{}, err
	}
	result := contracts.DisclosureResult{
		SchemaVersion:     contracts.ProvenanceSchemaVersion,
		WorkspaceID:       record.WorkspaceID,
		EvidenceID:        record.ID,
		SourceReference:   record.SourceReference,
		Kind:              record.Kind,
		MediaType:         record.MediaType,
		Level:             normalized.Level,
		EvidenceHash:      record.EvidenceHash,
		RawSizeBytes:      record.RawSizeBytes,
		SupersedesID:      record.SupersedesID,
		IntegrityVerified: true,
		QueryCount:        1,
	}
	result.SupersededBy, err = readIDsTx(ctx, tx, `
		SELECT id FROM fornix.evidence_records
		WHERE workspace_id=$1 AND supersedes_id=$2 ORDER BY id LIMIT $3`,
		record.WorkspaceID, record.ID, normalized.MaxNodes+1)
	if err != nil {
		return contracts.DisclosureResult{}, fmt.Errorf("read supersession lineage: %w", err)
	}
	result.QueryCount++
	if len(result.SupersededBy) > normalized.MaxNodes {
		result.SupersededBy = result.SupersededBy[:normalized.MaxNodes]
		result.Truncated = true
	}
	result.ContradictedBy, err = readContradictionIDsTx(ctx, tx, record.WorkspaceID, record.ID, normalized.MaxNodes+1)
	if err != nil {
		return contracts.DisclosureResult{}, fmt.Errorf("read contradiction lineage: %w", err)
	}
	result.QueryCount++
	if len(result.ContradictedBy) > normalized.MaxNodes {
		result.ContradictedBy = result.ContradictedBy[:normalized.MaxNodes]
		result.Truncated = true
	}
	if normalized.IncludeProvenance != nil && *normalized.IncludeProvenance && normalized.MaxDepth > 0 {
		edges, traversalQueries, traversalTruncated, err := traverseTx(ctx, tx, record.WorkspaceID, record.ID, normalized.MaxDepth, normalized.MaxNodes)
		if err != nil {
			return contracts.DisclosureResult{}, fmt.Errorf("read provenance: %w", err)
		}
		result.Provenance = edges
		result.ProvenanceTruncated = traversalTruncated
		result.QueryCount += traversalQueries
		result.Truncated = result.Truncated || traversalTruncated
	}

	result.Gist, result.Truncated = fitDisclosureText(record.Gist, normalized.MaxBytes-result.TotalBytes, normalized.MaxTokens-result.TotalTokens, result.Truncated)
	result.TotalBytes += len(result.Gist)
	result.TotalTokens += contracts.EstimateTokens(result.Gist)
	if normalized.Level == contracts.DisclosureDetail || normalized.Level == contracts.DisclosureRaw {
		var detail string
		detail, result.Truncated = fitDisclosureText(record.Detail, normalized.MaxBytes-result.TotalBytes, normalized.MaxTokens-result.TotalTokens, result.Truncated)
		result.Detail = detail
		result.TotalBytes += len(detail)
		result.TotalTokens += contracts.EstimateTokens(detail)
	}
	if normalized.Level == contracts.DisclosureRaw {
		if len(raw) <= normalized.MaxBytes-result.TotalBytes && contracts.EstimateTokens(string(raw)) <= normalized.MaxTokens-result.TotalTokens {
			result.RawPayload = append([]byte(nil), raw...)
			result.TotalBytes += len(raw)
			result.TotalTokens += contracts.EstimateTokens(string(raw))
		} else {
			result.Truncated = true
		}
	}
	result.ContentHash, err = disclosureContentHash(result)
	if err != nil {
		return contracts.DisclosureResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DisclosureResult{}, fmt.Errorf("commit disclosure: %w", err)
	}
	return result, nil
}

func normalizeEvidenceInput(input EvidencePutInput) (EvidencePutInput, string, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.SourceReference = strings.TrimSpace(input.SourceReference)
	input.DeduplicationKey = strings.TrimSpace(input.DeduplicationKey)
	input.Kind = strings.TrimSpace(input.Kind)
	input.MediaType = strings.TrimSpace(input.MediaType)
	input.Gist = strings.TrimSpace(input.Gist)
	if input.WorkspaceID == "" || input.SourceReference == "" || input.Gist == "" {
		return EvidencePutInput{}, "", fmt.Errorf("%w: workspace_id, source_reference, and gist are required", ErrInvalidEvidence)
	}
	if input.DeduplicationKey == "" {
		input.DeduplicationKey = input.SourceReference
	}
	if input.Kind == "" {
		input.Kind = defaultEvidenceKind
	}
	if input.MediaType == "" {
		input.MediaType = defaultEvidenceMediaType
	}
	if len(input.RawPayload) == 0 || len(input.RawPayload) > contracts.MaxEvidenceRawBytes {
		return EvidencePutInput{}, "", fmt.Errorf("%w: raw payload must be between 1 and %d bytes", ErrInvalidEvidence, contracts.MaxEvidenceRawBytes)
	}
	if len(input.Gist) > contracts.MaxEvidenceGistBytes || len(input.Detail) > contracts.MaxEvidenceDetailBytes {
		return EvidencePutInput{}, "", fmt.Errorf("%w: derived disclosure is too large", ErrInvalidEvidence)
	}
	if len(input.WorkspaceID) > 256 || len(input.SourceReference) > 2048 || len(input.DeduplicationKey) > 256 || len(input.Kind) > 128 || len(input.MediaType) > 256 {
		return EvidencePutInput{}, "", fmt.Errorf("%w: identity field is too large", ErrInvalidEvidence)
	}
	input.RawPayload = append([]byte(nil), input.RawPayload...)
	digest := sha256.Sum256(input.RawPayload)
	return input, hex.EncodeToString(digest[:]), nil
}

func getEvidenceForDisclosureTx(ctx context.Context, tx pgx.Tx, request contracts.DisclosureRequest) (contracts.SourceRecord, []byte, error) {
	if request.EvidenceID > 0 {
		return getEvidenceTx(ctx, tx, request.WorkspaceID, request.EvidenceID)
	}
	var record contracts.SourceRecord
	var raw []byte
	err := tx.QueryRow(ctx, `
		SELECT id, workspace_id, source_reference, deduplication_key, kind,
			media_type, gist, detail, raw_payload, raw_size_bytes, evidence_hash,
			supersedes_id, created_at
		FROM fornix.evidence_records
		WHERE workspace_id=$1 AND source_reference=$2
		ORDER BY id DESC LIMIT 1`, request.WorkspaceID, request.SourceReference).
		Scan(&record.ID, &record.WorkspaceID, &record.SourceReference,
			&record.DeduplicationKey, &record.Kind, &record.MediaType, &record.Gist,
			&record.Detail, &raw, &record.RawSizeBytes, &record.EvidenceHash,
			&record.SupersedesID, &record.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.SourceRecord{}, nil, ErrEvidenceNotFound
	}
	if err != nil {
		return contracts.SourceRecord{}, nil, fmt.Errorf("read evidence: %w", err)
	}
	if err := verifyEvidence(record.EvidenceHash, raw, record.RawSizeBytes); err != nil {
		return contracts.SourceRecord{}, nil, err
	}
	record.IntegrityVerified = true
	return record, raw, nil
}

func getEvidenceTx(ctx context.Context, tx pgx.Tx, workspaceID string, id int64) (contracts.SourceRecord, []byte, error) {
	if strings.TrimSpace(workspaceID) == "" || id <= 0 {
		return contracts.SourceRecord{}, nil, ErrEvidenceNotFound
	}
	var record contracts.SourceRecord
	var raw []byte
	err := tx.QueryRow(ctx, `
		SELECT id, workspace_id, source_reference, deduplication_key, kind,
			media_type, gist, detail, raw_payload, raw_size_bytes, evidence_hash,
			supersedes_id, created_at
		FROM fornix.evidence_records
		WHERE workspace_id=$1 AND id=$2
		FOR SHARE`, workspaceID, id).
		Scan(&record.ID, &record.WorkspaceID, &record.SourceReference,
			&record.DeduplicationKey, &record.Kind, &record.MediaType, &record.Gist,
			&record.Detail, &raw, &record.RawSizeBytes, &record.EvidenceHash,
			&record.SupersedesID, &record.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.SourceRecord{}, nil, ErrEvidenceNotFound
	}
	if err != nil {
		return contracts.SourceRecord{}, nil, fmt.Errorf("read evidence: %w", err)
	}
	if err := verifyEvidence(record.EvidenceHash, raw, record.RawSizeBytes); err != nil {
		return contracts.SourceRecord{}, nil, err
	}
	record.IntegrityVerified = true
	return record, raw, nil
}

func verifyEvidence(expectedHash string, raw []byte, expectedSize int64) error {
	if expectedSize != int64(len(raw)) {
		return fmt.Errorf("%w: raw size mismatch", ErrEvidenceIntegrity)
	}
	digest := sha256.Sum256(raw)
	if !strings.EqualFold(expectedHash, hex.EncodeToString(digest[:])) {
		return fmt.Errorf("%w: evidence_hash mismatch", ErrEvidenceIntegrity)
	}
	return nil
}

func readIDsTx(ctx context.Context, tx pgx.Tx, query string, args ...any) ([]int64, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func readContradictionIDsTx(ctx context.Context, tx pgx.Tx, workspaceID string, id int64, limit int) ([]int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT CASE WHEN from_evidence_id=$2 THEN to_evidence_id ELSE from_evidence_id END
		FROM fornix.provenance_edges
		WHERE workspace_id=$1 AND relation=$3
		  AND (from_evidence_id=$2 OR to_evidence_id=$2)
		ORDER BY id LIMIT $4`, workspaceID, id, string(contracts.RelationContradicts), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var other int64
		if err := rows.Scan(&other); err != nil {
			return nil, err
		}
		ids = append(ids, other)
	}
	return ids, rows.Err()
}

func traverseTx(ctx context.Context, tx pgx.Tx, workspaceID string, root int64, maxDepth, maxNodes int) ([]contracts.ProvenanceEdge, int, bool, error) {
	if maxDepth <= 0 || maxNodes <= 0 {
		return nil, 0, false, nil
	}
	frontier := []int64{root}
	seenNodes := map[int64]bool{root: true}
	seenEdges := make(map[int64]contracts.ProvenanceEdge)
	truncated := false
	queries := 0
	for depth := 1; depth <= maxDepth && len(frontier) > 0; depth++ {
		queries++
		rows, err := tx.Query(ctx, `
			SELECT id, workspace_id, from_evidence_id, to_evidence_id, relation,
				metadata, created_at
			FROM fornix.provenance_edges
			WHERE workspace_id=$1
			  AND (from_evidence_id = ANY($2::bigint[]) OR to_evidence_id = ANY($2::bigint[]))
			ORDER BY relation, from_evidence_id, to_evidence_id, id
			LIMIT $3`, workspaceID, frontier, maxNodes+1)
		if err != nil {
			return nil, queries, truncated, err
		}
		next := make([]int64, 0, maxNodes)
		nextSet := make(map[int64]bool)
		rowCount := 0
		for rows.Next() {
			rowCount++
			var edge contracts.ProvenanceEdge
			if err := rows.Scan(&edge.ID, &edge.WorkspaceID, &edge.FromEvidenceID,
				&edge.ToEvidenceID, &edge.Relation, &edge.Metadata, &edge.CreatedAt); err != nil {
				rows.Close()
				return nil, queries, truncated, err
			}
			for _, current := range frontier {
				if edge.FromEvidenceID != current && edge.ToEvidenceID != current {
					continue
				}
				if _, exists := seenEdges[edge.ID]; exists {
					break
				}
				edge.Depth = depth
				if edge.FromEvidenceID == current {
					edge.Direction = "outgoing"
				} else {
					edge.Direction = "incoming"
				}
				seenEdges[edge.ID] = edge
				neighbor := edge.FromEvidenceID
				if neighbor == current {
					neighbor = edge.ToEvidenceID
				}
				if !seenNodes[neighbor] && len(seenNodes) < maxNodes && !nextSet[neighbor] {
					seenNodes[neighbor] = true
					nextSet[neighbor] = true
					next = append(next, neighbor)
				}
				break
			}
			if len(seenEdges) >= maxNodes {
				truncated = true
				break
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, queries, truncated, err
		}
		if rowCount > maxNodes {
			truncated = true
		}
		frontier = next
	}
	if len(frontier) > 0 {
		// The next frontier exists but the caller's depth budget ended. The
		// returned edges are still valid; the annotation makes the boundary
		// observable to replay/disclosure callers.
		truncated = true
	}
	edges := make([]contracts.ProvenanceEdge, 0, len(seenEdges))
	for _, edge := range seenEdges {
		edges = append(edges, edge)
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Depth != edges[j].Depth {
			return edges[i].Depth < edges[j].Depth
		}
		if edges[i].Relation != edges[j].Relation {
			return edges[i].Relation < edges[j].Relation
		}
		if edges[i].FromEvidenceID != edges[j].FromEvidenceID {
			return edges[i].FromEvidenceID < edges[j].FromEvidenceID
		}
		if edges[i].ToEvidenceID != edges[j].ToEvidenceID {
			return edges[i].ToEvidenceID < edges[j].ToEvidenceID
		}
		return edges[i].ID < edges[j].ID
	})
	return edges, queries, truncated, nil
}

func normalizeTraversal(request contracts.ProvenanceTraversalRequest) (string, int, int, error) {
	workspaceID := strings.TrimSpace(request.WorkspaceID)
	if workspaceID == "" || request.EvidenceID <= 0 {
		return "", 0, 0, fmt.Errorf("workspace_id and positive evidence_id are required")
	}
	depth := request.MaxDepth
	if depth == 0 {
		depth = contracts.DefaultProvenanceDepth
	}
	nodes := request.MaxNodes
	if nodes == 0 {
		nodes = contracts.DefaultProvenanceNodes
	}
	if depth < 0 || depth > contracts.MaxProvenanceDepth || nodes < 1 || nodes > contracts.MaxProvenanceNodes {
		return "", 0, 0, fmt.Errorf("provenance bounds exceed configured limits")
	}
	return workspaceID, depth, nodes, nil
}

func fitDisclosureText(value string, maxBytes, maxTokens int, truncated bool) (string, bool) {
	if value == "" {
		return "", truncated
	}
	if maxBytes <= 0 || maxTokens <= 0 {
		return "", true
	}
	if len(value) <= maxBytes && contracts.EstimateTokens(value) <= maxTokens {
		return value, truncated
	}
	var builder strings.Builder
	for _, r := range value {
		candidateBytes := len(string(r))
		if builder.Len()+candidateBytes > maxBytes {
			break
		}
		candidate := builder.String() + string(r)
		if contracts.EstimateTokens(candidate) > maxTokens {
			break
		}
		builder.WriteRune(r)
	}
	return builder.String(), true
}

func disclosureContentHash(result contracts.DisclosureResult) (string, error) {
	input := struct {
		SchemaVersion   int                        `json:"schema_version"`
		WorkspaceID     string                     `json:"workspace_id"`
		EvidenceID      int64                      `json:"evidence_id"`
		SourceReference string                     `json:"source_reference"`
		Kind            string                     `json:"kind"`
		MediaType       string                     `json:"media_type"`
		Level           contracts.DisclosureLevel  `json:"level"`
		EvidenceHash    string                     `json:"evidence_hash"`
		RawSizeBytes    int64                      `json:"raw_size_bytes"`
		Gist            string                     `json:"gist,omitempty"`
		Detail          string                     `json:"detail,omitempty"`
		RawPayload      []byte                     `json:"raw_payload,omitempty"`
		SupersedesID    *int64                     `json:"supersedes_id,omitempty"`
		SupersededBy    []int64                    `json:"superseded_by,omitempty"`
		ContradictedBy  []int64                    `json:"contradicted_by,omitempty"`
		Provenance      []contracts.ProvenanceEdge `json:"provenance,omitempty"`
		ProvenanceTrunc bool                       `json:"provenance_truncated,omitempty"`
		Truncated       bool                       `json:"truncated"`
		TotalBytes      int                        `json:"total_bytes"`
		TotalTokens     int                        `json:"total_tokens"`
	}{
		SchemaVersion: result.SchemaVersion, WorkspaceID: result.WorkspaceID,
		EvidenceID: result.EvidenceID, SourceReference: result.SourceReference,
		Kind: result.Kind, MediaType: result.MediaType, Level: result.Level,
		EvidenceHash: result.EvidenceHash, RawSizeBytes: result.RawSizeBytes,
		Gist: result.Gist, Detail: result.Detail, RawPayload: result.RawPayload,
		SupersedesID: result.SupersedesID, SupersededBy: result.SupersededBy,
		ContradictedBy: result.ContradictedBy, Provenance: result.Provenance,
		ProvenanceTrunc: result.ProvenanceTruncated, Truncated: result.Truncated,
		TotalBytes: result.TotalBytes, TotalTokens: result.TotalTokens,
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("hash disclosure: %w", err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func supersessionCycleTx(ctx context.Context, tx pgx.Tx, workspaceID string, fromID, toID int64) (bool, error) {
	var cycle bool
	err := tx.QueryRow(ctx, `
		WITH RECURSIVE newer(id) AS (
			SELECT $3::bigint
			UNION
			SELECT e.to_evidence_id
			FROM fornix.provenance_edges e
			JOIN newer n ON e.from_evidence_id=n.id
			WHERE e.workspace_id=$1 AND e.relation=$4
		)
		SELECT EXISTS(SELECT 1 FROM newer WHERE id=$2)`,
		workspaceID, fromID, toID, string(contracts.RelationSupersedes)).Scan(&cycle)
	return cycle, err
}

func normalizeJSON(raw []byte) ([]byte, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return []byte(`{}`), nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("must be valid JSON")
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func jsonEqual(left, right []byte) bool {
	leftJSON, err := normalizeJSON(left)
	if err != nil {
		return false
	}
	rightJSON, err := normalizeJSON(right)
	if err != nil {
		return false
	}
	var leftValue, rightValue any
	if json.Unmarshal(leftJSON, &leftValue) != nil || json.Unmarshal(rightJSON, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func uniquePositiveIDs(ids []int64) []int64 {
	seen := make(map[int64]bool, len(ids))
	result := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id > 0 && !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func sameOptionalID(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func validRelation(relation contracts.ProvenanceRelation) bool {
	switch relation {
	case contracts.RelationDerivedFrom, contracts.RelationSupports,
		contracts.RelationContradicts, contracts.RelationSupersedes,
		contracts.RelationCausedBy, contracts.RelationRefines:
		return true
	default:
		return false
	}
}
