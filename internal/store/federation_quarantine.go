package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
)

var ErrFederationQuarantineConflict = errors.New("federation quarantine idempotency conflict")

const maxFederationQuarantineRead = contracts.MaxFederationQuarantinePageSize

// QuarantineLegacyPeers records a redacted disposition for historical global
// peers. It deliberately selects no bearer_token column and never creates a
// workspace peer projection. The caller chooses an audit workspace solely so
// RLS and authorization can scope the audit record.
func (s *FederationStore) QuarantineLegacyPeers(ctx context.Context, request contracts.FederationLegacyQuarantineRequest) (contracts.FederationLegacyQuarantinePage, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.FederationLegacyQuarantinePage{}, false, fmt.Errorf("federation store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.FederationLegacyQuarantinePage{}, false, err
	}
	requestHash := contracts.HashStrings(request.RequestID, request.IdempotencyKey, request.AuditWorkspaceID, request.Reason, fmt.Sprint(request.Limit), request.Actor.ID, request.Actor.Kind)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.AuditWorkspaceID)
	if err != nil {
		return contracts.FederationLegacyQuarantinePage{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var existingHash string
	err = tx.QueryRow(ctx, `
		SELECT row_hash
		FROM fornix.federation_legacy_quarantine
		WHERE audit_workspace_id=$1 AND idempotency_key=$2
		ORDER BY id
		LIMIT 1
		FOR UPDATE`, request.AuditWorkspaceID, request.IdempotencyKey).Scan(&existingHash)
	if err == nil {
		// A record's row_hash is not the request hash. Read the request hash from
		// the immutable event marker instead of adding a second operation table.
		var eventPayload []byte
		_ = tx.QueryRow(ctx, `
			SELECT payload
			FROM fornix.control_events
			WHERE workspace_id=$1 AND idempotency_key=$2
			ORDER BY sequence DESC LIMIT 1`, request.AuditWorkspaceID, "federation-quarantine:"+request.AuditWorkspaceID+":"+request.IdempotencyKey).Scan(&eventPayload)
		var eventMarker struct {
			RequestHash string `json:"request_hash"`
		}
		_ = json.Unmarshal(eventPayload, &eventMarker)
		if eventMarker.RequestHash != "" && eventMarker.RequestHash != requestHash {
			return contracts.FederationLegacyQuarantinePage{}, false, ErrFederationQuarantineConflict
		}
		page, readErr := readFederationQuarantineByRequestTx(ctx, tx, request.AuditWorkspaceID, request.IdempotencyKey)
		if readErr != nil {
			return contracts.FederationLegacyQuarantinePage{}, false, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.FederationLegacyQuarantinePage{}, false, err
		}
		return page, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return contracts.FederationLegacyQuarantinePage{}, false, err
	}

	var relation string
	if err := tx.QueryRow(ctx, `SELECT COALESCE(to_regclass('fornix.federation_peers')::text,'')`).Scan(&relation); err != nil {
		return contracts.FederationLegacyQuarantinePage{}, false, err
	}
	if relation == "" {
		if err := tx.Commit(ctx); err != nil {
			return contracts.FederationLegacyQuarantinePage{}, false, err
		}
		return contracts.FederationLegacyQuarantinePage{}, false, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT id,url,last_pull_at,last_pull_high_water
		FROM fornix.federation_peers
		ORDER BY id ASC
		LIMIT $1`, request.Limit)
	if err != nil {
		return contracts.FederationLegacyQuarantinePage{}, false, err
	}
	defer rows.Close()
	type legacyPeer struct {
		id         string
		url        string
		lastPullAt *time.Time
		highWater  int64
	}
	legacyPeers := make([]legacyPeer, 0)
	for rows.Next() {
		var legacyID, sourceURL string
		var lastPullAt *time.Time
		var highWater int64
		if err := rows.Scan(&legacyID, &sourceURL, &lastPullAt, &highWater); err != nil {
			return contracts.FederationLegacyQuarantinePage{}, false, err
		}
		legacyPeers = append(legacyPeers, legacyPeer{id: legacyID, url: sourceURL, lastPullAt: lastPullAt, highWater: highWater})
	}
	if err := rows.Err(); err != nil {
		return contracts.FederationLegacyQuarantinePage{}, false, err
	}
	rows.Close()
	for _, legacy := range legacyPeers {
		legacyID, sourceURL, lastPullAt, highWater := legacy.id, legacy.url, legacy.lastPullAt, legacy.highWater
		createdAt := time.Now().UTC()
		pullAt := ""
		if lastPullAt != nil {
			pullAt = lastPullAt.UTC().Format(time.RFC3339Nano)
		}
		record := contracts.FederationLegacyQuarantineRecord{
			SchemaVersion:    contracts.FederationQuarantineSchemaVersion,
			ID:               "legacy-federation-" + contracts.HashStrings(request.AuditWorkspaceID, "fornix.federation_peers", legacyID)[:48],
			AuditWorkspaceID: request.AuditWorkspaceID, LegacyTable: "fornix.federation_peers", LegacyPeerID: strings.TrimSpace(legacyID),
			SourceURLHash: contracts.HashStrings(sourceURL),
			RowHash:       contracts.HashStrings(legacyID, sourceURL, fmt.Sprint(highWater), pullAt),
			Disposition:   contracts.FederationLegacyQuarantined, Reason: request.Reason,
			RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey, Actor: request.Actor, CreatedAt: createdAt,
		}
		if err := record.Normalize(); err != nil {
			return contracts.FederationLegacyQuarantinePage{}, false, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.federation_legacy_quarantine(
				id,audit_workspace_id,schema_version,legacy_table,legacy_peer_id,source_url_hash,row_hash,disposition,reason,request_id,idempotency_key,actor,created_at,retention_class,retention_state,retention_deadline)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,'operational','active',$13::timestamptz + INTERVAL '30 days')
			ON CONFLICT (audit_workspace_id,legacy_table,legacy_peer_id) DO NOTHING`,
			record.ID, record.AuditWorkspaceID, record.SchemaVersion, record.LegacyTable, record.LegacyPeerID,
			record.SourceURLHash, record.RowHash, record.Disposition, record.Reason, record.RequestID,
			record.IdempotencyKey, mustJSON(record.Actor), record.CreatedAt); err != nil {
			return contracts.FederationLegacyQuarantinePage{}, false, err
		}
	}
	payload := mustJSON(map[string]any{"request_hash": requestHash, "audit_workspace_id": request.AuditWorkspaceID, "idempotency_key": request.IdempotencyKey, "disposition": contracts.FederationLegacyQuarantined})
	event := contracts.EventEnvelope{EventID: contracts.NewID("evt-federation-quarantine"), EventType: "federation.legacy_quarantine_recorded", SchemaVersion: contracts.EventSchemaVersion, OccurredAt: time.Now().UTC(), Scope: contracts.Scope{WorkspaceID: request.AuditWorkspaceID}, Actor: request.Actor, IdempotencyKey: "federation-quarantine:" + request.AuditWorkspaceID + ":" + request.IdempotencyKey, Payload: payload}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.FederationLegacyQuarantinePage{}, false, err
	}
	page, err := readFederationQuarantineByRequestTx(ctx, tx, request.AuditWorkspaceID, request.IdempotencyKey)
	if err != nil {
		return contracts.FederationLegacyQuarantinePage{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationLegacyQuarantinePage{}, false, err
	}
	return page, false, nil
}

// ListLegacyQuarantine returns only redacted records under the explicit audit
// workspace. The cursor is an opaque lexicographic record ID.
func (s *FederationStore) ListLegacyQuarantine(ctx context.Context, workspaceID, cursor string, limit int) (contracts.FederationLegacyQuarantinePage, error) {
	if s == nil || s.pool == nil {
		return contracts.FederationLegacyQuarantinePage{}, fmt.Errorf("federation store is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return contracts.FederationLegacyQuarantinePage{}, fmt.Errorf("audit_workspace_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > maxFederationQuarantineRead {
		limit = maxFederationQuarantineRead
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.FederationLegacyQuarantinePage{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT id,audit_workspace_id,schema_version,legacy_table,legacy_peer_id,source_url_hash,row_hash,disposition,reason,request_id,idempotency_key,actor,created_at
		FROM fornix.federation_legacy_quarantine
		WHERE audit_workspace_id=$1 AND id>$2
		ORDER BY id ASC LIMIT $3`, workspaceID, strings.TrimSpace(cursor), limit+1)
	if err != nil {
		return contracts.FederationLegacyQuarantinePage{}, err
	}
	defer rows.Close()
	page := contracts.FederationLegacyQuarantinePage{Items: make([]contracts.FederationLegacyQuarantineRecord, 0)}
	for rows.Next() {
		record, scanErr := scanFederationQuarantine(rows)
		if scanErr != nil {
			return contracts.FederationLegacyQuarantinePage{}, scanErr
		}
		if len(page.Items) == limit {
			page.NextCursor = page.Items[len(page.Items)-1].ID
			break
		}
		page.Items = append(page.Items, record)
	}
	if err := rows.Err(); err != nil {
		return contracts.FederationLegacyQuarantinePage{}, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationLegacyQuarantinePage{}, err
	}
	return page, nil
}

func readFederationQuarantineByRequestTx(ctx context.Context, tx pgx.Tx, workspaceID, idempotencyKey string) (contracts.FederationLegacyQuarantinePage, error) {
	rows, err := tx.Query(ctx, `
		SELECT id,audit_workspace_id,schema_version,legacy_table,legacy_peer_id,source_url_hash,row_hash,disposition,reason,request_id,idempotency_key,actor,created_at
		FROM fornix.federation_legacy_quarantine
		WHERE audit_workspace_id=$1 AND idempotency_key=$2
		ORDER BY id ASC`, workspaceID, idempotencyKey)
	if err != nil {
		return contracts.FederationLegacyQuarantinePage{}, err
	}
	defer rows.Close()
	page := contracts.FederationLegacyQuarantinePage{Items: make([]contracts.FederationLegacyQuarantineRecord, 0, maxFederationQuarantineRead)}
	for rows.Next() {
		record, scanErr := scanFederationQuarantine(rows)
		if scanErr != nil {
			return contracts.FederationLegacyQuarantinePage{}, scanErr
		}
		if len(page.Items) < maxFederationQuarantineRead {
			page.Items = append(page.Items, record)
		}
	}
	return page, rows.Err()
}

func scanFederationQuarantine(row interface{ Scan(...any) error }) (contracts.FederationLegacyQuarantineRecord, error) {
	var record contracts.FederationLegacyQuarantineRecord
	var actorJSON []byte
	if err := row.Scan(&record.ID, &record.AuditWorkspaceID, &record.SchemaVersion, &record.LegacyTable, &record.LegacyPeerID, &record.SourceURLHash, &record.RowHash, &record.Disposition, &record.Reason, &record.RequestID, &record.IdempotencyKey, &actorJSON, &record.CreatedAt); err != nil {
		return contracts.FederationLegacyQuarantineRecord{}, err
	}
	if err := json.Unmarshal(actorJSON, &record.Actor); err != nil {
		return contracts.FederationLegacyQuarantineRecord{}, err
	}
	if err := record.Normalize(); err != nil {
		return contracts.FederationLegacyQuarantineRecord{}, err
	}
	return record, nil
}
