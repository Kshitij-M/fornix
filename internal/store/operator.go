package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
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
	ErrWorkspaceNotFound = errors.New("workspace not found")
	ErrOperatorConflict  = errors.New("operator request conflicts with existing state")
	ErrIngestNotFound    = errors.New("repository ingest not found")
)

type WorkspacePage struct {
	Items      []contracts.Workspace `json:"workspaces"`
	NextCursor string                `json:"next_cursor,omitempty"`
}

type IdentityPage struct {
	Items      []contracts.Identity `json:"identities"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

type RolePage struct {
	Items      []contracts.Role `json:"roles"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type APIKeyPage struct {
	Items      []contracts.APIKey `json:"api_keys"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

type IngestPage struct {
	Items      []contracts.RepositoryIngest `json:"ingests"`
	NextCursor string                       `json:"next_cursor,omitempty"`
}

type OperatorStore struct {
	pool   *pgxpool.Pool
	events *EventStore
}

func NewOperatorStore(pool *pgxpool.Pool, events *EventStore) *OperatorStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &OperatorStore{pool: pool, events: events}
}

func (s *OperatorStore) Bootstrap(ctx context.Context, request contracts.WorkspaceBootstrapRequest) (contracts.WorkspaceBootstrapResult, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("operator store is not configured")
	}
	normalized, err := request.Normalize()
	if err != nil {
		return contracts.WorkspaceBootstrapResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("begin workspace bootstrap: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var workspaceExisted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM fornix.workspaces WHERE id=$1)`, normalized.WorkspaceID).Scan(&workspaceExisted); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("check workspace: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.workspaces(id,display_name,status,default_provider,tool_root)
		VALUES($1,$2,'active',$3,$4)
		ON CONFLICT (id) DO UPDATE SET
		  display_name=CASE WHEN fornix.workspaces.display_name='' THEN EXCLUDED.display_name ELSE fornix.workspaces.display_name END,
		  default_provider=CASE WHEN fornix.workspaces.default_provider='' THEN EXCLUDED.default_provider ELSE fornix.workspaces.default_provider END,
		  tool_root=CASE WHEN fornix.workspaces.tool_root='' THEN EXCLUDED.tool_root ELSE fornix.workspaces.tool_root END,
		  updated_at=clock_timestamp()`, normalized.WorkspaceID, normalized.DisplayName, normalized.DefaultProvider, normalized.ToolRoot); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("ensure workspace: %w", err)
	}

	var workspace contracts.Workspace
	if err := tx.QueryRow(ctx, `SELECT 1,id,display_name,status,default_provider,tool_root,created_at,updated_at FROM fornix.workspaces WHERE id=$1`, normalized.WorkspaceID).
		Scan(&workspace.SchemaVersion, &workspace.ID, &workspace.DisplayName, &workspace.Status, &workspace.DefaultProvider, &workspace.ToolRoot, &workspace.CreatedAt, &workspace.UpdatedAt); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("read workspace: %w", err)
	}

	identityID := strings.TrimSpace(normalized.IdentityID)
	if identityID == "" {
		identityID = stableOperatorID("identity", normalized.WorkspaceID, normalized.Subject)
	}
	var identity contracts.Identity
	if err := tx.QueryRow(ctx, `
		INSERT INTO fornix.identities(id,workspace_id,subject,kind,display_name,status)
		VALUES($1,$2,$3,$4,$5,'active')
		ON CONFLICT (workspace_id,subject) DO UPDATE SET
		  display_name=CASE WHEN fornix.identities.display_name='' THEN EXCLUDED.display_name ELSE fornix.identities.display_name END,
		  updated_at=clock_timestamp()
		RETURNING 1,id,workspace_id,subject,kind,display_name,status,created_at,updated_at`, identityID, normalized.WorkspaceID, normalized.Subject, normalized.IdentityKind, normalized.Subject).
		Scan(&identity.SchemaVersion, &identity.ID, &identity.WorkspaceID, &identity.Subject, &identity.Kind, &identity.DisplayName, &identity.Status, &identity.CreatedAt, &identity.UpdatedAt); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("ensure bootstrap identity: %w", err)
	}

	roleID := stableOperatorID("role", normalized.WorkspaceID, normalized.RoleName)
	permissionsJSON, _ := json.Marshal(normalized.Permissions)
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.roles(id,workspace_id,name,permissions)
		VALUES($1,$2,$3,$4::jsonb)
		ON CONFLICT (workspace_id,name) DO NOTHING`, roleID, normalized.WorkspaceID, normalized.RoleName, permissionsJSON); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("ensure bootstrap role: %w", err)
	}
	var role contracts.Role
	var storedPermissions []byte
	if err := tx.QueryRow(ctx, `SELECT 1,id,workspace_id,name,permissions,created_at,updated_at FROM fornix.roles WHERE workspace_id=$1 AND name=$2 FOR UPDATE`, normalized.WorkspaceID, normalized.RoleName).
		Scan(&role.SchemaVersion, &role.ID, &role.WorkspaceID, &role.Name, &storedPermissions, &role.CreatedAt, &role.UpdatedAt); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("read bootstrap role: %w", err)
	}
	if err := json.Unmarshal(storedPermissions, &role.Permissions); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("decode bootstrap role: %w", err)
	}
	mergedPermissions, err := mergePermissions(role.Permissions, normalized.Permissions)
	if err != nil {
		return contracts.WorkspaceBootstrapResult{}, err
	}
	if len(mergedPermissions) != len(role.Permissions) {
		mergedJSON, _ := json.Marshal(mergedPermissions)
		if _, err := tx.Exec(ctx, `UPDATE fornix.roles SET permissions=$1::jsonb,updated_at=clock_timestamp() WHERE workspace_id=$2 AND id=$3`, mergedJSON, normalized.WorkspaceID, role.ID); err != nil {
			return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("extend bootstrap role: %w", err)
		}
		role.Permissions = mergedPermissions
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.identity_role_bindings(workspace_id,identity_id,role_id,granted_by)
		VALUES($1,$2,$3,$4)
		ON CONFLICT (workspace_id,identity_id,role_id) DO NOTHING`, normalized.WorkspaceID, identity.ID, role.ID, normalized.Actor.ID); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("bind bootstrap role: %w", err)
	}

	var apiKey contracts.APIKey
	var token string
	var expiresAt, revokedAt, lastUsedAt *time.Time
	var apiKeyCreated bool
	var apiKeyID, prefix, status string
	if err := tx.QueryRow(ctx, `
		SELECT id,prefix,status,expires_at,revoked_at,created_at,last_used_at
		FROM fornix.api_keys
		WHERE workspace_id=$1 AND identity_id=$2 AND status='active'
		ORDER BY created_at,id LIMIT 1 FOR UPDATE`, normalized.WorkspaceID, identity.ID).
		Scan(&apiKeyID, &prefix, &status, &expiresAt, &revokedAt, &apiKey.CreatedAt, &lastUsedAt); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("read bootstrap api key: %w", err)
		}
		apiKeyCreated = true
		apiKeyID = stableOperatorID("key", normalized.WorkspaceID, normalized.Subject, normalized.IdempotencyKey)
		secretBytes := make([]byte, 32)
		if _, err := rand.Read(secretBytes); err != nil {
			return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("generate bootstrap api key: %w", err)
		}
		secret := hex.EncodeToString(secretBytes)
		token = "fornix_" + apiKeyID + "_" + secret
		prefix = "fornix_" + apiKeyID + "_"
		digest := sha256.Sum256([]byte(secret))
		if err := tx.QueryRow(ctx, `INSERT INTO fornix.api_keys(id,workspace_id,identity_id,prefix,token_hash,status,expires_at) VALUES($1,$2,$3,$4,$5,'active',$6) RETURNING created_at`, apiKeyID, normalized.WorkspaceID, identity.ID, prefix, digest[:], normalized.APIKeyExpiresAt).Scan(&apiKey.CreatedAt); err != nil {
			return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("insert bootstrap api key: %w", err)
		}
		expiresAt = normalized.APIKeyExpiresAt
	} else {
		if expiresAt != nil && !expiresAt.After(time.Now().UTC()) {
			return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("existing bootstrap api key is expired; rotate it explicitly")
		}
	}
	apiKey.SchemaVersion, apiKey.ID, apiKey.WorkspaceID, apiKey.IdentityID = contracts.IdentitySchemaVersion, apiKeyID, normalized.WorkspaceID, identity.ID
	apiKey.Prefix, apiKey.Status, apiKey.ExpiresAt, apiKey.RevokedAt, apiKey.LastUsedAt = prefix, status, expiresAt, revokedAt, lastUsedAt
	if apiKey.Status == "" {
		apiKey.Status = contracts.APIKeyActive
	}

	requestID := strings.TrimSpace(normalized.RequestID)
	if requestID == "" {
		requestID = normalized.IdempotencyKey
	}
	auditActor := normalized.Actor
	if auditActor.ID == "" {
		auditActor = contracts.ActorRef{ID: "bootstrap", Kind: "bootstrap", WorkspaceID: normalized.WorkspaceID}
	}
	auditActorJSON, err := json.Marshal(auditActor)
	if err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("encode bootstrap actor: %w", err)
	}
	metadata := map[string]string{"identity_id": identity.ID, "role_id": role.ID, "api_key_id": apiKey.ID}
	metadataJSON, _ := json.Marshal(metadata)
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.operator_audit(workspace_id,request_id,idempotency_key,actor,operation,resource,outcome,metadata)
		VALUES($1,$2,$3,$4::jsonb,'workspace.bootstrap',$1,$5,$6::jsonb)
		ON CONFLICT (workspace_id,request_id,operation,resource) DO NOTHING`, normalized.WorkspaceID, requestID, normalized.IdempotencyKey, auditActorJSON, map[bool]string{true: "accepted", false: "deduped"}[apiKeyCreated], metadataJSON); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("write bootstrap audit: %w", err)
	}
	event, err := contracts.NewEvent("operator.workspace_bootstrapped", map[string]any{"workspace_id": normalized.WorkspaceID, "identity_id": identity.ID, "role_id": role.ID, "api_key_id": apiKey.ID, "default_provider": workspace.DefaultProvider, "tool_root": workspace.ToolRoot})
	if err != nil {
		return contracts.WorkspaceBootstrapResult{}, err
	}
	event.Scope.WorkspaceID, event.Actor, event.IdempotencyKey = normalized.WorkspaceID, auditActor, normalized.IdempotencyKey
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("append bootstrap event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.WorkspaceBootstrapResult{}, fmt.Errorf("commit workspace bootstrap: %w", err)
	}
	return contracts.WorkspaceBootstrapResult{Workspace: workspace, Identity: identity, Role: role, APIKey: apiKey, APIKeyToken: token, Created: !workspaceExisted, TokenCreated: apiKeyCreated}, nil
}

func (s *OperatorStore) GetWorkspace(ctx context.Context, id string) (contracts.Workspace, error) {
	if s == nil || s.pool == nil {
		return contracts.Workspace{}, fmt.Errorf("operator store is not configured")
	}
	var workspace contracts.Workspace
	err := s.pool.QueryRow(ctx, `SELECT 1,id,display_name,status,default_provider,tool_root,created_at,updated_at FROM fornix.workspaces WHERE id=$1`, strings.TrimSpace(id)).Scan(&workspace.SchemaVersion, &workspace.ID, &workspace.DisplayName, &workspace.Status, &workspace.DefaultProvider, &workspace.ToolRoot, &workspace.CreatedAt, &workspace.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.Workspace{}, ErrWorkspaceNotFound
	}
	return workspace, err
}

func (s *OperatorStore) ListWorkspaces(ctx context.Context, limit int, cursor string) (WorkspacePage, error) {
	limit = boundedOperatorLimit(limit)
	query := `SELECT 1,id,display_name,status,default_provider,tool_root,created_at,updated_at FROM fornix.workspaces WHERE id > $1 ORDER BY id LIMIT $2`
	rows, err := s.pool.Query(ctx, query, strings.TrimSpace(cursor), limit+1)
	if err != nil {
		return WorkspacePage{}, err
	}
	defer rows.Close()
	page := WorkspacePage{Items: make([]contracts.Workspace, 0, limit)}
	for rows.Next() {
		var item contracts.Workspace
		if err := rows.Scan(&item.SchemaVersion, &item.ID, &item.DisplayName, &item.Status, &item.DefaultProvider, &item.ToolRoot, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return WorkspacePage{}, err
		}
		page.Items = append(page.Items, item)
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, rows.Err()
}

func (s *OperatorStore) UpsertRepositoryIngest(ctx context.Context, input contracts.RepositoryIngestRequest) (contracts.RepositoryIngest, bool, error) {
	normalized, err := input.Normalize()
	if err != nil {
		return contracts.RepositoryIngest{}, false, err
	}
	requestHash := normalized.RequestHash()
	id := stableOperatorID("ingest", normalized.WorkspaceID, normalized.Repository, normalized.ManifestHash)
	var record contracts.RepositoryIngest
	err = s.pool.QueryRow(ctx, `
		INSERT INTO fornix.repository_ingests(id,workspace_id,repository,source_root,manifest_hash,request_hash,idempotency_key,status,file_count,chunk_count,symbol_count,byte_count,last_error)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING
		RETURNING 1,id,workspace_id,repository,source_root,manifest_hash,request_hash,idempotency_key,status,file_count,chunk_count,symbol_count,byte_count,last_error,created_at,updated_at`, id, normalized.WorkspaceID, normalized.Repository, normalized.SourceRoot, normalized.ManifestHash, requestHash, normalized.IdempotencyKey, normalized.Status, normalized.FileCount, normalized.ChunkCount, normalized.SymbolCount, normalized.ByteCount, normalized.LastError).
		Scan(&record.SchemaVersion, &record.ID, &record.WorkspaceID, &record.Repository, &record.SourceRoot, &record.ManifestHash, &record.RequestHash, &record.IdempotencyKey, &record.Status, &record.FileCount, &record.ChunkCount, &record.SymbolCount, &record.ByteCount, &record.LastError, &record.CreatedAt, &record.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			var existing contracts.RepositoryIngest
			err = s.pool.QueryRow(ctx, `SELECT 1,id,workspace_id,repository,source_root,manifest_hash,request_hash,idempotency_key,status,file_count,chunk_count,symbol_count,byte_count,last_error,created_at,updated_at FROM fornix.repository_ingests WHERE workspace_id=$1 AND idempotency_key=$2`, normalized.WorkspaceID, normalized.IdempotencyKey).
				Scan(&existing.SchemaVersion, &existing.ID, &existing.WorkspaceID, &existing.Repository, &existing.SourceRoot, &existing.ManifestHash, &existing.RequestHash, &existing.IdempotencyKey, &existing.Status, &existing.FileCount, &existing.ChunkCount, &existing.SymbolCount, &existing.ByteCount, &existing.LastError, &existing.CreatedAt, &existing.UpdatedAt)
			if err != nil {
				return contracts.RepositoryIngest{}, false, err
			}
			if existing.RequestHash != requestHash {
				return contracts.RepositoryIngest{}, false, fmt.Errorf("%w: ingest idempotency key reused with a different request", ErrOperatorConflict)
			}
			return existing, false, nil
		}
		if isUniqueViolation(err) {
			return contracts.RepositoryIngest{}, false, ErrOperatorConflict
		}
		return contracts.RepositoryIngest{}, false, err
	}
	return record, true, nil
}

func (s *OperatorStore) GetRepositoryIngest(ctx context.Context, workspaceID, id string) (contracts.RepositoryIngest, error) {
	var record contracts.RepositoryIngest
	err := s.pool.QueryRow(ctx, `SELECT 1,id,workspace_id,repository,source_root,manifest_hash,request_hash,idempotency_key,status,file_count,chunk_count,symbol_count,byte_count,last_error,created_at,updated_at FROM fornix.repository_ingests WHERE workspace_id=$1 AND id=$2`, strings.TrimSpace(workspaceID), strings.TrimSpace(id)).Scan(&record.SchemaVersion, &record.ID, &record.WorkspaceID, &record.Repository, &record.SourceRoot, &record.ManifestHash, &record.RequestHash, &record.IdempotencyKey, &record.Status, &record.FileCount, &record.ChunkCount, &record.SymbolCount, &record.ByteCount, &record.LastError, &record.CreatedAt, &record.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.RepositoryIngest{}, ErrIngestNotFound
	}
	return record, err
}

func (s *OperatorStore) ListRepositoryIngests(ctx context.Context, workspaceID string, limit int, cursor string) (IngestPage, error) {
	limit = boundedOperatorLimit(limit)
	rows, err := s.pool.Query(ctx, `SELECT 1,id,workspace_id,repository,source_root,manifest_hash,request_hash,idempotency_key,status,file_count,chunk_count,symbol_count,byte_count,last_error,created_at,updated_at FROM fornix.repository_ingests WHERE workspace_id=$1 AND id > $2 ORDER BY id LIMIT $3`, strings.TrimSpace(workspaceID), strings.TrimSpace(cursor), limit+1)
	if err != nil {
		return IngestPage{}, err
	}
	defer rows.Close()
	page := IngestPage{Items: make([]contracts.RepositoryIngest, 0, limit)}
	for rows.Next() {
		var item contracts.RepositoryIngest
		if err := rows.Scan(&item.SchemaVersion, &item.ID, &item.WorkspaceID, &item.Repository, &item.SourceRoot, &item.ManifestHash, &item.RequestHash, &item.IdempotencyKey, &item.Status, &item.FileCount, &item.ChunkCount, &item.SymbolCount, &item.ByteCount, &item.LastError, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return IngestPage{}, err
		}
		page.Items = append(page.Items, item)
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, rows.Err()
}

func stableOperatorID(prefix string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte(prefix))
	for _, part := range parts {
		h.Write([]byte{0})
		h.Write([]byte(strings.TrimSpace(part)))
	}
	return prefix + "_" + hex.EncodeToString(h.Sum(nil)[:16])
}

func mergePermissions(left, right []contracts.Permission) ([]contracts.Permission, error) {
	return contracts.NormalizePermissions(append(append([]contracts.Permission(nil), left...), right...))
}

func boundedOperatorLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 100 {
		return 100
	}
	return limit
}
