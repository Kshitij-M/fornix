package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	OperatorSchemaVersion = 1
	WorkspaceActive       = "active"
	WorkspaceDisabled     = "disabled"
	IngestPending         = "pending"
	IngestRunning         = "running"
	IngestSucceeded       = "succeeded"
	IngestFailed          = "failed"
)

var workspaceIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

type Workspace struct {
	SchemaVersion   int       `json:"schema_version"`
	ID              string    `json:"id"`
	DisplayName     string    `json:"display_name"`
	Status          string    `json:"status"`
	DefaultProvider string    `json:"default_provider"`
	ToolRoot        string    `json:"tool_root,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type WorkspaceBootstrapRequest struct {
	WorkspaceID     string       `json:"workspace_id"`
	DisplayName     string       `json:"display_name,omitempty"`
	Subject         string       `json:"subject,omitempty"`
	IdentityKind    string       `json:"identity_kind,omitempty"`
	IdentityID      string       `json:"identity_id,omitempty"`
	RoleName        string       `json:"role_name,omitempty"`
	Permissions     []Permission `json:"permissions,omitempty"`
	DefaultProvider string       `json:"default_provider,omitempty"`
	ToolRoot        string       `json:"tool_root,omitempty"`
	APIKeyExpiresAt *time.Time   `json:"api_key_expires_at,omitempty"`
	IdempotencyKey  string       `json:"idempotency_key,omitempty"`
	RequestID       string       `json:"-"`
	Actor           ActorRef     `json:"-"`
}

type WorkspaceBootstrapResult struct {
	Workspace    Workspace `json:"workspace"`
	Identity     Identity  `json:"identity"`
	Role         Role      `json:"role"`
	APIKey       APIKey    `json:"api_key"`
	APIKeyToken  string    `json:"api_key_token,omitempty"`
	Created      bool      `json:"created"`
	TokenCreated bool      `json:"token_created"`
}

type RepositoryIngestRequest struct {
	WorkspaceID    string `json:"workspace_id"`
	Repository     string `json:"repository"`
	SourceRoot     string `json:"source_root,omitempty"`
	ManifestHash   string `json:"manifest_hash"`
	FileCount      int    `json:"file_count"`
	ChunkCount     int    `json:"chunk_count"`
	SymbolCount    int    `json:"symbol_count"`
	ByteCount      int64  `json:"byte_count"`
	Status         string `json:"status"`
	LastError      string `json:"last_error,omitempty"`
	IdempotencyKey string `json:"idempotency_key"`
}

type RepositoryIngest struct {
	SchemaVersion  int       `json:"schema_version"`
	ID             string    `json:"id"`
	WorkspaceID    string    `json:"workspace_id"`
	Repository     string    `json:"repository"`
	SourceRoot     string    `json:"source_root,omitempty"`
	ManifestHash   string    `json:"manifest_hash"`
	RequestHash    string    `json:"request_hash"`
	IdempotencyKey string    `json:"idempotency_key"`
	Status         string    `json:"status"`
	FileCount      int       `json:"file_count"`
	ChunkCount     int       `json:"chunk_count"`
	SymbolCount    int       `json:"symbol_count"`
	ByteCount      int64     `json:"byte_count"`
	LastError      string    `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type OperatorAudit struct {
	ID             int64             `json:"id"`
	WorkspaceID    string            `json:"workspace_id"`
	RequestID      string            `json:"request_id"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Actor          AuditActor        `json:"actor"`
	Operation      string            `json:"operation"`
	Resource       string            `json:"resource,omitempty"`
	Outcome        string            `json:"outcome"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
}

func (r WorkspaceBootstrapRequest) Normalize() (WorkspaceBootstrapRequest, error) {
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	if !workspaceIDPattern.MatchString(r.WorkspaceID) {
		return WorkspaceBootstrapRequest{}, fmt.Errorf("workspace_id must be an alphanumeric slug of at most 64 characters")
	}
	r.DisplayName = strings.TrimSpace(r.DisplayName)
	if r.DisplayName == "" {
		r.DisplayName = r.WorkspaceID
	}
	r.Subject = strings.TrimSpace(r.Subject)
	if r.Subject == "" {
		r.Subject = "operator"
	}
	r.IdentityKind = strings.TrimSpace(r.IdentityKind)
	if r.IdentityKind == "" {
		r.IdentityKind = "user"
	}
	r.RoleName = strings.TrimSpace(r.RoleName)
	if r.RoleName == "" {
		r.RoleName = "operator"
	}
	r.DefaultProvider = strings.ToLower(strings.TrimSpace(r.DefaultProvider))
	if r.DefaultProvider == "" {
		r.DefaultProvider = "fake"
	}
	r.ToolRoot = strings.TrimSpace(r.ToolRoot)
	if r.ToolRoot != "" && !filepath.IsAbs(r.ToolRoot) {
		return WorkspaceBootstrapRequest{}, fmt.Errorf("tool_root must be an absolute path")
	}
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	if r.IdempotencyKey == "" {
		r.IdempotencyKey = "bootstrap:" + r.WorkspaceID + ":" + r.Subject
	}
	if len(r.IdempotencyKey) > MaxIdempotencyLength {
		return WorkspaceBootstrapRequest{}, fmt.Errorf("idempotency_key is too large")
	}
	permissions, err := NormalizePermissions(r.Permissions)
	if err != nil {
		return WorkspaceBootstrapRequest{}, err
	}
	if len(permissions) == 0 {
		permissions = []Permission{
			PermissionWorkspaceRead, PermissionWorkspaceWrite, PermissionIdentityAdmin,
			PermissionCredentialUse, PermissionTaskRead, PermissionTaskMutate,
			PermissionTaskExecute, PermissionRetrievalRead, PermissionRetrievalWrite,
			PermissionEvidenceRead, PermissionEvidenceWrite, PermissionAgentRun,
			PermissionAgentRead, PermissionModelInvoke, PermissionToolExecute,
			PermissionToolApprove, PermissionEvaluationRead, PermissionEvaluationRun,
			PermissionEvaluationWrite, PermissionSchedulerRun,
		}
	}
	r.Permissions = permissions
	return r, nil
}

func (r RepositoryIngestRequest) Normalize() (RepositoryIngestRequest, error) {
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.Repository = strings.TrimSpace(r.Repository)
	r.SourceRoot = strings.TrimSpace(r.SourceRoot)
	r.ManifestHash = strings.ToLower(strings.TrimSpace(r.ManifestHash))
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	if r.WorkspaceID == "" || r.Repository == "" || r.IdempotencyKey == "" {
		return RepositoryIngestRequest{}, fmt.Errorf("workspace_id, repository, and idempotency_key are required")
	}
	if len(r.ManifestHash) != 64 {
		return RepositoryIngestRequest{}, fmt.Errorf("manifest_hash must be a SHA-256 digest")
	}
	if r.Status == "" {
		r.Status = IngestSucceeded
	}
	if r.Status != IngestPending && r.Status != IngestRunning && r.Status != IngestSucceeded && r.Status != IngestFailed {
		return RepositoryIngestRequest{}, fmt.Errorf("invalid ingest status %q", r.Status)
	}
	if r.FileCount < 0 || r.ChunkCount < 0 || r.SymbolCount < 0 || r.ByteCount < 0 {
		return RepositoryIngestRequest{}, fmt.Errorf("ingest counts cannot be negative")
	}
	return r, nil
}

func HashRepositoryManifest(files map[string]string) string {
	keys := make([]string, 0, len(files))
	for key := range files {
		keys = append(keys, key)
	}
	sortStrings(keys)
	h := sha256.New()
	for _, key := range keys {
		h.Write([]byte(key))
		h.Write([]byte("\x00"))
		h.Write([]byte(files[key]))
		h.Write([]byte("\x00"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (r RepositoryIngestRequest) RequestHash() string {
	clone := r
	clone.IdempotencyKey, clone.Status, clone.LastError = "", "", ""
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
