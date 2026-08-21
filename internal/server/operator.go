package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func (s *server) handleOperatorBootstrap(w http.ResponseWriter, r *http.Request) {
	var request contracts.WorkspaceBootstrapRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid bootstrap request")
		return
	}
	request.RequestID = requestIDFromRequest(r)
	request.Actor = requestActor(r)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	result, err := s.operator.Bootstrap(r.Context(), request)
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	if result.Workspace.ToolRoot != "" {
		_ = s.toolExecutor.Policy.RegisterWorkspaceTool(result.Workspace.ID, "fornix.repository.read", "repository.read", result.Workspace.ToolRoot)
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleOperatorWorkspaces(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/operator/workspaces" || r.URL.Path == "/v1/operator/workspaces/" {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		page, err := s.operator.ListWorkspaces(r.Context(), limit, r.URL.Query().Get("cursor"))
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		if principal, ok := principalFromRequest(r); ok && !principal.Development {
			filtered := make([]contracts.Workspace, 0, 1)
			for _, workspace := range page.Items {
				if workspace.ID == principal.WorkspaceID {
					filtered = append(filtered, workspace)
				}
			}
			page.Items = filtered
			page.NextCursor = ""
		}
		writeJSON(w, http.StatusOK, page)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/operator/workspaces/")
	if r.Method != http.MethodGet || strings.TrimSpace(id) == "" {
		writeErr(w, http.StatusMethodNotAllowed, "GET workspace only")
		return
	}
	if principal, ok := principalFromRequest(r); ok && !principal.Development && principal.WorkspaceID != id {
		writeErr(w, http.StatusForbidden, "workspace access denied")
		return
	}
	workspace, err := s.operator.GetWorkspace(r.Context(), id)
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, workspace)
}

func (s *server) handleOperatorIdentities(w http.ResponseWriter, r *http.Request) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	if r.URL.Path == "/v1/operator/identities" || r.URL.Path == "/v1/operator/identities/" {
		if r.Method == http.MethodGet {
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			page, err := s.auth.ListIdentities(r.Context(), workspaceID, limit, r.URL.Query().Get("cursor"))
			if err != nil {
				writeOperatorError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, page)
			return
		}
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
			return
		}
		var input struct {
			Subject     string                 `json:"subject"`
			Kind        string                 `json:"kind,omitempty"`
			DisplayName string                 `json:"display_name,omitempty"`
			Permissions []contracts.Permission `json:"permissions,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid identity request")
			return
		}
		identity, err := s.auth.CreateIdentity(r.Context(), contracts.IdentityInput{WorkspaceID: workspaceID, Subject: input.Subject, Kind: input.Kind, DisplayName: input.DisplayName, Permissions: input.Permissions})
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, identity)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/operator/identities/"), "/")
	if len(parts) == 2 && parts[1] == "disable" && r.Method == http.MethodPost {
		if err := s.auth.DisableIdentity(r.Context(), workspaceID, parts[0]); err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"id": parts[0], "status": contracts.IdentityDisabled})
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		identity, err := s.auth.GetIdentity(r.Context(), workspaceID, parts[0])
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, identity)
		return
	}
	writeErr(w, http.StatusNotFound, "unknown identity operation")
}

func (s *server) handleOperatorRoles(w http.ResponseWriter, r *http.Request) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	if r.URL.Path == "/v1/operator/roles" || r.URL.Path == "/v1/operator/roles/" {
		if r.Method == http.MethodGet {
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			page, err := s.auth.ListRoles(r.Context(), workspaceID, limit, r.URL.Query().Get("cursor"))
			if err != nil {
				writeOperatorError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, page)
			return
		}
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
			return
		}
		var input struct {
			IdentityID  string                 `json:"identity_id"`
			Name        string                 `json:"name"`
			Permissions []contracts.Permission `json:"permissions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid role request")
			return
		}
		role, err := s.auth.GrantRole(r.Context(), workspaceID, input.IdentityID, input.Name, input.Permissions, requestActor(r).ID)
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, role)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/operator/roles/"), "/")
	if len(parts) == 3 && parts[2] == "unbind" && r.Method == http.MethodPost {
		if err := s.auth.UnbindRole(r.Context(), workspaceID, parts[0], parts[1]); err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"identity_id": parts[0], "role_id": parts[1], "unbound": true})
		return
	}
	writeErr(w, http.StatusNotFound, "unknown role operation")
}

func (s *server) handleOperatorAPIKeys(w http.ResponseWriter, r *http.Request) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	if r.URL.Path == "/v1/operator/api-keys" || r.URL.Path == "/v1/operator/api-keys/" {
		if r.Method == http.MethodGet {
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			page, err := s.auth.ListAPIKeys(r.Context(), workspaceID, limit, r.URL.Query().Get("cursor"))
			if err != nil {
				writeOperatorError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, page)
			return
		}
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
			return
		}
		var input struct {
			IdentityID string     `json:"identity_id"`
			ExpiresAt  *time.Time `json:"expires_at,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid api key request")
			return
		}
		key, token, err := s.auth.CreateAPIKey(r.Context(), contracts.APIKeyInput{WorkspaceID: workspaceID, IdentityID: input.IdentityID, ExpiresAt: input.ExpiresAt})
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"api_key": key, "api_key_token": token, "token_created": true})
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/operator/api-keys/"), "/")
	if len(parts) >= 2 && r.Method == http.MethodPost {
		switch parts[1] {
		case "rotate":
			var input struct {
				ExpiresAt *time.Time `json:"expires_at,omitempty"`
			}
			_ = json.NewDecoder(r.Body).Decode(&input)
			key, token, err := s.auth.RotateAPIKey(r.Context(), workspaceID, parts[0], input.ExpiresAt)
			if err != nil {
				writeOperatorError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"api_key": key, "api_key_token": token, "token_created": true})
			return
		case "revoke":
			if err := s.auth.RevokeAPIKey(r.Context(), workspaceID, parts[0]); err != nil {
				writeOperatorError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"id": parts[0], "status": contracts.APIKeyRevoked})
			return
		}
	}
	writeErr(w, http.StatusNotFound, "unknown api key operation")
}

func (s *server) handleOperatorIngests(w http.ResponseWriter, r *http.Request) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	if r.Method == http.MethodGet {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		page, err := s.operator.ListRepositoryIngests(r.Context(), workspaceID, limit, r.URL.Query().Get("cursor"))
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
		return
	}
	var request contracts.RepositoryIngestRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid ingest request")
		return
	}
	request.WorkspaceID = workspaceID
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	record, created, err := s.operator.UpsertRepositoryIngest(r.Context(), request)
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ingest": record, "created": created, "deduped": !created})
}

func (s *server) handleOperatorIngest(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/operator/ingests/")
	if id == "" || strings.Contains(id, "/") || r.Method != http.MethodGet {
		writeErr(w, http.StatusNotFound, "ingest id required")
		return
	}
	record, err := s.operator.GetRepositoryIngest(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), id)
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func writeOperatorError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, store.ErrWorkspaceNotFound), errors.Is(err, store.ErrIdentityNotFound), errors.Is(err, store.ErrAPIKeyNotFound), errors.Is(err, store.ErrIngestNotFound), errors.Is(err, store.ErrIngestJobNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrOperatorConflict), errors.Is(err, store.ErrIdentityExists), errors.Is(err, store.ErrIngestConflict):
		status = http.StatusConflict
	case errors.Is(err, store.ErrWorkspaceViolation), errors.Is(err, store.ErrAuthorizationDenied), errors.Is(err, store.ErrIngestFence):
		status = http.StatusForbidden
	case errors.Is(err, store.ErrIngestCheckpoint), errors.Is(err, store.ErrIngestTerminal):
		status = http.StatusConflict
	case errors.Is(err, store.ErrIngestPathChanged):
		status = http.StatusConflict
	}
	writeErr(w, status, shortError(err, 320))
}

// Keep a context helper in this file so future operator actions use the same
// bounded request lifetime as the existing handlers.
func operatorContext(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 15*time.Second)
}
