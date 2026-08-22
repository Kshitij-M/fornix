package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

type requestContextKey string

const (
	principalContextKey requestContextKey = "fornix.principal"
	requestIDContextKey requestContextKey = "fornix.request_id"
)

func withPrincipal(r *http.Request, principal contracts.Principal) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), principalContextKey, principal))
}

func principalFromRequest(r *http.Request) (contracts.Principal, bool) {
	if r == nil {
		return contracts.Principal{}, false
	}
	principal, ok := r.Context().Value(principalContextKey).(contracts.Principal)
	return principal, ok
}

func requestIDFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if id, ok := r.Context().Value(requestIDContextKey).(string); ok {
		return id
	}
	return strings.TrimSpace(r.Header.Get("X-Request-ID"))
}

func requestActor(r *http.Request) contracts.ActorRef {
	if principal, ok := principalFromRequest(r); ok {
		return principal.Actor()
	}
	return contracts.ActorRef{ID: "internal", Kind: "service"}
}

func (s *server) authenticateRequest(r *http.Request) (contracts.Principal, error) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(header, "Bearer ") {
		return contracts.Principal{}, store.ErrUnauthenticated
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	if token == "" {
		return contracts.Principal{}, store.ErrUnauthenticated
	}
	// Bootstrap is intentionally a narrow, explicit escape hatch. It is only
	// accepted on the workspace-bootstrap route and is never persisted or
	// included in the resulting principal/audit payload.
	if r.URL.Path == "/v1/operator/workspaces/bootstrap" && strings.TrimSpace(s.bootstrapKey) != "" {
		expected := sha256.Sum256([]byte(s.bootstrapKey))
		provided := sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(expected[:], provided[:]) == 1 {
			workspaceID := contracts.DefaultWorkspaceID
			if candidates := requestWorkspaceCandidates(r); len(candidates) > 0 {
				workspaceID = candidates[0]
			}
			return contracts.Principal{ID: "bootstrap", WorkspaceID: workspaceID, Subject: "bootstrap", Kind: "bootstrap", DisplayName: "bootstrap", Permissions: []contracts.Permission{contracts.AdminWildcard}, Authenticated: true}, nil
		}
	}
	if s.authMode == "development" {
		expected := sha256.Sum256([]byte(s.apiKey))
		provided := sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(expected[:], provided[:]) != 1 {
			return contracts.Principal{}, store.ErrUnauthenticated
		}
		workspaceID := contracts.DefaultWorkspaceID
		if candidates := requestWorkspaceCandidates(r); len(candidates) > 0 {
			workspaceID = candidates[0]
		}
		return contracts.Principal{ID: "development", WorkspaceID: workspaceID, Subject: "development", Kind: "development", DisplayName: "development", Permissions: []contracts.Permission{contracts.AdminWildcard}, Authenticated: true, Development: true}, nil
	}
	if s.auth == nil {
		return contracts.Principal{}, store.ErrUnauthenticated
	}
	return s.auth.Authenticate(r.Context(), token)
}

func isPublicPath(path string) bool {
	return path == "/healthz" || path == "/readyz" || path == "/v1/health"
}

func (s *server) securityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		principal, err := s.authenticateRequest(r)
		if err != nil {
			if errors.Is(err, store.ErrUnauthenticated) {
				writeErr(w, http.StatusUnauthorized, "unauthorised")
				return
			}
			writeErr(w, http.StatusServiceUnavailable, "authentication unavailable")
			return
		}
		r = withPrincipal(r, principal)
		if err := validateRequestWorkspace(r, principal); err != nil {
			writeErr(w, http.StatusForbidden, "workspace access denied")
			return
		}
		if s.auth == nil {
			writeErr(w, http.StatusServiceUnavailable, "authorization unavailable")
			return
		}
		permission := permissionForRequest(r)
		decision, err := s.auth.Authorize(r.Context(), principal, requestIDFromRequest(r), permission, r.URL.Path, r.Method, r.URL.Path)
		if errors.Is(err, store.ErrAuthorizationDenied) {
			writeErr(w, http.StatusForbidden, "forbidden")
			return
		}
		if err != nil {
			writeErr(w, http.StatusServiceUnavailable, "authorization unavailable")
			return
		}
		if !decision.Allowed {
			writeErr(w, http.StatusForbidden, "forbidden")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func requestWorkspaceCandidates(r *http.Request) []string {
	if r == nil {
		return nil
	}
	candidates := make([]string, 0, 3)
	if value := strings.TrimSpace(r.Header.Get("X-Workspace-ID")); value != "" {
		candidates = append(candidates, value)
	}
	if value := strings.TrimSpace(r.URL.Query().Get("workspace_id")); value != "" {
		candidates = append(candidates, value)
	}
	if bodyWorkspace := peekBodyWorkspace(r); bodyWorkspace != "" {
		candidates = append(candidates, bodyWorkspace)
	}
	return candidates
}

func peekBodyWorkspace(r *http.Request) string {
	if r == nil || r.Body == nil || (r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodPatch) {
		return ""
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		r.Body = io.NopCloser(bytes.NewReader(nil))
		return ""
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var envelope struct {
		WorkspaceID string `json:"workspace_id"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return ""
	}
	return strings.TrimSpace(envelope.WorkspaceID)
}

func validateRequestWorkspace(r *http.Request, principal contracts.Principal) error {
	candidates := requestWorkspaceCandidates(r)
	if len(candidates) == 0 || principal.Development {
		for _, candidate := range candidates {
			if candidate != candidates[0] {
				return store.ErrWorkspaceViolation
			}
		}
		return nil
	}
	for _, candidate := range candidates {
		if candidate != candidates[0] || candidate != principal.WorkspaceID {
			return store.ErrWorkspaceViolation
		}
	}
	return nil
}

func permissionForRequest(r *http.Request) contracts.Permission {
	path := r.URL.Path
	switch {
	case path == "/v1/operator/workspaces/bootstrap":
		return contracts.PermissionIdentityAdmin
	case strings.HasPrefix(path, "/v1/operator/api-keys") || strings.HasPrefix(path, "/v1/operator/identities") || strings.HasPrefix(path, "/v1/operator/roles"):
		return contracts.PermissionIdentityAdmin
	case strings.HasPrefix(path, "/v1/operator/workspaces"):
		if r.Method == http.MethodGet {
			return contracts.PermissionWorkspaceRead
		}
		return contracts.PermissionWorkspaceWrite
	case strings.HasPrefix(path, "/v1/operator/ingests"):
		if r.Method == http.MethodGet {
			return contracts.PermissionRetrievalRead
		}
		return contracts.PermissionRetrievalWrite
	case strings.HasPrefix(path, "/v1/operator/ingest"):
		if r.Method == http.MethodGet || strings.HasSuffix(path, "/dry-run") {
			return contracts.PermissionRetrievalRead
		}
		return contracts.PermissionRetrievalWrite
	case path == "/v1/model/complete":
		return contracts.PermissionModelInvoke
	case path == "/v1/tools/execute":
		return contracts.PermissionToolExecute
	case strings.HasPrefix(path, "/v1/tools/approvals/"):
		return contracts.PermissionToolApprove
	case strings.HasPrefix(path, "/v1/agent/run"):
		if r.Method == http.MethodGet || strings.HasSuffix(path, "/replay") {
			return contracts.PermissionAgentRead
		}
		return contracts.PermissionAgentRun
	case path == "/v1/retrieve" || path == "/v1/rag" || path == "/v1/memo/search" || path == "/v1/symbol/search" || path == "/v1/router/recommend":
		return contracts.PermissionRetrievalRead
	case path == "/v1/evaluations/retrieval/surfaces":
		if r.Method == http.MethodGet {
			return contracts.PermissionEvaluationRead
		}
		return contracts.PermissionEvaluationWrite
	case path == "/v1/evaluations/retrieval/runs":
		return contracts.PermissionEvaluationRun
	case path == "/v1/evaluations/datasets":
		return contracts.PermissionEvaluationWrite
	case strings.HasPrefix(path, "/v1/evaluations/runs/"):
		return contracts.PermissionEvaluationRead
	case path == "/v1/evidence/disclose" || path == "/v1/evidence/provenance":
		return contracts.PermissionEvidenceRead
	case path == "/v1/evidence" || path == "/v1/evidence/edge":
		return contracts.PermissionEvidenceWrite
	case path == "/v1/artifacts/disclose" || path == "/v1/artifacts/provenance":
		return contracts.PermissionEvidenceRead
	case path == "/v1/artifacts/backfill" || path == "/v1/artifacts/retention" || path == "/v1/artifacts/integrity" || path == "/v1/artifacts/metrics":
		return contracts.PermissionWorkspaceWrite
	case path == "/v1/observability/metrics" || path == "/v1/metrics":
		return contracts.PermissionWorkspaceRead
	case path == "/v1/work-receipts/disclose" || strings.HasPrefix(path, "/v1/work-receipts/") && r.Method == http.MethodGet:
		return contracts.PermissionReceiptRead
	case path == "/v1/work-receipts":
		if r.Method == http.MethodGet {
			return contracts.PermissionReceiptRead
		}
		return contracts.PermissionReceiptWrite
	case path == "/v1/changes/dry-run" || path == "/v1/changes":
		return contracts.PermissionChangePropose
	case path == "/v1/changes/disclose":
		return contracts.PermissionChangeDisclose
	case strings.HasPrefix(path, "/v1/changes/"):
		if strings.HasSuffix(path, "/approve") {
			return contracts.PermissionChangeApprove
		}
		if strings.HasSuffix(path, "/apply") {
			return contracts.PermissionChangeApply
		}
		if r.Method == http.MethodGet {
			return contracts.PermissionChangeRead
		}
		return contracts.PermissionChangeRead
	case path == "/v1/artifacts":
		return contracts.PermissionEvidenceWrite
	case path == "/v1/chunks":
		return contracts.PermissionRetrievalWrite
	case strings.HasPrefix(path, "/v1/task"):
		if path == "/v1/tasks" && r.Method == http.MethodGet {
			return contracts.PermissionTaskRead
		}
		if strings.HasSuffix(path, "/claim") || strings.HasSuffix(path, "/complete") || strings.HasSuffix(path, "/renew") || strings.HasSuffix(path, "/fail") || strings.HasSuffix(path, "/cancel") {
			return contracts.PermissionTaskExecute
		}
		if r.Method == http.MethodGet {
			return contracts.PermissionTaskRead
		}
		return contracts.PermissionTaskMutate
	case strings.HasPrefix(path, "/v1/memo") || strings.HasPrefix(path, "/v1/symbol"):
		if r.Method == http.MethodGet {
			return contracts.PermissionRetrievalRead
		}
		return contracts.PermissionRetrievalWrite
	case strings.HasPrefix(path, "/v1/session") || strings.HasPrefix(path, "/v1/coord"):
		if r.Method == http.MethodGet {
			return contracts.PermissionWorkspaceRead
		}
		return contracts.PermissionWorkspaceWrite
	case strings.HasPrefix(path, "/v1/scheduler"):
		return contracts.PermissionSchedulerRun
	default:
		return contracts.PermissionWorkspaceRead
	}
}
