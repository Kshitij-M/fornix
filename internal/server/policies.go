package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// handlePolicies owns the policy authority's HTTP surface. Policy bodies are
// declarative and immutable; only lifecycle bindings are mutable, and every
// mutation is delegated to the transactional PolicyStore.
func (s *server) handlePolicies(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/policies/"), "/")
	if path == "" {
		writeErr(w, http.StatusNotFound, "policy id and version required")
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) != 2 && len(parts) != 3 {
		writeErr(w, http.StatusNotFound, "unknown policy operation")
		return
	}
	policyID, err := url.PathUnescape(parts[0])
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid policy id")
		return
	}
	version, err := url.PathUnescape(parts[1])
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid policy version")
		return
	}
	if len(parts) == 2 {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		s.handlePolicyGet(w, r, policyID, version)
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	s.handlePolicyLifecycle(w, r, policyID, version, parts[2])
}

func (s *server) handlePolicyCreate(w http.ResponseWriter, r *http.Request) {
	var input contracts.PolicyCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writePolicyError(w, http.StatusBadRequest, fmt.Errorf("invalid policy create request: %w", err))
		return
	}
	input.WorkspaceID = requestWorkspace(r, input.WorkspaceID)
	input.Actor = requestActor(r)
	input.RequestID = requestIDFromRequest(r)
	if input.IdempotencyKey == "" {
		input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if input.IdempotencyKey == "" {
		writePolicyError(w, http.StatusBadRequest, errors.New("idempotency_key is required"))
		return
	}
	ctx, cancel := contextWithPolicyTimeout(r)
	defer cancel()
	version, created, err := s.policies.Create(ctx, input)
	if err != nil {
		writePolicyError(w, policyHTTPStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"policy": version, "created": created, "deduped": !created})
}

func (s *server) handlePolicyList(w http.ResponseWriter, r *http.Request) {
	limit, err := policyLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writePolicyError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := contextWithPolicyTimeout(r)
	defer cancel()
	page, err := s.policies.List(ctx, requestWorkspace(r, r.URL.Query().Get("workspace_id")), limit, r.URL.Query().Get("cursor"))
	if err != nil {
		writePolicyError(w, policyHTTPStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *server) handlePolicyGet(w http.ResponseWriter, r *http.Request, policyID, version string) {
	ctx, cancel := contextWithPolicyTimeout(r)
	defer cancel()
	policy, err := s.policies.Get(ctx, requestWorkspace(r, r.URL.Query().Get("workspace_id")), policyID, version)
	if err != nil {
		writePolicyError(w, policyHTTPStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *server) handlePolicyLifecycle(w http.ResponseWriter, r *http.Request, policyID, version, operation string) {
	if operation != "activate" && operation != "default" && operation != "retire" {
		writeErr(w, http.StatusNotFound, "unknown policy operation")
		return
	}
	var input struct {
		WorkspaceID    string `json:"workspace_id,omitempty"`
		PolicyHash     string `json:"policy_hash,omitempty"`
		Reason         string `json:"reason,omitempty"`
		IdempotencyKey string `json:"idempotency_key,omitempty"`
	}
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			writePolicyError(w, http.StatusBadRequest, fmt.Errorf("invalid policy lifecycle request: %w", err))
			return
		}
	}
	workspaceID := requestWorkspace(r, input.WorkspaceID)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.IdempotencyKey == "" {
		input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if input.IdempotencyKey == "" {
		input.IdempotencyKey = operation + ":" + workspaceID + ":" + policyID + ":" + version
	}
	request := contracts.PolicyLifecycleRequest{
		RequestID: requestIDFromRequest(r), IdempotencyKey: input.IdempotencyKey,
		WorkspaceID: workspaceID, Actor: requestActor(r), Reason: input.Reason,
		Policy: contracts.ValidationPolicyRef{WorkspaceID: workspaceID, PolicyID: policyID, Version: version, PolicyHash: input.PolicyHash},
	}
	ctx, cancel := contextWithPolicyTimeout(r)
	defer cancel()
	var (
		stored  contracts.ValidationPolicyVersion
		deduped bool
		err     error
	)
	switch operation {
	case "activate":
		stored, deduped, err = s.policies.Activate(ctx, request)
	case "default":
		stored, deduped, err = s.policies.SetDefault(ctx, request)
	case "retire":
		stored, deduped, err = s.policies.Retire(ctx, request)
	}
	if err != nil {
		writePolicyError(w, policyHTTPStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"policy": stored, "created": !deduped, "deduped": deduped})
}

func (s *server) handlePolicyResolve(w http.ResponseWriter, r *http.Request, dryRun bool) {
	var input contracts.PolicyEvaluationRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writePolicyError(w, http.StatusBadRequest, fmt.Errorf("invalid policy resolution request: %w", err))
		return
	}
	input.WorkspaceID = requestWorkspace(r, input.WorkspaceID)
	input.Actor = requestActor(r)
	input.RequestID = requestIDFromRequest(r)
	if input.IdempotencyKey == "" {
		input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	ctx, cancel := contextWithPolicyTimeout(r)
	defer cancel()
	var (
		resolution contracts.PolicyResolution
		err        error
	)
	if dryRun {
		resolution, err = s.policies.DryRunResolve(ctx, input)
	} else {
		resolution, err = s.policies.Resolve(ctx, input)
	}
	if err != nil {
		writePolicyError(w, policyHTTPStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"resolution": resolution, "dry_run": dryRun})
}

func (s *server) handlePolicyCompare(w http.ResponseWriter, r *http.Request) {
	var input contracts.PolicyCompareRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writePolicyError(w, http.StatusBadRequest, fmt.Errorf("invalid policy comparison request: %w", err))
		return
	}
	input.WorkspaceID = requestWorkspace(r, input.WorkspaceID)
	ctx, cancel := contextWithPolicyTimeout(r)
	defer cancel()
	comparison, err := s.policies.Compare(ctx, input)
	if err != nil {
		writePolicyError(w, policyHTTPStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, comparison)
}

func (s *server) handlePolicyAudit(w http.ResponseWriter, r *http.Request) {
	limit, err := policyLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writePolicyError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := contextWithPolicyTimeout(r)
	defer cancel()
	page, err := s.policies.Audit(ctx, requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("policy_id"), r.URL.Query().Get("version"), limit, r.URL.Query().Get("cursor"))
	if err != nil {
		writePolicyError(w, policyHTTPStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func contextWithPolicyTimeout(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 15*time.Second)
}

func policyLimit(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return 50, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, errors.New("limit must be a positive integer")
	}
	if value > contracts.MaxPolicyPageSize {
		value = contracts.MaxPolicyPageSize
	}
	return value, nil
}

func policyHTTPStatus(err error) int {
	switch {
	case errors.Is(err, store.ErrPolicyNotFound), errors.Is(err, store.ErrPolicyDefaultMissing):
		return http.StatusNotFound
	case errors.Is(err, store.ErrPolicyConflict), errors.Is(err, store.ErrPolicyImmutable), errors.Is(err, store.ErrPolicyRetired), errors.Is(err, store.ErrPolicyInvalidTransition):
		return http.StatusConflict
	case errors.Is(err, store.ErrWorkspaceViolation):
		return http.StatusForbidden
	default:
		return http.StatusUnprocessableEntity
	}
}

func writePolicyError(w http.ResponseWriter, status int, err error) {
	writeErr(w, status, shortError(err, 320))
}
