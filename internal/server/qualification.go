package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// qualificationSignerRequest deliberately contains only public trust
// metadata. Private signing keys are deployment-owned and never cross this
// API boundary.
type qualificationSignerRequest struct {
	WorkspaceID     string    `json:"workspace_id,omitempty"`
	DeploymentID    string    `json:"deployment_id"`
	KeyID           string    `json:"key_id"`
	PublicKey       string    `json:"public_key"`
	ValidFrom       time.Time `json:"valid_from"`
	ValidUntil      time.Time `json:"valid_until"`
	SupersedesKeyID string    `json:"supersedes_key_id,omitempty"`
}

func (s *server) handleQualificationSigners(w http.ResponseWriter, r *http.Request) {
	if s.qualificationTrust == nil {
		writeErr(w, http.StatusServiceUnavailable, "qualification trust is unavailable")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/qualification/signers")
	if path == "" || path == "/" {
		switch r.Method {
		case http.MethodGet:
			limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			page, err := s.qualificationTrust.ListSigners(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), limit, r.URL.Query().Get("cursor"))
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, page)
			return
		case http.MethodPost:
			var input qualificationSignerRequest
			if err := decodeQualificationJSON(r, &input); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid qualification signer request")
				return
			}
			workspaceID := requestWorkspace(r, input.WorkspaceID)
			signer, created, err := s.qualificationTrust.RegisterSigner(r.Context(), contracts.QualificationTrustedSignerInput{
				WorkspaceID: workspaceID, DeploymentID: input.DeploymentID, KeyID: input.KeyID,
				PublicKey: input.PublicKey, ValidFrom: input.ValidFrom, ValidUntil: input.ValidUntil,
				SupersedesKeyID: input.SupersedesKeyID, Actor: qualificationAuditActor(r),
			})
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"signer": signer, "created": created})
			return
		default:
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
			return
		}
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) != 2 || parts[1] != "revoke" || r.Method != http.MethodPost || strings.TrimSpace(parts[0]) == "" {
		writeErr(w, http.StatusNotFound, "unknown qualification signer operation")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	if err := s.qualificationTrust.RevokeSigner(r.Context(), workspaceID, r.URL.Query().Get("deployment_id"), parts[0], qualificationAuditActor(r)); err != nil {
		writeQualificationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deployment_id": strings.TrimSpace(r.URL.Query().Get("deployment_id")), "key_id": parts[0], "revoked": true})
}

// handleQualificationImport accepts an exact bounded signed bundle, verifies
// it against the deployment-owned trust catalog, and writes the accepted
// record transactionally. Dry-run validates without creating durable rows.
func (s *server) handleQualificationImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	if s.qualificationTrust == nil {
		writeErr(w, http.StatusServiceUnavailable, "qualification trust is unavailable")
		return
	}
	var envelope struct {
		WorkspaceID     string          `json:"workspace_id,omitempty"`
		DeploymentID    string          `json:"deployment_id"`
		TargetHash      string          `json:"target_hash"`
		SourceReference string          `json:"source_reference,omitempty"`
		RequestID       string          `json:"request_id,omitempty"`
		IdempotencyKey  string          `json:"idempotency_key,omitempty"`
		CausationID     string          `json:"causation_id,omitempty"`
		CorrelationID   string          `json:"correlation_id,omitempty"`
		SignedBundle    json.RawMessage `json:"signed_bundle"`
		DryRun          bool            `json:"dry_run,omitempty"`
	}
	if err := decodeQualificationJSON(r, &envelope); err != nil || len(bytes.TrimSpace(envelope.SignedBundle)) == 0 {
		writeErr(w, http.StatusBadRequest, "invalid qualification import request")
		return
	}
	var signed contracts.SignedQualificationBundle
	if err := decodeQualificationRaw(envelope.SignedBundle, &signed); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid signed qualification bundle")
		return
	}
	idempotencyKey := strings.TrimSpace(envelope.IdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if idempotencyKey == "" {
		idempotencyKey = requestIDFromRequest(r)
	}
	request := contracts.QualificationImportRequest{
		WorkspaceID: requestWorkspace(r, envelope.WorkspaceID), DeploymentID: envelope.DeploymentID,
		TargetHash: envelope.TargetHash, SourceReference: envelope.SourceReference,
		RequestID: envelope.RequestID, IdempotencyKey: idempotencyKey,
		CausationID: envelope.CausationID, CorrelationID: envelope.CorrelationID,
		SignedBundle: signed, SignedBytes: append([]byte(nil), envelope.SignedBundle...),
		Actor: qualificationAuditActor(r), DryRun: envelope.DryRun,
	}
	if err := s.qualificationAuthorityImportReady(r.Context(), request.WorkspaceID); err != nil {
		writeQualificationError(w, err)
		return
	}
	result, err := s.qualificationTrust.ImportAuthorized(r.Context(), request, time.Now().UTC())
	if err != nil {
		writeQualificationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleQualificationSnapshots publishes and discloses the bounded trust
// snapshot used to authorize new qualification imports. Publication is
// deployment administration; disclosure is hash-preserving and read-only.
func (s *server) handleQualificationSnapshots(w http.ResponseWriter, r *http.Request) {
	if s.qualificationTrust == nil {
		writeErr(w, http.StatusServiceUnavailable, "qualification trust is unavailable")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/qualification/snapshots")
	if path == "" || path == "/" {
		switch r.Method {
		case http.MethodGet:
			limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			page, err := s.qualificationTrust.ListSnapshots(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), limit, r.URL.Query().Get("cursor"))
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, page)
			return
		case http.MethodPost:
			var envelope struct {
				WorkspaceID     string          `json:"workspace_id,omitempty"`
				DeploymentID    string          `json:"deployment_id"`
				SourceReference string          `json:"source_reference,omitempty"`
				RequestID       string          `json:"request_id,omitempty"`
				IdempotencyKey  string          `json:"idempotency_key,omitempty"`
				CausationID     string          `json:"causation_id,omitempty"`
				CorrelationID   string          `json:"correlation_id,omitempty"`
				SignedSnapshot  json.RawMessage `json:"signed_snapshot"`
				DryRun          bool            `json:"dry_run,omitempty"`
			}
			if err := decodeQualificationJSON(r, &envelope); err != nil || len(bytes.TrimSpace(envelope.SignedSnapshot)) == 0 {
				writeErr(w, http.StatusBadRequest, "invalid qualification trust snapshot request")
				return
			}
			var snapshot contracts.QualificationTrustSnapshot
			if err := decodeQualificationRaw(envelope.SignedSnapshot, &snapshot); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid qualification trust snapshot")
				return
			}
			workspaceID := requestWorkspace(r, envelope.WorkspaceID)
			idempotencyKey := strings.TrimSpace(envelope.IdempotencyKey)
			if idempotencyKey == "" {
				idempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
			}
			if idempotencyKey == "" {
				idempotencyKey = requestIDFromRequest(r)
			}
			request := contracts.QualificationTrustSnapshotPublishRequest{
				WorkspaceID: workspaceID, DeploymentID: envelope.DeploymentID,
				SourceReference: envelope.SourceReference, RequestID: envelope.RequestID,
				IdempotencyKey: idempotencyKey, CausationID: envelope.CausationID,
				CorrelationID: envelope.CorrelationID, SignedSnapshot: snapshot,
				SignedBytes: append([]byte(nil), envelope.SignedSnapshot...),
				Actor:       qualificationAuditActor(r), DryRun: envelope.DryRun,
			}
			record, created, err := s.qualificationTrust.PublishSnapshot(r.Context(), request, time.Now().UTC())
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"record": record, "created": created})
			return
		default:
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
			return
		}
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 2 && parts[1] == "revoke" && r.Method == http.MethodPost && strings.TrimSpace(parts[0]) != "" {
		workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
		if err := s.qualificationTrust.RevokeSnapshot(r.Context(), workspaceID, r.URL.Query().Get("deployment_id"), parts[0], qualificationAuditActor(r)); err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"snapshot_id": parts[0], "revoked": true})
		return
	}
	if len(parts) != 1 || parts[0] == "" || r.Method != http.MethodGet {
		writeErr(w, http.StatusNotFound, "snapshot id required")
		return
	}
	disclosure, err := s.qualificationTrust.DiscloseSnapshot(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), parts[0])
	if err != nil {
		writeQualificationError(w, err)
		return
	}
	response := map[string]any{"record": disclosure.Record, "signed_snapshot": disclosure.SignedSnapshot}
	if r.URL.Query().Get("raw") == "true" {
		response["source_bytes"] = disclosure.SourceBytes
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *server) handleQualificationImports(w http.ResponseWriter, r *http.Request) {
	if s.qualificationTrust == nil {
		writeErr(w, http.StatusServiceUnavailable, "qualification trust is unavailable")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/qualification/imports")
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	deploymentID := r.URL.Query().Get("deployment_id")
	if path == "" || path == "/" {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		page, err := s.qualificationTrust.ListImports(r.Context(), workspaceID, deploymentID, limit, r.URL.Query().Get("cursor"))
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
		return
	}
	if r.Method != http.MethodGet || strings.Contains(strings.TrimPrefix(path, "/"), "/") || strings.TrimSpace(strings.TrimPrefix(path, "/")) == "" {
		writeErr(w, http.StatusNotFound, "import id required")
		return
	}
	disclosure, err := s.qualificationTrust.DiscloseImport(r.Context(), workspaceID, deploymentID, strings.TrimPrefix(path, "/"))
	if err != nil {
		writeQualificationError(w, err)
		return
	}
	response := map[string]any{"record": disclosure.Record, "signed_bundle": disclosure.SignedBundle}
	if r.URL.Query().Get("raw") == "true" {
		response["source_bytes"] = disclosure.SourceBytes
	}
	writeJSON(w, http.StatusOK, response)
}

func qualificationAuditActor(r *http.Request) contracts.AuditActor {
	actor := requestActor(r)
	return contracts.AuditActor{ID: actor.ID, WorkspaceID: actor.WorkspaceID, Kind: actor.Kind}
}

func qualificationPageLimit(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > contracts.MaxQualificationPageSize {
		return 0, fmt.Errorf("limit must be between 1 and %d", contracts.MaxQualificationPageSize)
	}
	return value, nil
}

func decodeQualificationJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

func decodeQualificationRaw(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

func writeQualificationError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, store.ErrQualificationSignerNotFound), errors.Is(err, store.ErrQualificationImportNotFound), errors.Is(err, store.ErrQualificationSnapshotNotFound), errors.Is(err, store.ErrDeploymentReleaseNotFound), errors.Is(err, store.ErrDeploymentEvidenceNotFound), errors.Is(err, store.ErrDeploymentVerificationNotFound), errors.Is(err, store.ErrReadinessSnapshotNotFound), errors.Is(err, store.ErrIncidentAnnotationNotFound), errors.Is(err, store.ErrReadinessPolicyNotFound), errors.Is(err, store.ErrQualificationRetentionPolicyNotFound), errors.Is(err, store.ErrQualificationRetentionMetadataNotFound), errors.Is(err, store.ErrQualificationRefreshNotFound), errors.Is(err, store.ErrQualificationScheduleNotFound), errors.Is(err, store.ErrQualificationScheduleAttempt):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrQualificationSignerConflict), errors.Is(err, store.ErrQualificationImportConflict), errors.Is(err, store.ErrQualificationImportCursor), errors.Is(err, store.ErrQualificationSignerCursor), errors.Is(err, store.ErrQualificationSnapshotConflict), errors.Is(err, store.ErrQualificationSnapshotDowngrade), errors.Is(err, store.ErrQualificationSnapshotCursor), errors.Is(err, store.ErrDeploymentReleaseConflict), errors.Is(err, store.ErrDeploymentEvidenceConflict), errors.Is(err, store.ErrDeploymentEvidenceLifecycle), errors.Is(err, store.ErrDeploymentQualificationCursor), errors.Is(err, store.ErrDeploymentVerificationConflict), errors.Is(err, store.ErrReadinessSnapshotConflict), errors.Is(err, store.ErrReadinessSnapshotCursor), errors.Is(err, store.ErrIncidentAnnotationConflict), errors.Is(err, store.ErrIncidentAnnotationCursor), errors.Is(err, store.ErrReadinessPolicyConflict), errors.Is(err, store.ErrReadinessPolicyCursor), errors.Is(err, store.ErrQualificationRetentionConflict), errors.Is(err, store.ErrQualificationRetentionCursor), errors.Is(err, store.ErrQualificationRefreshConflict), errors.Is(err, store.ErrQualificationRefreshCursor), errors.Is(err, store.ErrQualificationScheduleConflict), errors.Is(err, store.ErrQualificationScheduleCursor):
		status = http.StatusConflict
	case errors.Is(err, store.ErrQualificationSignerRevoked), errors.Is(err, store.ErrQualificationSignerSuperseded), errors.Is(err, store.ErrQualificationSignerNotYetValid), errors.Is(err, store.ErrQualificationSignerExpired), errors.Is(err, store.ErrQualificationImportStale), errors.Is(err, store.ErrQualificationSnapshotRevoked), errors.Is(err, store.ErrQualificationSnapshotExpired), errors.Is(err, store.ErrQualificationSnapshotSignerUntrusted), errors.Is(err, store.ErrDeploymentEvidenceStale), errors.Is(err, store.ErrQualificationRefreshStale), errors.Is(err, store.ErrDeploymentVerificationStale), errors.Is(err, store.ErrDeploymentAdmissionNotReady), errors.Is(err, store.ErrQualificationScheduleHeld), errors.Is(err, store.ErrQualificationScheduleOwned), errors.Is(err, store.ErrQualificationScheduleFenced), errors.Is(err, store.ErrQualificationScheduleExpired), errors.Is(err, store.ErrQualificationScheduleReleased), errors.Is(err, store.ErrQualificationScheduleNotDue), errors.Is(err, store.ErrQualificationScheduleTerminal), errors.Is(err, store.ErrQualificationScheduleRecovery):
		status = http.StatusForbidden
	default:
		status = http.StatusBadRequest
	}
	writeErr(w, status, shortError(err, 320))
}
