package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
)

var (
	ErrModelCallConflict = errors.New("model call idempotency key reused with a different request")
	ErrModelCallMissing  = errors.New("model call not found")
)

// ModelCallStore is the Postgres implementation of model.CallRecorder. The
// import is intentionally one-way: store implements the runtime interface but
// the provider implementations do not know about SQL.
type ModelCallStore struct {
	pool          *pgxpool.Pool
	artifacts     *ArtifactStore
	observability *ObservabilityStore
}

func (s *ModelCallStore) SetObservability(observer *ObservabilityStore) {
	if s != nil {
		s.observability = observer
	}
}

func NewModelCallStore(pool *pgxpool.Pool) *ModelCallStore {
	return &ModelCallStore{pool: pool, artifacts: NewArtifactStore(pool)}
}

// SetArtifactFailureHook is a deterministic test seam for proving that model
// completion and its artifact reference commit or roll back together.
func (s *ModelCallStore) SetArtifactFailureHook(hook func(string) error) {
	if s != nil && s.artifacts != nil {
		s.artifacts.SetFailureHook(hook)
	}
}

func (s *ModelCallStore) Start(ctx context.Context, request contracts.ModelRequest, requestEvidence []byte) (model.CallStart, error) {
	if s == nil || s.pool == nil {
		return model.CallStart{}, fmt.Errorf("model call store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return model.CallStart{}, fmt.Errorf("normalize model call: %w", err)
	}
	requestHash, err := request.RequestHash()
	if err != nil {
		return model.CallStart{}, err
	}
	requestEvidence = validEvidence(model.RedactBytes(requestEvidence))
	if len(requestEvidence) == 0 {
		return model.CallStart{}, fmt.Errorf("model request evidence must be valid JSON")
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return model.CallStart{}, err
	}
	taskJSON, err := jsonOrEmpty(request.Task)
	if err != nil {
		return model.CallStart{}, err
	}
	sessionJSON, err := jsonOrEmpty(request.Session)
	if err != nil {
		return model.CallStart{}, err
	}
	metadataJSON, err := json.Marshal(request.Metadata)
	if err != nil {
		return model.CallStart{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.CallStart{}, fmt.Errorf("begin model call start: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.model_calls(
			workspace_id, request_id, idempotency_key, request_hash, schema_version,
			causation_id, correlation_id, provider, endpoint, model, metadata,
			actor, task_ref, session_ref, status,
			request_evidence
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,$13::jsonb,$14::jsonb,$15,$16::jsonb)
		ON CONFLICT (workspace_id, idempotency_key) DO NOTHING`,
		request.WorkspaceID, request.RequestID, request.IdempotencyKey,
		requestHash, request.SchemaVersion, request.CausationID, request.CorrelationID,
		request.Provider.Provider, request.Provider.Endpoint, request.Provider.Model,
		metadataJSON, actorJSON, taskJSON, sessionJSON,
		contracts.ModelCallRunning, requestEvidence)
	if err != nil {
		return model.CallStart{}, fmt.Errorf("reserve model call: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		record, readErr := readModelCallTx(ctx, tx, request.WorkspaceID, request.IdempotencyKey)
		if readErr != nil {
			return model.CallStart{}, readErr
		}
		if record.RequestHash != requestHash {
			return model.CallStart{}, fmt.Errorf("%w: %s", ErrModelCallConflict, request.IdempotencyKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return model.CallStart{}, fmt.Errorf("commit duplicate model call read: %w", err)
		}
		return model.CallStart{Record: record, Existing: true}, nil
	}
	record, err := readModelCallTx(ctx, tx, request.WorkspaceID, request.IdempotencyKey)
	if err != nil {
		return model.CallStart{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.CallStart{}, fmt.Errorf("commit model call start: %w", err)
	}
	return model.CallStart{Record: record}, nil
}

func (s *ModelCallStore) Attempt(ctx context.Context, workspaceID, requestID string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("model call store is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	requestID = strings.TrimSpace(requestID)
	if workspaceID == "" || requestID == "" {
		return fmt.Errorf("workspace_id and request_id are required")
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE fornix.model_calls
		SET attempt_count=attempt_count+1, started_at=COALESCE(started_at, now())
		WHERE workspace_id=$1 AND request_id=$2 AND status='running'`, workspaceID, requestID)
	if err != nil {
		return fmt.Errorf("record model call attempt: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("record model call attempt: %w", ErrModelCallMissing)
	}
	return nil
}

func (s *ModelCallStore) Finish(ctx context.Context, result contracts.ModelCallResult) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("model call store is not configured")
	}
	result.RequestID = strings.TrimSpace(result.RequestID)
	result.WorkspaceID = strings.TrimSpace(result.WorkspaceID)
	if result.RequestID == "" || result.WorkspaceID == "" {
		return fmt.Errorf("workspace_id and request_id are required")
	}
	if result.Status != contracts.ModelCallSucceeded && result.Status != contracts.ModelCallFailed {
		return fmt.Errorf("invalid terminal model call status %q", result.Status)
	}
	usageJSON, err := json.Marshal(result.Usage)
	if err != nil {
		return err
	}
	costJSON, err := json.Marshal(result.Cost)
	if err != nil {
		return err
	}
	var responseJSON []byte
	if result.Response != nil {
		responseJSON, err = json.Marshal(result.Response)
		if err != nil {
			return err
		}
	}
	var failureJSON []byte
	if result.Failure != nil {
		failureJSON, err = json.Marshal(result.Failure)
		if err != nil {
			return err
		}
	}
	responseEvidence := validEvidence(model.RedactBytes(result.ResponseEvidence))
	if len(responseEvidence) == 0 {
		responseEvidence = []byte(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin model call finish: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var callID int64
	var currentStatus, provider, modelName string
	var actorJSON, taskJSON, sessionJSON []byte
	var startedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT id,status,provider,model,actor,task_ref,session_ref,started_at FROM fornix.model_calls WHERE workspace_id=$1 AND request_id=$2 FOR UPDATE`, result.WorkspaceID, result.RequestID).Scan(&callID, &currentStatus, &provider, &modelName, &actorJSON, &taskJSON, &sessionJSON, &startedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrModelCallMissing
	}
	if err != nil {
		return fmt.Errorf("lock model call before finish: %w", err)
	}
	if currentStatus != contracts.ModelCallRunning {
		if currentStatus == result.Status {
			return nil
		}
		return fmt.Errorf("model call is already terminal with status %q", currentStatus)
	}
	var actor contracts.ActorRef
	if len(actorJSON) > 0 && string(actorJSON) != "null" {
		if err := json.Unmarshal(actorJSON, &actor); err != nil {
			return fmt.Errorf("decode model actor: %w", err)
		}
	}
	task, err := decodeEntityRef(taskJSON)
	if err != nil {
		return fmt.Errorf("decode model task: %w", err)
	}
	session, err := decodeEntityRef(sessionJSON)
	if err != nil {
		return fmt.Errorf("decode model session: %w", err)
	}
	var responseArtifactID any
	if result.Status == contracts.ModelCallSucceeded && s.artifacts != nil {
		artifact, artifactErr := s.artifacts.PutTx(ctx, tx, ArtifactPutInput{
			WorkspaceID: result.WorkspaceID, Kind: "model-response-evidence", MediaType: "application/json",
			Raw: responseEvidence, Manifest: contracts.ArtifactManifest{
				Gist:     "redacted model response evidence",
				Metadata: map[string]string{"provider": provider, "model": modelName, "model_call_id": fmt.Sprintf("%d", callID)},
			}, SourceKind: "model_call", SourceID: result.RequestID, Role: "response",
			IdempotencyKey: "model-response:" + result.RequestID, Actor: actor,
		})
		if artifactErr != nil {
			return fmt.Errorf("store model response artifact: %w", artifactErr)
		}
		responseArtifactID = artifact.Artifact.ID
	}
	resultTag := result.Status
	updated, err := tx.Exec(ctx, `
		UPDATE fornix.model_calls
		SET status=$1,
		    attempt_count=GREATEST(attempt_count,$2),
		    content_emitted=$3,
		    provider_request_id=$4,
		    usage=$5::jsonb,
		    cost=$6::jsonb,
		    failure=$7::jsonb,
		    response=$8::jsonb,
		    response_evidence=$9::jsonb,
		    response_artifact_id=$10,
		    duration_ms=GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (now() - started_at)) * 1000)::bigint),
		    finished_at=now()
		WHERE workspace_id=$11 AND request_id=$12 AND status='running'`,
		resultTag, result.AttemptCount, result.ContentEmitted,
		strings.TrimSpace(result.ProviderRequestID), usageJSON, costJSON,
		nullJSON(failureJSON), nullJSON(responseJSON), responseEvidence,
		responseArtifactID, result.WorkspaceID, result.RequestID)
	if err != nil {
		return fmt.Errorf("finish model call: %w", err)
	}
	if updated.RowsAffected() == 1 {
		if s.observability != nil {
			var durationMS int64
			if err := tx.QueryRow(ctx, `SELECT duration_ms FROM fornix.model_calls WHERE workspace_id=$1 AND request_id=$2`, result.WorkspaceID, result.RequestID).Scan(&durationMS); err != nil {
				return fmt.Errorf("read model call duration: %w", err)
			}
			finishedAt := time.Now().UTC()
			started := finishedAt.Add(-time.Duration(durationMS) * time.Millisecond)
			if startedAt != nil {
				started = *startedAt
			}
			observation := contracts.RunObservation{WorkspaceID: result.WorkspaceID, IdempotencyKey: "model-observation:" + result.RequestID, Kind: contracts.ObservationModel, Component: "model_gateway", Operation: "complete", Outcome: result.Status, Actor: actor, Task: task, Session: session, SourceKind: "model_call", SourceID: result.RequestID, StartedAt: started, FinishedAt: finishedAt, DurationMS: durationMS, InputTokens: result.Usage.InputTokens, OutputTokens: result.Usage.OutputTokens, TotalTokens: result.Usage.TotalTokens, UsageMeasured: strings.TrimSpace(result.Usage.Source) != "", UsageEstimated: strings.TrimSpace(result.Usage.Source) == "" && result.Usage.TotalTokens > 0, CostUSD: result.Cost.TotalCostUSD, CostKnown: strings.TrimSpace(result.Cost.Source) != "" || result.Cost.TotalCostUSD > 0, RetryCount: maxInt(result.AttemptCount-1, 0), Evidence: responseEvidence}
			if len(observation.Evidence) > contracts.MaxObservationEvidence {
				observation.Evidence = []byte(`{"redacted":true,"truncated":true}`)
			}
			if err := s.observability.recordObservationTx(ctx, tx, observation); err != nil {
				return err
			}
			if err := s.observability.recordCostTx(ctx, tx, contracts.CostLedgerEntry{WorkspaceID: result.WorkspaceID, IdempotencyKey: "model-cost:" + result.RequestID, Category: contracts.CostModel, Basis: "provider_usage", SourceKind: "model_call", SourceID: result.RequestID, Actor: actor, Task: task, Session: session, Units: float64(result.Usage.TotalTokens), AmountUSD: result.Cost.TotalCostUSD, AmountKnown: strings.TrimSpace(result.Cost.Source) != "" || result.Cost.TotalCostUSD > 0, Measured: strings.TrimSpace(result.Usage.Source) != "", Estimated: strings.TrimSpace(result.Usage.Source) == "" && result.Usage.TotalTokens > 0, InputTokens: result.Usage.InputTokens, OutputTokens: result.Usage.OutputTokens, DurationMS: durationMS}); err != nil {
				return err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit model call finish: %w", err)
		}
		return nil
	}
	return fmt.Errorf("model call finish did not update a running record")
}

func (s *ModelCallStore) Get(ctx context.Context, workspaceID, idempotencyKey string) (contracts.ModelCallRecord, error) {
	if s == nil || s.pool == nil {
		return contracts.ModelCallRecord{}, fmt.Errorf("model call store is not configured")
	}
	record, err := readModelCall(ctx, s.pool, workspaceID, idempotencyKey)
	if err != nil {
		return contracts.ModelCallRecord{}, err
	}
	return record, nil
}

func readModelCall(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, idempotencyKey string) (contracts.ModelCallRecord, error) {
	return readModelCallWithQuery(ctx, queryer, workspaceID, idempotencyKey)
}

func readModelCallTx(ctx context.Context, tx pgx.Tx, workspaceID, idempotencyKey string) (contracts.ModelCallRecord, error) {
	return readModelCallWithQuery(ctx, tx, workspaceID, idempotencyKey)
}

func readModelCallWithQuery(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, idempotencyKey string) (contracts.ModelCallRecord, error) {
	var record contracts.ModelCallRecord
	var metadataJSON, actorJSON, taskJSON, sessionJSON, usageJSON, costJSON, failureJSON, responseJSON, requestEvidence, responseEvidence []byte
	var responseArtifactID *int64
	var startedAt, finishedAt *time.Time
	var provider, endpoint, modelName, status, providerRequestID string
	var causationID, correlationID string
	var attemptCount int
	var contentEmitted bool
	var durationMS int64
	err := queryer.QueryRow(ctx, `
		SELECT id, workspace_id, request_id, idempotency_key, request_hash,
		       schema_version, causation_id, correlation_id,
		       provider, endpoint, model, metadata, actor, task_ref, session_ref, status,
		       attempt_count, content_emitted, provider_request_id, usage, cost,
		       failure, response, request_evidence, response_evidence, response_artifact_id,
		       created_at, started_at, finished_at, duration_ms
		FROM fornix.model_calls
		WHERE workspace_id=$1 AND idempotency_key=$2
		FOR UPDATE`, workspaceID, idempotencyKey).Scan(
		&record.ID, &record.WorkspaceID, &record.RequestID, &record.IdempotencyKey, &record.RequestHash,
		&record.SchemaVersion, &causationID, &correlationID,
		&provider, &endpoint, &modelName, &metadataJSON, &actorJSON, &taskJSON, &sessionJSON, &status,
		&attemptCount, &contentEmitted, &providerRequestID, &usageJSON, &costJSON,
		&failureJSON, &responseJSON, &requestEvidence, &responseEvidence, &responseArtifactID,
		&record.CreatedAt, &startedAt, &finishedAt, &durationMS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.ModelCallRecord{}, ErrModelCallMissing
		}
		return contracts.ModelCallRecord{}, fmt.Errorf("read model call: %w", err)
	}
	record.Provider = contracts.ProviderRef{Provider: provider, Endpoint: endpoint, Model: modelName}
	record.CausationID = causationID
	record.CorrelationID = correlationID
	if err := json.Unmarshal(metadataJSON, &record.Metadata); err != nil {
		return contracts.ModelCallRecord{}, fmt.Errorf("decode model metadata: %w", err)
	}
	record.Status = status
	record.AttemptCount = attemptCount
	record.ContentEmitted = contentEmitted
	record.ProviderRequestID = providerRequestID
	record.RequestEvidence = append([]byte(nil), requestEvidence...)
	record.ResponseEvidence = append([]byte(nil), responseEvidence...)
	if responseArtifactID != nil && *responseArtifactID > 0 {
		artifact, err := readArtifact(ctx, queryer, record.WorkspaceID, *responseArtifactID, false)
		if err != nil {
			return contracts.ModelCallRecord{}, fmt.Errorf("read model response artifact: %w", err)
		}
		ref, refErr := readArtifactRef(ctx, queryer, `WHERE r.workspace_id=$1 AND r.artifact_id=$2 AND r.source_kind='model_call' AND r.source_id=$3 AND r.role='response'`, record.WorkspaceID, artifact.ID, record.RequestID)
		if refErr != nil && !errors.Is(refErr, ErrArtifactNotFound) {
			return contracts.ModelCallRecord{}, fmt.Errorf("read model response artifact reference: %w", refErr)
		}
		ref.SchemaVersion = contracts.ArtifactSchemaVersion
		ref.ArtifactID = artifact.ID
		ref.WorkspaceID = artifact.WorkspaceID
		ref.ContentHash = artifact.ContentHash
		ref.MediaType = artifact.MediaType
		ref.ByteSize = artifact.ByteSize
		record.ResponseArtifact = &ref
	}
	if err := json.Unmarshal(actorJSON, &record.Actor); err != nil {
		return contracts.ModelCallRecord{}, fmt.Errorf("decode model actor: %w", err)
	}
	record.Task, err = decodeEntityRef(taskJSON)
	if err != nil {
		return contracts.ModelCallRecord{}, err
	}
	record.Session, err = decodeEntityRef(sessionJSON)
	if err != nil {
		return contracts.ModelCallRecord{}, err
	}
	if err := json.Unmarshal(usageJSON, &record.Usage); err != nil {
		return contracts.ModelCallRecord{}, fmt.Errorf("decode model usage: %w", err)
	}
	if err := json.Unmarshal(costJSON, &record.Cost); err != nil {
		return contracts.ModelCallRecord{}, fmt.Errorf("decode model cost: %w", err)
	}
	if len(failureJSON) > 0 && string(failureJSON) != "null" {
		record.Failure = &contracts.ModelFailure{}
		if err := json.Unmarshal(failureJSON, record.Failure); err != nil {
			return contracts.ModelCallRecord{}, fmt.Errorf("decode model failure: %w", err)
		}
	}
	if len(responseJSON) > 0 && string(responseJSON) != "null" {
		record.Response = &contracts.ModelResponse{}
		if err := json.Unmarshal(responseJSON, record.Response); err != nil {
			return contracts.ModelCallRecord{}, fmt.Errorf("decode model response: %w", err)
		}
	}
	record.StartedAt = startedAt
	record.FinishedAt = finishedAt
	record.DurationMS = durationMS
	return record, nil
}

func validEvidence(value []byte) []byte {
	if len(value) == 0 || len(value) > contracts.MaxModelEvidenceBytes || !json.Valid(value) {
		return nil
	}
	return append([]byte(nil), value...)
}

func nullJSON(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func jsonOrEmpty(value any) ([]byte, error) {
	if value == nil {
		return []byte(`{}`), nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func decodeEntityRef(value []byte) (*contracts.EntityRef, error) {
	if len(value) == 0 || string(value) == "null" || string(value) == "{}" {
		return nil, nil
	}
	var ref contracts.EntityRef
	if err := json.Unmarshal(value, &ref); err != nil {
		return nil, fmt.Errorf("decode entity reference: %w", err)
	}
	return &ref, nil
}
