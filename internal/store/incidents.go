package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrIncidentNotFound         = errors.New("incident not found")
	ErrIncidentConflict         = errors.New("incident identity conflict")
	ErrIncidentIdempotency      = errors.New("incident idempotency conflict")
	ErrIncidentWorkspace        = errors.New("incident workspace violation")
	ErrIncidentApprovalNotFound = errors.New("incident approval not found")
	ErrIncidentApprovalConflict = errors.New("incident approval conflict")
)

// IncidentStore owns the durable adapter projection for the universal
// incident qualification workflow. It does not own workflow leases,
// connector execution, raw evidence bytes, or receipts.
type IncidentStore struct {
	pool     *pgxpool.Pool
	events   *EventStore
	evidence *EvidenceStore
}

type IncidentIngestResult struct {
	Incident  contracts.Incident      `json:"incident"`
	Evidence  contracts.SourceRecord  `json:"evidence"`
	Event     contracts.EventEnvelope `json:"event"`
	Duplicate bool                    `json:"duplicate"`
}

// IncidentApprovalRecord is the durable, hash-only approval identity. The
// decision is append-only; evidence remains the source of any disclosed
// operator-facing detail.
type IncidentApprovalRecord struct {
	Approval     contracts.IncidentApproval `json:"approval"`
	EvidenceID   int64                      `json:"evidence_id"`
	EvidenceHash string                     `json:"evidence_hash"`
	Duplicate    bool                       `json:"duplicate"`
}

// NewIncidentStore composes the incident projection with existing authorities.
func NewIncidentStore(pool *pgxpool.Pool, events *EventStore, evidence *EvidenceStore) *IncidentStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	if evidence == nil {
		evidence = NewEvidenceStore(pool)
	}
	return &IncidentStore{pool: pool, events: events, evidence: evidence}
}

// Ingest records one fake or pre-verified signed event. The event payload is
// persisted once as immutable evidence; a duplicate natural delivery returns
// the original incident without appending another authoritative event.
func (s *IncidentStore) Ingest(ctx context.Context, event contracts.IncidentEvent) (IncidentIngestResult, error) {
	if s == nil || s.pool == nil || s.events == nil || s.evidence == nil {
		return IncidentIngestResult{}, fmt.Errorf("incident store is not configured")
	}
	if strings.TrimSpace(event.IdempotencyKey) == "" {
		event.IdempotencyKey = "incident:" + event.NaturalIdentity() + ":" + contracts.ArtifactContentHash(event.Payload)
	}
	if err := event.Normalize(); err != nil {
		return IncidentIngestResult{}, err
	}
	eventHash := event.StableHash()
	if eventHash == "" {
		return IncidentIngestResult{}, fmt.Errorf("incident event hash is invalid")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, event.WorkspaceID)
	if err != nil {
		return IncidentIngestResult{}, fmt.Errorf("begin incident ingest: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var existingHash, existingID string
	err = tx.QueryRow(ctx, `SELECT request_hash, incident_id FROM fornix.incident_idempotency WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, event.WorkspaceID, event.IdempotencyKey).Scan(&existingHash, &existingID)
	if err == nil {
		if existingHash != eventHash {
			return IncidentIngestResult{}, ErrIncidentIdempotency
		}
		incident, getErr := readIncidentTx(ctx, tx, event.WorkspaceID, existingID)
		if getErr != nil {
			return IncidentIngestResult{}, getErr
		}
		if err := tx.Commit(ctx); err != nil {
			return IncidentIngestResult{}, fmt.Errorf("commit duplicate incident ingest: %w", err)
		}
		return IncidentIngestResult{Incident: incident, Duplicate: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return IncidentIngestResult{}, fmt.Errorf("read incident idempotency: %w", err)
	}

	evidenceResult, err := s.evidence.PutTx(ctx, tx, EvidencePutInput{
		WorkspaceID:      event.WorkspaceID,
		SourceReference:  "incident:" + event.SourceSystem + ":" + event.ExternalID,
		DeduplicationKey: "payload:" + event.PayloadHash,
		Kind:             "incident-event", MediaType: "application/json",
		Gist:       "bounded incident event " + event.SourceSystem,
		Detail:     "immutable incident payload; disclose through the evidence API",
		RawPayload: event.Payload, Actor: event.Actor,
		CausationID: event.CausationID, CorrelationID: event.CorrelationID,
	})
	if err != nil {
		return IncidentIngestResult{}, fmt.Errorf("preserve incident evidence: %w", err)
	}
	incidentID := "inc_" + event.NaturalIdentity()[:32]
	summaryHash := ""
	if event.Summary != "" {
		summaryHash = incidentHashString(event.Summary)
	}
	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.incident_records(
			workspace_id,id,source_system,external_id,severity,summary_hash,
			payload_hash,payload_evidence_id,status,event_hash,actor
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
		ON CONFLICT (workspace_id, source_system, external_id) DO NOTHING
		RETURNING id`, event.WorkspaceID, incidentID, event.SourceSystem, event.ExternalID,
		event.Severity, summaryHash, event.PayloadHash, evidenceResult.Record.ID,
		contracts.IncidentStatusOpen, eventHash, mustJSON(event.Actor)).Scan(&insertedID)
	created := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return IncidentIngestResult{}, fmt.Errorf("insert incident: %w", err)
	}
	if !created {
		incident, getErr := readIncidentByNaturalTx(ctx, tx, event.WorkspaceID, event.SourceSystem, event.ExternalID, true)
		if getErr != nil {
			return IncidentIngestResult{}, getErr
		}
		if incident.PayloadHash != event.PayloadHash || incident.EventHash != eventHash {
			return IncidentIngestResult{}, ErrIncidentConflict
		}
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.incident_idempotency(workspace_id,idempotency_key,request_hash,incident_id) VALUES($1,$2,$3,$4)`, event.WorkspaceID, event.IdempotencyKey, eventHash, incident.ID); err != nil {
			return IncidentIngestResult{}, fmt.Errorf("record duplicate incident idempotency: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.incident_event_deliveries(workspace_id,delivery_key,incident_id,event_hash,duplicate,actor,occurred_at) VALUES($1,$2,$3,$4,true,$5::jsonb,$6) ON CONFLICT DO NOTHING`, event.WorkspaceID, "delivery:"+event.IdempotencyKey, incident.ID, eventHash, mustJSON(event.Actor), event.OccurredAt); err != nil {
			return IncidentIngestResult{}, fmt.Errorf("record duplicate incident delivery: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return IncidentIngestResult{}, fmt.Errorf("commit duplicate incident: %w", err)
		}
		return IncidentIngestResult{Incident: incident, Evidence: evidenceResult.Record, Duplicate: true}, nil
	}

	incident, err := readIncidentTx(ctx, tx, event.WorkspaceID, insertedID)
	if err != nil {
		return IncidentIngestResult{}, err
	}
	payload, _ := json.Marshal(map[string]any{
		"incident_id": incident.ID, "source_system": incident.SourceSystem,
		"external_id": incident.ExternalID, "payload_hash": incident.PayloadHash,
		"evidence_id": evidenceResult.Record.ID, "event_hash": eventHash,
	})
	controlEvent := contracts.EventEnvelope{
		EventID: contracts.NewID("incident-event"), EventType: contracts.IncidentEventType,
		SchemaVersion: contracts.EventSchemaVersion, OccurredAt: event.OccurredAt,
		Scope: contracts.Scope{WorkspaceID: event.WorkspaceID, Subject: incident.ID},
		Actor: event.Actor, CausationID: event.CausationID, CorrelationID: event.CorrelationID,
		IdempotencyKey: "incident.received:" + incident.ID, Payload: payload,
		Provenance: contracts.Provenance{SourceArtifactRefs: []string{"evidence:" + strconv.FormatInt(evidenceResult.Record.ID, 10)}},
	}
	appended, err := s.events.AppendTx(ctx, tx, controlEvent)
	if err != nil {
		return IncidentIngestResult{}, fmt.Errorf("append incident event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.incident_idempotency(workspace_id,idempotency_key,request_hash,incident_id) VALUES($1,$2,$3,$4)`, event.WorkspaceID, event.IdempotencyKey, eventHash, incident.ID); err != nil {
		return IncidentIngestResult{}, fmt.Errorf("record incident idempotency: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.incident_event_deliveries(workspace_id,delivery_key,incident_id,event_hash,duplicate,actor,occurred_at) VALUES($1,$2,$3,$4,false,$5::jsonb,$6)`, event.WorkspaceID, "delivery:"+event.IdempotencyKey, incident.ID, eventHash, mustJSON(event.Actor), event.OccurredAt); err != nil {
		return IncidentIngestResult{}, fmt.Errorf("record incident delivery: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return IncidentIngestResult{}, fmt.Errorf("commit incident ingest: %w", err)
	}
	incident.EventHash = eventHash
	return IncidentIngestResult{Incident: incident, Evidence: evidenceResult.Record, Event: appended.Event}, nil
}

// Get reads one incident through both key dimensions to prevent accidental
// cross-workspace disclosure.
func (s *IncidentStore) Get(ctx context.Context, workspaceID, incidentID string) (contracts.Incident, error) {
	if s == nil || s.pool == nil {
		return contracts.Incident{}, fmt.Errorf("incident store is not configured")
	}
	workspaceID, incidentID = strings.TrimSpace(workspaceID), strings.TrimSpace(incidentID)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.Incident{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	value, err := readIncidentTx(ctx, tx, workspaceID, incidentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.Incident{}, ErrIncidentNotFound
	}
	if err != nil {
		return contracts.Incident{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.Incident{}, err
	}
	return value, nil
}

// LinkWorkflow updates only the current adapter projection. Workflow and
// transition history remain authoritative in WorkflowStore.
func (s *IncidentStore) LinkWorkflow(ctx context.Context, workspaceID, incidentID, workflowID, status string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("incident store is not configured")
	}
	if strings.TrimSpace(workflowID) == "" || strings.TrimSpace(incidentID) == "" {
		return fmt.Errorf("incident and workflow ids are required")
	}
	result, err := workspaceExec(ctx, s.pool, workspaceID, `UPDATE fornix.incident_records SET workflow_id=$3,status=$4,updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, workspaceID, incidentID, workflowID, status)
	if err != nil {
		return fmt.Errorf("link incident workflow: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrIncidentNotFound
	}
	return nil
}

// LinkReceipt adds the derived receipt identity without changing the event
// or workflow histories.
func (s *IncidentStore) LinkReceipt(ctx context.Context, workspaceID, incidentID, receiptID, status string) error {
	result, err := workspaceExec(ctx, s.pool, workspaceID, `UPDATE fornix.incident_records SET receipt_id=$3,status=$4,updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, workspaceID, incidentID, receiptID, status)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrIncidentNotFound
	}
	return nil
}

// RecordApproval appends one hash-only approval decision. The primary key is
// the waiting workflow step, so a retry cannot replace a prior decision. A
// matching retry returns the original record and can safely resume through
// WorkflowStore's idempotent transition command.
func (s *IncidentStore) RecordApproval(ctx context.Context, approval contracts.IncidentApproval, evidenceID int64, evidenceHash string) (IncidentApprovalRecord, error) {
	if s == nil || s.pool == nil {
		return IncidentApprovalRecord{}, fmt.Errorf("incident store is not configured")
	}
	if err := approval.Normalize(); err != nil {
		return IncidentApprovalRecord{}, err
	}
	evidenceHash = strings.ToLower(strings.TrimSpace(evidenceHash))
	if evidenceID <= 0 || !isEvidenceHash(evidenceHash) {
		return IncidentApprovalRecord{}, fmt.Errorf("approval evidence reference is invalid")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, approval.WorkspaceID)
	if err != nil {
		return IncidentApprovalRecord{}, fmt.Errorf("begin incident approval: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	actorJSON, _ := json.Marshal(approval.Actor)
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.incident_approvals(
			workspace_id,run_id,step_id,idempotency_key,operation_hash,plan_hash,
			decision,decision_hash,actor,evidence_id,evidence_hash,decided_at
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12)
		ON CONFLICT DO NOTHING`,
		approval.WorkspaceID, approval.RunID, approval.StepID, approval.IdempotencyKey,
		approval.OperationHash, approval.PlanHash, approval.Decision, approval.DecisionHash,
		actorJSON, evidenceID, evidenceHash, approval.DecidedAt)
	if err != nil {
		return IncidentApprovalRecord{}, fmt.Errorf("record incident approval: %w", err)
	}
	duplicate := inserted.RowsAffected() == 0
	var record IncidentApprovalRecord
	var actorRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT operation_hash,plan_hash,decision,decision_hash,actor,evidence_id,evidence_hash,decided_at,idempotency_key
		FROM fornix.incident_approvals
		WHERE workspace_id=$1 AND run_id=$2 AND step_id=$3 FOR UPDATE`,
		approval.WorkspaceID, approval.RunID, approval.StepID).Scan(
		&record.Approval.OperationHash, &record.Approval.PlanHash, &record.Approval.Decision,
		&record.Approval.DecisionHash, &actorRaw, &record.EvidenceID, &record.EvidenceHash,
		&record.Approval.DecidedAt, &record.Approval.IdempotencyKey)
	if errors.Is(err, pgx.ErrNoRows) {
		// The insert may have conflicted on the workspace-scoped idempotency
		// key for a different run/step. Do not leak a database constraint
		// error or treat that key reuse as a new approval.
		return IncidentApprovalRecord{}, ErrIncidentApprovalConflict
	}
	if err != nil {
		return IncidentApprovalRecord{}, fmt.Errorf("read incident approval: %w", err)
	}
	if err := json.Unmarshal(actorRaw, &record.Approval.Actor); err != nil {
		return IncidentApprovalRecord{}, fmt.Errorf("decode incident approval actor: %w", err)
	}
	record.Approval.SchemaVersion = contracts.IncidentSchemaVersion
	record.Approval.WorkspaceID, record.Approval.RunID, record.Approval.StepID = approval.WorkspaceID, approval.RunID, approval.StepID
	record.Approval.DecisionHash = strings.ToLower(record.Approval.DecisionHash)
	record.EvidenceHash = strings.ToLower(record.EvidenceHash)
	if record.Approval.IdempotencyKey != approval.IdempotencyKey || record.Approval.DecisionHash != approval.DecisionHash || record.EvidenceHash != evidenceHash {
		return IncidentApprovalRecord{}, fmt.Errorf("%w: approval decision does not match existing record", ErrIncidentApprovalConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return IncidentApprovalRecord{}, fmt.Errorf("commit incident approval: %w", err)
	}
	record.Duplicate = duplicate
	return record, nil
}

// FindApproval locates an approval by caller-supplied idempotency key. A
// missing record is not an error so a terminal workflow can fail closed unless
// the request is a true duplicate.
func (s *IncidentStore) FindApproval(ctx context.Context, workspaceID, runID, idempotencyKey string) (contracts.IncidentApproval, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.IncidentApproval{}, false, fmt.Errorf("incident store is not configured")
	}
	var approval contracts.IncidentApproval
	var actorRaw []byte
	workspaceID, runID, idempotencyKey = strings.TrimSpace(workspaceID), strings.TrimSpace(runID), strings.TrimSpace(idempotencyKey)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.IncidentApproval{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	err = tx.QueryRow(ctx, `
		SELECT step_id,operation_hash,plan_hash,decision,decision_hash,actor,decided_at
		FROM fornix.incident_approvals
		WHERE workspace_id=$1 AND run_id=$2 AND idempotency_key=$3`,
		workspaceID, runID, idempotencyKey).Scan(&approval.StepID, &approval.OperationHash, &approval.PlanHash,
		&approval.Decision, &approval.DecisionHash, &actorRaw, &approval.DecidedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.IncidentApproval{}, false, nil
	}
	if err != nil {
		return contracts.IncidentApproval{}, false, err
	}
	if err := json.Unmarshal(actorRaw, &approval.Actor); err != nil {
		return contracts.IncidentApproval{}, false, err
	}
	approval.SchemaVersion = contracts.IncidentSchemaVersion
	approval.WorkspaceID, approval.RunID, approval.IdempotencyKey = workspaceID, runID, idempotencyKey
	if err := tx.Commit(ctx); err != nil {
		return contracts.IncidentApproval{}, false, err
	}
	return approval, true, nil
}

func readIncidentTx(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, incidentID string) (contracts.Incident, error) {
	var value contracts.Incident
	var actorJSON []byte
	err := queryer.QueryRow(ctx, `SELECT id,workspace_id,source_system,external_id,severity,summary_hash,payload_hash,payload_evidence_id,status,workflow_id,receipt_id,event_hash,actor,created_at,updated_at FROM fornix.incident_records WHERE workspace_id=$1 AND id=$2`, workspaceID, incidentID).Scan(
		&value.ID, &value.WorkspaceID, &value.SourceSystem, &value.ExternalID, &value.Severity,
		&value.SummaryHash, &value.PayloadHash, &value.PayloadEvidenceID, &value.Status,
		&value.WorkflowID, &value.ReceiptID, &value.EventHash, &actorJSON, &value.CreatedAt, &value.UpdatedAt)
	if err == nil {
		value.SchemaVersion = contracts.IncidentSchemaVersion
		err = json.Unmarshal(actorJSON, &value.Actor)
	}
	return value, err
}

func readIncidentByNaturalTx(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, source, external string, lock bool) (contracts.Incident, error) {
	query := `SELECT id,workspace_id,source_system,external_id,severity,summary_hash,payload_hash,payload_evidence_id,status,workflow_id,receipt_id,event_hash,actor,created_at,updated_at FROM fornix.incident_records WHERE workspace_id=$1 AND source_system=$2 AND external_id=$3`
	if lock {
		query += " FOR UPDATE"
	}
	var value contracts.Incident
	var actorJSON []byte
	err := queryer.QueryRow(ctx, query, workspaceID, source, external).Scan(&value.ID, &value.WorkspaceID, &value.SourceSystem, &value.ExternalID, &value.Severity, &value.SummaryHash, &value.PayloadHash, &value.PayloadEvidenceID, &value.Status, &value.WorkflowID, &value.ReceiptID, &value.EventHash, &actorJSON, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return contracts.Incident{}, err
	}
	if err := json.Unmarshal(actorJSON, &value.Actor); err != nil {
		return contracts.Incident{}, fmt.Errorf("decode incident actor: %w", err)
	}
	value.SchemaVersion = contracts.IncidentSchemaVersion
	return value, nil
}

func incidentHashString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
