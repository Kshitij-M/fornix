package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// IncidentSchemaVersion versions the domain-neutral incident qualification
// contract. Incident records are an adapter-owned resource; workflow,
// operation, evidence, artifact, and receipt authorities remain shared.
const IncidentSchemaVersion = 1

const (
	IncidentDeliveryFake   = "fake"
	IncidentDeliverySigned = "signed"

	IncidentSeverityInfo     = "info"
	IncidentSeverityWarning  = "warning"
	IncidentSeverityCritical = "critical"

	IncidentStatusOpen             = "open"
	IncidentStatusInvestigating    = "investigating"
	IncidentStatusAwaitingApproval = "awaiting_approval"
	IncidentStatusRemediating      = "remediating"
	IncidentStatusResolved         = "resolved"
	IncidentStatusFailed           = "failed"
	IncidentStatusCancelled        = "cancelled"
	IncidentStatusVerificationFail = "verification_failed"
)

const (
	IncidentResourceKind = "incident"
	IncidentEventType    = "incident.received"
	IncidentMaxPayload   = 64 << 10
	IncidentMaxSummary   = 512
	IncidentMaxSource    = 128
)

// IncidentEvent is the bounded ingress envelope. Payload is accepted only at
// the ingress boundary and is immediately preserved as immutable evidence;
// durable incident/workflow rows retain hashes and references instead.
type IncidentEvent struct {
	SchemaVersion   int             `json:"schema_version,omitempty"`
	DeliveryID      string          `json:"delivery_id,omitempty"`
	WorkspaceID     string          `json:"workspace_id"`
	SourceSystem    string          `json:"source_system"`
	ExternalID      string          `json:"external_id"`
	Severity        string          `json:"severity"`
	Summary         string          `json:"summary,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	PayloadHash     string          `json:"payload_hash,omitempty"`
	DeliveryMode    string          `json:"delivery_mode"`
	SignatureHash   string          `json:"signature_hash,omitempty"`
	SignatureScheme string          `json:"signature_scheme,omitempty"`
	OccurredAt      time.Time       `json:"occurred_at"`
	RequestID       string          `json:"request_id,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key"`
	CausationID     string          `json:"causation_id,omitempty"`
	CorrelationID   string          `json:"correlation_id,omitempty"`
	Actor           ActorRef        `json:"actor"`
}

// Normalize validates the ingress boundary and computes a content hash when
// callers supplied raw payload bytes. It performs no I/O or verification.
func (e *IncidentEvent) Normalize() error {
	if e == nil {
		return fmt.Errorf("incident event is nil")
	}
	if e.SchemaVersion == 0 {
		e.SchemaVersion = IncidentSchemaVersion
	}
	if e.SchemaVersion != IncidentSchemaVersion {
		return fmt.Errorf("unsupported incident schema_version %d", e.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(e.WorkspaceID)
	if err != nil {
		return err
	}
	source, err := normalizeDomainName(e.SourceSystem, "incident source_system", IncidentMaxSource, true)
	if err != nil {
		return err
	}
	externalID, err := normalizeDomainIdentifier(e.ExternalID, "incident external_id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	deliveryID, err := normalizeDomainIdentifier(e.DeliveryID, "incident delivery_id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	key, err := normalizeDomainIdentifier(e.IdempotencyKey, "incident idempotency_key", MaxIdempotencyLength, true)
	if err != nil {
		return err
	}
	requestID, err := normalizeDomainIdentifier(e.RequestID, "incident request_id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	causation, err := normalizeDomainIdentifier(e.CausationID, "incident causation_id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	correlation, err := normalizeDomainIdentifier(e.CorrelationID, "incident correlation_id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	if err := normalizeDomainActor(&e.Actor, workspace); err != nil {
		return err
	}
	e.Severity = strings.ToLower(strings.TrimSpace(e.Severity))
	switch e.Severity {
	case IncidentSeverityInfo, IncidentSeverityWarning, IncidentSeverityCritical:
	default:
		return fmt.Errorf("unsupported incident severity %q", e.Severity)
	}
	e.DeliveryMode = strings.ToLower(strings.TrimSpace(e.DeliveryMode))
	if e.DeliveryMode != IncidentDeliveryFake && e.DeliveryMode != IncidentDeliverySigned {
		return fmt.Errorf("unsupported incident delivery_mode %q", e.DeliveryMode)
	}
	if e.DeliveryMode == IncidentDeliverySigned {
		if _, err := normalizeDomainHash(e.SignatureHash, "incident signature_hash", true); err != nil {
			return err
		}
		if _, err := normalizeDomainName(e.SignatureScheme, "incident signature_scheme", 64, true); err != nil {
			return err
		}
	}
	if len(e.Payload) == 0 || len(e.Payload) > IncidentMaxPayload {
		return fmt.Errorf("incident payload must be between 1 and %d bytes", IncidentMaxPayload)
	}
	if !json.Valid(e.Payload) {
		return fmt.Errorf("incident payload must be valid JSON")
	}
	payloadHash := ArtifactContentHash(e.Payload)
	if e.PayloadHash != "" {
		if _, err := normalizeDomainHash(e.PayloadHash, "incident payload_hash", true); err != nil {
			return err
		}
		if strings.ToLower(e.PayloadHash) != payloadHash {
			return fmt.Errorf("incident payload_hash does not match payload")
		}
	}
	if len(e.Summary) > IncidentMaxSummary || strings.ContainsAny(e.Summary, "\x00\r\n") {
		return fmt.Errorf("incident summary is invalid or exceeds %d characters", IncidentMaxSummary)
	}
	e.Summary = strings.TrimSpace(e.Summary)
	e.PayloadHash = payloadHash
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Unix(0, 0).UTC()
	} else {
		e.OccurredAt = e.OccurredAt.UTC()
	}
	e.WorkspaceID, e.SourceSystem, e.ExternalID = workspace, source, externalID
	e.DeliveryID, e.RequestID, e.IdempotencyKey = deliveryID, requestID, key
	e.CausationID, e.CorrelationID = causation, correlation
	return nil
}

// NaturalIdentity is stable across delivery retries and excludes request
// identity, timestamps, and raw payload bytes.
func (e IncidentEvent) NaturalIdentity() string {
	return HashStrings(e.WorkspaceID, e.SourceSystem, e.ExternalID)
}

// StableHash identifies the logical event content and delivery domain.
func (e IncidentEvent) StableHash() string {
	clone := e
	if clone.Normalize() != nil {
		return ""
	}
	clone.Payload = nil
	clone.DeliveryID, clone.RequestID, clone.IdempotencyKey = "", "", ""
	clone.CausationID, clone.CorrelationID = "", ""
	raw, err := json.Marshal(clone)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// Incident is the current, bounded adapter projection. Evidence and
// artifacts are authoritative for payload/report bytes.
type Incident struct {
	SchemaVersion     int       `json:"schema_version"`
	ID                string    `json:"id"`
	WorkspaceID       string    `json:"workspace_id"`
	SourceSystem      string    `json:"source_system"`
	ExternalID        string    `json:"external_id"`
	Severity          string    `json:"severity"`
	SummaryHash       string    `json:"summary_hash,omitempty"`
	PayloadHash       string    `json:"payload_hash"`
	PayloadEvidenceID int64     `json:"payload_evidence_id"`
	Status            string    `json:"status"`
	WorkflowID        string    `json:"workflow_id,omitempty"`
	ReceiptID         string    `json:"receipt_id,omitempty"`
	EventHash         string    `json:"event_hash"`
	Actor             ActorRef  `json:"actor"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// IncidentWorkflowRequest starts or replays one fake-first cross-domain
// workflow. Raw event payload is preserved as evidence by the service.
type IncidentWorkflowRequest struct {
	Event          IncidentEvent `json:"event"`
	RunID          string        `json:"run_id,omitempty"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	RequestID      string        `json:"request_id,omitempty"`
	Actor          ActorRef      `json:"actor"`
	ApprovalToken  string        `json:"approval_token,omitempty"`
}

// IncidentApproval is the hash-only decision bound to one workflow plan and
// operation. It never carries comments, prompts, or credentials.
type IncidentApproval struct {
	SchemaVersion  int       `json:"schema_version"`
	WorkspaceID    string    `json:"workspace_id"`
	RunID          string    `json:"run_id"`
	OperationHash  string    `json:"operation_hash"`
	PlanHash       string    `json:"plan_hash"`
	StepID         string    `json:"step_id"`
	Decision       string    `json:"decision"`
	DecisionHash   string    `json:"decision_hash"`
	Actor          ActorRef  `json:"actor"`
	IdempotencyKey string    `json:"idempotency_key"`
	DecidedAt      time.Time `json:"decided_at"`
}

// Normalize validates a hash-only approval decision. Approval is a workflow
// input, not a permission grant by itself; the service still checks the
// persisted waiting step and operation/plan identity.
func (a *IncidentApproval) Normalize() error {
	if a == nil {
		return fmt.Errorf("incident approval is nil")
	}
	if a.SchemaVersion == 0 {
		a.SchemaVersion = IncidentSchemaVersion
	}
	if a.SchemaVersion != IncidentSchemaVersion {
		return fmt.Errorf("unsupported incident approval schema_version %d", a.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(a.WorkspaceID)
	if err != nil {
		return err
	}
	if _, err := normalizeDomainIdentifier(a.RunID, "incident approval run_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if _, err := normalizeDomainIdentifier(a.StepID, "incident approval step_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if _, err := normalizeDomainIdentifier(a.IdempotencyKey, "incident approval idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if _, err := normalizeDomainHash(a.OperationHash, "incident approval operation_hash", true); err != nil {
		return err
	}
	if _, err := normalizeDomainHash(a.PlanHash, "incident approval plan_hash", true); err != nil {
		return err
	}
	a.Decision = strings.ToLower(strings.TrimSpace(a.Decision))
	if a.Decision != "approve" && a.Decision != "reject" {
		return fmt.Errorf("incident approval decision must be approve or reject")
	}
	if err := normalizeDomainActor(&a.Actor, workspace); err != nil {
		return err
	}
	if a.DecidedAt.IsZero() {
		a.DecidedAt = time.Unix(0, 0).UTC()
	} else {
		a.DecidedAt = a.DecidedAt.UTC()
	}
	a.WorkspaceID = workspace
	a.DecisionHash = HashStrings(workspace, a.RunID, a.StepID, a.OperationHash, a.PlanHash, a.Decision)
	return nil
}

// StableHash returns the approval decision identity without delivery time or
// idempotency identity.
func (a IncidentApproval) StableHash() string {
	clone := a
	if clone.Normalize() != nil {
		return ""
	}
	return clone.DecisionHash
}

// IncidentWorkflowResult is the redacted operator-facing result. Details are
// disclosed through the existing evidence, artifact, receipt, and replay APIs.
type IncidentWorkflowResult struct {
	Incident     Incident          `json:"incident"`
	Workflow     WorkflowRun       `json:"workflow"`
	Receipt      *WorkReceipt      `json:"receipt,omitempty"`
	Approval     *IncidentApproval `json:"approval,omitempty"`
	ReplayHash   string            `json:"replay_hash,omitempty"`
	ReplayVerify bool              `json:"replay_verified"`
	Duplicate    bool              `json:"duplicate"`
}
