package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// WorkspaceCoordinationSchemaVersion versions the workspace-scoped control
// message authority introduced after the historical global coordination
// table. Old global rows are never silently assigned to this schema.
const WorkspaceCoordinationSchemaVersion = 1

const (
	CoordinationMessageEventType = "coordination.message_recorded"
	RouterObservationEventType   = "router.observation_recorded"
	MaxCoordinationBodyBytes     = 64 << 10
	MaxCoordinationSubjectBytes  = 512
	MaxCoordinationAddressBytes  = 256
	MaxRouterCategoryBytes       = 256
	MaxRouterModelBytes          = 256
)

// CoordinationMessage is an append-only workspace message. Sequence is
// assigned by Postgres and is the only ordering authority for read-after
// sequence queries.
type CoordinationMessage struct {
	SchemaVersion  int       `json:"schema_version"`
	ID             string    `json:"id"`
	Sequence       int64     `json:"sequence,omitempty"`
	WorkspaceID    string    `json:"workspace_id"`
	RequestID      string    `json:"request_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	RequestHash    string    `json:"request_hash"`
	Sender         string    `json:"sender"`
	Recipient      string    `json:"recipient"`
	Subject        string    `json:"subject"`
	Body           string    `json:"body,omitempty"`
	Actor          ActorRef  `json:"actor"`
	CausationID    string    `json:"causation_id,omitempty"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	OriginHost     string    `json:"origin_host,omitempty"`
	OccurredAt     time.Time `json:"occurred_at"`
	CreatedAt      time.Time `json:"created_at"`
}

// Normalize validates the bounded message and derives its request hash from
// caller-owned fields. Durable sequence and creation time remain Postgres
// facts and are not part of the idempotency identity.
func (m *CoordinationMessage) Normalize() error {
	if m == nil {
		return fmt.Errorf("coordination message is nil")
	}
	if m.SchemaVersion == 0 {
		m.SchemaVersion = WorkspaceCoordinationSchemaVersion
	}
	if m.SchemaVersion != WorkspaceCoordinationSchemaVersion {
		return fmt.Errorf("unsupported coordination schema_version %d", m.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(m.WorkspaceID)
	if err != nil {
		return err
	}
	m.WorkspaceID = workspace
	if strings.TrimSpace(m.ID) == "" {
		m.ID = NewID("coord")
	}
	if strings.TrimSpace(m.RequestID) == "" {
		m.RequestID = NewID("coordreq")
	}
	for name, value := range map[string]*string{
		"coordination message id":      &m.ID,
		"coordination request_id":      &m.RequestID,
		"coordination idempotency_key": &m.IdempotencyKey,
		"coordination sender":          &m.Sender,
		"coordination recipient":       &m.Recipient,
		"coordination subject":         &m.Subject,
		"coordination causation_id":    &m.CausationID,
		"coordination correlation_id":  &m.CorrelationID,
		"coordination origin_host":     &m.OriginHost,
	} {
		*value = strings.TrimSpace(*value)
		if *value == "" && name != "coordination idempotency_key" && name != "coordination causation_id" && name != "coordination correlation_id" && name != "coordination origin_host" {
			return fmt.Errorf("%s is required", name)
		}
		if len(*value) > MaxCoordinationAddressBytes && (name == "coordination sender" || name == "coordination recipient") {
			return fmt.Errorf("%s is too large", name)
		}
	}
	if m.IdempotencyKey == "" {
		return fmt.Errorf("coordination idempotency_key is required")
	}
	if len(m.IdempotencyKey) > MaxIdempotencyLength || len(m.RequestID) > MaxIdempotencyLength || len(m.ID) > MaxEventIDLength {
		return fmt.Errorf("coordination identity is too large")
	}
	if len(m.Subject) > MaxCoordinationSubjectBytes {
		return fmt.Errorf("coordination subject is too large")
	}
	if len([]byte(m.Body)) > MaxCoordinationBodyBytes {
		return fmt.Errorf("coordination body exceeds %d bytes", MaxCoordinationBodyBytes)
	}
	if m.Actor.WorkspaceID == "" {
		m.Actor.WorkspaceID = workspace
	}
	if err := m.Actor.normalize(); err != nil {
		return err
	}
	if m.Actor.WorkspaceID != workspace {
		return fmt.Errorf("coordination actor crosses workspace boundary")
	}
	if m.OccurredAt.IsZero() {
		m.OccurredAt = time.Now().UTC()
	}
	m.OccurredAt = m.OccurredAt.UTC()
	if m.CreatedAt.IsZero() {
		m.CreatedAt = m.OccurredAt
	}
	m.CreatedAt = m.CreatedAt.UTC()
	hash := m.StableHash()
	if m.RequestHash != "" && m.RequestHash != hash {
		return fmt.Errorf("coordination request_hash does not match message")
	}
	m.RequestHash = hash
	return nil
}

// StableHash excludes server-assigned identity and time fields.
func (m CoordinationMessage) StableHash() string {
	value := struct {
		SchemaVersion  int
		WorkspaceID    string
		RequestID      string
		IdempotencyKey string
		Sender         string
		Recipient      string
		Subject        string
		Body           string
		Actor          ActorRef
		CausationID    string
		CorrelationID  string
		OriginHost     string
	}{m.SchemaVersion, m.WorkspaceID, m.RequestID, m.IdempotencyKey, m.Sender, m.Recipient, m.Subject, m.Body, m.Actor, m.CausationID, m.CorrelationID, m.OriginHost}
	raw, _ := json.Marshal(value)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// RouterObservation is an append-only workspace telemetry record used only
// to derive bounded recommendations. It never replaces model-call authority.
type RouterObservation struct {
	SchemaVersion  int       `json:"schema_version"`
	ID             int64     `json:"id,omitempty"`
	WorkspaceID    string    `json:"workspace_id"`
	RequestID      string    `json:"request_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	RequestHash    string    `json:"request_hash"`
	TaskCategory   string    `json:"task_category"`
	ModelID        string    `json:"model_id"`
	CostUSD        float64   `json:"cost_usd"`
	LatencyMS      int64     `json:"latency_ms"`
	Outcome        string    `json:"outcome"`
	OutcomeScore   *float64  `json:"outcome_score,omitempty"`
	Actor          ActorRef  `json:"actor"`
	CausationID    string    `json:"causation_id,omitempty"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	ObservedAt     time.Time `json:"observed_at"`
	CreatedAt      time.Time `json:"created_at"`
}

func (o *RouterObservation) Normalize() error {
	if o == nil {
		return fmt.Errorf("router observation is nil")
	}
	if o.SchemaVersion == 0 {
		o.SchemaVersion = WorkspaceCoordinationSchemaVersion
	}
	if o.SchemaVersion != WorkspaceCoordinationSchemaVersion {
		return fmt.Errorf("unsupported router observation schema_version %d", o.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(o.WorkspaceID)
	if err != nil {
		return err
	}
	o.WorkspaceID = workspace
	o.RequestID = strings.TrimSpace(o.RequestID)
	o.IdempotencyKey = strings.TrimSpace(o.IdempotencyKey)
	o.TaskCategory = strings.TrimSpace(o.TaskCategory)
	o.ModelID = strings.TrimSpace(o.ModelID)
	o.Outcome = strings.ToLower(strings.TrimSpace(o.Outcome))
	if o.RequestID == "" || o.IdempotencyKey == "" || o.TaskCategory == "" || o.ModelID == "" {
		return fmt.Errorf("router observation request, idempotency, category, and model are required")
	}
	if len(o.TaskCategory) > MaxRouterCategoryBytes || len(o.ModelID) > MaxRouterModelBytes || len(o.RequestID) > MaxIdempotencyLength || len(o.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("router observation identity is too large")
	}
	if o.CostUSD < 0 || o.LatencyMS < 0 {
		return fmt.Errorf("router observation cost and latency cannot be negative")
	}
	if o.Outcome == "" {
		o.Outcome = "unknown"
	}
	if o.OutcomeScore != nil && (*o.OutcomeScore < 0 || *o.OutcomeScore > 1) {
		return fmt.Errorf("router observation outcome_score must be between 0 and 1")
	}
	if o.Actor.WorkspaceID == "" {
		o.Actor.WorkspaceID = workspace
	}
	if err := o.Actor.normalize(); err != nil {
		return err
	}
	if o.Actor.WorkspaceID != workspace {
		return fmt.Errorf("router observation actor crosses workspace boundary")
	}
	if o.ObservedAt.IsZero() {
		o.ObservedAt = time.Now().UTC()
	}
	o.ObservedAt = o.ObservedAt.UTC()
	if o.CreatedAt.IsZero() {
		o.CreatedAt = o.ObservedAt
	}
	o.CreatedAt = o.CreatedAt.UTC()
	hash := o.StableHash()
	if o.RequestHash != "" && o.RequestHash != hash {
		return fmt.Errorf("router observation request_hash does not match observation")
	}
	o.RequestHash = hash
	return nil
}

func (o RouterObservation) StableHash() string {
	value := struct {
		SchemaVersion  int
		WorkspaceID    string
		RequestID      string
		IdempotencyKey string
		TaskCategory   string
		ModelID        string
		CostUSD        float64
		LatencyMS      int64
		Outcome        string
		OutcomeScore   *float64
		Actor          ActorRef
		CausationID    string
		CorrelationID  string
	}{o.SchemaVersion, o.WorkspaceID, o.RequestID, o.IdempotencyKey, o.TaskCategory, o.ModelID, o.CostUSD, o.LatencyMS, o.Outcome, o.OutcomeScore, o.Actor, o.CausationID, o.CorrelationID}
	raw, _ := json.Marshal(value)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// RouterRecommendation is a derived aggregate, never an authority record.
type RouterRecommendation struct {
	ModelID     string  `json:"model_id"`
	CostUSDAvg  float64 `json:"cost_usd_avg"`
	LatencyP50  float64 `json:"latency_p50"`
	SuccessRate float64 `json:"success_rate"`
	SampleSize  int64   `json:"sample_size"`
}
