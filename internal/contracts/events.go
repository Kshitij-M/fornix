package contracts

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	DefaultWorkspaceID   = "default"
	EventSchemaVersion   = 1
	MaxEventPayloadBytes = 4 << 20
	MaxEventIDLength     = 128
	MaxIdempotencyLength = 256
)

const (
	DeltaSet    = "set"
	DeltaAdd    = "add"
	DeltaRemove = "remove"
	DeltaDelete = "delete"
)

var eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*(\.[a-z0-9_-]+)+$`)

// Scope identifies the authorization and delivery boundary of an event.
// WorkspaceID is explicit so it can never be inferred from free-form payload text.
type Scope struct {
	WorkspaceID string `json:"workspace_id"`
	Visibility  string `json:"visibility,omitempty"`
	Subject     string `json:"subject,omitempty"`
}

// ActorRef identifies the principal that caused a state transition without
// carrying credentials or bearer tokens into the event log.
type ActorRef struct {
	ID   string `json:"id,omitempty"`
	Kind string `json:"kind,omitempty"`
	Name string `json:"name,omitempty"`
}

// EntityRef gives a stable typed reference to a task or session.
type EntityRef struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	WorkspaceID string `json:"workspace_id,omitempty"`
}

// ArtifactReference points at immutable or content-addressed data. Event rows
// contain references and metadata, not large artifact bodies.
type ArtifactReference struct {
	Ref       string `json:"ref"`
	Kind      string `json:"kind,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

// Provenance records the evidence chain from which a state transition was
// derived. It is intentionally additive and never replaces the raw payload.
type Provenance struct {
	SourceEventIDs     []string `json:"source_event_ids,omitempty"`
	SourceArtifactRefs []string `json:"source_artifact_refs,omitempty"`
	SourcePaths        []string `json:"source_paths,omitempty"`
}

// StateDelta is a deterministic mutation proposal over a typed state path.
// The value remains JSON so the contract can evolve without losing data.
type StateDelta struct {
	Op       string          `json:"op"`
	Path     string          `json:"path"`
	Value    json.RawMessage `json:"value,omitempty"`
	Evidence []string        `json:"evidence,omitempty"`
}

// EventEnvelope is the durable control-plane contract. Sequence and
// RecordedAt are assigned by the Postgres event store; all other fields are
// part of the caller's immutable event request.
type EventEnvelope struct {
	EventID        string              `json:"event_id"`
	Sequence       uint64              `json:"sequence,omitempty"`
	EventType      string              `json:"event_type"`
	SchemaVersion  int                 `json:"schema_version"`
	OccurredAt     time.Time           `json:"occurred_at"`
	RecordedAt     time.Time           `json:"recorded_at,omitempty"`
	Scope          Scope               `json:"scope"`
	Actor          ActorRef            `json:"actor,omitempty"`
	Task           *EntityRef          `json:"task,omitempty"`
	Session        *EntityRef          `json:"session,omitempty"`
	CausationID    string              `json:"causation_id,omitempty"`
	CorrelationID  string              `json:"correlation_id,omitempty"`
	IdempotencyKey string              `json:"idempotency_key,omitempty"`
	StateDeltas    []StateDelta        `json:"state_deltas,omitempty"`
	Artifacts      []ArtifactReference `json:"artifacts,omitempty"`
	Provenance     Provenance          `json:"provenance,omitempty"`
	Payload        json.RawMessage     `json:"payload"`
}

// NewEvent creates a valid event request with a generated identity and raw
// JSON payload. It performs no model or network calls.
func NewEvent(eventType string, payload any) (EventEnvelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return EventEnvelope{}, fmt.Errorf("marshal event payload: %w", err)
	}
	event := EventEnvelope{
		EventID:       NewID("evt"),
		EventType:     eventType,
		SchemaVersion: EventSchemaVersion,
		OccurredAt:    time.Now().UTC(),
		Scope:         Scope{WorkspaceID: DefaultWorkspaceID},
		Payload:       raw,
	}
	if err := event.Normalize(); err != nil {
		return EventEnvelope{}, err
	}
	return event, nil
}

// Clone returns an event value that can be normalized or serialized without
// mutating slices, raw JSON, or entity references owned by the caller. Store
// boundaries may receive the same logical event concurrently for duplicate
// delivery, so normalization must not race on shared backing arrays.
func (e EventEnvelope) Clone() EventEnvelope {
	clone := e
	clone.Payload = append(json.RawMessage(nil), e.Payload...)
	clone.Task = cloneEntityRef(e.Task)
	clone.Session = cloneEntityRef(e.Session)
	clone.StateDeltas = make([]StateDelta, len(e.StateDeltas))
	for i, delta := range e.StateDeltas {
		clone.StateDeltas[i] = delta
		clone.StateDeltas[i].Value = append(json.RawMessage(nil), delta.Value...)
		clone.StateDeltas[i].Evidence = append([]string(nil), delta.Evidence...)
	}
	clone.Artifacts = append([]ArtifactReference(nil), e.Artifacts...)
	clone.Provenance.SourceEventIDs = append([]string(nil), e.Provenance.SourceEventIDs...)
	clone.Provenance.SourceArtifactRefs = append([]string(nil), e.Provenance.SourceArtifactRefs...)
	clone.Provenance.SourcePaths = append([]string(nil), e.Provenance.SourcePaths...)
	return clone
}

func cloneEntityRef(ref *EntityRef) *EntityRef {
	if ref == nil {
		return nil
	}
	clone := *ref
	return &clone
}

// NewID creates a non-secret opaque identifier without adding a UUID
// dependency to the control-plane contract package.
func NewID(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is exceptionally rare; the timestamp keeps the ID
		// useful for diagnostics while the event store still enforces uniqueness.
		return strings.Trim(prefix+"_"+fmt.Sprintf("%d", time.Now().UnixNano()), "_")
	}
	return strings.Trim(prefix, "_") + "_" + hex.EncodeToString(b)
}

// Normalize validates and fills deterministic defaults on an event request.
func (e *EventEnvelope) Normalize() error {
	if e == nil {
		return fmt.Errorf("event is nil")
	}
	e.EventID = strings.TrimSpace(e.EventID)
	if e.EventID == "" {
		e.EventID = NewID("evt")
	}
	if len(e.EventID) > MaxEventIDLength {
		return fmt.Errorf("event_id exceeds %d characters", MaxEventIDLength)
	}
	e.EventType = strings.ToLower(strings.TrimSpace(e.EventType))
	if !eventTypePattern.MatchString(e.EventType) {
		return fmt.Errorf("event_type %q must use dot-separated lowercase segments", e.EventType)
	}
	if e.SchemaVersion == 0 {
		e.SchemaVersion = EventSchemaVersion
	}
	if e.SchemaVersion != EventSchemaVersion {
		return fmt.Errorf("unsupported event schema_version %d", e.SchemaVersion)
	}
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Now().UTC()
	}
	e.OccurredAt = e.OccurredAt.UTC()
	e.IdempotencyKey = strings.TrimSpace(e.IdempotencyKey)
	if len(e.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("idempotency_key exceeds %d characters", MaxIdempotencyLength)
	}
	if err := e.Scope.normalize(); err != nil {
		return err
	}
	if err := e.Actor.normalize(); err != nil {
		return err
	}
	if err := validateEntityRef(e.Task, "task"); err != nil {
		return err
	}
	if err := validateEntityRef(e.Session, "session"); err != nil {
		return err
	}
	if len(e.Payload) == 0 {
		e.Payload = json.RawMessage(`{}`)
	}
	if len(e.Payload) > MaxEventPayloadBytes || !json.Valid(e.Payload) {
		return fmt.Errorf("event payload must be valid JSON no larger than %d bytes", MaxEventPayloadBytes)
	}
	for i := range e.StateDeltas {
		if err := e.StateDeltas[i].normalize(); err != nil {
			return fmt.Errorf("state_deltas[%d]: %w", i, err)
		}
	}
	for i := range e.Artifacts {
		if err := e.Artifacts[i].normalize(); err != nil {
			return fmt.Errorf("artifacts[%d]: %w", i, err)
		}
	}
	e.Provenance.normalize()
	return nil
}

func (s *Scope) normalize() error {
	s.WorkspaceID = strings.TrimSpace(s.WorkspaceID)
	if s.WorkspaceID == "" {
		s.WorkspaceID = DefaultWorkspaceID
	}
	s.Visibility = strings.TrimSpace(s.Visibility)
	s.Subject = strings.TrimSpace(s.Subject)
	return nil
}

func (a *ActorRef) normalize() error {
	a.ID = strings.TrimSpace(a.ID)
	a.Kind = strings.TrimSpace(a.Kind)
	a.Name = strings.TrimSpace(a.Name)
	if len(a.ID) > MaxEventIDLength || len(a.Kind) > 64 || len(a.Name) > 256 {
		return fmt.Errorf("actor reference is too large")
	}
	return nil
}

func validateEntityRef(ref *EntityRef, expectedKind string) error {
	if ref == nil {
		return nil
	}
	ref.ID = strings.TrimSpace(ref.ID)
	ref.Kind = strings.ToLower(strings.TrimSpace(ref.Kind))
	ref.WorkspaceID = strings.TrimSpace(ref.WorkspaceID)
	if ref.ID == "" || ref.Kind == "" {
		return fmt.Errorf("%s reference requires id and kind", expectedKind)
	}
	if ref.Kind != expectedKind {
		return fmt.Errorf("%s reference kind must be %q", expectedKind, expectedKind)
	}
	return nil
}

func (a *ArtifactReference) normalize() error {
	a.Ref = strings.TrimSpace(a.Ref)
	a.Kind = strings.TrimSpace(a.Kind)
	a.SHA256 = strings.ToLower(strings.TrimSpace(a.SHA256))
	a.MediaType = strings.TrimSpace(a.MediaType)
	if a.Ref == "" {
		return fmt.Errorf("ref is required")
	}
	if len(a.Ref) > 2048 || len(a.Kind) > 128 || len(a.SHA256) > 64 || len(a.MediaType) > 256 {
		return fmt.Errorf("artifact reference is too large")
	}
	if a.SHA256 != "" {
		if len(a.SHA256) != sha256.Size*2 {
			return fmt.Errorf("sha256 must contain %d hexadecimal characters", sha256.Size*2)
		}
		if _, err := hex.DecodeString(a.SHA256); err != nil {
			return fmt.Errorf("sha256 must be hexadecimal: %w", err)
		}
	}
	if a.SizeBytes < 0 {
		return fmt.Errorf("size_bytes cannot be negative")
	}
	return nil
}

func (d *StateDelta) normalize() error {
	d.Op = strings.ToLower(strings.TrimSpace(d.Op))
	if d.Op != DeltaSet && d.Op != DeltaAdd && d.Op != DeltaRemove && d.Op != DeltaDelete {
		return fmt.Errorf("op %q must be set, add, remove, or delete", d.Op)
	}
	d.Path = strings.TrimSpace(d.Path)
	if d.Path == "" || !strings.HasPrefix(d.Path, "/") {
		return fmt.Errorf("path must be an absolute slash-prefixed path")
	}
	if d.Op != DeltaDelete {
		if len(d.Value) == 0 || !json.Valid(d.Value) {
			return fmt.Errorf("value must be valid JSON for op %q", d.Op)
		}
	}
	for i := range d.Evidence {
		d.Evidence[i] = strings.TrimSpace(d.Evidence[i])
	}
	return nil
}

func (p *Provenance) normalize() {
	p.SourceEventIDs = cleanStrings(p.SourceEventIDs)
	p.SourceArtifactRefs = cleanStrings(p.SourceArtifactRefs)
	p.SourcePaths = cleanStrings(p.SourcePaths)
}

func cleanStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

// RequestHash returns the stable content hash used to compare retries. Server
// assigned identity, sequence, and wall-clock recording time are excluded so
// a retried logical request can use a newly generated event ID.
func RequestHash(event EventEnvelope) (string, error) {
	input := struct {
		EventType     string              `json:"event_type"`
		SchemaVersion int                 `json:"schema_version"`
		Scope         Scope               `json:"scope"`
		Actor         ActorRef            `json:"actor,omitempty"`
		Task          *EntityRef          `json:"task,omitempty"`
		Session       *EntityRef          `json:"session,omitempty"`
		CausationID   string              `json:"causation_id,omitempty"`
		CorrelationID string              `json:"correlation_id,omitempty"`
		StateDeltas   []StateDelta        `json:"state_deltas,omitempty"`
		Artifacts     []ArtifactReference `json:"artifacts,omitempty"`
		Provenance    Provenance          `json:"provenance,omitempty"`
		Payload       json.RawMessage     `json:"payload"`
	}{
		EventType:     event.EventType,
		SchemaVersion: event.SchemaVersion,
		Scope:         event.Scope,
		Actor:         event.Actor,
		Task:          event.Task,
		Session:       event.Session,
		CausationID:   event.CausationID,
		CorrelationID: event.CorrelationID,
		StateDeltas:   event.StateDeltas,
		Artifacts:     event.Artifacts,
		Provenance:    event.Provenance,
		Payload:       canonicalJSON(event.Payload),
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("marshal event hash input: %w", err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalJSON(raw json.RawMessage) json.RawMessage {
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return raw
	}
	return append(json.RawMessage(nil), compact.Bytes()...)
}
