package contracts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNewEventAndNormalize(t *testing.T) {
	event, err := NewEvent("task.completed", map[string]any{"status": "done"})
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if event.EventID == "" || event.SchemaVersion != EventSchemaVersion {
		t.Fatalf("event defaults missing: %+v", event)
	}
	if event.Scope.WorkspaceID != DefaultWorkspaceID {
		t.Fatalf("workspace = %q, want %q", event.Scope.WorkspaceID, DefaultWorkspaceID)
	}
	if !json.Valid(event.Payload) {
		t.Fatalf("payload is not valid JSON: %s", event.Payload)
	}

	event.Task = &EntityRef{ID: " 42 ", Kind: "TASK"}
	event.Artifacts = []ArtifactReference{{
		Ref:    "  artifact://sha256/abc ",
		SHA256: strings.Repeat("A", 64),
	}}
	if err := event.Normalize(); err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if event.Task.ID != "42" || event.Task.Kind != "task" {
		t.Fatalf("task was not normalized: %+v", event.Task)
	}
	if event.Artifacts[0].SHA256 != strings.Repeat("a", 64) {
		t.Fatalf("artifact hash was not normalized: %+v", event.Artifacts[0])
	}
}

func TestNormalizeRejectsInvalidContracts(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*EventEnvelope)
	}{
		{name: "event type", mutate: func(e *EventEnvelope) { e.EventType = "Task Completed" }},
		{name: "task kind", mutate: func(e *EventEnvelope) { e.Task = &EntityRef{ID: "1", Kind: "memo"} }},
		{name: "delta path", mutate: func(e *EventEnvelope) {
			e.StateDeltas = []StateDelta{{Op: DeltaSet, Path: "tasks/1", Value: json.RawMessage(`true`)}}
		}},
		{name: "delta value", mutate: func(e *EventEnvelope) {
			e.StateDeltas = []StateDelta{{Op: DeltaSet, Path: "/tasks/1", Value: json.RawMessage(`{`)}}
		}},
		{name: "artifact hash", mutate: func(e *EventEnvelope) { e.Artifacts = []ArtifactReference{{Ref: "artifact://x", SHA256: "not-a-hash"}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event, err := NewEvent("task.completed", map[string]bool{"ok": true})
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(&event)
			if err := event.Normalize(); err == nil {
				t.Fatalf("Normalize() unexpectedly succeeded: %+v", event)
			}
		})
	}
}

func TestRequestHashIgnoresServerAssignedFieldsAndCanonicalizesPayload(t *testing.T) {
	first, err := NewEvent("task.completed", json.RawMessage(` { "status": "done" } `))
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.EventID = "evt_other"
	second.Sequence = 900
	second.OccurredAt = time.Now().Add(24 * time.Hour)
	second.RecordedAt = time.Now().Add(48 * time.Hour)
	second.Payload = json.RawMessage(`{"status":"done"}`)
	firstHash, err := RequestHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := RequestHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("hashes differ for equivalent retries: %s != %s", firstHash, secondHash)
	}
}
