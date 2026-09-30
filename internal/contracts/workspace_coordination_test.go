package contracts

import (
	"strings"
	"testing"
)

func TestCoordinationMessageNormalizationAndHashing(t *testing.T) {
	message := CoordinationMessage{
		WorkspaceID: "workspace-a", IdempotencyKey: "coord-1", Sender: "worker-a",
		Recipient: "worker-b", Subject: "handoff", Body: "bounded",
		Actor: ActorRef{ID: "actor-a", Kind: "worker"},
	}
	if err := message.Normalize(); err != nil {
		t.Fatal(err)
	}
	if message.RequestHash == "" || len(message.RequestHash) != 64 || message.Actor.WorkspaceID != message.WorkspaceID {
		t.Fatalf("normalized message = %+v", message)
	}
	copyMessage := message
	if err := copyMessage.Normalize(); err != nil {
		t.Fatal(err)
	}
	if copyMessage.RequestHash != message.RequestHash {
		t.Fatalf("hash changed on normalization: %s != %s", copyMessage.RequestHash, message.RequestHash)
	}
	message.Body = strings.Repeat("x", MaxCoordinationBodyBytes+1)
	if err := message.Normalize(); err == nil {
		t.Fatal("oversized coordination body was accepted")
	}
}

func TestRouterObservationNormalizationRejectsCrossWorkspaceAndInvalidScores(t *testing.T) {
	score := 1.1
	observation := RouterObservation{WorkspaceID: "workspace-a", RequestID: "request-1", IdempotencyKey: "router-1", TaskCategory: "review", ModelID: "fake", OutcomeScore: &score, Actor: ActorRef{ID: "actor", WorkspaceID: "workspace-a"}}
	if err := observation.Normalize(); err == nil {
		t.Fatal("invalid router score was accepted")
	}
	score = 0.5
	observation.Actor.WorkspaceID = "workspace-b"
	if err := observation.Normalize(); err == nil {
		t.Fatal("cross-workspace router actor was accepted")
	}
}
