package federation

import (
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestNormalizeImportedMessagesSortsAndRejectsContradictorySequences(t *testing.T) {
	peer := contracts.FederationPeer{ID: "peer-a", WorkspaceID: "workspace-a", RemoteWorkspaceID: "remote-a"}
	request := contracts.FederationPollRequest{WorkspaceID: "workspace-a", PeerID: "peer-a", Actor: contracts.ActorRef{ID: "actor", Kind: "service", WorkspaceID: "workspace-a"}}
	attempt := contracts.FederationPollAttempt{ID: "attempt-a", StartedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	remote := []contracts.CoordinationMessage{
		{Sequence: 2, Sender: "remote", Recipient: "local", Subject: "two", Body: "two"},
		{Sequence: 1, Sender: "remote", Recipient: "local", Subject: "one", Body: "one"},
	}
	messages, err := normalizeImportedMessages(peer, request, attempt, remote)
	if err != nil || len(messages) != 2 || messages[0].IdempotencyKey != "federation:peer-a:1" || messages[1].IdempotencyKey != "federation:peer-a:2" {
		t.Fatalf("normalized messages=%+v err=%v", messages, err)
	}
	contradictory := append(remote, contracts.CoordinationMessage{Sequence: 1, Sender: "remote", Recipient: "local", Subject: "one", Body: "changed"})
	if _, err := normalizeImportedMessages(peer, request, attempt, contradictory); err == nil {
		t.Fatal("contradictory remote sequence was accepted")
	}
}
