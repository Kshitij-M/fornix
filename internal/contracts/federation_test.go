package contracts

import (
	"strings"
	"testing"
	"time"
)

func TestFederationPeerConfigHashExcludesAuditActor(t *testing.T) {
	peer := FederationPeer{
		ID: "peer-a", WorkspaceID: "workspace-a", RemoteWorkspaceID: "remote-a",
		EndpointURL: "https://peer.example.test/root", CredentialRef: "provider/federation",
		CreatedBy: ActorRef{ID: "operator-a", Kind: "human", WorkspaceID: "workspace-a"},
	}
	if err := peer.Normalize(); err != nil {
		t.Fatal(err)
	}
	firstHash := peer.ConfigHash
	peer.CreatedBy.ID = "operator-b"
	peer.ConfigHash = ""
	if err := peer.Normalize(); err != nil {
		t.Fatal(err)
	}
	if peer.ConfigHash != firstHash {
		t.Fatalf("audit actor changed configuration hash: first=%s second=%s", firstHash, peer.ConfigHash)
	}
}

func TestFederationPeerRejectsInlineSecretAndUnsafeEndpoint(t *testing.T) {
	base := FederationPeer{ID: "peer-a", WorkspaceID: "workspace-a", RemoteWorkspaceID: "remote-a", EndpointURL: "https://peer.example.test", CredentialRef: "provider/federation", CreatedBy: ActorRef{ID: "operator", Kind: "human", WorkspaceID: "workspace-a"}}
	for _, candidate := range []FederationPeer{
		func() FederationPeer { value := base; value.CredentialRef = "Bearer secret"; return value }(),
		func() FederationPeer {
			value := base
			value.EndpointURL = "https://user:password@peer.example.test"
			return value
		}(),
		func() FederationPeer {
			value := base
			value.EndpointURL = "https://peer.example.test?token=secret"
			return value
		}(),
	} {
		if err := candidate.Normalize(); err == nil {
			t.Fatalf("unsafe federation peer was accepted: %+v", candidate)
		}
	}
}

func TestFederationPollRequestHashIgnoresWorkerLeaseAndTime(t *testing.T) {
	request := FederationPollRequest{SchemaVersion: FederationSchemaVersion, RequestID: "request-a", IdempotencyKey: "poll-a", WorkspaceID: "workspace-a", PeerID: "peer-a", FromSequence: 4, OwnerID: "worker-a", Fence: 1, Actor: ActorRef{ID: "actor", Kind: "service", WorkspaceID: "workspace-a"}, CorrelationID: "correlation", OccurredAt: time.Now().UTC()}
	first := request.RequestHash()
	request.OwnerID = "worker-b"
	request.Fence = 9
	request.OccurredAt = request.OccurredAt.Add(time.Hour)
	if request.RequestHash() != first {
		t.Fatal("worker lease metadata changed poll request identity")
	}
	if strings.Contains(first, "worker") {
		t.Fatal("poll request hash contains raw worker identity")
	}
}
