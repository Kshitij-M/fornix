package federation_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/credentials"
	federationruntime "github.com/omaveda/fornix/internal/federation"
	"github.com/omaveda/fornix/internal/store"
	"github.com/omaveda/fornix/internal/testutil"
)

func TestPollerUsesFencedCredentialAndControlledEgressWithDeterministicImport(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	pool, workspace := newPollerPool(t)
	var requests atomic.Int32
	var leaseValidations atomic.Int32
	var rejectValidationAt atomic.Int32
	secretValue := "poll-secret-must-not-escape"
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/v1/coord/recent" || r.URL.Query().Get("after_sequence") != "0" || r.URL.Query().Get("limit") == "" {
			http.Error(w, "bad route", http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+secretValue {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("X-Request-ID", "remote-request-1")
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": []contracts.CoordinationMessage{{
			Sequence: 1, Sender: "remote", Recipient: "local", Subject: "incident", Body: "bounded remote message",
			Actor: contracts.ActorRef{ID: "remote-actor", Kind: "service", WorkspaceID: "remote-workspace"}, OccurredAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		}}})
	}))
	defer remote.Close()
	events := store.NewEventStore(pool)
	coordination := store.NewWorkspaceCoordinationStore(pool, events)
	peers := store.NewFederationStore(pool, events, coordination)
	actor := contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}
	peer := contracts.FederationPeer{ID: "peer-a", WorkspaceID: workspace, RemoteWorkspaceID: "remote-workspace", EndpointURL: remote.URL, CredentialRef: "provider/federation", AllowPrivateNetwork: true, CreatedBy: actor}
	if _, _, err := peers.CreatePeer(context.Background(), contracts.FederationPeerCommand{RequestID: "peer-request", IdempotencyKey: "peer-key", WorkspaceID: workspace, Peer: peer, Actor: actor}); err != nil {
		t.Fatal(err)
	}
	resolver := credentials.LeaseResolverFunc{
		AcquireFunc: func(_ context.Context, workspaceID, reference, purpose string, _ time.Duration) (credentials.Lease, error) {
			ref, err := credentials.ParseRef(reference)
			if err != nil {
				return credentials.Lease{}, err
			}
			secret, err := credentials.NewSecret([]byte(secretValue))
			if err != nil {
				return credentials.Lease{}, err
			}
			return credentials.Lease{Reference: ref, WorkspaceID: workspaceID, LeaseID: "credential-1", Purpose: purpose, Fence: 1, RevocationEpoch: 1, SourceVersion: credentials.LocalSourceVersion, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}, nil
		},
		ValidateLeaseFunc: func(context.Context, credentials.Lease) error {
			call := leaseValidations.Add(1)
			if rejectValidationAt.Load() != 0 && call == rejectValidationAt.Load() {
				return credentials.ErrLeaseRevoked
			}
			return nil
		},
	}
	poller := &federationruntime.Poller{Peers: peers, Credentials: resolver}
	request := contracts.FederationPollRequest{RequestID: "poll-request", IdempotencyKey: "poll-key", WorkspaceID: workspace, PeerID: peer.ID, OwnerID: "poll-worker", Actor: actor, FromSequence: 0, CorrelationID: "poll-correlation"}
	attempt, err := poller.Poll(context.Background(), request)
	if err != nil || attempt.State != contracts.FederationPollSucceeded || attempt.ImportedCount != 1 || requests.Load() != 1 || leaseValidations.Load() != 5 {
		t.Fatalf("poll attempt=%+v requests=%d validations=%d err=%v", attempt, requests.Load(), leaseValidations.Load(), err)
	}
	if attempt.ResponseHash == "" || attempt.ProviderRequestID != "remote-request-1" {
		t.Fatalf("remote facts were not retained safely: %+v", attempt)
	}
	replayed, err := poller.Poll(context.Background(), request)
	if err != nil || replayed.ID != attempt.ID || replayed.ResponseHash != attempt.ResponseHash || requests.Load() != 1 {
		t.Fatalf("replayed poll=%+v requests=%d err=%v", replayed, requests.Load(), err)
	}
	rejectValidationAt.Store(leaseValidations.Load() + 4)
	revokedRequest := request
	revokedRequest.RequestID = "poll-revoked-request"
	revokedRequest.IdempotencyKey = "poll-revoked-key"
	if _, err := poller.Poll(context.Background(), revokedRequest); err == nil {
		t.Fatal("poller sent a request after its final lease validation was revoked")
	}
	if requests.Load() != 1 {
		t.Fatalf("revoked lease crossed the network boundary: requests=%d", requests.Load())
	}
	var messages int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.workspace_coordination_messages WHERE workspace_id=$1`, workspace).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if messages != 1 {
		t.Fatalf("replay duplicated local message count=%d", messages)
	}
}

func TestPollerMarksRemoteFailureForRecoveryWithoutLeakingSecret(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	pool, workspace := newPollerPool(t)
	secretValue := "poll-secret-must-not-escape"
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "remote body must not be persisted", http.StatusBadGateway)
	}))
	defer remote.Close()
	events := store.NewEventStore(pool)
	coordination := store.NewWorkspaceCoordinationStore(pool, events)
	peers := store.NewFederationStore(pool, events, coordination)
	actor := contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}
	peer := contracts.FederationPeer{ID: "peer-failure", WorkspaceID: workspace, RemoteWorkspaceID: "remote-workspace", EndpointURL: remote.URL, CredentialRef: "provider/federation", AllowPrivateNetwork: true, CreatedBy: actor}
	if _, _, err := peers.CreatePeer(context.Background(), contracts.FederationPeerCommand{RequestID: "peer-request", IdempotencyKey: "peer-key", WorkspaceID: workspace, Peer: peer, Actor: actor}); err != nil {
		t.Fatal(err)
	}
	resolver := credentials.LeaseResolverFunc{AcquireFunc: func(_ context.Context, workspaceID, reference, purpose string, _ time.Duration) (credentials.Lease, error) {
		ref, err := credentials.ParseRef(reference)
		if err != nil {
			return credentials.Lease{}, err
		}
		secret, err := credentials.NewSecret([]byte(secretValue))
		if err != nil {
			return credentials.Lease{}, err
		}
		return credentials.Lease{Reference: ref, WorkspaceID: workspaceID, LeaseID: "credential-1", Purpose: purpose, Fence: 1, RevocationEpoch: 1, SourceVersion: credentials.LocalSourceVersion, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}, nil
	}, ValidateLeaseFunc: func(context.Context, credentials.Lease) error { return nil }}
	poller := &federationruntime.Poller{Peers: peers, Credentials: resolver}
	request := contracts.FederationPollRequest{RequestID: "poll-request", IdempotencyKey: "poll-key", WorkspaceID: workspace, PeerID: peer.ID, OwnerID: "poll-worker", Actor: actor}
	attempt, err := poller.Poll(context.Background(), request)
	if !errors.Is(err, federationruntime.ErrRecoveryRequired) || attempt.State != contracts.FederationPollRecoveryNeeded {
		t.Fatalf("failure attempt=%+v err=%v", attempt, err)
	}
	if strings.Contains(err.Error(), secretValue) {
		t.Fatalf("secret leaked through poll error")
	}
	if attempt.FailureCode != "remote_response" || attempt.ResponseHash == "" {
		t.Fatalf("remote failure facts=%+v", attempt)
	}
}

func TestPollerReconcilesRecordedResponseWithoutExternalEffects(t *testing.T) {
	pool, workspace := newPollerPool(t)
	events := store.NewEventStore(pool)
	coordination := store.NewWorkspaceCoordinationStore(pool, events)
	peers := store.NewFederationStore(pool, events, coordination)
	actor := contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}
	peer := contracts.FederationPeer{ID: "peer-reconcile", WorkspaceID: workspace, RemoteWorkspaceID: "remote-workspace", EndpointURL: "https://peer.example.test/root", CredentialRef: "provider/federation", AllowPrivateNetwork: true, CreatedBy: actor}
	if _, _, err := peers.CreatePeer(context.Background(), contracts.FederationPeerCommand{RequestID: "peer-request", IdempotencyKey: "peer-key", WorkspaceID: workspace, Peer: peer, Actor: actor}); err != nil {
		t.Fatal(err)
	}
	lease, err := peers.AcquirePeerLease(context.Background(), workspace, peer.ID, "reconcile-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	pollRequest := contracts.FederationPollRequest{RequestID: "poll-request", IdempotencyKey: "poll-key", WorkspaceID: workspace, PeerID: peer.ID, OwnerID: lease.OwnerID, Fence: lease.Fence, Actor: actor}
	attempt, duplicate, err := peers.BeginPoll(context.Background(), pollRequest, lease)
	if err != nil || duplicate {
		t.Fatalf("begin attempt=%+v duplicate=%t err=%v", attempt, duplicate, err)
	}
	ref, err := credentials.ParseRef(peer.CredentialRef)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := credentials.NewSecret([]byte("ephemeral-secret"))
	if err != nil {
		t.Fatal(err)
	}
	credentialLease := credentials.Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "credential-1", Purpose: "federation:provider", Fence: 1, RevocationEpoch: 1, SourceVersion: credentials.LocalSourceVersion, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}
	attempt, err = peers.MarkPollDispatching(context.Background(), attempt, lease, credentialLease)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{"messages": []contracts.CoordinationMessage{{Sequence: 1, Sender: "remote", Recipient: "local", Subject: "reconcile", Body: "recorded response", OccurredAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}}})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	responseHash := hex.EncodeToString(digest[:])
	attempt, err = peers.MarkPollRecoveryRequired(context.Background(), attempt, lease, "remote_uncertain", "provider-request-1", responseHash)
	if err != nil {
		t.Fatal(err)
	}
	poller := &federationruntime.Poller{Peers: peers}
	reconciled, imported, err := poller.Reconcile(context.Background(), contracts.FederationPollReconcileRequest{WorkspaceID: workspace, AttemptID: attempt.ID, OwnerID: lease.OwnerID, Fence: lease.Fence, ResponseHash: responseHash, ProviderRequestID: "provider-request-1", ResponsePayload: payload, Actor: actor})
	if err != nil || reconciled.State != contracts.FederationPollSucceeded || imported != 1 {
		t.Fatalf("reconciled attempt=%+v imported=%d err=%v", reconciled, imported, err)
	}
	replayed, imported, err := poller.Reconcile(context.Background(), contracts.FederationPollReconcileRequest{WorkspaceID: workspace, AttemptID: attempt.ID, OwnerID: lease.OwnerID, Fence: lease.Fence, ResponseHash: responseHash, ProviderRequestID: "provider-request-1", ResponsePayload: payload, Actor: actor})
	if err != nil || replayed.State != contracts.FederationPollSucceeded || imported != 0 {
		t.Fatalf("replayed reconciliation=%+v imported=%d err=%v", replayed, imported, err)
	}
}

func newPollerPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workspace := fmt.Sprintf("poller-test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, table := range []string{"workspace_federation_poll_attempts", "workspace_federation_peer_leases", "workspace_federation_peer_commands", "workspace_federation_peers", "workspace_coordination_messages", "control_events"} {
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM fornix."+table+" WHERE workspace_id=$1", workspace)
		}
		pool.Close()
	})
	return pool, workspace
}
