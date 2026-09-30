package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/credentials"
)

func newFederationTestStore(t *testing.T) (*FederationStore, *pgxpool.Pool, string, contracts.FederationPeer) {
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
	if err := ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workspace := fmt.Sprintf("federation-test-%d", time.Now().UnixNano())
	actor := contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}
	peer := contracts.FederationPeer{ID: "peer-a", WorkspaceID: workspace, RemoteWorkspaceID: "remote-a", EndpointURL: "https://peer.example.test/root", CredentialRef: "provider/federation", CreatedBy: actor}
	store := NewFederationStore(pool, NewEventStore(pool), NewWorkspaceCoordinationStore(pool, NewEventStore(pool)))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, table := range []string{"federation_retention_tombstones", "consumer_leases", "workspace_federation_poll_attempts", "workspace_federation_peer_leases", "workspace_federation_peer_commands", "workspace_federation_peers", "workspace_coordination_messages", "control_events"} {
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM fornix."+table+" WHERE workspace_id=$1", workspace)
		}
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.federation_legacy_quarantine WHERE audit_workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.federation_peers WHERE id LIKE $1`, "legacy-"+workspace+"-%")
		pool.Close()
	})
	return store, pool, workspace, peer
}

func TestFederationRetentionIsDryRunBoundedTombstonedAndWorkspaceScoped(t *testing.T) {
	store, pool, workspace, peer := newFederationTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreatePeer(ctx, federationPeerCommand(workspace, peer, "retention-peer")); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquirePeerLease(ctx, workspace, peer.ID, "retention-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	attempt, _, err := store.BeginPoll(ctx, contracts.FederationPollRequest{RequestID: "retention-request", IdempotencyKey: "retention-key", WorkspaceID: workspace, PeerID: peer.ID, OwnerID: lease.OwnerID, Fence: lease.Fence, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}}, lease)
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := credentials.ParseRef(peer.CredentialRef)
	secret, _ := credentials.NewSecret([]byte("retention-secret"))
	attempt, err = store.MarkPollDispatching(ctx, attempt, lease, credentials.Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "retention-credential", Purpose: "federation:provider", Fence: 1, RevocationEpoch: 1, SourceVersion: credentials.LocalSourceVersion, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CompletePoll(ctx, attempt, lease, nil, contracts.HashStrings("retention-response"), "retention-provider-request"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleasePeerLease(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE fornix.workspace_federation_poll_attempts SET retention_deadline=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND id=$2`, workspace, attempt.ID); err != nil {
		t.Fatal(err)
	}
	actor := contracts.ActorRef{ID: "retention-operator", Kind: "human", WorkspaceID: workspace}
	dry, err := store.RetentionSweep(ctx, contracts.FederationRetentionRequest{WorkspaceID: workspace, Before: time.Now().UTC(), BatchSize: 10, DryRun: true, Actor: actor})
	if err != nil || dry.PollCandidates != 1 || dry.PollExpired != 0 {
		t.Fatalf("retention dry run=%+v err=%v", dry, err)
	}
	var tombstones int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.federation_retention_tombstones WHERE workspace_id=$1`, workspace).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if tombstones != 0 {
		t.Fatalf("dry run created tombstones=%d", tombstones)
	}
	result, err := store.RetentionSweep(ctx, contracts.FederationRetentionRequest{WorkspaceID: workspace, Before: time.Now().UTC(), BatchSize: 10, Actor: actor})
	if err != nil || result.PollExpired != 1 || len(result.TombstoneHashes) != 1 {
		t.Fatalf("retention result=%+v err=%v", result, err)
	}
	var remaining, tombstoneRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.workspace_federation_poll_attempts WHERE workspace_id=$1 AND id=$2`, workspace, attempt.ID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.federation_retention_tombstones WHERE workspace_id=$1`, workspace).Scan(&tombstoneRows); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 || tombstoneRows != 1 {
		t.Fatalf("retention source/tombstone remaining=%d/%d", remaining, tombstoneRows)
	}
	repeated, err := store.RetentionSweep(ctx, contracts.FederationRetentionRequest{WorkspaceID: workspace, Before: time.Now().UTC(), BatchSize: 10, Actor: actor})
	if err != nil || repeated.PollExpired != 0 {
		t.Fatalf("repeated retention=%+v err=%v", repeated, err)
	}
	if _, err := store.RetentionSweep(ctx, contracts.FederationRetentionRequest{WorkspaceID: workspace + "-other", Before: time.Now().UTC(), BatchSize: 10, DryRun: true, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace + "-other"}}); err != nil {
		t.Fatal(err)
	}
}

func TestFederationRetentionOwnerFenceRejectsStaleSweep(t *testing.T) {
	store, _, workspace, _ := newFederationTestStore(t)
	ctx := context.Background()
	first, err := store.events.AcquireConsumerLease(ctx, workspace, contracts.FederationRetentionConsumerID, "retention-owner-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.events.AcquireConsumerLease(ctx, workspace, contracts.FederationRetentionConsumerID, "retention-owner-b", time.Minute); !errors.Is(err, ErrConsumerLeaseHeld) {
		t.Fatalf("active retention owner takeover=%v, want held", err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE fornix.consumer_leases SET lease_until=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND consumer_id=$2`, workspace, contracts.FederationRetentionConsumerID); err != nil {
		t.Fatal(err)
	}
	second, err := store.events.AcquireConsumerLease(ctx, workspace, contracts.FederationRetentionConsumerID, "retention-owner-b", time.Minute)
	if err != nil || second.Lease.Fence <= first.Lease.Fence {
		t.Fatalf("retention takeover=%+v err=%v", second, err)
	}
	_, err = store.RetentionSweep(ctx, contracts.FederationRetentionRequest{
		WorkspaceID: workspace, Before: time.Now().UTC(), DryRun: true,
		OwnerID: first.Lease.OwnerID, Fence: first.Lease.Fence,
		Actor: contracts.ActorRef{ID: "retention-owner-a", Kind: "service", WorkspaceID: workspace},
	})
	if !errors.Is(err, ErrConsumerLeaseFenced) {
		t.Fatalf("stale retention sweep=%v, want fenced", err)
	}
}

func federationPeerCommand(workspace string, peer contracts.FederationPeer, key string) contracts.FederationPeerCommand {
	actor := contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}
	return contracts.FederationPeerCommand{RequestID: "request-" + key, IdempotencyKey: key, WorkspaceID: workspace, Peer: peer, Actor: actor, CausationID: "cause-" + key, CorrelationID: "corr-" + key}
}

func TestFederationPeerCommandIsWorkspaceScopedIdempotentAndAuditable(t *testing.T) {
	store, pool, workspace, peer := newFederationTestStore(t)
	ctx := context.Background()
	first, duplicate, err := store.CreatePeer(ctx, federationPeerCommand(workspace, peer, "peer-create"))
	if err != nil || duplicate || first.Revision != 1 || first.ConfigHash == "" {
		t.Fatalf("first peer create=%+v duplicate=%t err=%v", first, duplicate, err)
	}
	repeated, duplicate, err := store.CreatePeer(ctx, federationPeerCommand(workspace, peer, "peer-create"))
	if err != nil || !duplicate || repeated.Revision != first.Revision {
		t.Fatalf("duplicate peer create=%+v duplicate=%t err=%v", repeated, duplicate, err)
	}
	changed := peer
	changed.EndpointURL = "https://peer.example.test/changed"
	if _, _, err := store.CreatePeer(ctx, federationPeerCommand(workspace, changed, "peer-create")); !errors.Is(err, ErrFederationPeerConflict) {
		t.Fatalf("idempotency conflict=%v, want ErrFederationPeerConflict", err)
	}
	var commandRows, eventRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.workspace_federation_peer_commands WHERE workspace_id=$1`, workspace).Scan(&commandRows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type=$2`, workspace, contracts.FederationEventPeerCommand).Scan(&eventRows); err != nil {
		t.Fatal(err)
	}
	if commandRows != 1 || eventRows != 1 {
		t.Fatalf("duplicate peer command created command_rows=%d event_rows=%d", commandRows, eventRows)
	}
	if _, err := store.GetPeer(ctx, workspace+"-other", peer.ID); !errors.Is(err, ErrFederationPeerNotFound) {
		t.Fatalf("cross-workspace peer lookup=%v, want not found", err)
	}
}

func TestFederationPeerLeaseTakeoverAndStaleFenceFailClosed(t *testing.T) {
	store, pool, workspace, peer := newFederationTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreatePeer(ctx, federationPeerCommand(workspace, peer, "lease-peer")); err != nil {
		t.Fatal(err)
	}
	first, err := store.AcquirePeerLease(ctx, workspace, peer.ID, "worker-a", time.Minute)
	if err != nil || first.Fence != 1 {
		t.Fatalf("first lease=%+v err=%v", first, err)
	}
	if _, err := store.AcquirePeerLease(ctx, workspace, peer.ID, "worker-b", time.Minute); !errors.Is(err, ErrFederationPeerLeaseHeld) {
		t.Fatalf("live lease takeover=%v, want held", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE fornix.workspace_federation_peer_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND peer_id=$2`, workspace, peer.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.AcquirePeerLease(ctx, workspace, peer.ID, "worker-b", time.Minute)
	if err != nil || second.Fence != 2 {
		t.Fatalf("takeover lease=%+v err=%v", second, err)
	}
	if err := store.ValidatePeerLease(ctx, first); !errors.Is(err, ErrFederationPeerLeaseFenced) {
		t.Fatalf("stale validation=%v, want fenced", err)
	}
	if _, err := store.RenewPeerLease(ctx, first, time.Minute); !errors.Is(err, ErrFederationPeerLeaseFenced) {
		t.Fatalf("stale renewal=%v, want fenced", err)
	}
}

func TestFederationPollLifecycleIsAtomicFencedAndDuplicateSafe(t *testing.T) {
	store, pool, workspace, peer := newFederationTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreatePeer(ctx, federationPeerCommand(workspace, peer, "poll-peer")); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquirePeerLease(ctx, workspace, peer.ID, "poll-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.FederationPollRequest{RequestID: "poll-request", IdempotencyKey: "poll-key", WorkspaceID: workspace, PeerID: peer.ID, OwnerID: lease.OwnerID, Fence: lease.Fence, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}, OccurredAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	attempt, duplicate, err := store.BeginPoll(ctx, request, lease)
	if err != nil || duplicate || attempt.State != contracts.FederationPollReserved {
		t.Fatalf("begin poll=%+v duplicate=%t err=%v", attempt, duplicate, err)
	}
	secret, err := credentials.NewSecret([]byte("ephemeral-federation-secret"))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := credentials.ParseRef(peer.CredentialRef)
	if err != nil {
		t.Fatal(err)
	}
	credentialLease := credentials.Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "credential-lease", Purpose: "federation:provider", Fence: 1, RevocationEpoch: 1, SourceVersion: credentials.LocalSourceVersion, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}
	store.SetFailureHook(func(point string) error {
		if point == "federation_poll_dispatching" {
			return errors.New("simulated dispatch crash")
		}
		return nil
	})
	if _, err := store.MarkPollDispatching(ctx, attempt, lease, credentialLease); err == nil {
		t.Fatal("dispatch crash hook unexpectedly succeeded")
	}
	store.SetFailureHook(nil)
	state, err := store.GetPollAttempt(ctx, workspace, attempt.ID)
	if err != nil || state.State != contracts.FederationPollReserved {
		t.Fatalf("crash changed reserved attempt state=%+v err=%v", state, err)
	}
	attempt, err = store.MarkPollDispatching(ctx, attempt, lease, credentialLease)
	if err != nil || attempt.State != contracts.FederationPollDispatching {
		t.Fatalf("dispatch attempt=%+v err=%v", attempt, err)
	}
	message := contracts.CoordinationMessage{WorkspaceID: workspace, RequestID: "remote-message-1", IdempotencyKey: "federation:peer-a:1", Sender: "remote", Recipient: "local", Subject: "bounded", Body: "message", Actor: request.Actor, OriginHost: peer.RemoteWorkspaceID, OccurredAt: request.OccurredAt}
	responseHash := contracts.HashStrings("remote-response")
	completed, imported, err := store.CompletePoll(ctx, attempt, lease, []contracts.CoordinationMessage{message}, responseHash, "remote-request-1")
	if err != nil || imported != 1 || completed.State != contracts.FederationPollSucceeded {
		t.Fatalf("complete poll=%+v imported=%d err=%v", completed, imported, err)
	}
	replayed, imported, err := store.CompletePoll(ctx, attempt, lease, []contracts.CoordinationMessage{message}, responseHash, "remote-request-1")
	if err != nil || imported != 0 || replayed.State != contracts.FederationPollSucceeded {
		t.Fatalf("duplicate complete=%+v imported=%d err=%v", replayed, imported, err)
	}
	var messages, events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.workspace_coordination_messages WHERE workspace_id=$1`, workspace).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type=$2`, workspace, contracts.CoordinationMessageEventType).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if messages != 1 || events != 1 {
		t.Fatalf("duplicate imported message messages=%d events=%d", messages, events)
	}
	stale := lease
	stale.Fence = 1
	if _, _, err := store.CompletePoll(ctx, completed, contracts.FederationPeerLease{WorkspaceID: workspace, PeerID: peer.ID, OwnerID: "other", Fence: 99}, nil, responseHash, ""); !errors.Is(err, ErrFederationPeerLeaseFenced) {
		t.Fatalf("stale completion=%v, want fenced", err)
	}
	_ = stale
}

func TestFederationPollRecoveryCanBeReclaimedOnlyByHigherFence(t *testing.T) {
	store, pool, workspace, peer := newFederationTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreatePeer(ctx, federationPeerCommand(workspace, peer, "reclaim-peer")); err != nil {
		t.Fatal(err)
	}
	firstLease, err := store.AcquirePeerLease(ctx, workspace, peer.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.FederationPollRequest{RequestID: "reclaim-request", IdempotencyKey: "reclaim-key", WorkspaceID: workspace, PeerID: peer.ID, OwnerID: firstLease.OwnerID, Fence: firstLease.Fence, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}}
	attempt, _, err := store.BeginPoll(ctx, request, firstLease)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := credentials.NewSecret([]byte("ephemeral-reclaim-secret"))
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := credentials.ParseRef(peer.CredentialRef)
	attempt, err = store.MarkPollDispatching(ctx, attempt, firstLease, credentials.Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "credential-reclaim", Purpose: "federation:provider", Fence: 1, RevocationEpoch: 1, SourceVersion: credentials.LocalSourceVersion, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkPollRecoveryRequired(ctx, attempt, firstLease, "remote_uncertain", "provider-reclaim", contracts.HashStrings("response-reclaim")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE fornix.workspace_federation_peer_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND peer_id=$2`, workspace, peer.ID); err != nil {
		t.Fatal(err)
	}
	secondLease, err := store.AcquirePeerLease(ctx, workspace, peer.ID, "worker-b", time.Minute)
	if err != nil || secondLease.Fence <= firstLease.Fence {
		t.Fatalf("takeover lease=%+v err=%v", secondLease, err)
	}
	reclaimed, err := store.ReclaimPollAttempt(ctx, attempt, secondLease, contracts.ActorRef{ID: "takeover", Kind: "service", WorkspaceID: workspace})
	if err != nil || reclaimed.OwnerID != secondLease.OwnerID || reclaimed.Fence != secondLease.Fence || reclaimed.State != contracts.FederationPollRecoveryNeeded {
		t.Fatalf("reclaimed attempt=%+v err=%v", reclaimed, err)
	}
	if _, err := store.ReclaimPollAttempt(ctx, reclaimed, firstLease, request.Actor); !errors.Is(err, ErrFederationPeerLeaseFenced) {
		t.Fatalf("stale reclaim=%v, want fenced", err)
	}
}

func TestFederationConcurrentLeaseAcquisitionHasOneOwner(t *testing.T) {
	store, _, workspace, peer := newFederationTestStore(t)
	ctx := context.Background()
	if _, _, err := store.CreatePeer(ctx, federationPeerCommand(workspace, peer, "concurrent-peer")); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, owner := range []string{"worker-a", "worker-b"} {
		owner := owner
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.AcquirePeerLease(ctx, workspace, peer.ID, owner, time.Minute)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	var success, held int
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrFederationPeerLeaseHeld):
			held++
		default:
			t.Fatalf("unexpected concurrent lease result: %v", err)
		}
	}
	if success != 1 || held != 1 {
		t.Fatalf("concurrent lease results success=%d held=%d", success, held)
	}
}

func TestFederationLegacyQuarantineIsRedactedIdempotentAndPaginated(t *testing.T) {
	store, pool, workspace, _ := newFederationTestStore(t)
	ctx := context.Background()
	secret := "legacy-bearer-must-never-be-copied"
	for _, suffix := range []string{"a", "b"} {
		_, err := pool.Exec(ctx, `INSERT INTO fornix.federation_peers(id,url,bearer_token,last_pull_high_water) VALUES($1,$2,$3,$4)`, "legacy-"+workspace+"-"+suffix, "https://legacy.example.test/"+suffix, secret, 7)
		if err != nil {
			t.Fatal(err)
		}
	}
	actor := contracts.ActorRef{ID: "quarantine-operator", Kind: "human", WorkspaceID: workspace}
	request := contracts.FederationLegacyQuarantineRequest{RequestID: "quarantine-request", IdempotencyKey: "quarantine-key", AuditWorkspaceID: workspace, Reason: "historical ownership is unknown", Limit: 10, Actor: actor}
	first, duplicate, err := store.QuarantineLegacyPeers(ctx, request)
	if err != nil || duplicate || len(first.Items) != 2 {
		t.Fatalf("first quarantine page=%+v duplicate=%t err=%v", first, duplicate, err)
	}
	if strings.Contains(fmt.Sprintf("%+v", first), secret) {
		t.Fatalf("quarantine result leaked legacy secret: %+v", first)
	}
	repeated, duplicate, err := store.QuarantineLegacyPeers(ctx, request)
	if err != nil || !duplicate || len(repeated.Items) != len(first.Items) {
		t.Fatalf("duplicate quarantine page=%+v duplicate=%t err=%v", repeated, duplicate, err)
	}
	page, err := store.ListLegacyQuarantine(ctx, workspace, "", 1)
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("first quarantine list page=%+v err=%v", page, err)
	}
	page, err = store.ListLegacyQuarantine(ctx, workspace, page.NextCursor, 1)
	if err != nil || len(page.Items) != 1 || page.NextCursor != "" {
		t.Fatalf("second quarantine list page=%+v err=%v", page, err)
	}
	var storedSecret string
	if err := pool.QueryRow(ctx, `SELECT bearer_token FROM fornix.federation_peers WHERE id=$1`, "legacy-"+workspace+"-a").Scan(&storedSecret); err != nil {
		t.Fatal(err)
	}
	if storedSecret != secret {
		t.Fatalf("legacy fixture was unexpectedly changed")
	}
}
