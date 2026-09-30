package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/credentials"
)

var (
	ErrFederationPeerNotFound             = errors.New("federation peer not found")
	ErrFederationPeerConflict             = errors.New("federation peer command conflicts with existing state")
	ErrFederationPeerLeaseHeld            = errors.New("federation peer lease is held")
	ErrFederationPeerLeaseFenced          = errors.New("federation peer lease is stale")
	ErrFederationPeerLeaseMissing         = errors.New("federation peer lease is missing")
	ErrFederationPollConflict             = errors.New("federation poll conflicts with existing state")
	ErrFederationPollTerminal             = errors.New("federation poll is already terminal")
	ErrFederationExternalBoundaryRequired = errors.New("federation poll requires controlled boundary authority")
)

const (
	defaultFederationLeaseTTL = 30 * time.Second
	maxFederationLeaseTTL     = 5 * time.Minute
)

// FederationStore is the Postgres authority for workspace-scoped peer
// configuration, poll ownership, and redacted poll lifecycle. It never
// accepts or stores bearer-token material.
type FederationStore struct {
	pool                    *pgxpool.Pool
	events                  *EventStore
	coordination            *WorkspaceCoordinationStore
	requireExternalBoundary bool
	failureMu               sync.Mutex
	failureHook             func(string) error
}

func NewFederationStore(pool *pgxpool.Pool, events *EventStore, coordination *WorkspaceCoordinationStore) *FederationStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &FederationStore{pool: pool, events: events, coordination: coordination}
}

// SetExternalBoundaryAuthorityRequired enables strict production admission for
// federation network reads. Development fixtures may leave it disabled so
// they can exercise local lifecycle behavior without a live endpoint policy.
func (s *FederationStore) SetExternalBoundaryAuthorityRequired(required bool) {
	if s != nil {
		s.requireExternalBoundary = required
	}
}

// SetFailureHook provides deterministic crash points for qualification tests.
func (s *FederationStore) SetFailureHook(hook func(string) error) {
	if s == nil {
		return
	}
	s.failureMu.Lock()
	s.failureHook = hook
	s.failureMu.Unlock()
}

func (s *FederationStore) fail(point string) error {
	if s == nil {
		return nil
	}
	s.failureMu.Lock()
	hook := s.failureHook
	s.failureMu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(point)
}

// CreatePeer applies one idempotent workspace peer command and its event in a
// single transaction. Configuration revisions advance monotonically; command
// history remains append-only.
func (s *FederationStore) CreatePeer(ctx context.Context, command contracts.FederationPeerCommand) (contracts.FederationPeer, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.FederationPeer{}, false, fmt.Errorf("federation store is not configured")
	}
	// The command actor is the only authoritative creator identity. A caller
	// cannot smuggle a different audit actor through the peer projection.
	command.Peer.CreatedBy = command.Actor
	if err := command.Normalize(); err != nil {
		return contracts.FederationPeer{}, false, err
	}
	requestHash := command.RequestHash()
	actorJSON := mustJSON(command.Actor)
	tx, err := beginWorkspaceTx(ctx, s.pool, command.WorkspaceID)
	if err != nil {
		return contracts.FederationPeer{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentRevision int64
	readErr := tx.QueryRow(ctx, `
		SELECT revision FROM fornix.workspace_federation_peers
		WHERE workspace_id=$1 AND peer_id=$2 FOR UPDATE`, command.WorkspaceID, command.Peer.ID).Scan(&currentRevision)
	if errors.Is(readErr, pgx.ErrNoRows) {
		currentRevision = 0
	} else if readErr != nil {
		return contracts.FederationPeer{}, false, readErr
	}
	command.Peer.Revision = currentRevision + 1
	command.Peer.ConfigHash = command.Peer.StableHash()
	peerJSON := mustJSON(command.Peer)
	peerActorJSON := mustJSON(command.Peer.CreatedBy)

	commandID := contracts.NewID("federation-peer-command")
	var sequence int64
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.workspace_federation_peer_commands
			(id,workspace_id,peer_id,schema_version,request_id,idempotency_key,request_hash,revision,peer,actor,causation_id,correlation_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11,$12)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING
		RETURNING sequence`, commandID, command.WorkspaceID, command.Peer.ID, command.SchemaVersion, command.RequestID,
		command.IdempotencyKey, requestHash, command.Peer.Revision, peerJSON, actorJSON, command.CausationID, command.CorrelationID).Scan(&sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		var existingHash string
		if err := tx.QueryRow(ctx, `SELECT request_hash FROM fornix.workspace_federation_peer_commands WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, command.WorkspaceID, command.IdempotencyKey).Scan(&existingHash); err != nil {
			return contracts.FederationPeer{}, false, err
		}
		if existingHash != requestHash {
			return contracts.FederationPeer{}, false, fmt.Errorf("%w: %s", ErrFederationPeerConflict, command.IdempotencyKey)
		}
		peer, readPeerErr := readFederationPeer(ctx, tx, command.WorkspaceID, command.Peer.ID)
		if readPeerErr != nil {
			return contracts.FederationPeer{}, false, readPeerErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.FederationPeer{}, false, err
		}
		return peer, true, nil
	}
	if err != nil {
		return contracts.FederationPeer{}, false, fmt.Errorf("insert federation peer command: %w", err)
	}

	var createdAt, updatedAt time.Time
	var storedActorJSON []byte
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.workspace_federation_peers(
			workspace_id,peer_id,schema_version,remote_workspace_id,endpoint_url,credential_ref,
			allow_private_networks,max_messages,max_response_bytes,timeout_ms,status,revision,config_hash,created_by)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb)
		ON CONFLICT (workspace_id,peer_id) DO UPDATE SET
			schema_version=EXCLUDED.schema_version,remote_workspace_id=EXCLUDED.remote_workspace_id,
			endpoint_url=EXCLUDED.endpoint_url,credential_ref=EXCLUDED.credential_ref,
			allow_private_networks=EXCLUDED.allow_private_networks,max_messages=EXCLUDED.max_messages,
			max_response_bytes=EXCLUDED.max_response_bytes,timeout_ms=EXCLUDED.timeout_ms,
			status=EXCLUDED.status,revision=EXCLUDED.revision,config_hash=EXCLUDED.config_hash,
			updated_at=clock_timestamp()
		RETURNING created_at,updated_at,created_by`, command.WorkspaceID, command.Peer.ID, command.Peer.SchemaVersion,
		command.Peer.RemoteWorkspaceID, command.Peer.EndpointURL, command.Peer.CredentialRef, command.Peer.AllowPrivateNetwork,
		command.Peer.MaxMessages, command.Peer.MaxResponseBytes, command.Peer.TimeoutMS, command.Peer.Status,
		command.Peer.Revision, command.Peer.ConfigHash, peerActorJSON).Scan(&createdAt, &updatedAt, &storedActorJSON)
	if err != nil {
		return contracts.FederationPeer{}, false, fmt.Errorf("upsert federation peer: %w", err)
	}
	if err := json.Unmarshal(storedActorJSON, &command.Peer.CreatedBy); err != nil {
		return contracts.FederationPeer{}, false, err
	}
	command.Peer.CreatedAt, command.Peer.UpdatedAt = createdAt, updatedAt
	payload, err := json.Marshal(map[string]any{"peer": command.Peer, "request_id": command.RequestID, "revision": command.Peer.Revision, "command_sequence": sequence})
	if err != nil {
		return contracts.FederationPeer{}, false, err
	}
	event := contracts.EventEnvelope{EventID: contracts.NewID("evt-federation-peer"), EventType: contracts.FederationEventPeerCommand, SchemaVersion: contracts.EventSchemaVersion, OccurredAt: updatedAt, Scope: contracts.Scope{WorkspaceID: command.WorkspaceID, Subject: command.Peer.ID}, Actor: command.Actor, CausationID: command.CausationID, CorrelationID: command.CorrelationID, IdempotencyKey: "federation-peer:" + command.WorkspaceID + ":" + command.IdempotencyKey, Payload: payload}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.FederationPeer{}, false, fmt.Errorf("append federation peer event: %w", err)
	}
	if err := s.fail("federation_peer_before_commit"); err != nil {
		return contracts.FederationPeer{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationPeer{}, false, err
	}
	return command.Peer, false, nil
}

func (s *FederationStore) GetPeer(ctx context.Context, workspaceID, peerID string) (contracts.FederationPeer, error) {
	if s == nil || s.pool == nil {
		return contracts.FederationPeer{}, fmt.Errorf("federation store is not configured")
	}
	return readFederationPeer(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(peerID))
}

func (s *FederationStore) ListPeers(ctx context.Context, workspaceID string, limit int) ([]contracts.FederationPeer, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("federation store is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, fmt.Errorf("workspace_id is required")
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT workspace_id,peer_id,schema_version,remote_workspace_id,endpoint_url,credential_ref,allow_private_networks,max_messages,max_response_bytes,timeout_ms,status,revision,config_hash,created_by,created_at,updated_at FROM fornix.workspace_federation_peers WHERE workspace_id=$1 ORDER BY peer_id LIMIT $2`, workspaceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	peers := make([]contracts.FederationPeer, 0, limit)
	for rows.Next() {
		peer, scanErr := scanFederationPeer(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		peers = append(peers, peer)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return peers, nil
}

// AcquirePeerLease grants or takes over one peer lease. A live lease owned by
// another worker fails closed; takeover always increments the fence.
func (s *FederationStore) AcquirePeerLease(ctx context.Context, workspaceID, peerID, ownerID string, ttl time.Duration) (contracts.FederationPeerLease, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.FederationPeerLease{}, fmt.Errorf("federation store is not configured")
	}
	workspaceID, peerID, ownerID = strings.TrimSpace(workspaceID), strings.TrimSpace(peerID), strings.TrimSpace(ownerID)
	if workspaceID == "" || peerID == "" || ownerID == "" {
		return contracts.FederationPeerLease{}, fmt.Errorf("workspace_id, peer_id, and owner_id are required")
	}
	if ttl <= 0 {
		ttl = defaultFederationLeaseTTL
	}
	if ttl > maxFederationLeaseTTL {
		ttl = maxFederationLeaseTTL
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.FederationPeerLease{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM fornix.workspace_federation_peers WHERE workspace_id=$1 AND peer_id=$2 FOR SHARE`, workspaceID, peerID).Scan(&status); errors.Is(err, pgx.ErrNoRows) {
		return contracts.FederationPeerLease{}, ErrFederationPeerNotFound
	} else if err != nil {
		return contracts.FederationPeerLease{}, err
	}
	if status != contracts.FederationPeerActive {
		return contracts.FederationPeerLease{}, fmt.Errorf("federation peer is disabled")
	}
	var currentOwner string
	var currentFence int64
	var expiresAt time.Time
	now := time.Now().UTC()
	readErr := tx.QueryRow(ctx, `SELECT owner_id,fence,expires_at FROM fornix.workspace_federation_peer_leases WHERE workspace_id=$1 AND peer_id=$2 FOR UPDATE`, workspaceID, peerID).Scan(&currentOwner, &currentFence, &expiresAt)
	if errors.Is(readErr, pgx.ErrNoRows) {
		currentFence = 1
	} else if readErr != nil {
		return contracts.FederationPeerLease{}, readErr
	} else if expiresAt.After(now) && currentOwner != ownerID {
		return contracts.FederationPeerLease{}, ErrFederationPeerLeaseHeld
	} else if expiresAt.After(now) && currentOwner == ownerID {
		// Re-acquisition by the current owner is a renewal and does not create a
		// new fence or allow two same-owner poll calls to split authority.
	}
	if readErr == nil && !expiresAt.After(now) {
		currentFence++
	}
	leaseExpiry := now.Add(ttl)
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.workspace_federation_peer_leases(workspace_id,peer_id,owner_id,fence,expires_at,acquired_at,renewed_at)
		VALUES($1,$2,$3,$4,$5,clock_timestamp(),clock_timestamp())
		ON CONFLICT (workspace_id,peer_id) DO UPDATE SET owner_id=EXCLUDED.owner_id,fence=EXCLUDED.fence,expires_at=EXCLUDED.expires_at,renewed_at=clock_timestamp()`, workspaceID, peerID, ownerID, currentFence, leaseExpiry); err != nil {
		return contracts.FederationPeerLease{}, err
	}
	payload, _ := json.Marshal(map[string]any{"peer_id": peerID, "owner_id": ownerID, "fence": currentFence, "expires_at": leaseExpiry})
	event := contracts.EventEnvelope{EventID: contracts.NewID("evt-federation-lease"), EventType: "federation.peer_lease_acquired", SchemaVersion: contracts.EventSchemaVersion, OccurredAt: now, Scope: contracts.Scope{WorkspaceID: workspaceID, Subject: peerID}, Actor: contracts.ActorRef{ID: ownerID, Kind: "service", WorkspaceID: workspaceID}, IdempotencyKey: fmt.Sprintf("federation-lease:%s:%s:%d:%s", workspaceID, peerID, currentFence, leaseExpiry.UTC().Format(time.RFC3339Nano)), Payload: payload}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.FederationPeerLease{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationPeerLease{}, err
	}
	return contracts.FederationPeerLease{WorkspaceID: workspaceID, PeerID: peerID, OwnerID: ownerID, Fence: uint64(currentFence), ExpiresAt: leaseExpiry}, nil
}

func (s *FederationStore) RenewPeerLease(ctx context.Context, lease contracts.FederationPeerLease, ttl time.Duration) (contracts.FederationPeerLease, error) {
	if s == nil || s.pool == nil {
		return contracts.FederationPeerLease{}, fmt.Errorf("federation store is not configured")
	}
	if ttl <= 0 {
		ttl = defaultFederationLeaseTTL
	}
	if ttl > maxFederationLeaseTTL {
		ttl = maxFederationLeaseTTL
	}
	now := time.Now().UTC()
	expires := now.Add(ttl)
	result, err := workspaceExec(ctx, s.pool, lease.WorkspaceID, `UPDATE fornix.workspace_federation_peer_leases SET expires_at=$1,renewed_at=clock_timestamp() WHERE workspace_id=$2 AND peer_id=$3 AND owner_id=$4 AND fence=$5 AND expires_at>clock_timestamp()`, expires, lease.WorkspaceID, lease.PeerID, lease.OwnerID, lease.Fence)
	if err != nil {
		return contracts.FederationPeerLease{}, err
	}
	if result.RowsAffected() == 0 {
		return contracts.FederationPeerLease{}, ErrFederationPeerLeaseFenced
	}
	lease.ExpiresAt = expires
	return lease, nil
}

func (s *FederationStore) ReleasePeerLease(ctx context.Context, lease contracts.FederationPeerLease) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("federation store is not configured")
	}
	// Retain the last fence after release. Deleting the row would let a later
	// acquisition restart at fence 1, allowing an old event identity to be
	// reused and weakening stale-worker detection.
	result, err := workspaceExec(ctx, s.pool, lease.WorkspaceID, `UPDATE fornix.workspace_federation_peer_leases SET expires_at=clock_timestamp(), renewed_at=clock_timestamp() WHERE workspace_id=$1 AND peer_id=$2 AND owner_id=$3 AND fence=$4`, lease.WorkspaceID, lease.PeerID, lease.OwnerID, lease.Fence)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrFederationPeerLeaseFenced
	}
	return nil
}

func (s *FederationStore) ValidatePeerLease(ctx context.Context, lease contracts.FederationPeerLease) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("federation store is not configured")
	}
	var exists bool
	err := workspaceQueryRow(ctx, s.pool, lease.WorkspaceID, `SELECT EXISTS(SELECT 1 FROM fornix.workspace_federation_peer_leases WHERE workspace_id=$1 AND peer_id=$2 AND owner_id=$3 AND fence=$4 AND expires_at>clock_timestamp())`, []any{lease.WorkspaceID, lease.PeerID, lease.OwnerID, lease.Fence}, func(row pgx.Row) error {
		return row.Scan(&exists)
	})
	if err != nil {
		return err
	}
	if !exists {
		return ErrFederationPeerLeaseFenced
	}
	return nil
}

// BeginPoll reserves an attempt under an already acquired peer fence.
func (s *FederationStore) BeginPoll(ctx context.Context, request contracts.FederationPollRequest, lease contracts.FederationPeerLease) (contracts.FederationPollAttempt, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.FederationPollAttempt{}, false, fmt.Errorf("federation store is not configured")
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" {
		request.IdempotencyKey = fmt.Sprintf("federation-poll:%s:%d", request.PeerID, request.FromSequence)
	}
	if strings.TrimSpace(request.RequestID) == "" {
		request.RequestID = request.IdempotencyKey
	}
	if err := normalizePollRequest(&request); err != nil {
		return contracts.FederationPollAttempt{}, false, err
	}
	if lease.WorkspaceID != request.WorkspaceID || lease.PeerID != request.PeerID || lease.OwnerID != request.OwnerID || lease.Fence != request.Fence {
		return contracts.FederationPollAttempt{}, false, ErrFederationPeerLeaseFenced
	}
	requestHash := request.RequestHash()
	attemptID := "poll-" + contracts.HashStrings(request.WorkspaceID, request.PeerID, fmt.Sprint(request.FromSequence), requestHash)[:48]
	now := request.OccurredAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	attempt := contracts.FederationPollAttempt{SchemaVersion: contracts.FederationSchemaVersion, ID: attemptID, WorkspaceID: request.WorkspaceID, PeerID: request.PeerID, RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey, RequestHash: requestHash, FromSequence: request.FromSequence, State: contracts.FederationPollReserved, OwnerID: request.OwnerID, Fence: request.Fence, Actor: request.Actor, CausationID: request.CausationID, CorrelationID: request.CorrelationID, StartedAt: now, UpdatedAt: now}
	if err := attempt.Normalize(); err != nil {
		return contracts.FederationPollAttempt{}, false, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.FederationPollAttempt{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := validatePeerLeaseTx(ctx, tx, lease); err != nil {
		return contracts.FederationPollAttempt{}, false, err
	}
	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.workspace_federation_poll_attempts(
			id,workspace_id,peer_id,schema_version,request_id,idempotency_key,request_hash,from_sequence,state,owner_id,fence,actor,causation_id,correlation_id,started_at,updated_at,retention_class,retention_state,retention_deadline)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14,$15,$15,'operational','active',$15::timestamptz + INTERVAL '30 days')
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING
		RETURNING id`, attempt.ID, attempt.WorkspaceID, attempt.PeerID, attempt.SchemaVersion, attempt.RequestID, attempt.IdempotencyKey,
		attempt.RequestHash, attempt.FromSequence, attempt.State, attempt.OwnerID, attempt.Fence, mustJSON(attempt.Actor), attempt.CausationID, attempt.CorrelationID, attempt.StartedAt).Scan(&insertedID)
	if errors.Is(err, pgx.ErrNoRows) {
		var existingHash string
		if err := tx.QueryRow(ctx, `SELECT request_hash FROM fornix.workspace_federation_poll_attempts WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, attempt.WorkspaceID, attempt.IdempotencyKey).Scan(&existingHash); err != nil {
			return contracts.FederationPollAttempt{}, false, err
		}
		if existingHash != attempt.RequestHash {
			return contracts.FederationPollAttempt{}, false, ErrFederationPollConflict
		}
		existing, readErr := readFederationPollAttempt(ctx, tx, attempt.WorkspaceID, attempt.IdempotencyKey)
		if readErr != nil {
			return contracts.FederationPollAttempt{}, false, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.FederationPollAttempt{}, false, err
		}
		return existing, true, nil
	}
	if err != nil {
		return contracts.FederationPollAttempt{}, false, err
	}
	payload, _ := json.Marshal(attempt)
	event := contracts.EventEnvelope{EventID: contracts.NewID("evt-federation-poll"), EventType: contracts.FederationEventPoll, SchemaVersion: contracts.EventSchemaVersion, OccurredAt: attempt.StartedAt, Scope: contracts.Scope{WorkspaceID: attempt.WorkspaceID, Subject: attempt.ID}, Actor: attempt.Actor, CausationID: attempt.CausationID, CorrelationID: attempt.CorrelationID, IdempotencyKey: "federation-poll:" + attempt.WorkspaceID + ":" + attempt.IdempotencyKey, Payload: payload}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.FederationPollAttempt{}, false, err
	}
	if err := s.fail("federation_poll_reserved"); err != nil {
		return contracts.FederationPollAttempt{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationPollAttempt{}, false, err
	}
	return attempt, false, nil
}

func (s *FederationStore) MarkPollDispatching(ctx context.Context, attempt contracts.FederationPollAttempt, lease contracts.FederationPeerLease, credential credentials.Lease) (contracts.FederationPollAttempt, error) {
	if err := credential.Normalize(); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if !federationAttemptMatchesLease(attempt, lease) {
		return contracts.FederationPollAttempt{}, ErrFederationPeerLeaseFenced
	}
	if err := s.ValidatePeerLease(ctx, lease); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if s.requireExternalBoundary && attempt.ExternalBoundary == nil {
		return contracts.FederationPollAttempt{}, ErrFederationExternalBoundaryRequired
	}
	egressHash, destinationHash, networkBoundary, networkHash, err := boundaryColumns(attempt.ExternalBoundary)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	var updated contracts.FederationPollAttempt
	tx, err := beginWorkspaceTx(ctx, s.pool, attempt.WorkspaceID)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := validatePeerLeaseTx(ctx, tx, lease); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	var actorJSON []byte
	err = tx.QueryRow(ctx, `
		UPDATE fornix.workspace_federation_poll_attempts
		SET state='dispatching',credential_lease_id=$1,credential_lease_fence=$2,credential_revocation_epoch=$3,credential_source_version=$4,egress_policy_hash=$5,destination_policy_hash=$6,network_boundary=$7,network_boundary_hash=$8,updated_at=clock_timestamp()
		WHERE workspace_id=$9 AND id=$10 AND peer_id=$11 AND owner_id=$12 AND fence=$13 AND state IN ('reserved','recovery_required')
		RETURNING id,workspace_id,peer_id,schema_version,request_id,idempotency_key,request_hash,from_sequence,state,owner_id,fence,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,provider_request_id,response_hash,imported_count,failure_code,actor,causation_id,correlation_id,started_at,updated_at,completed_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash`, credential.LeaseID, credential.Fence, credential.RevocationEpoch, credential.SourceVersion, egressHash, destinationHash, networkBoundary, networkHash, attempt.WorkspaceID, attempt.ID, attempt.PeerID, lease.OwnerID, lease.Fence).Scan(
		&updated.ID, &updated.WorkspaceID, &updated.PeerID, &updated.SchemaVersion, &updated.RequestID, &updated.IdempotencyKey, &updated.RequestHash, &updated.FromSequence, &updated.State, &updated.OwnerID, &updated.Fence, &updated.CredentialLeaseID, &updated.CredentialLeaseFence, &updated.CredentialRevocationEpoch, &updated.CredentialSourceVersion, &updated.ProviderRequestID, &updated.ResponseHash, &updated.ImportedCount, &updated.FailureCode, &actorJSON, &updated.CausationID, &updated.CorrelationID, &updated.StartedAt, &updated.UpdatedAt, &updated.CompletedAt, &egressHash, &destinationHash, &networkBoundary, &networkHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.FederationPollAttempt{}, ErrFederationPeerLeaseFenced
	}
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if err := json.Unmarshal(actorJSON, &updated.Actor); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	updated.ExternalBoundary, err = boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if err := s.fail("federation_poll_dispatching"); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	return updated, nil
}

// ReclaimPollAttempt transfers a non-terminal recovery attempt to the current
// peer lease after expiry/takeover. The new fence must be strictly greater
// than the attempt fence, so a stale worker cannot reclaim its own work or
// overwrite a newer owner. This is a local transaction only.
func (s *FederationStore) ReclaimPollAttempt(ctx context.Context, attempt contracts.FederationPollAttempt, lease contracts.FederationPeerLease, actor contracts.ActorRef) (contracts.FederationPollAttempt, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.FederationPollAttempt{}, fmt.Errorf("federation store is not configured")
	}
	if attempt.WorkspaceID != lease.WorkspaceID || attempt.PeerID != lease.PeerID || lease.Fence <= attempt.Fence || strings.TrimSpace(lease.OwnerID) == "" {
		return contracts.FederationPollAttempt{}, ErrFederationPeerLeaseFenced
	}
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(actor.WorkspaceID) != attempt.WorkspaceID {
		actor = attempt.Actor
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, attempt.WorkspaceID)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := validatePeerLeaseTx(ctx, tx, lease); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM fornix.workspace_federation_poll_attempts WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, attempt.WorkspaceID, attempt.ID).Scan(&state); errors.Is(err, pgx.ErrNoRows) {
		return contracts.FederationPollAttempt{}, ErrFederationPeerLeaseFenced
	} else if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if state == contracts.FederationPollSucceeded || state == contracts.FederationPollFailed {
		return contracts.FederationPollAttempt{}, ErrFederationPollTerminal
	}
	var actorJSON []byte
	var egressHash, destinationHash, networkBoundary, networkHash string
	err = tx.QueryRow(ctx, `
			UPDATE fornix.workspace_federation_poll_attempts
		SET owner_id=$1,fence=$2,state='recovery_required',failure_code='takeover_recovery',actor=$3::jsonb,updated_at=clock_timestamp()
		WHERE workspace_id=$4 AND id=$5 AND fence<$2 AND state IN ('reserved','dispatching','recovery_required')
		RETURNING id,workspace_id,peer_id,schema_version,request_id,idempotency_key,request_hash,from_sequence,state,owner_id,fence,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,provider_request_id,response_hash,imported_count,failure_code,actor,causation_id,correlation_id,started_at,updated_at,completed_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash`, lease.OwnerID, lease.Fence, mustJSON(actor), attempt.WorkspaceID, attempt.ID).Scan(
		&attempt.ID, &attempt.WorkspaceID, &attempt.PeerID, &attempt.SchemaVersion, &attempt.RequestID, &attempt.IdempotencyKey, &attempt.RequestHash, &attempt.FromSequence, &attempt.State, &attempt.OwnerID, &attempt.Fence, &attempt.CredentialLeaseID, &attempt.CredentialLeaseFence, &attempt.CredentialRevocationEpoch, &attempt.CredentialSourceVersion, &attempt.ProviderRequestID, &attempt.ResponseHash, &attempt.ImportedCount, &attempt.FailureCode, &actorJSON, &attempt.CausationID, &attempt.CorrelationID, &attempt.StartedAt, &attempt.UpdatedAt, &attempt.CompletedAt, &egressHash, &destinationHash, &networkBoundary, &networkHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.FederationPollAttempt{}, ErrFederationPeerLeaseFenced
	}
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if err := json.Unmarshal(actorJSON, &attempt.Actor); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	attempt.ExternalBoundary, err = boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	payload := mustJSON(map[string]any{"attempt_id": attempt.ID, "state": attempt.State, "fence": attempt.Fence, "previous_fence": attempt.Fence - 1, "reason": "lease_takeover"})
	event := contracts.EventEnvelope{EventID: contracts.NewID("evt-federation-poll-reclaim"), EventType: contracts.FederationEventPoll, SchemaVersion: contracts.EventSchemaVersion, OccurredAt: attempt.UpdatedAt, Scope: contracts.Scope{WorkspaceID: attempt.WorkspaceID, Subject: attempt.ID}, Actor: attempt.Actor, CausationID: attempt.CausationID, CorrelationID: attempt.CorrelationID, IdempotencyKey: "federation-poll-reclaim:" + attempt.WorkspaceID + ":" + attempt.ID + ":" + fmt.Sprint(attempt.Fence), Payload: payload}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	return attempt, nil
}

// CompletePoll imports bounded messages and advances the attempt atomically.
// Message idempotency means a crash after import and before replay does not
// create a second coordination effect.
func (s *FederationStore) CompletePoll(ctx context.Context, attempt contracts.FederationPollAttempt, lease contracts.FederationPeerLease, messages []contracts.CoordinationMessage, responseHash, providerRequestID string) (contracts.FederationPollAttempt, int, error) {
	return s.completePoll(ctx, attempt, lease, messages, responseHash, providerRequestID, false)
}

// ReconcilePoll finalizes only a previously recorded recovery-required
// response. It performs no network or credential-manager work. The expected
// response hash and provider request identity are checked inside the same
// transaction that imports messages and advances the attempt, so an operator
// cannot race a takeover or turn an unknown response into success.
func (s *FederationStore) ReconcilePoll(ctx context.Context, attempt contracts.FederationPollAttempt, lease contracts.FederationPeerLease, messages []contracts.CoordinationMessage, responseHash, providerRequestID string) (contracts.FederationPollAttempt, int, error) {
	return s.completePoll(ctx, attempt, lease, messages, responseHash, providerRequestID, true)
}

func (s *FederationStore) completePoll(ctx context.Context, attempt contracts.FederationPollAttempt, lease contracts.FederationPeerLease, messages []contracts.CoordinationMessage, responseHash, providerRequestID string, recoveryOnly bool) (contracts.FederationPollAttempt, int, error) {
	if s == nil || s.pool == nil || s.events == nil || s.coordination == nil {
		return contracts.FederationPollAttempt{}, 0, fmt.Errorf("federation store is not configured")
	}
	if len(messages) > contracts.MaxFederationPollMessages {
		return contracts.FederationPollAttempt{}, 0, fmt.Errorf("federation response exceeds message budget")
	}
	responseHash = strings.ToLower(strings.TrimSpace(responseHash))
	if !isFederationSHA256(responseHash) {
		return contracts.FederationPollAttempt{}, 0, fmt.Errorf("federation response hash is invalid")
	}
	if !federationAttemptMatchesLease(attempt, lease) {
		return contracts.FederationPollAttempt{}, 0, ErrFederationPeerLeaseFenced
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, attempt.WorkspaceID)
	if err != nil {
		return contracts.FederationPollAttempt{}, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := validatePeerLeaseTx(ctx, tx, lease); err != nil {
		return contracts.FederationPollAttempt{}, 0, err
	}
	var state, recordedResponseHash, recordedProviderRequestID string
	if err := tx.QueryRow(ctx, `SELECT state,response_hash,provider_request_id FROM fornix.workspace_federation_poll_attempts WHERE workspace_id=$1 AND id=$2 AND owner_id=$3 AND fence=$4 FOR UPDATE`, attempt.WorkspaceID, attempt.ID, lease.OwnerID, lease.Fence).Scan(&state, &recordedResponseHash, &recordedProviderRequestID); errors.Is(err, pgx.ErrNoRows) {
		return contracts.FederationPollAttempt{}, 0, ErrFederationPeerLeaseFenced
	} else if err != nil {
		return contracts.FederationPollAttempt{}, 0, err
	}
	if state == contracts.FederationPollSucceeded {
		if recoveryOnly && (recordedResponseHash != responseHash || strings.TrimSpace(recordedProviderRequestID) != strings.TrimSpace(providerRequestID)) {
			return contracts.FederationPollAttempt{}, 0, fmt.Errorf("federation recovery evidence does not match completed attempt")
		}
		stored, readErr := readFederationPollAttempt(ctx, tx, attempt.WorkspaceID, attempt.IdempotencyKey)
		if readErr != nil {
			return contracts.FederationPollAttempt{}, 0, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.FederationPollAttempt{}, 0, err
		}
		return stored, 0, nil
	}
	if recoveryOnly {
		if state != contracts.FederationPollRecoveryNeeded {
			return contracts.FederationPollAttempt{}, 0, ErrFederationPollTerminal
		}
		if recordedResponseHash == "" || recordedResponseHash != responseHash {
			return contracts.FederationPollAttempt{}, 0, fmt.Errorf("federation recovery response hash does not match recorded evidence")
		}
		if strings.TrimSpace(recordedProviderRequestID) != strings.TrimSpace(providerRequestID) {
			return contracts.FederationPollAttempt{}, 0, fmt.Errorf("federation recovery provider request identity does not match recorded evidence")
		}
	}
	if state != contracts.FederationPollDispatching && state != contracts.FederationPollRecoveryNeeded && state != contracts.FederationPollReserved {
		return contracts.FederationPollAttempt{}, 0, ErrFederationPollTerminal
	}
	imported := 0
	for i := range messages {
		stored, duplicate, appendErr := s.coordination.appendMessageTx(ctx, tx, messages[i])
		if appendErr != nil {
			return contracts.FederationPollAttempt{}, imported, appendErr
		}
		if !duplicate && stored.ID != "" {
			imported++
		}
	}
	var updated contracts.FederationPollAttempt
	var actorJSON []byte
	var egressHash, destinationHash, networkBoundary, networkHash string
	now := time.Now().UTC()
	err = tx.QueryRow(ctx, `
		UPDATE fornix.workspace_federation_poll_attempts
		SET state='succeeded',response_hash=$1,provider_request_id=$2,imported_count=imported_count+$3,failure_code='',updated_at=$4,completed_at=$4
		WHERE workspace_id=$5 AND id=$6 AND owner_id=$7 AND fence=$8
		RETURNING id,workspace_id,peer_id,schema_version,request_id,idempotency_key,request_hash,from_sequence,state,owner_id,fence,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,provider_request_id,response_hash,imported_count,failure_code,actor,causation_id,correlation_id,started_at,updated_at,completed_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash`, responseHash, strings.TrimSpace(providerRequestID), imported, now, attempt.WorkspaceID, attempt.ID, lease.OwnerID, lease.Fence).Scan(
		&updated.ID, &updated.WorkspaceID, &updated.PeerID, &updated.SchemaVersion, &updated.RequestID, &updated.IdempotencyKey, &updated.RequestHash, &updated.FromSequence, &updated.State, &updated.OwnerID, &updated.Fence, &updated.CredentialLeaseID, &updated.CredentialLeaseFence, &updated.CredentialRevocationEpoch, &updated.CredentialSourceVersion, &updated.ProviderRequestID, &updated.ResponseHash, &updated.ImportedCount, &updated.FailureCode, &actorJSON, &updated.CausationID, &updated.CorrelationID, &updated.StartedAt, &updated.UpdatedAt, &updated.CompletedAt, &egressHash, &destinationHash, &networkBoundary, &networkHash)
	if err != nil {
		return contracts.FederationPollAttempt{}, imported, err
	}
	if err := json.Unmarshal(actorJSON, &updated.Actor); err != nil {
		return contracts.FederationPollAttempt{}, imported, err
	}
	updated.ExternalBoundary, err = boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash)
	if err != nil {
		return contracts.FederationPollAttempt{}, imported, err
	}
	payload, _ := json.Marshal(map[string]any{"attempt_id": updated.ID, "state": updated.State, "response_hash": responseHash, "imported_count": imported, "provider_request_id": providerRequestID})
	event := contracts.EventEnvelope{EventID: contracts.NewID("evt-federation-poll-complete"), EventType: contracts.FederationEventPoll, SchemaVersion: contracts.EventSchemaVersion, OccurredAt: now, Scope: contracts.Scope{WorkspaceID: updated.WorkspaceID, Subject: updated.ID}, Actor: updated.Actor, CausationID: updated.CausationID, CorrelationID: updated.CorrelationID, IdempotencyKey: "federation-poll-complete:" + updated.WorkspaceID + ":" + updated.IdempotencyKey, Payload: payload}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.FederationPollAttempt{}, imported, err
	}
	if err := s.fail("federation_poll_before_commit"); err != nil {
		return contracts.FederationPollAttempt{}, imported, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationPollAttempt{}, imported, err
	}
	return updated, imported, nil
}

func (s *FederationStore) MarkPollRecoveryRequired(ctx context.Context, attempt contracts.FederationPollAttempt, lease contracts.FederationPeerLease, failureCode, providerRequestID, responseHash string) (contracts.FederationPollAttempt, error) {
	if !federationAttemptMatchesLease(attempt, lease) {
		return contracts.FederationPollAttempt{}, ErrFederationPeerLeaseFenced
	}
	failureCode = strings.TrimSpace(failureCode)
	if failureCode == "" || len(failureCode) > 128 {
		return contracts.FederationPollAttempt{}, fmt.Errorf("federation failure_code is required and bounded")
	}
	responseHash = strings.ToLower(strings.TrimSpace(responseHash))
	if responseHash != "" && (len(responseHash) != 64 || !isFederationSHA256(responseHash)) {
		return contracts.FederationPollAttempt{}, fmt.Errorf("federation response_hash is invalid")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, attempt.WorkspaceID)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := validatePeerLeaseTx(ctx, tx, lease); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	result, err := tx.Exec(ctx, `
		UPDATE fornix.workspace_federation_poll_attempts
		SET state='recovery_required',failure_code=$1,provider_request_id=$2,response_hash=$3,updated_at=clock_timestamp()
		WHERE workspace_id=$4 AND id=$5 AND peer_id=$6 AND owner_id=$7 AND fence=$8 AND state IN ('reserved','dispatching','recovery_required')`, failureCode, strings.TrimSpace(providerRequestID), strings.TrimSpace(responseHash), attempt.WorkspaceID, attempt.ID, attempt.PeerID, lease.OwnerID, lease.Fence)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if result.RowsAffected() == 0 {
		return contracts.FederationPollAttempt{}, ErrFederationPeerLeaseFenced
	}
	updated, err := readFederationPollAttempt(ctx, tx, attempt.WorkspaceID, attempt.ID)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	payload, _ := json.Marshal(map[string]any{"attempt_id": updated.ID, "state": updated.State, "failure_code": failureCode, "response_hash": responseHash, "provider_request_id": strings.TrimSpace(providerRequestID)})
	event := contracts.EventEnvelope{EventID: contracts.NewID("evt-federation-poll-recovery"), EventType: contracts.FederationEventPoll, SchemaVersion: contracts.EventSchemaVersion, OccurredAt: updated.UpdatedAt, Scope: contracts.Scope{WorkspaceID: updated.WorkspaceID, Subject: updated.ID}, Actor: updated.Actor, CausationID: updated.CausationID, CorrelationID: updated.CorrelationID, IdempotencyKey: "federation-poll-recovery:" + updated.WorkspaceID + ":" + updated.IdempotencyKey, Payload: payload}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if err := s.fail("federation_poll_recovery_before_commit"); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	return updated, nil
}

func (s *FederationStore) GetPollAttempt(ctx context.Context, workspaceID, attemptID string) (contracts.FederationPollAttempt, error) {
	if s == nil || s.pool == nil {
		return contracts.FederationPollAttempt{}, fmt.Errorf("federation store is not configured")
	}
	return readFederationPollAttempt(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(attemptID))
}

func normalizePollRequest(request *contracts.FederationPollRequest) error {
	if request == nil {
		return fmt.Errorf("federation poll request is nil")
	}
	if request.SchemaVersion == 0 {
		request.SchemaVersion = contracts.FederationSchemaVersion
	}
	if request.SchemaVersion != contracts.FederationSchemaVersion {
		return fmt.Errorf("unsupported federation poll schema_version %d", request.SchemaVersion)
	}
	workspace, err := normalizeFederationWorkspace(request.WorkspaceID)
	if err != nil {
		return err
	}
	for field, value := range map[string]*string{"request_id": &request.RequestID, "idempotency_key": &request.IdempotencyKey, "peer_id": &request.PeerID, "owner_id": &request.OwnerID} {
		normalized, itemErr := normalizeIdentifier(*value, field, contracts.MaxIdempotencyLength, true)
		if itemErr != nil {
			return itemErr
		}
		*value = normalized
	}
	if request.FromSequence < 0 || request.Fence == 0 {
		return fmt.Errorf("federation poll sequence and fence are invalid")
	}
	if err := normalizeActor(&request.Actor, workspace); err != nil {
		return err
	}
	request.WorkspaceID = workspace
	if request.OccurredAt.IsZero() {
		request.OccurredAt = time.Now().UTC()
	}
	request.OccurredAt = request.OccurredAt.UTC()
	return nil
}

// Small local wrappers keep the store package's request validation independent
// from contracts' intentionally unexported normalization helpers.
func normalizeFederationWorkspace(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > contracts.MaxDomainWorkspaceIDLength || strings.ContainsAny(value, "\r\n\t") {
		return "", fmt.Errorf("workspace_id is invalid")
	}
	return value, nil
}

func normalizeIdentifier(value, field string, max int, required bool) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		if required {
			return "", fmt.Errorf("%s is required", field)
		}
		return "", nil
	}
	if len(value) > max || strings.ContainsAny(value, " \t\r\n") {
		return "", fmt.Errorf("%s is invalid", field)
	}
	return value, nil
}

func normalizeActor(actor *contracts.ActorRef, workspace string) error {
	if actor == nil || strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(actor.Kind) == "" || strings.TrimSpace(actor.WorkspaceID) != workspace {
		return fmt.Errorf("actor is invalid or crosses workspace")
	}
	return nil
}

func validatePeerLeaseTx(ctx context.Context, tx pgx.Tx, lease contracts.FederationPeerLease) error {
	var exists bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM fornix.workspace_federation_peer_leases WHERE workspace_id=$1 AND peer_id=$2 AND owner_id=$3 AND fence=$4 AND expires_at>clock_timestamp())`, lease.WorkspaceID, lease.PeerID, lease.OwnerID, lease.Fence).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return ErrFederationPeerLeaseFenced
	}
	return nil
}

func isFederationSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func federationAttemptMatchesLease(attempt contracts.FederationPollAttempt, lease contracts.FederationPeerLease) bool {
	return attempt.WorkspaceID == lease.WorkspaceID && attempt.PeerID == lease.PeerID &&
		attempt.OwnerID == lease.OwnerID && attempt.Fence == lease.Fence
}

func readFederationPeer(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, peerID string) (contracts.FederationPeer, error) {
	var peer contracts.FederationPeer
	var actorJSON []byte
	err := queryer.QueryRow(ctx, `SELECT workspace_id,peer_id,schema_version,remote_workspace_id,endpoint_url,credential_ref,allow_private_networks,max_messages,max_response_bytes,timeout_ms,status,revision,config_hash,created_by,created_at,updated_at FROM fornix.workspace_federation_peers WHERE workspace_id=$1 AND peer_id=$2`, workspaceID, peerID).Scan(&peer.WorkspaceID, &peer.ID, &peer.SchemaVersion, &peer.RemoteWorkspaceID, &peer.EndpointURL, &peer.CredentialRef, &peer.AllowPrivateNetwork, &peer.MaxMessages, &peer.MaxResponseBytes, &peer.TimeoutMS, &peer.Status, &peer.Revision, &peer.ConfigHash, &actorJSON, &peer.CreatedAt, &peer.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.FederationPeer{}, ErrFederationPeerNotFound
	}
	if err != nil {
		return contracts.FederationPeer{}, err
	}
	if err := json.Unmarshal(actorJSON, &peer.CreatedBy); err != nil {
		return contracts.FederationPeer{}, err
	}
	if err := peer.Normalize(); err != nil {
		return contracts.FederationPeer{}, err
	}
	return peer, nil
}

func readFederationPollAttempt(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, attemptID string) (contracts.FederationPollAttempt, error) {
	var attempt contracts.FederationPollAttempt
	var actorJSON []byte
	var egressHash, destinationHash, networkBoundary, networkHash string
	err := queryer.QueryRow(ctx, `SELECT id,workspace_id,peer_id,schema_version,request_id,idempotency_key,request_hash,from_sequence,state,owner_id,fence,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,provider_request_id,response_hash,imported_count,failure_code,actor,causation_id,correlation_id,started_at,updated_at,completed_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash FROM fornix.workspace_federation_poll_attempts WHERE workspace_id=$1 AND (id=$2 OR idempotency_key=$2)`, workspaceID, attemptID).Scan(&attempt.ID, &attempt.WorkspaceID, &attempt.PeerID, &attempt.SchemaVersion, &attempt.RequestID, &attempt.IdempotencyKey, &attempt.RequestHash, &attempt.FromSequence, &attempt.State, &attempt.OwnerID, &attempt.Fence, &attempt.CredentialLeaseID, &attempt.CredentialLeaseFence, &attempt.CredentialRevocationEpoch, &attempt.CredentialSourceVersion, &attempt.ProviderRequestID, &attempt.ResponseHash, &attempt.ImportedCount, &attempt.FailureCode, &actorJSON, &attempt.CausationID, &attempt.CorrelationID, &attempt.StartedAt, &attempt.UpdatedAt, &attempt.CompletedAt, &egressHash, &destinationHash, &networkBoundary, &networkHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.FederationPollAttempt{}, ErrFederationPollConflict
	}
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if err := json.Unmarshal(actorJSON, &attempt.Actor); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	attempt.ExternalBoundary, err = boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if err := attempt.Normalize(); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	return attempt, nil
}

func scanFederationPeer(row interface{ Scan(...any) error }) (contracts.FederationPeer, error) {
	var peer contracts.FederationPeer
	var actorJSON []byte
	if err := row.Scan(&peer.WorkspaceID, &peer.ID, &peer.SchemaVersion, &peer.RemoteWorkspaceID, &peer.EndpointURL, &peer.CredentialRef, &peer.AllowPrivateNetwork, &peer.MaxMessages, &peer.MaxResponseBytes, &peer.TimeoutMS, &peer.Status, &peer.Revision, &peer.ConfigHash, &actorJSON, &peer.CreatedAt, &peer.UpdatedAt); err != nil {
		return contracts.FederationPeer{}, err
	}
	if err := json.Unmarshal(actorJSON, &peer.CreatedBy); err != nil {
		return contracts.FederationPeer{}, err
	}
	if err := peer.Normalize(); err != nil {
		return contracts.FederationPeer{}, err
	}
	return peer, nil
}
