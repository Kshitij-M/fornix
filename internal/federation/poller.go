// Package federation contains the bounded, workspace-scoped federation
// adapter. It is deliberately separate from HTTP handlers so the same
// authority boundary can be exercised by a worker, an operator command, or a
// future control-plane API.
package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	pathpkg "path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/credentials"
	"github.com/omaveda/fornix/internal/store"
)

var (
	// ErrRecoveryRequired is intentionally generic. Remote errors and response
	// bodies are not safe to place in API errors or durable records.
	ErrRecoveryRequired = errors.New("federation poll requires recovery")
	ErrPollUnavailable  = errors.New("federation poll authority is unavailable")
)

const (
	defaultPeerLeaseTTL = 30 * time.Second
	credentialLeaseTTL  = 2 * time.Minute
	maxProviderIDBytes  = 128
)

// Poller executes one peer read only after durable peer ownership, credential
// lease, and controlled egress have all been established. A nil Credential
// resolver is a safe configuration error; it never falls back to an inline
// bearer token or ambient environment variable.
type Poller struct {
	Peers       *store.FederationStore
	Credentials credentials.LeaseResolver
	Client      *http.Client
	Resolver    connector.IPResolver
	Now         func() time.Time
}

// Reconcile finalizes a response already received by an external adapter.
// The payload is bounded, hashed, decoded, and discarded in memory. This
// method acquires only the local peer lease and never resolves a credential or
// performs a network request.
func (p *Poller) Reconcile(ctx context.Context, request contracts.FederationPollReconcileRequest) (contracts.FederationPollAttempt, int, error) {
	if p == nil || p.Peers == nil {
		return contracts.FederationPollAttempt{}, 0, ErrPollUnavailable
	}
	if err := request.Normalize(); err != nil {
		return contracts.FederationPollAttempt{}, 0, err
	}
	attempt, err := p.Peers.GetPollAttempt(ctx, request.WorkspaceID, request.AttemptID)
	if err != nil {
		return contracts.FederationPollAttempt{}, 0, err
	}
	peer, err := p.Peers.GetPeer(ctx, request.WorkspaceID, attempt.PeerID)
	if err != nil {
		return contracts.FederationPollAttempt{}, 0, err
	}
	if request.OwnerID == "" {
		request.OwnerID = request.Actor.ID
	}
	lease, err := p.Peers.AcquirePeerLease(ctx, request.WorkspaceID, attempt.PeerID, request.OwnerID, defaultPeerLeaseTTL)
	if err != nil {
		return contracts.FederationPollAttempt{}, 0, err
	}
	if request.Fence != 0 && lease.Fence != request.Fence {
		return contracts.FederationPollAttempt{}, 0, store.ErrFederationPeerLeaseFenced
	}
	if !attemptMatchesPeerLease(attempt, lease) {
		attempt, err = p.Peers.ReclaimPollAttempt(ctx, attempt, lease, request.Actor)
		if err != nil {
			return contracts.FederationPollAttempt{}, 0, err
		}
	}
	if attempt.ResponseHash == "" {
		return contracts.FederationPollAttempt{}, 0, fmt.Errorf("federation attempt has no recorded response evidence")
	}
	if int64(len(request.ResponsePayload)) > peer.MaxResponseBytes {
		return contracts.FederationPollAttempt{}, 0, connector.ErrEgressResponse
	}
	responseHash := sha256Hex(request.ResponsePayload)
	if request.ResponseHash != "" && request.ResponseHash != responseHash {
		return contracts.FederationPollAttempt{}, 0, fmt.Errorf("federation reconcile response hash does not match payload")
	}
	var envelope struct {
		Messages []contracts.CoordinationMessage `json:"messages"`
	}
	if err := json.Unmarshal(request.ResponsePayload, &envelope); err != nil || len(envelope.Messages) > peer.MaxMessages {
		return contracts.FederationPollAttempt{}, 0, fmt.Errorf("federation reconcile response payload is invalid")
	}
	pollRequest := contracts.FederationPollRequest{SchemaVersion: contracts.FederationSchemaVersion, WorkspaceID: request.WorkspaceID, PeerID: attempt.PeerID, FromSequence: attempt.FromSequence, OwnerID: lease.OwnerID, Fence: lease.Fence, Actor: request.Actor, CorrelationID: request.CorrelationID}
	messages, err := normalizeImportedMessages(peer, pollRequest, attempt, envelope.Messages)
	if err != nil {
		return contracts.FederationPollAttempt{}, 0, err
	}
	completed, imported, err := p.Peers.ReconcilePoll(ctx, attempt, lease, messages, responseHash, request.ProviderRequestID)
	if err != nil {
		return contracts.FederationPollAttempt{}, imported, err
	}
	return completed, imported, nil
}

// Poll performs one bounded read and atomically imports the remote messages
// into the local workspace coordination authority. A retry after an uncertain
// external call is at-least-once at the network boundary but exactly-once at
// the local durable message identity.
func (p *Poller) Poll(ctx context.Context, request contracts.FederationPollRequest) (contracts.FederationPollAttempt, error) {
	if p == nil || p.Peers == nil || p.Credentials == nil {
		return contracts.FederationPollAttempt{}, ErrPollUnavailable
	}
	now := p.now()
	peer, err := p.Peers.GetPeer(ctx, request.WorkspaceID, request.PeerID)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if request.WorkspaceID == "" {
		request.WorkspaceID = peer.WorkspaceID
	}
	if request.PeerID == "" {
		request.PeerID = peer.ID
	}
	if request.OwnerID == "" {
		return contracts.FederationPollAttempt{}, fmt.Errorf("federation poll owner_id is required")
	}

	lease, releaseLease, err := p.peerLease(ctx, request, now)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	defer func() {
		if releaseLease {
			_ = p.Peers.ReleasePeerLease(context.Background(), lease)
		}
	}()
	request.Fence = lease.Fence

	attempt, duplicate, err := p.Peers.BeginPoll(ctx, request, lease)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	if duplicate {
		switch attempt.State {
		case contracts.FederationPollSucceeded:
			return attempt, nil
		case contracts.FederationPollDispatching:
			if !attemptMatchesPeerLease(attempt, lease) {
				attempt, err = p.Peers.ReclaimPollAttempt(ctx, attempt, lease, request.Actor)
				if err != nil {
					return contracts.FederationPollAttempt{}, err
				}
			}
			// A prior worker may have crashed after the remote call began. Mark
			// the old dispatch uncertain before allowing a bounded retry.
			attempt, err = p.Peers.MarkPollRecoveryRequired(ctx, attempt, lease, "remote_uncertain", attempt.ProviderRequestID, attempt.ResponseHash)
			if err != nil {
				return contracts.FederationPollAttempt{}, err
			}
		case contracts.FederationPollReserved, contracts.FederationPollRecoveryNeeded:
			if !attemptMatchesPeerLease(attempt, lease) {
				attempt, err = p.Peers.ReclaimPollAttempt(ctx, attempt, lease, request.Actor)
				if err != nil {
					return contracts.FederationPollAttempt{}, err
				}
			}
		default:
			return contracts.FederationPollAttempt{}, fmt.Errorf("%w: unsupported poll state", ErrRecoveryRequired)
		}
	}

	ref, err := credentials.ParseRef(peer.CredentialRef)
	if err != nil {
		return p.recover(ctx, attempt, lease, "credential_reference", "", "")
	}
	provider := strings.SplitN(ref.String(), "/", 2)[0]
	if provider == "" || len(provider) > maxProviderIDBytes {
		return p.recover(ctx, attempt, lease, "credential_reference", "", "")
	}
	purpose := "federation:" + provider
	credentialLease, err := p.Credentials.Acquire(ctx, request.WorkspaceID, ref.String(), purpose, credentialLeaseTTL)
	if err != nil {
		credentialLease.Secret.Clear()
		return p.recover(ctx, attempt, lease, "credential_unavailable", "", "")
	}
	defer func() {
		_ = credentials.ReleaseLeaseBounded(p.Credentials, credentialLease)
	}()
	if err := credentialLease.Validate(request.WorkspaceID, ref.String(), purpose, p.now()); err != nil {
		return p.recover(ctx, attempt, lease, "credential_invalid", "", "")
	}
	if err := p.Credentials.ValidateLease(ctx, credentialLease); err != nil {
		return p.recover(ctx, attempt, lease, "credential_fenced", "", "")
	}
	client, requestURL, boundary, err := p.egressClient(peer, request.FromSequence)
	if err != nil {
		return p.recover(ctx, attempt, lease, "egress_policy", "", "")
	}
	attempt.ExternalBoundary = &boundary
	attempt, err = p.Peers.MarkPollDispatching(ctx, attempt, lease, credentialLease)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	// Revalidate after the dispatch marker is durable and immediately before
	// attaching secret bytes to the outbound request.
	if err := p.Credentials.ValidateLease(ctx, credentialLease); err != nil {
		return p.recover(ctx, attempt, lease, "credential_fenced", "", "")
	}
	secretBytes := credentialLease.Secret.Bytes()
	defer clearBytes(secretBytes)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return p.recover(ctx, attempt, lease, "request_build", "", "")
	}
	httpRequest.Header.Set("Authorization", "Bearer "+string(secretBytes))
	httpRequest.Header.Set("X-Workspace-ID", peer.RemoteWorkspaceID)
	httpRequest.Header.Set("Accept", "application/json")
	// Construction can consume enough time for revocation or expiry to occur.
	// Revalidate after the header is attached and immediately before network I/O.
	if err := p.Credentials.ValidateLease(ctx, credentialLease); err != nil {
		return p.recover(ctx, attempt, lease, "credential_fenced", "", "")
	}
	response, err := credentials.DoCredentialRequest(client, httpRequest)
	if err != nil {
		return p.recover(ctx, attempt, lease, "remote_uncertain", "", "")
	}
	body, readErr := boundedResponse(response, peer.MaxResponseBytes)
	providerRequestID := boundedHeader(response.Header.Get("X-Request-ID"))
	_ = response.Body.Close()
	responseHash := sha256Hex(body)
	if readErr != nil || response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return p.recover(ctx, attempt, lease, "remote_response", providerRequestID, responseHash)
	}
	var envelope struct {
		Messages []contracts.CoordinationMessage `json:"messages"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Messages) > peer.MaxMessages {
		return p.recover(ctx, attempt, lease, "remote_payload", providerRequestID, responseHash)
	}
	messages, err := normalizeImportedMessages(peer, request, attempt, envelope.Messages)
	if err != nil {
		return p.recover(ctx, attempt, lease, "remote_payload", providerRequestID, responseHash)
	}
	if err := p.Credentials.ValidateLease(ctx, credentialLease); err != nil {
		return p.recover(ctx, attempt, lease, "credential_fenced", providerRequestID, responseHash)
	}
	if err := p.Peers.ValidatePeerLease(ctx, lease); err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	completed, _, err := p.Peers.CompletePoll(ctx, attempt, lease, messages, responseHash, providerRequestID)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	return completed, nil
}

func (p *Poller) peerLease(ctx context.Context, request contracts.FederationPollRequest, now time.Time) (contracts.FederationPeerLease, bool, error) {
	if request.Fence != 0 {
		lease := contracts.FederationPeerLease{WorkspaceID: request.WorkspaceID, PeerID: request.PeerID, OwnerID: request.OwnerID, Fence: request.Fence}
		if err := p.Peers.ValidatePeerLease(ctx, lease); err != nil {
			return contracts.FederationPeerLease{}, false, err
		}
		return lease, false, nil
	}
	lease, err := p.Peers.AcquirePeerLease(ctx, request.WorkspaceID, request.PeerID, request.OwnerID, defaultPeerLeaseTTL)
	if err != nil {
		return contracts.FederationPeerLease{}, false, err
	}
	if !lease.ExpiresAt.After(now) {
		return contracts.FederationPeerLease{}, false, fmt.Errorf("%w: peer lease expired", ErrRecoveryRequired)
	}
	return lease, true, nil
}

func (p *Poller) egressClient(peer contracts.FederationPeer, fromSequence int64) (*http.Client, string, contracts.ExternalBoundaryAuthority, error) {
	parsed, err := url.Parse(peer.EndpointURL)
	if err != nil || parsed.Hostname() == "" {
		return nil, "", contracts.ExternalBoundaryAuthority{}, connector.ErrDestinationHost
	}
	basePath := parsed.EscapedPath()
	if basePath == "" {
		basePath = "/"
	}
	endpointPath := pathpkg.Join(basePath, "/v1/coord/recent")
	query := url.Values{}
	query.Set("after_sequence", strconv.FormatInt(fromSequence, 10))
	query.Set("limit", strconv.Itoa(peer.MaxMessages))
	parsed.Path, parsed.RawPath, parsed.RawQuery = endpointPath, "", query.Encode()
	policy := connector.EgressPolicy{
		Destination: connector.DestinationPolicy{
			AllowedSchemes:       []string{strings.ToLower(parsed.Scheme)},
			AllowedHosts:         []string{strings.ToLower(parsed.Hostname())},
			AllowedPathPrefixes:  []string{basePath},
			AllowPrivateNetworks: peer.AllowPrivateNetwork,
			AllowRedirects:       false,
		},
		MaxRequestBytes:  1,
		MaxResponseBytes: peer.MaxResponseBytes,
		Timeout:          time.Duration(peer.TimeoutMS) * time.Millisecond,
	}
	client, err := connector.NewEgressClient(policy, p.Client, connector.EgressOptions{Resolver: p.Resolver})
	if err != nil {
		return nil, "", contracts.ExternalBoundaryAuthority{}, err
	}
	boundary, err := connector.BoundaryAuthority(policy)
	if err != nil {
		return nil, "", contracts.ExternalBoundaryAuthority{}, err
	}
	return client, parsed.String(), boundary, nil
}

func (p *Poller) recover(ctx context.Context, attempt contracts.FederationPollAttempt, lease contracts.FederationPeerLease, code, providerRequestID, responseHash string) (contracts.FederationPollAttempt, error) {
	updated, err := p.Peers.MarkPollRecoveryRequired(ctx, attempt, lease, code, providerRequestID, responseHash)
	if err != nil {
		return contracts.FederationPollAttempt{}, err
	}
	return updated, fmt.Errorf("%w: %s", ErrRecoveryRequired, code)
}

func normalizeImportedMessages(peer contracts.FederationPeer, request contracts.FederationPollRequest, attempt contracts.FederationPollAttempt, remote []contracts.CoordinationMessage) ([]contracts.CoordinationMessage, error) {
	ordered := append([]contracts.CoordinationMessage(nil), remote...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Sequence < ordered[j].Sequence })
	seen := make(map[int64]string, len(ordered))
	messages := make([]contracts.CoordinationMessage, 0, len(ordered))
	for _, source := range ordered {
		if source.Sequence <= request.FromSequence || source.Sequence <= 0 {
			return nil, fmt.Errorf("remote sequence is invalid")
		}
		fingerprint := source.StableHash()
		if existing, exists := seen[source.Sequence]; exists {
			if existing != fingerprint {
				return nil, fmt.Errorf("remote sequence has contradictory duplicate")
			}
			continue
		}
		seen[source.Sequence] = fingerprint
		id := "federation-" + contracts.HashStrings(peer.ID, strconv.FormatInt(source.Sequence, 10), source.RequestHash)[:48]
		message := contracts.CoordinationMessage{
			SchemaVersion:  contracts.WorkspaceCoordinationSchemaVersion,
			ID:             id,
			WorkspaceID:    request.WorkspaceID,
			RequestID:      "federation:" + peer.ID + ":" + strconv.FormatInt(source.Sequence, 10),
			IdempotencyKey: "federation:" + peer.ID + ":" + strconv.FormatInt(source.Sequence, 10),
			Sender:         source.Sender,
			Recipient:      source.Recipient,
			Subject:        source.Subject,
			Body:           source.Body,
			Actor:          request.Actor,
			CausationID:    attempt.ID,
			CorrelationID:  request.CorrelationID,
			OriginHost:     peer.RemoteWorkspaceID,
			OccurredAt:     source.OccurredAt,
		}
		if message.OccurredAt.IsZero() {
			message.OccurredAt = attempt.StartedAt
		}
		if err := message.Normalize(); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func boundedResponse(response *http.Response, maxBytes int64) ([]byte, error) {
	if response == nil || response.Body == nil || maxBytes <= 0 {
		return nil, connector.ErrEgressResponse
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return nil, connector.ErrEgressResponse
	}
	if int64(len(body)) > maxBytes {
		return nil, connector.ErrEgressResponse
	}
	return body, nil
}

func sha256Hex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func boundedHeader(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 256 {
		return value[:256]
	}
	return value
}

func clearBytes(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

func (p *Poller) now() time.Time {
	if p != nil && p.Now != nil {
		return p.Now().UTC()
	}
	return time.Now().UTC()
}

func attemptMatchesPeerLease(attempt contracts.FederationPollAttempt, lease contracts.FederationPeerLease) bool {
	return attempt.WorkspaceID == lease.WorkspaceID && attempt.PeerID == lease.PeerID && attempt.OwnerID == lease.OwnerID && attempt.Fence == lease.Fence
}
