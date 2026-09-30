package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	pathpkg "path"
	"strings"
	"time"
)

// FederationSchemaVersion versions the workspace-scoped peer and poll
// authority. Historical global federation rows are not part of this schema.
const FederationSchemaVersion = 1

const (
	FederationPeerActive   = "active"
	FederationPeerDisabled = "disabled"

	FederationPollReserved       = "reserved"
	FederationPollDispatching    = "dispatching"
	FederationPollSucceeded      = "succeeded"
	FederationPollFailed         = "failed"
	FederationPollRecoveryNeeded = "recovery_required"

	FederationEventPeerCommand = "federation.peer_command_recorded"
	FederationEventPoll        = "federation.poll_recorded"

	MaxFederationPeerURLBytes       = 2048
	MaxFederationCredentialRefBytes = 128
	MaxFederationPollMessages       = 1000
	MaxFederationPollResponseBytes  = 16 << 20
	MaxFederationPollTimeout        = 2 * time.Minute
)

// FederationPeer is the current, redacted configuration projection for one
// remote Fornix workspace. CredentialRef is an identity only; secret bytes
// are resolved by a managed credential authority at the egress boundary.
type FederationPeer struct {
	SchemaVersion       int       `json:"schema_version"`
	ID                  string    `json:"id"`
	WorkspaceID         string    `json:"workspace_id"`
	RemoteWorkspaceID   string    `json:"remote_workspace_id"`
	EndpointURL         string    `json:"endpoint_url"`
	CredentialRef       string    `json:"credential_ref"`
	AllowPrivateNetwork bool      `json:"allow_private_networks,omitempty"`
	MaxMessages         int       `json:"max_messages"`
	MaxResponseBytes    int64     `json:"max_response_bytes"`
	TimeoutMS           int64     `json:"timeout_ms"`
	Status              string    `json:"status"`
	Revision            int64     `json:"revision"`
	ConfigHash          string    `json:"config_hash"`
	CreatedBy           ActorRef  `json:"created_by"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// FederationPeerCommand is an idempotent, append-only peer configuration
// command. The current peer projection may advance; command history never is
// overwritten.
type FederationPeerCommand struct {
	SchemaVersion  int               `json:"schema_version"`
	RequestID      string            `json:"request_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	WorkspaceID    string            `json:"workspace_id"`
	Peer           FederationPeer    `json:"peer"`
	Actor          ActorRef          `json:"actor"`
	CausationID    string            `json:"causation_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// FederationPeerLease is the durable owner/fence used by a poll attempt.
// Fence increases on takeover and is required for every attempt mutation.
type FederationPeerLease struct {
	WorkspaceID string    `json:"workspace_id"`
	PeerID      string    `json:"peer_id"`
	OwnerID     string    `json:"owner_id"`
	Fence       uint64    `json:"fence"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// FederationPollAttempt is the redacted lifecycle record for one bounded
// remote read. Response bodies are never stored; ResponseHash is sufficient
// to compare replayed observations without creating a second evidence store.
type FederationPollAttempt struct {
	SchemaVersion             int    `json:"schema_version"`
	ID                        string `json:"id"`
	WorkspaceID               string `json:"workspace_id"`
	PeerID                    string `json:"peer_id"`
	RequestID                 string `json:"request_id"`
	IdempotencyKey            string `json:"idempotency_key"`
	RequestHash               string `json:"request_hash"`
	FromSequence              int64  `json:"from_sequence"`
	State                     string `json:"state"`
	OwnerID                   string `json:"owner_id"`
	Fence                     uint64 `json:"fence"`
	CredentialLeaseID         string `json:"credential_lease_id,omitempty"`
	CredentialLeaseFence      uint64 `json:"credential_lease_fence,omitempty"`
	CredentialRevocationEpoch uint64 `json:"credential_revocation_epoch,omitempty"`
	CredentialSourceVersion   string `json:"credential_source_version,omitempty"`
	// ExternalBoundary is the hash-only egress envelope used for the peer
	// read. The peer poll remains at-least-once at the network boundary.
	ExternalBoundary  *ExternalBoundaryAuthority `json:"external_boundary,omitempty"`
	ProviderRequestID string                     `json:"provider_request_id,omitempty"`
	ResponseHash      string                     `json:"response_hash,omitempty"`
	ImportedCount     int                        `json:"imported_count"`
	FailureCode       string                     `json:"failure_code,omitempty"`
	StartedAt         time.Time                  `json:"started_at"`
	UpdatedAt         time.Time                  `json:"updated_at"`
	CompletedAt       *time.Time                 `json:"completed_at,omitempty"`
	Actor             ActorRef                   `json:"actor"`
	CausationID       string                     `json:"causation_id,omitempty"`
	CorrelationID     string                     `json:"correlation_id,omitempty"`
}

// FederationPollRequest is the explicit command for one peer read. The poll
// identity is derived from peer revision and starting sequence so retries are
// deterministic and can safely re-import already seen remote messages.
type FederationPollRequest struct {
	SchemaVersion  int       `json:"schema_version"`
	RequestID      string    `json:"request_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	WorkspaceID    string    `json:"workspace_id"`
	PeerID         string    `json:"peer_id"`
	FromSequence   int64     `json:"from_sequence"`
	OwnerID        string    `json:"owner_id"`
	Fence          uint64    `json:"fence"`
	Actor          ActorRef  `json:"actor"`
	CausationID    string    `json:"causation_id,omitempty"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	OccurredAt     time.Time `json:"occurred_at"`
}

// FederationPollReconcileRequest supplies a bounded response envelope that a
// deployment already received. ResponsePayload is transient input only; it
// is hashed and decoded in memory and is never stored by the reconciliation
// authority. Reconciliation never performs network or credential work.
type FederationPollReconcileRequest struct {
	SchemaVersion     int             `json:"schema_version"`
	WorkspaceID       string          `json:"workspace_id"`
	AttemptID         string          `json:"attempt_id"`
	OwnerID           string          `json:"owner_id"`
	Fence             uint64          `json:"fence"`
	ResponseHash      string          `json:"response_hash,omitempty"`
	ProviderRequestID string          `json:"provider_request_id,omitempty"`
	ResponsePayload   json.RawMessage `json:"response_payload"`
	Actor             ActorRef        `json:"actor"`
	CausationID       string          `json:"causation_id,omitempty"`
	CorrelationID     string          `json:"correlation_id,omitempty"`
}

func (r *FederationPollReconcileRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("federation poll reconcile request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = FederationSchemaVersion
	}
	if r.SchemaVersion != FederationSchemaVersion {
		return fmt.Errorf("unsupported federation poll reconcile schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	for name, value := range map[string]*string{
		"attempt_id":          &r.AttemptID,
		"owner_id":            &r.OwnerID,
		"provider_request_id": &r.ProviderRequestID,
		"causation_id":        &r.CausationID,
		"correlation_id":      &r.CorrelationID,
	} {
		*value = strings.TrimSpace(*value)
		if name == "attempt_id" || name == "owner_id" {
			if *value == "" || len(*value) > MaxIdempotencyLength || strings.ContainsAny(*value, " \t\r\n") {
				return fmt.Errorf("federation reconcile %s is invalid", name)
			}
		}
		if len(*value) > MaxIdempotencyLength {
			return fmt.Errorf("federation reconcile %s is too large", name)
		}
	}
	if len(r.ResponsePayload) == 0 || len(r.ResponsePayload) > MaxFederationPollResponseBytes {
		return fmt.Errorf("federation reconcile response payload is empty or too large")
	}
	if r.ResponseHash != "" {
		r.ResponseHash = strings.ToLower(strings.TrimSpace(r.ResponseHash))
		if !isLowerHexHash(r.ResponseHash) {
			return fmt.Errorf("federation reconcile response_hash is invalid")
		}
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	r.WorkspaceID = workspace
	return nil
}

func (p *FederationPeer) Normalize() error {
	if p == nil {
		return fmt.Errorf("federation peer is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = FederationSchemaVersion
	}
	if p.SchemaVersion != FederationSchemaVersion {
		return fmt.Errorf("unsupported federation peer schema_version %d", p.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(p.WorkspaceID)
	if err != nil {
		return err
	}
	remoteWorkspace, err := normalizeDomainWorkspace(p.RemoteWorkspaceID)
	if err != nil {
		return fmt.Errorf("remote workspace: %w", err)
	}
	p.ID, err = normalizeDomainIdentifier(p.ID, "federation peer id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	parsed, err := url.Parse(strings.TrimSpace(p.EndpointURL))
	if err != nil || parsed.User != nil || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("federation endpoint_url must be an http(s) URL without credentials, query, or fragment")
	}
	cleanPath := pathpkg.Clean(parsed.Path)
	if cleanPath == "." || cleanPath == "" {
		cleanPath = "/"
	}
	if !strings.HasPrefix(cleanPath, "/") || strings.Contains(cleanPath, "..") {
		return fmt.Errorf("federation endpoint_url path is invalid")
	}
	parsed.Path, parsed.RawPath = cleanPath, ""
	p.EndpointURL = strings.TrimRight(parsed.String(), "/")
	if len(p.EndpointURL) == 0 || len(p.EndpointURL) > MaxFederationPeerURLBytes {
		return fmt.Errorf("federation endpoint_url is missing or too large")
	}
	p.CredentialRef = strings.TrimSpace(p.CredentialRef)
	if !validCredentialReference(p.CredentialRef) || len(p.CredentialRef) > MaxFederationCredentialRefBytes {
		return fmt.Errorf("federation credential_ref is invalid")
	}
	if p.MaxMessages == 0 {
		p.MaxMessages = 500
	}
	if p.MaxResponseBytes == 0 {
		p.MaxResponseBytes = 1 << 20
	}
	if p.TimeoutMS == 0 {
		p.TimeoutMS = 10_000
	}
	if p.MaxMessages < 1 || p.MaxMessages > MaxFederationPollMessages || p.MaxResponseBytes < 1 || p.MaxResponseBytes > MaxFederationPollResponseBytes || p.TimeoutMS < 1 || time.Duration(p.TimeoutMS)*time.Millisecond > MaxFederationPollTimeout {
		return fmt.Errorf("federation poll budgets are outside bounds")
	}
	if p.Status == "" {
		p.Status = FederationPeerActive
	}
	if p.Status != FederationPeerActive && p.Status != FederationPeerDisabled {
		return fmt.Errorf("invalid federation peer status %q", p.Status)
	}
	if p.Revision < 0 {
		return fmt.Errorf("federation peer revision cannot be negative")
	}
	if err := normalizeDomainActor(&p.CreatedBy, workspace); err != nil {
		return err
	}
	p.WorkspaceID, p.RemoteWorkspaceID = workspace, remoteWorkspace
	if !p.CreatedAt.IsZero() {
		p.CreatedAt = p.CreatedAt.UTC()
	}
	if !p.UpdatedAt.IsZero() {
		p.UpdatedAt = p.UpdatedAt.UTC()
	}
	hash := p.StableHash()
	if p.ConfigHash != "" && strings.ToLower(strings.TrimSpace(p.ConfigHash)) != hash {
		return fmt.Errorf("federation peer config_hash does not match configuration")
	}
	p.ConfigHash = hash
	return nil
}

func (p FederationPeer) StableHash() string {
	clone := p
	clone.Revision, clone.ConfigHash = 0, ""
	clone.CreatedAt, clone.UpdatedAt = time.Time{}, time.Time{}
	// CreatedBy is audit metadata, not peer behavior. A configuration hash
	// must remain stable when a later operator revises the same endpoint and
	// budget under a different authenticated actor.
	clone.CreatedBy = ActorRef{}
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (c *FederationPeerCommand) Normalize() error {
	if c == nil {
		return fmt.Errorf("federation peer command is nil")
	}
	if c.SchemaVersion == 0 {
		c.SchemaVersion = FederationSchemaVersion
	}
	if c.SchemaVersion != FederationSchemaVersion {
		return fmt.Errorf("unsupported federation peer command schema_version %d", c.SchemaVersion)
	}
	var err error
	if c.RequestID, err = normalizeDomainIdentifier(c.RequestID, "federation request_id", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if c.IdempotencyKey, err = normalizeDomainIdentifier(c.IdempotencyKey, "federation idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	workspace, err := normalizeDomainWorkspace(c.WorkspaceID)
	if err != nil {
		return err
	}
	if err := c.Peer.Normalize(); err != nil {
		return err
	}
	if c.Peer.WorkspaceID != workspace {
		return fmt.Errorf("federation peer command crosses workspace boundary")
	}
	if err := normalizeDomainActor(&c.Actor, workspace); err != nil {
		return err
	}
	if err := normalizeDomainMetadata(c.Metadata); err != nil {
		return err
	}
	c.WorkspaceID = workspace
	return nil
}

func (c FederationPeerCommand) RequestHash() string {
	clone := c
	clone.RequestID, clone.IdempotencyKey = "", ""
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (p FederationPollRequest) RequestHash() string {
	clone := p
	clone.RequestID, clone.IdempotencyKey, clone.OwnerID, clone.Fence, clone.OccurredAt = "", "", "", 0, time.Time{}
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (a *FederationPollAttempt) Normalize() error {
	if a == nil {
		return fmt.Errorf("federation poll attempt is nil")
	}
	if a.SchemaVersion == 0 {
		a.SchemaVersion = FederationSchemaVersion
	}
	if a.SchemaVersion != FederationSchemaVersion {
		return fmt.Errorf("unsupported federation poll schema_version %d", a.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(a.WorkspaceID)
	if err != nil {
		return err
	}
	for field, value := range map[string]*string{"poll id": &a.ID, "peer id": &a.PeerID, "request id": &a.RequestID, "idempotency key": &a.IdempotencyKey} {
		normalized, itemErr := normalizeDomainIdentifier(*value, "federation "+field, MaxIdempotencyLength, true)
		if itemErr != nil {
			return itemErr
		}
		*value = normalized
	}
	a.RequestHash, err = normalizeDomainHash(a.RequestHash, "federation poll request_hash", true)
	if err != nil {
		return err
	}
	if a.ResponseHash, err = normalizeDomainHash(a.ResponseHash, "federation poll response_hash", false); err != nil {
		return err
	}
	a.ExternalBoundary = CloneExternalBoundary(a.ExternalBoundary)
	if a.ExternalBoundary != nil {
		if err := a.ExternalBoundary.Normalize(); err != nil {
			return fmt.Errorf("federation poll external boundary: %w", err)
		}
	}
	if a.FromSequence < 0 || a.ImportedCount < 0 || a.ImportedCount > MaxFederationPollMessages || a.Fence == 0 || a.OwnerID == "" {
		return fmt.Errorf("federation poll sequence, import count, owner, and fence are invalid")
	}
	a.State = strings.ToLower(strings.TrimSpace(a.State))
	switch a.State {
	case FederationPollReserved, FederationPollDispatching, FederationPollSucceeded, FederationPollFailed, FederationPollRecoveryNeeded:
	default:
		return fmt.Errorf("invalid federation poll state %q", a.State)
	}
	if err := normalizeDomainActor(&a.Actor, workspace); err != nil {
		return err
	}
	a.WorkspaceID = workspace
	if a.StartedAt.IsZero() {
		a.StartedAt = time.Now().UTC()
	}
	a.StartedAt, a.UpdatedAt = a.StartedAt.UTC(), a.UpdatedAt.UTC()
	if a.CompletedAt != nil {
		value := a.CompletedAt.UTC()
		a.CompletedAt = &value
	}
	return nil
}

func validCredentialReference(value string) bool {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	for _, part := range parts {
		if len(part) > MaxDomainNameLength || strings.ContainsAny(part, " \t\r\n\\:") {
			return false
		}
	}
	return true
}
