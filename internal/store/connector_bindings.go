package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrConnectorBindingNotFound = errors.New("connector binding not found")
	ErrConnectorBindingConflict = errors.New("connector binding conflicts with existing state")
)

// ConnectorBindingStore is the Postgres authority for immutable workspace
// connector binding identities. It does not instantiate adapters or resolve
// credentials; those remain explicit runtime concerns.
type ConnectorBindingStore struct {
	pool        *pgxpool.Pool
	events      *EventStore
	failureHook func(string) error
}

func NewConnectorBindingStore(pool *pgxpool.Pool, events *EventStore) *ConnectorBindingStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &ConnectorBindingStore{pool: pool, events: events}
}

// SetFailureHook provides deterministic transaction crash points for tests.
func (s *ConnectorBindingStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

func (s *ConnectorBindingStore) fail(stage string) error {
	if s != nil && s.failureHook != nil {
		return s.failureHook(stage)
	}
	return nil
}

// Create registers one immutable binding. Repeating the same idempotent
// request returns the original binding; reusing the key with changed
// configuration fails closed.
func (s *ConnectorBindingStore) Create(ctx context.Context, request contracts.ConnectorBindingRequest) (contracts.ConnectorBinding, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.ConnectorBinding{}, false, fmt.Errorf("connector binding store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.ConnectorBinding{}, false, err
	}
	requestHash := request.RequestHash()
	connectorJSON := mustJSON(request.Binding.Connector)
	configurationJSON := append([]byte(nil), request.Binding.Configuration...)
	credentialsJSON := mustJSON(request.Binding.CredentialRefs)
	actorJSON := mustJSON(request.Actor)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.ConnectorBinding{}, false, fmt.Errorf("begin connector binding create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// The idempotency row has a foreign key to the immutable binding, so the
	// binding is inserted first. A concurrent retry waits on the same unique
	// binding row and then resolves the idempotency record below.
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.connector_bindings(
		 workspace_id,binding_id,schema_version,connector,binding_kind,binding_version,config_hash,configuration,credential_refs,status,created_by)
		VALUES($1,$2,$3,$4::jsonb,$5,$6,$7,$8::jsonb,$9::jsonb,$10,$11::jsonb)
		ON CONFLICT DO NOTHING`, request.WorkspaceID, request.Binding.ID, request.Binding.SchemaVersion, connectorJSON,
		request.Binding.Kind, request.Binding.Version, request.Binding.ConfigHash, configurationJSON, credentialsJSON, request.Binding.Status, actorJSON)
	if err != nil {
		return contracts.ConnectorBinding{}, false, fmt.Errorf("insert connector binding: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		var existingHash, bindingID string
		err := tx.QueryRow(ctx, `SELECT request_hash,binding_id FROM fornix.connector_binding_idempotency WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, request.WorkspaceID, request.IdempotencyKey).Scan(&existingHash, &bindingID)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.ConnectorBinding{}, false, ErrConnectorBindingConflict
		}
		if err != nil {
			return contracts.ConnectorBinding{}, false, err
		}
		if existingHash != requestHash {
			return contracts.ConnectorBinding{}, false, ErrConnectorBindingConflict
		}
		binding, readErr := readConnectorBinding(ctx, tx, request.WorkspaceID, bindingID)
		if readErr != nil {
			return contracts.ConnectorBinding{}, false, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ConnectorBinding{}, false, err
		}
		return binding, true, nil
	}
	inserted, err = tx.Exec(ctx, `
		INSERT INTO fornix.connector_binding_idempotency(workspace_id,idempotency_key,request_hash,binding_id)
		VALUES($1,$2,$3,$4) ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`,
		request.WorkspaceID, request.IdempotencyKey, requestHash, request.Binding.ID)
	if err != nil {
		return contracts.ConnectorBinding{}, false, fmt.Errorf("reserve connector binding idempotency: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		var existingHash, bindingID string
		if err := tx.QueryRow(ctx, `SELECT request_hash,binding_id FROM fornix.connector_binding_idempotency WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, request.WorkspaceID, request.IdempotencyKey).Scan(&existingHash, &bindingID); err != nil {
			return contracts.ConnectorBinding{}, false, err
		}
		if existingHash != requestHash {
			return contracts.ConnectorBinding{}, false, ErrConnectorBindingConflict
		}
		binding, readErr := readConnectorBinding(ctx, tx, request.WorkspaceID, bindingID)
		if readErr != nil {
			return contracts.ConnectorBinding{}, false, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ConnectorBinding{}, false, err
		}
		return binding, true, nil
	}
	event, err := contracts.NewEvent("connector.binding_created", map[string]any{
		"binding_id": request.Binding.ID, "binding_kind": request.Binding.Kind, "binding_version": request.Binding.Version,
		"connector": request.Binding.Connector, "config_hash": request.Binding.ConfigHash,
	})
	if err != nil {
		return contracts.ConnectorBinding{}, false, err
	}
	event.Scope = contracts.Scope{WorkspaceID: request.WorkspaceID, Subject: request.Binding.ID}
	event.Actor = request.Actor
	event.CorrelationID, event.CausationID = request.RequestID, request.RequestID
	event.IdempotencyKey = "connector-binding:" + request.WorkspaceID + ":" + request.IdempotencyKey
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ConnectorBinding{}, false, fmt.Errorf("append connector binding event: %w", err)
	}
	if err := s.fail("connector_binding_created"); err != nil {
		return contracts.ConnectorBinding{}, false, err
	}
	binding, err := readConnectorBinding(ctx, tx, request.WorkspaceID, request.Binding.ID)
	if err != nil {
		return contracts.ConnectorBinding{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ConnectorBinding{}, false, fmt.Errorf("commit connector binding: %w", err)
	}
	return binding, false, nil
}

func (s *ConnectorBindingStore) Get(ctx context.Context, workspaceID, bindingID string) (contracts.ConnectorBinding, error) {
	if s == nil || s.pool == nil {
		return contracts.ConnectorBinding{}, fmt.Errorf("connector binding store is not configured")
	}
	return readConnectorBinding(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(bindingID))
}

func readConnectorBinding(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, bindingID string) (contracts.ConnectorBinding, error) {
	var binding contracts.ConnectorBinding
	var connectorJSON, configurationJSON, credentialsJSON, actorJSON []byte
	err := queryer.QueryRow(ctx, `
		SELECT schema_version,binding_id,workspace_id,connector,binding_kind,binding_version,config_hash,configuration,credential_refs,status,created_by,created_at
		FROM fornix.connector_bindings WHERE workspace_id=$1 AND binding_id=$2`, workspaceID, bindingID).
		Scan(&binding.SchemaVersion, &binding.ID, &binding.WorkspaceID, &connectorJSON, &binding.Kind, &binding.Version, &binding.ConfigHash, &configurationJSON, &credentialsJSON, &binding.Status, &actorJSON, &binding.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.ConnectorBinding{}, ErrConnectorBindingNotFound
		}
		return contracts.ConnectorBinding{}, err
	}
	if err := json.Unmarshal(connectorJSON, &binding.Connector); err != nil {
		return contracts.ConnectorBinding{}, err
	}
	binding.Configuration = append(json.RawMessage(nil), configurationJSON...)
	if err := json.Unmarshal(credentialsJSON, &binding.CredentialRefs); err != nil {
		return contracts.ConnectorBinding{}, err
	}
	if err := json.Unmarshal(actorJSON, &binding.CreatedBy); err != nil {
		return contracts.ConnectorBinding{}, err
	}
	if err := binding.Normalize(); err != nil {
		return contracts.ConnectorBinding{}, err
	}
	return binding, nil
}
