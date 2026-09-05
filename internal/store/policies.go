package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	policyruntime "github.com/omaveda/fornix/internal/policy"
)

var (
	ErrPolicyNotFound          = errors.New("validation policy not found")
	ErrPolicyConflict          = errors.New("validation policy request conflicts with existing state")
	ErrPolicyImmutable         = errors.New("validation policy version is immutable")
	ErrPolicyRetired           = errors.New("validation policy version is retired")
	ErrPolicyInvalidTransition = errors.New("invalid validation policy lifecycle transition")
	ErrPolicyDefaultMissing    = errors.New("workspace has no active default validation policy")
)

// PolicyPage is a bounded deterministic policy-version page.
type PolicyPage struct {
	Items      []contracts.ValidationPolicyVersion `json:"policies"`
	NextCursor string                              `json:"next_cursor,omitempty"`
}

// PolicyAuditPage is a bounded append-only audit page.
type PolicyAuditPage struct {
	Items      []contracts.PolicyAuditRecord `json:"audit"`
	NextCursor string                        `json:"next_cursor,omitempty"`
}

// PolicyStore is the Postgres authority for declarative policy packs. It
// stores immutable bodies and uses lifecycle/audit rows for all state changes.
type PolicyStore struct {
	pool        *pgxpool.Pool
	events      *EventStore
	resolver    *policyruntime.Resolver
	failureHook func(string) error
}

// NewPolicyStore constructs a policy authority. The resolver is optional for
// migrations and can be attached once the server has built its registry.
func NewPolicyStore(pool *pgxpool.Pool, events *EventStore, resolver *policyruntime.Resolver) *PolicyStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &PolicyStore{pool: pool, events: events, resolver: resolver}
}

// SetResolver attaches the process-local deterministic validator registry.
func (s *PolicyStore) SetResolver(resolver *policyruntime.Resolver) {
	if s != nil {
		s.resolver = resolver
	}
}

// SetFailureHook provides deterministic transaction crash points for tests.
func (s *PolicyStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

func (s *PolicyStore) fail(stage string) error {
	if s != nil && s.failureHook != nil {
		return s.failureHook(stage)
	}
	return nil
}

// Create persists one immutable policy version. Reusing a key with the same
// request returns the existing version; reusing it with a different request
// fails closed.
func (s *PolicyStore) Create(ctx context.Context, request contracts.PolicyCreateRequest) (contracts.ValidationPolicyVersion, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("policy store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	if s.resolver == nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("policy resolver is not configured")
	}
	pack, err := s.resolver.ValidatePack(request.Pack)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	request.Pack = pack
	requestHash := request.RequestHash()
	actor, _ := json.Marshal(request.Actor)
	packJSON, _ := json.Marshal(request.Pack)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("begin policy create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.validation_policy_idempotency(workspace_id,idempotency_key,request_hash,operation,policy_id,version,policy_hash)
		VALUES($1,$2,$3,'create',$4,$5,$6)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, request.WorkspaceID, request.IdempotencyKey, requestHash, request.Pack.PolicyID, request.Pack.Version, request.Pack.PolicyHash)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("reserve policy idempotency: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		var existingHash, operation, policyID, version string
		if err := tx.QueryRow(ctx, `SELECT request_hash,operation,policy_id,version FROM fornix.validation_policy_idempotency WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, request.WorkspaceID, request.IdempotencyKey).Scan(&existingHash, &operation, &policyID, &version); err != nil {
			return contracts.ValidationPolicyVersion{}, false, err
		}
		if existingHash != requestHash || operation != "create" {
			return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("%w: idempotency key", ErrPolicyConflict)
		}
		versionValue, readErr := readPolicyVersionTx(ctx, tx, request.WorkspaceID, policyID, version, false)
		if readErr != nil {
			return contracts.ValidationPolicyVersion{}, false, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ValidationPolicyVersion{}, false, err
		}
		return versionValue, false, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_policies(workspace_id,policy_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, request.WorkspaceID, request.Pack.PolicyID); err != nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("create policy identity: %w", err)
	}
	versionInsert, err := tx.Exec(ctx, `
		INSERT INTO fornix.validation_policy_versions(workspace_id,policy_id,version,policy_hash,pack,status,actor)
		VALUES($1,$2,$3,$4,$5::jsonb,'draft',$6::jsonb)
		ON CONFLICT (workspace_id,policy_id,version) DO NOTHING`, request.WorkspaceID, request.Pack.PolicyID, request.Pack.Version, request.Pack.PolicyHash, packJSON, actor)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("insert policy version: %w", err)
	}
	if versionInsert.RowsAffected() == 0 {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("%w: policy version already has a different body", ErrPolicyConflict)
	}
	for ordinal, rule := range request.Pack.Rules {
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_policy_rules(workspace_id,policy_id,version,ordinal,validator_id,validator_version,required) VALUES($1,$2,$3,$4,$5,$6,$7)`, request.WorkspaceID, request.Pack.PolicyID, request.Pack.Version, ordinal, rule.Validator.ID, rule.Validator.Version, rule.Required); err != nil {
			return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("insert policy rule: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_policy_transitions(workspace_id,policy_id,version,from_status,to_status,operation,actor,request_id,idempotency_key,reason) VALUES($1,$2,$3,'','draft','create',$4::jsonb,$5,$6,$7)`, request.WorkspaceID, request.Pack.PolicyID, request.Pack.Version, actor, request.RequestID, request.IdempotencyKey, "policy version created"); err != nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("record policy transition: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_policy_audit(workspace_id,policy_id,version,policy_hash,operation,from_status,to_status,actor,request_id,idempotency_key,allowed,reason) VALUES($1,$2,$3,$4,'create','','draft',$5::jsonb,$6,$7,TRUE,$8)`, request.WorkspaceID, request.Pack.PolicyID, request.Pack.Version, request.Pack.PolicyHash, actor, request.RequestID, request.IdempotencyKey, "policy version created"); err != nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("record policy audit: %w", err)
	}
	if err := s.fail("policy_created"); err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	event, err := contracts.NewEvent("policy.created", map[string]any{"policy_id": request.Pack.PolicyID, "version": request.Pack.Version, "policy_hash": request.Pack.PolicyHash, "status": string(contracts.PolicyDraft)})
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	event.Scope = contracts.Scope{WorkspaceID: request.WorkspaceID, Subject: request.Pack.PolicyID}
	event.Actor = request.Actor
	ref := request.Pack.Ref()
	event.Policy = &ref
	event.CorrelationID, event.CausationID = request.RequestID, request.RequestID
	event.IdempotencyKey = "policy:create:" + request.WorkspaceID + ":" + request.IdempotencyKey
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("append policy event: %w", err)
	}
	value, err := readPolicyVersionTx(ctx, tx, request.WorkspaceID, request.Pack.PolicyID, request.Pack.Version, false)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("commit policy create: %w", err)
	}
	return value, true, nil
}

// Get reads a policy version, including draft and retired versions.
func (s *PolicyStore) Get(ctx context.Context, workspaceID, policyID, version string) (contracts.ValidationPolicyVersion, error) {
	if s == nil || s.pool == nil {
		return contracts.ValidationPolicyVersion{}, fmt.Errorf("policy store is not configured")
	}
	return readPolicyVersion(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(policyID), strings.TrimSpace(version))
}

// List returns policy versions ordered newest-first by creation time and then
// by identity. Cursor is the last returned policy version's encoded identity.
func (s *PolicyStore) List(ctx context.Context, workspaceID string, limit int, cursor string) (PolicyPage, error) {
	if s == nil || s.pool == nil {
		return PolicyPage{}, fmt.Errorf("policy store is not configured")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > contracts.MaxPolicyPageSize {
		limit = contracts.MaxPolicyPageSize
	}
	workspaceID = strings.TrimSpace(workspaceID)
	query := `SELECT policy_id,version FROM fornix.validation_policy_versions WHERE workspace_id=$1 ORDER BY created_at DESC,policy_id,version LIMIT $2`
	args := []any{workspaceID, limit + 1}
	if cursor != "" {
		parts := strings.SplitN(cursor, "\x00", 2)
		if len(parts) != 2 {
			return PolicyPage{}, fmt.Errorf("invalid policy cursor")
		}
		query = `SELECT policy_id,version FROM fornix.validation_policy_versions WHERE workspace_id=$1 AND (created_at,policy_id,version) < (SELECT created_at,policy_id,version FROM fornix.validation_policy_versions WHERE workspace_id=$1 AND policy_id=$2 AND version=$3) ORDER BY created_at DESC,policy_id,version LIMIT $4`
		args = []any{workspaceID, parts[0], parts[1], limit + 1}
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return PolicyPage{}, err
	}
	pairs := make([][2]string, 0, limit+1)
	for rows.Next() {
		var policyID, version string
		if err := rows.Scan(&policyID, &version); err != nil {
			rows.Close()
			return PolicyPage{}, err
		}
		pairs = append(pairs, [2]string{policyID, version})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PolicyPage{}, err
	}
	rows.Close()
	page := PolicyPage{Items: make([]contracts.ValidationPolicyVersion, 0, limit)}
	for _, pair := range pairs {
		value, err := s.Get(ctx, workspaceID, pair[0], pair[1])
		if err != nil {
			return PolicyPage{}, err
		}
		page.Items = append(page.Items, value)
	}
	if len(page.Items) > limit {
		last := page.Items[limit-1]
		page.NextCursor = last.PolicyID + "\x00" + last.Version
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// Activate makes an exact version active. If another version of the same
// policy is active, it is retired in this transaction, providing an auditable
// rollback without mutating either policy body.
func (s *PolicyStore) Activate(ctx context.Context, request contracts.PolicyLifecycleRequest) (contracts.ValidationPolicyVersion, bool, error) {
	return s.lifecycle(ctx, request, "activate")
}

// SetDefault binds the workspace's default to one active exact version.
func (s *PolicyStore) SetDefault(ctx context.Context, request contracts.PolicyLifecycleRequest) (contracts.ValidationPolicyVersion, bool, error) {
	return s.lifecycle(ctx, request, "default")
}

// Retire prevents a version from admitting new work. Existing records retain
// their snapshot and remain replayable.
func (s *PolicyStore) Retire(ctx context.Context, request contracts.PolicyLifecycleRequest) (contracts.ValidationPolicyVersion, bool, error) {
	return s.lifecycle(ctx, request, "retire")
}

func (s *PolicyStore) lifecycle(ctx context.Context, request contracts.PolicyLifecycleRequest, operation string) (contracts.ValidationPolicyVersion, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("policy store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	request.Policy.SchemaVersion = contracts.PolicySchemaVersion
	requestHash := request.RequestHash()
	actor, _ := json.Marshal(request.Actor)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	inserted, err := tx.Exec(ctx, `INSERT INTO fornix.validation_policy_idempotency(workspace_id,idempotency_key,request_hash,operation,policy_id,version,policy_hash) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, request.WorkspaceID, request.IdempotencyKey, requestHash, operation, request.Policy.PolicyID, request.Policy.Version, request.Policy.PolicyHash)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	if inserted.RowsAffected() == 0 {
		var existingHash, existingOperation, policyID, version string
		if err := tx.QueryRow(ctx, `SELECT request_hash,operation,policy_id,version FROM fornix.validation_policy_idempotency WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, request.WorkspaceID, request.IdempotencyKey).Scan(&existingHash, &existingOperation, &policyID, &version); err != nil {
			return contracts.ValidationPolicyVersion{}, false, err
		}
		if existingHash != requestHash || existingOperation != operation {
			return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("%w: idempotency key", ErrPolicyConflict)
		}
		value, err := readPolicyVersionTx(ctx, tx, request.WorkspaceID, policyID, version, false)
		if err != nil {
			return contracts.ValidationPolicyVersion{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ValidationPolicyVersion{}, false, err
		}
		return value, true, nil
	}
	if operation == "activate" {
		// Serialize competing activations for one policy identity before
		// retiring the previous active version and promoting the target. The
		// partial unique index is a final safeguard, not the synchronization
		// mechanism: without this lock two transactions can both observe no
		// active target and race into a uniqueness violation.
		var lockedPolicyID string
		if err := tx.QueryRow(ctx, `SELECT policy_id FROM fornix.validation_policies WHERE workspace_id=$1 AND policy_id=$2 FOR UPDATE`, request.WorkspaceID, request.Policy.PolicyID).Scan(&lockedPolicyID); err != nil {
			return contracts.ValidationPolicyVersion{}, false, err
		}
	}
	value, err := readPolicyVersionTx(ctx, tx, request.WorkspaceID, request.Policy.PolicyID, request.Policy.Version, true)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	if request.Policy.PolicyHash != "" && request.Policy.PolicyHash != value.PolicyHash {
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("%w: policy hash", ErrPolicyConflict)
	}
	from := value.Status
	status := value.Status
	switch operation {
	case "activate":
		if value.Status == contracts.PolicyActive {
			status = contracts.PolicyActive
		} else if value.Status == contracts.PolicyDraft || value.Status == contracts.PolicyRetired {
			status = contracts.PolicyActive
		} else {
			return contracts.ValidationPolicyVersion{}, false, ErrPolicyInvalidTransition
		}
		if status == contracts.PolicyActive {
			if _, err := tx.Exec(ctx, `UPDATE fornix.validation_policy_versions SET status='retired',retired_at=COALESCE(retired_at,clock_timestamp()),updated_at=clock_timestamp() WHERE workspace_id=$1 AND policy_id=$2 AND status='active' AND version<>$3`, request.WorkspaceID, request.Policy.PolicyID, request.Policy.Version); err != nil {
				return contracts.ValidationPolicyVersion{}, false, err
			}
			if _, err := tx.Exec(ctx, `UPDATE fornix.validation_policy_versions SET status='active',activated_at=COALESCE(activated_at,clock_timestamp()),retired_at=NULL,updated_at=clock_timestamp() WHERE workspace_id=$1 AND policy_id=$2 AND version=$3`, request.WorkspaceID, request.Policy.PolicyID, request.Policy.Version); err != nil {
				return contracts.ValidationPolicyVersion{}, false, err
			}
		}
	case "default":
		if value.Status != contracts.PolicyActive {
			return contracts.ValidationPolicyVersion{}, false, ErrPolicyRetired
		}
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_policy_defaults(workspace_id,policy_id,version,policy_hash,actor) VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT (workspace_id) DO UPDATE SET policy_id=EXCLUDED.policy_id,version=EXCLUDED.version,policy_hash=EXCLUDED.policy_hash,actor=EXCLUDED.actor,updated_at=clock_timestamp()`, request.WorkspaceID, value.PolicyID, value.Version, value.PolicyHash, actor); err != nil {
			return contracts.ValidationPolicyVersion{}, false, err
		}
	case "retire":
		if value.Status == contracts.PolicyDraft || value.Status == contracts.PolicyActive {
			status = contracts.PolicyRetired
		} else if value.Status != contracts.PolicyRetired {
			return contracts.ValidationPolicyVersion{}, false, ErrPolicyInvalidTransition
		}
		if status == contracts.PolicyRetired && value.Status != contracts.PolicyRetired {
			if _, err := tx.Exec(ctx, `UPDATE fornix.validation_policy_versions SET status='retired',retired_at=clock_timestamp(),updated_at=clock_timestamp() WHERE workspace_id=$1 AND policy_id=$2 AND version=$3`, request.WorkspaceID, value.PolicyID, value.Version); err != nil {
				return contracts.ValidationPolicyVersion{}, false, err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM fornix.validation_policy_defaults WHERE workspace_id=$1 AND policy_id=$2 AND version=$3`, request.WorkspaceID, value.PolicyID, value.Version); err != nil {
				return contracts.ValidationPolicyVersion{}, false, err
			}
		}
	default:
		return contracts.ValidationPolicyVersion{}, false, fmt.Errorf("unsupported policy operation %q", operation)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_policy_transitions(workspace_id,policy_id,version,from_status,to_status,operation,actor,request_id,idempotency_key,reason) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10)`, request.WorkspaceID, value.PolicyID, value.Version, from, status, operation, actor, request.RequestID, request.IdempotencyKey, request.Reason); err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.validation_policy_audit(workspace_id,policy_id,version,policy_hash,operation,from_status,to_status,actor,request_id,idempotency_key,allowed,reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,TRUE,$11)`, request.WorkspaceID, value.PolicyID, value.Version, value.PolicyHash, operation, from, status, actor, request.RequestID, request.IdempotencyKey, request.Reason); err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	if err := s.fail("policy_transitioned"); err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	event, err := contracts.NewEvent("policy."+operation, map[string]any{"policy_id": value.PolicyID, "version": value.Version, "policy_hash": value.PolicyHash, "from_status": from, "to_status": status})
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	event.Scope = contracts.Scope{WorkspaceID: request.WorkspaceID, Subject: value.PolicyID}
	event.Actor = request.Actor
	ref := value.Pack.Ref()
	event.Policy = &ref
	event.CausationID, event.CorrelationID = request.RequestID, request.RequestID
	event.IdempotencyKey = "policy:" + operation + ":" + request.WorkspaceID + ":" + request.IdempotencyKey
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	value, err = readPolicyVersionTx(ctx, tx, request.WorkspaceID, value.PolicyID, value.Version, false)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ValidationPolicyVersion{}, false, err
	}
	return value, false, nil
}

// Default returns the currently bound active workspace policy.
func (s *PolicyStore) Default(ctx context.Context, workspaceID string) (contracts.ValidationPolicyVersion, error) {
	if s == nil || s.pool == nil {
		return contracts.ValidationPolicyVersion{}, fmt.Errorf("policy store is not configured")
	}
	var policyID, version string
	err := s.pool.QueryRow(ctx, `SELECT policy_id,version FROM fornix.validation_policy_defaults WHERE workspace_id=$1`, strings.TrimSpace(workspaceID)).Scan(&policyID, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ValidationPolicyVersion{}, ErrPolicyDefaultMissing
	}
	if err != nil {
		return contracts.ValidationPolicyVersion{}, err
	}
	value, err := s.Get(ctx, workspaceID, policyID, version)
	if err != nil {
		return contracts.ValidationPolicyVersion{}, err
	}
	if value.Status != contracts.PolicyActive {
		return contracts.ValidationPolicyVersion{}, ErrPolicyRetired
	}
	return value, nil
}

// LockActiveTx linearizes an admission against policy retirement or rollback.
// Resolution may happen before the caller opens its mutation transaction; the
// caller must take this lock again before inserting authoritative work so a
// concurrent lifecycle transition cannot admit a retired policy.
func (s *PolicyStore) LockActiveTx(ctx context.Context, tx pgx.Tx, ref *contracts.ValidationPolicyRef) error {
	if ref == nil {
		return nil
	}
	if tx == nil {
		return fmt.Errorf("policy transaction is nil")
	}
	copy := *ref
	if err := copy.Normalize(); err != nil {
		return err
	}
	var status, policyHash string
	err := tx.QueryRow(ctx, `SELECT status,policy_hash FROM fornix.validation_policy_versions WHERE workspace_id=$1 AND policy_id=$2 AND version=$3 FOR UPDATE`, copy.WorkspaceID, copy.PolicyID, copy.Version).Scan(&status, &policyHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrPolicyNotFound
	}
	if err != nil {
		return err
	}
	if copy.PolicyHash != "" && copy.PolicyHash != policyHash {
		return fmt.Errorf("%w: policy hash", ErrPolicyConflict)
	}
	if status != string(contracts.PolicyActive) {
		return ErrPolicyRetired
	}
	return nil
}

// Resolve selects an exact active policy or the workspace default. No policy
// is a compatibility resolution and therefore has no synthetic policy hash.
func (s *PolicyStore) Resolve(ctx context.Context, request contracts.PolicyEvaluationRequest) (contracts.PolicyResolution, error) {
	if s == nil || s.pool == nil {
		return contracts.PolicyResolution{}, fmt.Errorf("policy store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.PolicyResolution{}, err
	}
	var value contracts.ValidationPolicyVersion
	var err error
	if request.Policy != nil {
		ref := *request.Policy
		if err := ref.Normalize(); err != nil {
			return contracts.PolicyResolution{}, err
		}
		if ref.WorkspaceID != request.WorkspaceID {
			return contracts.PolicyResolution{}, fmt.Errorf("policy crosses workspace boundary")
		}
		value, err = s.Get(ctx, request.WorkspaceID, ref.PolicyID, ref.Version)
		if err == nil && ref.PolicyHash != "" && ref.PolicyHash != value.PolicyHash {
			return contracts.PolicyResolution{}, fmt.Errorf("%w: policy hash", ErrPolicyConflict)
		}
	} else {
		value, err = s.Default(ctx, request.WorkspaceID)
		if errors.Is(err, ErrPolicyDefaultMissing) {
			if s.resolver == nil {
				return contracts.PolicyResolution{}, fmt.Errorf("policy resolver is not configured")
			}
			return s.resolver.Resolve(nil, request)
		}
	}
	if err != nil {
		return contracts.PolicyResolution{}, err
	}
	if value.Status != contracts.PolicyActive {
		return contracts.PolicyResolution{}, ErrPolicyRetired
	}
	ref := value.Pack.Ref()
	request.Policy = &ref
	if s.resolver == nil {
		return contracts.PolicyResolution{}, fmt.Errorf("policy resolver is not configured")
	}
	return s.resolver.Resolve(&value.Pack, request)
}

// DryRunResolve performs the same fail-closed admission calculation without a
// write or audit side effect.
func (s *PolicyStore) DryRunResolve(ctx context.Context, request contracts.PolicyEvaluationRequest) (contracts.PolicyResolution, error) {
	return s.Resolve(ctx, request)
}

// Audit returns bounded policy audit history in insertion order.
func (s *PolicyStore) Audit(ctx context.Context, workspaceID, policyID, version string, limit int, cursor string) (PolicyAuditPage, error) {
	if s == nil || s.pool == nil {
		return PolicyAuditPage{}, fmt.Errorf("policy store is not configured")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > contracts.MaxPolicyPageSize {
		limit = contracts.MaxPolicyPageSize
	}
	query := `SELECT id,workspace_id,policy_id,version,policy_hash,operation,from_status,to_status,actor,request_id,idempotency_key,allowed,reason,created_at FROM fornix.validation_policy_audit WHERE workspace_id=$1 AND ($2='' OR policy_id=$2) AND ($3='' OR version=$3) ORDER BY id LIMIT $4`
	args := []any{strings.TrimSpace(workspaceID), strings.TrimSpace(policyID), strings.TrimSpace(version), limit + 1}
	if cursor != "" {
		id, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil || id < 1 {
			return PolicyAuditPage{}, fmt.Errorf("invalid policy audit cursor")
		}
		query = `SELECT id,workspace_id,policy_id,version,policy_hash,operation,from_status,to_status,actor,request_id,idempotency_key,allowed,reason,created_at FROM fornix.validation_policy_audit WHERE workspace_id=$1 AND ($2='' OR policy_id=$2) AND ($3='' OR version=$3) AND id>$4 ORDER BY id LIMIT $5`
		args = []any{strings.TrimSpace(workspaceID), strings.TrimSpace(policyID), strings.TrimSpace(version), id, limit + 1}
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return PolicyAuditPage{}, err
	}
	defer rows.Close()
	page := PolicyAuditPage{Items: make([]contracts.PolicyAuditRecord, 0, limit)}
	for rows.Next() {
		var item contracts.PolicyAuditRecord
		var actorJSON []byte
		var from, to string
		if err := rows.Scan(&item.ID, &item.WorkspaceID, &item.PolicyID, &item.Version, &item.PolicyHash, &item.Operation, &from, &to, &actorJSON, &item.RequestID, &item.IdempotencyKey, &item.Allowed, &item.Reason, &item.CreatedAt); err != nil {
			return PolicyAuditPage{}, err
		}
		item.SchemaVersion = contracts.PolicySchemaVersion
		item.FromStatus = contracts.PolicyLifecycleStatus(from)
		item.ToStatus = contracts.PolicyLifecycleStatus(to)
		if err := json.Unmarshal(actorJSON, &item.Actor); err != nil {
			return PolicyAuditPage{}, err
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return PolicyAuditPage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = strconv.FormatInt(page.Items[limit-1].ID, 10)
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// Compare returns the stable field-level difference between two immutable
// policy bodies in one workspace.
func (s *PolicyStore) Compare(ctx context.Context, request contracts.PolicyCompareRequest) (contracts.PolicyComparison, error) {
	if err := request.Normalize(); err != nil {
		return contracts.PolicyComparison{}, err
	}
	left, err := s.Get(ctx, request.WorkspaceID, request.Left.PolicyID, request.Left.Version)
	if err != nil {
		return contracts.PolicyComparison{}, err
	}
	right, err := s.Get(ctx, request.WorkspaceID, request.Right.PolicyID, request.Right.Version)
	if err != nil {
		return contracts.PolicyComparison{}, err
	}
	if request.Left.PolicyHash != "" && request.Left.PolicyHash != left.PolicyHash || request.Right.PolicyHash != "" && request.Right.PolicyHash != right.PolicyHash {
		return contracts.PolicyComparison{}, ErrPolicyConflict
	}
	changed := make([]string, 0, 8)
	if policyRulesHash(left.Pack) != policyRulesHash(right.Pack) {
		changed = append(changed, "rules")
	}
	if left.Pack.Budget != right.Pack.Budget {
		changed = append(changed, "budget")
	}
	if left.Pack.Approval.Mode != right.Pack.Approval.Mode {
		changed = append(changed, "approval.mode")
	}
	if strings.Join(left.Pack.Approval.RequireFor, "\x00") != strings.Join(right.Pack.Approval.RequireFor, "\x00") {
		changed = append(changed, "approval.require_for")
	}
	if left.Pack.RequireReindex != right.Pack.RequireReindex {
		changed = append(changed, "require_reindex")
	}
	if left.Pack.RequireTaskFence != right.Pack.RequireTaskFence {
		changed = append(changed, "require_task_fence")
	}
	if left.Pack.SafetyFloors != right.Pack.SafetyFloors {
		changed = append(changed, "safety_floors")
	}
	comparison := contracts.PolicyComparison{WorkspaceID: request.WorkspaceID, Left: left.Pack.Ref(), Right: right.Pack.Ref(), Changed: changed, Same: len(changed) == 0}
	raw, _ := json.Marshal(comparison)
	comparison.Hash = contracts.ArtifactContentHash(raw)
	return comparison, nil
}

func readPolicyVersion(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, policyID, version string) (contracts.ValidationPolicyVersion, error) {
	return readPolicyVersionTx(ctx, queryer, workspaceID, policyID, version, false)
}

func readPolicyVersionTx(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, policyID, version string, forUpdate bool) (contracts.ValidationPolicyVersion, error) {
	query := `SELECT workspace_id,policy_id,version,policy_hash,pack,status,actor,created_at,updated_at,activated_at,retired_at FROM fornix.validation_policy_versions WHERE workspace_id=$1 AND policy_id=$2 AND version=$3`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var value contracts.ValidationPolicyVersion
	var packJSON, actorJSON []byte
	var status string
	err := queryer.QueryRow(ctx, query, workspaceID, policyID, version).Scan(&value.WorkspaceID, &value.PolicyID, &value.Version, &value.PolicyHash, &packJSON, &status, &actorJSON, &value.CreatedAt, &value.UpdatedAt, &value.ActivatedAt, &value.RetiredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ValidationPolicyVersion{}, ErrPolicyNotFound
	}
	if err != nil {
		return contracts.ValidationPolicyVersion{}, err
	}
	if err := json.Unmarshal(packJSON, &value.Pack); err != nil {
		return contracts.ValidationPolicyVersion{}, err
	}
	if err := json.Unmarshal(actorJSON, &value.Actor); err != nil {
		return contracts.ValidationPolicyVersion{}, err
	}
	value.SchemaVersion = contracts.PolicySchemaVersion
	value.Status = contracts.PolicyLifecycleStatus(status)
	value.Pack.PolicyHash = value.PolicyHash
	return value, nil
}

// policyRulesHash provides a cheap canonical comparison identity for normalized
// policy rules without exposing rule payloads in audit logs.
func policyRulesHash(p contracts.ValidationPolicyPack) string {
	raw, _ := json.Marshal(p.Rules)
	return contracts.ArtifactContentHash(raw)
}
