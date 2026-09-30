package store

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrTrustSignerNotFound   = errors.New("trust signer not found")
	ErrTrustSignerConflict   = errors.New("trust signer conflicts with an existing key")
	ErrTrustSignerRevoked    = errors.New("trust signer is revoked")
	ErrTrustPolicyNotFound   = errors.New("trust policy not found")
	ErrTrustPolicyConflict   = errors.New("trust policy conflicts with an existing revision")
	ErrSchemaCatalogNotFound = errors.New("schema catalog not found")
	ErrSchemaCatalogConflict = errors.New("schema catalog conflicts with an existing revision")
)

// TrustCatalogStore is the Postgres authority for signed connector and
// capability trust snapshots. It stores public keys and detached signatures,
// never private signing keys or credentials. The process-local connector
// registry remains the execution cache; this store is the durable source that
// must be loaded before production admission.
type TrustCatalogStore struct {
	pool *pgxpool.Pool
}

// NewTrustCatalogStore constructs a durable trust catalog over pool.
func NewTrustCatalogStore(pool *pgxpool.Pool) *TrustCatalogStore {
	return &TrustCatalogStore{pool: pool}
}

// RegisterSigner installs one workspace-scoped public verification key. A
// signer ID is immutable: rotation uses a new signer ID so old signatures can
// remain attributable in append-only history.
func (s *TrustCatalogStore) RegisterSigner(ctx context.Context, workspaceID, signerID string, publicKey ed25519.PublicKey, actor contracts.AuditActor) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("trust catalog is not configured")
	}
	workspaceID, signerID = strings.TrimSpace(workspaceID), strings.TrimSpace(signerID)
	if workspaceID == "" || signerID == "" || len(signerID) > 128 || len(publicKey) != ed25519.PublicKeySize {
		return connector.ErrTrustSignature
	}
	if err := validateTrustAuditActor(actor, workspaceID); err != nil {
		return err
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return fmt.Errorf("begin trust signer registration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	var existing []byte
	err = tx.QueryRow(ctx, `
		SELECT status,public_key
		FROM fornix.trust_signers
		WHERE workspace_id=$1 AND signer_id=$2
		ORDER BY id DESC
		LIMIT 1
		FOR UPDATE`, workspaceID, signerID).Scan(&status, &existing)
	if err == nil {
		if status == "revoked" {
			return ErrTrustSignerRevoked
		}
		if !equalBytes(existing, publicKey) {
			return ErrTrustSignerConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit idempotent trust signer registration: %w", err)
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read trust signer: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.trust_signers(workspace_id,signer_id,signature_scheme,public_key,status)
		VALUES($1,$2,'ed25519',$3,'active')`, workspaceID, signerID, []byte(publicKey)); err != nil {
		return fmt.Errorf("insert trust signer: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.trust_signer_events(workspace_id,signer_id,event,actor,metadata)
		VALUES($1,$2,'registered',$3::jsonb,$4::jsonb)`, workspaceID, signerID, actorJSON, `{"public_key_sha256":"`+trustPublicKeyHash(publicKey)+`"}`); err != nil {
		return fmt.Errorf("record trust signer registration: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust signer registration: %w", err)
	}
	return nil
}

// RevokeSigner immediately prevents new policy publication and invalidates
// existing policy loads that rely on this signer. The signer row is mutable
// only for current status; the append-only event preserves the audit history.
func (s *TrustCatalogStore) RevokeSigner(ctx context.Context, workspaceID, signerID string, actor contracts.AuditActor) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("trust catalog is not configured")
	}
	workspaceID, signerID = strings.TrimSpace(workspaceID), strings.TrimSpace(signerID)
	if workspaceID == "" || signerID == "" {
		return ErrTrustSignerNotFound
	}
	if err := validateTrustAuditActor(actor, workspaceID); err != nil {
		return err
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return fmt.Errorf("begin trust signer revocation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := tx.Exec(ctx, `
		UPDATE fornix.trust_signers
		SET status='revoked',revoked_at=clock_timestamp()
		WHERE workspace_id=$1 AND signer_id=$2 AND status='active'`, workspaceID, signerID)
	if err != nil {
		return fmt.Errorf("revoke trust signer: %w", err)
	}
	if result.RowsAffected() == 0 {
		var status string
		if err := tx.QueryRow(ctx, `SELECT status FROM fornix.trust_signers WHERE workspace_id=$1 AND signer_id=$2 ORDER BY id DESC LIMIT 1`, workspaceID, signerID).Scan(&status); errors.Is(err, pgx.ErrNoRows) {
			return ErrTrustSignerNotFound
		} else if err != nil {
			return fmt.Errorf("read revoked trust signer: %w", err)
		} else if status == "revoked" {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit idempotent trust signer revocation: %w", err)
			}
			return nil
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.trust_signer_events(workspace_id,signer_id,event,actor)
		VALUES($1,$2,'revoked',$3::jsonb)`, workspaceID, signerID, actorJSON); err != nil {
		return fmt.Errorf("record trust signer revocation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust signer revocation: %w", err)
	}
	return nil
}

// PublishSignedPolicy verifies a detached policy against the currently
// active signer, enforces strict workspace/revision ordering, and persists it
// idempotently. A duplicate policy hash is a successful no-op.
func (s *TrustCatalogStore) PublishSignedPolicy(ctx context.Context, policy connector.TrustPolicy, now time.Time, actor contracts.AuditActor) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("trust catalog is not configured")
	}
	if err := validateTrustAuditActor(actor, policy.WorkspaceID); err != nil {
		return err
	}
	workspaceID := strings.TrimSpace(policy.WorkspaceID)
	if workspaceID == "" || policy.Revision == "" {
		return connector.ErrTrustSignature
	}
	if policy.SignatureScheme != "ed25519" || policy.PolicyHash == "" || len(policy.Entries) == 0 {
		return connector.ErrTrustSignature
	}
	if policy.StableHash() != policy.PolicyHash || !validCatalogTrustEntries(policy.Entries) {
		return connector.ErrTrustSignature
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return fmt.Errorf("begin trust policy publish: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// A workspace can temporarily have more than one active signer during
	// rotation. Serialize revision allocation across all of them.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-trust-catalog:' || $1, 0))`, workspaceID); err != nil {
		return fmt.Errorf("lock trust catalog workspace: %w", err)
	}
	var publicKey []byte
	var signerStatus string
	if err := tx.QueryRow(ctx, `
		SELECT public_key,status
		FROM fornix.trust_signers
		WHERE workspace_id=$1 AND signer_id=$2
		ORDER BY id DESC
		LIMIT 1
		FOR UPDATE`, workspaceID, policy.SignerID).Scan(&publicKey, &signerStatus); errors.Is(err, pgx.ErrNoRows) {
		return ErrTrustSignerNotFound
	} else if err != nil {
		return fmt.Errorf("read trust signer for policy: %w", err)
	} else if signerStatus != "active" {
		return ErrTrustSignerRevoked
	}
	if err := policy.Verify(map[string]ed25519.PublicKey{policy.SignerID: ed25519.PublicKey(publicKey)}, now); err != nil {
		return err
	}
	var currentRevision int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(revision),0) FROM fornix.trust_policies WHERE workspace_id=$1`, workspaceID).Scan(&currentRevision); err != nil {
		return fmt.Errorf("read trust policy revision: %w", err)
	}
	revision, err := parseTrustRevision(policy.Revision)
	if err != nil || revision <= currentRevision {
		var existing string
		if err := tx.QueryRow(ctx, `SELECT policy_hash FROM fornix.trust_policies WHERE workspace_id=$1 AND policy_hash=$2`, workspaceID, policy.PolicyHash).Scan(&existing); err == nil && existing == policy.PolicyHash {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return nil
		}
		return connector.ErrTrustDowngrade
	}
	entriesJSON, err := json.Marshal(policy.Entries)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.trust_policies(workspace_id,revision,policy_hash,entries,signature_scheme,signer_id,signature,issued_at,expires_at,actor)
		VALUES($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10::jsonb)`, workspaceID, revision, policy.PolicyHash, entriesJSON, policy.SignatureScheme, policy.SignerID, policy.Signature, policy.IssuedAt.UTC(), policy.ExpiresAt.UTC(), actorJSON); err != nil {
		if isUniqueViolation(err) {
			return ErrTrustPolicyConflict
		}
		return fmt.Errorf("insert trust policy: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust policy publish: %w", err)
	}
	return nil
}

// CurrentSignedPolicy returns the highest non-expired policy whose signer is
// still active. It verifies the detached signature again on read, so a signer
// revocation takes effect without waiting for a process restart.
func (s *TrustCatalogStore) CurrentSignedPolicy(ctx context.Context, workspaceID string, now time.Time) (connector.TrustPolicy, ed25519.PublicKey, error) {
	if s == nil || s.pool == nil {
		return connector.TrustPolicy{}, nil, fmt.Errorf("trust catalog is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return connector.TrustPolicy{}, nil, ErrTrustPolicyNotFound
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return connector.TrustPolicy{}, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var revision int64
	var policyHash, entriesRaw, scheme, signerID, signature string
	var issuedAt, expiresAt time.Time
	var publicKey []byte
	err = tx.QueryRow(ctx, `
		SELECT p.revision,p.policy_hash,p.entries::text,p.signature_scheme,p.signer_id,p.signature,p.issued_at,p.expires_at,s.public_key
		FROM fornix.trust_policies p
		JOIN fornix.trust_signers s
		  ON s.workspace_id=p.workspace_id AND s.signer_id=p.signer_id AND s.status='active'
		WHERE p.workspace_id=$1 AND p.expires_at>clock_timestamp()
		ORDER BY p.revision DESC
		LIMIT 1`, workspaceID).Scan(&revision, &policyHash, &entriesRaw, &scheme, &signerID, &signature, &issuedAt, &expiresAt, &publicKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return connector.TrustPolicy{}, nil, ErrTrustPolicyNotFound
	}
	if err != nil {
		return connector.TrustPolicy{}, nil, fmt.Errorf("read current trust policy: %w", err)
	}
	var entries []connector.TrustEntry
	if err := json.Unmarshal([]byte(entriesRaw), &entries); err != nil || !validCatalogTrustEntries(entries) {
		return connector.TrustPolicy{}, nil, connector.ErrTrustSignature
	}
	policy := connector.TrustPolicy{WorkspaceID: workspaceID, Revision: fmt.Sprintf("%d", revision), Entries: entries, PolicyHash: policyHash, SignatureScheme: scheme, SignerID: signerID, Signature: signature, IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC()}
	key := ed25519.PublicKey(append([]byte(nil), publicKey...))
	if err := policy.Verify(map[string]ed25519.PublicKey{signerID: key}, now); err != nil {
		return connector.TrustPolicy{}, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return connector.TrustPolicy{}, nil, err
	}
	return policy, key, nil
}

// PublishSignedSchemaCatalog verifies and durably publishes a schema
// fingerprint catalog against the current workspace signer. The catalog is a
// separate authority from the capability allowlist and is append-only.
func (s *TrustCatalogStore) PublishSignedSchemaCatalog(ctx context.Context, catalog connector.SchemaCatalog, now time.Time, actor contracts.AuditActor) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("trust catalog is not configured")
	}
	catalog.Entries = append([]connector.SchemaEntry(nil), catalog.Entries...)
	if err := catalog.Normalize(); err != nil || catalog.SignatureScheme != "ed25519" || catalog.Signature == "" {
		return connector.ErrSchemaSignature
	}
	if err := validateTrustAuditActor(actor, catalog.WorkspaceID); err != nil {
		return err
	}
	workspaceID := strings.TrimSpace(catalog.WorkspaceID)
	revision, err := parseTrustRevision(catalog.Revision)
	if err != nil {
		return connector.ErrSchemaDowngrade
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	entriesJSON, err := json.Marshal(catalog.Entries)
	if err != nil {
		return connector.ErrSchemaSignature
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return fmt.Errorf("begin schema catalog publish: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-trust-catalog:' || $1, 0))`, workspaceID); err != nil {
		return fmt.Errorf("lock schema catalog workspace: %w", err)
	}
	var publicKey []byte
	var signerStatus string
	if err := tx.QueryRow(ctx, `
		SELECT public_key,status FROM fornix.trust_signers
		WHERE workspace_id=$1 AND signer_id=$2 ORDER BY id DESC LIMIT 1 FOR UPDATE`, workspaceID, catalog.SignerID).Scan(&publicKey, &signerStatus); errors.Is(err, pgx.ErrNoRows) {
		return ErrTrustSignerNotFound
	} else if err != nil {
		return fmt.Errorf("read schema catalog signer: %w", err)
	} else if signerStatus != "active" {
		return ErrTrustSignerRevoked
	}
	if err := catalog.Verify(map[string]ed25519.PublicKey{catalog.SignerID: ed25519.PublicKey(publicKey)}, now); err != nil {
		return err
	}
	var currentRevision int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(revision),0) FROM fornix.trust_schema_catalogs WHERE workspace_id=$1`, workspaceID).Scan(&currentRevision); err != nil {
		return fmt.Errorf("read schema catalog revision: %w", err)
	}
	if revision <= currentRevision {
		var existing string
		if err := tx.QueryRow(ctx, `SELECT catalog_hash FROM fornix.trust_schema_catalogs WHERE workspace_id=$1 AND catalog_hash=$2`, workspaceID, catalog.CatalogHash).Scan(&existing); err == nil && existing == catalog.CatalogHash {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return nil
		}
		return connector.ErrSchemaDowngrade
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.trust_schema_catalogs(workspace_id,revision,catalog_hash,entries,signature_scheme,signer_id,signature,issued_at,expires_at,actor)
		VALUES($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10::jsonb)`, workspaceID, revision, catalog.CatalogHash, entriesJSON, catalog.SignatureScheme, catalog.SignerID, catalog.Signature, catalog.IssuedAt.UTC(), catalog.ExpiresAt.UTC(), actorJSON); err != nil {
		if isUniqueViolation(err) {
			return ErrSchemaCatalogConflict
		}
		return fmt.Errorf("insert schema catalog: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit schema catalog: %w", err)
	}
	return nil
}

// CurrentSignedSchemaCatalog returns the highest non-expired catalog whose
// signer remains active, re-verifying its detached signature on every read.
func (s *TrustCatalogStore) CurrentSignedSchemaCatalog(ctx context.Context, workspaceID string, now time.Time) (connector.SchemaCatalog, ed25519.PublicKey, error) {
	if s == nil || s.pool == nil {
		return connector.SchemaCatalog{}, nil, fmt.Errorf("trust catalog is not configured")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return connector.SchemaCatalog{}, nil, ErrSchemaCatalogNotFound
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return connector.SchemaCatalog{}, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var revision int64
	var catalogHash, entriesRaw, scheme, signerID, signature string
	var issuedAt, expiresAt time.Time
	var publicKey []byte
	err = tx.QueryRow(ctx, `
		SELECT c.revision,c.catalog_hash,c.entries::text,c.signature_scheme,c.signer_id,c.signature,c.issued_at,c.expires_at,s.public_key
		FROM fornix.trust_schema_catalogs c
		JOIN fornix.trust_signers s ON s.workspace_id=c.workspace_id AND s.signer_id=c.signer_id AND s.status='active'
		WHERE c.workspace_id=$1 AND c.expires_at>clock_timestamp()
		ORDER BY c.revision DESC LIMIT 1`, workspaceID).Scan(&revision, &catalogHash, &entriesRaw, &scheme, &signerID, &signature, &issuedAt, &expiresAt, &publicKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return connector.SchemaCatalog{}, nil, ErrSchemaCatalogNotFound
	}
	if err != nil {
		return connector.SchemaCatalog{}, nil, fmt.Errorf("read current schema catalog: %w", err)
	}
	var entries []connector.SchemaEntry
	if err := json.Unmarshal([]byte(entriesRaw), &entries); err != nil {
		return connector.SchemaCatalog{}, nil, connector.ErrSchemaSignature
	}
	catalog := connector.SchemaCatalog{WorkspaceID: workspaceID, Revision: strconv.FormatInt(revision, 10), Entries: entries, CatalogHash: catalogHash, SignatureScheme: scheme, SignerID: signerID, Signature: signature, IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC()}
	key := ed25519.PublicKey(append([]byte(nil), publicKey...))
	if err := catalog.Verify(map[string]ed25519.PublicKey{signerID: key}, now); err != nil {
		return connector.SchemaCatalog{}, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return connector.SchemaCatalog{}, nil, err
	}
	return catalog, key, nil
}

// InstallCurrentSchemaCatalog loads and verifies the durable catalog before
// enabling signed-schema admission in the process-local registry.
func (s *TrustCatalogStore) InstallCurrentSchemaCatalog(ctx context.Context, registry *connector.Registry, workspaceID string, now time.Time) error {
	if registry == nil {
		return connector.ErrRegistryNil
	}
	catalog, publicKey, err := s.CurrentSignedSchemaCatalog(ctx, workspaceID, now)
	if err != nil {
		return err
	}
	if err := registry.SetTrustSigner(catalog.SignerID, publicKey); err != nil {
		return err
	}
	if err := registry.SetSignedSchemaCatalog(catalog, now); err != nil {
		return err
	}
	registry.RequireSignedSchemaCatalog(true)
	return nil
}

// InstallCurrent loads the durable policy into a process-local registry only
// after verification. Reinstalling the same policy is an idempotent no-op;
// installing an older or different revision remains the registry's explicit
// monotonic-admission decision.
func (s *TrustCatalogStore) InstallCurrent(ctx context.Context, registry *connector.Registry, workspaceID string, now time.Time) error {
	if registry == nil {
		return connector.ErrRegistryNil
	}
	policy, publicKey, err := s.CurrentSignedPolicy(ctx, workspaceID, now)
	if err != nil {
		return err
	}
	if err := registry.SetTrustSigner(policy.SignerID, publicKey); err != nil {
		return err
	}
	registry.RequireTrustPolicy(true)
	registry.RequireSignedTrustPolicy(true)
	if existing, ok := registry.TrustPolicy(workspaceID); ok && existing.StableHash() == policy.StableHash() && existing.Revision == policy.Revision && strings.TrimSpace(existing.Signature) != "" {
		return nil
	}
	return registry.SetSignedTrustPolicy(policy, now)
}

func validateTrustAuditActor(actor contracts.AuditActor, workspaceID string) error {
	if strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(actor.WorkspaceID) != strings.TrimSpace(workspaceID) || len(actor.ID) > 128 || len(actor.Kind) > 64 || len(actor.APIKeyID) > 128 {
		return fmt.Errorf("trust audit actor is invalid")
	}
	return nil
}

func validCatalogTrustEntries(entries []connector.TrustEntry) bool {
	if len(entries) == 0 {
		return false
	}
	for i, entry := range entries {
		if entry.ConnectorHash != strings.ToLower(entry.ConnectorHash) {
			return false
		}
		if _, err := hex.DecodeString(entry.ConnectorHash); err != nil || len(entry.ConnectorHash) != 64 {
			return false
		}
		if entry.CapabilityHash != strings.ToLower(entry.CapabilityHash) {
			return false
		}
		if _, err := hex.DecodeString(entry.CapabilityHash); err != nil || len(entry.CapabilityHash) != 64 {
			return false
		}
		if i > 0 && (entries[i-1].ConnectorHash > entry.ConnectorHash || entries[i-1].ConnectorHash == entry.ConnectorHash && entries[i-1].CapabilityHash >= entry.CapabilityHash) {
			return false
		}
	}
	return true
}

func parseTrustRevision(value string) (int64, error) {
	canonical := strings.TrimSpace(value)
	revision, err := strconv.ParseInt(canonical, 10, 64)
	if err != nil || revision <= 0 || canonical != strconv.FormatInt(revision, 10) {
		return 0, connector.ErrTrustDowngrade
	}
	return revision, nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func trustPublicKeyHash(value []byte) string {
	return hex.EncodeToString(trustSHA256Sum(value))
}

func trustSHA256Sum(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}
