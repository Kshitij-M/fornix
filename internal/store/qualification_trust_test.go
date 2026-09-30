package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

type qualificationTrustFixture struct {
	store     *QualificationTrustStore
	pool      *pgxpool.Pool
	workspace string
	actor     contracts.AuditActor
	keyID     string
	private   ed25519.PrivateKey
	public    ed25519.PublicKey
	target    string
}

func newQualificationTrustFixture(t *testing.T) *qualificationTrustFixture {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	workspace := fmt.Sprintf("test-qualification-trust-%d", time.Now().UnixNano())
	seed := bytes.Repeat([]byte{0x73}, ed25519.SeedSize)
	private := ed25519.NewKeyFromSeed(seed)
	fixture := &qualificationTrustFixture{
		store: NewQualificationTrustStore(pool), pool: pool, workspace: workspace,
		actor: contracts.AuditActor{ID: "qualification-operator", WorkspaceID: workspace, Kind: "test"},
		keyID: "deployment-key-v1", private: private, public: private.Public().(ed25519.PublicKey),
		target: contracts.HashStrings("qualification-target", workspace),
	}
	t.Cleanup(func() { pool.Close() })
	return fixture
}

func (f *qualificationTrustFixture) signed(t *testing.T, keyID string, private ed25519.PrivateKey, runID string) (contracts.SignedQualificationBundle, []byte) {
	t.Helper()
	report := contracts.QualificationReport{
		RunID: runID, WorkspaceID: f.workspace, TargetHash: f.target, Outcome: contracts.QualificationOutcomePassed,
		Cases: []contracts.QualificationCase{{Name: "authority", Category: contracts.QualificationCategoryAuthority, Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings("evidence", runID)}},
	}
	if err := report.Normalize(); err != nil {
		t.Fatalf("normalize qualification report: %v", err)
	}
	bundle := contracts.QualificationBundle{
		Report: report,
		Manifest: contracts.QualificationManifest{
			RunID: runID, WorkspaceID: f.workspace, TargetHash: f.target, ReportHash: report.ReportHash,
			RunnerVersion: "test", CommitHash: contracts.HashStrings("commit", runID),
			EnvironmentNames: []string{"FORNIX_MODE"},
			Checks:           []contracts.QualificationCheckManifest{{Name: "authority", Version: "1", Category: contracts.QualificationCategoryAuthority, ExecutionMode: "offline", InputHash: contracts.HashStrings("input", runID), Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings("evidence", runID)}},
		},
	}
	if err := bundle.Normalize(); err != nil {
		t.Fatalf("normalize qualification bundle: %v", err)
	}
	signed, err := contracts.SignQualificationBundle(bundle, keyID, private)
	if err != nil {
		t.Fatalf("sign qualification bundle: %v", err)
	}
	raw, err := json.Marshal(signed)
	if err != nil {
		t.Fatalf("marshal signed bundle: %v", err)
	}
	return signed, raw
}

func (f *qualificationTrustFixture) register(t *testing.T, keyID, publicKey string, supersedes string) (contracts.QualificationTrustedSigner, bool) {
	t.Helper()
	now := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	signer, created, err := f.store.RegisterSigner(context.Background(), contracts.QualificationTrustedSignerInput{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", KeyID: keyID, PublicKey: publicKey,
		ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), SupersedesKeyID: supersedes, Actor: f.actor,
	})
	if err != nil {
		t.Fatalf("register signer: %v", err)
	}
	return signer, created
}

func (f *qualificationTrustFixture) importRequest(signed contracts.SignedQualificationBundle, raw []byte, idempotency string, dryRun bool) contracts.QualificationImportRequest {
	return contracts.QualificationImportRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", TargetHash: f.target,
		SourceReference: "qualification-run", RequestID: "request-" + idempotency,
		IdempotencyKey: idempotency, SignedBundle: signed, SignedBytes: append([]byte(nil), raw...),
		Actor: f.actor, DryRun: dryRun,
	}
}

func (f *qualificationTrustFixture) publishSnapshot(t *testing.T, revision int64, publisherKey string, publisher ed25519.PrivateKey, entries []contracts.QualificationTrustSnapshotEntry) contracts.QualificationTrustSnapshotRecord {
	t.Helper()
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	snapshot, err := contracts.SignQualificationTrustSnapshot(contracts.QualificationTrustSnapshot{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", Revision: revision,
		IssuedAt: from, ExpiresAt: from.Add(24 * time.Hour), Entries: entries,
	}, publisherKey, publisher)
	if err != nil {
		t.Fatalf("sign trust snapshot: %v", err)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal trust snapshot: %v", err)
	}
	record, created, err := f.store.PublishSnapshot(context.Background(), contracts.QualificationTrustSnapshotPublishRequest{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", SourceReference: "test-snapshot",
		RequestID: fmt.Sprintf("snapshot-request-%d", revision), IdempotencyKey: fmt.Sprintf("snapshot-%d", revision),
		SignedSnapshot: snapshot, SignedBytes: raw, Actor: f.actor,
	}, from.Add(time.Hour))
	if err != nil || !created {
		t.Fatalf("publish trust snapshot record=%+v created=%v err=%v", record, created, err)
	}
	return record
}

func TestQualificationTrustImportsAreAuthorizedIdempotentAndDisclosable(t *testing.T) {
	f := newQualificationTrustFixture(t)
	f.register(t, f.keyID, fmt.Sprintf("%x", f.public), "")
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	f.publishSnapshot(t, 1, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{{DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public), ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour)}})
	signed, raw := f.signed(t, f.keyID, f.private, "qualification-run-1")
	first, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signed, raw, "import-1", false), time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC))
	if err != nil || !first.Created || first.Deduplicated {
		t.Fatalf("first import result=%+v err=%v", first, err)
	}
	second, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signed, raw, "import-1", false), time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC))
	if err != nil || second.Record.ID != first.Record.ID || !second.Deduplicated || second.Created {
		t.Fatalf("replay result=%+v err=%v", second, err)
	}
	if first.Record.TrustSnapshotRevision != 1 || first.Record.TrustSnapshotHash == "" {
		t.Fatalf("import did not bind trust snapshot: %+v", first.Record)
	}
	page, err := f.store.ListImports(context.Background(), f.workspace, "deployment-a", 100, "")
	if err != nil || len(page.Items) != 1 || page.Items[0].SignedHash != signed.Signature.SignedHash {
		t.Fatalf("import page=%+v err=%v", page, err)
	}
	disclosure, err := f.store.DiscloseImport(context.Background(), f.workspace, "deployment-a", first.Record.ID)
	if err != nil || !bytes.Equal(disclosure.SourceBytes, raw) || disclosure.SignedBundle.Signature.SignedHash != signed.Signature.SignedHash {
		t.Fatalf("disclosure differs: record=%+v err=%v", disclosure.Record, err)
	}
	if _, err := f.store.ImportAuthorized(context.Background(), f.importRequest(signed, raw, "different-idempotency", false), time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC)); !errors.Is(err, ErrQualificationImportConflict) {
		t.Fatalf("same signed identity with different idempotency err=%v", err)
	}
}

func TestQualificationTrustRotationRevocationAndDryRunFailClosed(t *testing.T) {
	f := newQualificationTrustFixture(t)
	f.register(t, f.keyID, fmt.Sprintf("%x", f.public), "")
	secondPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x74}, ed25519.SeedSize))
	secondPublic := secondPrivate.Public().(ed25519.PublicKey)
	rotated, created := f.register(t, "deployment-key-v2", fmt.Sprintf("%x", secondPublic), f.keyID)
	if !created || rotated.SupersedesKeyID != f.keyID {
		t.Fatalf("rotation signer=%+v created=%v", rotated, created)
	}
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	f.publishSnapshot(t, 1, "deployment-key-v2", secondPrivate, []contracts.QualificationTrustSnapshotEntry{{DeploymentID: "deployment-a", KeyID: "deployment-key-v2", PublicKey: fmt.Sprintf("%x", secondPublic), ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour)}})
	old, oldRaw := f.signed(t, f.keyID, f.private, "qualification-old")
	if _, err := f.store.ImportAuthorized(context.Background(), f.importRequest(old, oldRaw, "old-import", false), time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC)); !errors.Is(err, ErrQualificationSnapshotSignerUntrusted) {
		t.Fatalf("snapshot-excluded signer import err=%v", err)
	}
	newSigned, newRaw := f.signed(t, "deployment-key-v2", secondPrivate, "qualification-new")
	dry, err := f.store.ImportAuthorized(context.Background(), f.importRequest(newSigned, newRaw, "new-dry-run", true), time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC))
	if err != nil || !dry.Validated || !dry.DryRun || dry.Created {
		t.Fatalf("dry-run result=%+v err=%v", dry, err)
	}
	page, err := f.store.ListImports(context.Background(), f.workspace, "deployment-a", 100, "")
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("dry-run created durable import page=%+v err=%v", page, err)
	}
	if err := f.store.RevokeSigner(context.Background(), f.workspace, "deployment-a", "deployment-key-v2", f.actor); err != nil {
		t.Fatalf("revoke rotated signer: %v", err)
	}
	if _, err := f.store.ImportAuthorized(context.Background(), f.importRequest(newSigned, newRaw, "new-import", false), time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC)); !errors.Is(err, ErrQualificationSnapshotSignerUntrusted) {
		t.Fatalf("revoked snapshot publisher import err=%v", err)
	}
}

func TestQualificationTrustSnapshotReplayAndRevocation(t *testing.T) {
	f := newQualificationTrustFixture(t)
	f.register(t, f.keyID, fmt.Sprintf("%x", f.public), "")
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	first := f.publishSnapshot(t, 1, f.keyID, f.private, []contracts.QualificationTrustSnapshotEntry{{DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public), ValidFrom: from.Add(-time.Hour), ValidUntil: from.Add(24 * time.Hour)}})
	disclosure, err := f.store.DiscloseSnapshot(context.Background(), f.workspace, "deployment-a", first.ID)
	if err != nil || len(disclosure.SourceBytes) == 0 {
		t.Fatalf("snapshot disclosure err=%v", err)
	}
	request := contracts.QualificationTrustSnapshotPublishRequest{WorkspaceID: f.workspace, DeploymentID: "deployment-a", IdempotencyKey: "snapshot-1", SignedSnapshot: disclosure.SignedSnapshot, SignedBytes: disclosure.SourceBytes, Actor: f.actor}
	if replay, created, err := f.store.PublishSnapshot(context.Background(), request, from.Add(time.Hour)); err != nil || created || replay.ID != first.ID {
		t.Fatalf("snapshot replay record=%+v created=%v err=%v", replay, created, err)
	}
	if err := f.store.RevokeSnapshot(context.Background(), f.workspace, "deployment-a", first.ID, f.actor); err != nil {
		t.Fatalf("revoke snapshot: %v", err)
	}
	if _, err := f.store.CurrentSnapshot(context.Background(), f.workspace, "deployment-a", from.Add(time.Hour)); !errors.Is(err, ErrQualificationSnapshotNotFound) {
		t.Fatalf("revoked snapshot remained current: %v", err)
	}
}

func TestQualificationTrustConcurrentRegistrationHasOneCreatedSigner(t *testing.T) {
	f := newQualificationTrustFixture(t)
	input := contracts.QualificationTrustedSignerInput{
		WorkspaceID: f.workspace, DeploymentID: "deployment-a", KeyID: f.keyID, PublicKey: fmt.Sprintf("%x", f.public),
		ValidFrom: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC), ValidUntil: time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), Actor: f.actor,
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	created := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, didCreate, err := f.store.RegisterSigner(context.Background(), input)
			results <- err
			created <- didCreate
		}()
	}
	wg.Wait()
	close(results)
	close(created)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent registration error: %v", err)
		}
	}
	createdCount := 0
	for didCreate := range created {
		if didCreate {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count=%d, want one", createdCount)
	}
}
