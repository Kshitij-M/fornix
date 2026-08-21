package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func newArtifactTestStore(t *testing.T) (*ArtifactStore, *pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping test database: %v", err)
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	workspaceID := fmt.Sprintf("test-artifacts-%d", time.Now().UnixNano())
	t.Cleanup(pool.Close)
	return NewArtifactStore(pool), pool, workspaceID
}

func artifactInput(workspaceID, sourceID, key string, raw []byte) ArtifactPutInput {
	return ArtifactPutInput{
		WorkspaceID: workspaceID, Kind: "tool-output", MediaType: "text/plain", Raw: raw,
		Manifest:   contracts.ArtifactManifest{Gist: "stable output", Detail: "bounded derived detail"},
		SourceKind: "tool_run", SourceID: sourceID, Role: "stdout", IdempotencyKey: key,
		Actor: contracts.ActorRef{ID: "actor-1", Kind: "test", WorkspaceID: workspaceID},
	}
}

func TestArtifactStoreConcurrentContentDedupAndStableDisclosure(t *testing.T) {
	store, pool, workspaceID := newArtifactTestStore(t)
	raw := []byte(strings.Repeat("fornix-artifact\n", 25_000))
	input := artifactInput(workspaceID, "tool-1", "artifact-write-once", raw)
	const writers = 12
	results := make(chan ArtifactPutResult, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.Put(context.Background(), input)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	created, refs := 0, 0
	var first ArtifactPutResult
	for result := range results {
		if result.Created {
			created++
		}
		if result.RefCreated {
			refs++
		}
		if first.Artifact.ID == 0 {
			first = result
		}
		if result.Artifact.ID != first.Artifact.ID || result.Artifact.ContentHash != first.Artifact.ContentHash {
			t.Fatalf("concurrent dedup returned different artifact: first=%+v result=%+v", first, result)
		}
	}
	if created != 1 || refs != 1 {
		t.Fatalf("concurrent dedup created=%d refs=%d", created, refs)
	}
	var artifactCount, refCount int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.artifacts WHERE workspace_id=$1`, workspaceID).Scan(&artifactCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.artifact_refs WHERE workspace_id=$1`, workspaceID).Scan(&refCount); err != nil {
		t.Fatal(err)
	}
	if artifactCount != 1 || refCount != 1 {
		t.Fatalf("durable dedup counts artifacts=%d refs=%d", artifactCount, refCount)
	}

	request := contracts.ArtifactDisclosureRequest{
		WorkspaceID: workspaceID, ArtifactID: first.Artifact.ID, Level: contracts.ArtifactDisclosureRaw,
		MaxBytes: len(raw) + 1024, MaxTokens: contracts.EstimateTokens(string(raw)) + 1024,
		MaxItems: 8, IncludeProvenance: true, MaxDepth: 2,
	}
	one, err := store.Disclose(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	two, err := store.Disclose(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(one.Raw, raw) || one.ContentHash != contracts.ArtifactContentHash(raw) || one.ContentViewHash != two.ContentViewHash || !one.IntegrityVerified {
		t.Fatalf("unstable or unverifiable disclosure: one=%+v two=%+v", one, two)
	}
	if one.TotalBytes > request.MaxBytes || one.TotalTokens > request.MaxTokens {
		t.Fatalf("disclosure exceeded budget: %+v", one)
	}

	bounded, err := store.Disclose(context.Background(), contracts.ArtifactDisclosureRequest{
		WorkspaceID: workspaceID, ArtifactID: first.Artifact.ID, Level: contracts.ArtifactDisclosureRaw,
		MaxBytes: 12, MaxTokens: 3, MaxItems: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bounded.Truncated || len(bounded.Raw) != 0 || bounded.TotalBytes > 12 || bounded.TotalTokens > 3 {
		t.Fatalf("raw disclosure budget was not hard bounded: %+v", bounded)
	}
}

func TestArtifactStoreWorkspaceProvenanceIntegrityAndCrashRollback(t *testing.T) {
	store, pool, workspaceID := newArtifactTestStore(t)
	ctx := context.Background()
	first, err := store.Put(ctx, artifactInput(workspaceID, "evidence-1", "evidence-artifact-1", []byte("first artifact")))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(ctx, artifactInput(workspaceID, "evidence-2", "evidence-artifact-2", []byte("second artifact")))
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := store.AddProvenance(ctx, ArtifactProvenanceInput{WorkspaceID: workspaceID, FromArtifact: second.Artifact.ID, ToArtifact: first.Artifact.ID, Relation: "derives"}); err != nil || !created {
		t.Fatalf("add provenance created=%v err=%v", created, err)
	}
	linkAgain, created, err := store.AddProvenance(ctx, ArtifactProvenanceInput{WorkspaceID: workspaceID, FromArtifact: second.Artifact.ID, ToArtifact: first.Artifact.ID, Relation: "derives"})
	if err != nil || created || linkAgain.ID == 0 {
		t.Fatalf("duplicate provenance created=%v link=%+v err=%v", created, linkAgain, err)
	}
	disclosed, err := store.Disclose(ctx, contracts.ArtifactDisclosureRequest{WorkspaceID: workspaceID, ArtifactID: second.Artifact.ID, Level: contracts.ArtifactDisclosureDetail, MaxBytes: 4096, MaxTokens: 4096, MaxItems: 8, IncludeProvenance: true, MaxDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(disclosed.Provenance) != 1 || disclosed.Provenance[0].ToArtifact != first.Artifact.ID {
		t.Fatalf("deterministic provenance disclosure=%+v", disclosed.Provenance)
	}
	foreign := workspaceID + "-foreign"
	if _, err := store.Get(ctx, foreign, first.Artifact.ID); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("cross-workspace artifact read error=%v", err)
	}
	if _, err := store.Disclose(ctx, contracts.ArtifactDisclosureRequest{WorkspaceID: foreign, ArtifactID: first.Artifact.ID}); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("cross-workspace disclosure error=%v", err)
	}
	if err := store.Verify(ctx, workspaceID, first.Artifact.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE fornix.artifact_chunks SET raw_bytes='tampered' WHERE workspace_id=$1 AND artifact_id=$2`, workspaceID, first.Artifact.ID); err == nil {
		t.Fatal("raw chunk update unexpectedly succeeded")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM fornix.artifact_chunks WHERE workspace_id=$1 AND artifact_id=$2 AND chunk_index=0`, workspaceID, first.Artifact.ID); err != nil {
		t.Fatalf("remove chunk for corruption test: %v", err)
	}
	if err := store.Verify(ctx, workspaceID, first.Artifact.ID); !errors.Is(err, ErrArtifactIntegrity) {
		t.Fatalf("corruption error=%v", err)
	}

	store.SetFailureHook(func(stage string) error {
		if stage == "reference_inserted" {
			return errors.New("injected artifact crash")
		}
		return nil
	})
	_, err = store.Put(ctx, artifactInput(workspaceID, "crash-source", "crash-on-reference", []byte("transaction must roll back")))
	store.SetFailureHook(nil)
	if err == nil || !strings.Contains(err.Error(), "injected artifact crash") {
		t.Fatalf("crash hook error=%v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifacts WHERE workspace_id=$1 AND content_hash=$2`, workspaceID, contracts.ArtifactContentHash([]byte("transaction must roll back"))).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("crash left orphan artifact count=%d", count)
	}
}

func TestArtifactStoreRetentionDeletionSafety(t *testing.T) {
	store, _, workspaceID := newArtifactTestStore(t)
	ctx := context.Background()
	deleteAfter := time.Now().UTC().Add(-time.Minute)
	input := artifactInput(workspaceID, "retained", "retained-artifact", []byte("retention payload"))
	input.Retention = contracts.RetentionPolicy{DeleteAfter: &deleteAfter, AllowDelete: true}
	artifact, err := store.Put(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Delete(ctx, workspaceID, artifact.Artifact.ID); !errors.Is(err, ErrArtifactRetention) {
		t.Fatalf("unarchived deletion error=%v", err)
	}
	if _, err := store.Archive(ctx, workspaceID, artifact.Artifact.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Delete(ctx, workspaceID, artifact.Artifact.ID); !errors.Is(err, ErrArtifactReferenced) {
		t.Fatalf("authoritative reference deletion error=%v", err)
	}

	nonAuth := input
	nonAuth.Raw = []byte("deletable payload")
	nonAuth.SourceID = "non-authoritative"
	nonAuth.IdempotencyKey = "non-authoritative-artifact"
	nonAuth.NonAuthoritative = true
	deletable, err := store.Put(ctx, nonAuth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Archive(ctx, workspaceID, deletable.Artifact.ID); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.Delete(ctx, workspaceID, deletable.Artifact.ID)
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Status != contracts.ArtifactDeleted {
		t.Fatalf("deletion did not create tombstone=%+v", deleted)
	}
	if _, err := store.Disclose(ctx, contracts.ArtifactDisclosureRequest{WorkspaceID: workspaceID, ArtifactID: deleted.ID}); !errors.Is(err, ErrArtifactDeleted) {
		t.Fatalf("deleted disclosure error=%v", err)
	}
}

func TestArtifactStoreLatencyAndStorageImpact(t *testing.T) {
	store, pool, workspaceID := newArtifactTestStore(t)
	ctx := context.Background()
	times := make([]time.Duration, 0, 20)
	for i := 0; i < 20; i++ {
		raw := []byte(fmt.Sprintf("artifact-%02d\n%s", i, strings.Repeat("bounded-content ", 2048)))
		started := time.Now()
		if _, err := store.Put(ctx, artifactInput(workspaceID, fmt.Sprintf("latency-%d", i), fmt.Sprintf("latency-key-%d", i), raw)); err != nil {
			t.Fatal(err)
		}
		times = append(times, time.Since(started))
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	var relationBytes, artifactRows, chunkRows, refRows int64
	if err := pool.QueryRow(ctx, `SELECT pg_total_relation_size('fornix.artifacts') + pg_total_relation_size('fornix.artifact_chunks') + pg_total_relation_size('fornix.artifact_refs') + pg_total_relation_size('fornix.artifact_provenance')`).Scan(&relationBytes); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifacts WHERE workspace_id=$1`, workspaceID).Scan(&artifactRows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifact_chunks WHERE workspace_id=$1`, workspaceID).Scan(&chunkRows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifact_refs WHERE workspace_id=$1`, workspaceID).Scan(&refRows); err != nil {
		t.Fatal(err)
	}
	t.Logf("artifact create+reference samples=%d p50=%s p95=%s max=%s artifact_rows=%d chunk_rows=%d ref_rows=%d artifact_relation_bytes=%d", len(times), times[9], times[19], times[len(times)-1], artifactRows, chunkRows, refRows, relationBytes)
}
