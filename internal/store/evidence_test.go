package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func newEvidenceTestStore(t *testing.T) (*EvidenceStore, *pgxpool.Pool, string) {
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
	workspaceID := fmt.Sprintf("test-evidence-%d", time.Now().UnixNano())
	store := NewEvidenceStore(pool)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.provenance_edges WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.evidence_records WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	return store, pool, workspaceID
}

func evidenceInput(workspaceID, source, raw string) EvidencePutInput {
	return EvidencePutInput{
		WorkspaceID: workspaceID, SourceReference: source, DeduplicationKey: "stable-" + source,
		Kind: "memo", MediaType: "text/plain", Gist: "a durable gist",
		Detail: "the immutable detail for " + source, RawPayload: []byte(raw),
	}
}

func TestEvidenceDuplicateDisclosureAndStableHash(t *testing.T) {
	store, pool, workspaceID := newEvidenceTestStore(t)
	ctx := context.Background()
	input := evidenceInput(workspaceID, "memo:one", `{"answer":"alpha"}`)
	first, err := store.Put(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.Put(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || duplicate.Created || duplicate.Record.ID != first.Record.ID {
		t.Fatalf("duplicate result=%+v first=%+v", duplicate, first)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.evidence_records WHERE workspace_id=$1`, workspaceID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("duplicate created %d rows", count)
	}

	for _, level := range []contracts.DisclosureLevel{contracts.DisclosureGist, contracts.DisclosureDetail, contracts.DisclosureRaw} {
		request := contracts.DisclosureRequest{WorkspaceID: workspaceID, EvidenceID: first.Record.ID, Level: level, MaxBytes: 4096, MaxTokens: 4096}
		one, err := store.Disclose(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		two, err := store.Disclose(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if one.ContentHash == "" || one.ContentHash != two.ContentHash {
			t.Fatalf("unstable %s disclosure: one=%+v two=%+v", level, one, two)
		}
		if one.EvidenceHash != first.Record.EvidenceHash || !one.IntegrityVerified {
			t.Fatalf("disclosure lost integrity metadata: %+v", one)
		}
		if level == contracts.DisclosureGist && (one.Detail != "" || len(one.RawPayload) != 0) {
			t.Fatalf("gist disclosure leaked deeper content: %+v", one)
		}
		if level == contracts.DisclosureDetail && one.Detail == "" || level == contracts.DisclosureRaw && !bytes.Equal(one.RawPayload, input.RawPayload) {
			t.Fatalf("unexpected %s body: %+v", level, one)
		}
	}

	conflict := input
	conflict.Detail = "a different derived view"
	if _, err := store.Put(ctx, conflict); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("conflicting duplicate error=%v", err)
	}
}

func TestEvidenceSupersessionContradictionTraversalAndWorkspaceIsolation(t *testing.T) {
	store, _, workspaceID := newEvidenceTestStore(t)
	ctx := context.Background()
	old, err := store.Put(ctx, evidenceInput(workspaceID, "memo:old", `{"version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.Put(ctx, evidenceInput(workspaceID, "memo:other", `{"version":0}`))
	if err != nil {
		t.Fatal(err)
	}
	newInput := evidenceInput(workspaceID, "memo:new", `{"version":2}`)
	newInput.SupersedesID = &old.Record.ID
	newInput.Contradicts = []int64{other.Record.ID}
	newRecord, err := store.Put(ctx, newInput)
	if err != nil {
		t.Fatal(err)
	}
	if newRecord.Record.SupersedesID == nil || *newRecord.Record.SupersedesID != old.Record.ID {
		t.Fatalf("supersession metadata=%+v", newRecord.Record)
	}

	disclosed, err := store.Disclose(ctx, contracts.DisclosureRequest{WorkspaceID: workspaceID, EvidenceID: old.Record.ID, Level: contracts.DisclosureGist, MaxDepth: 1, MaxNodes: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(disclosed.SupersededBy) != 1 || disclosed.SupersededBy[0] != newRecord.Record.ID {
		t.Fatalf("supersession disclosure=%+v", disclosed)
	}
	if len(disclosed.Provenance) != 1 || disclosed.Provenance[0].Relation != contracts.RelationSupersedes {
		t.Fatalf("provenance disclosure=%+v", disclosed.Provenance)
	}
	if len(disclosed.ContradictedBy) != 0 {
		t.Fatalf("old evidence should not be contradicted=%v", disclosed.ContradictedBy)
	}

	newDisclosure, err := store.Disclose(ctx, contracts.DisclosureRequest{WorkspaceID: workspaceID, EvidenceID: newRecord.Record.ID, Level: contracts.DisclosureDetail, MaxDepth: 3, MaxNodes: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(newDisclosure.ContradictedBy) != 1 || newDisclosure.ContradictedBy[0] != other.Record.ID {
		t.Fatalf("contradiction disclosure=%+v", newDisclosure)
	}
	traversed, err := store.Traverse(ctx, contracts.ProvenanceTraversalRequest{WorkspaceID: workspaceID, EvidenceID: newRecord.Record.ID, MaxDepth: 3, MaxNodes: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(traversed) != 2 || traversed[0].Relation != contracts.RelationContradicts || traversed[1].Relation != contracts.RelationSupersedes {
		t.Fatalf("deterministic traversal=%+v", traversed)
	}

	foreign := workspaceID + "-foreign"
	if _, err := store.Disclose(ctx, contracts.DisclosureRequest{WorkspaceID: foreign, EvidenceID: old.Record.ID}); !errors.Is(err, ErrEvidenceNotFound) {
		t.Fatalf("cross-workspace disclosure error=%v", err)
	}
	if _, err := store.AddEdge(ctx, contracts.ProvenanceEdgeInput{WorkspaceID: foreign, FromEvidenceID: newRecord.Record.ID, ToEvidenceID: old.Record.ID, Relation: contracts.RelationSupports}); !errors.Is(err, ErrEvidenceNotFound) {
		t.Fatalf("cross-workspace edge error=%v", err)
	}
}

func TestEvidenceAppendOnlyCycleAndBoundedDisclosure(t *testing.T) {
	store, pool, workspaceID := newEvidenceTestStore(t)
	ctx := context.Background()
	a, err := store.Put(ctx, evidenceInput(workspaceID, "a", `{"a":true}`))
	if err != nil {
		t.Fatal(err)
	}
	bInput := evidenceInput(workspaceID, "b", `{"b":true}`)
	bInput.SupersedesID = &a.Record.ID
	b, err := store.Put(ctx, bInput)
	if err != nil {
		t.Fatal(err)
	}
	cInput := evidenceInput(workspaceID, "c", `{"c":true}`)
	cInput.SupersedesID = &b.Record.ID
	c, err := store.Put(ctx, cInput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddEdge(ctx, contracts.ProvenanceEdgeInput{WorkspaceID: workspaceID, FromEvidenceID: b.Record.ID, ToEvidenceID: c.Record.ID, Relation: contracts.RelationSupersedes}); !errors.Is(err, ErrEvidenceCycle) {
		t.Fatalf("cycle error=%v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE fornix.evidence_records SET gist='tampered' WHERE id=$1`, a.Record.ID); err == nil {
		t.Fatal("evidence update unexpectedly succeeded")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM fornix.evidence_records WHERE id=$1`, a.Record.ID); err == nil {
		t.Fatal("evidence delete unexpectedly succeeded")
	}

	long := evidenceInput(workspaceID, "long", `{"raw":"01234567890123456789"}`)
	long.Gist = "a deliberately long gist that must be bounded"
	long.Detail = "detail that must not escape the hard disclosure budget"
	created, err := store.Put(ctx, long)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Disclose(ctx, contracts.DisclosureRequest{WorkspaceID: workspaceID, EvidenceID: created.Record.ID, Level: contracts.DisclosureRaw, MaxBytes: 12, MaxTokens: 3, MaxDepth: 1, MaxNodes: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalBytes > 12 || result.TotalTokens > 3 || !result.Truncated {
		t.Fatalf("hard disclosure budget violated=%+v", result)
	}
	if len(result.RawPayload) != 0 {
		t.Fatal("partial raw payload was disclosed")
	}
	if result.EvidenceHash == "" || result.RawSizeBytes != int64(len(long.RawPayload)) {
		t.Fatalf("bounded result lost evidence identity=%+v", result)
	}
}

func TestEvidenceConcurrentDuplicateWriters(t *testing.T) {
	store, pool, workspaceID := newEvidenceTestStore(t)
	input := evidenceInput(workspaceID, "concurrent", `{"same":true}`)
	const writers = 12
	results := make(chan EvidencePutResult, writers)
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
	var ids []int64
	created := 0
	for result := range results {
		ids = append(ids, result.Record.ID)
		if result.Created {
			created++
		}
	}
	if created != 1 || len(ids) != writers {
		t.Fatalf("concurrent dedupe created=%d results=%d", created, len(ids))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("duplicate writers returned distinct IDs=%v", ids)
		}
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.evidence_records WHERE workspace_id=$1`, workspaceID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent duplicate rows=%d", count)
	}
}

func TestEventAppendCreatesTransactionalEvidence(t *testing.T) {
	store, pool, workspaceID := newEvidenceTestStore(t)
	events := NewEventStore(pool)
	event, err := contracts.NewEvent("evidence.test", map[string]any{"value": "raw"})
	if err != nil {
		t.Fatal(err)
	}
	event.Scope.WorkspaceID = workspaceID
	event.IdempotencyKey = "event-evidence-once"
	first, err := events.Append(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := events.Append(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.Event.Sequence != first.Event.Sequence {
		t.Fatalf("duplicate event=%+v first=%+v", duplicate, first)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.evidence_records WHERE workspace_id=$1 AND kind='control_event'`, workspaceID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("event evidence count=%d", count)
	}

	_ = store
}

func TestEvidenceDisclosureLatencyAndStorageImpact(t *testing.T) {
	store, pool, workspaceID := newEvidenceTestStore(t)
	created, err := store.Put(context.Background(), evidenceInput(workspaceID, "latency", `{"latency":true}`))
	if err != nil {
		t.Fatal(err)
	}
	samples := make([]time.Duration, 0, 20)
	for i := 0; i < 20; i++ {
		started := time.Now()
		if _, err := store.Disclose(context.Background(), contracts.DisclosureRequest{WorkspaceID: workspaceID, EvidenceID: created.Record.ID, Level: contracts.DisclosureGist, MaxBytes: 1024, MaxTokens: 256, IncludeProvenance: boolPtr(false)}); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, time.Since(started))
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var relationBytes int64
	if err := pool.QueryRow(context.Background(), `SELECT pg_total_relation_size('fornix.evidence_records') + pg_total_relation_size('fornix.provenance_edges')`).Scan(&relationBytes); err != nil {
		t.Fatal(err)
	}
	t.Logf("disclosure latency samples=%d p50=%s p95=%s max=%s query_count=3 evidence_graph_relation_bytes=%d", len(samples), samples[len(samples)/2], samples[(len(samples)*95)/100], samples[len(samples)-1], relationBytes)
}

func boolPtr(value bool) *bool { return &value }
