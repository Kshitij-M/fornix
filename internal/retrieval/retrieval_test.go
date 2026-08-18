package retrieval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pgvector "github.com/pgvector/pgvector-go"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestPlanAndCompileAreDeterministic(t *testing.T) {
	request := contracts.RetrievalRequest{
		WorkspaceID: "workspace-a", Query: "alpha", ExactSourceRefs: []string{"memo:2", "memo:1", "memo:2"},
		MaxItems: 3, MaxBytes: 32, MaxTokens: 8,
	}
	firstPlan, normalized, err := BuildPlan(request)
	if err != nil {
		t.Fatal(err)
	}
	secondPlan, _, err := BuildPlan(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(firstPlan, secondPlan) || PlanHash(firstPlan) != PlanHash(secondPlan) {
		t.Fatalf("plans are not deterministic: first=%+v second=%+v", firstPlan, secondPlan)
	}
	items := []contracts.ContextItem{
		{WorkspaceID: normalized.WorkspaceID, SourceReference: "memo:2", Kind: "memo", Text: "beta evidence", EvidenceHash: "b", Score: 0.8},
		{WorkspaceID: normalized.WorkspaceID, SourceReference: "memo:1", Kind: "memo", Text: "alpha evidence that will be bounded", EvidenceHash: "a", Score: 1},
	}
	first := Compile(firstPlan.RequestHash, normalized.WorkspaceID, firstPlan.Budget, items)
	second := Compile(firstPlan.RequestHash, normalized.WorkspaceID, firstPlan.Budget, items)
	if first.ContentHash == "" || first.ContentHash != second.ContentHash {
		t.Fatalf("content hash is not stable: first=%q second=%q", first.ContentHash, second.ContentHash)
	}
	if first.TotalBytes > firstPlan.Budget.MaxBytes || first.TotalTokens > firstPlan.Budget.MaxTokens || len(first.Items) > firstPlan.Budget.MaxItems {
		t.Fatalf("hard budget exceeded: pack=%+v budget=%+v", first, firstPlan.Budget)
	}
	if !first.Truncated {
		t.Fatal("expected deterministic truncation")
	}
	if !validUTF8(first.Items[0].Text) {
		t.Fatal("truncation produced invalid UTF-8")
	}
}

func TestPlanGatesVectorWithoutEmbedding(t *testing.T) {
	plan, _, err := BuildPlan(contracts.RetrievalRequest{WorkspaceID: "workspace-a", Query: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	vector := plan.Stages[len(plan.Stages)-1]
	if vector.Enabled || vector.Reason != "query_embedding_not_supplied" {
		t.Fatalf("vector gate = %+v, want disabled without embedding", vector)
	}
}

func TestPlanRejectsInvalidEmbedding(t *testing.T) {
	_, _, err := BuildPlan(contracts.RetrievalRequest{WorkspaceID: "workspace-a", QueryEmbedding: []float32{1}})
	if err == nil || !strings.Contains(err.Error(), "dimensions") {
		t.Fatalf("invalid embedding error = %v, want dimension validation", err)
	}
}

func TestCompilerAbstainsWhenNoItemFits(t *testing.T) {
	pack := Compile("request", "workspace-a", contracts.RetrievalBudget{MaxItems: 2, MaxBytes: 1, MaxTokens: 1}, []contracts.ContextItem{{
		WorkspaceID: "workspace-b", SourceReference: "memo:1", Kind: "memo", Text: "too large", EvidenceHash: "hash", Score: 1,
	}})
	if !pack.Abstained || len(pack.Items) != 0 || pack.ContentHash == "" {
		t.Fatalf("expected explicit abstention, got %+v", pack)
	}
}

func newRetrievalTestStore(t *testing.T) (*Store, *pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workspaceID := fmt.Sprintf("test-retrieval-%d", time.Now().UnixNano())
	otherWorkspace := workspaceID + "-other"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.symbol_edges WHERE src_id IN (SELECT id FROM fornix.symbols WHERE workspace_id=ANY($1)) OR dst_id IN (SELECT id FROM fornix.symbols WHERE workspace_id=ANY($1))`, []string{workspaceID, otherWorkspace})
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.symbols WHERE workspace_id=ANY($1)`, []string{workspaceID, otherWorkspace})
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.memos WHERE workspace_id=ANY($1)`, []string{workspaceID, otherWorkspace})
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.chunks WHERE workspace_id=ANY($1)`, []string{workspaceID, otherWorkspace})
		pool.Close()
	})
	return NewStore(pool), pool, workspaceID
}

func TestRetrieveIsWorkspaceScopedAndStable(t *testing.T) {
	retriever, pool, workspaceID := newRetrievalTestStore(t)
	otherWorkspace := workspaceID + "-other"
	insertMemo(t, pool, workspaceID, "alpha source", "alpha evidence for workspace A")
	insertMemo(t, pool, otherWorkspace, "alpha source", "alpha evidence for workspace B")
	request := contracts.RetrievalRequest{WorkspaceID: workspaceID, Query: "alpha", MaxItems: 2, MinResults: 1, MinScore: 0.75}
	first, err := retriever.Retrieve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := retriever.Retrieve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Pack.ContentHash != second.Pack.ContentHash || !reflect.DeepEqual(first.Pack.Items, second.Pack.Items) {
		t.Fatalf("repeated retrieval changed: first=%+v second=%+v", first.Pack, second.Pack)
	}
	if len(first.Pack.Items) != 1 || first.Pack.Items[0].WorkspaceID != workspaceID || strings.Contains(first.Pack.Items[0].Text, "workspace B") {
		t.Fatalf("workspace leaked or unexpected result: %+v", first.Pack.Items)
	}
	if first.Trace.Stages[2].Status != "skipped" || first.Trace.Stages[3].Status != "skipped" {
		t.Fatalf("expensive stages should be skipped without graph anchors/embedding: %+v", first.Trace.Stages)
	}

	const readers = 8
	results := make(chan string, readers)
	errors := make(chan error, readers)
	var group sync.WaitGroup
	for i := 0; i < readers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := retriever.Retrieve(context.Background(), request)
			if err != nil {
				errors <- err
				return
			}
			results <- result.Pack.ContentHash
		}()
	}
	group.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	for hash := range results {
		if hash != first.Pack.ContentHash {
			t.Fatalf("concurrent reader changed content hash: %s vs %s", hash, first.Pack.ContentHash)
		}
	}
}

func TestRetrieveDeduplicatesAndExpandsGraphDeterministically(t *testing.T) {
	retriever, pool, workspaceID := newRetrievalTestStore(t)
	memoID := insertMemo(t, pool, workspaceID, "exact alpha", "alpha duplicate evidence")
	request := contracts.RetrievalRequest{
		WorkspaceID: workspaceID, ExactSourceRefs: []string{fmt.Sprintf("memo:%d", memoID)}, MemoType: "general",
		MaxItems: 1, MinResults: 1, MinScore: 0.75,
	}
	result, err := retriever.Retrieve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Pack.Items) != 1 || result.Trace.Duplicates == 0 {
		t.Fatalf("duplicate structured delivery was not collapsed: %+v", result)
	}
	if result.Trace.Stages[1].Status != "skipped" || result.Trace.Stages[2].Status != "skipped" {
		t.Fatalf("later stages should be skipped after exact budget satisfaction: %+v", result.Trace.Stages)
	}

	rootID := insertSymbol(t, pool, workspaceID, "rootAlpha", "root signature")
	neighborID := insertSymbol(t, pool, workspaceID, "neighborAlpha", "neighbor signature")
	_, err = pool.Exec(context.Background(), `INSERT INTO fornix.symbol_edges(src_id,dst_id,edge_kind) VALUES($1,$2,'calls')`, rootID, neighborID)
	if err != nil {
		t.Fatal(err)
	}
	graphResult, err := retriever.Retrieve(context.Background(), contracts.RetrievalRequest{
		WorkspaceID: workspaceID, Query: "rootAlpha", MaxItems: 3, MinResults: 2, MinScore: 0.75,
	})
	if err != nil {
		t.Fatal(err)
	}
	refs := make([]string, 0, len(graphResult.Pack.Items))
	for _, item := range graphResult.Pack.Items {
		refs = append(refs, item.SourceReference)
	}
	if !containsString(refs, fmt.Sprintf("symbol:%d", neighborID)) {
		t.Fatalf("graph neighbor missing: refs=%v trace=%+v", refs, graphResult.Trace)
	}
	if graphResult.Trace.Stages[2].Status != "completed" {
		t.Fatalf("graph stage was not completed: %+v", graphResult.Trace.Stages)
	}
}

func TestRetrieveVectorRunsOnlyWhenJustified(t *testing.T) {
	retriever, pool, workspaceID := newRetrievalTestStore(t)
	vector := make([]float32, 768)
	hash := sha256hex("vector memo")
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO fornix.memos(workspace_id,title,content,type,tags,sha256,embedding)
		VALUES($1,'vector','vector memo','general','{}',$2,$3)
		RETURNING id`, workspaceID, hash, pgvector.NewVector(vector)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	result, err := retriever.Retrieve(context.Background(), contracts.RetrievalRequest{
		WorkspaceID: workspaceID, QueryEmbedding: vector, EnableGraph: boolPtr(false), MaxItems: 1, MinResults: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Pack.Items) != 1 || result.Pack.Items[0].SourceReference != fmt.Sprintf("memo:%d", id) {
		t.Fatalf("vector result missing: %+v", result.Pack)
	}
	if result.Trace.Stages[3].Status != "completed" || result.Trace.Stages[3].Queries == 0 {
		t.Fatalf("justified vector stage did not run: %+v", result.Trace.Stages)
	}
}

func TestRetrieveLatencyAndStorageImpact(t *testing.T) {
	retriever, pool, workspaceID := newRetrievalTestStore(t)
	for i := 0; i < 10; i++ {
		insertMemo(t, pool, workspaceID, fmt.Sprintf("latency alpha %02d", i), "alpha retrieval measurement")
	}
	request := contracts.RetrievalRequest{WorkspaceID: workspaceID, Query: "alpha", MaxItems: 5, MaxBytes: 4096, MaxTokens: 1024}
	times := make([]time.Duration, 0, 20)
	queries := 0
	for i := 0; i < 20; i++ {
		started := time.Now()
		result, err := retriever.Retrieve(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		times = append(times, time.Since(started))
		for _, stage := range result.Trace.Stages {
			queries += stage.Queries
		}
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	var relationBytes, logicalBytes int64
	if err := pool.QueryRow(context.Background(), `SELECT pg_total_relation_size('fornix.memos')`).Scan(&relationBytes); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT COALESCE(sum(length(title)+length(content)+length(sha256)),0) FROM fornix.memos WHERE workspace_id=$1`, workspaceID).Scan(&logicalBytes); err != nil {
		t.Fatal(err)
	}
	t.Logf("retrieval warm samples=20 p50=%s p95=%s max=%s avg_sql_queries=%.2f relation_bytes=%d workspace_logical_bytes=%d", times[9], times[18], times[19], float64(queries)/20, relationBytes, logicalBytes)
}

func insertMemo(t *testing.T, pool *pgxpool.Pool, workspaceID, title, content string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO fornix.memos(workspace_id,title,content,type,tags,sha256)
		VALUES($1,$2,$3,'general','{}',$4) RETURNING id`, workspaceID, title, content, sha256hex(title+"\n"+content)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func insertSymbol(t *testing.T, pool *pgxpool.Pool, workspaceID, name, signature string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO fornix.symbols(workspace_id,repo,file_path,symbol_name,symbol_kind,language,line_start,line_end,signature,docstring,sha256)
		VALUES($1,'repo','file.go',$2,'function','go',1,2,$3,'',$4) RETURNING id`, workspaceID, name, signature, sha256hex(name+signature)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func sha256hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func boolPtr(value bool) *bool { return &value }

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
