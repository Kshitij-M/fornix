package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestOversizedToolOutputIsArtifactBackedAndIdempotent(t *testing.T) {
	store, _, workspace := newToolRunTestStore(t)
	ctx := context.Background()
	startedAt := time.Now()
	request := durableToolRequest(workspace, "oversized-output")
	run, _, err := store.Reserve(ctx, request, contracts.ToolModeAutomatic)
	if err != nil {
		t.Fatal(err)
	}
	run, err = store.MarkStarted(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	stdout := strings.Repeat("output-", contracts.MaxToolEvidenceBytes/len("output-")+100)
	finished, err := store.Finish(ctx, run, contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: stdout, Stderr: "small stderr"})
	if err != nil {
		t.Fatal(err)
	}
	if finished.StdoutArtifact == nil || finished.ResultArtifact == nil {
		t.Fatalf("oversized output refs missing: %+v", finished)
	}
	if len(finished.Result.Stdout) >= len(stdout) || !strings.Contains(finished.Result.Stdout, "fornix-artifact") {
		t.Fatalf("inline compatibility marker missing: len=%d result=%+v", len(finished.Result.Stdout), finished.Result)
	}
	disclosed, err := store.artifacts.Disclose(ctx, contracts.ArtifactDisclosureRequest{WorkspaceID: workspace, ArtifactID: finished.StdoutArtifact.ArtifactID, Level: contracts.ArtifactDisclosureRaw, MaxBytes: contracts.MaxArtifactDisclosureBytes, MaxTokens: contracts.MaxArtifactDisclosureTokens})
	if err != nil {
		t.Fatal(err)
	}
	if string(disclosed.Raw) != stdout || disclosed.ContentHash != finished.StdoutArtifact.ContentHash {
		t.Fatalf("stdout artifact mismatch: bytes=%d hash=%s ref=%+v", len(disclosed.Raw), disclosed.ContentHash, finished.StdoutArtifact)
	}
	replayed, err := store.Get(ctx, workspace, request.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.StdoutArtifact == nil || replayed.StdoutArtifact.ArtifactID != finished.StdoutArtifact.ArtifactID || replayed.ResultArtifact.ArtifactID != finished.ResultArtifact.ArtifactID {
		t.Fatalf("duplicate read changed artifact identity: %+v", replayed)
	}
	if _, err := store.Finish(ctx, run, contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: stdout}); err != nil {
		t.Fatal("terminal duplicate should be replayable: ", err)
	}
	t.Logf("oversized tool output path bytes=%d latency=%s", len(stdout), time.Since(startedAt))
}

func TestArtifactBackedToolOutputCrashRollsBackSourceAndReference(t *testing.T) {
	store, pool, workspace := newToolRunTestStore(t)
	ctx := context.Background()
	request := durableToolRequest(workspace, "artifact-output-crash")
	run, _, err := store.Reserve(ctx, request, contracts.ToolModeAutomatic)
	if err != nil {
		t.Fatal(err)
	}
	run, err = store.MarkStarted(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	store.SetArtifactFailureHook(func(stage string) error {
		if stage == "reference_inserted" {
			return errors.New("injected output artifact crash")
		}
		return nil
	})
	_, err = store.Finish(ctx, run, contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: strings.Repeat("crash-output-", contracts.MaxToolEvidenceBytes/len("crash-output-")+100)})
	store.SetArtifactFailureHook(nil)
	if err == nil || !strings.Contains(err.Error(), "injected output artifact crash") {
		t.Fatalf("finish error=%v", err)
	}
	recovered, err := store.Get(ctx, workspace, request.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != contracts.ToolRunRunning || recovered.Result != nil {
		t.Fatalf("source changed after rollback: %+v", recovered)
	}
	var refs, artifacts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifact_refs WHERE workspace_id=$1 AND source_kind='tool_run' AND source_id=$2`, workspace, run.ID).Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifacts WHERE workspace_id=$1`, workspace).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if refs != 0 || artifacts != 0 {
		t.Fatalf("rollback left output artifacts refs=%d artifacts=%d", refs, artifacts)
	}
}

func TestOversizedEvidenceRawIsArtifactBackedAndDisclosureStable(t *testing.T) {
	store, _, workspace := newEvidenceTestStore(t)
	ctx := context.Background()
	startedAt := time.Now()
	raw := []byte(strings.Repeat("evidence-", contracts.MaxEvidenceRawBytes/len("evidence-")+100))
	input := EvidencePutInput{WorkspaceID: workspace, SourceReference: "large:memo", DeduplicationKey: "large", Kind: "memo", MediaType: "text/plain", Gist: "large evidence", RawPayload: raw, Actor: contracts.ActorRef{ID: "actor", Kind: "test", WorkspaceID: workspace}, CausationID: "cause-1", CorrelationID: "corr-1"}
	first, err := store.Put(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Record.RawArtifact == nil || first.Record.RawSizeBytes != int64(len(raw)) {
		t.Fatalf("raw artifact link missing: %+v", first.Record)
	}
	result, err := store.Disclose(ctx, contracts.DisclosureRequest{WorkspaceID: workspace, EvidenceID: first.Record.ID, Level: contracts.DisclosureRaw, MaxBytes: contracts.MaxEvidenceRawBytes, MaxTokens: contracts.MaxEvidenceRawBytes})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RawPayload) != contracts.MaxEvidenceRawBytes || !result.Truncated {
		// The raw disclosure ceiling is intentionally hard; identity must remain
		// stable even when the full artifact is larger than the request budget.
		if len(result.RawPayload) != 0 || !result.Truncated {
			t.Fatalf("raw disclosure budget violated: bytes=%d truncated=%t", len(result.RawPayload), result.Truncated)
		}
	}
	again, err := store.Disclose(ctx, contracts.DisclosureRequest{WorkspaceID: workspace, EvidenceID: first.Record.ID, Level: contracts.DisclosureGist, MaxBytes: 4096, MaxTokens: 4096})
	if err != nil {
		t.Fatal(err)
	}
	again2, err := store.Disclose(ctx, contracts.DisclosureRequest{WorkspaceID: workspace, EvidenceID: first.Record.ID, Level: contracts.DisclosureGist, MaxBytes: 4096, MaxTokens: 4096})
	if err != nil || again.ContentHash != again2.ContentHash {
		t.Fatalf("disclosure hash unstable: one=%+v two=%+v err=%v", again, again2, err)
	}
	duplicate, err := store.Put(ctx, input)
	if err != nil || duplicate.Created || duplicate.Record.RawArtifact.ArtifactID != first.Record.RawArtifact.ArtifactID {
		t.Fatalf("duplicate evidence changed artifact: %+v err=%v", duplicate, err)
	}
	t.Logf("oversized evidence path bytes=%d latency=%s", len(raw), time.Since(startedAt))
}

func TestOversizedAgentHistoryIsArtifactBackedAndReconstructable(t *testing.T) {
	runs, _, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	run, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "oversized-history"))
	if err != nil {
		t.Fatal(err)
	}
	run.State = contracts.AgentRunRunning
	run.History = make([]contracts.ModelMessage, contracts.MaxAgentHistoryMessages)
	for i := range run.History {
		run.History[i] = contracts.ModelMessage{Role: "assistant", Content: fmt.Sprintf("message-%d-%s", i, strings.Repeat("h", 20_000))}
	}
	current, err := runs.Get(ctx, workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	next := current
	next.State = contracts.AgentRunRunning
	next.History = run.History
	committed, err := runs.Commit(ctx, current, next, contracts.AgentEventCheckpointed, map[string]any{"output": "large"})
	if err != nil {
		t.Fatal(err)
	}
	if committed.HistoryArtifact == nil {
		t.Fatalf("history artifact missing: %+v", committed)
	}
	reloaded, err := runs.Get(ctx, workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.History) != len(run.History) || reloaded.StateHash != committed.StateHash || reloaded.HistoryArtifact.ArtifactID != committed.HistoryArtifact.ArtifactID {
		t.Fatalf("hydrated agent history changed: len=%d hash=%s ref=%+v", len(reloaded.History), reloaded.StateHash, reloaded.HistoryArtifact)
	}
}

func TestArtifactBackedAgentOutputCrashRollsBackCheckpointAndReference(t *testing.T) {
	runs, pool, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	run, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "artifact-history-crash"))
	if err != nil {
		t.Fatal(err)
	}
	next := run
	next.State = contracts.AgentRunRunning
	next.History = []contracts.ModelMessage{{Role: "assistant", Content: strings.Repeat("history-crash-", contracts.MaxAgentHistoryBytes/len("history-crash-")+100)}}
	runs.SetArtifactFailureHook(func(stage string) error {
		if stage == "reference_inserted" {
			return errors.New("injected agent output artifact crash")
		}
		return nil
	})
	_, err = runs.Commit(ctx, run, next, contracts.AgentEventCheckpointed, map[string]any{"test": "artifact-crash"})
	runs.SetArtifactFailureHook(nil)
	if err == nil || !strings.Contains(err.Error(), "injected agent output artifact crash") {
		t.Fatalf("commit error=%v", err)
	}
	recovered, err := runs.Get(ctx, workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.StateVersion != run.StateVersion || recovered.HistoryArtifact != nil || len(recovered.History) != len(run.History) {
		t.Fatalf("checkpoint changed after rollback: before=%+v after=%+v", run.Checkpoint(), recovered.Checkpoint())
	}
	var refs, artifacts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifact_refs WHERE workspace_id=$1 AND source_kind='agent_run' AND source_id LIKE $2`, workspace, run.ID+"%").Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifacts WHERE workspace_id=$1`, workspace).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if refs != 0 || artifacts != 0 {
		t.Fatalf("rollback left agent artifacts refs=%d artifacts=%d", refs, artifacts)
	}
}

func TestArtifactBackfillIsDryRunSafeResumableAndIdempotent(t *testing.T) {
	store, pool, workspace := newToolRunTestStore(t)
	ctx := context.Background()
	request := durableToolRequest(workspace, "backfill-output")
	run, _, err := store.Reserve(ctx, request, contracts.ToolModeAutomatic)
	if err != nil {
		t.Fatal(err)
	}
	large := contracts.ToolResult{RequestID: request.RequestID, RunID: run.ID, ToolID: request.ToolID, Status: contracts.ToolRunSucceeded, Stdout: strings.Repeat("legacy-", contracts.MaxToolEvidenceBytes/len("legacy-")+100)}
	encoded, _ := json.Marshal(large)
	if _, err := pool.Exec(ctx, `UPDATE fornix.tool_runs SET result=$3::jsonb,response_evidence='{}'::jsonb,status='succeeded' WHERE workspace_id=$1 AND id=$2`, workspace, run.ID, encoded); err != nil {
		t.Fatal(err)
	}
	dry, err := store.artifacts.Backfill(ctx, contracts.ArtifactBackfillRequest{WorkspaceID: workspace, SourceKind: "tool_run", BatchSize: 10, DryRun: true})
	if err != nil || dry.Eligible != 1 {
		t.Fatalf("backfill dry run=%+v err=%v", dry, err)
	}
	var refs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifact_refs WHERE workspace_id=$1`, workspace).Scan(&refs); err != nil || refs != 0 {
		t.Fatalf("dry run mutated refs=%d err=%v", refs, err)
	}
	backfillStartedAt := time.Now()
	backfilled, err := store.artifacts.Backfill(ctx, contracts.ArtifactBackfillRequest{WorkspaceID: workspace, SourceKind: "tool_run", BatchSize: 10})
	if err != nil || backfilled.Linked != 1 {
		t.Fatalf("backfill=%+v err=%v", backfilled, err)
	}
	repeated, err := store.artifacts.Backfill(ctx, contracts.ArtifactBackfillRequest{WorkspaceID: workspace, SourceKind: "tool_run", BatchSize: 10})
	if err != nil || repeated.Examined != 0 {
		t.Fatalf("repeat backfill=%+v err=%v", repeated, err)
	}
	t.Logf("tool backfill examined=%d linked=%d throughput=%.2f rows/s", backfilled.Examined, backfilled.Linked, float64(backfilled.Examined)/time.Since(backfillStartedAt).Seconds())
}

func TestEvidenceBackfillLinksLegacyOversizedInlineRow(t *testing.T) {
	store, pool, workspace := newEvidenceTestStore(t)
	ctx := context.Background()
	raw := []byte(strings.Repeat("legacy-evidence-", contracts.MaxEvidenceRawBytes/len("legacy-evidence-")+100))
	hash := contracts.ArtifactContentHash(raw)
	var evidenceID int64
	if err := pool.QueryRow(ctx, `INSERT INTO fornix.evidence_records(workspace_id,source_reference,deduplication_key,kind,media_type,gist,detail,raw_payload,raw_size_bytes,evidence_hash) VALUES($1,'legacy:backfill','legacy','memo','text/plain','legacy gist','legacy detail',$2,$3,$4) RETURNING id`, workspace, raw, int64(len(raw)), hash).Scan(&evidenceID); err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now()
	result, err := store.artifacts.Backfill(ctx, contracts.ArtifactBackfillRequest{WorkspaceID: workspace, SourceKind: "evidence", BatchSize: 10, Actor: contracts.ActorRef{ID: "backfill", Kind: "test", WorkspaceID: workspace}})
	if err != nil || result.Eligible != 1 || result.Linked != 1 {
		t.Fatalf("evidence backfill=%+v err=%v", result, err)
	}
	var artifactID *int64
	if err := pool.QueryRow(ctx, `SELECT raw_artifact_id FROM fornix.evidence_records WHERE workspace_id=$1 AND id=$2`, workspace, evidenceID).Scan(&artifactID); err != nil {
		t.Fatal(err)
	}
	if artifactID == nil {
		t.Fatal("legacy evidence row was not linked")
	}
	repeated, err := store.artifacts.Backfill(ctx, contracts.ArtifactBackfillRequest{WorkspaceID: workspace, SourceKind: "evidence", BatchSize: 10})
	if err != nil || repeated.Examined != 0 {
		t.Fatalf("repeated evidence backfill=%+v err=%v", repeated, err)
	}
	t.Logf("evidence backfill bytes=%d latency=%s", len(raw), time.Since(startedAt))
}

func TestAgentBackfillLinksLegacyOversizedHistory(t *testing.T) {
	runs, pool, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	run, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "agent-backfill"))
	if err != nil {
		t.Fatal(err)
	}
	history := []contracts.ModelMessage{{Role: "assistant", Content: strings.Repeat("legacy-history-", contracts.MaxAgentHistoryBytes/len("legacy-history-")+100)}}
	historyJSON, err := json.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE fornix.agent_runs SET history=$3::jsonb WHERE workspace_id=$1 AND id=$2`, workspace, run.ID, historyJSON); err != nil {
		t.Fatal(err)
	}
	result, err := runs.artifacts.Backfill(ctx, contracts.ArtifactBackfillRequest{WorkspaceID: workspace, SourceKind: "agent_run", BatchSize: 10, Actor: contracts.ActorRef{ID: "backfill", Kind: "test", WorkspaceID: workspace}})
	if err != nil || result.Eligible != 1 || result.Linked != 1 {
		t.Fatalf("agent backfill=%+v err=%v", result, err)
	}
	reloaded, err := runs.Get(ctx, workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.HistoryArtifact == nil || len(reloaded.History) != len(history) || reloaded.History[0].Content != history[0].Content {
		t.Fatalf("agent history backfill did not reconstruct: %+v", reloaded)
	}
	repeated, err := runs.artifacts.Backfill(ctx, contracts.ArtifactBackfillRequest{WorkspaceID: workspace, SourceKind: "agent_run", BatchSize: 10})
	if err != nil || repeated.Examined != 0 {
		t.Fatalf("repeated agent backfill=%+v err=%v", repeated, err)
	}
}

func TestArtifactRetentionSweepAndIntegrityReport(t *testing.T) {
	store, pool, workspace := newArtifactTestStore(t)
	ctx := context.Background()
	past := time.Now().UTC().Add(-time.Minute)
	input := artifactInput(workspace, "sweep", "sweep", []byte("sweep content"))
	input.Retention = contracts.RetentionPolicy{ArchiveAfter: &past, DeleteAfter: &past, AllowDelete: true}
	input.NonAuthoritative = true
	artifact, err := store.Put(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := store.Metrics(ctx, workspace)
	if err != nil || metrics.Artifacts != 1 || metrics.ArtifactBytes < int64(len(input.Raw)) {
		t.Fatalf("artifact metrics=%+v err=%v", metrics, err)
	}
	t.Logf("artifact metrics artifacts=%d artifact_bytes=%d chunk_bytes=%d refs=%d dedup_ratio=%.2f", metrics.Artifacts, metrics.ArtifactBytes, metrics.ChunkBytes, metrics.References, metrics.DedupRatio)
	dry, err := store.RetentionSweep(ctx, contracts.ArtifactRetentionSweepRequest{WorkspaceID: workspace, BatchSize: 10, DryRun: true, Now: past.Add(time.Minute)})
	if err != nil || dry.Archived != 1 || dry.Deleted != 0 {
		t.Fatalf("retention dry run=%+v err=%v", dry, err)
	}
	archived, err := store.RetentionSweep(ctx, contracts.ArtifactRetentionSweepRequest{WorkspaceID: workspace, BatchSize: 10, Now: past.Add(time.Minute)})
	if err != nil || archived.Archived != 1 {
		t.Fatalf("retention archive=%+v err=%v", archived, err)
	}
	deleted, err := store.RetentionSweep(ctx, contracts.ArtifactRetentionSweepRequest{WorkspaceID: workspace, BatchSize: 10, Now: past.Add(time.Minute)})
	if err != nil || deleted.Deleted != 1 {
		t.Fatalf("retention delete=%+v err=%v", deleted, err)
	}
	if _, err := store.Get(ctx, workspace, artifact.Artifact.ID); err != nil {
		t.Fatal(err)
	}
	// A referenced artifact is reported as blocked and remains active/archived.
	protectedInput := artifactInput(workspace, "protected", "protected", []byte("protected content"))
	protectedInput.Retention = contracts.RetentionPolicy{ArchiveAfter: &past, DeleteAfter: &past, AllowDelete: true}
	protected, err := store.Put(ctx, protectedInput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RetentionSweep(ctx, contracts.ArtifactRetentionSweepRequest{WorkspaceID: workspace, BatchSize: 10, Now: past.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	report, err := store.RetentionSweep(ctx, contracts.ArtifactRetentionSweepRequest{WorkspaceID: workspace, BatchSize: 10, Now: past.Add(time.Minute)})
	if err != nil || report.Blocked != 1 {
		t.Fatalf("protected retention report=%+v err=%v", report, err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM fornix.artifact_chunks WHERE workspace_id=$1 AND artifact_id=$2 AND chunk_index=0`, workspace, protected.Artifact.ID); err != nil {
		t.Fatal(err)
	}
	integrity, err := store.VerifyBatch(ctx, contracts.ArtifactIntegrityRequest{WorkspaceID: workspace, BatchSize: 10, DryRun: true})
	if err != nil || integrity.Corrupt == 0 {
		t.Fatalf("integrity report=%+v err=%v", integrity, err)
	}
	if !errors.Is(store.Verify(ctx, workspace, protected.Artifact.ID), ErrArtifactIntegrity) {
		t.Fatal("single-artifact verification did not detect corruption")
	}
}
