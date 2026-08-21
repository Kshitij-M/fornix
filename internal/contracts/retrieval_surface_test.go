package contracts

import (
	"strings"
	"testing"
)

func testRetrievalSurface(workspace, requestID, key string) RetrievalSurface {
	hash := strings.Repeat("a", 64)
	return RetrievalSurface{
		ID:             "surface-test-1",
		WorkspaceID:    workspace,
		RequestID:      requestID,
		IdempotencyKey: key,
		RequestHash:    hash,
		PlanHash:       strings.Repeat("b", 64),
		ContextHash:    strings.Repeat("c", 64),
		Budget:         RetrievalBudget{MaxItems: 2, MaxBytes: 1024, MaxTokens: 256},
		Trace: RetrievalTrace{
			PlanHash:      strings.Repeat("b", 64),
			Stages:        []RetrievalStageTrace{{Name: StageLexical, Status: "failed", Error: "driver exposed prompt text"}},
			CompiledItems: 1,
		},
		References: []RetrievalSurfaceReference{{
			SourceReference: "memo:1", Kind: "memo", EvidenceHash: strings.Repeat("d", 64), Score: 0.9, Stage: StageLexical,
		}},
	}
}

func TestRetrievalSurfaceNormalizeRedactsAndHashesLogicalPayload(t *testing.T) {
	one := testRetrievalSurface("workspace-a", "request-a", "capture-a")
	one.Trace.Stages[0].DurationMicros = 10
	if err := one.Normalize(); err != nil {
		t.Fatal(err)
	}
	if one.Trace.Stages[0].Error != "stage_failed" {
		t.Fatalf("unstable stage error was retained: %+v", one.Trace.Stages)
	}
	if one.PayloadHash == "" || one.CanonicalPayloadHash() != one.PayloadHash {
		t.Fatalf("payload hash is not canonical: %+v", one)
	}

	two := testRetrievalSurface("workspace-a", "request-b", "capture-b")
	two.Trace.Stages[0].DurationMicros = 9000
	if err := two.Normalize(); err != nil {
		t.Fatal(err)
	}
	if one.PayloadHash != two.PayloadHash {
		t.Fatalf("delivery metadata changed logical surface hash: %s != %s", one.PayloadHash, two.PayloadHash)
	}
	if strings.Contains(one.Trace.Stages[0].Error, "prompt") {
		t.Fatal("raw stage error leaked into surface")
	}
}

func TestRetrievalSurfaceContextPackPreservesEvidenceIdentityWithoutText(t *testing.T) {
	surface := testRetrievalSurface("workspace-a", "request-a", "capture-a")
	if err := surface.Normalize(); err != nil {
		t.Fatal(err)
	}
	pack := surface.ContextPack()
	if pack.WorkspaceID != surface.WorkspaceID || pack.ContentHash != surface.ContextHash || len(pack.Items) != 1 {
		t.Fatalf("unexpected reconstructed pack: %+v", pack)
	}
	if pack.Items[0].EvidenceHash != surface.References[0].EvidenceHash || pack.Items[0].Text != "" {
		t.Fatalf("surface did not preserve redacted evidence identity: %+v", pack.Items[0])
	}
}

func TestRetrievalSurfaceRejectsDuplicateReferences(t *testing.T) {
	surface := testRetrievalSurface("workspace-a", "request-a", "capture-a")
	surface.References = append(surface.References, surface.References[0])
	if err := surface.Normalize(); err == nil {
		t.Fatal("expected duplicate evidence reference rejection")
	}
}
