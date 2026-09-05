package validation

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/omaveda/fornix/internal/change"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/ingest"
)

func TestDefaultRegistryProducesStablePostChangeResults(t *testing.T) {
	root := t.TempDir()
	oldContent := []byte("before\n")
	newContent := []byte("after\n")
	path := filepath.Join(root, "README.md")
	if err := os.WriteFile(path, oldContent, 0o644); err != nil {
		t.Fatal(err)
	}
	workspace := "workspace-validation"
	actor := contracts.ActorRef{ID: "tester", Kind: "test", WorkspaceID: workspace}
	source, err := change.CaptureSnapshot(context.Background(), workspace, "repository", root, []string{"README.md"}, actor)
	if err != nil {
		t.Fatal(err)
	}
	planned, err := change.Plan(contracts.ChangeProposalRequest{
		WorkspaceID: workspace, IdempotencyKey: "proposal-1", Repository: "repository", Source: source,
		ApprovalMode: contracts.ChangeApprovalAutomatic, Operations: []contracts.ChangeOperationInput{{
			Type: contracts.ChangeOpReplace, Path: "README.md", ExpectedHash: source.Files[0].ContentHash, Content: newContent,
		}},
	}, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, newContent, 0o644); err != nil {
		t.Fatal(err)
	}
	observation, err := change.ObserveAppliedPacketState(context.Background(), root, planned.Packet)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.ValidationRequest{ID: "validation-run-1", WorkspaceID: workspace, PacketHash: planned.Packet.StableHash(), ExpectedTreeHash: planned.ExpectedTreeHash, Repository: "repository"}
	plan := contracts.ValidationPlan{SchemaVersion: contracts.ValidationSchemaVersion, WorkspaceID: workspace, RequestHash: "request-hash", Validators: contracts.DefaultValidatorRefs(), Budget: contracts.DefaultValidationBudget()}
	input := Input{Request: request, Plan: plan, Proposal: contracts.ChangeProposal{ID: "proposal-1", WorkspaceID: workspace, Repository: "repository", Source: source, Operations: planned.Packet.Operations, Budgets: planned.Packet.Budgets}, Application: contracts.ChangeApplication{ID: "application-1", WorkspaceID: workspace, Status: contracts.ChangeApplied, ResultTreeHash: planned.ExpectedTreeHash}, Root: root, MountRoot: root, Observation: observation, Discovery: &ingest.DiscoveryResult{ManifestHash: "manifest-hash", Files: []ingest.DiscoveredFile{{File: contracts.IngestFile{Path: "README.md", ContentHash: contracts.ArtifactContentHash(newContent), ByteSize: int64(len(newContent)), Mode: 0o644}}}}}
	first, err := registry.ValidatePlan(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.ValidatePlan(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(contracts.DefaultValidatorRefs()) {
		t.Fatalf("result count = %d", len(first))
	}
	for i := range first {
		if first[i].Outcome != contracts.ValidationOutcomePassed {
			t.Fatalf("validator %s outcome = %s failure=%+v", first[i].Validator.ID, first[i].Outcome, first[i].Failure)
		}
		if first[i].StableHash() != second[i].StableHash() {
			t.Fatalf("validator %s result hash changed", first[i].Validator.ID)
		}
		if first[i].InputHash != second[i].InputHash {
			t.Fatalf("validator %s input hash changed", first[i].Validator.ID)
		}
	}
}

func TestRegistryRejectsDuplicateValidatorIdentity(t *testing.T) {
	registry := NewRegistry()
	validator := builtinValidator{id: contracts.ValidationValidatorSafety, check: validateSafety}
	if err := registry.Register(validator); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(validator); err == nil {
		t.Fatal("expected duplicate validator rejection")
	}
}

func TestObserveAppliedPacketStateRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "outside.txt")
	if err := os.WriteFile(target, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "link.txt")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	_, err := change.ObserveAppliedPacketState(context.Background(), root, contracts.ChangePacket{WorkspaceID: "workspace", Source: contracts.ChangeSourceSnapshot{Files: []contracts.ChangeSourceFile{{Path: "link.txt"}}}})
	if err == nil {
		t.Fatal("expected symlink rejection")
	}
}
