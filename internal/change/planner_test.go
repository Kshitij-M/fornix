package change

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestNormalizeRelativePathRejectsTraversalAndAbsolutePaths(t *testing.T) {
	for _, value := range []string{"../outside", "/absolute", `..\outside`, "", "a\x00b"} {
		if _, err := NormalizeRelativePath(value); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("NormalizeRelativePath(%q) error = %v, want ErrUnsafePath", value, err)
		}
	}
	got, err := NormalizeRelativePath(`dir\file.txt`)
	if err != nil || got != "dir/file.txt" {
		t.Fatalf("normalized path = %q, err = %v", got, err)
	}
}

func TestPlanAndApplyCreateIsDeterministicAndHashVerified(t *testing.T) {
	root := t.TempDir()
	request := contracts.ChangeProposalRequest{
		WorkspaceID: "workspace-a", Repository: "repo", IdempotencyKey: "change-1",
		Operations: []contracts.ChangeOperationInput{{ID: "create", Type: contracts.ChangeOpCreate, Path: "report.txt", Content: []byte("verified report")}},
	}
	snapshot, err := CaptureSnapshot(context.Background(), request.WorkspaceID, request.Repository, root, []string{"report.txt"}, contracts.ActorRef{ID: "operator", WorkspaceID: "workspace-a"})
	if err != nil {
		t.Fatal(err)
	}
	planned, err := Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if planned.Packet.ExpectedTreeHash == "" || planned.ExpectedTreeHash != planned.Packet.ExpectedTreeHash {
		t.Fatal("planner did not persist expected tree hash in packet")
	}
	firstHash := planned.Packet.StableHash()
	plannedAgain, err := Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != plannedAgain.Packet.StableHash() || string(planned.Diff) != string(plannedAgain.Diff) {
		t.Fatal("equivalent plans are not deterministic")
	}
	result, err := (Executor{}).Apply(context.Background(), root, planned.Packet, func(_ context.Context, workspaceID, contentHash string) ([]byte, error) {
		if workspaceID != "workspace-a" || contentHash != planned.Packet.Operations[0].NewContentHash {
			t.Fatalf("resolver scope/hash = %q/%q", workspaceID, contentHash)
		}
		return planned.Contents["create"], nil
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultTreeHash != planned.ExpectedTreeHash || result.AppliedOperations != 1 {
		t.Fatalf("apply result = %#v", result)
	}
	content, err := os.ReadFile(filepath.Join(root, "report.txt"))
	if err != nil || string(content) != "verified report" {
		t.Fatalf("applied content = %q, err = %v", content, err)
	}
}

func TestDryRunNeverWritesAndStaleSourceFailsClosed(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "README.md")
	if err := os.WriteFile(path, []byte("before"), 0o640); err != nil {
		t.Fatal(err)
	}
	snapshot, err := CaptureSnapshot(context.Background(), "workspace-a", "repo", root, []string{"README.md"}, contracts.ActorRef{ID: "operator", WorkspaceID: "workspace-a"})
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.ChangeProposalRequest{
		WorkspaceID: "workspace-a", Repository: "repo", IdempotencyKey: "replace-1",
		Operations: []contracts.ChangeOperationInput{{ID: "replace", Type: contracts.ChangeOpReplace, Path: "README.md", ExpectedHash: snapshot.Files[0].ContentHash, Content: []byte("after")}},
	}
	planned, err := Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (Executor{}).Apply(context.Background(), root, planned.Packet, func(context.Context, string, string) ([]byte, error) { return planned.Contents["replace"], nil }, true)
	if err != nil || result.Changed || result.ResultTreeHash != planned.ExpectedTreeHash {
		t.Fatalf("dry-run result = %#v, err = %v", result, err)
	}
	content, _ := os.ReadFile(path)
	if string(content) != "before" {
		t.Fatalf("dry-run modified source: %q", content)
	}
	if err := os.WriteFile(path, []byte("concurrent change"), 0o640); err != nil {
		t.Fatal(err)
	}
	_, err = (Executor{}).Apply(context.Background(), root, planned.Packet, func(context.Context, string, string) ([]byte, error) { return planned.Contents["replace"], nil }, false)
	if !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("stale source error = %v, want ErrSourceConflict", err)
	}
}

func TestSafeJoinRejectsSymlinkComponents(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(root, "linked")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := SafeJoin(root, "linked/file.txt"); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("SafeJoin symlink error = %v", err)
	}
	if _, err := CaptureSnapshot(context.Background(), "workspace-a", "repo", root, []string{"linked/file.txt"}, contracts.ActorRef{ID: "operator", WorkspaceID: "workspace-a"}); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("snapshot symlink error = %v", err)
	}
}

func TestPlanRejectsDuplicateAndOversizedOperations(t *testing.T) {
	root := t.TempDir()
	snapshot, err := CaptureSnapshot(context.Background(), "workspace-a", "repo", root, []string{"a", "b"}, contracts.ActorRef{})
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.ChangeProposalRequest{WorkspaceID: "workspace-a", Repository: "repo", IdempotencyKey: "duplicate", Operations: []contracts.ChangeOperationInput{{Type: contracts.ChangeOpCreate, Path: "a", Content: []byte("a")}, {Type: contracts.ChangeOpCreate, Path: "a", Content: []byte("b")}}}
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrOperation) {
		t.Fatalf("duplicate operation error = %v", err)
	}
	request = contracts.ChangeProposalRequest{WorkspaceID: "workspace-a", Repository: "repo", IdempotencyKey: "budget", Budgets: contracts.ChangeBudgets{MaxFileBytes: 3}, Operations: []contracts.ChangeOperationInput{{Type: contracts.ChangeOpCreate, Path: "b", Content: []byte(strings.Repeat("x", 4))}}}
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrChangeBudget) {
		t.Fatalf("budget error = %v", err)
	}
}
