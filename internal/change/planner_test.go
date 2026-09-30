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

func TestApplyRootedIORejectsOutsideSymlinkSwapAfterPrecondition(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "nested")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "report.txt"), []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "report.txt")
	if err := os.WriteFile(outsideFile, []byte("outside-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := CaptureSnapshot(context.Background(), "workspace-a", "repo", root, []string{"nested/report.txt"}, contracts.ActorRef{ID: "operator", WorkspaceID: "workspace-a"})
	if err != nil {
		t.Fatal(err)
	}
	planned, err := Plan(contracts.ChangeProposalRequest{
		WorkspaceID: "workspace-a", Repository: "repo", IdempotencyKey: "rooted-symlink-swap",
		Operations: []contracts.ChangeOperationInput{{ID: "replace", Type: contracts.ChangeOpReplace, Path: "nested/report.txt", ExpectedHash: snapshot.Files[0].ContentHash, Content: []byte("after")}},
	}, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	resolver := func(context.Context, string, string) ([]byte, error) {
		// This runs after the precondition check. Replace the checked directory
		// with a symlink to a file outside the opened root before the write.
		if err := os.Rename(parent, filepath.Join(root, "nested-original")); err != nil {
			return nil, err
		}
		if err := os.Symlink(outside, parent); err != nil {
			return nil, err
		}
		return []byte("after"), nil
	}
	if _, err := (Executor{}).Apply(context.Background(), root, planned.Packet, resolver, false); err == nil {
		t.Fatal("apply succeeded after the target parent was replaced by an outside symlink")
	}
	if got, err := os.ReadFile(outsideFile); err != nil || string(got) != "outside-secret" {
		t.Fatalf("outside file changed: %q, err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "nested-original", "report.txt")); err != nil || string(got) != "before" {
		t.Fatalf("original file changed: %q, err=%v", got, err)
	}
}

func TestApplyRejectsPreexistingSymlinkRootAndPath(t *testing.T) {
	target := t.TempDir()
	targetFile := filepath.Join(target, "report.txt")
	if err := os.WriteFile(targetFile, []byte("must remain unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.Symlink(target, filepath.Join(root, "linked")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	packet := contracts.ChangePacket{
		WorkspaceID: "workspace-a",
		Operations:  []contracts.ChangeOperation{{Type: contracts.ChangeOpReplace, Path: "linked/report.txt", ExpectedHash: contracts.ArtifactContentHash([]byte("must remain unchanged")), NewContentHash: contracts.ArtifactContentHash([]byte("replacement")), NewMode: 0o600}},
	}
	if _, err := (Executor{}).Apply(context.Background(), root, packet, func(context.Context, string, string) ([]byte, error) {
		return []byte("replacement"), nil
	}, false); err == nil {
		t.Fatal("apply followed a pre-existing symlink component")
	}
	if _, err := (Executor{}).Apply(context.Background(), filepath.Join(root, "linked"), packet, nil, false); err == nil {
		t.Fatal("apply accepted a symlink as the configured repository root")
	}
	if got, err := os.ReadFile(targetFile); err != nil || string(got) != "must remain unchanged" {
		t.Fatalf("symlink target changed: %q, err=%v", got, err)
	}
}

func TestApplyCreateDoesNotReplaceFileCreatedAfterPrecondition(t *testing.T) {
	root := t.TempDir()
	snapshot, err := CaptureSnapshot(context.Background(), "workspace-a", "repo", root, []string{"created.txt"}, contracts.ActorRef{ID: "operator", WorkspaceID: "workspace-a"})
	if err != nil {
		t.Fatal(err)
	}
	planned, err := Plan(contracts.ChangeProposalRequest{
		WorkspaceID: "workspace-a", Repository: "repo", IdempotencyKey: "create-no-clobber",
		Operations: []contracts.ChangeOperationInput{{ID: "create", Type: contracts.ChangeOpCreate, Path: "created.txt", Content: []byte("approved")}},
	}, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (Executor{}).Apply(context.Background(), root, planned.Packet, func(context.Context, string, string) ([]byte, error) {
		if err := os.WriteFile(filepath.Join(root, "created.txt"), []byte("concurrent"), 0o600); err != nil {
			return nil, err
		}
		return []byte("approved"), nil
	}, false)
	if err == nil {
		t.Fatal("create replaced a file written after precondition validation")
	}
	if got, err := os.ReadFile(filepath.Join(root, "created.txt")); err != nil || string(got) != "concurrent" {
		t.Fatalf("concurrent file was overwritten: %q, err=%v", got, err)
	}
}

func TestApplyRootedDeleteRenameAndChmod(t *testing.T) {
	root := t.TempDir()
	seed := map[string]struct {
		content string
		mode    os.FileMode
	}{
		"rename.txt": {content: "move me", mode: 0o640},
		"delete.txt": {content: "remove me", mode: 0o600},
		"chmod.txt":  {content: "keep me", mode: 0o600},
	}
	paths := make([]string, 0, len(seed))
	for path, item := range seed {
		if err := os.WriteFile(filepath.Join(root, path), []byte(item.content), item.mode); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	snapshot, err := CaptureSnapshot(context.Background(), "workspace-a", "repo", root, paths, contracts.ActorRef{ID: "operator", WorkspaceID: "workspace-a"})
	if err != nil {
		t.Fatal(err)
	}
	byPath := make(map[string]contracts.ChangeSourceFile, len(snapshot.Files))
	for _, file := range snapshot.Files {
		byPath[file.Path] = file
	}
	planned, err := Plan(contracts.ChangeProposalRequest{
		WorkspaceID: "workspace-a", Repository: "repo", IdempotencyKey: "rooted-file-ops",
		Operations: []contracts.ChangeOperationInput{
			{ID: "rename", Type: contracts.ChangeOpRename, Path: "rename.txt", Destination: "nested/renamed.txt", ExpectedHash: byPath["rename.txt"].ContentHash},
			{ID: "delete", Type: contracts.ChangeOpDelete, Path: "delete.txt", ExpectedHash: byPath["delete.txt"].ContentHash},
			{ID: "chmod", Type: contracts.ChangeOpChmod, Path: "chmod.txt", ExpectedHash: byPath["chmod.txt"].ContentHash, NewMode: 0o640},
		},
	}, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (Executor{}).Apply(context.Background(), root, planned.Packet, nil, false)
	if err != nil {
		t.Fatalf("apply result=%#v expected=%s: %v", result, planned.ExpectedTreeHash, err)
	}
	if result.ResultTreeHash != planned.ExpectedTreeHash || result.AppliedOperations != 3 {
		t.Fatalf("apply result = %#v", result)
	}
	if got, err := os.ReadFile(filepath.Join(root, "nested", "renamed.txt")); err != nil || string(got) != "move me" {
		t.Fatalf("renamed content = %q, err=%v", got, err)
	}
	if _, err := os.Lstat(filepath.Join(root, "delete.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted file remains or stat failed unexpectedly: %v", err)
	}
	info, err := os.Stat(filepath.Join(root, "chmod.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("chmod mode = %v, err=%v", info.Mode().Perm(), err)
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
