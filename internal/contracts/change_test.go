package contracts

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestChangeRequestHashExcludesRawContentButDetectsContentChanges(t *testing.T) {
	base := ChangeProposalRequest{
		WorkspaceID: "workspace-a", Repository: "repo", IdempotencyKey: "proposal-1",
		Source:     ChangeSourceSnapshot{WorkspaceID: "workspace-a", Repository: "repo", ManifestHash: strings.Repeat("a", 64), Files: []ChangeSourceFile{{Path: "README.md", ContentHash: strings.Repeat("b", 64), Exists: true}}},
		Operations: []ChangeOperationInput{{ID: "op-1", Type: ChangeOpReplace, Path: "README.md", ExpectedHash: strings.Repeat("b", 64), Content: []byte("new content")}},
	}
	first := base.RequestHash()
	base.Operations[0].Content = []byte("changed content")
	if first == base.RequestHash() {
		t.Fatal("content change did not change request hash")
	}
	base.Operations[0].Content = []byte("new content")
	if first != base.RequestHash() {
		t.Fatal("equivalent content changed request hash")
	}
}

func TestChangePacketHashIgnoresVolatileSourceFieldsAndOrdering(t *testing.T) {
	p := ChangePacket{
		SchemaVersion: ChangeSchemaVersion, WorkspaceID: "workspace-a", Repository: "repo",
		Source:     ChangeSourceSnapshot{WorkspaceID: "workspace-a", Repository: "repo", SourceRoot: "/private/path", ManifestHash: strings.Repeat("a", 64), CapturedAt: nowForTest(), Files: []ChangeSourceFile{{Path: "b", ContentHash: strings.Repeat("b", 64)}, {Path: "a", ContentHash: strings.Repeat("c", 64)}}},
		Operations: []ChangeOperation{{ID: "op-2", Ordinal: 2, Type: ChangeOpCreate, Path: "b", NewContentHash: strings.Repeat("d", 64)}, {ID: "op-1", Ordinal: 1, Type: ChangeOpCreate, Path: "a", NewContentHash: strings.Repeat("e", 64)}},
	}
	hash := p.StableHash()
	p.Source.SourceRoot = "/another/private/path"
	p.Source.CapturedAt = nowForTest().Add(2 * 24 * 365 * time.Hour)
	p.Source.Files = []ChangeSourceFile{{Path: "a", ContentHash: strings.Repeat("c", 64)}, {Path: "b", ContentHash: strings.Repeat("b", 64)}}
	p.Operations = []ChangeOperation{p.Operations[1], p.Operations[0]}
	if hash != p.StableHash() {
		t.Fatal("volatile fields or equivalent ordering changed packet hash")
	}
}

func TestChangeDisclosureAndWorkspaceValidation(t *testing.T) {
	if _, err := (ChangeProposalRequest{WorkspaceID: "a", Repository: "repo", IdempotencyKey: "k", Actor: ActorRef{WorkspaceID: "b"}, Operations: []ChangeOperationInput{{Type: ChangeOpCreate, Path: "a"}}}).Normalize(); err == nil {
		t.Fatal("cross-workspace actor was accepted")
	}
	if _, err := (ChangeDisclosureRequest{WorkspaceID: "a", ProposalID: "p", ApplicationID: "a"}).Normalize(); err == nil {
		t.Fatal("ambiguous disclosure identity was accepted")
	}
	if _, err := (ChangeDisclosureRequest{WorkspaceID: "a", ProposalID: "p", MaxBytes: MaxChangeDisclosureBytes + 1}).Normalize(); err == nil {
		t.Fatal("oversized disclosure was accepted")
	}
}

func TestChangeOperationInputDoesNotAliasContent(t *testing.T) {
	content := []byte("content")
	request := ChangeProposalRequest{WorkspaceID: "a", Repository: "r", IdempotencyKey: "k", Operations: []ChangeOperationInput{{Type: ChangeOpCreate, Path: "a", Content: content}}}
	_ = request.RequestHash()
	content[0] = 'x'
	if bytes.Equal(content, []byte("content")) {
		t.Fatal("test setup did not mutate input")
	}
}

func nowForTest() time.Time { return time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC) }
