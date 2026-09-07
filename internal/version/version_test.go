package version

import (
	"strings"
	"testing"
)

func TestCurrentUsesSafeUnknownDefaults(t *testing.T) {
	oldCommit, oldDate := Commit, Date
	t.Cleanup(func() { Commit, Date = oldCommit, oldDate })
	Commit, Date = "", ""

	info := Current()
	if info.Name != "fornix" || info.Version == "" || info.Commit != "unknown" || info.BuildDate != "unknown" {
		t.Fatalf("Current() = %+v", info)
	}
	if !strings.Contains(info.String(), "fornix ") {
		t.Fatalf("String() = %q", info.String())
	}
	if _, err := info.JSON(); err != nil {
		t.Fatalf("JSON() error = %v", err)
	}
}

func TestCurrentPreservesInjectedMetadata(t *testing.T) {
	oldCommit, oldDate := Commit, Date
	t.Cleanup(func() { Commit, Date = oldCommit, oldDate })
	Commit, Date = "abc123", "2026-01-02T03:04:05Z"

	info := Current()
	if info.Commit != Commit || info.BuildDate != Date {
		t.Fatalf("Current() = %+v", info)
	}
}
