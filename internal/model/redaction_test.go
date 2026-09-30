package model

import (
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestRequestEvidenceContainsStructureButNeverPromptContent(t *testing.T) {
	request := contracts.NewModelRequest("workspace-a", "fake", "fake-model", "do not persist this prompt")
	request.RequestID = "request-evidence"
	request.Messages = []contracts.ModelMessage{{Role: "user", Content: "private message body"}}
	request.Metadata = map[string]string{"trace": "arbitrary user text must not be persisted"}
	evidence, err := RequestEvidence(request)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(evidence)
	for _, forbidden := range []string{"do not persist this prompt", "private message body", "arbitrary user text"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("request evidence contains raw input %q: %s", forbidden, encoded)
		}
	}
	for _, expected := range []string{"\"prompt_hash\"", "\"content_hash\"", "\"metadata_keys\""} {
		if !strings.Contains(encoded, expected) {
			t.Fatalf("request evidence omitted %q: %s", expected, encoded)
		}
	}
}
