package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestConnectorBindingIsVersionedRedactedAndHashable(t *testing.T) {
	binding := ConnectorBinding{
		ID: "billing-api", WorkspaceID: "workspace-a", Connector: ConnectorRef{WorkspaceID: "workspace-a", Name: "httpapi", Version: "1"},
		Kind: ConnectorBindingHTTPAPI, Version: 1, Configuration: json.RawMessage(`{"base_url":"https://api.example.com","allowed_hosts":["api.example.com"]}`),
		CredentialRefs: []string{"provider/api"}, CreatedBy: ActorRef{ID: "operator", Kind: "human", WorkspaceID: "workspace-a"},
	}
	if err := binding.Normalize(); err != nil {
		t.Fatal(err)
	}
	if binding.ConfigHash == "" || binding.StableHash() == "" {
		t.Fatalf("binding hashes missing: %+v", binding)
	}
	raw, _ := json.Marshal(binding)
	if strings.Contains(string(raw), "password") || strings.Contains(string(raw), "secret") {
		t.Fatalf("binding output contains forbidden secret-like field: %s", raw)
	}
	binding.Configuration = json.RawMessage(`{"dsn":"postgres://user:password@example"}`)
	if err := binding.Normalize(); err == nil {
		t.Fatal("secret-bearing binding configuration was accepted")
	}
}

func TestConnectorBindingRejectsWorkspaceAndKindConflicts(t *testing.T) {
	binding := ConnectorBinding{ID: "db", WorkspaceID: "workspace-a", Connector: ConnectorRef{WorkspaceID: "workspace-b", Name: "sqlreadonly", Version: "1"}, Kind: ConnectorBindingSQLReadonly, Version: 1, Configuration: json.RawMessage(`{"database_ref":"warehouse"}`), CreatedBy: ActorRef{ID: "operator", Kind: "human", WorkspaceID: "workspace-a"}}
	if err := binding.Normalize(); err == nil {
		t.Fatal("cross-workspace connector binding was accepted")
	}
	binding.Connector.WorkspaceID = binding.WorkspaceID
	binding.Kind = "unknown"
	if err := binding.Normalize(); err == nil {
		t.Fatal("unknown connector binding kind was accepted")
	}
}
