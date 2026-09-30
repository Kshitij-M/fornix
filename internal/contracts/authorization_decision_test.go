package contracts

import (
	"testing"
	"time"
)

func TestAuthorizationDecisionHashBindsCurrentDecisionContext(t *testing.T) {
	base := AuthorizationDecision{
		SchemaVersion: IdentitySchemaVersion,
		RequestID:     "request-1",
		WorkspaceID:   "workspace-1",
		Actor: AuditActor{
			ID: "identity-1", WorkspaceID: "workspace-1", Kind: "user", APIKeyID: "key-1",
		},
		Permission: PermissionTaskRead,
		Resource:   "task:list",
		Method:     "GET",
		Path:       "/v1/tasks",
		Allowed:    true,
		Reason:     "permission_granted",
		DecidedAt:  time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	baseHash := base.Hash()
	if baseHash == "" || baseHash != base.Hash() {
		t.Fatalf("decision hash is empty or unstable: %q", baseHash)
	}

	retry := base
	retry.DecidedAt = retry.DecidedAt.Add(time.Hour)
	if retry.Hash() != baseHash {
		t.Fatal("decision timestamp changed the stable retry hash")
	}

	cases := map[string]func(*AuthorizationDecision){
		"permission outcome": func(d *AuthorizationDecision) { d.Allowed = false },
		"decision reason":    func(d *AuthorizationDecision) { d.Reason = "credential_inactive" },
		"HTTP method":        func(d *AuthorizationDecision) { d.Method = "POST" },
		"route":              func(d *AuthorizationDecision) { d.Path = "/v1/tasks/1" },
		"actor":              func(d *AuthorizationDecision) { d.Actor.ID = "identity-2" },
		"credential":         func(d *AuthorizationDecision) { d.Actor.APIKeyID = "key-2" },
		"capability":         func(d *AuthorizationDecision) { d.Permission = PermissionTaskMutate },
		"resource":           func(d *AuthorizationDecision) { d.Resource = "task:complete" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			decision := base
			mutate(&decision)
			if got := decision.Hash(); got == baseHash {
				t.Fatalf("changed decision context retained hash %q", got)
			}
		})
	}
}
