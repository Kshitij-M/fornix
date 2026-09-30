package store

import (
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestWorkflowTransitionActorMustBelongToRunWorkspace(t *testing.T) {
	tests := []struct {
		name  string
		actor contracts.ActorRef
		want  bool
	}{
		{name: "requester", actor: contracts.ActorRef{ID: "requester", Kind: "user", WorkspaceID: "workspace-a"}, want: true},
		{name: "distinct reviewer", actor: contracts.ActorRef{ID: "reviewer", Kind: "user", WorkspaceID: "workspace-a"}, want: true},
		{name: "cross-workspace actor", actor: contracts.ActorRef{ID: "reviewer", Kind: "user", WorkspaceID: "workspace-b"}},
		{name: "missing actor", actor: contracts.ActorRef{Kind: "user", WorkspaceID: "workspace-a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := workflowTransitionActorAllowed("workspace-a", test.actor)
			if got != test.want {
				t.Fatalf("workflowTransitionActorAllowed()=%t, want %t", got, test.want)
			}
		})
	}
}
