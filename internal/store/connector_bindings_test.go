package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestConnectorBindingStoreIsIdempotentImmutableAndWorkspaceScoped(t *testing.T) {
	_, pool, workspace := newOperationTestStore(t)
	bindings := NewConnectorBindingStore(pool, NewEventStore(pool))
	request := connectorBindingRequest(t, workspace, "binding-create")
	first, duplicate, err := bindings.Create(context.Background(), request)
	if err != nil || duplicate || first.ID != request.Binding.ID {
		t.Fatalf("first binding=%+v duplicate=%v err=%v", first, duplicate, err)
	}
	replayed, duplicate, err := bindings.Create(context.Background(), request)
	if err != nil || !duplicate || replayed.StableHash() != first.StableHash() {
		t.Fatalf("replayed binding=%+v duplicate=%v err=%v", replayed, duplicate, err)
	}
	conflict := request
	conflict.Binding.Configuration = json.RawMessage(`{"base_url":"https://other.example.com"}`)
	conflict.Binding.ConfigHash = ""
	if _, _, err := bindings.Create(context.Background(), conflict); !errors.Is(err, contractsErrConnectorBindingConflict()) {
		t.Fatalf("conflict error=%v", err)
	}
	if _, err := bindings.Get(context.Background(), "workspace-b", first.ID); !errors.Is(err, ErrConnectorBindingNotFound) {
		t.Fatalf("cross-workspace get error=%v", err)
	}
	bindings.SetFailureHook(func(stage string) error {
		if stage == "connector_binding_created" {
			return errors.New("injected binding crash")
		}
		return nil
	})
	crashRequest := connectorBindingRequest(t, workspace, "binding-crash")
	crashRequest.Binding.ID = "binding-crash"
	if _, _, err := bindings.Create(context.Background(), crashRequest); err == nil {
		t.Fatal("injected binding crash unexpectedly committed")
	}
	bindings.SetFailureHook(nil)
	if _, err := bindings.Get(context.Background(), workspace, crashRequest.Binding.ID); !errors.Is(err, ErrConnectorBindingNotFound) {
		t.Fatalf("crash left binding state: %v", err)
	}
}

func TestConnectorBindingStoreConcurrentDuplicateRequestsConverge(t *testing.T) {
	_, pool, workspace := newOperationTestStore(t)
	bindings := NewConnectorBindingStore(pool, NewEventStore(pool))
	request := connectorBindingRequest(t, workspace, "binding-concurrent")
	const workers = 12
	var group sync.WaitGroup
	results := make(chan struct {
		binding contracts.ConnectorBinding
		dup     bool
		err     error
	}, workers)
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			binding, duplicate, err := bindings.Create(context.Background(), request)
			results <- struct {
				binding contracts.ConnectorBinding
				dup     bool
				err     error
			}{binding: binding, dup: duplicate, err: err}
		}()
	}
	group.Wait()
	close(results)
	var firstHash string
	duplicates := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent binding create failed: %v", result.err)
		}
		if result.binding.StableHash() == "" {
			t.Fatal("concurrent binding create returned an invalid binding")
		}
		if firstHash == "" {
			firstHash = result.binding.StableHash()
		}
		if result.binding.StableHash() != firstHash {
			t.Fatalf("concurrent binding hashes diverged: %s != %s", result.binding.StableHash(), firstHash)
		}
		if result.dup {
			duplicates++
		}
	}
	if duplicates != workers-1 {
		t.Fatalf("duplicate count=%d, want %d", duplicates, workers-1)
	}
}

func connectorBindingRequest(t *testing.T, workspace, key string) contracts.ConnectorBindingRequest {
	t.Helper()
	request := contracts.ConnectorBindingRequest{RequestID: key + "-request", IdempotencyKey: key, WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}, Binding: contracts.ConnectorBinding{ID: "binding-http", WorkspaceID: workspace, Connector: contracts.ConnectorRef{WorkspaceID: workspace, Name: "httpapi", Version: "1"}, Kind: contracts.ConnectorBindingHTTPAPI, Version: 1, Configuration: json.RawMessage(`{"base_url":"https://api.example.com","allowed_hosts":["api.example.com"]}`), CredentialRefs: []string{"provider/api"}, CreatedBy: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}}}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	return request
}

// Kept as a function to make the test's error assertion independent of any
// exported aliasing policy in the store package.
func contractsErrConnectorBindingConflict() error { return ErrConnectorBindingConflict }
