package sqlreadonly_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/omaveda/fornix/internal/adapters/sqlreadonly"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestSQLReadonlyQueryUsesFixtureAndStableEvidence(t *testing.T) {
	statement := "SELECT id FROM public.items WHERE id = $1"
	fixture := &sqlreadonly.FixtureDatabase{QueryResults: map[string]sqlreadonly.QueryResult{
		connector.HashPayload([]byte(statement)): {Columns: []string{"id"}, Rows: [][]string{{"one"}}, Bytes: 3},
	}, DescribeResult: map[string]sqlreadonly.QueryResult{}, ExplainResults: map[string]sqlreadonly.QueryResult{}}
	resolver := connector.NewStaticPayloadResolver()
	payload := sqlreadonly.Payload{Statement: statement, Parameters: []string{"one"}, Limit: 10}
	request, adapter := sqlRequest(t, resolver, fixture, payload)
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	outcome, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Result.OutputHash == "" || len(outcome.Result.Evidence) != 1 || fixture.Calls != 1 {
		t.Fatalf("unexpected SQL result=%+v calls=%d", outcome.Result, fixture.Calls)
	}
	second, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{})
	if err != nil || outcome.Result.OutputHash != second.Result.OutputHash || fixture.Calls != 2 {
		t.Fatalf("fixture replay was not stable: first=%+v second=%+v calls=%d err=%v", outcome.Result, second.Result, fixture.Calls, err)
	}
}

func TestSQLReadonlyRejectsMutationInjectionAndUnallowlistedTables(t *testing.T) {
	for name, statement := range map[string]string{
		"write":             "UPDATE public.items SET id = $1",
		"unbound-parameter": "SELECT id FROM public.items WHERE id = $2",
		"multi":             "SELECT id FROM public.items; DROP TABLE public.items",
		"comment":           "SELECT id FROM public.items -- bypass",
		"unqualified":       "SELECT id FROM items",
		"other-table":       "SELECT id FROM public.secrets",
		"explain-analyze":   "SELECT id FROM public.items ANALYZE",
	} {
		t.Run(name, func(t *testing.T) {
			resolver := connector.NewStaticPayloadResolver()
			fixture := &sqlreadonly.FixtureDatabase{QueryResults: map[string]sqlreadonly.QueryResult{}, DescribeResult: map[string]sqlreadonly.QueryResult{}, ExplainResults: map[string]sqlreadonly.QueryResult{}}
			request, adapter := sqlRequest(t, resolver, fixture, sqlreadonly.Payload{Statement: statement, Parameters: []string{"one"}})
			registry := connector.NewRegistry()
			if err := registry.Register(adapter); err != nil {
				t.Fatal(err)
			}
			if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
				t.Fatal("unsafe SQL statement unexpectedly executed")
			}
			if fixture.Calls != 0 {
				t.Fatalf("unsafe statement reached database calls=%d", fixture.Calls)
			}
		})
	}
}

func TestSQLReadonlyRejectsCrossWorkspaceAndOversizedPayload(t *testing.T) {
	resolver := connector.NewStaticPayloadResolver()
	fixture := &sqlreadonly.FixtureDatabase{QueryResults: map[string]sqlreadonly.QueryResult{}, DescribeResult: map[string]sqlreadonly.QueryResult{}, ExplainResults: map[string]sqlreadonly.QueryResult{}}
	request, adapter := sqlRequest(t, resolver, fixture, sqlreadonly.Payload{Statement: "SELECT id FROM public.items"})
	request.Target.WorkspaceID = "workspace-b"
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("cross-workspace SQL target unexpectedly executed")
	}

	oversizedBinding := sqlreadonly.Binding{ID: "warehouse", WorkspaceID: "workspace-a", DatabaseRef: "warehouse", AllowedSchemas: []string{"public"}, AllowedTables: []string{"public.items"}, MaxResultBytes: 4}
	oversizedFixture := &sqlreadonly.FixtureDatabase{QueryResults: map[string]sqlreadonly.QueryResult{connector.HashPayload([]byte("SELECT id FROM public.items")): {Columns: []string{"id"}, Rows: [][]string{{"too-large"}}, Bytes: 9}}, DescribeResult: map[string]sqlreadonly.QueryResult{}, ExplainResults: map[string]sqlreadonly.QueryResult{}}
	oversizedResolver := connector.NewStaticPayloadResolver()
	oversizedRequest, oversizedAdapter := sqlRequestWithBinding(t, oversizedResolver, oversizedFixture, sqlreadonly.Payload{Statement: "SELECT id FROM public.items"}, oversizedBinding)
	registry = connector.NewRegistry()
	if err := registry.Register(oversizedAdapter); err != nil {
		t.Fatal(err)
	}
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), oversizedRequest, connector.AdmissionOptions{}); err == nil {
		t.Fatal("oversized SQL result unexpectedly succeeded")
	} else if strings.Contains(err.Error(), "too-large") {
		t.Fatal("raw SQL output appeared in error")
	}
}

func TestSQLReadonlyEnforcesDeterministicCostBudget(t *testing.T) {
	statement := "SELECT id FROM public.items"
	resolver := connector.NewStaticPayloadResolver()
	fixture := &sqlreadonly.FixtureDatabase{
		QueryResults: map[string]sqlreadonly.QueryResult{
			connector.HashPayload([]byte(statement)): {Columns: []string{"id"}, Rows: [][]string{{"one"}}, Bytes: 3},
		},
		DescribeResult: map[string]sqlreadonly.QueryResult{}, ExplainResults: map[string]sqlreadonly.QueryResult{},
	}
	binding := sqlreadonly.Binding{ID: "warehouse", WorkspaceID: "workspace-a", DatabaseRef: "warehouse", AllowedSchemas: []string{"public"}, AllowedTables: []string{"public.items"}, MaxCostUnits: 1}
	request, adapter := sqlRequestWithBinding(t, resolver, fixture, sqlreadonly.Payload{Statement: statement}, binding)
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("query exceeded its cost budget without failing")
	}
	if fixture.Calls != 1 {
		t.Fatalf("cost budget should be checked after bounded fixture execution, calls=%d", fixture.Calls)
	}
}

func TestPGDatabaseUsesReadOnlyTransaction(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	// Package tests run concurrently against a fresh integration database. Make
	// this adapter-level test self-sufficient instead of racing the server or
	// another package's first migration application.
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	database := sqlreadonly.PGDatabase{Pool: pool}
	if _, err := database.QueryReadOnly(ctx, sqlreadonly.QueryRequest{Statement: "SELECT version FROM fornix.schema_migrations LIMIT 1", MaxRows: 1, MaxBytes: 4096, Timeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.QueryReadOnly(ctx, sqlreadonly.QueryRequest{Statement: "UPDATE fornix.schema_migrations SET checksum = checksum", MaxRows: 1, MaxBytes: 4096, Timeout: time.Second}); err == nil {
		t.Fatal("read-only SQL adapter executed a mutation")
	}
}

func sqlRequest(t *testing.T, resolver *connector.StaticPayloadResolver, fixture *sqlreadonly.FixtureDatabase, payload sqlreadonly.Payload) (contracts.OperationRequest, *sqlreadonly.Connector) {
	t.Helper()
	binding := sqlreadonly.Binding{ID: "warehouse", WorkspaceID: "workspace-a", DatabaseRef: "warehouse", AllowedSchemas: []string{"public"}, AllowedTables: []string{"public.items"}}
	return sqlRequestWithBinding(t, resolver, fixture, payload, binding)
}

func sqlRequestWithBinding(t *testing.T, resolver *connector.StaticPayloadResolver, fixture *sqlreadonly.FixtureDatabase, payload sqlreadonly.Payload, binding sqlreadonly.Binding) (contracts.OperationRequest, *sqlreadonly.Connector) {
	t.Helper()
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	inputHash := hash(payloadBytes)
	if err := resolver.Put(binding.WorkspaceID, inputHash, payloadBytes); err != nil {
		t.Fatal(err)
	}
	adapter, err := sqlreadonly.NewConnector(binding, resolver.Resolve, fixture)
	if err != nil {
		t.Fatal(err)
	}
	definition := adapter.Capabilities()[1].Definition()
	return contracts.OperationRequest{ID: "operation-sql", RequestID: "request-sql", IdempotencyKey: "sql-idempotency", WorkspaceID: binding.WorkspaceID, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: binding.WorkspaceID}, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: binding.WorkspaceID, System: contracts.SystemRef{WorkspaceID: binding.WorkspaceID, Type: "database", ID: binding.ID, Version: "1"}, Kind: sqlreadonly.ResourceKind, ID: binding.ID, Version: "1"}, InputType: sqlreadonly.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: inputHash, Profile: definition.Profile}, adapter
}

func hash(value []byte) string { digest := sha256.Sum256(value); return hex.EncodeToString(digest[:]) }
