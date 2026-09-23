package connector_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/omaveda/fornix/internal/adapters/httpapi"
	"github.com/omaveda/fornix/internal/adapters/sqlreadonly"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestReferenceConnectorsPassSharedReadConformance(t *testing.T) {
	workspace := "workspace-reference"
	httpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"items":[{"id":"one"}]}`))
	}))
	defer httpServer.Close()
	payloads := connector.NewStaticPayloadResolver()
	httpPayload := httpapi.Payload{Method: http.MethodGet, Path: "/items"}
	httpPayloadBytes, _ := json.Marshal(httpPayload)
	httpHash := connector.HashPayload(httpPayloadBytes)
	if err := payloads.Put(workspace, httpHash, httpPayloadBytes); err != nil {
		t.Fatal(err)
	}
	httpAdapter, err := httpapi.NewConnector(httpapi.Binding{ID: "reference-http", WorkspaceID: workspace, BaseURL: httpServer.URL, AllowPrivateNetworks: true}, payloads.Resolve, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	statement := "SELECT id FROM public.items"
	sqlPayload := sqlreadonly.Payload{Statement: statement}
	sqlPayloadBytes, _ := json.Marshal(sqlPayload)
	sqlHash := connector.HashPayload(sqlPayloadBytes)
	if err := payloads.Put(workspace, sqlHash, sqlPayloadBytes); err != nil {
		t.Fatal(err)
	}
	describePayload := sqlreadonly.Payload{Schema: "public", Table: "items"}
	describePayloadBytes, _ := json.Marshal(describePayload)
	describeHash := connector.HashPayload(describePayloadBytes)
	if err := payloads.Put(workspace, describeHash, describePayloadBytes); err != nil {
		t.Fatal(err)
	}
	explainPayload := sqlreadonly.Payload{Statement: statement}
	explainPayloadBytes, _ := json.Marshal(explainPayload)
	explainHash := connector.HashPayload(explainPayloadBytes)
	sqlAdapter, err := sqlreadonly.NewConnector(sqlreadonly.Binding{ID: "reference_sql", WorkspaceID: workspace, DatabaseRef: "reference", AllowedSchemas: []string{"public"}, AllowedTables: []string{"public.items"}}, payloads.Resolve, &sqlreadonly.FixtureDatabase{
		QueryResults:   map[string]sqlreadonly.QueryResult{connector.HashPayload([]byte(statement)): {Columns: []string{"id"}, Rows: [][]string{{"one"}}, Bytes: 3}},
		DescribeResult: map[string]sqlreadonly.QueryResult{"public.items": {Columns: []string{"column_name", "data_type"}, Rows: [][]string{{"id", "text"}}, Bytes: 10}},
		ExplainResults: map[string]sqlreadonly.QueryResult{connector.HashPayload([]byte(statement)): {Columns: []string{"QUERY PLAN"}, Rows: [][]string{{"Index Scan"}}, Bytes: 10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(httpAdapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(sqlAdapter); err != nil {
		t.Fatal(err)
	}
	httpDefinition := httpAdapter.Capabilities()[0].Definition()
	httpListDefinition := httpAdapter.Capabilities()[1].Definition()
	sqlDescribeDefinition := sqlAdapter.Capabilities()[0].Definition()
	sqlDefinition := sqlAdapter.Capabilities()[1].Definition()
	sqlExplainDefinition := sqlAdapter.Capabilities()[2].Definition()
	for name, request := range map[string]contracts.OperationRequest{
		"http-read":    referenceRequest(workspace, httpDefinition, "http", "reference-http", httpHash, httpapi.ResourceKind, httpapi.InputType),
		"http-list":    referenceRequest(workspace, httpListDefinition, "http", "reference-http", httpHash, httpapi.ResourceKind, httpapi.InputType),
		"sql-describe": referenceRequest(workspace, sqlDescribeDefinition, "database", "reference_sql", describeHash, sqlreadonly.ResourceKind, sqlreadonly.InputType),
		"sql-query":    referenceRequest(workspace, sqlDefinition, "database", "reference_sql", sqlHash, sqlreadonly.ResourceKind, sqlreadonly.InputType),
		"sql-explain":  referenceRequest(workspace, sqlExplainDefinition, "database", "reference_sql", explainHash, sqlreadonly.ResourceKind, sqlreadonly.InputType),
	} {
		failures := connector.RunConformanceSuite(context.Background(), registry, request, connector.AdmissionOptions{})
		for _, failure := range failures {
			t.Errorf("%s conformance case %s: %v", name, failure.Case, failure.Err)
		}
	}
}

func referenceRequest(workspace string, definition contracts.CapabilityDefinition, systemType, systemID, inputHash, resourceKind, inputType string) contracts.OperationRequest {
	return contracts.OperationRequest{ID: "operation-" + definition.Ref.Name, RequestID: "request-" + definition.Ref.Name, IdempotencyKey: "idempotency-" + definition.Ref.Name, WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: systemType, ID: systemID, Version: "1"}, Kind: resourceKind, ID: systemID, Version: "1"}, InputType: inputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: inputHash, Profile: definition.Profile}
}
