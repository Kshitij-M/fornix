package qualification

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/adapters/httpapi"
	"github.com/omaveda/fornix/internal/adapters/repository"
	"github.com/omaveda/fornix/internal/adapters/sqlreadonly"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/testutil"
)

func TestMatrixQualifiesBuiltInReadAdaptersDeterministically(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	const workspace = "workspace-adapter-matrix"
	httpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"items":[{"id":"one"}]}`))
	}))
	defer httpServer.Close()

	resolver := connector.NewStaticPayloadResolver()
	httpPayload := httpapi.Payload{Method: http.MethodGet, Path: "/items"}
	httpHash := putPayload(t, resolver, workspace, httpPayload)
	httpAdapter, err := httpapi.NewConnector(httpapi.Binding{ID: "matrix-http", WorkspaceID: workspace, BaseURL: httpServer.URL, AllowPrivateNetworks: true}, resolver.Resolve, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	sqlPayload := sqlreadonly.Payload{SchemaVersion: 2, Schema: "public", Table: "items", Columns: []string{"id"}}
	sqlHash := putPayload(t, resolver, workspace, sqlPayload)
	queryKey, err := sqlreadonly.QueryIdentityHash(sqlreadonly.QueryRequest{Schema: "public", Table: "items", Columns: []string{"id"}, Limit: sqlreadonly.DefaultMaxRows, MaxRows: sqlreadonly.DefaultMaxRows, MaxBytes: sqlreadonly.DefaultMaxResultBytes, Timeout: sqlreadonly.DefaultTimeout})
	if err != nil {
		t.Fatal(err)
	}
	sqlAdapter, err := sqlreadonly.NewConnector(sqlreadonly.Binding{ID: "matrix_sql", WorkspaceID: workspace, DatabaseRef: "matrix", AllowedSchemas: []string{"public"}, AllowedTables: []string{"public.items"}}, resolver.Resolve, &sqlreadonly.FixtureDatabase{
		QueryResults:   map[string]sqlreadonly.QueryResult{queryKey: {Columns: []string{"id"}, Rows: [][]string{{"one"}}, Bytes: 3}},
		DescribeResult: map[string]sqlreadonly.QueryResult{},
		ExplainResults: map[string]sqlreadonly.QueryResult{},
	})
	if err != nil {
		t.Fatal(err)
	}

	repositoryAdapter, err := repository.NewConnector(workspace, "1", func(context.Context, contracts.OperationRequest) (repository.Inspection, error) {
		return repository.Inspection{OutputHash: contracts.HashStrings("repository-output"), ReportHash: contracts.HashStrings("repository-report"), Evidence: []contracts.OperationEvidenceRef{{WorkspaceID: workspace, SourceReference: "repository:matrix:snapshot", EvidenceHash: contracts.HashStrings("repository-evidence"), Role: "snapshot"}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fakeAdapter, err := fakeincident.NewConnector(workspace)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	for _, adapter := range []connector.Connector{httpAdapter, sqlAdapter, repositoryAdapter, fakeAdapter} {
		if err := registry.Register(adapter); err != nil {
			t.Fatal(err)
		}
	}
	var httpRead, sqlQuery, repositoryInspect, incidentRead connector.Capability
	for _, capability := range httpAdapter.Capabilities() {
		if capability.Definition().Ref.Name == httpapi.ReadCapabilityName {
			httpRead = capability
		}
	}
	for _, capability := range sqlAdapter.Capabilities() {
		if capability.Definition().Ref.Name == sqlreadonly.QueryCapabilityName {
			sqlQuery = capability
		}
	}
	repositoryInspect = repositoryAdapter.Capabilities()[0]
	for _, capability := range fakeAdapter.Capabilities() {
		if capability.Definition().Ref.Name == "incident.read" {
			incidentRead = capability
		}
	}
	if httpRead == nil || sqlQuery == nil || repositoryInspect == nil || incidentRead == nil {
		t.Fatal("matrix fixture capabilities are incomplete")
	}
	entries := []MatrixEntry{
		{Name: "sql-read", Registry: registry, Request: matrixRequest(workspace, sqlQuery.Definition(), "database", "matrix_sql", sqlHash, sqlreadonly.ResourceKind, sqlreadonly.InputType)},
		{Name: "repository-read", Registry: registry, Request: matrixRequest(workspace, repositoryInspect.Definition(), "repository", "matrix-repository", contracts.HashStrings("repository-input"), repository.ResourceKind, repository.CapabilityInputType)},
		{Name: "incident-read", Registry: registry, Request: matrixRequest(workspace, incidentRead.Definition(), "monitoring", "matrix-monitor", contracts.HashStrings("incident-input"), fakeincident.ResourceKind, fakeincident.InputType)},
		{Name: "http-read", Registry: registry, Request: matrixRequest(workspace, httpRead.Definition(), "http", "matrix-http", httpHash, httpapi.ResourceKind, httpapi.InputType)},
	}
	targetHash := contracts.HashStrings("deployment", "matrix")
	first, err := RunMatrix(context.Background(), "matrix-run", workspace, targetHash, entries, connector.ConformanceReportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := RunMatrix(context.Background(), "matrix-run", workspace, targetHash, entries, connector.ConformanceReportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Outcome != contracts.QualificationOutcomePassed || first.ReportHash == "" || first.ReportHash != second.ReportHash || len(first.Cases) != len(entries) {
		t.Fatalf("matrix was not deterministic: first=%+v second=%+v", first, second)
	}
	if first.Cases[0].Name > first.Cases[len(first.Cases)-1].Name {
		t.Fatalf("matrix cases are not sorted: %+v", first.Cases)
	}
}

func TestMatrixBlocksEffectfulCapabilitiesByDefault(t *testing.T) {
	const workspace = "workspace-adapter-matrix-effect"
	adapter, err := fakeincident.NewConnector(workspace)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	var remediation connector.Capability
	for _, capability := range adapter.Capabilities() {
		if capability.Definition().Ref.Name == "incident.remediate" {
			remediation = capability
		}
	}
	if remediation == nil {
		t.Fatal("remediation capability is missing")
	}
	report, err := RunMatrix(context.Background(), "matrix-effect-run", workspace, contracts.HashStrings("deployment", "matrix-effect"), []MatrixEntry{{Name: "incident-remediate", Registry: registry, Request: matrixRequest(workspace, remediation.Definition(), "monitoring", "matrix-monitor", contracts.HashStrings("incident-input"), fakeincident.ResourceKind, fakeincident.InputType)}}, connector.ConformanceReportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Outcome != contracts.QualificationOutcomeBlocked || len(report.Cases) != 1 || report.Cases[0].Outcome != contracts.QualificationOutcomeBlocked {
		t.Fatalf("effectful matrix was not blocked: %+v", report)
	}
}

func TestMatrixRejectsDuplicateAndCrossWorkspaceEntries(t *testing.T) {
	const workspace = "workspace-adapter-matrix-validation"
	adapter, err := fakeincident.NewConnector(workspace)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	definition := adapter.Capabilities()[0].Definition()
	entry := MatrixEntry{Name: "duplicate", Registry: registry, Request: matrixRequest(workspace, definition, "monitoring", "matrix-monitor", contracts.HashStrings("incident-input"), fakeincident.ResourceKind, fakeincident.InputType)}
	if _, err := RunMatrix(context.Background(), "matrix-validation", workspace, contracts.HashStrings("deployment", "validation"), []MatrixEntry{entry, entry}, connector.ConformanceReportOptions{}); err == nil {
		t.Fatal("duplicate matrix entries were accepted")
	}
	foreign := entry
	foreign.Name = "foreign"
	foreign.Request.WorkspaceID = "workspace-other"
	if _, err := RunMatrix(context.Background(), "matrix-validation", workspace, contracts.HashStrings("deployment", "validation"), []MatrixEntry{foreign}, connector.ConformanceReportOptions{}); err == nil {
		t.Fatal("cross-workspace matrix entry was accepted")
	}
}

func TestMatrixRejectsNilContext(t *testing.T) {
	if _, err := RunMatrix(nil, "matrix-nil-context", "workspace-matrix", contracts.HashStrings("deployment", "nil-context"), []MatrixEntry{{Name: "entry", Registry: connector.NewRegistry()}}, connector.ConformanceReportOptions{}); err == nil {
		t.Fatal("nil context was accepted")
	}
}

func putPayload(t *testing.T, resolver *connector.StaticPayloadResolver, workspace string, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	hash := connector.HashPayload(raw)
	if err := resolver.Put(workspace, hash, raw); err != nil {
		t.Fatal(err)
	}
	return hash
}

func matrixRequest(workspace string, definition contracts.CapabilityDefinition, systemType, systemID, inputHash, resourceKind, inputType string) contracts.OperationRequest {
	return contracts.OperationRequest{ID: "matrix-" + definition.Ref.Name + "-" + systemID, RequestID: "request-" + definition.Ref.Name + "-" + systemID, IdempotencyKey: "idempotency-" + definition.Ref.Name + "-" + systemID, WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "qualification-operator", Kind: "service", WorkspaceID: workspace}, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: systemType, ID: systemID, Version: "1"}, Kind: resourceKind, ID: systemID, Version: "1"}, InputType: inputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: inputHash, Profile: definition.Profile}
}
