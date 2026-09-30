package sqlreadonly_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/omaveda/fornix/internal/adapters/sqlreadonly"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestSQLReadonlyQueryUsesFixtureAndStableEvidence(t *testing.T) {
	payload := queryPayload()
	key := queryKey(t, payload)
	fixture := &sqlreadonly.FixtureDatabase{QueryResults: map[string]sqlreadonly.QueryResult{
		key: {Columns: []string{"id"}, Rows: [][]string{{"one"}}, Bytes: 3},
	}, DescribeResult: map[string]sqlreadonly.QueryResult{}, ExplainResults: map[string]sqlreadonly.QueryResult{}}
	resolver := connector.NewStaticPayloadResolver()
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

func TestSQLReadonlyRejectsLegacySQLAndUnsafeStructuredTermsBeforeDatabase(t *testing.T) {
	legacyPayloads := [][]byte{
		[]byte(`{"schema_version":1,"statement":"SELECT pg_read_file('/etc/passwd')"}`),
		[]byte(`{"schema_version":2,"schema":"public","table":"items","columns":["id"],"statement":"SELECT pg_read_file('/etc/passwd')"}`),
	}
	for i, rawPayload := range legacyPayloads {
		t.Run("legacy-sql-"+string(rune('a'+i)), func(t *testing.T) {
			fixture := emptyFixture()
			resolver := connector.NewStaticPayloadResolver()
			request, adapter := sqlRequestBytes(t, resolver, fixture, rawPayload)
			if err := executeSQL(t, request, adapter); err == nil {
				t.Fatal("legacy or unknown SQL field unexpectedly executed")
			}
			if fixture.Calls != 0 {
				t.Fatalf("invalid payload reached database calls=%d", fixture.Calls)
			}
		})
	}

	invalid := map[string]sqlreadonly.Payload{
		"unallowlisted-table": {SchemaVersion: 2, Schema: "public", Table: "secrets", Columns: []string{"id"}},
		"injected-column":     {SchemaVersion: 2, Schema: "public", Table: "items", Columns: []string{"id;drop"}},
		"unknown-operator":    {SchemaVersion: 2, Schema: "public", Table: "items", Columns: []string{"id"}, Filters: []sqlreadonly.Filter{{Column: "id", Operator: "= 1 OR true --", Value: json.RawMessage(`1`)}}},
		"function-expression": {SchemaVersion: 2, Schema: "public", Table: "items", Columns: []string{"pg_read_file()"}},
		"select-star":         {SchemaVersion: 2, Schema: "public", Table: "items", Columns: []string{"*"}},
	}
	for name, payload := range invalid {
		t.Run(name, func(t *testing.T) {
			fixture := emptyFixture()
			resolver := connector.NewStaticPayloadResolver()
			request, adapter := sqlRequest(t, resolver, fixture, payload)
			if err := executeSQL(t, request, adapter); err == nil {
				t.Fatal("invalid structured query unexpectedly executed")
			}
			if fixture.Calls != 0 {
				t.Fatalf("invalid structured query reached database calls=%d", fixture.Calls)
			}
		})
	}
}

func TestSQLReadonlyRejectsCrossWorkspaceAndBoundsResults(t *testing.T) {
	resolver := connector.NewStaticPayloadResolver()
	fixture := emptyFixture()
	request, adapter := sqlRequest(t, resolver, fixture, queryPayload())
	request.Target.WorkspaceID = "workspace-b"
	if err := executeSQL(t, request, adapter); err == nil {
		t.Fatal("cross-workspace SQL target unexpectedly executed")
	}

	payload := queryPayload()
	key := queryKey(t, payload)
	oversizedFixture := emptyFixture()
	oversizedFixture.QueryResults[key] = sqlreadonly.QueryResult{Columns: []string{"id"}, Rows: [][]string{{"too-large"}}, Bytes: 9}
	oversizedBinding := sqlreadonly.Binding{ID: "warehouse", WorkspaceID: "workspace-a", DatabaseRef: "warehouse", AllowedSchemas: []string{"public"}, AllowedTables: []string{"public.items"}, MaxResultBytes: 4}
	oversizedResolver := connector.NewStaticPayloadResolver()
	oversizedRequest, oversizedAdapter := sqlRequestWithBinding(t, oversizedResolver, oversizedFixture, payload, oversizedBinding)
	if err := executeSQL(t, oversizedRequest, oversizedAdapter); err == nil {
		t.Fatal("oversized SQL result unexpectedly succeeded")
	} else if strings.Contains(err.Error(), "too-large") {
		t.Fatal("raw SQL output appeared in error")
	}
}

func TestSQLReadonlyEnforcesDeterministicCostBudget(t *testing.T) {
	payload := queryPayload()
	fixture := emptyFixture()
	fixture.QueryResults[queryKey(t, payload)] = sqlreadonly.QueryResult{Columns: []string{"id"}, Rows: [][]string{{"one"}}, Bytes: 3}
	binding := sqlreadonly.Binding{ID: "warehouse", WorkspaceID: "workspace-a", DatabaseRef: "warehouse", AllowedSchemas: []string{"public"}, AllowedTables: []string{"public.items"}, MaxCostUnits: 1}
	resolver := connector.NewStaticPayloadResolver()
	request, adapter := sqlRequestWithBinding(t, resolver, fixture, payload, binding)
	if err := executeSQL(t, request, adapter); err == nil {
		t.Fatal("query exceeded its cost budget without failing")
	}
	if fixture.Calls != 1 {
		t.Fatalf("cost budget should be checked after bounded fixture execution, calls=%d", fixture.Calls)
	}
}

func TestSQLReadonlyEnforcesPerRequestBudgetsAfterDatabaseReturns(t *testing.T) {
	tests := map[string]struct {
		payload sqlreadonly.Payload
		result  sqlreadonly.QueryResult
	}{
		"row-limit": {
			payload: func() sqlreadonly.Payload { payload := queryPayload(); payload.Limit = 1; return payload }(),
			result:  sqlreadonly.QueryResult{Columns: []string{"id"}, Rows: [][]string{{"one"}, {"two"}}, Bytes: 6},
		},
		"byte-limit": {
			payload: func() sqlreadonly.Payload { payload := queryPayload(); payload.MaxBytes = 1; return payload }(),
			result:  sqlreadonly.QueryResult{Columns: []string{"id"}, Rows: [][]string{{"one"}}, Bytes: 3},
		},
		"inconsistent-byte-accounting": {
			payload: queryPayload(),
			result:  sqlreadonly.QueryResult{Columns: []string{"id"}, Rows: [][]string{{"one"}}, Bytes: 1},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := emptyFixture()
			fixture.QueryResults[queryKey(t, test.payload)] = test.result
			resolver := connector.NewStaticPayloadResolver()
			request, adapter := sqlRequest(t, resolver, fixture, test.payload)
			if err := executeSQL(t, request, adapter); err == nil {
				t.Fatal("database result exceeded the request budget without failing")
			}
			if fixture.Calls != 1 {
				t.Fatalf("fixture database calls=%d, want 1", fixture.Calls)
			}
		})
	}
}

func TestPGDatabaseUsesReadOnlyTransactionAndRejectsNonBaseRelations(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	schema := "public"
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	testTable := "fornix_sqlreadonly_items_" + suffix
	if _, err := pool.Exec(ctx, `CREATE TABLE "public".`+quoteForTest(testTable)+` (
		version text PRIMARY KEY,
		checksum text NOT NULL,
		exact_value numeric NOT NULL,
		nullable_text text,
		byte_data bytea NOT NULL,
		large_text text NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS "public".`+quoteForTest(testTable))
	}()
	if _, err := pool.Exec(ctx, `INSERT INTO "public".`+quoteForTest(testTable)+` (version,checksum,exact_value,nullable_text,byte_data,large_text) VALUES
		('1','alpha',9007199254740993.1234567890123456789,NULL,decode('00ff','hex'),'small'),
		('2','beta',9007199254740993.1234567890123456789,'<nil>',decode('01fe','hex'),repeat('x',1048576))`); err != nil {
		t.Fatal(err)
	}
	binding := sqlreadonly.Binding{ID: "qualification", WorkspaceID: "workspace-qualification", DatabaseRef: "qualification", AllowedSchemas: []string{schema}, AllowedTables: []string{schema + "." + testTable}}
	database, err := sqlreadonly.NewPGDatabase(pool, binding)
	if err != nil {
		t.Fatal(err)
	}
	query := sqlreadonly.QueryRequest{Schema: schema, Table: testTable, Columns: []string{"version"}, Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second}
	if _, err := database.QueryReadOnly(ctx, query); err != nil {
		t.Fatalf("persistent base-table query failed: %v", err)
	}
	// Keep one pooled session and exercise different prepared-statement shapes
	// in sequence. A fixed adapter-level statement name used to make this
	// depend on request order and pool assignment.
	query.Columns = []string{"version", "checksum"}
	query.Filters = []sqlreadonly.Filter{{Column: "version", Operator: sqlreadonly.FilterGreater, Value: json.RawMessage(`0`)}}
	if _, err := database.QueryReadOnly(ctx, query); err != nil {
		t.Fatalf("second query shape failed on the reused pool connection: %v", err)
	}
	if _, err := database.Describe(ctx, schema, testTable, 10, 4096, time.Second); err != nil {
		t.Fatalf("describe failed after query on the reused pool connection: %v", err)
	}
	if _, err := database.ExplainReadOnly(ctx, query); err != nil {
		t.Fatalf("explain failed after query on the reused pool connection: %v", err)
	}
	exactQuery := sqlreadonly.QueryRequest{
		Schema: schema, Table: testTable, Columns: []string{"checksum"},
		Filters: []sqlreadonly.Filter{{Column: "exact_value", Operator: sqlreadonly.FilterEqual, Value: json.RawMessage(`9007199254740993.1234567890123456789`)}},
		Limit:   1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second,
	}
	if result, err := database.QueryReadOnly(ctx, exactQuery); err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "alpha" {
		t.Fatalf("exact numeric filter did not match its precise value: result=%+v err=%v", result, err)
	}
	for _, row := range []struct {
		version string
		null    bool
	}{
		{version: "1", null: true},
		{version: "2", null: false},
	} {
		nullQuery := sqlreadonly.QueryRequest{
			Schema: schema, Table: testTable, Columns: []string{"nullable_text"},
			Filters: []sqlreadonly.Filter{{Column: "version", Operator: sqlreadonly.FilterEqual, Value: json.RawMessage(fmt.Sprintf("%q", row.version))}},
			Limit:   1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second,
		}
		result, err := database.QueryReadOnly(ctx, nullQuery)
		if err != nil || len(result.Rows) != 1 || len(result.Nulls) != 1 || result.Nulls[0][0] != row.null {
			t.Fatalf("SQL NULL distinction for version=%s: result=%+v err=%v", row.version, result, err)
		}
		if !row.null && result.Rows[0][0] != "<nil>" {
			t.Fatalf("literal nil text was changed: %+v", result)
		}
	}
	largeQuery := sqlreadonly.QueryRequest{
		Schema: schema, Table: testTable, Columns: []string{"large_text"},
		Filters: []sqlreadonly.Filter{{Column: "version", Operator: sqlreadonly.FilterEqual, Value: json.RawMessage(`"2"`)}},
		Limit:   1, MaxRows: 1, MaxBytes: 8, Timeout: time.Second,
	}
	largeResult, err := database.QueryReadOnly(ctx, largeQuery)
	if err != nil || !largeResult.Truncated || len(largeResult.Rows) != 0 || largeResult.Bytes > largeQuery.MaxBytes {
		t.Fatalf("large-cell result was not rejected within the disclosed-result budget: result=%+v err=%v", largeResult, err)
	}
	enumName := "fornix_sqlreadonly_enum_" + suffix
	customTable := "fornix_sqlreadonly_custom_" + suffix
	if _, err := pool.Exec(ctx, `CREATE TYPE "public".`+quoteForTest(enumName)+` AS ENUM ('one')`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), `DROP TYPE IF EXISTS "public".`+quoteForTest(enumName)) }()
	if _, err := pool.Exec(ctx, `CREATE TABLE "public".`+quoteForTest(customTable)+` (custom_value "public".`+quoteForTest(enumName)+`)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS "public".`+quoteForTest(customTable))
	}()
	customBinding := sqlreadonly.Binding{ID: "qualification_custom", WorkspaceID: "workspace-qualification", DatabaseRef: "qualification", AllowedSchemas: []string{"public"}, AllowedTables: []string{"public." + customTable}}
	customDB, err := sqlreadonly.NewPGDatabase(pool, customBinding)
	if err != nil {
		t.Fatal(err)
	}
	customQuery := sqlreadonly.QueryRequest{Schema: "public", Table: customTable, Columns: []string{"custom_value"}, Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second}
	if _, err := customDB.QueryReadOnly(ctx, customQuery); err == nil {
		t.Fatal("read-only adapter accepted a user-defined column type")
	}
	readonlyTx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readonlyTx.Exec(ctx, `UPDATE `+quoteForTest(schema)+`.`+quoteForTest(testTable)+` SET checksum=checksum`); err == nil {
		_ = readonlyTx.Rollback(ctx)
		t.Fatal("PostgreSQL read-only transaction accepted a mutation")
	}
	_ = readonlyTx.Rollback(ctx)

	if _, err := pool.Exec(ctx, `CREATE TEMP VIEW fornix_readonly_qualification_view AS SELECT version FROM `+quoteForTest(schema)+`.`+quoteForTest(testTable)); err != nil {
		t.Fatal(err)
	}
	tempTable := "fornix_sqlreadonly_temp_" + suffix
	if _, err := pool.Exec(ctx, `CREATE TEMP TABLE `+quoteForTest(tempTable)+` (version text)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS pg_temp.`+quoteForTest(tempTable))
	}()
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP VIEW IF EXISTS pg_temp.fornix_readonly_qualification_view`)
	}()
	var tempSchema string
	if err := pool.QueryRow(ctx, `SELECT n.nspname FROM pg_catalog.pg_namespace AS n WHERE n.oid=pg_catalog.pg_my_temp_schema()`).Scan(&tempSchema); err != nil {
		t.Fatal(err)
	}
	viewBinding := sqlreadonly.Binding{ID: "qualification_view", WorkspaceID: "workspace-qualification", DatabaseRef: "qualification", AllowedSchemas: []string{tempSchema}, AllowedTables: []string{tempSchema + ".fornix_readonly_qualification_view"}}
	viewDB, err := sqlreadonly.NewPGDatabase(pool, viewBinding)
	if err != nil {
		t.Fatal(err)
	}
	viewQuery := sqlreadonly.QueryRequest{Schema: tempSchema, Table: "fornix_readonly_qualification_view", Columns: []string{"version"}, Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second}
	if _, err := viewDB.QueryReadOnly(ctx, viewQuery); err == nil {
		t.Fatal("read-only adapter accepted a view")
	}
	tempBinding := sqlreadonly.Binding{ID: "qualification_temp", WorkspaceID: "workspace-qualification", DatabaseRef: "qualification", AllowedSchemas: []string{tempSchema}, AllowedTables: []string{tempSchema + "." + tempTable}}
	tempDB, err := sqlreadonly.NewPGDatabase(pool, tempBinding)
	if err != nil {
		t.Fatal(err)
	}
	tempQuery := sqlreadonly.QueryRequest{Schema: tempSchema, Table: tempTable, Columns: []string{"version"}, Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second}
	if _, err := tempDB.QueryReadOnly(ctx, tempQuery); err == nil {
		t.Fatal("read-only adapter accepted a temporary table")
	}
}

func queryPayload() sqlreadonly.Payload {
	return sqlreadonly.Payload{SchemaVersion: 2, Schema: "public", Table: "items", Columns: []string{"id"}, Filters: []sqlreadonly.Filter{{Column: "id", Operator: sqlreadonly.FilterEqual, Value: json.RawMessage(`"one"`)}}, Limit: 10}
}

func emptyFixture() *sqlreadonly.FixtureDatabase {
	return &sqlreadonly.FixtureDatabase{QueryResults: map[string]sqlreadonly.QueryResult{}, DescribeResult: map[string]sqlreadonly.QueryResult{}, ExplainResults: map[string]sqlreadonly.QueryResult{}}
}

func queryKey(t *testing.T, payload sqlreadonly.Payload) string {
	t.Helper()
	if err := payload.Normalize(sqlreadonly.QueryCapabilityName); err != nil {
		t.Fatal(err)
	}
	key, err := sqlreadonly.QueryIdentityHash(sqlreadonly.QueryRequest{Schema: payload.Schema, Table: payload.Table, Columns: payload.Columns, Filters: payload.Filters, OrderBy: payload.OrderBy, Limit: payload.Limit, MaxRows: payload.Limit, MaxBytes: sqlreadonly.DefaultMaxResultBytes, Timeout: sqlreadonly.DefaultTimeout})
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func executeSQL(t *testing.T, request contracts.OperationRequest, adapter *sqlreadonly.Connector) error {
	t.Helper()
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	_, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{})
	return err
}

func sqlRequest(t *testing.T, resolver *connector.StaticPayloadResolver, fixture *sqlreadonly.FixtureDatabase, payload sqlreadonly.Payload) (contracts.OperationRequest, *sqlreadonly.Connector) {
	t.Helper()
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return sqlRequestBytes(t, resolver, fixture, payloadBytes)
}

func sqlRequestBytes(t *testing.T, resolver *connector.StaticPayloadResolver, fixture *sqlreadonly.FixtureDatabase, payloadBytes []byte) (contracts.OperationRequest, *sqlreadonly.Connector) {
	t.Helper()
	binding := sqlreadonly.Binding{ID: "warehouse", WorkspaceID: "workspace-a", DatabaseRef: "warehouse", AllowedSchemas: []string{"public"}, AllowedTables: []string{"public.items"}}
	return sqlRequestWithBindingBytes(t, resolver, fixture, payloadBytes, binding)
}

func sqlRequestWithBinding(t *testing.T, resolver *connector.StaticPayloadResolver, fixture *sqlreadonly.FixtureDatabase, payload sqlreadonly.Payload, binding sqlreadonly.Binding) (contracts.OperationRequest, *sqlreadonly.Connector) {
	t.Helper()
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return sqlRequestWithBindingBytes(t, resolver, fixture, payloadBytes, binding)
}

func sqlRequestWithBindingBytes(t *testing.T, resolver *connector.StaticPayloadResolver, fixture *sqlreadonly.FixtureDatabase, payloadBytes []byte, binding sqlreadonly.Binding) (contracts.OperationRequest, *sqlreadonly.Connector) {
	t.Helper()
	inputHash := hash(payloadBytes)
	if err := resolver.Put(binding.WorkspaceID, inputHash, payloadBytes); err != nil {
		t.Fatal(err)
	}
	adapter, err := sqlreadonly.NewConnector(binding, resolver.Resolve, fixture)
	if err != nil {
		t.Fatal(err)
	}
	definition := adapter.Capabilities()[1].Definition()
	request := contracts.OperationRequest{ID: "operation-sql", RequestID: "request-sql", IdempotencyKey: "sql-idempotency", WorkspaceID: binding.WorkspaceID, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: binding.WorkspaceID}, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: binding.WorkspaceID, System: contracts.SystemRef{WorkspaceID: binding.WorkspaceID, Type: "database", ID: binding.ID, Version: "1"}, Kind: sqlreadonly.ResourceKind, ID: binding.ID, Version: "1"}, InputType: sqlreadonly.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: inputHash, Profile: definition.Profile}
	return request, adapter
}

func quoteForTest(identifier string) string { return `"` + identifier + `"` }

func hash(value []byte) string { digest := sha256.Sum256(value); return hex.EncodeToString(digest[:]) }
