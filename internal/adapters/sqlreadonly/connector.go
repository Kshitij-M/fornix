// Package sqlreadonly exposes bounded, structured read-only SQL capabilities
// and never makes an external database the authority for Fornix operation
// state.
package sqlreadonly

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

const (
	ConnectorName          = "sqlreadonly"
	ConnectorVersion       = "2"
	DescribeCapabilityName = "describe"
	QueryCapabilityName    = "query_readonly"
	ExplainCapabilityName  = "explain_readonly"
	ResourceKind           = "database_table"
	InputType              = "sql.request.v2"
	DefaultMaxRows         = 1000
	MaxMaxRows             = 10000
	DefaultMaxResultBytes  = 1 << 20
	MaxMaxResultBytes      = 16 << 20
	DefaultMaxCostUnits    = 100000
	MaxMaxCostUnits        = 1000000
	DefaultTimeout         = 5 * time.Second
	MaxTimeout             = 10 * time.Minute
	MaxPayloadBytes        = 64 << 10
	MaxQueryColumns        = 64
	MaxFilters             = 32
	MaxOrderTerms          = 8
	MaxInValues            = 32
	MaxParameterBytes      = 4096
)

var (
	identifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

// Binding is the non-secret workspace binding for one external database.
// DatabaseRef is a logical identity; the connection pool is supplied by the
// process and is never persisted in this value.
type Binding struct {
	ID             string   `json:"id"`
	WorkspaceID    string   `json:"workspace_id"`
	DatabaseRef    string   `json:"database_ref"`
	CredentialRef  string   `json:"credential_ref,omitempty"`
	AllowedSchemas []string `json:"allowed_schemas"`
	AllowedTables  []string `json:"allowed_tables"`
	MaxRows        int      `json:"max_rows"`
	MaxResultBytes int64    `json:"max_result_bytes"`
	MaxCostUnits   int64    `json:"max_cost_units"`
	TimeoutMS      int64    `json:"timeout_ms"`
}

// Payload is the versioned structured query envelope resolved by
// OperationRequest.InputHash. It intentionally has no SQL text field.
type Payload struct {
	SchemaVersion int         `json:"schema_version"`
	Schema        string      `json:"schema"`
	Table         string      `json:"table"`
	Columns       []string    `json:"columns,omitempty"`
	Filters       []Filter    `json:"filters,omitempty"`
	OrderBy       []OrderTerm `json:"order_by,omitempty"`
	Limit         int         `json:"limit,omitempty"`
	MaxBytes      int64       `json:"max_bytes,omitempty"`
}

// FilterOperator is a closed set of comparison operators emitted by the SQL
// compiler. Callers cannot provide SQL fragments.
type FilterOperator string

const (
	FilterEqual        FilterOperator = "eq"
	FilterNotEqual     FilterOperator = "neq"
	FilterLess         FilterOperator = "lt"
	FilterLessEqual    FilterOperator = "lte"
	FilterGreater      FilterOperator = "gt"
	FilterGreaterEqual FilterOperator = "gte"
	FilterLike         FilterOperator = "like"
	FilterILike        FilterOperator = "ilike"
	FilterIn           FilterOperator = "in"
	FilterIsNull       FilterOperator = "is_null"
	FilterIsNotNull    FilterOperator = "is_not_null"
)

// Filter contains one structured predicate. Value and Values must be JSON
// scalar literals and are always bound as driver parameters.
type Filter struct {
	Column   string            `json:"column"`
	Operator FilterOperator    `json:"operator"`
	Value    json.RawMessage   `json:"value,omitempty"`
	Values   []json.RawMessage `json:"values,omitempty"`
}

// OrderDirection is the only caller-controlled ordering fragment accepted.
type OrderDirection string

const (
	OrderAscending  OrderDirection = "asc"
	OrderDescending OrderDirection = "desc"
)

type OrderTerm struct {
	Column    string         `json:"column"`
	Direction OrderDirection `json:"direction"`
}

// QueryRequest is the normalized structured query passed to a database
// driver. It cannot carry caller-authored SQL.
type QueryRequest struct {
	Schema   string
	Table    string
	Columns  []string
	Filters  []Filter
	OrderBy  []OrderTerm
	Limit    int
	MaxRows  int
	MaxBytes int64
	Timeout  time.Duration
}

// QueryResult is an ephemeral bounded result. Connector results persist only
// its hashes and counts in OperationResult; Rows never cross that boundary.
type QueryResult struct {
	Columns []string
	Rows    [][]string
	// Nulls preserves SQL NULL separately from a text value such as "<nil>".
	// Each entry, when present, has one bool per cell in the corresponding row.
	Nulls     [][]bool `json:"nulls,omitempty"`
	Bytes     int64
	CostUnits int64
	Truncated bool
}

// Database is the narrow driver seam. Implementations must enforce the
// read-only transaction and timeout contract before returning.
type Database interface {
	QueryReadOnly(context.Context, QueryRequest) (QueryResult, error)
	Describe(context.Context, string, string, int, int64, time.Duration) (QueryResult, error)
	ExplainReadOnly(context.Context, QueryRequest) (QueryResult, error)
}

// PGDatabase adapts a pgx pool to the read-only driver seam. Its table binding
// is supplied by NewConnector (or NewPGDatabase) and cannot be widened by an
// individual query request.
type PGDatabase struct {
	Pool    *pgxpool.Pool
	binding Binding
}

// NewPGDatabase creates a driver with an explicit immutable table allowlist.
func NewPGDatabase(pool *pgxpool.Pool, binding Binding) (PGDatabase, error) {
	if pool == nil {
		return PGDatabase{}, fmt.Errorf("SQL database is not configured")
	}
	if err := binding.Normalize(); err != nil {
		return PGDatabase{}, err
	}
	return PGDatabase{Pool: pool, binding: binding}, nil
}

func (d PGDatabase) QueryReadOnly(ctx context.Context, request QueryRequest) (QueryResult, error) {
	return d.query(ctx, request, false)
}

func (d PGDatabase) ExplainReadOnly(ctx context.Context, request QueryRequest) (QueryResult, error) {
	return d.query(ctx, request, true)
}

func (d PGDatabase) Describe(ctx context.Context, schema, table string, maxRows int, maxBytes int64, timeout time.Duration) (QueryResult, error) {
	request := QueryRequest{Schema: schema, Table: table, MaxRows: maxRows, MaxBytes: maxBytes, Timeout: timeout}
	if err := d.validateBinding(request); err != nil {
		return QueryResult{}, err
	}
	return d.describe(ctx, request)
}

func (d PGDatabase) query(ctx context.Context, request QueryRequest, explain bool) (QueryResult, error) {
	if d.Pool == nil {
		return QueryResult{}, fmt.Errorf("SQL database is not configured")
	}
	if err := request.Normalize(); err != nil {
		return QueryResult{}, err
	}
	if err := d.validateBinding(request); err != nil {
		return QueryResult{}, err
	}
	statement, parameters, err := compileQuery(request)
	if err != nil {
		return QueryResult{}, err
	}
	if explain {
		statement = "EXPLAIN (FORMAT JSON) " + statement
	}
	ctx, cancel := context.WithTimeout(ctx, request.Timeout)
	defer cancel()
	tx, err := d.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL read transaction unavailable", err)
	}
	defer rollbackReadOnly(tx)
	if _, err := tx.Exec(ctx, `SELECT set_config('statement_timeout',$1,true)`, fmt.Sprintf("%dms", request.Timeout.Milliseconds())); err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL timeout configuration failed", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('search_path','pg_catalog',true)`); err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL search path configuration failed", err)
	}
	if err := validateCatalogRelation(ctx, tx, request, false); err != nil {
		return QueryResult{}, err
	}
	// Query the generated statement directly. A fixed prepared-statement name
	// is unsafe on pooled connections because the SQL shape changes with the
	// selected table, columns, and filters. pgx owns statement caching by SQL
	// text when configured; the adapter must not alias distinct statements.
	rows, err := tx.Query(ctx, statement, parameters...)
	if err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL read failed", err)
	}
	result, err := collectRows(ctx, rows, request.MaxRows, request.MaxBytes)
	if err != nil {
		return QueryResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL read commit failed", err)
	}
	return result, nil
}

func (d PGDatabase) describe(ctx context.Context, request QueryRequest) (QueryResult, error) {
	if d.Pool == nil {
		return QueryResult{}, fmt.Errorf("SQL database is not configured")
	}
	if request.MaxRows < 1 || request.MaxRows > MaxMaxRows || request.MaxBytes < 1 || request.MaxBytes > MaxMaxResultBytes || request.Timeout <= 0 || request.Timeout > MaxTimeout {
		return QueryResult{}, fmt.Errorf("SQL describe budgets are outside bounds")
	}
	ctx, cancel := context.WithTimeout(ctx, request.Timeout)
	defer cancel()
	tx, err := d.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL read transaction unavailable", err)
	}
	defer rollbackReadOnly(tx)
	if _, err := tx.Exec(ctx, `SELECT set_config('statement_timeout',$1,true)`, fmt.Sprintf("%dms", request.Timeout.Milliseconds())); err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL timeout configuration failed", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('search_path','pg_catalog',true)`); err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL search path configuration failed", err)
	}
	if err := validateCatalogRelation(ctx, tx, request, true); err != nil {
		return QueryResult{}, err
	}
	statement := `SELECT column_name,data_type FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position`
	rows, err := tx.Query(ctx, statement, request.Schema, request.Table)
	if err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL describe failed", err)
	}
	result, err := collectRows(ctx, rows, request.MaxRows, request.MaxBytes)
	if err != nil {
		return QueryResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL read commit failed", err)
	}
	return result, nil
}

func rollbackReadOnly(tx pgx.Tx) {
	if tx == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(cleanupCtx)
}

func isStatementTimeout(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == "57014"
}

func boundedDatabaseError(ctx context.Context, message string, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || isStatementTimeout(err) {
		return context.DeadlineExceeded
	}
	return fmt.Errorf("%s", message)
}

func (d PGDatabase) validateBinding(request QueryRequest) error {
	if d.binding.ID == "" || d.binding.WorkspaceID == "" {
		return fmt.Errorf("SQL database binding is not configured")
	}
	if !d.binding.allowedTable(request.Schema, request.Table) {
		return fmt.Errorf("SQL table is not allowlisted")
	}
	return nil
}

func validateCatalogRelation(ctx context.Context, tx pgx.Tx, request QueryRequest, describe bool) error {
	rows, err := tx.Query(ctx, `
SELECT c.relkind::text,c.relpersistence::text,COALESCE(a.attname,''),
       COALESCE(tn.nspname='pg_catalog' AND t.typtype='b' AND t.typcategory<>'A',false)
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid=c.relnamespace
LEFT JOIN pg_catalog.pg_attribute AS a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_type AS t ON t.oid=a.atttypid
LEFT JOIN pg_catalog.pg_namespace AS tn ON tn.oid=t.typnamespace
WHERE n.nspname=$1 AND c.relname=$2
ORDER BY a.attnum`, request.Schema, request.Table)
	if err != nil {
		return boundedDatabaseError(ctx, "SQL table metadata is unavailable", err)
	}
	defer rows.Close()
	found := false
	var relationKind, persistence string
	columns := make(map[string]bool)
	for rows.Next() {
		var rowKind, rowPersistence string
		var name string
		var builtin bool
		if err := rows.Scan(&rowKind, &rowPersistence, &name, &builtin); err != nil {
			return boundedDatabaseError(ctx, "SQL table metadata is unavailable", err)
		}
		if !found {
			found = true
			relationKind, persistence = rowKind, rowPersistence
		} else if relationKind != rowKind || persistence != rowPersistence {
			return fmt.Errorf("SQL table metadata changed during validation")
		}
		if name != "" {
			columns[name] = builtin
		}
	}
	if err := rows.Err(); err != nil {
		return boundedDatabaseError(ctx, "SQL table metadata is unavailable", err)
	}
	if !found {
		return fmt.Errorf("SQL table is unavailable")
	}
	if relationKind != "r" || persistence != "p" {
		return fmt.Errorf("SQL connector permits only persistent ordinary tables")
	}
	if len(columns) == 0 {
		return fmt.Errorf("SQL table has no readable columns")
	}
	if describe {
		return nil
	}
	for _, column := range referencedColumns(request) {
		builtin, exists := columns[column]
		if !exists || !builtin {
			return fmt.Errorf("SQL query column is unavailable or has an unsupported type")
		}
	}
	return nil
}

func collectRows(ctx context.Context, rows pgx.Rows, maxRows int, maxBytes int64) (QueryResult, error) {
	defer rows.Close()
	fields := rows.FieldDescriptions()
	result := QueryResult{Columns: make([]string, 0, len(fields))}
	for _, field := range fields {
		result.Columns = append(result.Columns, field.Name)
	}
	for rows.Next() {
		if len(result.Rows) >= maxRows {
			result.Truncated = true
			break
		}
		values, err := rows.Values()
		if err != nil {
			return QueryResult{}, boundedDatabaseError(ctx, "SQL result decode failed", err)
		}
		row := make([]string, len(values))
		nulls := make([]bool, len(values))
		rowBytes := int64(0)
		for index, value := range values {
			if value == nil {
				nulls[index] = true
				continue
			}
			row[index] = fmt.Sprint(value)
			rowBytes += int64(len(row[index]))
		}
		if result.Bytes+rowBytes > maxBytes {
			result.Truncated = true
			break
		}
		result.Rows = append(result.Rows, row)
		result.Nulls = append(result.Nulls, nulls)
		result.Bytes += rowBytes
		result.CostUnits += 1 + (rowBytes+1023)/1024
	}
	if err := rows.Err(); err != nil {
		return QueryResult{}, boundedDatabaseError(ctx, "SQL result iteration failed", err)
	}
	return result, nil
}

// FixtureDatabase is an offline deterministic database for conformance,
// replay, and unit tests. Keys are statement/schema/table identities; no live
// database is contacted.
type FixtureDatabase struct {
	QueryResults   map[string]QueryResult
	DescribeResult map[string]QueryResult
	ExplainResults map[string]QueryResult
	Calls          int
}

func (f *FixtureDatabase) QueryReadOnly(_ context.Context, request QueryRequest) (QueryResult, error) {
	if f == nil {
		return QueryResult{}, fmt.Errorf("fixture database is nil")
	}
	f.Calls++
	key, err := QueryIdentityHash(request)
	if err != nil {
		return QueryResult{}, err
	}
	result, ok := f.QueryResults[key]
	if !ok {
		return QueryResult{}, fmt.Errorf("fixture query not found")
	}
	return cloneResult(result), nil
}

func (f *FixtureDatabase) Describe(_ context.Context, schema, table string, _ int, _ int64, _ time.Duration) (QueryResult, error) {
	if f == nil {
		return QueryResult{}, fmt.Errorf("fixture database is nil")
	}
	f.Calls++
	result, ok := f.DescribeResult[schema+"."+table]
	if !ok {
		return QueryResult{}, fmt.Errorf("fixture description not found")
	}
	return cloneResult(result), nil
}

func (f *FixtureDatabase) ExplainReadOnly(_ context.Context, request QueryRequest) (QueryResult, error) {
	if f == nil {
		return QueryResult{}, fmt.Errorf("fixture database is nil")
	}
	f.Calls++
	key, err := QueryIdentityHash(request)
	if err != nil {
		return QueryResult{}, err
	}
	result, ok := f.ExplainResults[key]
	if !ok {
		return QueryResult{}, fmt.Errorf("fixture explanation not found")
	}
	return cloneResult(result), nil
}

type Connector struct {
	binding       Binding
	definitionRef contracts.ConnectorRef
	resolver      connector.PayloadResolver
	database      Database
	definitions   map[string]contracts.CapabilityDefinition
}

func NewConnector(binding Binding, resolver connector.PayloadResolver, database Database) (*Connector, error) {
	if err := binding.Normalize(); err != nil {
		return nil, err
	}
	if resolver == nil || database == nil {
		return nil, fmt.Errorf("SQL payload resolver and database are required")
	}
	boundDatabase, err := bindPGDatabase(database, binding)
	if err != nil {
		return nil, err
	}
	database = boundDatabase
	ref := contracts.ConnectorRef{WorkspaceID: binding.WorkspaceID, Name: ConnectorName, Version: ConnectorVersion}
	if err := ref.Normalize(); err != nil {
		return nil, err
	}
	credentialRefs := []string(nil)
	if binding.CredentialRef != "" {
		credentialRefs = []string{binding.CredentialRef}
	}
	definitions := make(map[string]contracts.CapabilityDefinition, 3)
	for _, spec := range []struct {
		name, input, output string
		effect              contracts.EffectClass
	}{
		{name: DescribeCapabilityName, input: "sql.describe.input.v2", output: "sql.describe.output.v1", effect: contracts.EffectClassObservation},
		{name: QueryCapabilityName, input: "sql.query.input.v2", output: "sql.query.output.v1", effect: contracts.EffectClassReadOnly},
		{name: ExplainCapabilityName, input: "sql.explain.input.v2", output: "sql.explain.output.v1", effect: contracts.EffectClassObservation},
	} {
		definition := contracts.CapabilityDefinition{WorkspaceID: binding.WorkspaceID, Ref: contracts.CapabilityRef{WorkspaceID: binding.WorkspaceID, Connector: ref, Name: spec.name, Version: "2"}, Description: "bounded read-only SQL capability", InputSchemaVersion: 2, InputSchemaHash: schemaHash(spec.input), OutputSchemaVersion: 1, OutputSchemaHash: schemaHash(spec.output), Effect: spec.effect, Profile: sqlProfile(), Evidence: []contracts.EvidenceRequirement{{WorkspaceID: binding.WorkspaceID, Kind: "sql_result", MinItems: 1, MaxItems: 1, RequireHash: true, RequireProvenance: true}}, ResourceKinds: []string{ResourceKind}, RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"}, MaxRows: binding.MaxRows, RateLimitPerMinute: 60, RequiredCredentialRefs: credentialRefs, SupportsCancellation: true, SupportsIdempotency: true, SupportsVerification: true, Enabled: true}
		if err := definition.Normalize(); err != nil {
			return nil, fmt.Errorf("SQL capability %s: %w", spec.name, err)
		}
		definitions[spec.name] = definition
	}
	return &Connector{binding: binding, definitionRef: ref, resolver: resolver, database: database, definitions: definitions}, nil
}

func (b *Binding) Normalize() error {
	if b == nil || b.ID == "" || b.WorkspaceID == "" || b.DatabaseRef == "" {
		return fmt.Errorf("SQL binding identity is required")
	}
	if !identifierPattern.MatchString(strings.ToLower(b.ID)) || !identifierPattern.MatchString(strings.ToLower(b.DatabaseRef)) {
		return fmt.Errorf("SQL binding identity is invalid")
	}
	b.ID, b.DatabaseRef = strings.ToLower(b.ID), strings.ToLower(b.DatabaseRef)
	b.AllowedSchemas = normalizeIdentifiers(b.AllowedSchemas)
	b.AllowedTables = normalizeIdentifiers(b.AllowedTables)
	if len(b.AllowedSchemas) == 0 || len(b.AllowedTables) == 0 {
		return fmt.Errorf("SQL binding requires schema and table allowlists")
	}
	for _, schema := range b.AllowedSchemas {
		if !identifierPattern.MatchString(schema) {
			return fmt.Errorf("SQL schema allowlist contains an invalid identifier")
		}
	}
	for _, table := range b.AllowedTables {
		parts := strings.Split(table, ".")
		if len(parts) != 2 || !identifierPattern.MatchString(parts[0]) || !identifierPattern.MatchString(parts[1]) {
			return fmt.Errorf("SQL table allowlist requires schema.table identifiers")
		}
		if !contains(b.AllowedSchemas, parts[0]) {
			return fmt.Errorf("SQL table allowlist schema is not allowlisted")
		}
	}
	if b.MaxRows == 0 {
		b.MaxRows = DefaultMaxRows
	}
	if b.MaxResultBytes == 0 {
		b.MaxResultBytes = DefaultMaxResultBytes
	}
	if b.MaxCostUnits == 0 {
		b.MaxCostUnits = DefaultMaxCostUnits
	}
	if b.TimeoutMS == 0 {
		b.TimeoutMS = DefaultTimeout.Milliseconds()
	}
	if b.MaxRows < 1 || b.MaxRows > MaxMaxRows || b.MaxResultBytes < 1 || b.MaxResultBytes > MaxMaxResultBytes || b.MaxCostUnits < 1 || b.MaxCostUnits > MaxMaxCostUnits || b.TimeoutMS < 1 || time.Duration(b.TimeoutMS)*time.Millisecond > MaxTimeout {
		return fmt.Errorf("SQL binding budgets are outside bounds")
	}
	if b.CredentialRef != "" && !strings.Contains(b.CredentialRef, "/") {
		return fmt.Errorf("SQL credential reference is invalid")
	}
	return nil
}

func (b Binding) Contract(actor contracts.ActorRef) (contracts.ConnectorBinding, error) {
	if err := b.Normalize(); err != nil {
		return contracts.ConnectorBinding{}, err
	}
	configuration, err := json.Marshal(b)
	if err != nil {
		return contracts.ConnectorBinding{}, err
	}
	refs := []string(nil)
	if b.CredentialRef != "" {
		refs = []string{b.CredentialRef}
	}
	value := contracts.ConnectorBinding{ID: b.ID, WorkspaceID: b.WorkspaceID, Connector: contracts.ConnectorRef{WorkspaceID: b.WorkspaceID, Name: ConnectorName, Version: ConnectorVersion}, Kind: contracts.ConnectorBindingSQLReadonly, Version: 1, Configuration: configuration, CredentialRefs: refs, Status: contracts.ConnectorBindingActive, CreatedBy: actor}
	if err := value.Normalize(); err != nil {
		return contracts.ConnectorBinding{}, err
	}
	return value, nil
}

func (c *Connector) Definition() contracts.ConnectorRef { return c.definitionRef }

func (c *Connector) Capabilities() []connector.Capability {
	if c == nil {
		return nil
	}
	names := []string{DescribeCapabilityName, QueryCapabilityName, ExplainCapabilityName}
	out := make([]connector.Capability, 0, len(names))
	for _, name := range names {
		out = append(out, &capability{parent: c, definition: c.definitions[name], name: name})
	}
	return out
}

func (c *Connector) Health(context.Context) connector.HealthStatus {
	if c == nil || c.database == nil || c.resolver == nil {
		return connector.HealthStatus{Status: connector.HealthUnavailable, Reason: "SQL connector is not configured"}
	}
	return connector.HealthStatus{Status: connector.HealthReady}
}

type capability struct {
	parent     *Connector
	definition contracts.CapabilityDefinition
	name       string
}

func (c *capability) Definition() contracts.CapabilityDefinition { return c.definition }

func (c *capability) Validate(request contracts.OperationRequest) error {
	if c == nil || c.parent == nil {
		return fmt.Errorf("SQL capability is not configured")
	}
	if err := contracts.ValidateOperationRequest(request, c.definition); err != nil {
		return err
	}
	if request.Target.System.Type != "database" || request.Target.System.ID != c.parent.binding.ID {
		return fmt.Errorf("SQL request target is not bound to connector")
	}
	return nil
}

func (c *capability) Plan(request contracts.OperationRequest) (contracts.OperationPlan, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationPlan{}, err
	}
	plan := contracts.OperationPlan{ID: request.ID + "-plan", OperationID: request.ID, OperationHash: request.StableHash(), WorkspaceID: request.WorkspaceID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: request.ID + "-sql", Ordinal: 0, Kind: "sql." + c.name, Capability: request.Capability, Target: request.Target, Effect: c.definition.Effect, Profile: request.Profile, Evidence: c.definition.Evidence, InputHash: request.InputHash}}}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationPlan{}, err
	}
	return plan, nil
}

func (c *capability) Execute(ctx context.Context, request contracts.OperationRequest, plan contracts.OperationPlan) (contracts.OperationResult, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationResult{}, err
	}
	payloadBytes, err := c.parent.resolver(ctx, request.WorkspaceID, request.InputHash)
	if err != nil || !connector.VerifyPayload(payloadBytes, request.InputHash) || len(payloadBytes) > MaxPayloadBytes {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: err}
	}
	var payload Payload
	decoder := json.NewDecoder(bytes.NewReader(payloadBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: err}
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: fmt.Errorf("SQL payload contains trailing data")}
	}
	if err := payload.Normalize(c.name); err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: err}
	}
	if !c.parent.allowedTable(payload.Schema, payload.Table) {
		return contracts.OperationResult{}, &connector.FailureError{Code: "unauthorized", Retryable: false}
	}
	resultMaxRows := min(payload.Limit, c.parent.binding.MaxRows)
	resultMaxBytes := minBytes(payload.MaxBytes, c.parent.binding.MaxResultBytes)
	var result QueryResult
	switch c.name {
	case DescribeCapabilityName:
		result, err = c.parent.database.Describe(ctx, payload.Schema, payload.Table, resultMaxRows, resultMaxBytes, time.Duration(c.parent.binding.TimeoutMS)*time.Millisecond)
	case QueryCapabilityName:
		result, err = c.parent.database.QueryReadOnly(ctx, payload.queryRequest(c.parent.binding))
	case ExplainCapabilityName:
		result, err = c.parent.database.ExplainReadOnly(ctx, payload.queryRequest(c.parent.binding))
	}
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return contracts.OperationResult{}, &connector.FailureError{Code: "cancelled", Retryable: false}
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) || isStatementTimeout(err) {
			return contracts.OperationResult{}, &connector.FailureError{Code: "timeout", Retryable: false, Err: err}
		}
		return contracts.OperationResult{}, &connector.FailureError{Code: "adapter", Retryable: false, Err: err}
	}
	if err := validateQueryResult(result, resultMaxRows, resultMaxBytes); err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "budget", Retryable: false}
	}
	result.CostUnits = boundedCostUnits(result)
	if result.CostUnits > c.parent.binding.MaxCostUnits {
		return contracts.OperationResult{}, &connector.FailureError{Code: "budget", Retryable: false}
	}
	summary := resultSummary(result, request.InputHash)
	summaryBytes, _ := json.Marshal(summary)
	outputHash := connector.HashPayload(summaryBytes)
	evidenceHash := connector.HashPayload(append(append([]byte(nil), summaryBytes...), []byte("\x00"+request.InputHash)...))
	evidence := contracts.OperationEvidenceRef{WorkspaceID: request.WorkspaceID, SourceReference: "sql:" + c.parent.binding.ID + ":" + request.InputHash[:16], EvidenceHash: evidenceHash, Role: "sql_result"}
	operationResult := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash, Evidence: []contracts.OperationEvidenceRef{evidence}, Steps: []contracts.OperationStepResult{{StepID: plan.Steps[0].ID, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash, Evidence: []contracts.OperationEvidenceRef{evidence}}}}
	if err := operationResult.Normalize(); err != nil {
		return contracts.OperationResult{}, err
	}
	return operationResult, nil
}

func (p *Payload) Normalize(kind string) error {
	if p == nil {
		return fmt.Errorf("SQL payload is nil")
	}
	if p.SchemaVersion != 2 {
		return fmt.Errorf("SQL payload schema version is unsupported")
	}
	if p.Limit == 0 {
		p.Limit = DefaultMaxRows
	}
	if p.Limit < 1 || p.Limit > MaxMaxRows || p.MaxBytes < 0 || p.MaxBytes > MaxMaxResultBytes {
		return fmt.Errorf("SQL result budget is invalid")
	}
	if p.MaxBytes == 0 {
		p.MaxBytes = DefaultMaxResultBytes
	}
	if !identifierPattern.MatchString(strings.ToLower(p.Schema)) || !identifierPattern.MatchString(strings.ToLower(p.Table)) {
		return fmt.Errorf("SQL relation identity is invalid")
	}
	p.Schema, p.Table = strings.ToLower(p.Schema), strings.ToLower(p.Table)
	if kind == DescribeCapabilityName {
		if len(p.Columns) != 0 || len(p.Filters) != 0 || len(p.OrderBy) != 0 {
			return fmt.Errorf("SQL describe request cannot include query terms")
		}
		return nil
	}
	if kind != QueryCapabilityName && kind != ExplainCapabilityName {
		return fmt.Errorf("SQL capability is unknown")
	}
	query := p.queryRequest(Binding{MaxRows: MaxMaxRows, MaxResultBytes: MaxMaxResultBytes, TimeoutMS: int64(MaxTimeout / time.Millisecond)})
	if err := query.Normalize(); err != nil {
		return err
	}
	p.Schema, p.Table = query.Schema, query.Table
	p.Columns, p.Filters, p.OrderBy = query.Columns, query.Filters, query.OrderBy
	return nil
}

func resultSummary(result QueryResult, inputHash string) map[string]any {
	canonical, _ := json.Marshal(result)
	columns := append([]string(nil), result.Columns...)
	return map[string]any{"input_hash": inputHash, "columns": columns, "columns_hash": connector.HashPayload([]byte(strings.Join(columns, "\x00"))), "rows": len(result.Rows), "bytes": result.Bytes, "cost_units": boundedCostUnits(result), "truncated": result.Truncated, "result_hash": connector.HashPayload(canonical)}
}

func validateQueryResult(result QueryResult, maxRows int, maxBytes int64) error {
	if result.Bytes < 0 || result.CostUnits < 0 || result.Bytes > maxBytes || len(result.Rows) > maxRows || len(result.Columns) == 0 || len(result.Columns) > MaxQueryColumns {
		return fmt.Errorf("SQL result exceeds its declared bounds")
	}
	var measuredBytes int64
	if len(result.Nulls) != 0 && len(result.Nulls) != len(result.Rows) {
		return fmt.Errorf("SQL result null metadata shape is invalid")
	}
	for rowIndex, row := range result.Rows {
		if len(row) != len(result.Columns) {
			return fmt.Errorf("SQL result row shape is invalid")
		}
		if len(result.Nulls) != 0 && len(result.Nulls[rowIndex]) != len(row) {
			return fmt.Errorf("SQL result null metadata shape is invalid")
		}
		for _, value := range row {
			measuredBytes += int64(len(value))
		}
	}
	if measuredBytes != result.Bytes || measuredBytes > maxBytes {
		return fmt.Errorf("SQL result byte accounting is invalid")
	}
	return nil
}

// boundedCostUnits is a conservative deterministic budget proxy for adapters
// whose database driver does not expose a portable planner cost. It is not
// presented as a provider billing amount: each returned row costs one unit
// plus one unit per started KiB of encoded values, with a one-unit query base.
func boundedCostUnits(result QueryResult) int64 {
	calculated := int64(len(result.Rows)) + (result.Bytes+1023)/1024 + 1
	if result.CostUnits > calculated {
		return result.CostUnits
	}
	return calculated
}

func cloneResult(result QueryResult) QueryResult {
	result.Columns = append([]string(nil), result.Columns...)
	result.Rows = append([][]string(nil), result.Rows...)
	for i := range result.Rows {
		result.Rows[i] = append([]string(nil), result.Rows[i]...)
	}
	return result
}

func (b *Binding) allowedTable(schema, table string) bool {
	return contains(b.AllowedSchemas, strings.ToLower(schema)) && contains(b.AllowedTables, strings.ToLower(schema)+"."+strings.ToLower(table))
}
func (c *Connector) allowedTable(schema, table string) bool {
	return c.binding.allowedTable(schema, table)
}
func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func normalizeIdentifiers(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
func min(a, b int) int {
	if a < 1 {
		a = DefaultMaxRows
	}
	if a > b {
		return b
	}
	return a
}
func minBytes(a, b int64) int64 {
	if a <= 0 || a > b {
		return b
	}
	return a
}
func schemaHash(value string) string { return connector.HashPayload([]byte(value)) }
func sqlProfile() contracts.ExecutionProfile {
	profile := contracts.DefaultExecutionProfile()
	profile.MaxInputBytes = MaxPayloadBytes
	profile.MaxOutputBytes = DefaultMaxResultBytes
	profile.MaxRetries = 0
	return profile
}
