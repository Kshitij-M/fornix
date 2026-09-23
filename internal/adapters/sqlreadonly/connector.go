// Package sqlreadonly exposes only bounded, read-only SQL capabilities. It
// rejects unsafe statements before they reach an external database and never
// makes the external database the authority for Fornix operation state.
package sqlreadonly

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

const (
	ConnectorName          = "sqlreadonly"
	ConnectorVersion       = "1"
	DescribeCapabilityName = "describe"
	QueryCapabilityName    = "query_readonly"
	ExplainCapabilityName  = "explain_readonly"
	ResourceKind           = "database_table"
	InputType              = "sql.request"
	DefaultMaxRows         = 1000
	MaxMaxRows             = 10000
	DefaultMaxResultBytes  = 1 << 20
	MaxMaxResultBytes      = 16 << 20
	DefaultMaxCostUnits    = 100000
	MaxMaxCostUnits        = 1000000
	DefaultTimeout         = 5 * time.Second
	MaxTimeout             = 10 * time.Minute
	MaxStatementBytes      = 64 << 10
	MaxParameters          = 32
	MaxParameterBytes      = 4096
)

var (
	identifierPattern     = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	unsafeKeywordPattern  = regexp.MustCompile(`(?i)\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|call|do|begin|commit|rollback|savepoint|prepare|execute|merge|refresh|vacuum|analyze|listen|notify|lock)\b`)
	tableReferencePattern = regexp.MustCompile(`(?i)\b(from|join)\s+([a-z_][a-z0-9_.]*)`)
	tableKeywordPattern   = regexp.MustCompile(`(?i)\b(from|join)\b`)
	parameterPattern      = regexp.MustCompile(`\$([0-9]+)`)
	ErrUnsafeStatement    = errors.New("SQL statement is not permitted by read-only connector")
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

// Payload is the typed SQL envelope resolved by OperationRequest.InputHash.
// Parameter values are ephemeral and never copied into Fornix authority.
type Payload struct {
	SchemaVersion int      `json:"schema_version"`
	Statement     string   `json:"statement,omitempty"`
	Parameters    []string `json:"parameters,omitempty"`
	Schema        string   `json:"schema,omitempty"`
	Table         string   `json:"table,omitempty"`
	Limit         int      `json:"limit,omitempty"`
	MaxBytes      int64    `json:"max_bytes,omitempty"`
}

// QueryRequest is the already validated request passed to a database driver.
type QueryRequest struct {
	Statement  string
	Parameters []string
	MaxRows    int
	MaxBytes   int64
	Timeout    time.Duration
}

// QueryResult is an ephemeral bounded result. Connector results persist only
// its hashes and counts in OperationResult; Rows never cross that boundary.
type QueryResult struct {
	Columns   []string
	Rows      [][]string
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

// PGDatabase adapts a pgx pool to the read-only driver seam.
type PGDatabase struct{ Pool *pgxpool.Pool }

func (d PGDatabase) QueryReadOnly(ctx context.Context, request QueryRequest) (QueryResult, error) {
	return d.query(ctx, request, request.Statement)
}

func (d PGDatabase) ExplainReadOnly(ctx context.Context, request QueryRequest) (QueryResult, error) {
	return d.query(ctx, request, "EXPLAIN (FORMAT JSON) "+request.Statement)
}

func (d PGDatabase) Describe(ctx context.Context, schema, table string, maxRows int, maxBytes int64, timeout time.Duration) (QueryResult, error) {
	return d.query(ctx, QueryRequest{Parameters: []string{schema, table}, MaxRows: maxRows, MaxBytes: maxBytes, Timeout: timeout}, `SELECT column_name,data_type FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position`)
}

func (d PGDatabase) query(ctx context.Context, request QueryRequest, statement string) (QueryResult, error) {
	if d.Pool == nil {
		return QueryResult{}, fmt.Errorf("SQL database is not configured")
	}
	tx, err := d.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return QueryResult{}, fmt.Errorf("SQL read transaction unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('statement_timeout',$1,true)`, fmt.Sprintf("%dms", request.Timeout.Milliseconds())); err != nil {
		return QueryResult{}, fmt.Errorf("SQL timeout configuration failed")
	}
	if _, err := tx.Prepare(ctx, "fornix_readonly", statement); err != nil {
		return QueryResult{}, fmt.Errorf("SQL statement preparation failed")
	}
	values := make([]any, len(request.Parameters))
	for i, value := range request.Parameters {
		values[i] = value
	}
	rows, err := tx.Query(ctx, "fornix_readonly", values...)
	if err != nil {
		return QueryResult{}, fmt.Errorf("SQL read failed")
	}
	result, err := collectRows(rows, request.MaxRows, request.MaxBytes)
	if err != nil {
		return QueryResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return QueryResult{}, fmt.Errorf("SQL read commit failed")
	}
	return result, nil
}

func collectRows(rows pgx.Rows, maxRows int, maxBytes int64) (QueryResult, error) {
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
			return QueryResult{}, fmt.Errorf("SQL result decode failed")
		}
		row := make([]string, len(values))
		rowBytes := int64(0)
		for index, value := range values {
			row[index] = fmt.Sprint(value)
			rowBytes += int64(len(row[index]))
		}
		if result.Bytes+rowBytes > maxBytes {
			result.Truncated = true
			break
		}
		result.Rows = append(result.Rows, row)
		result.Bytes += rowBytes
		result.CostUnits += 1 + (rowBytes+1023)/1024
	}
	if err := rows.Err(); err != nil {
		return QueryResult{}, fmt.Errorf("SQL result iteration failed")
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
	result, ok := f.QueryResults[connector.HashPayload([]byte(request.Statement))]
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
	result, ok := f.ExplainResults[connector.HashPayload([]byte(request.Statement))]
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
		{name: DescribeCapabilityName, input: "sql.describe.input.v1", output: "sql.describe.output.v1", effect: contracts.EffectClassObservation},
		{name: QueryCapabilityName, input: "sql.query.input.v1", output: "sql.query.output.v1", effect: contracts.EffectClassReadOnly},
		{name: ExplainCapabilityName, input: "sql.explain.input.v1", output: "sql.explain.output.v1", effect: contracts.EffectClassObservation},
	} {
		definition := contracts.CapabilityDefinition{WorkspaceID: binding.WorkspaceID, Ref: contracts.CapabilityRef{WorkspaceID: binding.WorkspaceID, Connector: ref, Name: spec.name, Version: "1"}, Description: "bounded read-only SQL capability", InputSchemaVersion: 1, InputSchemaHash: schemaHash(spec.input), OutputSchemaVersion: 1, OutputSchemaHash: schemaHash(spec.output), Effect: spec.effect, Profile: sqlProfile(), Evidence: []contracts.EvidenceRequirement{{WorkspaceID: binding.WorkspaceID, Kind: "sql_result", MinItems: 1, MaxItems: 1, RequireHash: true, RequireProvenance: true}}, ResourceKinds: []string{ResourceKind}, RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"}, MaxRows: binding.MaxRows, RateLimitPerMinute: 60, RequiredCredentialRefs: credentialRefs, SupportsCancellation: true, SupportsIdempotency: true, SupportsVerification: true, Enabled: true}
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
	if err != nil || !connector.VerifyPayload(payloadBytes, request.InputHash) || len(payloadBytes) > MaxMaxResultBytes {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: err}
	}
	var payload Payload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: err}
	}
	if err := payload.Normalize(c.name); err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: err}
	}
	if c.name == DescribeCapabilityName {
		if !c.parent.allowedTable(payload.Schema, payload.Table) {
			return contracts.OperationResult{}, &connector.FailureError{Code: "unauthorized", Retryable: false}
		}
	}
	var result QueryResult
	switch c.name {
	case DescribeCapabilityName:
		result, err = c.parent.database.Describe(ctx, payload.Schema, payload.Table, min(payload.Limit, c.parent.binding.MaxRows), minBytes(payload.MaxBytes, c.parent.binding.MaxResultBytes), time.Duration(c.parent.binding.TimeoutMS)*time.Millisecond)
	case QueryCapabilityName:
		if err = validateStatement(payload.Statement, c.parent.binding, payload.Parameters); err == nil {
			result, err = c.parent.database.QueryReadOnly(ctx, QueryRequest{Statement: payload.Statement, Parameters: payload.Parameters, MaxRows: min(payload.Limit, c.parent.binding.MaxRows), MaxBytes: minBytes(payload.MaxBytes, c.parent.binding.MaxResultBytes), Timeout: time.Duration(c.parent.binding.TimeoutMS) * time.Millisecond})
		}
	case ExplainCapabilityName:
		if err = validateStatement(payload.Statement, c.parent.binding, payload.Parameters); err == nil {
			result, err = c.parent.database.ExplainReadOnly(ctx, QueryRequest{Statement: payload.Statement, Parameters: payload.Parameters, MaxRows: min(payload.Limit, c.parent.binding.MaxRows), MaxBytes: minBytes(payload.MaxBytes, c.parent.binding.MaxResultBytes), Timeout: time.Duration(c.parent.binding.TimeoutMS) * time.Millisecond})
		}
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return contracts.OperationResult{}, &connector.FailureError{Code: "timeout", Retryable: false, Err: err}
		}
		return contracts.OperationResult{}, &connector.FailureError{Code: "adapter", Retryable: false, Err: err}
	}
	if result.Bytes > c.parent.binding.MaxResultBytes {
		return contracts.OperationResult{}, &connector.FailureError{Code: "budget", Retryable: false}
	}
	if len(result.Rows) > c.parent.binding.MaxRows {
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
	if p.SchemaVersion == 0 {
		p.SchemaVersion = 1
	}
	if p.SchemaVersion != 1 || len(p.Parameters) > MaxParameters {
		return fmt.Errorf("SQL payload schema or parameter budget is invalid")
	}
	for _, parameter := range p.Parameters {
		if len(parameter) > MaxParameterBytes || strings.ContainsRune(parameter, '\x00') {
			return fmt.Errorf("SQL parameter is too large or invalid")
		}
	}
	if p.Limit == 0 {
		p.Limit = DefaultMaxRows
	}
	if p.Limit < 1 || p.Limit > MaxMaxRows || p.MaxBytes < 0 || p.MaxBytes > MaxMaxResultBytes {
		return fmt.Errorf("SQL result budget is invalid")
	}
	if kind == DescribeCapabilityName {
		if !identifierPattern.MatchString(strings.ToLower(p.Schema)) || !identifierPattern.MatchString(strings.ToLower(p.Table)) {
			return fmt.Errorf("SQL describe identity is invalid")
		}
		p.Schema, p.Table = strings.ToLower(p.Schema), strings.ToLower(p.Table)
		return nil
	}
	if err := validateStatementShape(p.Statement); err != nil {
		return err
	}
	return nil
}

func validateStatement(statement string, binding Binding, parameters []string) error {
	if err := validateStatementShape(statement); err != nil {
		return err
	}
	references := tableReferencePattern.FindAllStringSubmatch(strings.ToLower(statement), -1)
	for _, reference := range references {
		parts := strings.Split(reference[2], ".")
		if len(parts) != 2 || !contains(binding.AllowedSchemas, parts[0]) || !contains(binding.AllowedTables, reference[2]) {
			return fmt.Errorf("SQL table is not allowlisted")
		}
	}
	if len(tableKeywordPattern.FindAllString(statement, -1)) != len(references) {
		return fmt.Errorf("SQL table references must be explicit schema.table identities")
	}
	for _, match := range parameterPattern.FindAllStringSubmatch(statement, -1) {
		index, err := strconv.Atoi(match[1])
		if err != nil || index == 0 || index > len(parameters) {
			return fmt.Errorf("SQL parameter placeholder is not bound")
		}
	}
	return nil
}

func validateStatementShape(statement string) error {
	trimmed := strings.TrimSpace(statement)
	if trimmed == "" || len(trimmed) > MaxStatementBytes || strings.ContainsRune(trimmed, '\x00') || strings.Contains(trimmed, ";") || strings.Contains(trimmed, "--") || strings.Contains(trimmed, "/*") || strings.Contains(trimmed, "*/") {
		return ErrUnsafeStatement
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 || (strings.ToLower(fields[0]) != "select" && strings.ToLower(fields[0]) != "with") {
		return ErrUnsafeStatement
	}
	if unsafeKeywordPattern.MatchString(trimmed) {
		return ErrUnsafeStatement
	}
	return nil
}

func resultSummary(result QueryResult, inputHash string) map[string]any {
	canonical, _ := json.Marshal(result)
	columns := append([]string(nil), result.Columns...)
	return map[string]any{"input_hash": inputHash, "columns": columns, "columns_hash": connector.HashPayload([]byte(strings.Join(columns, "\x00"))), "rows": len(result.Rows), "bytes": result.Bytes, "cost_units": boundedCostUnits(result), "truncated": result.Truncated, "result_hash": connector.HashPayload(canonical)}
}

// boundedCostUnits is a conservative deterministic budget proxy for adapters
// whose database driver does not expose a portable planner cost. It is not
// presented as a provider billing amount: each returned row costs one unit
// plus one unit per started KiB of encoded values, with a one-unit query base.
func boundedCostUnits(result QueryResult) int64 {
	if result.CostUnits > 0 {
		return result.CostUnits
	}
	return int64(len(result.Rows)) + (result.Bytes+1023)/1024 + 1
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
	profile.MaxInputBytes = MaxStatementBytes
	profile.MaxOutputBytes = DefaultMaxResultBytes
	profile.MaxRetries = 0
	return profile
}
