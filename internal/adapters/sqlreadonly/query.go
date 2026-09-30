package sqlreadonly

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/omaveda/fornix/internal/connector"
)

const MaxQueryParameters = 128

// Normalize validates and canonicalizes a structured query. It never parses
// SQL because callers cannot supply SQL syntax.
func (q *QueryRequest) Normalize() error {
	if q == nil {
		return fmt.Errorf("SQL query is nil")
	}
	if !identifierPattern.MatchString(strings.ToLower(q.Schema)) || !identifierPattern.MatchString(strings.ToLower(q.Table)) {
		return fmt.Errorf("SQL relation identity is invalid")
	}
	q.Schema, q.Table = strings.ToLower(q.Schema), strings.ToLower(q.Table)
	if q.Limit == 0 {
		q.Limit = DefaultMaxRows
	}
	if q.MaxRows == 0 {
		q.MaxRows = DefaultMaxRows
	}
	if q.Limit < 1 || q.Limit > MaxMaxRows || q.MaxRows < 1 || q.MaxRows > MaxMaxRows || q.Limit > q.MaxRows {
		return fmt.Errorf("SQL row budget is outside bounds")
	}
	if q.MaxBytes < 1 || q.MaxBytes > MaxMaxResultBytes || q.Timeout <= 0 || q.Timeout > MaxTimeout {
		return fmt.Errorf("SQL byte or timeout budget is outside bounds")
	}
	if len(q.Columns) == 0 || len(q.Columns) > MaxQueryColumns {
		return fmt.Errorf("SQL query requires a bounded explicit column list")
	}
	seenColumns := make(map[string]struct{}, len(q.Columns))
	for i, column := range q.Columns {
		column = strings.ToLower(column)
		if !identifierPattern.MatchString(column) {
			return fmt.Errorf("SQL column identifier is invalid")
		}
		if _, exists := seenColumns[column]; exists {
			return fmt.Errorf("SQL query contains duplicate columns")
		}
		seenColumns[column] = struct{}{}
		q.Columns[i] = column
	}
	if len(q.Filters) > MaxFilters || len(q.OrderBy) > MaxOrderTerms {
		return fmt.Errorf("SQL filter or ordering budget is outside bounds")
	}
	parameterCount := 0
	for i := range q.Filters {
		filter := &q.Filters[i]
		filter.Column = strings.ToLower(filter.Column)
		if !identifierPattern.MatchString(filter.Column) {
			return fmt.Errorf("SQL filter column is invalid")
		}
		switch filter.Operator {
		case FilterEqual, FilterNotEqual, FilterLess, FilterLessEqual, FilterGreater, FilterGreaterEqual, FilterLike, FilterILike:
			if len(filter.Value) == 0 || len(filter.Values) != 0 {
				return fmt.Errorf("SQL comparison filter requires exactly one value")
			}
			canonical, err := canonicalScalar(filter.Value)
			if err != nil {
				return err
			}
			if (filter.Operator == FilterLike || filter.Operator == FilterILike) && canonical[0] != '"' {
				return fmt.Errorf("SQL pattern filters require a string value")
			}
			filter.Value, filter.Values = canonical, nil
			parameterCount++
		case FilterIn:
			if len(filter.Value) != 0 || len(filter.Values) == 0 || len(filter.Values) > MaxInValues {
				return fmt.Errorf("SQL IN filter requires a bounded value list")
			}
			values := make([]json.RawMessage, 0, len(filter.Values))
			for _, value := range filter.Values {
				canonical, err := canonicalScalar(value)
				if err != nil {
					return err
				}
				values = append(values, canonical)
			}
			// IN is set-like. Sorting and removing duplicate literals gives
			// semantically equivalent inputs one deterministic parameter order.
			sortRawMessages(values)
			filter.Values = compactRawMessages(values)
			parameterCount += len(filter.Values)
		case FilterIsNull, FilterIsNotNull:
			if len(filter.Value) != 0 || len(filter.Values) != 0 {
				return fmt.Errorf("SQL null filter does not accept values")
			}
		default:
			return fmt.Errorf("SQL filter operator is unsupported")
		}
	}
	if parameterCount+1 > MaxQueryParameters { // Reserve one bind for LIMIT.
		return fmt.Errorf("SQL query parameter budget is outside bounds")
	}
	for i := range q.OrderBy {
		term := &q.OrderBy[i]
		term.Column = strings.ToLower(term.Column)
		if !identifierPattern.MatchString(term.Column) {
			return fmt.Errorf("SQL order column is invalid")
		}
		term.Direction = OrderDirection(strings.ToLower(string(term.Direction)))
		if term.Direction != OrderAscending && term.Direction != OrderDescending {
			return fmt.Errorf("SQL order direction is unsupported")
		}
		for j := 0; j < i; j++ {
			if q.OrderBy[j].Column == term.Column {
				return fmt.Errorf("SQL query contains duplicate order columns")
			}
		}
	}
	// Filter conjunction order does not affect semantics, so canonicalize it.
	sortFilters(q.Filters)
	return nil
}

func (p Payload) queryRequest(binding Binding) QueryRequest {
	maxRows := binding.MaxRows
	if maxRows < 1 {
		maxRows = MaxMaxRows
	}
	maxBytes := binding.MaxResultBytes
	if maxBytes < 1 {
		maxBytes = MaxMaxResultBytes
	}
	timeout := time.Duration(binding.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	limit := p.Limit
	if limit == 0 {
		limit = DefaultMaxRows
	}
	return QueryRequest{
		Schema: p.Schema, Table: p.Table, Columns: append([]string(nil), p.Columns...),
		Filters: cloneFilters(p.Filters), OrderBy: append([]OrderTerm(nil), p.OrderBy...),
		Limit: min(limit, maxRows), MaxRows: min(limit, maxRows),
		MaxBytes: minBytes(p.MaxBytes, maxBytes), Timeout: timeout,
	}
}

// QueryIdentityHash returns a stable hash of query semantics for deterministic
// fixtures and replay. Execution budgets are intentionally not query identity.
func QueryIdentityHash(request QueryRequest) (string, error) {
	if err := request.Normalize(); err != nil {
		return "", err
	}
	identity := struct {
		Schema  string      `json:"schema"`
		Table   string      `json:"table"`
		Columns []string    `json:"columns"`
		Filters []Filter    `json:"filters"`
		OrderBy []OrderTerm `json:"order_by"`
		Limit   int         `json:"limit"`
	}{request.Schema, request.Table, request.Columns, request.Filters, request.OrderBy, request.Limit}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode SQL query identity")
	}
	return connector.HashPayload(encoded), nil
}

func compileQuery(request QueryRequest) (string, []any, error) {
	if err := request.Normalize(); err != nil {
		return "", nil, err
	}
	var sql strings.Builder
	sql.WriteString("SELECT ")
	for i, column := range request.Columns {
		if i > 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(quoteIdentifier(column))
	}
	// PostgreSQL inheritance would otherwise read rows from child relations
	// that are not named by the configured table binding.
	sql.WriteString(" FROM ONLY ")
	sql.WriteString(quoteIdentifier(request.Schema))
	sql.WriteByte('.')
	sql.WriteString(quoteIdentifier(request.Table))
	parameters := make([]any, 0, MaxQueryParameters+1)
	if len(request.Filters) != 0 {
		sql.WriteString(" WHERE ")
		for i, filter := range request.Filters {
			if i > 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(quoteIdentifier(filter.Column))
			switch filter.Operator {
			case FilterIsNull:
				sql.WriteString(" IS NULL")
			case FilterIsNotNull:
				sql.WriteString(" IS NOT NULL")
			case FilterIn:
				sql.WriteString(" IN (")
				for j, raw := range filter.Values {
					if j > 0 {
						sql.WriteByte(',')
					}
					parameter, err := scalarParameter(raw)
					if err != nil {
						return "", nil, err
					}
					parameters = append(parameters, parameter)
					sql.WriteByte('$')
					sql.WriteString(strconv.Itoa(len(parameters)))
				}
				sql.WriteByte(')')
			default:
				operator, ok := sqlOperator(filter.Operator)
				if !ok {
					return "", nil, fmt.Errorf("SQL filter operator is unsupported")
				}
				sql.WriteByte(' ')
				sql.WriteString(operator)
				sql.WriteString(" $")
				parameter, err := scalarParameter(filter.Value)
				if err != nil {
					return "", nil, err
				}
				parameters = append(parameters, parameter)
				sql.WriteString(strconv.Itoa(len(parameters)))
			}
		}
	}
	if len(request.OrderBy) != 0 {
		sql.WriteString(" ORDER BY ")
		for i, term := range request.OrderBy {
			if i > 0 {
				sql.WriteByte(',')
			}
			sql.WriteString(quoteIdentifier(term.Column))
			if term.Direction == OrderDescending {
				sql.WriteString(" DESC")
			} else {
				sql.WriteString(" ASC")
			}
		}
	}
	parameters = append(parameters, request.Limit)
	sql.WriteString(" LIMIT $")
	sql.WriteString(strconv.Itoa(len(parameters)))
	return sql.String(), parameters, nil
}

func sqlOperator(operator FilterOperator) (string, bool) {
	switch operator {
	case FilterEqual:
		return "=", true
	case FilterNotEqual:
		return "<>", true
	case FilterLess:
		return "<", true
	case FilterLessEqual:
		return "<=", true
	case FilterGreater:
		return ">", true
	case FilterGreaterEqual:
		return ">=", true
	case FilterLike:
		return "LIKE", true
	case FilterILike:
		return "ILIKE", true
	default:
		return "", false
	}
}

func canonicalScalar(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > MaxParameterBytes || bytes.ContainsRune(raw, '\x00') {
		return nil, fmt.Errorf("SQL parameter is too large or invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("SQL parameter must be a JSON scalar")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("SQL parameter must contain one JSON scalar")
	}
	switch value.(type) {
	case string, bool, json.Number:
		if text, ok := value.(string); ok && strings.ContainsRune(text, '\x00') {
			return nil, fmt.Errorf("SQL string parameter contains an invalid null byte")
		}
	default:
		return nil, fmt.Errorf("SQL parameter must be a string, number, or boolean")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("SQL parameter is invalid")
	}
	return canonical, nil
}

func scalarParameter(raw json.RawMessage) (any, error) {
	canonical, err := canonicalScalar(raw)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("SQL parameter is invalid")
	}
	switch typed := value.(type) {
	case json.Number:
		if !strings.ContainsAny(string(typed), ".eE") {
			integer, err := typed.Int64()
			if err == nil {
				return integer, nil
			}
		}
		return exactSQLNumber(string(typed))
	case string, bool:
		return typed, nil
	default:
		return nil, fmt.Errorf("SQL parameter must be a string, number, or boolean")
	}
}

// exactSQLNumber adapts a JSON decimal to pgx's numeric/text/integer/float
// codec interfaces without first rounding it through binary64. PostgreSQL
// infers the parameter type from the compared column; implementing the
// corresponding pgtype valuers keeps exact numeric and text comparisons safe.
func exactSQLNumber(raw string) (exactSQLNumeric, error) {
	text := raw
	negative := strings.HasPrefix(text, "-")
	if negative {
		text = text[1:]
	}
	var explicitExponent int64
	if exponentAt := strings.IndexAny(text, "eE"); exponentAt >= 0 {
		exponent, err := strconv.ParseInt(text[exponentAt+1:], 10, 32)
		if err != nil {
			return exactSQLNumeric{}, fmt.Errorf("SQL numeric parameter is outside supported bounds")
		}
		explicitExponent = exponent
		text = text[:exponentAt]
	}
	fractionalDigits := int64(0)
	if point := strings.IndexByte(text, '.'); point >= 0 {
		fractionalDigits = int64(len(text) - point - 1)
		text = text[:point] + text[point+1:]
	}
	digits := strings.TrimLeft(text, "0")
	if digits == "" {
		return exactSQLNumeric{value: pgtype.Numeric{Int: new(big.Int), Valid: true}}, nil
	}
	trailingZeros := len(digits) - len(strings.TrimRight(digits, "0"))
	if trailingZeros != 0 {
		digits = strings.TrimRight(digits, "0")
	}
	exponent := explicitExponent - fractionalDigits + int64(trailingZeros)
	integerDigits := int64(len(digits)) + exponent
	if integerDigits < 0 {
		integerDigits = 0
	}
	if exponent < -16383 || integerDigits > 131072 {
		return exactSQLNumeric{}, fmt.Errorf("SQL numeric parameter is outside PostgreSQL numeric bounds")
	}
	integer, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return exactSQLNumeric{}, fmt.Errorf("SQL numeric parameter is invalid")
	}
	if negative {
		integer.Neg(integer)
	}
	return exactSQLNumeric{value: pgtype.Numeric{Int: integer, Exp: int32(exponent), Valid: true}}, nil
}

type exactSQLNumeric struct{ value pgtype.Numeric }

func (n exactSQLNumeric) NumericValue() (pgtype.Numeric, error) { return n.value, nil }
func (n exactSQLNumeric) Int64Value() (pgtype.Int8, error)      { return n.value.Int64Value() }
func (n exactSQLNumeric) Float64Value() (pgtype.Float8, error)  { return n.value.Float64Value() }
func (n exactSQLNumeric) TextValue() (pgtype.Text, error) {
	encoded, err := n.value.MarshalJSON()
	if err != nil {
		return pgtype.Text{}, err
	}
	return pgtype.Text{String: string(encoded), Valid: true}, nil
}

func referencedColumns(request QueryRequest) []string {
	capacity := len(request.Columns) + len(request.Filters) + len(request.OrderBy)
	seen := make(map[string]struct{}, capacity)
	columns := make([]string, 0, capacity)
	appendColumn := func(column string) {
		if _, ok := seen[column]; !ok {
			seen[column] = struct{}{}
			columns = append(columns, column)
		}
	}
	for _, column := range request.Columns {
		appendColumn(column)
	}
	for _, filter := range request.Filters {
		appendColumn(filter.Column)
	}
	for _, term := range request.OrderBy {
		appendColumn(term.Column)
	}
	return columns
}

func cloneFilters(filters []Filter) []Filter {
	if filters == nil {
		return nil
	}
	cloned := make([]Filter, len(filters))
	for i, filter := range filters {
		cloned[i] = filter
		cloned[i].Value = append(json.RawMessage(nil), filter.Value...)
		cloned[i].Values = make([]json.RawMessage, len(filter.Values))
		for j, value := range filter.Values {
			cloned[i].Values[j] = append(json.RawMessage(nil), value...)
		}
	}
	return cloned
}

func sortRawMessages(values []json.RawMessage) {
	sort.Slice(values, func(i, j int) bool { return bytes.Compare(values[i], values[j]) < 0 })
}

func compactRawMessages(values []json.RawMessage) []json.RawMessage {
	if len(values) < 2 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if !bytes.Equal(result[len(result)-1], value) {
			result = append(result, value)
		}
	}
	return result
}

func sortFilters(filters []Filter) {
	sort.SliceStable(filters, func(i, j int) bool {
		left, _ := json.Marshal(filters[i])
		right, _ := json.Marshal(filters[j])
		return bytes.Compare(left, right) < 0
	})
}

func quoteIdentifier(identifier string) string {
	// Normalize has already restricted identifiers to lowercase ASCII names.
	return `"` + identifier + `"`
}

func bindingHash(binding Binding) string {
	encoded, _ := json.Marshal(binding)
	return connector.HashPayload(encoded)
}

func bindPGDatabase(database Database, binding Binding) (Database, error) {
	switch db := database.(type) {
	case PGDatabase:
		bound, err := db.withBinding(binding)
		return bound, err
	case *PGDatabase:
		if db == nil {
			return nil, fmt.Errorf("SQL database is not configured")
		}
		bound, err := db.withBinding(binding)
		return bound, err
	default:
		return database, nil
	}
}

func (d PGDatabase) withBinding(binding Binding) (PGDatabase, error) {
	if d.Pool == nil {
		return PGDatabase{}, fmt.Errorf("SQL database is not configured")
	}
	if d.binding.ID != "" && bindingHash(d.binding) != bindingHash(binding) {
		return PGDatabase{}, fmt.Errorf("SQL database binding does not match connector binding")
	}
	d.binding = binding
	return d, nil
}
