package sqlreadonly

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestCompileQueryIsDeterministicAndBindsValues(t *testing.T) {
	request := QueryRequest{
		Schema: "PUBLIC", Table: "Items", Columns: []string{"ID", "name"},
		Filters: []Filter{
			{Column: "name", Operator: FilterEqual, Value: json.RawMessage(`"x' OR true --"`)},
			{Column: "id", Operator: FilterIn, Values: []json.RawMessage{json.RawMessage(`2`), json.RawMessage(`1`), json.RawMessage(`1`)}},
		},
		OrderBy: []OrderTerm{{Column: "name", Direction: OrderDescending}},
		Limit:   10, MaxRows: 20, MaxBytes: 4096, Timeout: time.Second,
	}
	statement, parameters, err := compileQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	wantStatement := `SELECT "id","name" FROM ONLY "public"."items" WHERE "id" IN ($1,$2) AND "name" = $3 ORDER BY "name" DESC LIMIT $4`
	if statement != wantStatement {
		t.Fatalf("compiled SQL\n got: %s\nwant: %s", statement, wantStatement)
	}
	wantParameters := []any{int64(1), int64(2), "x' OR true --", 10}
	if !reflect.DeepEqual(parameters, wantParameters) {
		t.Fatalf("parameters=%#v, want %#v", parameters, wantParameters)
	}
	if strings.Contains(statement, "x' OR true") || strings.Contains(statement, "--") {
		t.Fatalf("filter value was interpolated into SQL: %s", statement)
	}
	secondStatement, secondParameters, err := compileQuery(request)
	if err != nil || secondStatement != statement || !reflect.DeepEqual(secondParameters, parameters) {
		t.Fatalf("compiler output changed: %s %#v err=%v", secondStatement, secondParameters, err)
	}
}

func TestQueryNormalizationRejectsSyntaxAndUnboundedInputs(t *testing.T) {
	tests := map[string]QueryRequest{
		"expression-column": {Schema: "public", Table: "items", Columns: []string{"coalesce(id,0)"}, Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second},
		"unknown-operator":  {Schema: "public", Table: "items", Columns: []string{"id"}, Filters: []Filter{{Column: "id", Operator: "= 1 OR true", Value: json.RawMessage(`1`)}}, Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second},
		"object-value":      {Schema: "public", Table: "items", Columns: []string{"id"}, Filters: []Filter{{Column: "id", Operator: FilterEqual, Value: json.RawMessage(`{"x":1}`)}}, Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second},
		"like-number":       {Schema: "public", Table: "items", Columns: []string{"id"}, Filters: []Filter{{Column: "id", Operator: FilterLike, Value: json.RawMessage(`1`)}}, Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second},
		"empty-columns":     {Schema: "public", Table: "items", Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second},
		"too-many-values":   {Schema: "public", Table: "items", Columns: []string{"id"}, Limit: 1, MaxRows: 1, MaxBytes: 4096, Timeout: time.Second, Filters: []Filter{{Column: "id", Operator: FilterIn, Values: makeINValues(MaxQueryParameters)}}},
	}
	for name, request := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := compileQuery(request); err == nil {
				t.Fatal("invalid structured query compiled")
			}
		})
	}
}

func TestQueryIdentityCanonicalizesCommutativePredicates(t *testing.T) {
	left := QueryRequest{Schema: "public", Table: "items", Columns: []string{"id"}, Filters: []Filter{
		{Column: "id", Operator: FilterGreater, Value: json.RawMessage(`1`)},
		{Column: "id", Operator: FilterLess, Value: json.RawMessage(`9`)},
	}, Limit: 10, MaxRows: 10, MaxBytes: 4096, Timeout: time.Second}
	right := left
	right.Filters = []Filter{left.Filters[1], left.Filters[0]}
	leftHash, err := QueryIdentityHash(left)
	if err != nil {
		t.Fatal(err)
	}
	rightHash, err := QueryIdentityHash(right)
	if err != nil {
		t.Fatal(err)
	}
	if leftHash != rightHash {
		t.Fatalf("commutative predicates got different identities: %s != %s", leftHash, rightHash)
	}
}

func TestPayloadQueryRequestPreservesEmptyFilterIdentity(t *testing.T) {
	payload := Payload{SchemaVersion: 2, Schema: "public", Table: "items", Columns: []string{"id"}}
	if err := payload.Normalize(QueryCapabilityName); err != nil {
		t.Fatal(err)
	}
	binding := Binding{MaxRows: DefaultMaxRows, MaxResultBytes: DefaultMaxResultBytes, TimeoutMS: DefaultTimeout.Milliseconds()}
	got, err := QueryIdentityHash(payload.queryRequest(binding))
	if err != nil {
		t.Fatal(err)
	}
	want, err := QueryIdentityHash(QueryRequest{
		Schema: "public", Table: "items", Columns: []string{"id"},
		Limit: DefaultMaxRows, MaxRows: DefaultMaxRows,
		MaxBytes: DefaultMaxResultBytes, Timeout: DefaultTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("payload query identity=%s, want %s", got, want)
	}
}

func TestScalarParameterPreservesExactDecimalAndLargeIntegerValues(t *testing.T) {
	for _, input := range []struct {
		literal string
		want    string
	}{
		{literal: `9007199254740993.1234567890123456789`, want: `9007199254740993.1234567890123456789`},
		{literal: `1.25e2`, want: `125`},
		{literal: `9223372036854775808`, want: `9223372036854775808`},
	} {
		value, err := scalarParameter(json.RawMessage(input.literal))
		if err != nil {
			t.Fatalf("scalarParameter(%s): %v", input.literal, err)
		}
		numeric, ok := value.(exactSQLNumeric)
		if !ok {
			t.Fatalf("scalarParameter(%s) returned %T, want exactSQLNumeric", input.literal, value)
		}
		text, err := numeric.TextValue()
		if err != nil || text.String != input.want {
			t.Fatalf("scalarParameter(%s) text=%q err=%v, want %q", input.literal, text.String, err, input.want)
		}
	}
}

func TestScalarParameterRejectsPostgresNumericOverflow(t *testing.T) {
	if _, err := scalarParameter(json.RawMessage(`1e200000`)); err == nil {
		t.Fatal("numeric value outside PostgreSQL bounds was accepted")
	}
}

func TestBoundedDatabaseErrorClassifiesTimeoutAndCancellation(t *testing.T) {
	if err := boundedDatabaseError(context.Background(), "hidden database detail", &pgconn.PgError{Code: "57014"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("statement timeout was not classified: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := boundedDatabaseError(ctx, "hidden database detail", errors.New("driver error")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was not classified: %v", err)
	}
	if err := boundedDatabaseError(context.Background(), "SQL read failed", errors.New("secret connection details")); err == nil || err.Error() != "SQL read failed" {
		t.Fatalf("database error was not redacted: %v", err)
	}
}

func makeINValues(count int) []json.RawMessage {
	values := make([]json.RawMessage, count)
	for i := range values {
		values[i] = json.RawMessage(`1`)
	}
	return values
}
