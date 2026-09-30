package sqlreadonly

import "testing"

func TestResultDigestDistinguishesSQLNullFromLiteralNilText(t *testing.T) {
	nullResult := QueryResult{
		Columns: []string{"value"},
		Rows:    [][]string{{""}},
		Nulls:   [][]bool{{true}},
		Bytes:   0,
	}
	textResult := QueryResult{
		Columns: []string{"value"},
		Rows:    [][]string{{"<nil>"}},
		Nulls:   [][]bool{{false}},
		Bytes:   int64(len("<nil>")),
	}
	nullHash := resultSummary(nullResult, "input")["result_hash"]
	textHash := resultSummary(textResult, "input")["result_hash"]
	if nullHash == textHash {
		t.Fatalf("distinct SQL results have identical result digests: %v", nullHash)
	}
}
