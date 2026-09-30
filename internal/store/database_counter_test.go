package store

import (
	"errors"
	"math"
	"testing"
)

func TestDatabaseCounterRejectsUnsignedOverflow(t *testing.T) {
	if got, err := databaseCounter(uint64(math.MaxInt64)); err != nil || got != math.MaxInt64 {
		t.Fatalf("max representable counter = %d, error=%v", got, err)
	}
	if _, err := databaseCounter(uint64(math.MaxInt64) + 1); !errors.Is(err, errDatabaseCounterOverflow) {
		t.Fatalf("overflow error = %v", err)
	}
}
