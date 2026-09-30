package store

import (
	"errors"
	"math"
)

var errDatabaseCounterOverflow = errors.New("counter exceeds PostgreSQL bigint range")

// databaseCounter converts an unsigned domain fence/counter to PostgreSQL's
// signed BIGINT only after proving the value is representable. Failing closed
// here avoids wraparound turning a large stale token into a valid negative
// database value.
func databaseCounter(value uint64) (int64, error) {
	if value > uint64(math.MaxInt64) {
		return 0, errDatabaseCounterOverflow
	}
	return int64(value), nil
}
