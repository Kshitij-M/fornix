// Package profile owns the private, host-local metadata used by the Fornix
// CLI. A profile is convenience state, never control-plane authority: callers
// must continue to treat PostgreSQL as authoritative for workspace and run
// state.
//
// The package requires callers to supply an explicit absolute root. The caller
// may obtain that value from FORNIX_HOME, a command-line flag, or its own test
// configuration; this package deliberately does not read ambient environment
// variables. Profile directories and files are restricted to owner access,
// writes are atomic, and lifecycle mutations can share a process lock.
package profile
