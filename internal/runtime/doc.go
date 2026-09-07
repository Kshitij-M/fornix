// Package runtime owns the deterministic local-container lifecycle used by the
// Fornix CLI. It renders a versioned, embedded Compose manifest and executes
// Docker through structured arguments with bounded time and output.
//
// The package does not install Docker, persist provider credentials, bootstrap
// application identities, or become a control-plane authority. Callers remain
// responsible for supplying required secrets through the process environment
// and for treating PostgreSQL as the authoritative Fornix state store.
package runtime
