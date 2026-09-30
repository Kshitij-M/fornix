// Package testutil contains small environment checks shared by integration-
// style unit tests. Production code must not depend on this package.
package testutil

import (
	"fmt"
	"net"
)

// RequireLocalHTTP skips a test that needs httptest.NewServer when the
// execution environment denies both loopback listener families. The test
// continues to use the real HTTP server whenever either family is available.
func RequireLocalHTTP(t interface {
	Helper()
	Skipf(string, ...any)
}) {
	t.Helper()
	if err := CheckLocalHTTP(); err != nil {
		t.Skipf("local HTTP listener unavailable: %v", err)
	}
}

// CheckLocalHTTP probes the same IPv4-then-IPv6 loopback options used by
// net/http/httptest. It binds only an ephemeral loopback port and immediately
// closes it; it does not access an external network.
func CheckLocalHTTP() error {
	var ipv4Err error
	listener, ipv4Err := net.Listen("tcp", "127.0.0.1:0")
	if ipv4Err == nil {
		return listener.Close()
	}
	listener, ipv6Err := net.Listen("tcp6", "[::1]:0")
	if ipv6Err == nil {
		return listener.Close()
	}
	return fmt.Errorf("IPv4 loopback: %v; IPv6 loopback: %w", ipv4Err, ipv6Err)
}
