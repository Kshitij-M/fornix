package connector

import "testing"

func TestConformanceReportSanitizesUntrustedCaseAndErrorTokens(t *testing.T) {
	if got := boundedCaseName("case\nwith-secret"); got != "unknown_case" {
		t.Fatalf("unsafe case token was not sanitized: %q", got)
	}
	if got := boundedErrorCode("bad error secret"); got != "qualification_failed" {
		t.Fatalf("unsafe error token was not sanitized: %q", got)
	}
}
