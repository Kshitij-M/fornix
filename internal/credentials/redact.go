package credentials

import (
	"regexp"
	"strings"
)

// Redacted is the stable marker used by all credential formatting helpers.
const Redacted = "[REDACTED]"

var bearerPattern = regexp.MustCompile(`(?i)\bbearer\s+[a-z0-9._~+/=-]+`)

// IsSensitiveKey classifies bounded configuration field names. It is
// deliberately conservative so diagnostics redact unfamiliar provider secret
// fields instead of attempting to enumerate every vendor's naming scheme.
func IsSensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	normalized = strings.NewReplacer("-", "", "_", "", ".", "", " ", "").Replace(normalized)
	for _, marker := range []string{"apikey", "accesstoken", "refreshtoken", "authorization", "credential", "password", "secret", "privatekey"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

// RedactValue returns an empty string for absent values and a stable marker
// for every configured value. It never emits a partial secret or length hint.
func RedactValue(value string) string {
	if value == "" {
		return ""
	}
	return Redacted
}

// RedactText masks bearer credentials and any explicitly supplied Secret.
// The replacement is deterministic and does not mutate the supplied secrets.
func RedactText(value string, secrets ...Secret) string {
	value = bearerPattern.ReplaceAllString(value, "Bearer "+Redacted)
	for _, secret := range secrets {
		if len(secret.value) != 0 {
			value = strings.ReplaceAll(value, string(secret.value), Redacted)
		}
	}
	return value
}

// RedactMap copies values while replacing fields classified by
// IsSensitiveKey. The input map is never mutated.
func RedactMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	redacted := make(map[string]string, len(values))
	for key, value := range values {
		if IsSensitiveKey(key) {
			redacted[key] = RedactValue(value)
		} else {
			redacted[key] = value
		}
	}
	return redacted
}
