package model

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

var bearerPattern = regexp.MustCompile(`(?i)bearer\s+[a-z0-9._~+/=-]+`)
var secretKeyPattern = regexp.MustCompile(`(?i)(api[_-]?key|authorization|access[_-]?token|refresh[_-]?token|secret|password|credential)`)

// RedactJSON canonicalizes JSON and replaces secret-looking object fields.
// It is deliberately conservative: unknown strings are retained, while
// bearer-shaped values are masked even when they occur in free text.
func RedactJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return RedactBytes(encoded), nil
}

// RedactBytes masks credential-shaped values and secret-looking JSON fields,
// then applies the bounded evidence limit used by model/tool persistence.
// Callers must invoke it before logging, event append, or evidence storage.
func RedactBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(value, &decoded); err == nil {
		decoded = redactValue(decoded, 0)
		if encoded, marshalErr := json.Marshal(decoded); marshalErr == nil {
			return limitEvidence(encoded)
		}
	}
	return limitEvidence(bearerPattern.ReplaceAll(value, []byte("Bearer [REDACTED]")))
}

// RedactUnboundedBytes applies the same credential-shaped and secret-field
// redaction policy without the model-evidence size ceiling. It is used only
// before an oversized output is placed in the bounded, content-addressed
// artifact plane; callers still enforce the artifact maximum separately.
func RedactUnboundedBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(value, &decoded); err == nil {
		decoded = redactValue(decoded, 0)
		if encoded, marshalErr := json.Marshal(decoded); marshalErr == nil {
			return encoded
		}
	}
	return bearerPattern.ReplaceAll(value, []byte("Bearer [REDACTED]"))
}

func redactCredential(value []byte, credential string) []byte {
	credential = strings.TrimSpace(credential)
	if credential == "" || len(value) == 0 {
		return RedactBytes(value)
	}
	return RedactBytes(bytes.ReplaceAll(value, []byte(credential), []byte("[REDACTED]")))
}

func redactValue(value any, depth int) any {
	if depth > 16 {
		return "[REDACTED_DEPTH]"
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if secretKeyPattern.MatchString(key) {
				typed[key] = "[REDACTED]"
				continue
			}
			typed[key] = redactValue(child, depth+1)
		}
		return typed
	case []any:
		for i := range typed {
			typed[i] = redactValue(typed[i], depth+1)
		}
		return typed
	case string:
		return bearerPattern.ReplaceAllString(typed, "Bearer [REDACTED]")
	default:
		return value
	}
}

func limitEvidence(value []byte) []byte {
	if len(value) <= contracts.MaxModelEvidenceBytes {
		return append([]byte(nil), value...)
	}
	// Keep a valid JSON string rather than returning a malformed partial body.
	return bytes.Clone([]byte(`{"redacted":true,"truncated":true}`))
}

func redactText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "model provider failure"
	}
	return bearerPattern.ReplaceAllString(value, "Bearer [REDACTED]")
}

func redactCredentialText(value, credential string) string {
	value = redactText(value)
	credential = strings.TrimSpace(credential)
	if credential == "" {
		return value
	}
	return strings.ReplaceAll(value, credential, "[REDACTED]")
}
