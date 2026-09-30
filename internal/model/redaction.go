package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

// RequestEvidence returns a bounded structural summary of a model request.
// It intentionally records lengths, roles, and content hashes rather than
// prompts, message bodies, tool schemas, or arbitrary metadata values. This
// is the durable evidence form; providers still receive the original request
// only inside the immediate call boundary.
func RequestEvidence(request contracts.ModelRequest) ([]byte, error) {
	type messageSummary struct {
		Role         string `json:"role"`
		ContentHash  string `json:"content_hash,omitempty"`
		ContentBytes int    `json:"content_bytes"`
		ToolCalls    int    `json:"tool_calls,omitempty"`
	}
	type evidence struct {
		SchemaVersion int              `json:"schema_version"`
		WorkspaceID   string           `json:"workspace_id"`
		RequestID     string           `json:"request_id"`
		Provider      string           `json:"provider"`
		Model         string           `json:"model"`
		MessageCount  int              `json:"message_count"`
		PromptBytes   int              `json:"prompt_bytes"`
		PromptHash    string           `json:"prompt_hash,omitempty"`
		Messages      []messageSummary `json:"messages,omitempty"`
		ToolCount     int              `json:"tool_count"`
		MetadataKeys  []string         `json:"metadata_keys,omitempty"`
	}
	result := evidence{SchemaVersion: request.SchemaVersion, WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, Provider: request.Provider.Provider, Model: request.Provider.Model, MessageCount: len(request.Messages), PromptBytes: len([]byte(request.Prompt)), ToolCount: len(request.Tools)}
	if request.Prompt != "" {
		digest := sha256.Sum256([]byte(request.Prompt))
		result.PromptHash = hex.EncodeToString(digest[:])
	}
	for _, message := range request.Messages {
		summary := messageSummary{Role: message.Role, ContentBytes: len([]byte(message.Content)), ToolCalls: len(message.ToolCalls)}
		if message.Content != "" {
			digest := sha256.Sum256([]byte(message.Content))
			summary.ContentHash = hex.EncodeToString(digest[:])
		}
		result.Messages = append(result.Messages, summary)
	}
	for key := range request.Metadata {
		result.MetadataKeys = append(result.MetadataKeys, key)
	}
	sort.Strings(result.MetadataKeys)
	return RedactJSON(result)
}

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
