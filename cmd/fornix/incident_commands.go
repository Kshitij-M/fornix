package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// incidentCommand exposes the reference workflow without making incident
// handling repository-specific. The fake delivery path is offline and
// deterministic; any remediation remains approval-gated and auditable.
func (c *operatorCLI) incidentCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("incident requires start, get, approve, or replay")
	}
	switch args[0] {
	case "start":
		payloadText := valueArg(args[1:], "payload", `{"service":"example","status":"degraded"}`)
		var payload any
		if err := json.Unmarshal([]byte(payloadText), &payload); err != nil {
			return fmt.Errorf("--payload must be valid JSON: %w", err)
		}
		externalID := valueArg(args[1:], "external-id", "incident-"+sha256String(payloadText))
		idempotency := valueArg(args[1:], "idempotency", "incident:"+c.workspace+":"+externalID)
		delivery := valueArg(args[1:], "delivery", "fake")
		event := map[string]any{
			"workspace_id":    c.workspace,
			"source_system":   valueArg(args[1:], "source", "fornix-demo-monitor"),
			"external_id":     externalID,
			"severity":        valueArg(args[1:], "severity", "warning"),
			"summary":         valueArg(args[1:], "summary", "bounded incident qualification workflow"),
			"payload":         payload,
			"delivery_mode":   delivery,
			"idempotency_key": idempotency,
		}
		if delivery == "signed" {
			event["signature_hash"] = valueArg(args[1:], "signature-hash", strings.Repeat("0", 64))
			event["signature_scheme"] = valueArg(args[1:], "signature-scheme", "preverified")
		}
		body := map[string]any{"workspace_id": c.workspace, "idempotency_key": idempotency, "event": event}
		return c.requestPrint(http.MethodPost, "/v1/incident/workflows", body, false)
	case "get":
		runID := incidentRunID(args[1:])
		if runID == "" {
			return errors.New("incident get requires --id")
		}
		return c.requestPrint(http.MethodGet, "/v1/incident/workflows/"+url.PathEscape(runID)+"?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	case "approve":
		runID := incidentRunID(args[1:])
		if runID == "" {
			return errors.New("incident approve requires --id")
		}
		decision := valueArg(args[1:], "decision", "approve")
		body := map[string]any{"workspace_id": c.workspace, "decision": decision, "idempotency_key": valueArg(args[1:], "idempotency", "incident-approval:"+runID+":"+decision)}
		return c.requestPrint(http.MethodPost, "/v1/incident/workflows/"+url.PathEscape(runID)+"/approve", body, false)
	case "replay":
		runID := incidentRunID(args[1:])
		if runID == "" {
			return errors.New("incident replay requires --id")
		}
		path := "/v1/incident/workflows/" + url.PathEscape(runID) + "/replay?workspace_id=" + url.QueryEscape(c.workspace)
		return c.requestPrint(http.MethodPost, path, map[string]any{}, false)
	default:
		return fmt.Errorf("unknown incident command %q", args[0])
	}
}

func incidentRunID(args []string) string {
	// --id matches the compact operator commands; --run-id keeps the
	// incident surface self-describing when copied into automation.
	return valueArg(args, "id", valueArg(args, "run-id", ""))
}
