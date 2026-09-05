package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type operatorCLI struct {
	baseURL      string
	key          string
	bootstrapKey string
	workspace    string
	jsonOutput   bool
	client       *http.Client
}

func runCLI(args []string) error {
	if len(args) > 0 && args[0] == "serve" {
		return errors.New("serve is handled by the main server entrypoint")
	}
	fs := flag.NewFlagSet("fornix", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	urlFlag := fs.String("url", envOr("FORNIX_URL", "http://localhost:8201"), "Fornix HTTP URL")
	keyFlag := fs.String("key", os.Getenv("FORNIX_KEY"), "workspace API key")
	bootstrapFlag := fs.String("bootstrap-key", os.Getenv("FORNIX_BOOTSTRAP_KEY"), "one-time bootstrap key")
	workspaceFlag := fs.String("workspace", envOr("FORNIX_WORKSPACE_ID", "default"), "workspace scope")
	jsonFlag := fs.Bool("json", true, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cli := &operatorCLI{baseURL: strings.TrimRight(*urlFlag, "/"), key: *keyFlag, bootstrapKey: *bootstrapFlag, workspace: *workspaceFlag, jsonOutput: *jsonFlag, client: &http.Client{Timeout: 90 * time.Second}}
	parts := fs.Args()
	if len(parts) == 0 {
		return cli.usage()
	}
	switch parts[0] {
	case "health":
		return cli.requestPrint(http.MethodGet, "/v1/health", nil, false)
	case "readiness":
		return cli.requestPrint(http.MethodGet, "/readyz", nil, false)
	case "workspace":
		return cli.workspaceCommand(parts[1:])
	case "identity":
		return cli.identityCommand(parts[1:])
	case "role":
		return cli.roleCommand(parts[1:])
	case "api-key":
		return cli.apiKeyCommand(parts[1:])
	case "ingest":
		return cli.ingestCommand(parts[1:])
	case "task":
		return cli.taskCommand(parts[1:])
	case "run":
		return cli.runCommand(parts[1:])
	case "retrieve":
		return cli.retrieveCommand(parts[1:])
	case "evaluation":
		return cli.evaluationCommand(parts[1:])
	case "metrics":
		return cli.requestPrint(http.MethodGet, "/v1/metrics?workspace_id="+url.QueryEscape(cli.workspace), nil, false)
	case "artifact":
		return cli.artifactCommand(parts[1:])
	case "evidence":
		return cli.evidenceCommand(parts[1:])
	case "receipt":
		return cli.receiptCommand(parts[1:])
	case "change":
		return cli.changeCommand(parts[1:])
	case "validation":
		return cli.validationCommand(parts[1:])
	case "policy":
		return cli.policyCommand(parts[1:])
	case "reference-workflow":
		return cli.referenceWorkflow(parts[1:])
	default:
		return fmt.Errorf("unknown command %q; run fornix --help", parts[0])
	}
}

func (c *operatorCLI) usage() error {
	return errors.New("usage: fornix [--url URL] [--key KEY] [--workspace ID] <health|workspace|identity|role|api-key|ingest|task|run|retrieve|evaluation|metrics|artifact|evidence|receipt|change|validation|policy|reference-workflow>")
}

func (c *operatorCLI) workspaceCommand(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		return c.requestPrint(http.MethodGet, "/v1/operator/workspaces?limit=100", nil, false)
	}
	switch args[0] {
	case "get":
		id := valueArg(args[1:], "id", c.workspace)
		return c.requestPrint(http.MethodGet, "/v1/operator/workspaces/"+url.PathEscape(id), nil, false)
	case "bootstrap":
		workspace := valueArg(args[1:], "workspace", valueArg(args[1:], "id", c.workspace))
		body := map[string]any{"workspace_id": workspace, "display_name": valueArg(args[1:], "name", workspace), "subject": valueArg(args[1:], "subject", "operator"), "tool_root": valueArg(args[1:], "tool-root", "/workspace/fixtures/reference-repo"), "default_provider": "fake", "idempotency_key": valueArg(args[1:], "idempotency", "bootstrap:"+workspace)}
		response, err := c.request(http.MethodPost, "/v1/operator/workspaces/bootstrap", body, true)
		if err != nil {
			return err
		}
		if token := nestedString(response, "api_key_token"); token != "" && c.key == "" {
			c.key = token
		}
		return c.print(response)
	default:
		return fmt.Errorf("unknown workspace command %q", args[0])
	}
}

func (c *operatorCLI) identityCommand(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		return c.requestPrint(http.MethodGet, "/v1/operator/identities?limit=100&workspace_id="+url.QueryEscape(c.workspace), nil, false)
	}
	switch args[0] {
	case "create":
		body := map[string]any{"subject": valueArg(args[1:], "subject", "operator"), "kind": valueArg(args[1:], "kind", "user"), "display_name": valueArg(args[1:], "name", "")}
		if raw := valueArg(args[1:], "permissions", ""); raw != "" {
			body["permissions"] = splitCSV(raw)
		}
		return c.requestPrint(http.MethodPost, "/v1/operator/identities", body, false)
	case "disable":
		return c.requestPrint(http.MethodPost, "/v1/operator/identities/"+valueArg(args[1:], "id", "")+"/disable", nil, false)
	default:
		return fmt.Errorf("unknown identity command %q", args[0])
	}
}

func (c *operatorCLI) roleCommand(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		return c.requestPrint(http.MethodGet, "/v1/operator/roles?limit=100&workspace_id="+url.QueryEscape(c.workspace), nil, false)
	}
	switch args[0] {
	case "bind":
		return c.requestPrint(http.MethodPost, "/v1/operator/roles", map[string]any{"identity_id": valueArg(args[1:], "identity", ""), "name": valueArg(args[1:], "name", "operator"), "permissions": splitCSV(valueArg(args[1:], "permissions", string(contractsPermissionDefaults())))}, false)
	case "unbind":
		return c.requestPrint(http.MethodPost, "/v1/operator/roles/"+valueArg(args[1:], "identity", "")+"/"+valueArg(args[1:], "role", "")+"/unbind", nil, false)
	default:
		return fmt.Errorf("unknown role command %q", args[0])
	}
}

func contractsPermissionDefaults() string {
	return "workspace:read,task:read,task:mutate,task:execute,agent:run,agent:read,retrieval:read,retrieval:write,evidence:read,evidence:write,model:invoke,tool:execute,evaluation:read,evaluation:run,receipt:read,receipt:write,change:read,change:propose,change:approve,change:apply,change:validate,change:disclose"
}

func (c *operatorCLI) apiKeyCommand(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		return c.requestPrint(http.MethodGet, "/v1/operator/api-keys?limit=100&workspace_id="+url.QueryEscape(c.workspace), nil, false)
	}
	switch args[0] {
	case "create":
		return c.requestPrint(http.MethodPost, "/v1/operator/api-keys", map[string]any{"identity_id": valueArg(args[1:], "identity", "")}, false)
	case "rotate":
		response, err := c.request(http.MethodPost, "/v1/operator/api-keys/"+valueArg(args[1:], "id", "")+"/rotate", map[string]any{}, false)
		if err != nil {
			return err
		}
		if token := nestedString(response, "api_key_token"); token != "" {
			c.key = token
		}
		return c.print(response)
	case "revoke":
		return c.requestPrint(http.MethodPost, "/v1/operator/api-keys/"+valueArg(args[1:], "id", "")+"/revoke", nil, false)
	default:
		return fmt.Errorf("unknown api-key command %q", args[0])
	}
}

func (c *operatorCLI) ingestCommand(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		return c.requestPrint(http.MethodGet, "/v1/operator/ingest/jobs?limit=100&workspace_id="+url.QueryEscape(c.workspace), nil, false)
	}
	switch args[0] {
	case "dry-run":
		return c.requestPrint(http.MethodPost, "/v1/operator/ingest/dry-run", c.ingestBody(args[1:], "dry-run"), false)
	case "submit":
		return c.requestPrint(http.MethodPost, "/v1/operator/ingest/jobs", c.ingestBody(args[1:], "submit"), false)
	case "status":
		return c.requestPrint(http.MethodGet, "/v1/operator/ingest/jobs/"+url.PathEscape(valueArg(args[1:], "id", "")), nil, false)
	case "resume":
		body := map[string]any{"workspace_id": c.workspace, "batch_size": intValue(args[1:], "batch-size", 32), "worker_id": valueArg(args[1:], "worker", "fornix-cli-ingest-worker")}
		if owner := valueArg(args[1:], "task-owner", ""); owner != "" {
			body["task_owner_id"] = owner
			body["task_fence"] = uint64Value(args[1:], "task-fence", 0)
		}
		return c.requestPrint(http.MethodPost, "/v1/operator/ingest/jobs/"+url.PathEscape(valueArg(args[1:], "id", ""))+"/resume", body, false)
	case "cancel":
		return c.requestPrint(http.MethodPost, "/v1/operator/ingest/jobs/"+url.PathEscape(valueArg(args[1:], "id", ""))+"/cancel", map[string]any{"workspace_id": c.workspace}, false)
	default:
		return fmt.Errorf("unknown ingest command %q", args[0])
	}
}

func (c *operatorCLI) ingestBody(args []string, operation string) map[string]any {
	workspace := c.workspace
	root := valueArg(args, "source-root", envOr("FORNIX_REFERENCE_WORKDIR", "/workspace/fixtures/reference-repo"))
	repository := valueArg(args, "repository", "reference-repo")
	body := map[string]any{
		"workspace_id":    workspace,
		"idempotency_key": valueArg(args, "idempotency", "ingest:"+workspace+":"+repository+":"+operation),
		"source": map[string]any{
			"repository": repository, "source_root": root,
			"ignore_rules":    splitCSV(valueArg(args, "ignore", "")),
			"extract_symbols": valueArg(args, "symbols", "true") != "false",
			"embedding":       map[string]any{"enabled": valueArg(args, "embeddings", "false") == "true"},
		},
		"batch_size": intValue(args, "batch-size", 32),
	}
	return body
}

func (c *operatorCLI) taskCommand(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		return c.requestPrint(http.MethodGet, "/v1/tasks?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	}
	switch args[0] {
	case "create":
		body := map[string]any{"workspace_id": c.workspace, "title": valueArg(args[1:], "title", "Reference workflow"), "brief": valueArg(args[1:], "brief", "[fornix-reference-workflow] inspect the repository and produce a report"), "required_capabilities": []string{}, "max_attempts": 2}
		return c.requestPrint(http.MethodPost, "/v1/task", body, false)
	case "get":
		return c.requestPrint(http.MethodGet, "/v1/task/"+valueArg(args[1:], "id", "")+"?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	case "claim":
		return c.requestPrint(http.MethodPost, "/v1/task/claim", map[string]any{"workspace_id": c.workspace, "session_id": valueArg(args[1:], "session", "fornix-cli-worker"), "lease_ttl_ms": 120000}, false)
	case "complete":
		body := map[string]any{"workspace_id": c.workspace, "session_id": valueArg(args[1:], "session", "fornix-cli-worker"), "fence": uint64Value(args[1:], "fence", 0), "status": "done", "result": valueArg(args[1:], "result", "completed"), "idempotency_key": valueArg(args[1:], "idempotency", "task-complete")}
		return c.requestPrint(http.MethodPost, "/v1/task/"+valueArg(args[1:], "id", "")+"/complete", body, false)
	case "cancel":
		return c.requestPrint(http.MethodPost, "/v1/task/"+valueArg(args[1:], "id", "")+"/cancel", map[string]any{"workspace_id": c.workspace, "reason": valueArg(args[1:], "reason", "cancelled")}, false)
	default:
		return fmt.Errorf("unknown task command %q", args[0])
	}
}

func (c *operatorCLI) runCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("run requires create, get, or replay")
	}
	switch args[0] {
	case "get":
		return c.requestPrint(http.MethodGet, "/v1/agent/run/"+valueArg(args[1:], "id", "")+"?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	case "replay":
		return c.requestPrint(http.MethodPost, "/v1/agent/run/"+valueArg(args[1:], "id", "")+"/replay?workspace_id="+url.QueryEscape(c.workspace), map[string]any{}, false)
	default:
		return fmt.Errorf("unknown run command %q", args[0])
	}
}

func (c *operatorCLI) retrieveCommand(args []string) error {
	return c.requestPrint(http.MethodPost, "/v1/retrieve", map[string]any{"workspace_id": c.workspace, "query": valueArg(args, "query", ""), "max_items": 20, "max_bytes": 32768, "max_tokens": 8192}, false)
}

func (c *operatorCLI) evaluationCommand(args []string) error {
	if len(args) > 0 && args[0] == "surfaces" {
		return c.requestPrint(http.MethodGet, "/v1/evaluations/retrieval/surfaces?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	}
	if len(args) > 0 && args[0] == "status" {
		return c.requestPrint(http.MethodGet, "/v1/evaluations/runs/"+valueArg(args[1:], "id", "")+"?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	}
	return errors.New("evaluation supports surfaces and status; dataset/run APIs remain HTTP-compatible")
}

func (c *operatorCLI) artifactCommand(args []string) error {
	body := map[string]any{"workspace_id": c.workspace, "content_hash": valueArg(args, "hash", ""), "level": valueArg(args, "level", "gist"), "max_bytes": 32768, "max_tokens": 8192, "max_items": 100}
	return c.requestPrint(http.MethodPost, "/v1/artifacts/disclose", body, false)
}

func (c *operatorCLI) evidenceCommand(args []string) error {
	if len(args) > 0 && args[0] == "provenance" {
		return c.requestPrint(http.MethodPost, "/v1/evidence/provenance", map[string]any{"workspace_id": c.workspace, "evidence_id": int64Value(args[1:], "id", 0), "max_depth": 4, "max_nodes": 32}, false)
	}
	return c.requestPrint(http.MethodPost, "/v1/evidence/disclose", map[string]any{"workspace_id": c.workspace, "evidence_id": int64Value(args, "id", 0), "level": valueArg(args, "level", "gist"), "max_bytes": 32768, "max_tokens": 8192, "max_nodes": 32}, false)
}

func (c *operatorCLI) receiptCommand(args []string) error {
	if len(args) == 0 || args[0] == "get" {
		return c.requestPrint(http.MethodGet, "/v1/work-receipts/"+url.PathEscape(valueArg(args[1:], "id", ""))+"?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	}
	if args[0] == "disclose" {
		body := map[string]any{
			"workspace_id": c.workspace, "receipt_id": valueArg(args[1:], "id", ""),
			"level": valueArg(args[1:], "level", "gist"), "max_bytes": intValue(args[1:], "max-bytes", 32768),
			"max_tokens": intValue(args[1:], "max-tokens", 8192), "max_items": intValue(args[1:], "max-items", 64),
		}
		return c.requestPrint(http.MethodPost, "/v1/work-receipts/disclose", body, false)
	}
	return fmt.Errorf("unknown receipt command %q", args[0])
}

func (c *operatorCLI) changeCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("change requires dry-run, propose, get, approve, apply, or disclose")
	}
	switch args[0] {
	case "dry-run", "propose":
		body, err := c.changeBody(args[1:])
		if err != nil {
			return err
		}
		path := "/v1/changes"
		if args[0] == "dry-run" {
			path = "/v1/changes/dry-run"
		}
		return c.requestPrint(http.MethodPost, path, body, false)
	case "get":
		return c.requestPrint(http.MethodGet, "/v1/changes/"+url.PathEscape(valueArg(args[1:], "id", ""))+"?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	case "approve":
		body := map[string]any{
			"workspace_id":    c.workspace,
			"packet_hash":     valueArg(args[1:], "packet-hash", ""),
			"decision":        valueArg(args[1:], "decision", "approved"),
			"reason":          valueArg(args[1:], "reason", "operator decision"),
			"idempotency_key": valueArg(args[1:], "idempotency", "change-approval:"+valueArg(args[1:], "id", "")),
		}
		return c.requestPrint(http.MethodPost, "/v1/changes/"+url.PathEscape(valueArg(args[1:], "id", ""))+"/approve", body, false)
	case "apply":
		body := map[string]any{
			"workspace_id":    c.workspace,
			"packet_hash":     valueArg(args[1:], "packet-hash", ""),
			"idempotency_key": valueArg(args[1:], "idempotency", "change-apply:"+valueArg(args[1:], "id", "")),
			"dry_run":         valueArg(args[1:], "dry-run", "false") == "true",
		}
		return c.requestPrint(http.MethodPost, "/v1/changes/"+url.PathEscape(valueArg(args[1:], "id", ""))+"/apply", body, false)
	case "disclose":
		body := map[string]any{
			"workspace_id":   c.workspace,
			"proposal_id":    valueArg(args[1:], "id", ""),
			"application_id": valueArg(args[1:], "application-id", ""),
			"level":          valueArg(args[1:], "level", "gist"),
			"max_bytes":      intValue(args[1:], "max-bytes", 32768),
			"max_items":      intValue(args[1:], "max-items", 100),
		}
		if body["application_id"] == "" {
			delete(body, "application_id")
		}
		if body["proposal_id"] == "" {
			delete(body, "proposal_id")
		}
		return c.requestPrint(http.MethodPost, "/v1/changes/disclose", body, false)
	default:
		return fmt.Errorf("unknown change command %q", args[0])
	}
}

func (c *operatorCLI) validationCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("validation requires dry-run, run, status, results, replay, disclose, resume, cancel, or handoff")
	}
	switch args[0] {
	case "dry-run", "run":
		body := c.validationBody(args[1:])
		body["dry_run"] = args[0] == "dry-run"
		return c.requestPrint(http.MethodPost, "/v1/validations", body, false)
	case "status":
		return c.requestPrint(http.MethodGet, "/v1/validations/"+url.PathEscape(valueArg(args[1:], "id", ""))+"?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	case "results":
		id := url.PathEscape(valueArg(args[1:], "id", ""))
		path := "/v1/validations/" + id + "/results?workspace_id=" + url.QueryEscape(c.workspace) + "&limit=" + strconv.Itoa(intValue(args[1:], "limit", 64)) + "&offset=" + strconv.Itoa(intValue(args[1:], "offset", 0))
		return c.requestPrint(http.MethodGet, path, nil, false)
	case "replay":
		id := url.PathEscape(valueArg(args[1:], "id", ""))
		return c.requestPrint(http.MethodGet, "/v1/validations/"+id+"/replay?workspace_id="+url.QueryEscape(c.workspace)+"&limit="+strconv.Itoa(intValue(args[1:], "limit", 500)), nil, false)
	case "disclose":
		return c.requestPrint(http.MethodPost, "/v1/validations/disclose", map[string]any{"workspace_id": c.workspace, "validation_run_id": valueArg(args[1:], "id", ""), "level": valueArg(args[1:], "level", "gist"), "max_bytes": intValue(args[1:], "max-bytes", 32768), "max_items": intValue(args[1:], "max-items", 64)}, false)
	case "resume":
		id := url.PathEscape(valueArg(args[1:], "id", ""))
		return c.requestPrint(http.MethodPost, "/v1/validations/"+id+"/resume?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	case "cancel":
		id := url.PathEscape(valueArg(args[1:], "id", ""))
		return c.requestPrint(http.MethodPost, "/v1/validations/"+id+"/cancel?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	case "handoff":
		id := valueArg(args[1:], "id", "")
		path := "/v1/reindex-handoffs/" + url.PathEscape(id) + "?workspace_id=" + url.QueryEscape(c.workspace)
		if len(args) > 1 && args[1] == "submit" {
			path = "/v1/reindex-handoffs/" + url.PathEscape(valueArg(args[2:], "id", "")) + "/submit?workspace_id=" + url.QueryEscape(c.workspace)
			return c.requestPrint(http.MethodPost, path, nil, false)
		}
		return c.requestPrint(http.MethodGet, path, nil, false)
	default:
		return fmt.Errorf("unknown validation command %q", args[0])
	}
}

// policyCommand exposes the deterministic policy lifecycle without adding a
// second authority. It deliberately sends JSON over the authenticated HTTP
// API so CLI, API, and MCP requests share identical authorization and audit
// semantics.
func (c *operatorCLI) policyCommand(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		path := "/v1/policies?workspace_id=" + url.QueryEscape(c.workspace) + "&limit=" + strconv.Itoa(intValue(args[1:], "limit", 50))
		if cursor := valueArg(args[1:], "cursor", ""); cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		return c.requestPrint(http.MethodGet, path, nil, false)
	}
	switch args[0] {
	case "create":
		policyID := valueArg(args[1:], "id", "default")
		version := valueArg(args[1:], "version", "1")
		validators := make([]any, 0)
		for _, raw := range splitCSV(valueArg(args[1:], "validators", "")) {
			parts := strings.SplitN(raw, "@", 2)
			validatorVersion := "1"
			if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
				validatorVersion = strings.TrimSpace(parts[1])
			}
			if strings.TrimSpace(parts[0]) != "" {
				validators = append(validators, map[string]any{"validator": map[string]any{"id": strings.TrimSpace(parts[0]), "version": validatorVersion}, "required": true})
			}
		}
		pack := map[string]any{
			"workspace_id": c.workspace, "policy_id": policyID, "version": version,
			"rules": validators, "require_reindex": valueArg(args[1:], "require-reindex", "false") == "true",
			"approval": map[string]any{"mode": valueArg(args[1:], "approval", "required")},
		}
		body := map[string]any{"workspace_id": c.workspace, "pack": pack, "idempotency_key": valueArg(args[1:], "idempotency", "policy:create:"+c.workspace+":"+policyID+":"+version)}
		return c.requestPrint(http.MethodPost, "/v1/policies", body, false)
	case "get":
		return c.requestPrint(http.MethodGet, c.policyPath(valueArg(args[1:], "id", ""), valueArg(args[1:], "version", "1"))+"?workspace_id="+url.QueryEscape(c.workspace), nil, false)
	case "activate", "default", "retire":
		policyID := valueArg(args[1:], "id", "")
		version := valueArg(args[1:], "version", "1")
		body := map[string]any{"workspace_id": c.workspace, "policy_hash": valueArg(args[1:], "hash", ""), "reason": valueArg(args[1:], "reason", "operator policy "+args[0]), "idempotency_key": valueArg(args[1:], "idempotency", "policy:"+args[0]+":"+c.workspace+":"+policyID+":"+version)}
		return c.requestPrint(http.MethodPost, c.policyPath(policyID, version)+"/"+args[0], body, false)
	case "resolve", "dry-run-resolve":
		body := map[string]any{"workspace_id": c.workspace, "requested_approval_mode": valueArg(args[1:], "approval", "")}
		if policyID := valueArg(args[1:], "id", ""); policyID != "" {
			body["policy"] = map[string]any{"workspace_id": c.workspace, "policy_id": policyID, "version": valueArg(args[1:], "version", "1"), "policy_hash": valueArg(args[1:], "hash", "")}
		}
		path := "/v1/policies/resolve"
		if args[0] == "dry-run-resolve" {
			path = "/v1/policies/dry-run-resolve"
		}
		return c.requestPrint(http.MethodPost, path, body, false)
	case "compare":
		left := map[string]any{"workspace_id": c.workspace, "policy_id": valueArg(args[1:], "left-id", ""), "version": valueArg(args[1:], "left-version", "1"), "policy_hash": valueArg(args[1:], "left-hash", "")}
		right := map[string]any{"workspace_id": c.workspace, "policy_id": valueArg(args[1:], "right-id", ""), "version": valueArg(args[1:], "right-version", "1"), "policy_hash": valueArg(args[1:], "right-hash", "")}
		return c.requestPrint(http.MethodPost, "/v1/policies/compare", map[string]any{"workspace_id": c.workspace, "left": left, "right": right}, false)
	case "audit":
		path := "/v1/policies/audit?workspace_id=" + url.QueryEscape(c.workspace) + "&limit=" + strconv.Itoa(intValue(args[1:], "limit", 50))
		if policyID := valueArg(args[1:], "id", ""); policyID != "" {
			path += "&policy_id=" + url.QueryEscape(policyID)
		}
		if version := valueArg(args[1:], "version", ""); version != "" {
			path += "&version=" + url.QueryEscape(version)
		}
		if cursor := valueArg(args[1:], "cursor", ""); cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		return c.requestPrint(http.MethodGet, path, nil, false)
	default:
		return fmt.Errorf("unknown policy command %q", args[0])
	}
}

func (c *operatorCLI) policyPath(policyID, version string) string {
	return "/v1/policies/" + url.PathEscape(policyID) + "/" + url.PathEscape(version)
}

func (c *operatorCLI) validationBody(args []string) map[string]any {
	repository := valueArg(args, "repository", "reference-repo")
	body := map[string]any{
		"workspace_id": c.workspace, "repository": repository,
		"change_application_id": valueArg(args, "application-id", ""),
		"proposal_id":           valueArg(args, "proposal-id", ""),
		"packet_hash":           valueArg(args, "packet-hash", ""),
		"expected_tree_hash":    valueArg(args, "expected-tree-hash", ""),
		"idempotency_key":       valueArg(args, "idempotency", "validation:"+c.workspace+":"+repository+":"+valueArg(args, "application-id", "")),
		"source":                map[string]any{"repository": repository, "source_root": valueArg(args, "source-root", envOr("FORNIX_REFERENCE_WORKDIR", "/workspace/fixtures/reference-repo"))},
	}
	if owner := valueArg(args, "task-owner", ""); owner != "" {
		body["task_owner_id"], body["task_fence"] = owner, uint64Value(args, "task-fence", 0)
	}
	return body
}

func (c *operatorCLI) changeBody(args []string) (map[string]any, error) {
	content := []byte(valueArg(args, "content", ""))
	if path := valueArg(args, "content-file", ""); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read change content file: %w", err)
		}
		content = data
	}
	operation := map[string]any{
		"id":            valueArg(args, "operation-id", "op-1"),
		"type":          valueArg(args, "type", "replace_file"),
		"path":          valueArg(args, "path", ""),
		"expected_hash": valueArg(args, "expected-hash", ""),
	}
	if destination := valueArg(args, "destination", ""); destination != "" {
		operation["destination"] = destination
	}
	if len(content) > 0 {
		operation["content"] = content
	}
	if mode := valueArg(args, "mode", ""); mode != "" {
		parsed, err := strconv.ParseUint(mode, 8, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid file mode: %w", err)
		}
		operation["new_mode"] = parsed
	}
	root := valueArg(args, "source-root", envOr("FORNIX_REFERENCE_WORKDIR", "/workspace/fixtures/reference-repo"))
	repository := valueArg(args, "repository", "reference-repo")
	return map[string]any{
		"workspace_id":    c.workspace,
		"repository":      repository,
		"source":          map[string]any{"workspace_id": c.workspace, "repository": repository, "source_root": root},
		"operations":      []any{operation},
		"approval_mode":   valueArg(args, "approval-mode", "required"),
		"idempotency_key": valueArg(args, "idempotency", "change:"+c.workspace+":"+sha256String(repository+":"+operation["path"].(string)+":"+string(content))),
	}, nil
}

func (c *operatorCLI) referenceWorkflow(args []string) error {
	workspace := valueArg(args, "workspace", c.workspace)
	workdir := valueArg(args, "workdir", valueArg(args, "fixture", envOr("FORNIX_REFERENCE_WORKDIR", "/workspace/fixtures/reference-repo")))
	c.workspace = workspace
	// Bootstrap uses the explicit bootstrap credential, and switches to the
	// newly-created workspace API key only in memory for the remaining calls.
	bootstrap := map[string]any{"workspace_id": workspace, "display_name": "Fornix Reference Workspace", "subject": "reference-operator", "default_provider": "fake", "tool_root": workdir, "idempotency_key": "reference-bootstrap:" + workspace}
	bootstrapResponse, err := c.request(http.MethodPost, "/v1/operator/workspaces/bootstrap", bootstrap, true)
	if err != nil {
		return err
	}
	if token := nestedString(bootstrapResponse, "api_key_token"); token != "" && c.key == "" {
		c.key = token
	}
	ingest, err := c.request(http.MethodPost, "/v1/operator/ingest/jobs", map[string]any{"workspace_id": workspace, "idempotency_key": "reference-ingest:" + workspace, "source": map[string]any{"repository": "reference-repo", "source_root": workdir, "extract_symbols": true, "embedding": map[string]any{"enabled": false}}, "batch_size": 2}, false)
	if err != nil {
		return err
	}
	jobID := nestedID(ingest, "job", "id")
	if jobID == "" {
		return errors.New("reference workflow did not return an ingest job id")
	}
	for attempt := 0; attempt < 512; attempt++ {
		statusResponse, statusErr := c.request(http.MethodGet, "/v1/operator/ingest/jobs/"+url.PathEscape(jobID), nil, false)
		if statusErr != nil {
			return statusErr
		}
		job, _ := statusResponse["job"].(map[string]any)
		if job == nil {
			job = statusResponse
		}
		status := stringValue(job, "status")
		if status == "succeeded" {
			ingest = statusResponse
			break
		}
		if status == "failed" || status == "cancelled" {
			return fmt.Errorf("reference ingestion ended in %s", status)
		}
		if _, err := c.request(http.MethodPost, "/v1/operator/ingest/jobs/"+url.PathEscape(jobID)+"/resume", map[string]any{"workspace_id": workspace, "batch_size": 2, "worker_id": "fornix-reference-ingest-worker"}, false); err != nil {
			return err
		}
	}
	job, _ := ingest["job"].(map[string]any)
	if job == nil {
		job = ingest
	}
	manifestHash := stringValue(job, "manifest_hash")
	if stringValue(job, "status") != "succeeded" {
		return errors.New("reference ingestion did not reach succeeded")
	}
	sessionID := "fornix-reference-worker"
	if _, err := c.request(http.MethodPost, "/v1/session", map[string]any{"workspace_id": workspace, "id": sessionID, "host": "fornix-cli", "capabilities": []string{"repository.read", "agent.execute"}}, false); err != nil {
		return err
	}
	taskResponse, err := c.request(http.MethodPost, "/v1/task", map[string]any{"workspace_id": workspace, "title": "Reference repository report", "brief": "[fornix-reference-workflow] inspect README.md and produce a bounded report", "required_capabilities": []string{}, "created_by": "reference-operator", "max_attempts": 2}, false)
	if err != nil {
		return err
	}
	taskID := stringValue(taskResponse, "id")
	claim, err := c.request(http.MethodPost, "/v1/task/claim", map[string]any{"workspace_id": workspace, "session_id": sessionID, "lease_ttl_ms": 120000}, false)
	if err != nil {
		return err
	}
	fence := uint64ValueFromResponse(claim, "fence")
	runRequest := map[string]any{
		"workspace_id": workspace, "idempotency_key": "reference-run:" + taskID, "goal": "[fornix-reference-workflow] workdir=" + workdir + " Read README.md and summarize the repository.",
		"provider": map[string]any{"provider": "fake", "model": "fake-model"},
		"task":     map[string]any{"id": taskID, "kind": "task", "workspace_id": workspace},
		"session":  map[string]any{"id": sessionID, "kind": "session", "workspace_id": workspace}, "task_owner_id": sessionID, "task_fence": fence,
		"tools":     []any{map[string]any{"name": "fornix.repository.read", "description": "read repository files", "parameters": map[string]any{"type": "object"}}},
		"retrieval": map[string]any{"workspace_id": workspace, "query": "README repository overview", "repo": "reference-repo", "max_items": 8, "max_bytes": 8192, "max_tokens": 2048},
		"budget":    map[string]any{"max_turns": 3, "max_model_steps": 4, "max_tool_calls": 2, "max_context_bytes": 32768, "max_output_tokens": 512, "max_wall_time_ms": 30000, "max_cost_usd": 1, "max_tool_attempts": 1},
	}
	runResponse, err := c.request(http.MethodPost, "/v1/agent/run", runRequest, false)
	if err != nil {
		return err
	}
	runID := nestedID(runResponse, "run", "id")
	if runID == "" {
		runID = stringValue(runResponse, "id")
	}
	if runID == "" {
		return errors.New("reference workflow did not return an agent run id")
	}
	run := map[string]any{}
	if raw, ok := runResponse["run"].(map[string]any); ok {
		run = raw
	}
	report, _ := json.Marshal(map[string]any{"workflow": "fornix-reference", "task_id": taskID, "run_id": runID, "manifest_hash": manifestHash, "output": run["last_output"], "context_hash": run["context_hash"], "state_hash": run["state_hash"]})
	artifact, err := c.request(http.MethodPost, "/v1/artifacts", map[string]any{"workspace_id": workspace, "kind": "reference-report", "media_type": "application/json", "raw": report, "source_kind": "agent_run", "source_id": runID, "role": "report", "idempotency_key": "reference-report:" + runID}, false)
	if err != nil {
		return err
	}
	evidence, err := c.request(http.MethodPost, "/v1/evidence", map[string]any{"workspace_id": workspace, "source_reference": "agent-run:" + runID + ":report", "deduplication_key": "reference-report:" + runID, "kind": "agent-report", "media_type": "application/json", "gist": "deterministic reference workflow report", "detail": "artifact-backed report for the reference workflow", "raw_payload": json.RawMessage(report)}, false)
	if err != nil {
		return err
	}
	complete, err := c.request(http.MethodPost, "/v1/task/"+taskID+"/complete", map[string]any{"workspace_id": workspace, "session_id": sessionID, "fence": fence, "status": "done", "result": "report artifact created", "idempotency_key": "reference-task-complete:" + taskID}, false)
	if err != nil {
		return err
	}
	replay, err := c.request(http.MethodPost, "/v1/agent/run/"+runID+"/replay?workspace_id="+url.QueryEscape(workspace), map[string]any{}, false)
	if err != nil {
		return err
	}
	replayVerified := false
	if checkpoint, ok := replay["checkpoint"].(map[string]any); ok {
		events, eventsOK := replay["events"].([]any)
		replayVerified = eventsOK && stringValue(checkpoint, "state_hash") != "" && stringValue(checkpoint, "state_hash") == stringValue(run, "state_hash") && len(events) > 0
	}
	stateHash := stringValue(run, "state_hash")
	receiptRequest := map[string]any{
		"workspace_id": workspace, "idempotency_key": "reference-receipt:" + taskID,
		"work_kind": "task", "work_id": taskID,
		"task":          map[string]any{"id": taskID, "kind": "task", "workspace_id": workspace},
		"session":       map[string]any{"id": sessionID, "kind": "session", "workspace_id": workspace},
		"task_owner_id": sessionID, "task_fence": fence, "source_manifest_hash": manifestHash, "replay_hash": stateHash,
		"steps": []any{
			map[string]any{"ordinal": 0, "id": "ingest", "name": "repository ingestion", "kind": "ingest", "status": "succeeded", "source_kind": "repository_ingest", "source_id": jobID, "source_hash": manifestHash},
			map[string]any{"ordinal": 1, "id": "context", "name": "bounded context", "kind": "retrieval", "status": "succeeded", "output_hash": stringValue(run, "context_hash")},
			map[string]any{"ordinal": 2, "id": "agent-run", "name": "agent run", "kind": "agent", "status": "succeeded", "source_kind": "agent_run", "source_id": runID, "source_hash": stateHash},
			map[string]any{"ordinal": 3, "id": "report-artifact", "name": "report artifact", "kind": "artifact", "status": "succeeded", "source_kind": "artifact", "source_id": nestedID(artifact, "artifact", "id"), "source_hash": nestedFieldString(artifact, "artifact", "content_hash")},
			map[string]any{"ordinal": 4, "id": "report-evidence", "name": "report evidence", "kind": "evidence", "status": "succeeded", "source_kind": "evidence", "source_id": nestedID(evidence, "record", "id"), "source_hash": nestedFieldString(evidence, "record", "evidence_hash")},
			map[string]any{"ordinal": 5, "id": "task-completion", "name": "task completion", "kind": "task", "status": "succeeded", "source_kind": "task", "source_id": taskID},
		},
		"references": []any{
			map[string]any{"workspace_id": workspace, "kind": "task", "source_id": taskID},
			map[string]any{"workspace_id": workspace, "kind": "agent_run", "source_id": runID, "hash": stateHash},
			map[string]any{"workspace_id": workspace, "kind": "artifact", "source_id": nestedID(artifact, "artifact", "id"), "hash": nestedFieldString(artifact, "artifact", "content_hash"), "role": "report"},
			map[string]any{"workspace_id": workspace, "kind": "evidence", "source_id": nestedID(evidence, "record", "id"), "hash": nestedFieldString(evidence, "record", "evidence_hash"), "role": "report"},
		},
	}
	if artifactID := int64ValueFromString(nestedID(artifact, "artifact", "id")); artifactID > 0 {
		receiptRequest["artifacts"] = []any{map[string]any{"id": int64ValueFromString(nestedID(artifact, "reference", "id")), "artifact_id": artifactID, "workspace_id": workspace, "content_hash": nestedFieldString(artifact, "artifact", "content_hash"), "source_kind": "agent_run", "source_id": runID, "role": "report"}}
	}
	if evidenceID := int64ValueFromString(nestedID(evidence, "record", "id")); evidenceID > 0 {
		receiptRequest["evidence"] = []any{map[string]any{"id": evidenceID, "workspace_id": workspace, "evidence_hash": nestedFieldString(evidence, "record", "evidence_hash"), "source_reference": "agent-run:" + runID + ":report", "role": "report"}}
	}
	receipt, err := c.request(http.MethodPost, "/v1/work-receipts", receiptRequest, false)
	if err != nil {
		return err
	}
	return c.print(map[string]any{"workspace": workspace, "ingest": ingest, "task": taskResponse, "claim": claim, "run": runResponse, "artifact": artifact, "evidence": evidence, "completion": complete, "replay": replay, "replay_verified": replayVerified, "receipt": receipt})
}

func (c *operatorCLI) requestPrint(method, path string, body any, bootstrap bool) error {
	response, err := c.request(method, path, body, bootstrap)
	if err != nil {
		return err
	}
	return c.print(response)
}

func (c *operatorCLI) request(method, path string, body any, bootstrap bool) (map[string]any, error) {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequest(method, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	key := c.key
	if bootstrap && c.bootstrapKey != "" {
		key = c.bootstrapKey
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if c.workspace != "" {
		req.Header.Set("X-Workspace-ID", c.workspace)
	}
	req.Header.Set("X-Request-ID", "fornix-cli-"+sha256String(method+path+string(data)))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var decoded any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil && resp.StatusCode < 300 {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fornix %s %s returned %d: %s", method, path, resp.StatusCode, safeJSON(decoded))
	}
	result, ok := decoded.(map[string]any)
	if !ok {
		return map[string]any{"value": decoded}, nil
	}
	return result, nil
}

func (c *operatorCLI) print(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}
func safeJSON(value any) string { raw, _ := json.Marshal(value); return string(raw) }
func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
func sha256String(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:24]
}
func valueArg(args []string, name, fallback string) string {
	for i, arg := range args {
		if arg == "--"+name && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--"+name+"=") {
			return strings.TrimPrefix(arg, "--"+name+"=")
		}
	}
	return fallback
}
func uint64Value(args []string, name string, fallback uint64) uint64 {
	value, _ := strconv.ParseUint(valueArg(args, name, ""), 10, 64)
	if value == 0 {
		return fallback
	}
	return value
}
func intValue(args []string, name string, fallback int) int {
	value, _ := strconv.Atoi(valueArg(args, name, ""))
	if value <= 0 {
		return fallback
	}
	return value
}
func int64Value(args []string, name string, fallback int64) int64 {
	value, _ := strconv.ParseInt(valueArg(args, name, ""), 10, 64)
	if value == 0 {
		return fallback
	}
	return value
}
func int64ValueFromString(value string) int64 { n, _ := strconv.ParseInt(value, 10, 64); return n }
func splitCSV(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
func nestedString(value map[string]any, key string) string {
	if item, ok := value[key].(string); ok {
		return item
	}
	return ""
}
func stringValue(value map[string]any, key string) string {
	if item, ok := value[key].(string); ok {
		return item
	}
	if item, ok := value[key].(float64); ok {
		return strconv.FormatInt(int64(item), 10)
	}
	return ""
}
func nestedID(value map[string]any, object, key string) string {
	if item, ok := value[object].(map[string]any); ok {
		return stringValue(item, key)
	}
	return ""
}
func nestedFieldString(value map[string]any, object, key string) string {
	if item, ok := value[object].(map[string]any); ok {
		return stringValue(item, key)
	}
	return ""
}
func uint64ValueFromResponse(value map[string]any, key string) uint64 {
	if item, ok := value[key].(float64); ok {
		return uint64(item)
	}
	if item, ok := value[key].(string); ok {
		n, _ := strconv.ParseUint(item, 10, 64)
		return n
	}
	return 0
}
