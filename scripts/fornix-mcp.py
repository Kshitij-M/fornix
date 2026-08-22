#!/usr/bin/env python3
"""Fornix MCP compatibility shim — bridges Claude Code → Fornix HTTP API.

Tools exposed (v0.10.1 compatibility shim):
  - fornix__health
  - fornix__search        (memo search)
  - fornix__remember      (memo create)
  - fornix__symbol_search
  - fornix__symbol_callers
  - fornix__symbol_callees
  - fornix__symbol_context

Speaks MCP stdio (JSON-RPC 2.0). Configure in ~/.claude.json mcpServers.fornix
with command python3 + args [this file path] + env FORNIX_URL + FORNIX_KEY.
"""
from __future__ import annotations

import json
import os
import sys
import traceback
from typing import Any
from urllib import request as urlreq
from urllib.error import HTTPError, URLError

FORNIX_URL = os.environ.get("FORNIX_URL", "http://localhost:8201").rstrip("/")
FORNIX_KEY = os.environ.get("FORNIX_KEY", "")
FORNIX_WORKSPACE = os.environ.get("FORNIX_WORKSPACE_ID", "default")
TIMEOUT = float(os.environ.get("FORNIX_TIMEOUT", "20"))


def _call(method: str, path: str, body: Any | None = None, idempotency: str = "") -> Any:
    url = f"{FORNIX_URL}{path}"
    data = None
    headers = {"Authorization": f"Bearer {FORNIX_KEY}"}
    if FORNIX_WORKSPACE:
        headers["X-Workspace-ID"] = FORNIX_WORKSPACE
    if idempotency:
        headers["Idempotency-Key"] = idempotency
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urlreq.Request(url, data=data, headers=headers, method=method)
    try:
        with urlreq.urlopen(req, timeout=TIMEOUT) as r:
            return json.loads(r.read().decode() or "null")
    except HTTPError as e:
        msg = e.read().decode(errors="replace") if e.fp else str(e)
        raise RuntimeError(f"fornix {method} {path} -> {e.code}: {msg}") from None
    except URLError as e:
        raise RuntimeError(f"fornix {method} {path} unreachable: {e.reason}") from None


def tool_health(_args: dict) -> dict:
    return _call("GET", "/v1/health")


def tool_task_create(args: dict) -> dict:
    return _call("POST", "/v1/task", {
        "workspace_id": FORNIX_WORKSPACE,
        "title": args.get("title", "MCP task"),
        "brief": args.get("brief", ""),
        "required_capabilities": args.get("required_capabilities", []),
        "max_attempts": int(args.get("max_attempts") or 2),
    }, args.get("idempotency_key", ""))


def tool_task_list(_args: dict) -> dict:
    return _call("GET", f"/v1/tasks?workspace_id={FORNIX_WORKSPACE}")


def tool_task_get(args: dict) -> dict:
    return _call("GET", f"/v1/task/{int(args['task_id'])}?workspace_id={FORNIX_WORKSPACE}")


def _ingest_source(args: dict) -> dict:
    return {
        "repository": args.get("repository", "reference-repo"),
        "source_root": args.get("source_root", os.environ.get("FORNIX_REFERENCE_WORKDIR", "/workspace/fixtures/reference-repo")),
        "ignore_rules": list(args.get("ignore_rules") or [])[:128],
        "extract_symbols": bool(args.get("extract_symbols", True)),
        "embedding": {"enabled": bool(args.get("embedding", False))},
    }


def tool_ingest_dry_run(args: dict) -> dict:
    return _call("POST", "/v1/operator/ingest/dry-run", {
        "workspace_id": FORNIX_WORKSPACE,
        "source": _ingest_source(args),
        "batch_size": min(int(args.get("batch_size") or 32), 256),
    })


def tool_ingest_submit(args: dict) -> dict:
    key = args.get("idempotency_key", "ingest:mcp:" + args.get("repository", "reference-repo"))
    return _call("POST", "/v1/operator/ingest/jobs", {
        "workspace_id": FORNIX_WORKSPACE,
        "idempotency_key": key,
        "source": _ingest_source(args),
        "batch_size": min(int(args.get("batch_size") or 32), 256),
    }, key)


def tool_ingest_status(args: dict) -> dict:
    return _call("GET", f"/v1/operator/ingest/jobs/{args['job_id']}?workspace_id={FORNIX_WORKSPACE}")


def tool_ingest_resume(args: dict) -> dict:
    return _call("POST", f"/v1/operator/ingest/jobs/{args['job_id']}/resume", {
        "workspace_id": FORNIX_WORKSPACE,
        "batch_size": min(int(args.get("batch_size") or 32), 256),
        "worker_id": args.get("worker_id", "fornix-mcp-ingest-worker"),
    })


def tool_ingest_cancel(args: dict) -> dict:
    return _call("POST", f"/v1/operator/ingest/jobs/{args['job_id']}/cancel", {"workspace_id": FORNIX_WORKSPACE})


def tool_retrieve(args: dict) -> dict:
    return _call("POST", "/v1/retrieve", {
        "workspace_id": FORNIX_WORKSPACE,
        "query": args.get("query", ""),
        "max_items": min(int(args.get("max_items") or 20), 100),
        "max_bytes": min(int(args.get("max_bytes") or 32768), 1 << 20),
        "max_tokens": min(int(args.get("max_tokens") or 8192), 65536),
    }, args.get("idempotency_key", ""))


def tool_run_get(args: dict) -> dict:
    return _call("GET", f"/v1/agent/run/{args['run_id']}?workspace_id={FORNIX_WORKSPACE}")


def tool_run_replay(args: dict) -> dict:
    return _call("POST", f"/v1/agent/run/{args['run_id']}/replay?workspace_id={FORNIX_WORKSPACE}", {})


def tool_artifact_disclose(args: dict) -> dict:
    return _call("POST", "/v1/artifacts/disclose", {
        "workspace_id": FORNIX_WORKSPACE,
        "content_hash": args.get("content_hash", ""),
        "artifact_id": int(args.get("artifact_id") or 0),
        "level": args.get("level", "gist"),
        "max_bytes": min(int(args.get("max_bytes") or 32768), 1 << 20),
        "max_tokens": min(int(args.get("max_tokens") or 8192), 65536),
        "max_items": min(int(args.get("max_items") or 100), 100),
    })


def tool_evidence_disclose(args: dict) -> dict:
    return _call("POST", "/v1/evidence/disclose", {
        "workspace_id": FORNIX_WORKSPACE,
        "evidence_id": int(args.get("evidence_id") or 0),
        "source_reference": args.get("source_reference", ""),
        "level": args.get("level", "gist"),
        "max_bytes": min(int(args.get("max_bytes") or 32768), 1 << 20),
        "max_tokens": min(int(args.get("max_tokens") or 8192), 65536),
    })


def tool_receipt_get(args: dict) -> dict:
    receipt_id = str(args.get("receipt_id") or "").strip()
    if not receipt_id or "/" in receipt_id:
        raise ValueError("receipt_id required")
    return _call("GET", f"/v1/work-receipts/{receipt_id}?workspace_id={FORNIX_WORKSPACE}")


def tool_receipt_disclose(args: dict) -> dict:
    return _call("POST", "/v1/work-receipts/disclose", {
        "workspace_id": FORNIX_WORKSPACE,
        "receipt_id": str(args.get("receipt_id") or ""),
        "level": args.get("level", "gist"),
        "max_bytes": min(int(args.get("max_bytes") or 32768), 1 << 20),
        "max_tokens": min(int(args.get("max_tokens") or 8192), 262144),
        "max_items": min(int(args.get("max_items") or 64), 128),
    })


def tool_search(args: dict) -> dict:
    return _call("POST", "/v1/memo/search", {
        "query": args.get("query", ""),
        "top_k": int(args.get("top_k") or 10),
        "type":  args.get("type", ""),
        "mode":  args.get("mode", "hybrid"),
    })


def tool_remember(args: dict) -> dict:
    return _call("POST", "/v1/memo", {
        "title":   args.get("title", ""),
        "content": args.get("content", ""),
        "type":    args.get("type", "general"),
        "tags":    args.get("tags", []),
    })


def tool_symbol_search(args: dict) -> dict:
    return _call("POST", "/v1/symbol/search", {
        "query":       args.get("query", ""),
        "top_k":       int(args.get("top_k") or 10),
        "repo":        args.get("repo", ""),
        "symbol_kind": args.get("symbol_kind", ""),
        "mode":        args.get("mode", "hybrid"),
    })


def tool_symbol_callers(args: dict) -> dict:
    sid = args.get("symbol_id")
    if sid is None:
        raise ValueError("symbol_id required")
    return _call("GET", f"/v1/symbol/{int(sid)}/callers")


def tool_symbol_callees(args: dict) -> dict:
    sid = args.get("symbol_id")
    if sid is None:
        raise ValueError("symbol_id required")
    return _call("GET", f"/v1/symbol/{int(sid)}/callees")


def tool_symbol_context(args: dict) -> dict:
    hits = _call("POST", "/v1/symbol/search", {
        "query":       args.get("query", ""),
        "top_k":       int(args.get("top_k") or 5),
        "repo":        args.get("repo", ""),
        "symbol_kind": args.get("symbol_kind", ""),
        "mode":        args.get("mode", "hybrid"),
    })
    results = hits.get("results") or []
    if not results:
        return {"hit": None, "callers": [], "callees": [], "alternatives": []}
    top = results[0]
    callers = _call("GET", f"/v1/symbol/{int(top['id'])}/callers")
    callees = _call("GET", f"/v1/symbol/{int(top['id'])}/callees")
    return {
        "hit":          top,
        "callers":      callers.get("results") or [],
        "callees":      callees.get("results") or [],
        "alternatives": results[1:],
    }


TOOLS = [
    {
        "name": "fornix__health",
        "description": "Fornix service health: DB status, version, embedding model.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "fn": tool_health,
    },
    {
        "name": "fornix__search",
        "description": "Hybrid name+semantic search over Fornix memos. Returns ranked excerpts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "top_k": {"type": "integer", "minimum": 1, "maximum": 50},
                "type":  {"type": "string", "description": "filter by memo type"},
                "mode":  {"type": "string", "enum": ["hybrid", "tsvector", "semantic"]},
            },
            "required": ["query"],
        },
        "fn": tool_search,
    },
    {
        "name": "fornix__remember",
        "description": "Persist a memo to Fornix (deduplicates on sha256).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "title":   {"type": "string"},
                "content": {"type": "string"},
                "type":    {"type": "string"},
                "tags":    {"type": "array", "items": {"type": "string"}},
            },
            "required": ["content"],
        },
        "fn": tool_remember,
    },
    {
        "name": "fornix__symbol_search",
        "description": "Hybrid name+semantic search over indexed code symbols. Returns top N matches with file_path, line_start/end, signature, kind.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query":       {"type": "string"},
                "top_k":       {"type": "integer", "minimum": 1, "maximum": 50},
                "repo":        {"type": "string"},
                "symbol_kind": {"type": "string", "description": "filter by kind: function|method|struct|interface|class|const|var|type"},
                "mode":        {"type": "string", "enum": ["hybrid", "semantic", "name"]},
            },
            "required": ["query"],
        },
        "fn": tool_symbol_search,
    },
    {
        "name": "fornix__symbol_callers",
        "description": "Who calls this symbol? Returns incoming edges.",
        "inputSchema": {
            "type": "object",
            "properties": {"symbol_id": {"type": "integer"}},
            "required": ["symbol_id"],
        },
        "fn": tool_symbol_callers,
    },
    {
        "name": "fornix__symbol_callees",
        "description": "What does this symbol call? Returns outgoing edges.",
        "inputSchema": {
            "type": "object",
            "properties": {"symbol_id": {"type": "integer"}},
            "required": ["symbol_id"],
        },
        "fn": tool_symbol_callees,
    },
    {
        "name": "fornix__symbol_context",
        "description": "One-call code context: searches for the symbol, returns top hit + callers + callees grouped + runner-up matches as alternatives.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query":       {"type": "string"},
                "repo":        {"type": "string"},
                "symbol_kind": {"type": "string"},
                "top_k":       {"type": "integer", "minimum": 1, "maximum": 20},
                "mode":        {"type": "string", "enum": ["hybrid", "semantic", "name"]},
            },
            "required": ["query"],
        },
        "fn": tool_symbol_context,
    },
    {
        "name": "fornix__task_create",
        "description": "Create a workspace-scoped durable task.",
        "inputSchema": {"type": "object", "properties": {"title": {"type": "string"}, "brief": {"type": "string"}, "required_capabilities": {"type": "array", "items": {"type": "string"}}, "max_attempts": {"type": "integer", "minimum": 1, "maximum": 10}, "idempotency_key": {"type": "string"}}, "required": ["brief"]},
        "fn": tool_task_create,
    },
    {
        "name": "fornix__task_list",
        "description": "List tasks in the authenticated workspace.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "fn": tool_task_list,
    },
    {
        "name": "fornix__task_get",
        "description": "Read one workspace-scoped task.",
        "inputSchema": {"type": "object", "properties": {"task_id": {"type": "integer"}}, "required": ["task_id"]},
        "fn": tool_task_get,
    },
    {
        "name": "fornix__ingest_dry_run",
        "description": "Discover a mounted repository deterministically without durable mutation.",
        "inputSchema": {"type": "object", "properties": {"repository": {"type": "string"}, "source_root": {"type": "string"}, "ignore_rules": {"type": "array", "items": {"type": "string"}}, "extract_symbols": {"type": "boolean"}, "batch_size": {"type": "integer", "maximum": 256}}},
        "fn": tool_ingest_dry_run,
    },
    {
        "name": "fornix__ingest_submit",
        "description": "Submit an idempotent durable repository ingestion job.",
        "inputSchema": {"type": "object", "properties": {"repository": {"type": "string"}, "source_root": {"type": "string"}, "ignore_rules": {"type": "array", "items": {"type": "string"}}, "extract_symbols": {"type": "boolean"}, "embedding": {"type": "boolean"}, "batch_size": {"type": "integer", "maximum": 256}, "idempotency_key": {"type": "string"}}, "required": ["source_root"]},
        "fn": tool_ingest_submit,
    },
    {
        "name": "fornix__ingest_status",
        "description": "Read a workspace-scoped ingestion job and checkpoint.",
        "inputSchema": {"type": "object", "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]},
        "fn": tool_ingest_status,
    },
    {
        "name": "fornix__ingest_resume",
        "description": "Advance one bounded deterministic ingestion batch.",
        "inputSchema": {"type": "object", "properties": {"job_id": {"type": "string"}, "batch_size": {"type": "integer", "maximum": 256}, "worker_id": {"type": "string"}}, "required": ["job_id"]},
        "fn": tool_ingest_resume,
    },
    {
        "name": "fornix__ingest_cancel",
        "description": "Durably cancel an ingestion job without deleting source history.",
        "inputSchema": {"type": "object", "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]},
        "fn": tool_ingest_cancel,
    },
    {
        "name": "fornix__retrieve",
        "description": "Run deterministic bounded retrieval without model or tool side effects.",
        "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}, "max_items": {"type": "integer", "maximum": 100}, "max_bytes": {"type": "integer", "maximum": 1048576}, "max_tokens": {"type": "integer", "maximum": 65536}, "idempotency_key": {"type": "string"}}, "required": ["query"]},
        "fn": tool_retrieve,
    },
    {
        "name": "fornix__run_get",
        "description": "Inspect a durable agent run.",
        "inputSchema": {"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]},
        "fn": tool_run_get,
    },
    {
        "name": "fornix__run_replay",
        "description": "Replay an agent run from its durable event history without remote effects.",
        "inputSchema": {"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]},
        "fn": tool_run_replay,
    },
    {
        "name": "fornix__artifact_disclose",
        "description": "Read a bounded gist/detail/raw artifact disclosure by hash or id.",
        "inputSchema": {"type": "object", "properties": {"artifact_id": {"type": "integer"}, "content_hash": {"type": "string"}, "level": {"type": "string", "enum": ["gist", "detail", "raw"]}, "max_bytes": {"type": "integer", "maximum": 1048576}, "max_tokens": {"type": "integer", "maximum": 65536}}, "additionalProperties": False},
        "fn": tool_artifact_disclose,
    },
    {
        "name": "fornix__evidence_disclose",
        "description": "Read bounded workspace-scoped evidence with provenance-safe disclosure.",
        "inputSchema": {"type": "object", "properties": {"evidence_id": {"type": "integer"}, "source_reference": {"type": "string"}, "level": {"type": "string", "enum": ["gist", "detail", "raw"]}, "max_bytes": {"type": "integer", "maximum": 1048576}, "max_tokens": {"type": "integer", "maximum": 65536}}, "additionalProperties": False},
        "fn": tool_evidence_disclose,
    },
    {
        "name": "fornix__receipt_get",
        "description": "Inspect one immutable workspace-scoped Work Receipt without replaying external effects.",
        "inputSchema": {"type": "object", "properties": {"receipt_id": {"type": "string"}}, "required": ["receipt_id"], "additionalProperties": False},
        "fn": tool_receipt_get,
    },
    {
        "name": "fornix__receipt_disclose",
        "description": "Read a bounded gist/detail/raw Work Receipt disclosure with its stable canonical hash.",
        "inputSchema": {"type": "object", "properties": {"receipt_id": {"type": "string"}, "level": {"type": "string", "enum": ["gist", "detail", "raw"]}, "max_bytes": {"type": "integer", "maximum": 1048576}, "max_tokens": {"type": "integer", "maximum": 262144}, "max_items": {"type": "integer", "maximum": 128}}, "required": ["receipt_id"], "additionalProperties": False},
        "fn": tool_receipt_disclose,
    },
]
TOOL_INDEX = {t["name"]: t for t in TOOLS}


def send(msg: dict) -> None:
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def respond_ok(req_id, result):
    send({"jsonrpc": "2.0", "id": req_id, "result": result})


def respond_err(req_id, code, message):
    send({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})


def handle(msg: dict) -> None:
    method = msg.get("method", "")
    req_id = msg.get("id")
    if method == "initialize":
        respond_ok(req_id, {
            "protocolVersion": "2024-11-05",
            "serverInfo": {"name": "fornix-mcp", "version": "0.10.1"},
            "capabilities": {"tools": {"listChanged": False}},
        })
        return
    if method in ("notifications/initialized", "initialized"):
        return
    if method == "tools/list":
        respond_ok(req_id, {"tools": [
            {"name": t["name"], "description": t["description"], "inputSchema": t["inputSchema"]}
            for t in TOOLS
        ]})
        return
    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name", "")
        args = params.get("arguments") or {}
        tool = TOOL_INDEX.get(name)
        if not tool:
            respond_err(req_id, -32602, f"unknown tool: {name}")
            return
        try:
            result = tool["fn"](args)
            respond_ok(req_id, {"content": [{"type": "text", "text": json.dumps(result, default=str)}]})
        except Exception as e:
            respond_err(req_id, -32000, f"{name}: {e}")
        return
    if req_id is not None:
        respond_err(req_id, -32601, f"method not found: {method}")


def main() -> int:
    if not FORNIX_KEY:
        sys.stderr.write("fornix-mcp: FORNIX_KEY env var required\n")
        return 2
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        try:
            handle(msg)
        except Exception:
            traceback.print_exc(file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
