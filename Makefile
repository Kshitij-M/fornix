SHELL := /bin/sh

GO_IMAGE ?= golang:1.25.13
FORNIX_TEST_PG_DSN_DOCKER := $(subst 127.0.0.1,host.docker.internal,$(subst localhost,host.docker.internal,$(FORNIX_TEST_PG_DSN)))
GO_RUN ?= docker run --rm --add-host=host.docker.internal:host-gateway -u "$$(id -u):$$(id -g)" -e HOME=/tmp -e GOPATH=/tmp/go -e GOCACHE=/tmp/go-build -e GOMODCACHE=/tmp/go-mod -e FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN_DOCKER)" -v "$(CURDIR):/workspace" -w /workspace $(GO_IMAGE)
HOST_GO := $(shell command -v go 2>/dev/null)
GO_CMD := $(if $(HOST_GO),go,$(GO_RUN) go)
GOFMT_CMD := $(if $(HOST_GO),gofmt,$(GO_RUN) gofmt)
GO_FILES := $$(find . -name '*.go' -not -path './vendor/*' -print)
PYTHON ?= python3
PYTHON_VENV ?= .venv
PYTHON_ENV_BIN := $(PYTHON_VENV)/bin/python
PYTHON_BIN := $(if $(wildcard $(PYTHON_ENV_BIN)),$(PYTHON_ENV_BIN),$(PYTHON))
PYTHON_CHECK_BIN := $(if $(wildcard $(PYTHON_BIN)),$(PYTHON_BIN),$(PYTHON))
FORNIX_URL ?= http://localhost:8201
DOCKER ?= docker
# Keep local smokes authenticated when .env is absent. An explicit environment
# or command-line value still wins, while the fallback matches CI and the
# development-only key used by the smoke scripts.
FORNIX_KEY ?= $(shell value=$$(sed -n 's/^FORNIX_KEY=//p' .env 2>/dev/null | head -n 1); if [ -n "$$value" ]; then printf '%s' "$$value"; else printf '%s' 'fornix-ci-test-key'; fi)
# Docker Compose mounts this fixture at the container path below. Override it
# with the host path when the service is running directly on the host.
FORNIX_REFERENCE_WORKDIR ?= /workspace/fixtures/reference-repo
PROJECTION_PG_DSN ?= postgres://fornix:fornix-dev-only@host.docker.internal:55433/fornix?sslmode=disable
FORNIX_TEST_PG_DSN ?=

.PHONY: fmt fmt-check test test-race vet build package-check python-install python-check docs-check check verify hooks-install install-hooks hooks-uninstall uninstall-hooks hooks-check smoke smoke-local-cli smoke-local-runtime smoke-events smoke-projection smoke-leases smoke-tasks smoke-retrieval smoke-provenance smoke-model smoke-tools smoke-agent smoke-scheduler smoke-identity smoke-artifacts smoke-artifact-output smoke-observability smoke-retrieval-quality smoke-retrieval-evaluation smoke-reference-workflow smoke-reference-openai smoke-ingestion smoke-work-receipts smoke-changes smoke-validation smoke-policy operator-reference dev-up dev-up-ai dev-up-watcher dev-run dev-logs dev-down

fmt:
	$(GOFMT_CMD) -w $(GO_FILES)

fmt-check:
	@diff="$$( $(GOFMT_CMD) -d $(GO_FILES) )"; \
	if [ -n "$$diff" ]; then \
		printf '%s\n' "$$diff"; \
		echo 'Go files are not formatted; run make fmt.' >&2; \
		exit 1; \
	fi

test:
	$(GO_CMD) test ./...

test-race:
	$(GO_CMD) test -race ./...

vet:
	$(GO_CMD) vet ./...

build:
	mkdir -p bin
	$(GO_CMD) build -trimpath -o bin/fornix ./cmd/fornix
	$(GO_CMD) build -trimpath -o bin/fornix-watcher ./cmd/fornix-watcher
	$(GO_CMD) build -trimpath -o bin/fornix-eval ./cmd/fornix-eval

package-check:
	sh -n scripts/install.sh
	@test -x scripts/install.sh
	$(GO_CMD) test ./cmd/fornix ./internal/credentials ./internal/profile ./internal/runtime -count=1

python-install:
	$(PYTHON) -m venv $(PYTHON_VENV)
	$(PYTHON_ENV_BIN) -m pip install --upgrade pip
	$(PYTHON_ENV_BIN) -m pip install --requirement scripts/requirements.txt

python-check:
	$(PYTHON_CHECK_BIN) -m py_compile scripts/*.py
	$(PYTHON_CHECK_BIN) -m compileall -q scripts

docs-check:
	$(PYTHON_CHECK_BIN) scripts/check_docs.py

smoke-events:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.11-event-smokes.sh

smoke-projection:
	FORNIX_PROJECTION_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.12-projection-smokes.sh

smoke-leases:
	FORNIX_LEASE_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.13-lease-smokes.sh

smoke-tasks:
	FORNIX_TASK_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.14-task-smokes.sh

smoke-retrieval:
	FORNIX_RETRIEVAL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.15-retrieval-smokes.sh

smoke-provenance:
	FORNIX_PROVENANCE_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.16-provenance-smokes.sh

smoke-model:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.17-model-smokes.sh

smoke-tools:
	FORNIX_TOOL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.18-tool-smokes.sh

smoke-agent:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.19-agent-smokes.sh

smoke-scheduler:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.20-scheduler-smokes.sh

smoke-identity:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.21-identity-smokes.sh

smoke-artifacts:
	FORNIX_ARTIFACT_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.22-artifact-smokes.sh

smoke-artifact-output:
	FORNIX_ARTIFACT_OUTPUT_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.23-artifact-output-smokes.sh

smoke-observability:
	FORNIX_OBSERVABILITY_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.24-observability-smokes.sh

smoke-retrieval-quality:
	FORNIX_EVAL_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.25-retrieval-quality-smokes.sh

smoke-retrieval-evaluation:
	FORNIX_RETRIEVAL_EVAL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.26-retrieval-evaluation-smokes.sh

smoke-reference-workflow:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.27-reference-workflow-smokes.sh

smoke-reference-openai:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.28-openai-smoke.sh

smoke-ingestion:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.29-ingestion-smokes.sh

smoke-work-receipts:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.30-work-receipt-smokes.sh

smoke-changes:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.31-change-smokes.sh

smoke-validation:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.32-validation-smokes.sh

smoke-policy:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.33-policy-smokes.sh

operator-reference: build
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) bin/fornix reference-workflow --workspace $${FORNIX_WORKSPACE_ID:-reference-local} --fixture fixtures/reference-repo

smoke:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) PYTHON_BIN=$(PYTHON_BIN) scripts/test/v0.10-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.11-event-smokes.sh
	FORNIX_PROJECTION_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.12-projection-smokes.sh
	FORNIX_LEASE_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.13-lease-smokes.sh
	FORNIX_TASK_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.14-task-smokes.sh
	FORNIX_RETRIEVAL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.15-retrieval-smokes.sh
	FORNIX_PROVENANCE_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.16-provenance-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.17-model-smokes.sh
	FORNIX_TOOL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.18-tool-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.19-agent-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.20-scheduler-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.21-identity-smokes.sh
	FORNIX_ARTIFACT_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.22-artifact-smokes.sh
	FORNIX_ARTIFACT_OUTPUT_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.23-artifact-output-smokes.sh
	FORNIX_OBSERVABILITY_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.24-observability-smokes.sh
	FORNIX_EVAL_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.25-retrieval-quality-smokes.sh
	FORNIX_RETRIEVAL_EVAL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.26-retrieval-evaluation-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.27-reference-workflow-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.29-ingestion-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.30-work-receipt-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.31-change-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) FORNIX_BOOTSTRAP_KEY=$(FORNIX_BOOTSTRAP_KEY) FORNIX_REFERENCE_WORKDIR=$(FORNIX_REFERENCE_WORKDIR) scripts/test/v0.32-validation-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.33-policy-smokes.sh
	$(MAKE) smoke-local-cli

check: fmt-check test vet python-check docs-check package-check

smoke-local-cli: build
	scripts/test/v0.34-local-cli-smokes.sh

smoke-local-runtime: build
	$(DOCKER) build --tag $${FORNIX_LOCAL_IMAGE:-fornix-local:smoke} .
	FORNIX_LOCAL_IMAGE=$${FORNIX_LOCAL_IMAGE:-fornix-local:smoke} scripts/test/v0.35-local-runtime-smokes.sh

verify: check test-race build

hooks-install install-hooks:
	git config core.hooksPath .githooks
	@echo 'Installed Fornix hooks through core.hooksPath=.githooks'

hooks-uninstall uninstall-hooks:
	git config --unset core.hooksPath 2>/dev/null || true
	@echo 'Removed the repository-local hook override'

hooks-check:
	@test -x .githooks/pre-commit
	@test -x .githooks/pre-push
	@echo 'Fornix hooks are present and executable'

dev-up:
	docker compose --env-file .env up -d db

dev-up-ai:
	docker compose --env-file .env --profile ai up -d db ollama

dev-up-watcher:
	docker compose --env-file .env --profile app --profile watch up --build -d db fornix watcher

dev-run:
	docker compose --env-file .env --profile app up --build

dev-logs:
	docker compose --env-file .env --profile app --profile ai logs -f

dev-down:
	docker compose --env-file .env --profile app --profile ai down
