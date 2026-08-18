SHELL := /bin/sh

GO_IMAGE ?= golang:1.25.13
GO_RUN ?= docker run --rm -u "$$(id -u):$$(id -g)" -e HOME=/tmp -e GOPATH=/tmp/go -e GOCACHE=/tmp/go-build -e GOMODCACHE=/tmp/go-mod -e FORNIX_TEST_PG_DSN="$(FORNIX_TEST_PG_DSN)" -v "$(CURDIR):/workspace" -w /workspace $(GO_IMAGE)
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
FORNIX_KEY ?= $(shell sed -n 's/^FORNIX_KEY=//p' .env 2>/dev/null | head -n 1)
PROJECTION_PG_DSN ?= postgres://fornix:fornix-dev-only@host.docker.internal:55433/fornix?sslmode=disable
FORNIX_TEST_PG_DSN ?=

.PHONY: fmt test vet build python-install python-check check smoke smoke-events smoke-projection smoke-leases smoke-tasks smoke-retrieval smoke-provenance dev-up dev-up-ai dev-up-watcher dev-run dev-logs dev-down

fmt:
	$(GOFMT_CMD) -w $(GO_FILES)

test:
	$(GO_CMD) test ./...

vet:
	$(GO_CMD) vet ./...

build:
	mkdir -p bin
	$(GO_CMD) build -trimpath -o bin/fornix ./cmd/fornix
	$(GO_CMD) build -trimpath -o bin/fornix-watcher ./cmd/fornix-watcher

python-install:
	$(PYTHON) -m venv $(PYTHON_VENV)
	$(PYTHON_ENV_BIN) -m pip install --upgrade pip
	$(PYTHON_ENV_BIN) -m pip install --requirement scripts/requirements.txt

python-check:
	$(PYTHON_CHECK_BIN) -m py_compile scripts/*.py
	$(PYTHON_CHECK_BIN) -m compileall -q scripts

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

smoke:
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) PYTHON_BIN=$(PYTHON_BIN) scripts/test/v0.10-smokes.sh
	FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.11-event-smokes.sh
	FORNIX_PROJECTION_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.12-projection-smokes.sh
	FORNIX_LEASE_PG_DSN=$(PROJECTION_PG_DSN) scripts/test/v0.13-lease-smokes.sh
	FORNIX_TASK_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.14-task-smokes.sh
	FORNIX_RETRIEVAL_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.15-retrieval-smokes.sh
	FORNIX_PROVENANCE_PG_DSN=$(PROJECTION_PG_DSN) FORNIX_URL=$(FORNIX_URL) FORNIX_KEY=$(FORNIX_KEY) scripts/test/v0.16-provenance-smokes.sh

check: test vet python-check

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
