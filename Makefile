# SPDX-License-Identifier: MIT
# go-hamqtt — developer Makefile
#
# Tabs are required by GNU make. The whitespace rules below pin sane
# shell behaviour so a failing recipe step actually aborts the target
# instead of silently moving on.

SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

GO            ?= go
GOFUMPT       ?= gofumpt
GOLANGCI_LINT ?= golangci-lint
COVER_MIN     ?= 80
MODULE        := github.com/SukramJ/go-hamqtt

export CGO_ENABLED := 0

.PHONY: help
help: ## show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: setup
setup: ## install developer tooling (gofumpt, golangci-lint)
	$(GO) install mvdan.cc/gofumpt@latest
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

.PHONY: test
test: ## run the full test suite with race detector
	CGO_ENABLED=1 $(GO) test -race -count=1 -timeout=120s ./...

.PHONY: test-cover
test-cover: ## run tests + coverage report
	CGO_ENABLED=1 $(GO) test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -20

.PHONY: vet
vet: ## run go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## format with gofumpt (writes in place)
	$(GOFUMPT) -w .

.PHONY: fmt-check
fmt-check: ## fail when sources are not gofumpt-clean
	@diff=$$($(GOFUMPT) -l .); \
	if [ -n "$$diff" ]; then \
	  echo "gofumpt would rewrite:"; echo "$$diff"; exit 1; \
	fi

.PHONY: lint
lint: ## run golangci-lint
	$(GOLANGCI_LINT) run ./...

.PHONY: tidy
tidy: ## sync go.mod
	$(GO) mod tidy

.PHONY: check
check: vet fmt-check lint test ## the pre-commit / pre-push gate

.PHONY: cover-check
cover-check: ## fail when a package drops below COVER_MIN
	@CGO_ENABLED=1 $(GO) test -count=1 -covermode=atomic -coverprofile=coverage.out ./... >/dev/null
	@$(GO) tool cover -func=coverage.out | awk -v min=$(COVER_MIN) '\
	  /^total:/ { next } \
	  { pkg=$$1; sub(/\/[^\/]*$$/, "", pkg); cov[pkg]+=$$3+0; n[pkg]++ } \
	  END { bad=0; for (p in cov) { avg=cov[p]/n[p]; \
	    if (avg < min) { printf "FAIL %s (%.1f%% < %d%%)\n", p, avg, min; bad=1 } \
	    else printf "ok   %s (%.1f%% >= %d%%)\n", p, avg, min } exit bad }'

.PHONY: clean
clean: ## remove build artefacts
	rm -f coverage.out
