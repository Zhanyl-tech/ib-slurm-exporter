BINARY  := ib-slurm-exporter
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: help build test vet fmt-check tidy-check lint demo demo-once demo-layouts demo-check clean

help: ## Show this help
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Build the binary
	go build -ldflags '$(LDFLAGS)' -o bin/$(BINARY) ./cmd/$(BINARY)

test: ## Run tests against the synthetic tree
	go test -race -cover ./...

vet: ## go vet
	go vet ./...

fmt-check: ## Fail if any file is not gofmt-formatted
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

tidy-check: ## Fail if go.mod/go.sum are not tidy
	go mod tidy -diff

lint: fmt-check tidy-check vet test ## fmt + tidy + vet + test

demo: build ## Serve the synthetic node on :9836
	@echo "→ synthetic node, no InfiniBand required"
	@echo "→ http://localhost:9836/metrics"
	@./bin/$(BINARY) --demo

demo-once: build ## Print the synthetic exposition and exit
	@./bin/$(BINARY) --demo --once

demo-layouts: build ## Show all three cgroup layouts side by side
	@set -e; for l in v1 v2 v2-sluid; do \
		printf '\n\033[1m── cgroup layout: %s ──\033[0m\n' "$$l"; \
		out=$$(./bin/$(BINARY) --demo --demo-layout $$l --once 2>/dev/null); \
		printf '%s\n' "$$out" | grep -E '^ib_slurm_(cgroup_layout_info|device_jobs|unattributed|unresolved)'; \
	done; echo

demo-check: build ## Assert the demo's documented output (used by CI)
	./scripts/demo-check.sh bin/$(BINARY)

clean:
	rm -rf bin
