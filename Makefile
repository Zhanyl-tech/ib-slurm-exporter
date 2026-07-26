BINARY  := ib-slurm-exporter
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: help build test vet lint demo demo-once demo-layouts clean

help: ## Show this help
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Build the binary
	go build -ldflags '$(LDFLAGS)' -o bin/$(BINARY) ./cmd/$(BINARY)

test: ## Run tests against the synthetic tree
	go test -race -cover ./...

vet: ## go vet
	go vet ./...

lint: vet test ## vet + test

demo: build ## Serve the synthetic cluster on :9836
	@echo "→ synthetic cluster, no InfiniBand required"
	@echo "→ http://localhost:9836/metrics"
	@./bin/$(BINARY) --demo

demo-once: build ## Dump the synthetic exposition and exit
	@./bin/$(BINARY) --demo --once

demo-layouts: build ## Show all three cgroup layouts side by side
	@for l in v1 v2 v2-sluid; do \
		printf '\n\033[1m── cgroup layout: %s ──\033[0m\n' "$$l"; \
		./bin/$(BINARY) --demo --demo-layout $$l --once 2>/dev/null \
			| grep -E 'layout_info|device_jobs|unattributed|unresolved' ; \
	done; echo

clean:
	rm -rf bin
