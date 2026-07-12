.PHONY: help test test-race test-unit test-integration test-run

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

test: ## Run all unit tests
	go test -race -v ./...

test-integration: ## Run integration tests
	go test -tags=integration -race -v ./...

# Run a specific test by name, race on, repeated N times.
# Usage: make test-run TEST=TestConcurrentNoDoubleClaim
#        make test-run TEST=TestConcurrentNoDoubleClaim COUNT=20 TAGS=integration
COUNT ?= 10
TAGS ?=
test-run: ## Run one test by name: make test-run TEST=Name [COUNT=10] [TAGS=integration]
	go test -race -v -count=$(COUNT) -run $(TEST) $(if $(TAGS),-tags=$(TAGS),) ./...

# go test -tags=integration -race -v -count=10 -run TestConcurrentNoDoubleClaim ./...
