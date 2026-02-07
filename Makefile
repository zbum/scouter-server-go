# Project variables
BINARY_NAME := scouter-server
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

# Directories
DIST_DIR := dist

# Go commands
GOCMD := go
GOBUILD := $(GOCMD) build
GOTEST := $(GOCMD) test
GOFMT := gofmt
GOMOD := $(GOCMD) mod

.PHONY: all build clean test lint fmt run build-all help tidy

all: clean build

## Build commands
build: ## Build the binary
	@mkdir -p $(DIST_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(DIST_DIR)/$(BINARY_NAME) ./cmd/scouter-server

run: build ## Build and run
	./$(DIST_DIR)/$(BINARY_NAME)

clean: ## Remove build artifacts
	@rm -rf $(DIST_DIR)

## Development commands
test: ## Run tests with coverage
	$(GOTEST) -v -race -cover ./...

lint: ## Run linter
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...

fmt: ## Format code
	$(GOFMT) -s -w .

tidy: ## Tidy go.mod
	$(GOMOD) tidy

## Cross-compilation
build-all: clean ## Build for all platforms
	@mkdir -p $(DIST_DIR)
	@echo "Building for linux/amd64..."
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(DIST_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/scouter-server
	@echo "Building for linux/arm64..."
	GOOS=linux GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(DIST_DIR)/$(BINARY_NAME)-linux-arm64 ./cmd/scouter-server
	@echo "Building for darwin/amd64..."
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(DIST_DIR)/$(BINARY_NAME)-darwin-amd64 ./cmd/scouter-server
	@echo "Building for darwin/arm64..."
	GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(DIST_DIR)/$(BINARY_NAME)-darwin-arm64 ./cmd/scouter-server
	@echo "Building for windows/amd64..."
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(DIST_DIR)/$(BINARY_NAME)-windows-amd64.exe ./cmd/scouter-server
	@echo "Build complete! Binaries are in $(DIST_DIR)/"

## Help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
