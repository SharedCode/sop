# Makefile for SOP Agent API

.PHONY: help openapi openapi-install openapi-serve test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

openapi-install: ## Install swag CLI for OpenAPI generation
	@echo "Installing swag..."
	@go install github.com/swaggo/swag/cmd/swag@latest
	@echo "✓ swag installed"

openapi: ## Generate OpenAPI spec from Go annotations
	@echo "Generating OpenAPI specification..."
	@swag init \
		--generalInfo api_openapi.go \
		--parseDependency \
		--parseInternal \
		--output ./docs \
		--outputTypes yaml,json \
		--instanceName agent
	@echo "✓ OpenAPI spec generated: ./docs/swagger.yaml"
	@echo "✓ OpenAPI spec generated: ./docs/swagger.json"

openapi-serve: openapi ## Generate and serve OpenAPI docs
	@echo "Serving OpenAPI docs at http://localhost:8080"
	@echo "Press Ctrl+C to stop"
	@docker run --rm -p 8080:8080 \
		-v $(PWD)/docs/swagger.yaml:/usr/share/nginx/html/swagger.yaml \
		-e SWAGGER_JSON=/usr/share/nginx/html/swagger.yaml \
		swaggerapi/swagger-ui

test: ## Run tests
	@go test -v ./...

lint: ## Run linter
	@golangci-lint run ./...

build: ## Build package
	@go build ./...

clean: ## Clean generated files
	@rm -rf docs/
	@echo "✓ Cleaned generated files"
