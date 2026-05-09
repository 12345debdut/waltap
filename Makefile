.PHONY: build test vet clean run-cdc run-proxy docker-up docker-down

# Build both binaries
build:
	go build -o pgcdc ./cmd/pgcdc
	go build -o pgproxy ./cmd/pgproxy

# Run all tests with race detector
test:
	go test -race ./...

# Run tests with coverage report
test-cover:
	go test -race -cover ./...

# Run go vet
vet:
	go vet ./...

# Clean build artifacts
clean:
	rm -f pgcdc pgproxy

# Start the CDC pipeline (stdout sink, default settings)
run-cdc: build
	./pgcdc --sink stdout

# Start the read/write split proxy
run-proxy: build
	./pgproxy --listen :5434

# Start all Docker services (Postgres primary + replica + Kafka)
docker-up:
	docker compose up -d

# Stop all Docker services
docker-down:
	docker compose down

# Start only Postgres (primary + replica, no Kafka)
docker-up-pg:
	docker compose up -d postgres postgres-replica

# Show CDC help
help-cdc: build
	./pgcdc --help

# Show proxy help
help-proxy: build
	./pgproxy --help
