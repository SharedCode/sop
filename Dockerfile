# syntax=docker/dockerfile:1

# ==============================================================================
# Stage 1: Builder
# ==============================================================================
FROM golang:1.26.8-alpine AS builder

# Install build dependencies, CA certificates, and tzdata
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /src

# Leverage Docker layer caching for dependencies.
# Copy workspace and module definitions first before the source tree.
COPY go.work go.work.sum* ./
COPY go.mod go.sum ./
COPY adapters/cassandra/go.mod adapters/cassandra/go.sum* ./adapters/cassandra/
COPY adapters/redis/go.mod adapters/redis/go.sum* ./adapters/redis/
COPY ai/go.mod ai/go.sum* ./ai/
COPY incfs/go.mod incfs/go.sum* ./incfs/
COPY infs/go.mod infs/go.sum* ./infs/
COPY jsondb/go.mod jsondb/go.sum* ./jsondb/
COPY search/go.mod search/go.sum* ./search/

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy full application source tree
COPY . .

# Compile statically linked binary with stripped symbols and debug information
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/sop-server ./tools/httpserver

# ==============================================================================
# Stage 2: Integration Test Runner (Optional Target: --target test)
# ==============================================================================
FROM golang:1.26.8-alpine AS test
RUN apk add --no-cache redis
WORKDIR /app
COPY --from=builder /src /app
RUN mkdir -p /var/lib/sop
ENV datapath=/var/lib/sop
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
CMD ["docker-entrypoint.sh"]

# ==============================================================================
# Stage 3: Minimal Production Runtime
# ==============================================================================
FROM alpine:3.21 AS runtime

# Install certificates for secure TLS and timezone data
RUN apk add --no-cache ca-certificates tzdata

# Create unprivileged system user and group (UID/GID 10001)
RUN addgroup -g 10001 -S sop && \
    adduser -u 10001 -S sop -G sop && \
    mkdir -p /var/lib/sop && \
    chown -R sop:sop /var/lib/sop

# Copy statically compiled binary from builder stage
COPY --from=builder /out/sop-server /usr/local/bin/sop-server

# Drop root privileges and configure execution environment
USER 10001:10001
WORKDIR /var/lib/sop
ENV datapath=/var/lib/sop

# Expose HTTP service port
EXPOSE 8080

# Handle container lifecycle signals cleanly
STOPSIGNAL SIGTERM

# Health check against built-in HTTP health endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/api/health || exit 1

ENTRYPOINT ["sop-server"]
CMD ["-database", "/var/lib/sop", "-port", "8080", "-open-browser=false"]
