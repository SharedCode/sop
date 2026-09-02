FROM golang:1.26.8-alpine AS build-and-test

# Install Redis server for Alpine
# Note: 'redis' package on Alpine provides both redis-server and redis-cli
RUN apk add --no-cache redis

# Set up your Go application and test environment
WORKDIR /app

# Copy the application code and tests. go.work's local `use` directives
# (./infs, ./ai, etc.) mean go.mod resolution needs the full source tree,
# so copying manifests separately first would buy no cache benefit here.
COPY . .

# Download dependencies
RUN go mod download

# Create the data path folder & the env var.
RUN mkdir -p /var/lib/sop
ENV datapath=/var/lib/sop

# Create an entrypoint script to start Redis and then run tests
COPY docker-entrypoint.sh /usr/local/bin/

# Make the entrypoint script executable
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
CMD ["docker-entrypoint.sh"]

# Sample commands to build then run docker image:
# docker build -t mydi .
# docker run mydi
#   This will run the integration tests in infs package that requires Redis which is provided in docker image.
