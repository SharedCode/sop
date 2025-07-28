FROM golang:1.24.3-alpine AS build-and-test

# Install Redis server for Alpine
# Note: 'redis' package on Alpine provides both redis-server and redis-cli
RUN apk add --no-cache redis

# Set up your Go application and test environment
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of your application code and tests
COPY . .

# Build your Go application (if it's a runnable binary)
# This step is often needed before running tests or the main app
# If your tests are purely unit tests that don't need a compiled binary,
# you might skip 'go build' here, but it's good practice for a full app build.
#RUN go build -o myapp .

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
#   This will run the integration tests in inredfs package that requires Redis which is provided in docker image.
