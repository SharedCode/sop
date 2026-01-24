FROM golang:1.24.3-alpine AS build-and-test

# Install Redis server for Alpine
# Note: 'redis' package on Alpine provides both redis-server and redis-cli
RUN apk add --no-cache redis

# Set up your Go application and test environment
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./

# Copy the rest of your application code and tests
COPY . .

# Download dependencies
# Moved after COPY . . because go.mod references local module ./infs
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
