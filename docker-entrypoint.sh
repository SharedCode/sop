#!/bin/sh

set -e # Exit immediately if a command exits with a non-zero status

# Start Redis in the background
echo "Starting Redis server..."
# Bind to 127.0.0.1 for internal access within the same container
redis-server --port 6379 --bind 127.0.0.1 &
REDIS_PID=$! # Store the PID of the Redis server process

# Wait for Redis to be ready (e.g., by trying to connect to it)
echo "Waiting for Redis to be ready..."Hi Sho
until redis-cli -h 127.0.0.1 -p 6379 ping; do
    echo "Redis is unavailable - sleeping"
    sleep 1
done
echo "Redis is up and running!"

echo $datapath

# Now, run your Go tests
echo "Running Go tests..."
go test -v ./inredfs/integrationtests/

# Optional: Clean up Redis process if tests finish successfully
# In a test container, the container will exit after tests, so this might not be strictly necessary,
# but it's good practice for general multi-process management.
kill $REDIS_PID
echo "Tests finished and Redis stopped."

# Exit with the status of the test command
exit $?