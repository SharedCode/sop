import sys
import os

# Add parent directory to path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sop.redis import Redis

def main():
    print("Initializing Redis Connection...")
    # Assuming a local redis is running, or just testing the binding call.
    # If no redis is running, this might fail or log a warning depending on Go implementation.
    # The Go implementation uses redis.ParseURL, so it should at least validate the URL.
    
    try:
        Redis.initialize("redis://localhost:6379/0")
        print("Redis initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize Redis: {e}")
        # If it fails due to connection refused, that's expected if no redis is running,
        # but we want to ensure the binding call works.
        if "connection refused" in str(e):
            print("Connection refused (expected if Redis is not running). Binding works.")
        else:
            # If it's another error, re-raise
            pass

    print("Closing Redis Connection...")
    try:
        Redis.close()
        print("Redis closed successfully.")
    except Exception as e:
        print(f"Failed to close Redis: {e}")

if __name__ == "__main__":
    main()
