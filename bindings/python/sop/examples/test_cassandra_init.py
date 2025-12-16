import sys
import os

# Add parent directory to path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sop.database import Cassandra

def main():
    print("--- Testing Cassandra Initialization ---")
    
    # Configuration for a local Cassandra instance
    config = {
        "cluster_hosts": ["localhost"],
        "consistency": 1, # LocalQuorum
        "connection_timeout": 5000,
        "replication_clause": "{'class':'SimpleStrategy', 'replication_factor':1}"
    }
    
    print(f"Initializing Cassandra with config: {config}")
    
    try:
        Cassandra.initialize(config)
        print("Cassandra initialized successfully (or failed with connection error if not running).")
        
        # We expect it to fail if Cassandra isn't running locally, but the call itself should be valid.
        # If the library wasn't updated correctly, we'd get an AttributeError or similar.
        
    except Exception as e:
        print(f"Initialization result: {e}")
        # Check if it's a connection error (expected) vs a binding error (unexpected)
        if "connection refused" in str(e) or "dial tcp" in str(e) or "failed to create cassandra session" in str(e):
            print("SUCCESS: Caught expected connection error (Cassandra not running).")
        else:
            print("FAILURE: Unexpected error type.")
            raise e
    finally:
        print("Closing connection...")
        try:
            Cassandra.close()
            print("Connection closed.")
        except Exception as e:
            print(f"Error closing connection: {e}")

if __name__ == "__main__":
    main()
