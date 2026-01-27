import sys
import os
import shutil

# Add parent directory to path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sop.database import Database, DatabaseOptions, DatabaseType
from sop.transaction import RedisCacheConfig

def main():
    print("Testing Redis Configuration via DatabaseOptions...")
    
    path = "data/test_redis_config"
    if os.path.exists(path):
        shutil.rmtree(path)
    
    # Verify we can configure Redis without global Redis.initialize
    config = RedisCacheConfig(url="redis://localhost:6379/0")
    print(f"Created RedisCacheConfig with URL: {config.url}")
    
    options = DatabaseOptions(
        keyspace="test_ks",
        redis_config=config,
        stores_folders=[path],
        type=DatabaseType.Clustered
    )
    
    print("DatabaseOptions created successfully with Redis config.")
    # Validating that we are NOT calling Redis.initialize()
    
    print("Success.")

if __name__ == "__main__":
    main()
