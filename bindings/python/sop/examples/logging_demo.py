import os
import sys

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context, Logger, LogLevel
from sop.ai import Database, DatabaseType, Item as VectorItem
from sop.database import DatabaseOptions

def main():
    # 1. Configure Logging
    # We'll log to a file named 'sop_app.log' with DEBUG level to see all details.
    log_file = "sop_app.log"
    print(f"Configuring logger to write to {log_file}...")
    Logger.configure(LogLevel.Debug, log_file)

    # 2. Initialize Context & Database
    ctx = Context()
    db_path = "data/logging_demo_db"
    
    print(f"Opening database at {db_path}...")
    # This operation will generate logs on the Go side
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))

    # 3. Perform Operations
    print("Starting transaction...")
    with db.begin_transaction(ctx) as tx:
        
        # Open a Vector Store
        print("Opening Vector Store...")
        vs = db.open_vector_store(ctx, tx, "demo_vectors")
        
        # Upsert an item
        print("Upserting item...")
        vs.upsert(ctx, VectorItem(
            id="item_1",
            vector=[0.5, 0.5, 0.5],
            payload={"description": "A test item"}
        ))
        
        # The commit happens automatically at the end of the 'with' block
        print("Committing transaction...")

    # 4. Verify Logs
    if os.path.exists(log_file):
        print(f"\nSuccess! Log file created at {log_file}.")
        print("First 5 lines of log:")
        with open(log_file, 'r') as f:
            for i, line in enumerate(f):
                if i >= 5: break
                print(line.strip())
    else:
        print("Error: Log file was not created.")

if __name__ == "__main__":
    main()
