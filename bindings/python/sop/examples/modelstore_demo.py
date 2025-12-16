import os
import shutil
import sys
from dataclasses import dataclass

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

@dataclass
class MyModel:
    name: str
    version: str
    parameters: dict
    accuracy: float

def main():
    db_path = "model_store_demo_db"
    
    # Clean up previous run
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print(f"Initializing SOP Database at '{db_path}'...")
    ctx = Context()
    # Initialize the Database (Unified Mode)
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))

    # Start Transaction
    with db.begin_transaction(ctx) as trans:
        print("Transaction Started.")

        # Open a Model Store
        print("Opening Model Store 'experiment_1'...")
        store = db.open_model_store(ctx, trans, "experiment_1")

        # Create a sample model object
        model = MyModel(
            name="gpt-4-mini",
            version="1.0.0",
            parameters={"layers": 12, "heads": 8},
            accuracy=0.95
        )

        # Save the model
        print(f"Saving model '{model.name}' to category 'llm'...")
        store.save(ctx, category="llm", name=model.name, model=model)

        # List models in the category
        print("Listing models in category 'llm'...")
        models = store.list(ctx, category="llm")
        print(f"Found models: {models}")

        # Load the model back
        print(f"Loading model '{model.name}'...")
        loaded_data = store.get(ctx, category="llm", name=model.name)
        print(f"Loaded data: {loaded_data}")

        # Verify data
        if loaded_data['name'] == model.name and loaded_data['accuracy'] == model.accuracy:
            print("Verification Successful: Model data matches.")
        else:
            print("Verification Failed!")

        # Delete the model
        print(f"Deleting model '{model.name}'...")
        store.delete(ctx, category="llm", name=model.name)

        # Verify deletion
        models_after = store.list(ctx, category="llm")
        print(f"Models after deletion: {models_after}")

        if not models_after:
            print("Deletion Successful.")
        
        print("Committing Transaction...")

    # Clean up
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
    print("Demo completed successfully.")

if __name__ == "__main__":
    main()
