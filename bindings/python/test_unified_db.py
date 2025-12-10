import sys
import os
import shutil
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions
from sop.ai.vector import Item
from sop import Context

def test_unified_db():
    print("Initializing Unified Database...")
    path = f"/tmp/sop_unified_test_{int(time.time())}"
    if os.path.exists(path):
        shutil.rmtree(path)

    ctx = None
    db = None

    try:
        ctx = Context()
        db = Database(DatabaseOptions(stores_folders=[path], type=DatabaseType.Standalone))
        
        print("Opening Vector Store 'finance'...")
        tx = db.begin_transaction(ctx)
        vs = db.open_vector_store(ctx, tx, "finance")
        
        print("Upserting vector...")
        vs.upsert(ctx, Item(id="vec1", vector=[0.1, 0.2], payload={"info": "finance data"}))
        
        print("Querying vector...")
        hits = vs.query(ctx, [0.1, 0.2], k=1)
        print(f"Hits: {hits}")
        assert len(hits) > 0
        assert hits[0].id == "vec1"
        
        tx.commit(ctx)
        
        print("Opening Model Store 'finance'...")
        tx2 = db.begin_transaction(ctx)
        ms = db.open_model_store(ctx, tx2, "finance")
        
        print("Saving model...")
        ms.save(ctx, "regression", "model1", {"type": "linear_regression", "weights": [1.0, 2.0]})
        
        print("Loading model...")
        model = ms.get(ctx, "regression", "model1")
        print(f"Model: {model}")
        assert model["type"] == "linear_regression"

        print("Listing models in 'regression' category...")
        models = ms.list(ctx, "regression")
        print(f"Models: {models}")
        assert "model1" in models
        
        tx2.commit(ctx)
        print("Unified Database Test Passed!")
    finally:
        # Cleanup B-Trees from L2 Cache to prevent stale data in future runs
        if db and ctx:
            try:
                # Vector Store Cleanup
                try:
                    db.remove_vector_store(ctx, "finance")
                except Exception as e:
                    print(f"Error removing vector store: {e}")
                
                # Model Store Cleanup
                try:
                    db.remove_model_store(ctx, "finance")
                except Exception as e:
                    print(f"Error removing model store: {e}")

            except Exception as e:
                print(f"Error during cleanup: {e}")

        if os.path.exists(path):
            shutil.rmtree(path)

if __name__ == "__main__":
    test_unified_db()
