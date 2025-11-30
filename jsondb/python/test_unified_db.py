import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sop.ai.database import Database, DBType
from sop.ai.vector import Item

def test_unified_db():
    print("Initializing Unified Database...")
    db = Database(storage_path="/tmp/sop_unified_test", db_type=DBType.Standalone)
    
    print("Opening Vector Store 'finance'...")
    vs = db.open_vector_store("finance")
    
    print("Upserting vector...")
    vs.upsert(Item(id="vec1", vector=[0.1, 0.2], payload={"info": "finance data"}))
    
    print("Querying vector...")
    hits = vs.query([0.1, 0.2], k=1)
    print(f"Hits: {hits}")
    assert len(hits) > 0
    assert hits[0].id == "vec1"
    
    print("Opening Model Store 'finance'...")
    ms = db.open_model_store("finance")
    
    print("Saving model...")
    ms.save("regression", "model1", {"type": "linear_regression", "weights": [1.0, 2.0]})
    
    print("Loading model...")
    model = ms.get("regression", "model1")
    print(f"Model: {model}")
    assert model["type"] == "linear_regression"

    print("Listing models in 'regression' category...")
    models = ms.list("regression")
    print(f"Models: {models}")
    assert "model1" in models
    
    print("Unified Database Test Passed!")

if __name__ == "__main__":
    test_unified_db()
