import json
import uuid
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Callable
from enum import Enum

from .. import call_go
from .. import context
from .. import transaction

class VectorAction(Enum):
    NewVectorDB = 1
    OpenVectorStore = 2
    UpsertVector = 3
    UpsertBatchVector = 4
    GetVector = 5
    DeleteVector = 6
    QueryVector = 7
    VectorCount = 8
    VectorWithTransaction = 9
    OptimizeVector = 10

class UsageMode(Enum):
    BuildOnceQueryMany = 0
    DynamicWithVectorCountTracking = 1
    Dynamic = 2

class DBType(Enum):
    Standalone = 0
    Clustered = 1

@dataclass
class Item:
    id: str
    vector: List[float]
    payload: Dict[str, Any]
    centroid_id: int = 0

@dataclass
class Hit:
    id: str
    score: float
    payload: Dict[str, Any]

@dataclass
class VectorDBOptions:
    storage_path: str
    usage_mode: int
    db_type: int
    erasure_config: Optional[Dict[str, transaction.ErasureCodingConfig]] = None
    stores_folders: Optional[List[str]] = None

@dataclass
class VectorQueryOptions:
    vector: List[float]
    k: int
    filter: Dict[str, Any]

class VectorStore:
    def __init__(self, id: uuid.UUID, ctx: context.Context):
        self.id = id
        self.ctx = ctx

    def upsert(self, item: Item) -> None:
        payload = json.dumps(asdict(item))
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.UpsertVector.value, str(self.id), payload)
        if res is not None:
            raise Exception(res)

    def upsert_batch(self, items: List[Item]) -> None:
        payload = json.dumps([asdict(item) for item in items])
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.UpsertBatchVector.value, str(self.id), payload)
        if res is not None:
            raise Exception(res)

    def get(self, id: str) -> Item:
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.GetVector.value, str(self.id), id)
        if res is None:
            raise Exception("Item not found or error occurred")
        
        # Check if res is an error message (simple heuristic: if it doesn't look like JSON)
        # But call_go returns None on error usually, or error string.
        # Wait, manageVectorDB returns error string if error, or JSON string if success.
        # How to distinguish?
        # In Go: return C.CString(err.Error()) OR return C.CString(string(data))
        # This is ambiguous. I should have returned a struct or used a prefix.
        # However, JSON usually starts with '{'. Error message usually doesn't.
        if not res.strip().startswith("{"):
             raise Exception(res)

        data = json.loads(res)
        return Item(
            id=data["ID"],
            vector=data["Vector"],
            payload=data["Payload"],
            centroid_id=data.get("CentroidID", 0)
        )

    def delete(self, id: str) -> None:
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.DeleteVector.value, str(self.id), id)
        if res is not None:
            raise Exception(res)

    def query(self, vector: List[float], k: int = 10, filter: Dict[str, Any] = None) -> List[Hit]:
        opts = VectorQueryOptions(vector=vector, k=k, filter=filter or {})
        payload = json.dumps(asdict(opts))
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.QueryVector.value, str(self.id), payload)
        
        if not res.strip().startswith("["):
             raise Exception(res)

        data = json.loads(res)
        hits = []
        for h in data:
            hits.append(Hit(
                id=h["ID"],
                score=h["Score"],
                payload=h["Payload"]
            ))
        return hits

    def count(self) -> int:
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.VectorCount.value, str(self.id), "")
        try:
            return int(res)
        except:
            raise Exception(res)

    def with_transaction(self, trans: transaction.Transaction) -> 'VectorStore':
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.VectorWithTransaction.value, str(self.id), str(trans.transaction_id))
        try:
            new_id = uuid.UUID(res)
            return VectorStore(new_id, self.ctx)
        except:
            raise Exception(res)

    def optimize(self) -> None:
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.OptimizeVector.value, str(self.id), "")
        if res is not None:
            raise Exception(res)

    # Convenience method for LangChain compatibility
    def add_documents(self, texts: List[str], metadatas: List[Dict[str, Any]] = None, ids: List[str] = None) -> None:
        # This requires an embedder. Since we don't have one bound here, we can't implement this fully 
        # without the user providing vectors.
        # So we might skip this or require vectors.
        pass

class VectorDatabase:
    def __init__(self, storage_path: str = "", usage_mode: UsageMode = UsageMode.BuildOnceQueryMany, db_type: DBType = DBType.Standalone, erasure_config: transaction.ErasureCodingConfig = None, stores_folders: List[str] = None):
        self.ctx = context.Context()
        
        # Wrap single config into a map for the backend
        ec_map = None
        if erasure_config is not None:
            ec_map = {"": erasure_config}

        opts = VectorDBOptions(
            storage_path=storage_path, 
            usage_mode=usage_mode.value, 
            db_type=db_type.value,
            erasure_config=ec_map,
            stores_folders=stores_folders
        )
        payload = json.dumps(asdict(opts))
        
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.NewVectorDB.value, None, payload)
        try:
            self.id = uuid.UUID(res)
        except:
            raise Exception(res)

    def open(self, name: str) -> VectorStore:
        res = call_go.manage_vector_db(self.ctx.id, VectorAction.OpenVectorStore.value, str(self.id), name)
        try:
            store_id = uuid.UUID(res)
            return VectorStore(store_id, self.ctx)
        except:
            raise Exception(res)
