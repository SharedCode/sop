import json
import uuid
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Callable
from enum import Enum

from .. import call_go
from .. import context
from .. import transaction
from ..transaction import DBType

KEY_ID = "ID"
KEY_VECTOR = "Vector"
KEY_PAYLOAD = "Payload"
KEY_CENTROID_ID = "CentroidID"
KEY_SCORE = "Score"
KEY_META_ID = "id"
KEY_META_TRANSACTION_ID = "transaction_id"

class VectorAction(Enum):
    # NewVectorDB = 1
    OpenVectorStore = 2
    UpsertVector = 3
    UpsertBatchVector = 4
    GetVector = 5
    DeleteVector = 6
    QueryVector = 7
    VectorCount = 8
    # BeginTransaction = 9
    OptimizeVector = 10
    # CloseVectorDB = 11

class UsageMode(Enum):
    BuildOnceQueryMany = 0
    DynamicWithVectorCountTracking = 1
    Dynamic = 2

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

@dataclass
class VectorStoreConfig:
    usage_mode: int
    content_size: int

@dataclass
class VectorStoreTransportOptions:
    transaction_id: str
    name: str
    config: VectorStoreConfig
    storage_path: str = ""

class VectorStore:
    def __init__(self, id: uuid.UUID, transaction_id: uuid.UUID):
        self.id = id
        self.transaction_id = transaction_id

    def _get_target_id(self) -> str:
        return json.dumps({
            KEY_META_ID: str(self.id),
            KEY_META_TRANSACTION_ID: str(self.transaction_id)
        })

    def upsert(self, ctx: context.Context, item: Item) -> None:
        payload = json.dumps(asdict(item))
        res = call_go.manage_vector_db(ctx.id, VectorAction.UpsertVector.value, self._get_target_id(), payload)
        if res is not None:
            raise Exception(res)

    def upsert_batch(self, ctx: context.Context, items: List[Item]) -> None:
        payload = json.dumps([asdict(item) for item in items])
        res = call_go.manage_vector_db(ctx.id, VectorAction.UpsertBatchVector.value, self._get_target_id(), payload)
        if res is not None:
            raise Exception(res)

    def get(self, ctx: context.Context, id: str) -> Item:
        res = call_go.manage_vector_db(ctx.id, VectorAction.GetVector.value, self._get_target_id(), id)
        if res is None:
            raise Exception("Item not found or error occurred")
        
        if not res.strip().startswith("{"):
             raise Exception(res)

        data = json.loads(res)
        return Item(
            id=data[KEY_ID],
            vector=data[KEY_VECTOR],
            payload=data[KEY_PAYLOAD],
            centroid_id=data.get(KEY_CENTROID_ID, 0)
        )

    def delete(self, ctx: context.Context, id: str) -> None:
        res = call_go.manage_vector_db(ctx.id, VectorAction.DeleteVector.value, self._get_target_id(), id)
        if res is not None:
            raise Exception(res)

    def query(self, ctx: context.Context, vector: List[float], k: int = 10, filter: Dict[str, Any] = None) -> List[Hit]:
        opts = VectorQueryOptions(vector=vector, k=k, filter=filter or {})
        payload = json.dumps(asdict(opts))
        res = call_go.manage_vector_db(ctx.id, VectorAction.QueryVector.value, self._get_target_id(), payload)
        
        if not res.strip().startswith("["):
             raise Exception(res)

        data = json.loads(res)
        hits = []
        for h in data:
            hits.append(Hit(
                id=h[KEY_ID],
                score=h[KEY_SCORE],
                payload=h[KEY_PAYLOAD]
            ))
        return hits

    def count(self, ctx: context.Context) -> int:
        res = call_go.manage_vector_db(ctx.id, VectorAction.VectorCount.value, self._get_target_id(), "")
        try:
            return int(res)
        except:
            raise Exception(res)

    def optimize(self, ctx: context.Context) -> None:
        res = call_go.manage_vector_db(ctx.id, VectorAction.OptimizeVector.value, self._get_target_id(), "")
        if res is not None:
            raise Exception(res)

