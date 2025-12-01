import json
import uuid
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from enum import Enum

from .. import call_go
from .. import context
from .. import transaction

class ModelAction(Enum):
    NewBTreeModelStore = 1
    NewModelDB = 2
    OpenModelStore = 3
    SaveModel = 4
    LoadModel = 5
    ListModels = 6
    DeleteModel = 7
    CloseModelDB = 8

@dataclass
class Model:
    id: str
    algorithm: str
    hyperparameters: Dict[str, Any]
    parameters: List[float]
    metrics: Dict[str, float]
    is_active: bool

class ModelStore:
    def __init__(self, id: uuid.UUID, transaction_id: uuid.UUID):
        self.id = id
        self.transaction_id = transaction_id

    def _get_target_id(self) -> str:
        return json.dumps({
            "id": str(self.id),
            "transaction_id": str(self.transaction_id)
        })

    @staticmethod
    def open_btree_store(ctx: context.Context, trans: transaction.Transaction) -> 'ModelStore':
        opts = {"transaction_id": str(trans.transaction_id)}
        payload = json.dumps(opts)
        res = call_go.manage_model_store(ctx.id, ModelAction.NewBTreeModelStore.value, None, payload)
        try:
            id = uuid.UUID(res)
            return ModelStore(id, trans.transaction_id)
        except:
            raise Exception(res)

    def save(self, ctx: context.Context, category: str, name: str, model: Any) -> None:
        if hasattr(model, "__dataclass_fields__"):
            model_data = asdict(model)
        else:
            model_data = model

        item = {
            "category": category,
            "name": name,
            "model": model_data
        }
        payload = json.dumps(item)
        res = call_go.manage_model_store(ctx.id, ModelAction.SaveModel.value, self._get_target_id(), payload)
        if res is not None:
            raise Exception(res)

    def get(self, ctx: context.Context, category: str, name: str) -> Any:
        item = {
            "category": category,
            "name": name
        }
        payload = json.dumps(item)
        res = call_go.manage_model_store(ctx.id, ModelAction.LoadModel.value, self._get_target_id(), payload)
        if res is None:
             raise Exception("Model not found or error occurred")
        
        if not res.strip().startswith("{") and not res.strip().startswith("["):
             # It might be a primitive value JSON encoded, or error string.
             # But manageModelStore returns JSON.
             pass

        try:
            data = json.loads(res)
            # Try to convert to Model if it fits? 
            # For now, just return data to be flexible as Go store is generic.
            return data
        except:
            raise Exception(res)

    def delete(self, ctx: context.Context, category: str, name: str) -> None:
        item = {
            "category": category,
            "name": name
        }
        payload = json.dumps(item)
        res = call_go.manage_model_store(ctx.id, ModelAction.DeleteModel.value, self._get_target_id(), payload)
        if res is not None:
            raise Exception(res)

    def list(self, ctx: context.Context, category: str) -> List[str]:
        res = call_go.manage_model_store(ctx.id, ModelAction.ListModels.value, self._get_target_id(), category)
        if res.strip() == "null":
            return []
        if not res.strip().startswith("["):
             raise Exception(res)
        return json.loads(res)
