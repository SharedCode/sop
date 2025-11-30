import json
import uuid
from dataclasses import dataclass, asdict
from enum import Enum

from .. import call_go
from .. import context
from .model import ModelStore, ModelAction
from .vector import VectorStore, UsageMode, DBType

class Database:
    def __init__(self, storage_path: str = "", db_type: DBType = DBType.Standalone):
        self.ctx = context.Context()
        # We reuse ModelDBOptions structure on Go side which expects storage_path and db_type
        opts = {
            "storage_path": storage_path,
            "db_type": db_type.value
        }
        payload = json.dumps(opts)
        
        # Action NewModelDB (which is now NewDatabase)
        # We use manage_model_store because that's where NewDatabase is handled
        res = call_go.manage_model_store(self.ctx.id, ModelAction.NewModelDB.value, None, payload)
        try:
            self.id = uuid.UUID(res)
        except:
            raise Exception(res)

    def open_model_store(self, name: str) -> ModelStore:
        # Action OpenModelStore
        res = call_go.manage_model_store(self.ctx.id, ModelAction.OpenModelStore.value, str(self.id), name)
        try:
            store_id = uuid.UUID(res)
            return ModelStore(self.ctx, store_id)
        except:
            raise Exception(res)

    def open_vector_store(self, name: str) -> VectorStore:
        # Action 2 is OpenVectorStore in VectorAction
        # We use manage_vector_db because that's where OpenVectorStore is handled
        # We pass self.id (Database ID) as targetID. 
        # The Go side will check if it's a VectorDB or a Unified DB.
        res = call_go.manage_vector_db(self.ctx.id, 2, str(self.id), name)
        try:
            store_id = uuid.UUID(res)
            return VectorStore(store_id, self.ctx)
        except:
            raise Exception(res)
