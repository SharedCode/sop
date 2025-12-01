import json
import uuid
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from enum import Enum

from . import call_go
from . import context
from . import transaction
from .transaction import DBType
from .ai.model import ModelStore, ModelAction
from .ai.vector import VectorStore, UsageMode, VectorStoreTransportOptions, VectorStoreConfig

class Database:
    def __init__(self, ctx: context.Context, storage_path: str = "", db_type: DBType = DBType.Standalone, 
                 erasure_config: Optional[Dict[str, transaction.ErasureCodingConfig]] = None,
                 stores_folders: Optional[List[str]] = None):
        
        self.storage_path = storage_path # Store for reference
        
        # We reuse ModelDBOptions structure on Go side which expects storage_path and db_type
        opts = {
            "storage_path": storage_path,
            "db_type": db_type.value
        }
        
        if erasure_config:
            # Convert ErasureCodingConfig objects to dicts if needed, or rely on asdict/json serialization
            # Assuming transaction.ErasureCodingConfig is a dataclass
            opts["erasure_config"] = {k: asdict(v) for k, v in erasure_config.items()}
            
        if stores_folders:
            opts["stores_folders"] = stores_folders

        payload = json.dumps(opts)
        
        # Action NewModelDB (which is now NewDatabase)
        # We use manage_model_store because that's where NewDatabase is handled
        res = call_go.manage_model_store(ctx.id, ModelAction.NewModelDB.value, None, payload)
        try:
            self.id = uuid.UUID(res)
        except:
            raise Exception(res)

    def begin_transaction(self, ctx: context.Context, mode: int = 1, options: Optional[transaction.TransactionOptions] = None) -> transaction.Transaction:
        # We can use manage_vector_db BeginTransaction (Action 9) which works on dbRegistry
        if options:
            payload = json.dumps(asdict(options))
        else:
            payload = str(mode)
            
        res = call_go.manage_vector_db(ctx.id, 9, str(self.id), payload)
        try:
            tid = uuid.UUID(res)
            return transaction.Transaction(ctx, id=tid, begun=True)
        except:
            raise Exception(res)

    def open_model_store(self, ctx: context.Context, trans: transaction.Transaction, name: str) -> ModelStore:
        # Action NewBTreeModelStore (Action 1)
        # We use NewBTreeModelStore because OpenModelStore is not supported/deprecated
        opts = {
            "transaction_id": str(trans.transaction_id),
            "path": name # ModelStoreOptions uses "path" for name/path?
        }
        # Check ModelStoreOptions in Go:
        # type ModelStoreOptions struct {
        # 	Path          string `json:"path"`
        # 	TransactionID string `json:"transaction_id"`
        # }
        # So yes, "path" is used.
        
        payload = json.dumps(opts)
        res = call_go.manage_model_store(ctx.id, ModelAction.NewBTreeModelStore.value, None, payload)
        try:
            store_id = uuid.UUID(res)
            return ModelStore(store_id, trans.transaction_id)
        except:
            raise Exception(res)

    def open_vector_store(self, ctx: context.Context, trans: transaction.Transaction, name: str) -> VectorStore:
        # Action 2 is OpenVectorStore in VectorAction
        config = VectorStoreConfig(usage_mode=UsageMode.Dynamic.value, content_size=0)
        opts = VectorStoreTransportOptions(
            transaction_id=str(trans.transaction_id),
            name=name,
            config=config,
            storage_path=self.storage_path
        )
        payload = json.dumps(asdict(opts))

        res = call_go.manage_vector_db(ctx.id, 2, str(self.id), payload)
        try:
            store_id = uuid.UUID(res)
            return VectorStore(store_id, trans.transaction_id)
        except:
            raise Exception(res)

    def close(self, ctx: context.Context) -> None:
        call_go.manage_model_store(ctx.id, ModelAction.CloseModelDB.value, str(self.id), "")
