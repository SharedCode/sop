import json
import uuid
from dataclasses import asdict

from .. import call_go
from .. import context
from .. import transaction
from ..database import Database as BaseDatabase, DatabaseAction
from .model import ModelStore
from .vector import VectorStore, UsageMode, VectorStoreTransportOptions, VectorStoreConfig

class Database(BaseDatabase):
    """
    Database extends the core SOP Database to include AI capabilities 
    like Vector Stores and Model Stores.
    """

    def open_model_store(self, ctx: context.Context, trans: transaction.Transaction, name: str) -> ModelStore:
        # Action OpenModelStore (Action 5)
        opts = {
            "transaction_id": str(trans.transaction_id),
            "path": name
        }
        
        payload = json.dumps(opts)
        # Pass self.id as targetID so Go can find the Database instance
        res = call_go.manage_database(ctx.id, DatabaseAction.OpenModelStore.value, str(self.id), payload)
        try:
            store_id = uuid.UUID(res)
            return ModelStore(store_id, trans.transaction_id)
        except:
            raise Exception(res)

    def open_vector_store(self, ctx: context.Context, trans: transaction.Transaction, name: str) -> VectorStore:
        # Action OpenVectorStore (Action 6)
        config = VectorStoreConfig(usage_mode=UsageMode.Dynamic.value, content_size=0)
        opts = VectorStoreTransportOptions(
            transaction_id=str(trans.transaction_id),
            name=name,
            config=config,
            storage_path=self.storage_path
        )
        payload = json.dumps(asdict(opts))

        res = call_go.manage_database(ctx.id, DatabaseAction.OpenVectorStore.value, str(self.id), payload)
        try:
            store_id = uuid.UUID(res)
            return VectorStore(store_id, trans.transaction_id)
        except:
            raise Exception(res)
