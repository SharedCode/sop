import json
from typing import List
from dataclasses import dataclass
from enum import Enum
from . import call_go
from . import context

class SearchAction(Enum):
    Unknown = 0
    Add = 1
    Search = 2

@dataclass
class SearchResult:
    DocID: str
    Score: float

class Index:
    def __init__(self, ctx: context.Context, id: str, transaction_id: str):
        self.ctx = ctx
        self.id = id
        self.transaction_id = transaction_id
        self.meta = {
            "id": self.id,
            "transaction_id": self.transaction_id
        }
        self.meta_json = json.dumps(self.meta)

    def add(self, doc_id: str, text: str):
        payload = json.dumps({
            "doc_id": doc_id,
            "text": text
        })
        res = call_go.manage_search(self.ctx.id, SearchAction.Add.value, self.meta_json, payload)
        if res:
            raise Exception(res)

    def search(self, query: str) -> List[SearchResult]:
        payload = json.dumps({
            "query": query
        })
        res = call_go.manage_search(self.ctx.id, SearchAction.Search.value, self.meta_json, payload)
        try:
            # res is JSON string of list of results
            raw_results = json.loads(res)
            return [SearchResult(**r) for r in raw_results]
        except Exception as e:
            raise Exception(f"Search failed: {res} | Error: {e}")
