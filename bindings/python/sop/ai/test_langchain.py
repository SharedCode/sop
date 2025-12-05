import sys
import os
import pytest
from unittest.mock import MagicMock

# Ensure we can import sop
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sop import Context, Database
from sop.transaction import DBType
from sop.ai.langchain import SOPVectorStore

class FakeEmbeddings:
    def embed_documents(self, texts):
        # Return dummy vectors of dimension 2
        return [[0.1, 0.2] for _ in texts]
    
    def embed_query(self, text):
        return [0.1, 0.2]

def test_langchain_integration():
    ctx = Context()
    # Use a temporary path for the DB
    db = Database(ctx, storage_path="/tmp/sop_langchain_test", db_type=DBType.Standalone)
    
    embeddings = FakeEmbeddings()
    collection_name = "langchain_test"
    
    # Test initialization
    vector_store = SOPVectorStore(ctx, db, collection_name, embeddings)
    
    # Test add_texts
    texts = ["hello world", "foo bar"]
    metadatas = [{"source": "test"}, {"source": "test2"}]
    ids = vector_store.add_texts(texts, metadatas=metadatas)
    
    assert len(ids) == 2
    
    # Test similarity_search
    docs = vector_store.similarity_search("hello", k=1)
    assert len(docs) > 0
    assert docs[0].page_content in texts
    assert "source" in docs[0].metadata

def test_from_texts():
    ctx = Context()
    db = Database(ctx, storage_path="/tmp/sop_langchain_test_2", db_type=DBType.Standalone)
    embeddings = FakeEmbeddings()
    
    texts = ["apple", "banana"]
    metadatas = [{"type": "fruit"}, {"type": "fruit"}]
    
    vector_store = SOPVectorStore.from_texts(
        texts, 
        embeddings, 
        metadatas=metadatas, 
        ctx=ctx, 
        db=db, 
        collection_name="fruits"
    )
    
    assert isinstance(vector_store, SOPVectorStore)
    
    docs = vector_store.similarity_search("apple", k=1)
    assert len(docs) == 1
    assert docs[0].page_content in texts

def test_from_texts_missing_args():
    embeddings = FakeEmbeddings()
    texts = ["a"]
    
    with pytest.raises(ValueError):
        SOPVectorStore.from_texts(texts, embeddings)
