from typing import Any, Iterable, List, Optional, Type
import uuid
import logging

try:
    from langchain.vectorstores.base import VectorStore
    from langchain.docstore.document import Document
    from langchain.embeddings.base import Embeddings
except ImportError:
    # Create dummy classes if langchain is not installed
    # This allows the module to be imported even without langchain
    class VectorStore: pass
    class Embeddings: pass
    class Document: 
        def __init__(self, page_content, metadata):
            self.page_content = page_content
            self.metadata = metadata

from .. import context
from .. import transaction
from ..database import Database
from .vector import Item

logger = logging.getLogger(__name__)

class SOPVectorStore(VectorStore):
    """
    SOPVectorStore is a LangChain-compatible vector store wrapper for the SOP Database.
    It allows you to use SOP as a vector backend in LangChain pipelines.
    """

    def __init__(
        self,
        ctx: context.Context,
        db: Database,
        collection_name: str,
        embedding: Embeddings,
        **kwargs: Any,
    ):
        """
        Initialize the SOP Vector Store.

        Args:
            ctx: The SOP Context.
            db: The SOP Database instance.
            collection_name: The name of the vector store (collection) to use.
            embedding: The LangChain Embeddings model to use.
        """
        self.ctx = ctx
        self.db = db
        self.collection_name = collection_name
        self.embedding = embedding

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[str]:
        """
        Run more texts through the embeddings and add to the vectorstore.

        Args:
            texts: Iterable of strings to add to the vectorstore.
            metadatas: Optional list of metadatas associated with the texts.
            ids: Optional list of IDs for the documents.

        Returns:
            List of IDs of the added texts.
        """
        # 1. Generate Embeddings
        vectors = self.embedding.embed_documents(list(texts))
        
        # 2. Prepare Items for SOP
        items = []
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in texts]
        
        for i, text in enumerate(texts):
            metadata = metadatas[i] if metadatas else {}
            # Store the text in metadata so we can retrieve it later
            # This is a common pattern in Vector Stores that don't natively store "content" separate from payload
            metadata["text"] = text
            
            item = Item(
                id=ids[i],
                vector=vectors[i],
                payload=metadata
            )
            items.append(item)

        # 3. Upsert to SOP (with Transaction)
        # We create a short-lived transaction for this operation
        with self.db.begin_transaction(self.ctx, mode=transaction.TransactionMode.ForWriting.value) as trans:
            store = self.db.open_vector_store(self.ctx, trans, self.collection_name)
            store.upsert_batch(self.ctx, items)
            
        return ids

    def similarity_search(
        self, query: str, k: int = 4, **kwargs: Any
    ) -> List[Document]:
        """
        Return docs most similar to query.

        Args:
            query: Text to look up documents similar to.
            k: Number of Documents to return. Defaults to 4.

        Returns:
            List of Documents most similar to the query.
        """
        # 1. Embed Query
        vector = self.embedding.embed_query(query)

        # 2. Search SOP (with Transaction)
        with self.db.begin_transaction(self.ctx, mode=transaction.TransactionMode.ForReading.value) as trans:
            store = self.db.open_vector_store(self.ctx, trans, self.collection_name)
            hits = store.query(self.ctx, vector=vector, k=k)

            # 3. Convert Hits to Documents
            docs = []
            for hit in hits:
                payload = hit.payload.copy()
                # Extract the original text
                content = payload.pop("text", "")
                docs.append(Document(page_content=content, metadata=payload))
            
            return docs

    @classmethod
    def from_texts(
        cls: Type["SOPVectorStore"],
        texts: List[str],
        embedding: Embeddings,
        metadatas: Optional[List[dict]] = None,
        **kwargs: Any,
    ) -> "SOPVectorStore":
        """Return VectorStore initialized from texts and embeddings."""
        ctx = kwargs.get("ctx")
        db = kwargs.get("db")
        collection_name = kwargs.get("collection_name", "langchain_store")
        
        if not ctx or not db:
            raise ValueError("Must provide 'ctx' and 'db' in kwargs to initialize SOPVectorStore")

        sop_store = cls(ctx, db, collection_name, embedding)
        sop_store.add_texts(texts, metadatas, **kwargs)
        return sop_store
