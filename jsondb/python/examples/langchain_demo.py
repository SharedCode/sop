import uuid
from typing import List, Any, Iterable, Optional, Dict, Tuple
from dataclasses import asdict

# In a real scenario, you would import these from langchain
# from langchain.vectorstores.base import VectorStore
# from langchain.schema import Document
# from langchain.embeddings.base import Embeddings

from sop.ai.vector import VectorDatabase, DBType, Item

# --- Mocking LangChain Interfaces for this Demo ---
class Document:
    def __init__(self, page_content: str, metadata: dict = None):
        self.page_content = page_content
        self.metadata = metadata or {}

class Embeddings:
    def embed_query(self, text: str) -> List[float]:
        raise NotImplementedError
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError

# --- Simple Deterministic Embedder for Demo ---
class SimpleHashEmbedder(Embeddings):
    """
    A toy embedder that converts text to a fixed-size vector using hashing.
    In production, use OpenAIEmbeddings or HuggingFaceEmbeddings.
    """
    def __init__(self, dim: int = 3):
        self.dim = dim

    def _hash_text(self, text: str) -> List[float]:
        # Create a deterministic vector from string
        seed = sum(ord(c) for c in text)
        import random
        rng = random.Random(seed)
        return [rng.random() for _ in range(self.dim)]

    def embed_query(self, text: str) -> List[float]:
        return self._hash_text(text)

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return [self._hash_text(t) for t in texts]

# --- The SOP Adapter ---
class SOPVectorStore:
    """
    A LangChain-compatible wrapper for SOP Vector Database.
    """
    def __init__(self, sop_store, embedding: Embeddings):
        self.store = sop_store
        self.embedding = embedding

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[str]:
        """Run more texts through the embeddings and add to the vectorstore."""
        # 1. Generate Embeddings
        vectors = self.embedding.embed_documents(list(texts))
        
        # 2. Prepare Items for SOP
        items = []
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in texts]
        
        for i, text in enumerate(texts):
            metadata = metadatas[i] if metadatas else {}
            # Store the text in metadata so we can retrieve it later
            metadata["text"] = text
            
            item = Item(
                id=ids[i],
                vector=vectors[i],
                payload=metadata
            )
            items.append(item)

        # 3. Upsert to SOP
        self.store.upsert_batch(items)
        return ids

    def similarity_search(
        self, query: str, k: int = 4, **kwargs: Any
    ) -> List[Document]:
        """Return docs most similar to query."""
        # 1. Embed Query
        vector = self.embedding.embed_query(query)

        # 2. Search SOP
        hits = self.store.query(vector=vector, k=k)

        # 3. Convert Hits to Documents
        docs = []
        for hit in hits:
            # We stored the original text in the payload/metadata
            content = hit.payload.pop("text", "")
            docs.append(Document(page_content=content, metadata=hit.payload))
        
        return docs

# --- Hello World Demo ---
def main():
    print("Initializing SOP Vector Database (Standalone Mode)...")
    # 1. Setup SOP
    db = VectorDatabase(storage_path="./data/langchain_demo", db_type=DBType.Standalone)
    raw_store = db.open("demo_collection")

    # 2. Setup Embedder
    embedder = SimpleHashEmbedder(dim=3)

    # 3. Create the LangChain Wrapper
    vectorstore = SOPVectorStore(raw_store, embedder)

    # 4. Add Documents
    print("Adding documents...")
    texts = [
        "SOP is a high-performance Go library.",
        "LangChain is a framework for LLM applications.",
        "Python is a great language for AI.",
        "The sky is blue and the sun is bright."
    ]
    metadatas = [{"source": "sop_docs"}, {"source": "lc_docs"}, {"source": "general"}, {"source": "nature"}]
    
    ids = vectorstore.add_texts(texts, metadatas=metadatas)
    print(f"Added {len(ids)} documents.")

    # 5. Search
    query = "Go library"
    print(f"\nQuerying for: '{query}'")
    results = vectorstore.similarity_search(query, k=2)

    print("\nResults:")
    for i, doc in enumerate(results):
        print(f"{i+1}. {doc.page_content} (Metadata: {doc.metadata})")

if __name__ == "__main__":
    main()
