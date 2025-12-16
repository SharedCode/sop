print("Starting LangChain Demo...")
import os
import sys
from typing import List

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context, DatabaseOptions
from sop.ai import Database, DatabaseType

# --- Simple Deterministic Embedder for Demo ---
class SimpleHashEmbedder:
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

# --- Hello World Demo ---
def main():
    print("Initializing SOP Vector Database (Standalone Mode)...")
    db_path = "./data/langchain_demo"
    
    # Clean up
    import shutil
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    ctx = Context()
    # 1. Setup SOP
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))

    # 2. Setup Embedder
    embedder = SimpleHashEmbedder(dim=3)

    # 3. Create the LangChain Wrapper
    # We use the SOPVectorStore class from the library (sop.ai)
    vectorstore = db.vector_store(ctx, "demo_collection", embedder)

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

    # Clean up
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

if __name__ == "__main__":
    main()
