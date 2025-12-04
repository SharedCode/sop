# Text Search Engine Tutorial

This document explains how the text search engine in `ai/search` works, covering tokenization, indexing, and scoring.

## 1. Overview

The search engine uses an **Inverted Index** backed by B-Trees to store term-to-document mappings. It implements the **BM25** (Best Matching 25) algorithm for ranking search results, which is a probabilistic retrieval framework used by systems like Lucene and Elasticsearch.

## 2. Core Components

### 2.1 Tokenization (`tokenizer.go`)
Before text can be indexed or searched, it must be broken down into "tokens" (terms).
*   **Input**: "The quick brown fox!"
*   **Process**: Split by whitespace/punctuation, convert to lowercase.
*   **Output**: `["the", "quick", "brown", "fox"]`

### 2.2 Data Structures (`index.go`)
The `Index` struct manages four B-Trees to store the necessary statistics for BM25:

1.  **`postings`** (Inverted Index)
    *   **Key**: `term|docID` (e.g., "fox|doc1")
    *   **Value**: `Frequency` (How many times "fox" appears in "doc1")
    *   **Purpose**: Quickly find which documents contain a term and how often.

2.  **`termStats`**
    *   **Key**: `term` (e.g., "fox")
    *   **Value**: `DocCount` (Number of documents containing "fox")
    *   **Purpose**: Used to calculate **IDF** (Inverse Document Frequency). Rare terms (like "xylophone") have higher IDF than common terms (like "the").

3.  **`docStats`**
    *   **Key**: `docID` (e.g., "doc1")
    *   **Value**: `DocLength` (Total number of tokens in the document)
    *   **Purpose**: Used to normalize term frequency. A term appearing 3 times in a short tweet is more significant than 3 times in a long book.

4.  **`global`**
    *   **Keys**: `"total_docs"`, `"total_len"`
    *   **Values**: Integers
    *   **Purpose**: Used to calculate the average document length (`avgDL`) for the corpus.

## 3. Indexing Process (`Add` method)

When you add a document (`doc1`: "the quick brown fox"):

1.  **Tokenize**: `["the", "quick", "brown", "fox"]` (Length = 4).
2.  **Update `docStats`**: Store `doc1 -> 4`.
3.  **Calculate Frequencies**: `{"the": 1, "quick": 1, "brown": 1, "fox": 1}`.
4.  **Update `postings` & `termStats`**:
    *   For "fox":
        *   Add `postings`: `"fox|doc1" -> 1`.
        *   Increment `termStats`: `"fox" -> count + 1`. (Uses `Upsert` for atomicity).
5.  **Update `global`**:
    *   Increment `total_docs`.
    *   Add 4 to `total_len`.

## 4. Search Process (`Search` method)

When you search for "quick fox":

1.  **Tokenize Query**: `["quick", "fox"]`.
2.  **Fetch Global Stats**: Get `N` (Total Docs) and `avgDL` (Total Len / N).
3.  **Score Each Term**:
    *   For "quick":
        *   Get `DocCount` from `termStats` to calculate **IDF**.
        *   Scan `postings` starting at `"quick|"`.
        *   For each match (e.g., `"quick|doc1"`):
            *   Get `Frequency` (1) and `DocLength` (4).
            *   Calculate **BM25 Score** for this term/doc pair.
            *   Add to `doc1`'s total score.
    *   Repeat for "fox".
4.  **Rank**: Sort documents by their accumulated scores in descending order.

## 5. Example Usage

See `ai/search/index_test.go` for a runnable example.

```go
// 1. Create Index
idx, _ := NewIndex(ctx, trans, "my_index")

// 2. Add Documents
idx.Add(ctx, "doc1", "the quick brown fox")
idx.Add(ctx, "doc2", "jumps over the lazy dog")

// 3. Search
results, _ := idx.Search(ctx, "fox")
// results[0].DocID == "doc1"
```

## 6. Why BM25?

BM25 improves upon simple Term Frequency (TF) by:
1.  **Saturation**: The impact of term frequency diminishes. Seeing "fox" 100 times isn't 100x better than seeing it once.
2.  **Length Normalization**: Matches in shorter documents are valued higher than matches in very long documents (which might mention the term by chance).
