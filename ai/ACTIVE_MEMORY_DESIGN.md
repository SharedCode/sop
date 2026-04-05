# Active Self-Organizing Memory System

**Date:** April 4, 2026  
**Status:** Architecture & Implementation Plan  
**Goal:** Evolve the SOP AI Engine from an "Instructed Retrieval System" (relying on explicit human corrections and `manage_knowledge` calls) to an **"Autonomous Cognitive Engine"** running on continuous, self-organizing vector clustering within the SOP B-Tree.

---

## 1. The Core Cognitive Model

Instead of treating the AI as a chat partner that needs to be explicitly told the rules of a domain, we mimic human cognitive processes:
1. **Thoughts (Vectors):** Every user intent, generated AST (script), and database outcome is embedded into a high-dimensional vector.
2. **Categories (Centroids):** Taxonomies are not hardcoded namespaces (e.g., `rule`, `vocabulary`). They naturally emerge as localized clusters of vectors. The mathematical center of that cluster is the "Centroid."
3. **Consolidation (Sleep/Learn Cycle):** The system passively logs episodes to Short-Term Memory, and relies on background workers to organize them into Long-Term Memory (assigning to Centroids or splitting overloaded Centroids).

## 2. The SOP Advantage (Why B-Trees Make This Possible)

Standard vector DBs (HNSW graphs) struggle with massive reorganization because updating clusters requires complex graph rebuilds. SOP's transactional B-Tree architecture trivializes this:
* **The `CentroidID` Abstraction:** The `Vectors` B-Tree keys (`ai.VectorKey{CentroidID, Distance, ItemID}`) strictly reference the integer `CentroidID`. 
* **Zero-Cost Centroid Updates:** The mathematical center (the Centroid's vector itself) can be updated via a rolling average in the `Centroids` B-Tree *without* modifying the thousands of child vectors mapped to it.
* **Physical Data Locality:** Because vectors are indexed by `CentroidID` first, all "Thoughts" in a category are stored contiguously on disk. Scanning a category is blistering fast.
* **ACID Re-Categorization:** If a category gets too large, splitting it into two new Centroids and reassigning thousands of vectors is a safe, ACID-compliant series of B-Tree Deletes and Inserts. No graph corruption.

---

## 3. Implementation Roadmap

### Phase 1: Complete Core Vector Support (The "Brain Structure")
Before the AI can self-organize, the `ai/vector` package must support dynamic calculation, assignment, and re-routing of vectors to centroids.
* **AssignAndIndex(vector, itemID):** 
    1. Scan the `Centroids` B-Tree to find the closest centroid.
    2. Calculate `Distance`.
    3. Insert into the `Vectors` B-Tree (`VectorKey{CentroidID, Distance, ItemID}`).
    4. Update the Centroid's vector (rolling average) to prevent cluster drift.
* **SplitCentroid(centroidID) [The Cell Division Trigger]:**
    * Monitor centroid density. If a centroid exceeds a threshold (e.g., > 10,000 vectors), it is conceptually "too broad."
    * Extract its vectors, run a localized 2-Means clustering, delete the old centroid, create two new specific centroids, and re-index the vectors transactionally.

### Phase 2: Episodic Ingestion (Capturing "Thoughts")
Intercept the AI's execution pipeline so it remembers silently, eliminating the "Human Tax" of manual correction.
* **The Ephemeral Logger:** Wrap the script execution engine. When a script runs, serialize the context (`User Intent` + `AST Executed` + `Outcome: Success/Error`).
* **Embed & Store:** Generate a vector embedding for this outcome and insert it into the `TempVectors` B-Tree (Short-Term Memory).

### Phase 3: The Consolidator Process (The "Sleep" Cycle)
Organize short-term memories into long-term structures without blocking user queries.
* **Background Worker:** A scheduled goroutine (`vector.Consolidate()`) that reads from `TempVectors`.
* **Pruning:** Discard exact/near-exact duplicates (e.g., repeated successful queries).
* **Ingestion:** Feed the pruned thoughts into `AssignAndIndex`, migrating them to the persistent `Vectors` and `Centroids` B-Trees.

### Phase 4: Ambient Retrieval (Implicit Memory Integration)
Connect the organized memory seamlessly into the AI's prompt engine.
* **The "Pre-Prompt" Search (Hybrid Retrieval):** When a user asks a question, the system employs a dual-search strategy:
    * *Vector Search (Semantic):* Embeds the query text and does an AnnN search against the `ai/vector` database to find the closest Centroids and their top localized Thoughts. This bridges semantic gaps (e.g., mapping "Income" to "Revenue").
    * *BM25 Search (Lexical):* Runs a standard keyword search against the indexed episodic memory payloads. This is critical for catching exact identifiers, UUIDs, or specific error codes (e.g., "ORA-1045") that are often smeared or lost in high-dimensional vector spaces.
    * *Scoring:* Balances and merges the two result sets using an alpha coefficient: `Score = (\alpha * Vector_Sim) + ((1-\alpha) * BM25_Score)`.
* **Context Injection (Just-In-Time RAG):** Invisibly inject these retrieved, hybrid-scored memories into the LLM's system prompt (e.g., *"Context: The last time you queried about Payroll, joining `Employees` to `Salaries` succeeded."*). Let the LLM self-correct before it acts.

## 4. Performance Implications (The "Cognitive Load")

Transitioning to an Active Memory system introduces computational overhead, but the architecture isolates this from the user experience:
* **The Fast Path (Foreground):** Generating an embedding for the user's prompt and scanning the `Centroids` B-Tree takes ~50ms. Because data is physically grouped by `CentroidID`, disk I/O is highly localized and extremely fast.
* **The Slow Path (Background):** Embedding episodic logs and running K-Means clustering is CPU/GPU intensive. This is mitigated by isolating these tasks into asynchronous background workers (the "Sleep Cycle"), ensuring user requests are never blocked by the system's learning process.
* **Memory Overhead:** Dense vectors (e.g., 1024 dimensions) consume ~4KB each. To ensure blazing fast retrieval, the `Centroids` B-Tree (the category headers) should be kept primarily in RAM or Cache (~40MB for 10,000 categories).

## 5. Accuracy & Minimizing Hallucinations

A mathematically generated taxonomy (Centroids) offers significant accuracy advantages over human-labeled folders or purely generative rules, drastically reducing LLM hallucinations:

### 5.1. Semantic Precision over Keyword Matching
Because categories (Centroids) are mathematical centers of meaning rather than strings, they capture the *intent* of a domain. If the system learns a successful database rule about "Revenue," a user asking about "Income" will naturally map to the exact same Centroid in vector space, retrieving the correct rule without needing a human to link the synonyms.

### 5.2. Granular Context Isolation
Hallucinations often occur when an LLM is given too much irrelevant context. By physically grouping related "Thoughts" under specific Centroids, the Ambient Retrieval phase only pulls the exact top-K relevant memories for the current category. The LLM is forced to operate within a highly constrained, mathematically bounded context window, starving the hallucination engine of irrelevant noise.

### 5.3. Ground Truth Feedback Loops
The memories being clustered aren't arbitrary text; they are explicit, episodic outcomes from the SOP database itself (e.g., `AST Payload` + `Success/Error Status`). When the LLM retrieves a memory, it retrieves *ground truth execution history*. If a query caused a schema error yesterday, that error is bound to the Centroid. The LLM sees the exact mistake and the exact working solution, overriding its generative bias with factual history.

### 5.4. Mitigating "Concept Drift"
As the system learns, categories can become overloaded. The `SplitCentroid` function ensures that boundaries remain tight. If "Sales Operations" begins to accumulate both "Payroll" and "Lead Generation" queries, the system autonomously splits the Centroid into two tighter boundaries. This mathematical self-correction ensures that retrieved memories are consistently highly relevant, minimizing the chance of supplying the LLM with ambiguous or overlapping context.

---

## 6. Current Status
* B-Tree schema (`vector/architecture.go`) structurally supports this (`Centroids` vs `Vectors` via `CentroidID`).
* **Next Immediate Action:** Begin Phase 1 by implementing `AssignAndIndex` and finalizing the continuous clustering logic over `TempVectors` in the `ai/vector` package.