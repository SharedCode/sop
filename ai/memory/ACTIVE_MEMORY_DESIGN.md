# SOP Active Memory Pipeline: Phase 2 Design (The Butler Architecture)

## Objective
Implement Phase 2: LongTermMemory Provisioning & ShortTermMemory Interception to emulate human cognition within the SOP AI engine, acting as a deeply aware "Butler".

## Architectural Philosophy
The core design relies on **Conceptual Bounding** and mimicking human memory processing. The system separates the immediate capture of events (Short-Term Memory / ShortTermMemory) from deep, interconnected semantic structuring (Long-Term Memory / LongTermMemory). 

The "Butler" metaphor perfectly encapsulates what an Agentic Copilot should be: deeply aware of its tools (SOP), intimately familiar with its master (LongTermMemory), and focused on the immediate task (Selected KB/Domain). To achieve this "Omni" behavior without Context Overload, we rely on **Intent-Based Routing** and **Tool-Driven Retrieval**.

## Core Differentiators vs Traditional Vector DBs

This system formalizes the architectural divergence between the SOP Dynamic Knowledge Base and traditional Vector Databases (K-means, IVF, HNSW), mimicking human cognition rather than forcing mathematical proximity mapping up to the application layer.

### 1. Conceptual Bounding over Mathematical Voronoi Cells
Traditional vector DBs compute mathematical "clusters" (K-means). This generates rigid boundaries where context is lost based purely on Euclidean/Cosine distance.
- **SOP Dynamic DB:** Asks the intelligence layer (LLM) to form conceptual buckets *first* (e.g. "Tax Law", "Apples"). Vectors are then structurally bound within these Semantic Categories. It groups ideas not by raw geometry, but by contextual meaning.

### 2. Native Semantic Graph (DAG) 
Because concepts in a human brain are multifaceted (e.g. "Tomato" is both "Fruit" and "Cooking Ingredient"), traditional databases fail to elegantly represent this polyhierarchy without massive redundancy.
- **SOP Dynamic DB:** Organizes the B-Tree with native `CategoryParent` edges. It behaves as a Directed Acyclic Graph, natively storing the knowledge domain inside the storage engine, rather than forcing the application to build a graph *over* a dumb index.

### 3. Zero-Cost JSON Knowledge Migration
Vector databases serialize meaningless float arrays. Exporting a traditional vector DB yields raw numbers that cannot be ingested into a different context without the exact same underlying embedding model and historical data points.
- **SOP Dynamic DB:** The `ExportJSON` and `ImportJSON` capabilities output a named, conceptually mapped array of knowledge. This allows pre-trained "Knowledge Bases" to be sold, traded, or ported between applications because the conceptual schema is preserved alongside the vectors.

## The Omni AI Protocol (The Butler Architecture)

### 1. The Memory Interplay (ShortTermMemory -> LongTermMemory)
We treat the AI's Memory as a distinct, dedicated `KnowledgeBase` (e.g., `memory_<user_id>`). This ensures the user's Long-Term Memory persists across all their future chat sessions, while keeping their concurrent active chat threads separate. User Long-Term Memory (LongTermMemory) is entirely separated by their `UserID` to strictly enforce data privacy constraints across all users.
* **ShortTermMemory (The Scratchpad):** The immediate conversation, current task constraints, and explicit feedback. This lives in the active session and is quickly written as "Raw Thoughts" into a standard database table acting as an O(1) buffer (`user_active_scratchpad`). We enforce **Cryptographic Deduplication** within the ShortTermMemory (via SHA-256 caching inside `logEpisode`) - running identical queries over and over merely overwrites identical keys and prevents physical blob limits bypassing block inflation or duplicated sleep cycle consolidation attempts.
* **LongTermMemory (The Butler's Ledger):** As the AI completes chores, it continuously writes to this dedicated LongTermMemory KnowledgeBase. As thoughts flow into the LongTermMemory Knowledge Base, they undergo the **Semantic Anchor** progressive categorization strategy. The LLM generates a precise "Category" (e.g., "User Management Analytics") which becomes the geometric spatial center in the LongTermMemory vector store. During subsequent sleep cycles, the LLM can mutate or broaden Category descriptions if it notices patterns, meaning categorization quality strictly improves as more data flows in.
* **ReAct Loop Integration:** At the start of a prompt, the AI *always* does a cheap, high-speed lookup against its LongTermMemory to fetch "User Preferences" or "Historical Context" relevant to the current ask. This anchors the AI's personality.

### 2. The Multi-Tiered Routing & Prioritization Hierarchy
Instead of querying all KBs and concatenating the results, we equip the AI's ReAct loop with specific Retrieval Tools:
* **Tier 1: SOP KB (The Instruction Manual)**
  * *When:* Consulted if the intent classifier detects user asking about the platform, tools, patterns, or how to build with SOP.
  * *Why:* The AI must fetch facts from the SOP KB, never guessing how the underlying system works.
* **Tier 2: User Selected KB (The Active Domain)**
  * *When:* For the actual business logic of the user's prompt.
  * *Why:* Restricts domain extraction strictly to the user's active context (e.g., "Finance").
* **Tier 3: The LTM KB (The Butler's Memory)**
  * *When:* Automatically fetched to align the response with past habits or known rules.

### 3. Fallback Rules, Namespace Collisions, & Database Agnosticism
If the AI cannot answer a question using Tier 1 or Tier 2, we implement a Federated Fallback Strategy using a `search_custom_kbs` tool that hits arrays of custom KBs. KBs abstract away the physical database.
* **Handling Namespace Collisions:** User-selected KBs can reside in either the System DB or the Current DB (Tenant). If a collision occurs (e.g., `Finance` exists in both), the **Current DB strictly shadows/overrides** the System DB.
* **Access Control (Read-Only Global KBs):** The SOP KB (`SOP`) and LTM (User Preferences, e.g., `memory_<session_id>`) KBs reside in the System DB and are exposed as **Read-Only** in the UI, as they are meant to be read-only for the user. **All other KBs, whether in the System DB or a Tenant DB, are Read/Write.**
* **LTM Storage Location & Isolation:** The automatically provisioned `memory_<user_id>` resides exclusively in the **System DB**. This ensures the user's habits and preferences persist when switching between entirely isolated client environments (Tenants/Current DBs). Furthermore, **a user's LongTermMemory is strictly hidden from other users**; it only appears and is accessible to the specific user it belongs to.

### 4. Meta-Memory (Memory Management Learning)
To prevent LongTermMemory from becoming bloated with redundant data, the AI must explicitly learn *how* to manage its own memory. We will partition a specific category within the `memory_<user_id>` called `Meta_Cognition`.
*   **Purpose:** Store explicit rules discovered by the AI or user on when to store, reference, or deduplicate information relative to the active KBs.
*   **Mechanism:** During the Sleep Cycle, the LLM will consult these `Meta_Cognition` rules before deciding to commit an episode to LongTermMemory. Example rules the AI might learn:
    - *Rule 1 (Referencing):* "If data already exists in the SOP KB, do not duplicate it; just log a reference pointer."
    - *Rule 2 (Generalization):* "If the user solves a bug, generalize the solution instead of memorizing the exact stack trace."
*   **Rollout:** Begin simply by injecting a few hardcoded meta-rules into this category upon session creation. Over time, allow the LLM to update this section based on explicit user feedback (e.g. "Don't save this," or "Always remember this pattern").

---

## Component Architecture (Engine Level)

### 1. ShortTermMemory (STM - Scratchpad)
*   **Purpose:** Fast ingestion buffer for episodes and thoughts ("The Scratchpad").
*   **Mechanism:** Written to a standard table `user_active_scratchpad`.
*   **Constraint:** Must be an O(1) operation. It acts purely as a physical buffer and does *not* invoke LLM processing or categorization during the write path to prevent blocking real-time execution.

### 2. Semantic LongTermMemory (LTM)
*   **Purpose:** Deep, conceptually bounded storage of categorized thoughts ("The Butler's Ledger").
*   **Mechanism:** Uses the `KnowledgeBase` (implemented in Phase 1).
*   **Target:** Builds a Directed Acyclic Graph (DAG) of semantic categories and anchors vectors within these conceptual boundaries rather than raw float arrays.
*   **Constraint:** Requires LLM-based categorization (e.g., `kb.IngestThought`), making it too heavy for synchronous, real-time logging.

### 3. The Consolidator Process ("Sleep Cycle")
*   **Purpose:** Bridge the gap between ShortTermMemory and LongTermMemory.
*   **Mechanism:** A background worker that periodically wakes up to execute the **Cognitive Filtering Layer**.
*   **Workflow:**
    1.  Reads raw, buffered items from `user_active_scratchpad` (ShortTermMemory).
    2.  Invokes the LLM to evaluate the recent batch of ShortTermMemory thoughts with a Consolidation Prompt.
    3.  Performs **Noise Removal**: The LLM evaluates outcomes. If a thought was an arbitrary syntax error or useless transient query, the LLM discards it.
    4.  Performs **Semantic Merging (Abstract Deduplication)**: The LLM recognizes semantic equivalences (e.g. "Show me active users" and "List users who are active") and merges them into a single, high-quality "Golden Rule" / Intent.
    5.  Writes the refined, categorized "Golden Thoughts" structurally into the `memory_<user_id>` (LongTermMemory).
    6.  Prunes the migrated items from the ShortTermMemory buffer.

## Implementation Guidelines & Corrections
*   **`logEpisode` Execution:** The `logEpisode` function (the interception point) MUST write its serialized outcomes directly to the raw `user_active_scratchpad` buffer. It MUST NOT call `KnowledgeBase` methods (like `IngestThought`) because doing so bypasses the buffer, incorrectly routing thoughts directly into LongTermMemory in real-time.

---

## Implementation Map

### Phase 1: API & Configuration Plumbing (✅ Complete)
Getting the required data from the UI down into the engine.
* [x] **API Update:** Update the HTTP `ChatRequest` struct to accept `SelectedKBs []string` from the dropdown.
* [x] **Domain Interface Update:** Update `GenericDomain`'s `Memory()` function to explicitly accept the LLM `Generator` to allow Agentic RAG categorization upon load.
* [x] **Dependency Injection:** Verify `agent.CopilotAgent` and `agent.Dependencies` securely hold references to both the `SystemDB` and the Active Tenant `CurrentDB` simultaneously.
* [x] **Context Passing:** Pipe the `SelectedKBs` array from `main.ai.go` down through the agent initialization context into `ai.SessionPayload` so ReAct tools can natively access it during reasoning.

### Phase 2: Memory Infrastructure (ShortTermMemory & LongTermMemory Interplay) (✅ Complete)
Building the dedicated "Butler's Ledger."
* [x] **LongTermMemory Provisioning:** Modified the HTTP server and Copilot initialization to automatically discover or create `memory_<user_id>` to persist the user's memory ledger.
* [x] **ShortTermMemory Interception:** Completely refactored `active_memory.go` to use pure JSON B-Tree (`user_active_scratchpad`) without embeddings or `TempVector` dependencies, establishing a true O(1) buffer.
* [x] **ReAct Pre-Prompt Fetch:** Add logic to the start of the ReAct sequence that performs a lightning-fast vector search against `memory_<user_id>` to fetch user preferences, quietly injecting them into the System Prompt before standard execution.

### Phase 3: The Context Multiplexer & Retrieval Tools (✅ Complete)
Equipping the LLM with the tiered toolset to fetch data deterministically.
* [x] **Tier 1 (SOP Tool):** Consolidate/Create a `search_sop` tool that is hardcoded to only scan the `SystemDB` for SOP platform instructions.
* [x] **Tier 2 (Domain Tool):** Create a `search_domain_kb` tool that maps to the primary KB selected by the user.
* [x] **Tier 3 (Fallback Tool):** Build the `search_custom_kbs` tool that accepts a topic and iterates over the `SelectedKBs` array.
* [x] **Namespace Shadowing Logic:** Inside these tools, write the logic that checks if the requested KB exists in the Tenant DB first; if yes, use it (override). If no, check the System DB.

### Phase 4: Intent Routing & The Sleep Cycle (✅ Complete)
Completing the autonomous feedback loops.
* [x] **System Prompt Update:** Rewrite the Copilot's `system_prompt` explicitly instructing the LLM on *when* to use the SOP tool vs. the Domain tool.
* [x] **Sleep Cycle Scheduler:** Implement a background goroutine (cron-like) that periodically loops over `user_active_scratchpad`, structurally categorizes them via LLM against the Meta-Memory rules, and triggers ingestion into the Long-Term `memory_<user_id>`.
* [x] **Meta-Memory Seeding Background Routine:** Developed `seedMetaCognitionAsync` in `main.ai.go` which bypasses LLM latency during initialization by deterministically injecting `Meta_Cognition` baseline constraints for Sleep Cycle orchestration constraints.stration constraints.