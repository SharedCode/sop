# Active Memory
This document outlines the LongTermMemory Provisioning & ShortTermMemory Interception architecture that emulates human cognition within the SOP AI engine, acting as a deeply aware "Butler".

## Architectural Philosophy
The core architecture relies on **Conceptual Bounding** and mimicking human memory processing. The system separates the immediate capture of events (Short-Term Memory / ShortTermMemory) from deep, interconnected semantic structuring (Long-Term Memory / LongTermMemory). 

The "Butler" metaphor perfectly encapsulates what an Agentic Copilot should be: deeply aware of its tools (SOP), intimately familiar with its master (LongTermMemory), and focused on the immediate task (Selected KB/Domain). To achieve this "Omni" behavior without Context Overload, we rely on **Intent-Based Routing** and **Tool-Driven Retrieval**.

Because the data payloads ("thoughts") within our engine are categorized and organized dynamically by the LLM, the architecture intrinsically mimics the human mind. The efficiency of human thought and recall fundamentally relies on how well we organize and catalog our memories.

In this same vein, those who natively organize and catalog their memory thoughts process information with higher intelligence and faster thinking speeds. By structuring our cognitive memory engine such that concepts are bounded into semantic DAGs (Categories), we are effectively replicating this biological cognitive efficiency in software.

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

### 2. Multi-Tiered Routing & Prioritization Hierarchy
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

### 5. Thought Capture, Refinement, and Category Minting
LongTermMemory is not intended to be a passive archive of raw transcripts. Its purpose is to act as a continuous refinement layer over the LLM's future decisions. For that reason, the system must capture rich user-driven content, refine it over time, and continuously re-organize that content into better thoughts and categories.
*   **Capture Principle:** User interaction drives what becomes eligible for memory. Successful patterns, repeated preferences, useful corrections, and high-signal domain observations should be preserved. Low-value transient noise should remain in ShortTermMemory or be discarded during consolidation.
*   **Refinement Principle:** Thoughts are not immutable. During Sleep Cycles, the system should merge duplicates, generalize narrow observations into reusable guidance, and split overly broad thoughts into smaller, more precise units.
*   **Minting Principle:** Categories and thoughts are both first-class evolving assets. The LLM is allowed to mint new Categories, broaden old ones, narrow dense ones, and rewrite thought descriptions when this improves future retrieval and behavioral alignment.
*   **Behavioral Goal:** The output of this process is not merely better search recall. The refined LTM should actively bias future LLM choices, tool usage, style, and decision boundaries.

### 5.5. Knowledge Base Absorption into LTM
LongTermMemory should also reserve explicit capacity for absorbed Knowledge Bases. The intent is to let the agent internalize reusable skills, expertise, and durable domain knowledge from curated Spaces without forcing every future Ask loop to depend on that Space remaining actively mounted.
*   **Current Operating Model:** Today, the default composition is still Avatar + mounted KB + STM + LTM. The mounted Knowledge Base remains the primary way to give the LLM grounded domain context, and that flow must be stabilized before absorption becomes a primary runtime mode.
*   **Future Operating Model:** Over time, the system should support an Avatar simply carrying STM/LTM after a Knowledge Base has been absorbed. In that mode, the Avatar is no longer defined by a single live KB binding; it can later absorb another KB and accumulate additional expertise incrementally.
*   **Skill Depot Concept:** Inside LTM, we should carve out a dedicated logical depot for absorbed expertise. This depot can hold refined skills, procedural knowledge, domain heuristics, and high-value summaries that were originally curated inside a Knowledge Base.
*   **Absorption Model:** A Knowledge Base is not copied blindly into LTM. The absorption process should summarize, distill, and re-categorize its contents into durable thoughts and skills suitable for influencing future behavior.
*   **Resulting Capability:** By absorbing multiple Spaces into this depot, the AI can become multi-talented over time, carrying forward expertise from many domains while still respecting prompt budgets and routing boundaries.
*   **Governance Rule:** Absorbed KB content must remain attributable. We should preserve lineage back to the originating Space/Knowledge Base so the system can explain where a skill or expertise fragment came from, and so future refresh/rebuild operations can reconcile source changes.
*   **Future Retrieval Role:** This absorbed depot is a first-class part of LTM and should eventually participate in MRU projection alongside other LTM signals, but under explicit policy so absorbed skills do not crowd out fresher STM continuity.
*   **Near-Term Priority:** Before this transition is made first-class, we need to capture and stabilize exactly how a mounted KB benefits the LLM today: retrieval patterns, prompt contribution, continuity behavior, and the practical boundaries between KB-grounded execution and absorbed long-term skill.

### 6. LTM as a Managed Cognitive Asset
LongTermMemory must itself have lifecycle management characteristics similar to MRU/LRU systems.
*   **Bounded Growth:** LTM cannot grow forever without quality degradation. We will impose capacity and quality limits on retained thoughts and categories.
*   **Recency and Utility:** Thoughts that are recent, repeatedly useful, or behavior-shaping should stay near the active working frontier. Thoughts that become cold, superseded, or redundant should be archived, compacted, or purged.
*   **Refinement over Hoarding:** The system should prefer a smaller number of refined, high-signal thoughts over a large pile of raw observations.
*   **Archive Strategy:** Old thoughts do not have to disappear immediately. They may be moved into colder archival structures first, then eventually purged if they no longer contribute to future behavior or retrieval quality.

### 7. STM -> LTM -> MRU Feedback Loop
The intended direction is a closed loop rather than three isolated memory systems.
1.  **STM captures** the live interaction, immediate routing state, compact execution context, and raw thought candidates.
2.  **Sleep Cycle refines** STM into LTM by deduplicating, generalizing, categorizing, and re-writing thoughts into durable forms.
3.  **MRU rehydrates** from the most relevant STM and LTM entries for the next Ask loop.
4.  **Prompt assembly projects** only the highest-value working subset into the LLM prompt under budget.

Architectural rule:
- STM and LTM are the systems of record for memory.
- MRU is the transient projection layer for the next turn.
- Restart/reboot should rebuild MRU consciously from persisted STM/LTM rather than blindly replaying stale MRU snapshots.

### 8. Persona-Influenced Refinement
Later Sleep Cycle phases will incorporate persona influence deliberately.
*   **Persona as Flavor, not Leakage:** Persona influence should shape how thoughts are summarized, grouped, and prioritized without contaminating unrelated domains or violating sandbox boundaries.
*   **Scoped Refinement:** A Medical persona may prefer compliance-oriented summaries, while an Engineering persona may favor operational heuristics and debugging rules. The same raw episode may therefore be refined differently depending on persona scope.
*   **Identity Formation:** Over time, persona-influenced refinement gives the memory system a stable behavioral flavor. This is how LongTermMemory helps express identity and not just recall facts.

### 9. Evolution Drivers for LongTermMemory
The evolution of LTM is driven by the following goals:
*   **Continuity:** Preserve useful context across turns, sessions, restart, and domain switching.
*   **Refinement:** Improve the quality of stored thoughts rather than merely increasing quantity.
*   **Behavior Shaping:** Influence future LLM decisions, not just retrieval results.
*   **Compression:** Convert noisy episodic data into fewer, stronger, reusable thoughts.
*   **Identity:** Allow future persona-aware refinement to shape a stable behavioral style over time.
*   **Operational Safety:** Keep memory bounded, partitioned, and explainable enough to debug and govern.
*   **Skill Acquisition:** Allow curated Knowledge Bases to be absorbed into LTM as reusable expertise so the AI can accumulate multiple durable competencies over time.

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

## Future Enhancements & Roadmap

### Memory Evolution Roadmap
The long-term direction of the memory system is:
*   **Phase 1:** Introduce explicit STM/MRU projection seams so Ask-loop continuity and restart rehydration do not depend on ad hoc in-process state.
*   **Phase 2:** Separate STM-derived and LTM-derived working-memory contributions so recent continuity signals do not get crowded out by older long-term carry-over.
*   **Phase 3:** Add stronger thought lifecycle management in LTM, including refinement, archival, compaction, and purge policies.
*   **Phase 4:** Expand Sleep Cycle into a persona-aware refinement engine that can shape how durable thoughts are written back into LTM.
*   **Phase 5:** Use the refined LTM not only for retrieval but as a durable mechanism for continuously steering LLM behavior and decision quality.
*   **Phase 6:** Support controlled Knowledge Base absorption into LTM so curated Spaces can be distilled into a durable skill/expertise depot.
*   **Phase 7:** Evolve from "Avatar of a KB" toward "Avatar with accumulated expertise," where absorbed KBs become durable capabilities and live KB attachment becomes optional rather than defining.

**Performance & Architectural Considerations:**
Adding Categorical data to the `TextIndex` creates a lifecycle coupling issue when Categories are managed/refactored.
* **The Refactor Problem:** In the `MemoryManager`'s asynchronous `SleepCycle`, dense categories are periodically evaluated by the LLM and broken down (split/re-associated). If category strings are hard-indexed into the `TextIndex` for an `Item`, then every time an `Item` moves between categories during a sleep cycle, we must also issue an update/re-index command to the `TextIndex` to reflect its new category strings.
* **Slower Category Management:** This couples the fast B-Tree vector re-assignments with heavier Text Index I/O operations, potentially slowing down the previously streamlined Background `SleepCycle` consolidation.

**Strategy:**
We will implement category-to-text-index synchronization in the future, when we have stabilized the LLM-managed Categories to the point where the semantic clustering creates a stable ontology. Once the `SleepCycle` matures and no longer requires frequent "movements" or re-associations of items across Categories, the I/O penalty of re-indexing text will become negligible.

Furthermore, by combining this with the semantic taxonomy graph (`Category.ChildrenIDs` and `Category.ParentIDs`), we can potentially achieve near O(log C) traversal at query time to rapidly eliminate broad swaths of vector space.

We have opted to delay the implementation of this advanced crawler/search for now, but the B-Tree underlying structure and dynamic vector boundaries are fully prepared to support it when necessary.
