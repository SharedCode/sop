# SOP AI Platform: Current Design Plan 

## Upcoming Designs: The Cognitive Memory Architecture & Tri-State Bridge

The SOP Copilot is adopting a tri-state memory architecture structurally inspired by cognitive science. This architecture provides session continuity, experiential learning, and "sellable" Knowledge Base compilation.

### 1. Working Memory (MRU) - [UPDATED: Session-Scoped]
*   **Mechanism**: A fast, in-memory array of structs (`[]MRUItem`) attached to the `Session` state, ensuring it is contiguous for the User regardless of which agent is currently active. 
*   **Workflow**: When an Avatar successfully executes a tool, the final context is mapped. Omni orchestrates passing a copied snapshot of the Session MRU into the Avatar as context. The Avatar reads this "whiteboard", does its work, and returns its response to Omni. Omni locks the Session Mutex and appends the final result to the global MRU.
*   **Context Continuity (Dynamic Semantic Injection)**: Rather than falling back to placeholder text ("dummy" logs) during multi-turn exchanges, the engine dynamically pipes actual semantic RAG chunks (`kb.SearchSemantics`) straight into `MarkMRUCategory()`. If a user's follow-up query is semantically bare, the system seamlessly pulls the `Carried-Over Playbook Context` directly from the prior exchange in the MRU. This reliably eradicates conversational amnesia across the episodic ReAct loop.
*   **Usage**: Guarantees Thread Safety under high-speed `Ask()` concurrency, prevents conversational amnesia between Avatars, and keeps mutations strictly at the Session layer.

### 2. Episodic Memory (STM / Short-Term Memory) - [UPDATED: Avatar-Scoped]
*   **Mechanism**: A completely physically isolated B-Tree per agent (`stm_<agent_id>`) attached to the Avatar's explicit `MemoryUnit`. It has entirely dropped the monolithic global `Service` channel.
*   **Workflow**: The Avatar operates a private, localized batch-write loop connected to its own private channel. During its internal execution loop, it logs execution snapshots to its channel without opening SQL transactions. A background worker specifically belonging to that Avatar periodically flushes its local channel to the `stm_<agent_id>` tree natively.
*   **Usage**: Massively scales the ReAct loop without cross-session bottlenecks, ensures O(1) native Avatar memory decommissioning, and provides a personal "private scratchpad" strictly invisible to other avatars.



### 3. Declarative vs. Episodic Long-Term Memory (Bridging the Gap)
A major product differentiator is balancing "Curated" UI Knowledge Bases (Declarative) against Auto-Enriched Conversational Memory (Episodic LTM). Our strategy relies on two explicit bridges upcoming for implementation:

#### A. JIT Semantic Recall
*   Instead of blindly dumping hardcoded LTM namespaces (`"memory"`, `"term"`, `"schema"`) into every prompt, the `Ask()` loop will embed the latest user query, perform a fast semantic search against the user's personal LTM Vector database, and inject similarity hits. 

#### B. The "Document To Space" Tool (Declarative Bridge - Pending Implementation)
*   A new tool (e.g. `document_to_space`) will be available to the LLM. 
*   When the conversational loop successfully resolves a complex issue, the LLM can offer to document that insight directly into the user's pristine, "Sellable" UI spaces.

### 4. Memory Unit Boundaries (Omni vs. Avatar Isolation)
To support multi-agent orchestration (the Omni Architect delegating tasks to specific Avatars/Sub-Agents), we must establish explicit memory boundaries (`memory_unit` scoping) to prevent context pollution and ensure data privacy.

#### A. MRU Boundary (Working Memory Scoping)
*   **Omni**: Maintains a global MRU that spans the entire user session, tracking top-level context switching.
*   **Avatars**: Each Avatar instance initializes with a scoped, localized MRU injected by Omni, but its internal MRU shifting during sub-tasks does NOT automatically bleed back into the global Omni MRU unless explicitly returned in the Avatar's final payload.

#### B. STM Boundary (Episodic Logging Isolation)
*   **Omni**: Logs high-level orchestration episodes and final resolutions delivered to the user.
*   **Avatars**: Logs domain-specific execution episodes with an explicit `agent_id` or `avatar_id` tag. The background `SleepCycle` will respect these tags when vectorizing, ensuring that avatar-specific reasoning stays associated with the correct persona.

#### C. LTM Boundary (Knowledge Base Access) - [UPDATED: AllowedKBs]
*   The Avatar has a single private physical Vector DB (`ltm_<agent_id>`) for its own semantic procedural learnings swept during its private Sleep Cycle.
*   The `AllowedKBs` slice dictates which generalized or organizational Spaces (like "API Docs" or "HR") the Avatar is permitted to semantically recall context from or enrich (if permissions allow). It never spills its personal STM data into these foreign spaces.
### 5. Knowledge Base (KB) LLM Enrichment Pipeline 
The Knowledge Base enrichment process ("SleepCycle") translates raw, temporally-logged STM thoughts into structured, semantically searchable LTM namespaces. This enables scaling out facts reliably.
*   **Summarization**: Raw episodic data is passed to the LLM (via `GenerateSummaries`) using a targeted Prompt that extracts small, standalone factual observations.
*   **Vectorization**: The decomposed facts are embedded into float representation vectors.
*   **Mathematical Clustering**: Distance mathematics checks if the new vectors align perfectly with existing `CenterVector` boundaries (Cosine distance < `MaxMathCategoryDistance`).
*   **Fallback Cataloging via LLM**: If facts are mathematically "orphaned" or too divergent, the LLM acts as a taxonomic organizer (`GenerateCategories`), forcing categorization against predefined or dynamically generated labels using an injection of Persona.
*   **Category Stabilization**: `TriggerSleepCycle` solidifies the schema periodically recalculating CenterVectors and `VectorHash` for every established space.

## 6. Architectural Philosophy: Native Semantic Instruction vs Binary Hardcoding
We have strictly banned the practice of hardcoding prompt engineering limits inside the Go application binary (e.g. constant strings in `copilottools.go`). 
*   **Semantic Overrides**: Instead of injecting engine-agnostic DSL parameters (like "NEVER use JSONLogic, use CEL" inside `copilottools.go`), these architectural constraints are codified natively inside the Playbooks/Knowledge Bases (like `SYSTEM_KNOWLEDGE.md` / `sop_base_knowledge.json`).
*   **Dynamic Brain Alignment**: This ensures the "Dynamic Brain" Retrieval-Augmented Generation execution can natively map and constrain LLM boundaries per-domain without redeploying backend Go binaries. The LLM must "learn" the platform syntaxes directly (and solely) through semantic retrieval context injection.
