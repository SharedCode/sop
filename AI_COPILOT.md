# Building a "Local Expert" AI: Embedding an Intelligent Copilot into SOP Data Manager

I'm excited to share a major update to the **SOP Data Manager**, the GUI tool for our Scalable Objects Persistence (SOP) engine. We've moved beyond simple CRUD operations and integrated a fully context-aware **AI Copilot** directly into the workflow.

This isn't just a chatbot overlay; it's a **ReAct (Reasoning + Acting) Agent** deeply integrated with the database backend, designed to act as a "Local Expert" for your data.

> **Important**: The AI Copilot requires an LLM API Key (e.g., Gemini, OpenAI) to function. You must provide your own key in the "Environment Configuration" settings. If no key is supplied, the AI Copilot features will be disabled.

### 🔌 New: "No-LLM" Production Mode
SOP recognizes that **Production Environments** often have strict security policies that forbid external API calls (Air-gapped) or require zero external dependencies.

We have introduced a **Direct Command Interface** that works entirely offline:
*   **Slash Commands**: You can bypass the LLM entirely by using slash commands (e.g., `/select database=users`).
*   **Zero Dependencies**: This mode requires NO internet connection and NO API Key.
*   **Full Power**: You get access to the exact same backend tools (`select`, `run_script`, `manage_knowledge`) that the Agent uses, but driven manually by you.

This confirms our philosophy: **The AI is a helper, but the robust machinery underneath is yours to command.**

## The Problem: Context Switching
Managing complex NoSQL data often involves jumping between a GUI to view items and a terminal to run queries or scripts. You might see a record, wonder "how many other records have this specific field value?", and have to switch context to write a script to find out.

## The Solution: A Floating, Context-Aware Copilot
We've refactored the UI to introduce a persistent, **floating AI widget**.
*   **Always Available**: It floats above your data grid, draggable and resizable, so you never lose sight of the data you're analyzing.
*   **Tool-Equipped**: The AI isn't hallucinating answers. It has access to real backend tools:
    *   `list_stores()`: To understand your database topology.
    *   `get_schema()`: To analyze the structure and index specifications of your B-Trees.
    *   `search()` & `select()`: To query data directly.
*   **Visual Feedback**: When the AI performs a search, it doesn't just tell you the results—it can trigger the main data grid to **refresh** and show you the actual records.

## The Paradigm Shift: Introducing "Agentic Data"
The problem with today's data is that it is too structured—too rigidly SQL (tabular) or too arbitrarily NoSQL (document-based). Traditional schemas force the user to organize data for the *database's* convenience. This makes it difficult for AI to manage, mine, and aggregate data accurately on behalf of the user.

We invented a new primitive: **Agentic Data**. 
Instead of forcing data into rigid columns, Agentic Data is auto-managed by the AI. It is stored semantically (via `Concept`, `Category`, and `Description` embeddings) inside dedicated **Playbooks (Knowledge Bases)**. 
- **AI-Native**: Because the data speaks the AI's language (natural language + embeddings), the LLM can seamlessly mine it, reason over it, and self-correct it without complex SQL joins.
- **Configurable Personas**: Each Playbook is imbued with "AI-ness". When creating a Playbook, users define a **System Prompt** and an **Embedder**, dictating exactly *how* the AI should manage and draw persona from that specific data pool.
- **UI Reflection**: In the SOP Data Manager, Agentic Data isn't displayed in a dense tabular grid like standard crud stores. It is rendered as a responsive Card UI, subtly reminding the user that this entity is an "AI Maintained" context module.

## Under the Hood: A Secure RAG Pipeline
We purposely designed a **Retrieval-Augmented Generation (RAG)** pipeline to address the critical security needs of database management.
1.  **Local Power, Global Intelligence**: The backend is built in **Go**, keeping the execution logic and data access strictly local.
2.  **Data Privacy & Filtration**: Your production data *never* leaves the environment. Only the prompt for interpretation and reasoning is sent to the LLM. The actual data retrieval and manipulation happen locally via the SOP library.
3.  **Policy Enforcement**: By decoupling the reasoning (LLM) from the execution (Local Tools), we can enforce strict policies on what the agent can and cannot do, removing the risks associated with giving an AI direct access to corporate data.
4.  **ReAct Loop**: The LLM reasons about which tool to use, but the Go backend intercepts these calls, validates them, and executes them safely using ACID transactions.

### Progressive ReAct Loop

The ReAct loop in SOP is progressive by design, not a blind repeat-until-success loop.

*   **Macro Then Micro**: Routing gates prepare the Ask frame first. The inner native ReAct loop then executes inside that frame without re-running the gates on every retry.
*   **Ask-Anchored Working State**: After each inner tool step, the engine compacts the current grounded state into an Ask-local MRU summary. The next LLM call sees current focus, preserved valid work, confirmed facts, missing pieces, and suggested next tools.
*   **Structured Tool Guidance**: Tools can return both a user-visible payload and an internal `progress_hint`. That hint can say what improved, what is still missing, and which tool should come next.
*   **Concrete Retry Visibility**: The retry prompt now includes the actual generated tool arguments together with failure details and the most recent successful context, so the model can refine the next step from the improving script rather than regenerate the whole plan.
*   **Bounded Repair, Not Infinite Retry**: The loop starts with a small retry budget and only extends it when new grounded facts, a proven recovery pattern, or positive progress hints show real convergence.
*   **Recipe Learning Inside the Loop**: When a repair pattern succeeds, the engine learns an implicit recipe from that success. That recipe is not just stored for later asks; it also counts as live progress in the current Ask.
*   **Hard Stop Semantics**: Tools can explicitly signal terminal outcomes such as blocked, anti-success, or hard error. In those cases, the loop stops immediately instead of wasting retries.

In practical terms, this means SOP's ReAct loop can see the returned agent context, the script it already tried, the exact failure, the grounded facts it has accumulated, and the missing pieces that still need research. That is what allows the LLM to preserve what is already correct and refine only the next delta.

## Why This Matters
This architecture allows us to create a **fully controllable, customizable AI** necessary for enterprise databases.
*   **Agentic Interfaces**: We build a robust set of tools and give an AI agent the agency to use them, but within a secure sandbox.
*   **For Developers**: You get a natural language interface to your raw data. "Show me the top 5 users created yesterday" translates automatically to a B-Tree range query.
*   **For Operations**: Troubleshooting becomes conversational. "Why is this store empty?" prompts the AI to check schemas and transaction logs.

## Natural Language Programming: The "Lego Blocks" Evolution
We've taken this a step further with our **Script System**, evolving it into a robust scripting engine built on "Atomic Lego Blocks."

Instead of asking an LLM to generate raw, potentially unsafe code (which is hard to debug and secure), we use a **Compiler Approach**:
1.  **Intent Extraction**: The LLM analyzes your request (e.g., "Find users older than 25 who haven't logged in for a month").
2.  **Block Assembly**: It assembles a script using our pre-built, high-performance atomic functions (the "Lego Blocks").
    *   **`compare(val1, val2)`**: A universal comparator handling strings, numbers, and dates seamlessly.

## The AI Enablers: How We Broke the "Complexity Ceiling"

Many organizations fail to adopt AI for core operations because they hit a "wall" of unreliability or technical debt. We built specific architectural features—our "Moat"—to bypass these limits.

### 1. "Lazy to Strict" Automatic Refinement
LLMs yield volatile code. One day they write verbose scripts; the next, they skip variable declarations.
*   **The Problem**: If you store the "lazy" code generated by an AI, your system becomes fragile and unreadable. Implicit assumptions break over time.
*   **Our Solution**: We allow the AI to be "lazy" during creation, but we **automatically refine** the script into "strict" mode before saving it.
    *   Implicit `begin_tx` becomes `{"result_var": "tx"}`.
    *   Implicit wiring (`open_store`) gets explicit dependencies (`"transaction": "tx"`).
*   **Benefit**: You get the speed of AI generation with the stability of handwritten, strictly-typed code.

### 2. Runtime Context & Safety Nets
Scripts often fail because context (like "which transaction is active?") is lost between steps.
*   **Our Solution**: The execution engine maintains a "Context Stickiness" layer. If a step requires a transaction but none is provided, the engine intelligently resolves the safest active context.
*   **Benefit**: Scripts are robust against minor syntax errors or "forgetful" AI models.

### 3. Self-Correcting Knowledge Base (Evolution to Omni Protocol)
Most AI tools reset their memory after every session or depend on heavily loaded monolithic system prompts that bloat context. In our V1 architecture, we introduced `manage_knowledge` to store rules persistently.
*   **Our Solution (V2)**: SOP uses the Butler Architecture. Knowledge constraints and vocabulary are embedded in dynamic Vector spaces (e.g. SOP KB) derived natively from markdown. When the agent plans execution, "The Butler" fetches precise contextual nodes. 
*   **Benefit**: The system naturally self-corrects its trajectory based on current architectural instructions without manual prompt rewiring or bulky prompt pollution, remaining lightweight and highly tailored per-query.

---

## Technical Appendix: Configuration
(This section remains unchanged...)
    *   **`matchesMap(item, criteria)`**: A MongoDB-style query evaluator supporting operators like `$gt`, `$lt`, `$in`, and `$eq`.
    *   **`toFloat(val)`**: Robust type conversion for numerical analysis.
    *   **`Scan(store, options)`**: High-performance B-Tree traversal (Range, Prefix, Forward/Backward).
    *   **`JoinRightCursor(left, right, key)`**: Optimized Right Outer Join iterator.
3.  **Safe Execution**: These blocks are compiled into a SOP Script that runs on the bare-metal Go engine.

### Why "Lego Blocks"?
*   **Agility & Control**: We can tweak the underlying implementation of a block (e.g., optimizing `compare` for speed) without changing the AI's behavior.
*   **Safety**: The AI cannot "hallucinate" dangerous code. It can only arrange the safe blocks we provide.
*   **Performance**: The resulting scripts run at native Go speeds, not interpreted Python/JS speeds.
*   **Streaming & Efficiency**: Results are streamed directly to the UI or REST client. This allows processing **SQL Joins on huge B-Trees with minimal memory**, as we never load the full dataset into RAM.

*   **SystemDB**: These scripts are stored in a dedicated B-Tree, effectively turning your database into a programmable application server.

## Next Steps
We are expanding the toolset to include **Vector Search** capabilities, allowing you to perform semantic queries ("Find documents related to 'scalability'") right from the same floating window.

Check out the code and try running it locally with Ollama!

#Golang #AI #LLM #Database #SOP #OpenSource #AgenticUI #ReAct

## Future ETL Iteration: CLI/Headless AI Streaming
In addition to the interactive UI (where a user copy/pastes a sample, visually previews the transformed data, and uses an embedded mapping script to onboard data into SOP), we plan to expose an API endpoint for background streaming:
1. **Background Streaming (No-UI):** Allows users to pass a large CSV/JSON file to SOP programmatically over a CLI/REST API.
2. **Headless Execution:** The system uses the previously generated mapping script (generated by the AI Copilot and saved during the interactive UI session) to rapidly convert data object-by-object in the background.
3. **Data Quality Fallback:** If the structural rules established by the script fail for certain records, those raw records can be pushed to a queue for "AI-in-the-loop" slow-fallback mapping, preventing the streaming job from failing completely on dirty data.

## The Multi-KB Omni Pipeline & Custom Personas

A major architectural advancement in SOP's AI is the **Multi-KB Omni Pipeline**. Because SOP allows you to build multiple domain-specific Knowledge Bases (e.g., HR Policies, Medical Legal, Engineering Docs), sending all of them to the LLM at once would cause context collapse and hallucination. 

To solve this, the Copilot uses an intelligent cognitive routing system:

1. **Contextual Classification:** When you ask a question, the AI first analyzes your Ask + Short-Term Memory to determine *which* Knowledge Base is most relevant.
2. **Dynamic Rule Adoption:** If the relevant KB has a **Custom Persona / System Prompt** configured, the AI will *read that prompt first* before executing any searches. 

### Writing Effective Space Personas
When you create or edit a Space (Knowledge Base) in the UI, you can enable **"Use as Persona (Agentic Context)"** and provide a System Prompt. This is your opportunity to program *how* the AI should handle this specific data silo.

**Best Practices for Space Personas:**
- **Don't just say what it is; say HOW to use it:** Instead of "You are a legal bot," say: *"You are a Legal compliance auditor. When searching this KB, always cross-reference terms and cite the exact clause."*
- **Use the '✨ Enhance with AI' Button:** If you only have a rough idea, click the **✨ Enhance with AI** button. Our backend will expand your draft into robust instructions optimized for the Omni Pipeline!

### The Two Modalities of the AI Copilot
SOP provides two distinct paradigms for deploying the AI Copilot:

**Type 1: The Orchestrator (Meta-Agent / Omni-Persona)**
This is the default mode for administrators and platform engineers.
* **How it works:** The LLM retains its core identity as the "SOP Master Architect" routing manager.
* **Best For:** Complex multi-domain orchestration, DB management, and Swarm computing design.

**Type 2: Target Identification / Avatar Mode (Exclusive Content Immersion)**
This mode is designed for deploying end-user sandboxes.
* **How it works:** The LLM does NOT suppress the `OMNI_PERSONA`. Instead, Omni acts as the invisible governing orchestrator. When an Ask routes to an Avatar KB, Omni "hands off" execution—injecting the Avatar's System Prompt and exclusively running the execution slice using the Avatar's constraints and domain logic.
* **Best For:** Deploying a strictly compliant Medical Advisor or Legal Auditor where the end-user should not interact with technical backend tools, but the backend still requires SOP's architectural governance.

### 🎮 The "Game Engine vs. Interactive Character" Analogy
To understand this architecture, game designers will recognize a familiar paradigm. The SOP AI functions exactly like a modern RPG simulation engine.

- **The Game Engine (Omni Persona):** Omni acts as the invisible backend supervisor. It handles the routing, memory allocation, context switching, safety rails, and data pipelines. The user never "talks" to the engine directly when playing the game.
- **The Interactive Character (Avatar Persona):** The Avatar is the immersive 3D character or environment the user interacts with in real-time. Because of SOP's partitioned memory spaces, this character maintains its own distinct continuity, dialog history, and personality.

This decoupling allows developers to hot-swap "Avatars" (Knowledge Bases/Playbooks) while the "Game Engine" (Omni) safely manages the core infrastructure and memory isolation in the background.

### The Mechanics of Avatar Continuity (Stateless Immersion)
From an infrastructure perspective, **Avatar Mode is stateless per request.** 
When a user chats with "The Doctor" KB, it works like this:
1. **The Ask:** The user sends a prompt.
2. **The Omni Wrapper (Governance):** The Omni-Protocol wakes up in the backend, remaining the foundational system prompt, to perform safety checks and semantic routing. 
3. **The Sandbox Handoff:** Once detected, the system branches the execution slice into Avatar Mode. Omni natively reads the Avatar's System Prompt and strictly boundaries its queries and limits for that interaction.
4. **The Illusion of Continuity:** Under the hood, memory structures (STM and LTM) natively partition and tag their payloads based on the Avatar context, meaning the automated SOP memory flows run identically to normal, just in a sandboxed lane.

### Avatar Boundary Protocol (LTM Partitioning)
To ensure total semantic isolation between avatars (e.g., preventing personal medical facts from leaking into an Engineering context), SOP implements the **Avatar Boundary Protocol**:

1. **Transcript Tagging (The State Ledger):**
   Every message exchange inside the Short Term Memory (STM) array includes a `AvatarScope` or `KBContextID` metadata tag. This lets the backend know which sandbox the conversation occurred within.
2. **Partitioned LTM Storage (Memory Sandboxing):**
   During the Omni "Sleep Cycle" (when the background agent extracts long-term facts), memories are deposited into **partitioned subspaces** in the VectorDB. Instead of writing to a generic `LTM_Space_{UserID}`, facts are written explicitly to `LTM_Space_{UserID}_Avatar_{KBID}`. 
3. **Contextual Sleep Cycle Injection:**
   When reviewing a tagged transcript, the summarization payload receives a dynamic compliance injection: *"You are reviewing a transcript that occurred within the restricted [Avatar] Sandbox. Focus extractions and send them explicitly to its partition."* This allows highly-relevant, isolated retrieval without cross-contamination.

### Knowledge Base Auto-Enrichment (Implicit vs. Explicit)
A core capability of SOP's AI pipeline is treating Knowledge Bases (KBs) not just as read-only silos, but as living, evolving data structures. However, to prevent curated "Master Manuals" (like official SOP or HR handbooks) from being polluted by user hallucinations or transient scratchpad thoughts, we enforce a strict **Implicit vs. Explicit Enrichment Strategy** governed by Role-Based Access Control (RBAC).

#### 1. The Configuration Flag (`AllowAutoEnrichment`)
Every Space (Knowledge Base) configuration now includes an `AllowAutoEnrichment` boolean flag.
* **`false` (Curated / Textbook Mode):** This is the default. The LLM treats the KB as a strict textbook. It will only write to this KB if the user *explicitly* commands it via prompt (and if the user has correct RBAC write permissions).
* **`true` (Dynamic / Sandbox Mode):** The LLM treats the KB as a collaborative workspace. It actively builds upon the base knowledge dynamically.

#### 2. Implicit Enrichment Pipeline (The Sleep Cycle)
When `AllowAutoEnrichment` is actively enabled for a KB, the background memory worker (`active_memory.go`) invokes the Space's natively built `TriggerSleepCycle(ctx)`. This kicks off a sophisticated data transformation pipeline to convert episodic user interactions into structured, semantically queryable Long-Term Memory:
1. **Summarization (`GenerateSummaries`)**: Raw `[]Thought` episodic data is fed into the LLM with a targeted prompt to decompose the stream into distinct logical vectors and standalone factual observations.
2. **Vectorization**: Extracted facts are embedded into mathematical float vectors (`EmbedTexts`).
3. **Mathematical Clustering (`MaxMathCategoryDistance`)**: Before defaulting to expensive LLM logic to classify facts, SOP utilizes blazing-fast local Cosine Distance mathematics to determine if the new vector perfectly aligns with an existing `CenterVector` taxonomic boundary.
4. **Fallback Formative Cataloging (`GenerateCategories`)**: If facts are "orphaned" by mathematical distance, they are batched to the LLM taxonomy organizer. The LLM evaluates the batch, incorporating the `PersonaContext`, and actively assigns (or generates) formal category labels.
5. **Schema Stabilization**: Center Vectors and `VectorHash` boundaries are recalculating, organically solidifying the Space matrix for future querying.

#### 3. Future Direction: Knowledge Base Absorption into LTM
Beyond implicit enrichment, SOP's long-term direction is to allow a Knowledge Base to be absorbed into Long-Term Memory as distilled expertise.
* **Current Default:** Right now, the practical model is still Avatar + KB + STM + LTM. The mounted KB is the active grounding surface for the LLM, and the product needs to keep stabilizing how that helps before we reduce dependence on live KB attachment.
* **Goal:** Let the AI accumulate multiple durable skills from curated Spaces, effectively creating a reusable internal depot of expertise.
* **Method:** Absorption should summarize and refine a Space into durable thoughts, rules, and skill fragments rather than naively copying the entire Space into prompt-time working memory.
* **Benefit:** The AI can become multi-talented over time while still respecting routing, prompt budgets, and isolation boundaries.
* **Constraint:** Absorbed knowledge must retain lineage to the source Knowledge Base so it can be refreshed, governed, or removed later if the source changes.
* **Operating-Model Shift:** If absorption matures, the system can evolve from an Avatar that is primarily defined by one mounted KB into an Avatar that carries its own STM/LTM plus previously absorbed expertise, then later absorbs additional KBs as new skills.

### Episodic Working Memory (Context Carry-over)

One of the great challenges of interacting with LLMs is the tendency to lose vital execution context—or "skills"—between prompts. Often called "Context Collapse", an LLM might successfully use a database grammar or a complex prompt instruction in turn one, only to forget those precise rules in turn two, requiring the user to re-prompt. 

The traditional solution is to inject those rules heavily into the root system prompt, causing prompt-bloat and high token costs.

We bypassed this by structuring memory deeply into **Episodes (Interactions).** 

*   **The Problem:** Hardcoding static tool grammar (like AST parameters) into a fixed global context means the agent becomes overly-specialized to one domain. Continual token injection ruins flexibility, particularly when handling multiple custom Avatar KBs.
*   **The SOP Architecture Solution (Dynamic Semantic Injection):** Working Memory (`sess.MRU`) is decoupled from Long-Term and Short-Term structures. We have eliminated all hardcoded parser instructions from the Go backend. Instead, the backend engine evaluates the *Metadata of the Previous Interaction* and natively searches the exact Semantic Knowledge Base chunk corresponding to the execution sequence.
    *   If a user continues a domain-specific conversation that utilizes the same Avatar or the same Knowledge Base (Space), and the prompt is too bare to retrieve new context chunks, the system automatically pulls the `Carried-Over Playbook Context` from the immediate prior interaction.
    *   **The Result:** Completely agnostic capabilities. By actively fetching boundaries and domain-rules natively from RAG rather than Go-binary strings, the AI seamlessly "remembers" its temporary working skills (like AST format constraints) dynamically, without permanently token-bloating the global system or sacrificing framework agnosticism.

## Advanced RAG: Decoupled Document Contexts (Document Mode)
SOP Knowledge Bases natively support an advanced **Document Mode**, significantly improving search quality and Retrieval-Augmented Generation (RAG) capabilities.

Instead of bloating indexes with massive payloads, Knowledge Bases decouple the semantic mapping from the canonical text:
*   **Many-to-One Relationships:** Multiple Categories and distinct Indexes (represented as Items' Summaries vectors) can securely reference the exact same canonical `Document`.
*   **High-Quality Search Hits:** By vectorizing specific, highly-distinct summaries or contextual chunks that point back to a single overarching source document, searches become hyper-focused. This design guarantees higher quality hits without losing the broader source context during generation.
*   **Untampered Source Delivery:** Because the raw reference points to the full document, it allows the LLM to ingest and share the source document exactly as is, completely untampered, without requiring complex and often inaccurate reassembly of fragmented bits and pieces.

## The Cascading Router Architecture
Moving beyond simple pure-LLM classifications or expensive K-Means VectorDB routing, the Copilot has evolved to use a highly deterministic, resource-efficient **Cascading Router**. When interacting through the AI, the query navigates through up to four specialized phases:

1. **Explicit Prefix Match (O(1))**: A constant-time check assessing whether the prompt prefixes match any defined `RoutingPrefix` configuration within the available Domain/Persona Playbooks.
2. **Global MRU Momentum Match (O(N))**: Scans the most recent conversation threads (Most Recently Used). If consecutive exchanges resolve to the same Avatar/Knowledge Base, the Copilot assumes that context and routing remains locked—avoiding unnecessary LLM overhead.
3. **Domain Reference Centroid Match (Vector Math)**: The query vector is mapped against localized, pre-calculated `DomainReference` target vectors specific to each Knowledge Base using Cosine Similarity thresholds.
4. **LLM Fallback (Heuristic Tiebreaker)**: Only defaults into LLM reasoning if the query fails to hit any of the deterministic thresholds or explicit patterns above. 

This Cascading strategy eliminates reliance on unstructured indexes and ensures extreme low-latency when determining user intent, acting purely as an ultra-fast structural heuristic rather than an unpredictably expensive LLM router.
