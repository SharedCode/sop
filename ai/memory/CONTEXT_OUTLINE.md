# AI Context Outline

This document serves as the master map of available knowledge domains and procedural layers within the SOP platform. The AI Agent uses this outline during the `ClassifyTaskIntent` phase to dynamically determine which specific context files to load for a given user request to prevent prompt bloating.

## 1. Domains

The platform operates across two primary domains. A task may involve one or both.

*   **Stores**: Involves managing raw B-Tree items and standard persistence operations (e.g., inserting, fetching, updating, or deleting records).
*   **Spaces**: Involves managing Abstract Knowledge instances, vector search, clustering, and semantic data structure interactions (e.g., space minting, finding vectors).

---

## 2. Layers

Context is loaded in progressive layers depending on the complexity of the task.

### Layer 1: API Tools (Atomic Actions)
Defines the strict usage constraints, allowed parameters, and schema details for the tool registry components. 
*   **When to include**: Required any time the LLM needs to take action against a domain (e.g., "insert a record", "create a space").
*   **References**:
    *   `Stores API Tools Registry`
    *   `Spaces API Tools Registry`

### Layer 2: Domain Instruction Manuals (Orchestration)
Defines multi-step workflows, best practices, data format constraints, and domain-specific chaining patterns.
*   **When to include**: Required when a task is complex, involves querying or writing across multiple stores/spaces, or requires coordinating multiple API tools to achieve the objective.
*   **References**:
    *   `ai/memory/STORES_MANAGEMENT.md` (Stores Orchestration)
    *   `ai/memory/SPACE_MANAGEMENT.md` (Spaces Orchestration)

### Layer 3: Cross-Domain Orchestration
Defines architectural constraints and workflows for bridging the Stores and Spaces domains.
*   **When to include**: Required only when the task explicitly involves mixing logic between Stores and Spaces in a single operation.
*   **References**:
    *   `ai/memory/DOMAIN_MIX_AND_MATCH.md`
