# Knowledge Base Search Tools

This page details the specialized search tools the AI Copilot uses to query its Long-Term Memory and associated Vector Databases.

## tool search_sop

The `search_sop` tool provides a mechanism for a **knowledge base query** against the master SOP platform documentation.
* **Usage**: When a user asks how SOP works, needs architectural patterns, or API semantics, use `search_sop`.
* **Behavior**: It executes a semantic search spanning the top-level architectural and API docs, returning formatted instructions and code snippets.

## tool search_domain_kb

The `search_domain_kb` tool performs a **domain query** within the user's specific business models or app workspace.
* **Usage**: When a user asks about their own models, specific handlers they wrote, or their internal business logic.
* **Behavior**: Isolated to the current Omni-Persona workspace index context.

## tool search_custom_kbs

The `search_custom_kbs` tool acts aThe `search_custom_kbs` tool acts aThe `search_custom_kbpeThe `search_custom_kbs` tool**:The `search_custom_kbs` tool acts aThe `search_custom_kbs` tool acts aThe `search_custom_kbpeThe `search_custom_kbs` tool**:The DomThe `search_Behavior**: Accepts an array of category hints to scope the search domain manually.
