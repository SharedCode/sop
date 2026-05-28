# Spaces Manual
Use these rules when the user is working with Spaces.

<h2> 1. The Space Architecture</h2>
A Space is a specialized Knowledge Base structured around **Categories** (topics/domains) and **Items** (individual context chunks or documents).
- **Categories** group related knowledge.
- **Items** contain the actual textual `content` and semantic embeddings.

<h2> 2. Orchestration Workflows</h2>

<h3> A. Research and Discovery (Read)</h3>
When the user is exploring a Space:
1. **Explore Categories**: Use `list_space_categories` to see what high-level topics exist.
2. **Explore Items**: Use `list_space_items` to list what specific chunks exist within a category.
3. **Search Content**: Use `search_space` for semantic similarity queries to find the most relevant items across the space.

<h3> B. Data Mutation (Write)</h3>
When the user wants to store or update knowledge:
1. **Generated Content Storage**: Use `mint_to_space` to write generated knowledge into a Space. Ensure `content` is highly cohesive and meaningful out of context.
2. **Required Target Space**: When the user names a Space, pass that exact Space name in `mint_to_space.kb_name` even if the Space does not exist yet.
3. **Generated Lists**: If the user asks for multiple generated entries, call `mint_to_space` once per logical item unless they explicitly want a single combined note.
4. **Import vs Generate Rule**: If the content is generated in the current conversation, generate it first, then store it with `mint_to_space`.
5. **Full Space Deletion**: Use `delete_space` when the user wants to remove the entire Space.

<h3> C. The Vectorization Lifecycle</h3>
Vectorizing computes embeddings so data is searchable. Use a vectorization tool when the user explicitly asks for vectorization, embeddings, reindexing, or semantic refresh.
- Use `vectorize_space_items` to update specifically changed items.
- Use `vectorize_space_categories` to update an entire topic (if many items changed).
- Use `vectorize_space` only if triggering a full space reconciliation.
- Run vectorization as a single logical transaction in your reasoning steps where possible.

If the user only asks you to store, update, or delete knowledge, perform that mutation and stop there unless they also ask to vectorize.

<h3> D. Space Configuration</h3>
Spaces can be configured with specific personas, routing rules, and tool access controls to specialize how the AI behaves within them.
1. **Reading Config**: Use `read_space_config` to see the currently enforced setup (System Prompt, Allowed Tools, Routing Hooks).
2. **Setting Config**: Use `update_space_config` to adjust the currently enforced setup (System Prompt, Allowed Tools, Routing Hooks).

<h2> 3. Deletion Operations</h2>

Map the user's phrasing to the correct operation:

1. **If the user says `delete space Tasks`, `remove the Tasks space`, or `delete my Tasks space`**
	Use `delete_space(kb_name: "Tasks")`.

2. **If the user says `delete items from Tasks`, `remove these items from the Tasks space`, or `delete entries in Tasks`**
	Use the item-deletion path inside the existing Space.

3. **If the user says `delete categories from Tasks` or `remove categories in the Tasks space`**
	Use the category-deletion path inside the existing Space.

<h2> 4. Transactionality Guardrails</h2>
- Use `begin_tx(mode="write")` for transaction-managed mutations like `delete_space_items`.
- `mint_to_space` manages its own transaction.
- Use `begin_tx(mode="read")` for discovery operations like `list_space_categories` and `search_space`.
- Vectorization APIs manage their own transactional state.
- `delete_space` runs directly and should not be wrapped in `begin_tx` / `commit_tx`.

