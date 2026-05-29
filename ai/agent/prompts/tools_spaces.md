# Spaces Manual
Use these rules when the user is working with Spaces.

<h2> Core Conventions</h2>
- A Space is a knowledge base organized into categories and items.
- For read/discovery flows, prefer `begin_tx(mode="read")` -> `list_space_categories` / `list_space_items` / `search_space` / `read_space_config` -> `commit_tx`.
- For generated content, use `mint_to_space` and pass the exact target Space name in `mint_to_space.kb_name`, even if the Space does not exist yet.
- If the user asks for multiple generated entries, call `mint_to_space` once per logical item unless they explicitly want one combined note.
- If the content is generated in the current conversation, generate it first, then store it with `mint_to_space`.
- Use `delete_space` for full Space deletion.

<h2> Vectorization & Config</h2>
- Only use vectorization tools when the user explicitly asks for vectorization, embeddings, reindexing, or semantic refresh.
- Use `vectorize_space_items` for specific changed items, `vectorize_space_categories` for a topic-wide refresh, and `vectorize_space` only for full reconciliation.
- If the user only asks to store, update, or delete knowledge, perform that mutation and stop there unless they also ask to vectorize.
- Use `read_space_config` to inspect enforced Space behavior and `update_space_config` to change it.

<h2> Transactionality</h2>
- `mint_to_space` manages its own transaction.
- Use `begin_tx(mode="write")` for transaction-managed mutations such as item/category deletion paths.
- Vectorization APIs manage their own transactional state.
- `delete_space` runs directly and should not be wrapped in `begin_tx` / `commit_tx`.

