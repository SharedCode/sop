# AI Assistant User Guide

The SOP AI Assistant is a powerful, conversational interface for interacting with your SOP databases. It allows you to query data, perform CRUD operations, and automate complex workflows using natural language.

## Core Philosophy: Stateless vs. Stateful

To ensure system stability and prevent "dangling transactions," the Assistant operates in two distinct modes:

1.  **Stateless (Interactive & Recording)**: Every prompt is an independent unit of work.
    *   If you ask "Select all users", the Assistant opens a transaction, reads the data, and **immediately closes** the transaction.
    *   This applies even when **Recording**. Each step you record is executed and committed immediately.
2.  **Stateful (Playback)**: When **Playing a Macro**, the Assistant can maintain a transaction across multiple steps.
    *   This allows macros to perform complex, multi-step atomic operations (e.g., "Transfer funds: Debit A, Credit B").
    *   If any step fails, the entire macro transaction can be rolled back.

---

## 1. Natural Language Data Access

You can ask the Assistant to retrieve data using plain English. It translates your intent into optimized B-Tree lookups.

### Listing Resources
*   **"Show me all databases"** -> Lists available databases.
*   **"What stores are in the 'users' database?"** -> Lists tables/stores in that DB.

### Selecting Data
The `select` tool is powerful and supports filtering and field selection.

*   **Basic**: "Get the first 10 records from the 'users' store."
*   **Filtering**: "Find users in the 'users' store where the 'role' is 'admin'."
*   **Field Selection**: "Show me just the 'username' and 'email' for all users."
*   **Scanning**: "Scan the 'logs' store for entries with 'error' in the message." (Note: Scanning large stores can be slow; prefer key lookups).
*   **Ordering**: SOP stores are B-Trees and are naturally ordered by their Keys. Therefore, explicit `ORDER BY` clauses are not supported (and not needed). You always operate in the native B-Tree sort order.

### Finding Specific Records
*   **Exact Match**: "Find the user with key 'user_123'."
*   **Nearest Match**: "Find the user closest to 'user_125'." (Useful for finding range boundaries).

---

## 2. CRUD Operations

You can modify data directly.

### Adding Data
*   **"Add a new user to 'users' with key 'u_99' and name 'Alice'."**
*   **"Insert this JSON into 'config': {...}"**

### Updating Data
*   **"Update user 'u_99' and set 'status' to 'active'."**
*   **"Change the email for 'u_99' to 'alice@example.com'."**

### Deleting Data
*   **"Delete the record 'u_99' from 'users'."**

> **Note**: In **Stateless Mode**, these operations commit immediately. If you need to do multiple updates atomically, consider using a Macro.

---

## 3. Macro Management (Automation)

Macros allow you to record a sequence of actions and replay them later. This is "Natural Language Programming."

### Recording a Macro
1.  **Start**: Type `/record my_new_macro`.
    *   **Default (Compiled)**: Records the exact *actions* (tools) you perform. When played back, it executes these actions directly (fast, deterministic).
    *   **Interactive Mode**: Type `/record my_new_macro --ask`. Records your *prompts*. When played back, it asks the AI again (slower, but adapts to new data/context).
2.  **Teach**: Perform your actions step-by-step.
    *   "Select users where role is admin."
    *   "For each user, check their last login."
    *   "If last login is > 30 days, set status to inactive."
3.  **Stop**: Type `/stop`.

The macro is saved **incrementally** after every step. If you crash or disconnect, your progress is safe!

### Playing a Macro
*   **Run**: Type `/play my_new_macro`.
*   **With Arguments**: If your macro takes parameters (e.g., `user_id`), the Assistant will prompt you for them or you can provide them in the chat.

### Managing Macros
*   **List**: "List all macros."
*   **Inspect**: "Show me the steps for 'my_new_macro'."
*   **Refactor**: If you just had a great conversation and want to save it as a macro *after the fact*, say: "Refactor that last interaction into a macro named 'audit_workflow'."

---

## 4. Advanced Features

### Explicit Transaction Management
For complex, multi-step manual operations without creating a macro, you can manually control the transaction:
*   "Begin a transaction."
*   (Perform multiple updates...)
*   "Commit transaction."

### Swarm Computing (Async)
When defining macros manually (or asking the Assistant to edit them), you can mark steps as `is_async: true`. This allows the Assistant to execute multiple heavy tasks in parallel (e.g., "Summarize these 50 documents").

---

## Example Session

```text
User: /record onboard_user
Assistant: Recording started for macro 'onboard_user'.

User: Add a user to 'users' with key 'new_guy' and status 'pending'.
Assistant: Added user 'new_guy'. (Step recorded)

User: Add a log entry to 'audit_logs' saying "User new_guy created".
Assistant: Added log entry. (Step recorded)

User: /stop
Assistant: Recording stopped. Macro 'onboard_user' saved with 2 steps.

User: /play onboard_user
Assistant: Executing 'onboard_user'...
1. Added user...
2. Added log entry...
Done.
```
