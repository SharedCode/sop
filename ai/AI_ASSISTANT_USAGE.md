# AI Assistant User Guide

The SOP AI Assistant is a powerful, conversational interface for interacting with your SOP databases. It allows you to query data, perform CRUD operations, and automate complex workflows using natural language.

## Core Philosophy: Stateless vs. Stateful

To ensure system stability and prevent "dangling transactions," the Assistant operates in two distinct modes:

1.  **Stateless (Interactive & Recording)**: Every prompt is an independent unit of work.
    *   If you ask "Select all users", the Assistant opens a transaction, reads the data, and **immediately closes** the transaction.
    *   This applies even when **Recording**. Each step you record is executed and committed immediately.
2.  **Stateful (Playback)**: When **Playing a Script**, the Assistant can maintain a transaction across multiple steps.
    *   This allows scripts to perform complex, multi-step atomic operations (e.g., "Transfer funds: Debit A, Credit B").
    *   If any step fails, the entire script transaction can be rolled back.

---

## 1. Natural Language Data Access

You can ask the Assistant to retrieve data using plain English. It translates your intent into optimized B-Tree lookups.

### Listing Resources
*   **"Show me all databases"** -> Lists available databases.
*   **"What stores are in the 'users' database?"** -> Lists tables/stores in that DB.

### Selecting Data
The `select` tool is powerful and supports filtering and field selection.

*   **Basic**: "Get the first 10 records from the 'users' store."
*   **Filtering**: "Find users in the 'users' store where the 'role' is 'admin'." Supports MongoDB-style operators for comparisons: `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$eq`. Example: "Select employees where age > 30" (Assistant converts this to `{"age": {"$gt": 30}}`).
*   **Field Selection**: "Show me just the 'username' and 'email' for all users."
*   **Scanning**: "Scan the 'logs' store for entries with 'error' in the message." (Note: Scanning large stores can be slow; prefer key lookups).
*   **Ordering**: SOP stores are B-Trees and are naturally ordered by their Keys. Therefore, explicit `ORDER BY` clauses are not supported (and not needed). You always operate in the native B-Tree sort order.
*   **UI Display Note**: When selecting specific fields, the backend returns them in the requested order (e.g., `select salary, name` returns `salary` then `name`). However, the **UI Grid** always displays Key fields (columns from the Key object) *before* Value fields (columns from the Value object) for consistency. If you request a Value field followed by a Key field, they will appear as Key then Value in the grid. The raw JSON response (accessible via API) preserves your requested order within the Key and Value objects respectively.
*   **Views (Scripts)**: You can use a Script as a data source! If you have a script named 'active_users_view' that returns a list of users, you can query it like a table: "Select name, email from 'active_users_view'". This allows you to create complex "Views" using scripts (even with Joins) and query them simply. **Streaming Support**: Unlike traditional views that might materialize results, SOP streams script output directly. Field selection is applied "late-bound" as items flow through, ensuring high efficiency even for complex pipelines.

### Finding Specific Records
*   **Exact Match**: "Find the user with key 'user_123'."
*   **Nearest Match**: "Find the user closest to 'user_125'." (Useful for finding range boundaries).

### Efficient Query Scenarios

SOP is a high-performance database that uses B-Trees. To get the maximum speed (especially on large datasets), structure your questions to leverage the Index (Key) structure.

**How to write fast queries:**
The "Index" is defined by the Key fields of your store.
*   **Fast**: Filtering by the *first* field(s) of the Key (Prefix Match).
*   **Fast**: Joining on the Key fields.
*   **Slow**: Filtering by a field that is *not* at the start of the Key (requires a full scan).

| Scenario | Index (Key) Structure | Query Example | Status |
| :--- | :--- | :--- | :--- |
| **Exact Match** | `[Region, Dept]` | "Find employees in 'US' 'Sales'" | ⚡ **Fast** |
| **Prefix Match** | `[Region, Dept]` | "Find employees in 'US'" | ⚡ **Fast** |
| **Natural Sort** | `[Region, Dept]` | "Find 'US' employees, ordered by Dept" | ⚡ **Fast** |
| **Skip Prefix** | `[Region, Dept]` | "Find employees in 'Sales'" (Skipped Region) | 🐢 **Slow** (Full Scan) |
| **Sort Conflict** | `[Region, Dept]` | "Find 'US' employees, ordered by Dept DESC" (If index is ASC) | 🐢 **Slower** (Buffered) |

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

> **Note**: In **Stateless Mode**, these operations commit immediately. If you need to do multiple updates atomically, consider using a Script.

---

## 3. Script Management (Automation)

Scripts allow you to record a sequence of actions and replay them later. This is "Natural Language Programming."

### Recording a Script
1.  **Start**: Type `/record my_new_script`.
    *   **Default (Compiled)**: Records the exact *actions* (tools) you perform. When played back, it executes these actions directly (fast, deterministic).
    *   **Interactive Mode**: Type `/record my_new_script --ask`. Records your *prompts*. When played back, it asks the AI again (slower, but adapts to new data/context).
2.  **Teach**: Perform your actions step-by-step.
    *   "Select users where role is admin."
    *   "For each user, check their last login."
    *   "If last login is > 30 days, set status to inactive."
3.  **Stop**: Type `/stop`.

The script is saved **incrementally** after every step. If you crash or disconnect, your progress is safe!

### Playing a Script
*   **Run**: Type `/play my_new_script`.
*   **With Arguments**: If your script takes parameters (e.g., `user_id`), the Assistant will prompt you for them or you can provide them in the chat.

### Managing Scripts
*   **List**: "List all scripts."
*   **Inspect**: "Show me the steps for 'my_new_script'."
*   **Refactor**: If you just had a great conversation and want to save it as a script *after the fact*, say: "Refactor that last interaction into a script named 'audit_workflow'."

---

## 4. Advanced Features

### Explicit Transaction Management
For complex, multi-step manual operations without creating a script, you can manually control the transaction:
*   "Begin a transaction."
*   (Perform multiple updates...)
*   "Commit transaction."

### Swarm Computing (Async)
When defining scripts manually (or asking the Assistant to edit them), you can mark steps as `is_async: true`. This allows the Assistant to execute multiple heavy tasks in parallel (e.g., "Summarize these 50 documents").

---

## 5. Session Tools

*   **/last-tool**: Displays the exact JSON instructions (tool name and arguments) of the last executed action. Useful for debugging or verifying what the Assistant actually did.

---

## Example Session

```text
User: /record onboard_user
Assistant: Recording started for script 'onboard_user'.

User: Add a user to 'users' with key 'new_guy' and status 'pending'.
Assistant: Added user 'new_guy'. (Step recorded)

User: Add a log entry to 'audit_logs' saying "User new_guy created".
Assistant: Added log entry. (Step recorded)

User: /stop
Assistant: Recording stopped. Script 'onboard_user' saved with 2 steps.

User: /play onboard_user
Assistant: Executing 'onboard_user'...
1. Added user...
2. Added log entry...
Done.
```
