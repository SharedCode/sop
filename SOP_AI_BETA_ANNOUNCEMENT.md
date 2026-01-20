# SOP AI Reaches Beta: The Era of the Self-Correcting Copilot

**January 20, 2026** — We are proud to announce a major milestone for the SOP platform. Today, we are officially moving the SOP AI Kit and Platform Tools to **Beta status**. 

This release is not just a stabilization of existing features—it introduces a fundamental shift in how developers interact with data. We are rebranding our "AI Assistant" to **SOP AI Copilot**, reflecting a leap in capability from a simple helper to a proactive, intelligent partner in your development workflow.

Here is what defines this new Beta release:

## 1. The "Self-Correcting" Intelligence

The headline feature of this release is the introduction of a cognitive architecture that mimics human memory. The AI Copilot generally struggled with the "Goldfish Memory" problem—forgetting context as soon as a window closed. We have solved this with a dual-layer memory system:

### Short-Term Memory (Contextual Awareness)
The Copilot now maintains a robust "Short-Term Memory" within the run-loop of the agent. It tracks the immediate history of your current session, understanding references to previous queries ("filter *that* list by date") and maintaining the state of your current investigation without needing constant restatement of facts.

### Long-Term Memory (System Knowledge)
This is the game-changer for Enterprise teams. When you correct the AI Copilot—for example, teaching it that a `status` field uses integers (`1`) instead of strings (`"Active"`)—it doesn't just learn for now; it learns **forever**.
*   **ACID-Backed Learning:** These corrections are committed to the `SystemDB` using SOP's transactional B-Trees.
*   **Shared Intelligence:** If you configure a shared `SystemDB`, a correction made by one developer is instantly available to the entire team. The junior developer's struggle in the morning becomes the senior developer's productivity boost in the afternoon.

## 2. Platform Tools: The New Environment Wizard

To support this advanced architecture, complexity must be managed. The new **Environment Wizard** simplifies the setup of the SOP ecosystem.
*   **Visual Setup:** Create, manage, and switch between completely isolated environments (Dev, Test, Prod) through a GUI.
*   **Database Management:** Effortlessly attach User Databases and System Databases to your environments.
*   **One-Click Demo:** Spin up a fully populated eCommerce Demo Database to test drive the "Relation Intelligence" immediately.

## 3. Data Manager & Relation Intelligence

The Data Manager interface has been overhauled to support "Relation Intelligence." 
*   **Schema Awareness:** The Copilot now understands the relationships between your Stores (tables). It can infer Foreign Keys and join logic without explicit mapping in every prompt.
*   **Natural Language Mining:** You can type complex requests like *"Find all customers who bought 'Electronics' in the last month and spent over $500"*, and the Copilot will generate the precise multi-step execution script to retrieve that data.

## 4. Rich Storage Layer API

Under the hood, we've exposed powerful new API capabilities that the AI leverage directly:
*   **Advanced Joins:** Optimized `join` operations that function effectively even on non-indexed fields (though indexes are preferred).
*   **Complex Filtering:** Support for nested conditions and advanced operators (`$in`, `$gt`, etc.) within the scripting layer.
*   **Scripting Engine:** A Turing-complete JSON-based scripting language that allows the AI to compose complex data pipelines (Filter -> Project -> Join -> Sort) that run close to the data.

## 5. From "Assistant" to "Copilot"

Why the rebrand? "Assistant" implies a reactive tool. "Copilot" implies a proactive partner. With the stability of Beta, the introduction of self-correction, and the deep integration into the storage engine, SOP AI is now capable of navigating your data alongside you, catching errors before they happen, and learning from your domain expertise to become smarter every day.

---

*The SOP AI Beta is available now. Check out the [Getting Started Guide](GETTING_STARTED.md) or run the Environment Wizard to begin.*
