# Changelog

## v5.3.5
- **Range-over-func iterators for the in-memory B-Tree**: `All()` and `Range(from, to)` return `iter.Seq2` so callers can `for k, v := range b3.Range(102, 104)`. Range seeks straight to the start key; both are covered by unit tests. Quickstart example and demo GIF updated to use them.
- **Security: cleared all 32 open Dependabot alerts** (7 critical). Go: golang.org/x/crypto 0.52.0, golang.org/x/net 0.55.0, go-git/v5 5.19.1, go-billy/v5 5.9.0, cloudflare/circl 1.6.3. Java binding: jackson-databind 2.17.0 to 2.19.0.
- **Gated delivery pipeline** (`.github/workflows/deliver.yml`): every push to master runs build, tests, container packaging to GHCR (`sop-quickstart`), and a staging smoke test. Production promotion (image `:stable` tag plus GitHub Pages site deploy) sits behind a manual approval on the `production` environment.
- **Quickstart example** (`examples/quickstart`): zero-infrastructure in-memory B-Tree walkthrough (add, find, update, ordered scan). Packaged as a distroless container via `Dockerfile.quickstart`.
- **README demo GIF** recorded from the quickstart run; project site landing page added (`index.md`).

## SOP V2 build 54 (Upcoming)
- **Gate 1 Advanced KB Routing**: Major enhancements to specialized focused routing for knowledge base queries.
    - **Root Category Navigation**: Query `omni:<kb>` to display all root categories with item counts and subcategory information.
        - Example: `omni:sop` shows all top-level categories in the SOP knowledge base
        - Provides directory-style exploration without needing to know category names upfront
        - Navigation hints included for deeper exploration (e.g., "Navigate: omni:sop:language")
        - **Pagination**: 20 categories per page with `:page:<number>` or `/page/<number>` syntax
            - `omni:sop:page:2` or `omni:sop/page/2` - View page 2 of root categories
            - `omni:sop:language:page:3` or `omni:sop/language/page/3` - View page 3 of subcategories
            - Supports both `:` and `/` as separators (matches user's query style)
            - Shows page info: "(Page 2 of 5, showing 21-40 of 87)"
            - Navigation hints: "Previous: omni:sop:page:1 | Next: omni:sop:page:3"
            - LLM filtering suggestion for large sets (>40 categories)
    - **`:llm <instruction>` Meta-Token**: Added support for explicit LLM post-processing instructions using `:llm` suffix (e.g., `omni:sop:operations:performance:llm summarize top 3`).
        - **Clean Query Separation**: The `:llm` meta-token is automatically stripped from the KB search query and treated as post-retrieval guidance.
        - **TaskContextClassification Fields**: Added `CleanQuery` and `LLMInstruction` fields to properly separate user intent from meta-commands.
        - **Three-Way Routing**: Intelligent decision-making based on result count and `:llm` presence:
            - `:llm` present → LLM processes with instruction (highest priority)
            - 1-5 matches → Direct display (no LLM)
            - 6+ matches → Automatic LLM summarization
    - **Flexible Hierarchy Support**: Full support for any-depth category paths (e.g., `omni:kb:cat1:subcat1.1:subsubcat1.1.1:...`).
    - **Subcategory Navigation**: When a category path has no direct items (and no `:llm` instruction), automatically returns child categories with item counts and descriptions as navigation hints.
    - **Enhanced Parsing**: New `stripLLMInstruction()` function ensures consistent meta-token extraction across all query patterns.
    - **Architecture Improvements**: 
        - `getSubcategories()` function for root and path-level category display
        - `buildKBEnrichedQuery()` now uses clean queries without meta-tokens for proper LLM context
        - `trySpecializedFocusedRouting()` handles root navigation, flexible hierarchy, and meta-token parsing
        - Comprehensive test coverage for all routing patterns and hierarchy depths
    - **Roadmap - Quoted Text Search**: Proposed support for combined category + text queries (e.g., `omni:sop:language bindings "java tutorial"`).
    - **Documentation Updates**: Updated `AI_COPILOT.md`, `AI_COPILOT_USAGE.md`, and `IMPLEMENTATION.md` with comprehensive routing guides including root category navigation.

## SOP V2 build 53 (Upcoming)
- **Schema Format Enhancement**: Introduced flat schema format for better LLM understanding and correlation with Store Relations.
    - **New Fields**: Added `FlatSchema`, `KeyFields`, and `ValueFields` to `StoreInfo` for improved schema representation.
    - **Flat Schema**: Uses flat format without prefixes (e.g., `{"key": "string", "first_name": "string", "age": "number"}`) that directly correlates with relation field names.
    - **Field Lists**: `KeyFields` and `ValueFields` arrays explicitly identify which fields belong to the Key vs Value, enabling LLMs to correctly prefix fields when generating SQL-like predicates.
    - **LLM Compatibility**: The flat format follows JSON Schema standards and eliminates cognitive load for LLMs when mapping relation field names to schema fields.
    - **Backward Compatibility**: The legacy prefixed `Schema` field (e.g., `{"Key": "string", "Value.first_name": "string"}`) is maintained for existing tools and will be deprecated in a future major version.
    - **Automatic Inference**: Schema inference automatically populates both formats during B-Tree item insertion.
    - **Updated Instructions**: AI Copilot prompts now reference the flat schema format and guide LLMs to use `key_fields` and `value_fields` for proper field prefix determination.
- **Refactor**: Refactored `IndexSpecification` and `StoreInfo` to separate sorting logic from index definitions.
    - **CEL Expression**: The `CELexpression` field in `StoreInfo` is now the primary source for custom sorting logic.
    - **IndexSpecification**: Now strictly defines the fields used for indexing and optimization.
    - **Backward Compatibility**: Existing data stores where `IndexSpecification` contained the CEL expression are automatically detected and supported. No manual migration is required.
    - **Benefit**: This separation allows for cleaner metadata and enables the "Dual-Mode" architecture where native Go comparers and dynamic CEL expressions can coexist and interoperate seamlessly.
- **AI Copilot & Chat**:
    - **Structured Execution Results**: Enhanced the Script Execution Engine to emit structured events for every execution step (`step_start`, `record`, `outputs`). This ensures consistent real-time feedback for long-running scripts.
    - **Step Visibility**: The Chat interface and Script Runner now clearly demarcate each step (e.g., "**Step 1:** select"), providing better observability into the agent's reasoning process.
    - **Execution Indexing**: Implemented robust step indexing propagation to track progress across complex control flows and streamed results.
    - **Grounded Join Repair**: Tightened `execute_script` join guidance so multi-store repair prefers researched `relation + target` paths over invented join mappings, and clarified recovery prompts now preserve validation category, suggested fix examples, and attempted mappings when escalation to clarification is required.
- **UI Enhancements (Data Manager)**:
    - **CEL Editor**: Added a dedicated modal for editing `StoreInfo.CELexpression` with auto-generation capabilities based on Index Specifications.
    - **Bulk Delete**: Implemented a selection column in the data grid allowing users to select and delete multiple items at once.
    - **Mobile Support**: Improved responsiveness for mobile devices, including a fullscreen mode for the AI Chat Assistant.
    - **UX Improvements**: Added "Escape" key support for closing modals and improved column resizing behavior.

## SOP V2 build 52
- **Added a Data Browser utility**: A web-based tool to inspect and navigate SOP B-Tree repositories.
    - **Store Listing**: View all B-Trees in a registry.
    - **Data Grid**: Browse key/value pairs with pagination.
    - **Navigation**: Seamlessly navigate between data pages (Next/Previous).
    - **Search**: Find specific records using complex key inputs.
    - **JSON Inspection**: View complex value structures as formatted JSON.
