# Changelog

## SOP V2 build 53 (Upcoming)
- **Refactor**: Refactored `IndexSpecification` and `StoreInfo` to separate sorting logic from index definitions.
    - **CEL Expression**: The `CELexpression` field in `StoreInfo` is now the primary source for custom sorting logic.
    - **IndexSpecification**: Now strictly defines the fields used for indexing and optimization.
    - **Backward Compatibility**: Existing data stores where `IndexSpecification` contained the CEL expression are automatically detected and supported. No manual migration is required.
    - **Benefit**: This separation allows for cleaner metadata and enables the "Dual-Mode" architecture where native Go comparers and dynamic CEL expressions can coexist and interoperate seamlessly.
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
