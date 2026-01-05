# UI Architecture & Metadata Strategy

## Client-Side Persistence & Preferences
To ensure a seamless user experience across devices, the UI persists specific user preferences directly in the browser using `localStorage`. This avoids unnecessary server round-trips for purely presentational settings.

### Mobile Mode Strategy (`sop_ui_mobile_mode`)
The application supports a responsive design that adapts not just layout, but behavior, based on the device type. This is controlled by the `sop_ui_mobile_mode` preference.

*   **App Mode (Default)**:
    *   **Behavior**: Treats the mobile web view as a native app.
    *   **AI Widget**: Automatically opens and locks to full screen on mobile devices to maximize usability.
*   **Hybrid Mode**:
    *   **Behavior**: Balanced approach.
    *   **AI Widget**: Opens by default on mobile but remains minimizable, allowing access to the underlying data grid.
*   **Desktop Mode**:
    *   **Behavior**: Traditional web application behavior.
    *   **AI Widget**: Manual toggle only. Does not auto-open.

This preference is managed via the "Preferences" modal in the UI and applied immediately upon selection.

## Problem Statement: Hardcoded UI Logic
Currently, the SOP Data Manager UI (`tools/httpserver/templates/index.html`) contains hardcoded logic to handle specific display requirements for different stores. 

**Example:**
The `macros` store has a field named `database`. In the code, we explicitly check:
```javascript
if (currentStore === 'macros' && key === 'database') {
    // Render a dropdown populated from /api/databases
}
```
This approach is brittle and unscalable. As users create their own custom stores with unique requirements (e.g., a "User" store needing a "Role" dropdown, or a "Product" store needing a "Category" picker), we cannot keep adding hardcoded `if/else` blocks to the frontend.

## Proposed Solution: The "Meta Store" Pattern

To solve this, we propose treating UI configuration as **data**, stored within SOP itself. This aligns with the "SOP as a Platform" philosophy.

### 1. The `_meta_ui` Store
We will introduce a reserved store (or a convention-based store) called `_meta_ui` (or `sop_ui_config`). This store will map **Store Names** to **UI Schemas**.

**Schema Structure (Draft):**
```json
{
  "store_name": "macros",
  "fields": {
    "database": {
      "widget": "dropdown",
      "source": {
        "type": "api",
        "endpoint": "/api/databases",
        "value_field": "name",
        "label_field": "name"
      },
      "default_option": {
        "value": "",
        "label": "(None / Portable)"
      }
    },
    "steps": {
      "widget": "complex_tree",
      "modal_title": "Macro Steps Viewer"
    },
    "category": {
      "widget": "dropdown",
      "source": {
        "type": "static",
        "options": ["General", "Admin", "Data Processing", "Reporting"]
      }
    }
  }
}
```

### 2. Frontend Implementation
The frontend `showDetail` function will be refactored to be **data-driven**:

1.  **Load Metadata**: When opening a store, the frontend fetches the corresponding UI schema from `_meta_ui`.
2.  **Dynamic Rendering**: Instead of hardcoded checks, the renderer iterates through the item's keys and looks up the widget type in the schema.
    *   If `widget == 'dropdown'`, it fetches the source data (if API) or uses static options, then renders a `<select>`.
    *   If `widget == 'complex_tree'`, it renders the "View Complex" button.
    *   If no metadata exists, it falls back to the default text input.

### 3. Benefits
*   **Flexibility**: Users can define custom UI behaviors for their own data without touching the codebase.
*   **Consistency**: System stores (like `macros`) and user stores are treated identically.
*   **Scalability**: The UI code remains clean and generic, regardless of how many special field types are added.
*   **"Meta" Management**: Users can use the SOP Data Manager to edit the `_meta_ui` store itself, effectively building a "No-Code" admin interface builder.
