# SOP Demo Walkthrough

This guide will walk you through the key features of the SOP Data Manager, from initial setup to using the AI Copilot.

## Prerequisites

Ensure the SOP Database Server is running.
- **Run Locally**: `./sop-httpserver` (or `go run tools/httpserver/main.go` for developers)
- **Access**: Open your browser to [http://localhost:8080](http://localhost:8080)

---

## Step 1: The Setup Wizard

When you first access the server, you will be greeted by the **Setup Wizard**. This wizard configures the system registry and creates your first database.

1.  **System Configuration**:
    *   **Registry Path**: Leave as default (`/tmp/sop_data/registry` or similar) or choose a permanent location.
    *   **Port**: Default is `9092` (Note: The server might be running on 8080, but this configures the internal registry port).

2.  **Create Your First Database**:
    *   **Database Name**: Enter a name, e.g., `demo_db`.
    *   **Data Path**: Leave default or choose a location.
    *   **Type**: Select **Standalone** (easiest for testing) or **Clustered** (requires Redis).
    *   **Populate with Demo Data**: **CHECK THIS BOX**.
        *   *This option generates a sample E-commerce dataset (Users, Products, Orders) so you can test features immediately.*

3.  **Finish**:
    *   Click **Finish**. The server will initialize the database and reload the page.

---

## Step 2: Data Exploration

Once the page reloads, you will see the **Dashboard**.

1.  **View Stores**:
    *   In the sidebar, locate your database (`demo_db`).
    *   Expand it to see the **Stores** (Tables): `users`, `products`, `orders`.
    *   Click on **`products`**.

2.  **Grid View**:
    *   You will see a list of products (e.g., "Laptop", "Smartphone").
    *   **Edit**: Click a cell to display the Item Details pane & allow edit of a value (e.g., change price).
    *   **Add**: Click "+" (Add Item) to manually add a new product.

---

## Step 3: AI Copilot (Chat with Your Data)

The **AI Copilot** allows you to query your database using natural language. It converts your questions into deterministic database scripts.

1.  **Open Chat**:
    *   Click the **Chat Icon** in the bottom-left corner.

2.  **Try These Queries**:
    *   *"Show me all products that cost more than $500"*
        *   *Observe*: The agent writes a script using `select` with filters.
    *   *"Show me all users and their orders"*
        *   *Observe*: The agent performs a join.
    *   *"Add a new user named 'Alice' with email 'alice@example.com'"*
        *   *Observe*: The agent uses the `add` tool.

3.  **View The Script**:
    *   After the AI answers, you can see the actual script it generated and executed. This script is reusable!

---

## Step 4: Scripting (Advanced)

You can write your own scripts to automate tasks.

1.  **Open Script Editor**:
    *   Click the **Code Icon** `</>` in the top-right toolbar.
    *   Click **"New Script"**.

2.  **Write a Script**:
    *   Enter the following simple script:
    ```json
    [
      {
        "type": "command",
        "command": "select",
        "args": {
          "store": "users",
          "limit": 5
        }
      }
    ]
    ```

3.  **Run**:
    *   Click the **Play** button.
    *   See the results in the output pane.

---

## Database Management

You can now manage multiple databases directly from the UI.

### Adding a Database
1.  In the Sidebar, next to the **Databases** header, click the **+** (Add Database) button.
2.  **Basic Details**: Enter a name (e.g., `analytics_db`) and a data path.
3.  **Type**: Choose `Standalone` or `Clustered`.
4.  **Advanced Mode**: Toggle this to configure:
    *   **Stores Folders**: Specify multiple paths for data striping.
    *   **Erasure Coding (Data Files)**: Configure Data Chunks and Parity Chunks for high availability.
        *   **Pro Tip (The "Catch-All" Bucket)**: When defining Erasure configs, leaving the `Key` field **empty** tells SOP to use this configuration as the **Global Fallback**. Any store that doesn't match a specific key rule will automatically be stored using this configuration. This is the simplest way to ensure all your data is protected without micro-managing every store.

### Deleting a Database
1.  Select the database you want to delete from the dropdown.
2.  Click the **Trash Icon** in the Databases header.
3.  **Confirm**: Type the name of the database to confirm deletion. **Warning**: This will delete all data on disk for that database.

---

## Next Steps

- Check out [GETTING_STARTED.md](GETTING_STARTED.md) for installation details.
- Read [ARCHITECTURE.md](ARCHITECTURE.md) to understand the internals.
