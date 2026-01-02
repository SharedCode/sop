# SOP Data Manager (HTTP Server & UI)

A powerful, web-based management suite for SOP B-Tree repositories. This tool transforms SOP from a library into a full-fledged SOP HTTP Server with a rich User Interface.

## Features
- **Multi-Database Support**: Seamlessly switch between different databases (e.g., Local Dev, Production Cluster) from a single UI.
- **Full CRUD Management**: Create, Read, Update, and Delete records directly from the UI.
- **RDBMS-Grade Indexing**: Leverages SOP's `IndexSpecification` to support rich, compound indexes with multiple fields and custom sort orders.
- **Universal Database Management**: Manage *any* SOP database and B-Tree, regardless of the data type or language used to create it.
- **Store Listing**: View and manage all B-Trees in a database.
- **Bulk Management Friendly**: Efficiently handle large datasets and batch operations.
- **Data Grid**: Browse key/value pairs with pagination, resizable columns, and row highlighting.
- **Navigation**: Seamlessly navigate between data pages (Next/Previous) to explore large datasets.
- **Advanced Search**: Perform complex queries on multi-field keys to jump directly to specific records.
- **JSON Inspection**: View and edit complex value structures as formatted JSON.
- **Database Configuration View**: Inspect critical database settings directly from the UI, including Erasure Coding parameters, Redis configuration, and Cache types.
- **CEL Expression Editor**: Integrated editor for defining and testing Common Expression Language (CEL) scripts for custom sorting logic.
- **Bulk Operations**: Select multiple items in the grid for batch deletion.
- **Natural Language SQL**: Execute complex Selects, Joins, and CRUD operations using plain English via the AI Assistant.
- **Mobile Optimized**: Fully responsive design with touch-friendly controls and a fullscreen AI assistant for managing data on the go.

## The Power of the SOP Data Manager: From Embedded to Enterprise

The SOP Data Manager is not just a simple admin tool; it is a demonstration of the SOP architecture's flexibility. Because SOP is a library that turns your application into the database engine, this SOP HTTP Server is effectively a **universal database server** that can adapt to any scale.

### 1. Embedded & Single-Node
For local development, IoT devices, or single-user desktop applications, the SOP Data Manager acts as a standalone SOP HTTP Server.
- **Zero Setup**: Just point it to a folder.
- **Instant Access**: Serve one or many databases from a single lightweight process.
- **Use Case**: A developer inspecting their local data, or an embedded device exposing a management interface.

### 2. Enterprise Swarm (Clustered)
In a large-scale enterprise environment, the SOP Data Manager shines as a stateless management node within your storage swarm.
- **Scalable Hosting**: Deploy the SOP Data Manager on **Kubernetes**, **AWS EC2 Auto Scaling Groups**, or **Linux Bare Metal** farms.
- **Dynamic Scaling**: Spin up as many instances of the SOP Data Manager as needed to serve different teams or departments.
- **Cluster-Aware**: When configured with the same Redis endpoint as your production applications, the SOP Data Manager participates in the same distributed transaction protocols.
- **Safe Production Access**: You can view, edit, and manage live production data safely. The SOP Data Manager respects all distributed locks, ensuring that manual admin actions never corrupt data or violate ACID properties, even while your high-throughput microservices are hammering the same data files.

## Usage

### Prerequisites
- Go 1.20+ installed.
- An existing SOP database (folder containing SOP data).

### Environment Variables

- `SOP_ALLOW_INVALID_MAP_KEY`: Set to `true` to bypass the validation that requires Map Key types to have an Index Specification or CEL Expression. This is primarily for testing purposes.
- `GEMINI_API_KEY`: **Required for AI Assistant**. The API key for Google Gemini Pro. You can obtain one from [Google AI Studio](https://aistudio.google.com/).
- `OPENAI_API_KEY`: Optional. The API key for OpenAI (ChatGPT) if you prefer to use GPT models.
- `SOP_ROOT_PASSWORD`: Optional. Sets the admin password for protected operations (like schema updates). Overrides the `root_password` in `config.json`.

## User Guide

### 1. Managing Stores

The Data Manager provides a comprehensive interface for managing your B-Tree stores.

*   **Add Store**: Click the **+** button in the sidebar next to "Stores". This opens a modal where you can define the store name, key/value types, and advanced settings like Index Specifications or CEL Expressions.
*   **Edit Store**: Select a store, then click the **Pencil** icon in the main header.
    *   *Note*: For populated stores, structural fields (Key/Value types) are locked to prevent data corruption. You can still edit the Description and Cache settings.
*   **Delete Store**: Select a store, then click the **Trash** icon in the main header. You will be asked to type the store name to confirm deletion.

### 2. AI Assistant & Macros

The built-in AI Assistant allows you to interact with your data using natural language and automate tasks using Macros.

*   **Open Assistant**: Click the floating chat button in the bottom-left corner.
*   **Natural Language SQL**: Perform complex queries without writing code.
    *   *Select*: "Find all users where Age > 25 and City is 'Seattle'."
    *   *Join*: "Join 'Users' and 'Orders' on 'UserID' and show me the top 5 spenders."
    *   *CRUD*: "Add a new user named 'John Doe' with age 30." or "Delete the record with key 'user_123'."
*   **Record Macro**: Tell the assistant to record your actions.
    *   *Command*: "Record a macro named 'MyMacro' to find users in New York."
    *   The assistant will generate the steps and save them to the `SystemDB`.
*   **Play Macro**: Execute a saved macro.
    *   *Command*: "Play macro 'MyMacro'."
    *   The assistant will execute the recorded steps and display the results.
*   **Parameterized Macros (Beta)**: You can now record macros with placeholders and pass values at runtime.
    *   *Record*: "Record a macro 'FindUser' that finds a user by 'UserID'."
    *   *Play*: "Play macro 'FindUser' with UserID='123'."
*   **View Macro Steps**:
    *   If the assistant returns a macro trace, it will be displayed as an interactive tree view in the chat.
    *   You can also view raw macro data in the **SystemDB** (see below).

### 3. Advanced Queries (SQL Joins & Macros as Views)

The AI Assistant supports complex queries that mimic SQL operations, even though SOP is a NoSQL Key-Value store. It achieves high performance by leveraging the underlying B-Tree structure.

*   **Inner Join & Prefix Queries**: Ask the assistant to join two stores.
    *   *Command*: "Join 'Users' and 'Orders' on 'UserID'."
    *   **Performance**: The engine utilizes **Prefix Queries** in Hash Joins, fully exploiting the B-Tree's `Find` and navigation APIs. This allows for efficient range queries and lookups without full table scans.
*   **Macros as Views**: You can use a Macro as a data source in your queries, effectively treating it as a SQL View.
    *   *Command*: "Select * from 'MyMacro' where Age > 20."
    *   *Join*: "Join 'Users' and 'MyMacro' on 'RegionID'."
    *   **Efficiency**: Because Macros stream their results, using them as Views is extremely lightweight. The system pipelines the data, allowing for complex transformations with minimal memory footprint.
*   **Agent Streaming**: Results are streamed in real-time from the Agent to the Data Manager.
    *   **Benefit**: You see results immediately as they are found.
    *   **Resource Utilization**: This streaming architecture ensures **lightweight resource utilization**, as the server doesn't need to buffer the entire result set. It combines the raw speed of B-Tree navigation with the flexibility of an AI agent to provide a complete, high-performance solution.

### 4. SystemDB

The `SystemDB` is a special database that holds internal SOP metadata, including Registry information and Macros.

*   **View SystemDB**: In the "Databases" dropdown in the sidebar, select **SystemDB** (if available/configured).
*   **View Macros**:
    1.  Select the **Macro** store within SystemDB.
    2.  Browse the list of macros.
    3.  Click on a macro to view its details in the "Item Details" panel. The `Value` field contains the JSON definition of the macro steps.

### Running

You can run the SOP HTTP Server in two ways: using command-line flags for a single database, or using a configuration file for multiple databases.

#### Option 1: Single Database (Quick Start)

From the root of the `sop` repository:

```bash
go run ./tools/httpserver -database /path/to/your/sop/data
```

#### Option 2: Multiple Databases (Recommended)

Create a JSON configuration file (e.g., `config.json`) to define your environments:

```json
{
  "port": 8080,
  "root_password": "optional_admin_password",
  "databases": [
    {
      "name": "Local Development",
      "path": "/tmp/sop_data",
      "mode": "standalone"
    },
    {
      "name": "Production Cluster",
      "path": "/mnt/data/prod",
      "mode": "clustered",
      "redis": "redis-prod:6379"
    }
  ]
}
```

**Security Note:** For production environments, it is recommended to set the root password using the `SOP_ROOT_PASSWORD` environment variable instead of storing it in the config file. The environment variable takes precedence over the config file setting.

```bash
export SOP_ROOT_PASSWORD="my_secure_password"
go run ./tools/httpserver -config config.json
```

Run the SOP HTTP Server with the config file:

```bash
go run ./tools/httpserver -config config.json
```

### Administrative Features

#### Store Metadata Override
The Web UI allows administrators to perform advanced maintenance tasks, such as updating the **Index Specification** and/or **CEL Expression** of an existing store. These are structural changes that can affect data retrieval and sorting.

*   **Protection**: These operations are protected and require the `RootPassword` (configured via JSON or `SOP_ROOT_PASSWORD` env var).
*   **Workflow**: Structural fields are disabled by default for non-empty stores. Click the **Unlock** button (lock icon), enter the Admin Token, and upon validation, the fields become editable.
*   **Warning**: A warning is displayed immediately after validation to remind you that modifying the schema of a populated store is an advanced operation that may impact existing data ordering.

### Important Note on Concurrency

If database(s) are configured in **standalone mode**, ensure that the http server is the only process/app running to manage the database(s). Alternatively, you can add its HTTP REST endpoint to your embedded/standalone app so it can continue its function and serve HTTP pages at the same time.

If **clustered**, no worries, as SOP takes care of Redis-based coordination with other apps and/or SOP HTTP Servers managing databases using SOP in clustered mode.

### Accessing
Open your browser and navigate to:
http://localhost:8080

## Topology and Architecture

SOP is designed to break the traditional database monolith. Unlike conventional RDBMS where a central server process controls all data access, SOP is a **library** that turns your application cluster into the database engine itself.

### Deployment Models

Understanding how to deploy SOP in the enterprise requires distinguishing between **Standalone** and **Clustered** modes.

#### 1. Standalone Mode
In **Standalone** mode, the database is managed with the expectation that the SOP HTTP Server instance (or your application) is the sole manager of the data.

- **Topology**: Single Instance.
- **Use Case**: Local development, single-user desktop apps, or embedded scenarios.
- **Concurrency**: You can open multiple browser tabs or windows pointing to the same SOP HTTP Server. The server handles the coordination internally.
- **Guarantee**: Changes are persisted with full ACID guarantees.
- **Constraint**: Only *one* application process (the HTTP server) should be accessing the database files at a time.

#### 2. Clustered Mode
In **Clustered** mode, the database is designed to be managed by many SOP HTTP Server instances and application nodes simultaneously.

- **Topology**: Distributed / Swarm.
- **Use Case**: Kubernetes clusters, EC2 auto-scaling groups, or bare-metal server farms.
- **Coordination**: All instances must be configured with the same **Redis** cluster address. Redis is used for distributed locking and transaction coordination.
- **Scalability**: You can spin up as many instances of the SOP Data Manager as needed.
- **Guarantee**: Management actions from any node are orchestrated properly with ACID guarantees. If User A updates a record via Server 1, and User B tries to update the same record via Server 2, SOP uses Redis to detect the conflict and ensure data integrity.

### The "Masterless" Philosophy

The **SOP Data Management Suite** follows this philosophy:
- **No Central Server**: This tool is not a "master" node. It is simply another client application (a "viewer") that connects to the same distributed storage backend as your production apps.
- **Flat Architecture**: You can spin up as many instances of this management tool as needed, anywhere in your cluster or on your local machine.
- **Masterless**: Since SOP uses decentralized coordination (via Redis or File Locking), this management tool can perform full CRUD operations safely alongside your high-throughput production workloads without becoming a bottleneck or a single point of failure.
- **"All Masters"**: In the SOP ecosystem, every running process (including this UI) is a "master" capable of reading and writing data directly to the storage layer, coordinated only by the lightweight locking mechanism.
- **Swarm Participation**: Each user managing data via this app participates in **"swarm" computing**. Their changes to records are efficiently merged (or rejected if conflicting) by SOP, all under strict **ACID transaction guarantees**.
- **Network & Cloud Ready**: While it works great on a local laptop disk, SOP is built to manage data on **Network Attached Storage (NAS)**, **S3-mounted drives**, or **Cloud Volumes**. This allows you to manage massive datasets that exceed local disk capacity, shared across the entire cluster.

## Backend Implementation
- **Backend**: Go HTTP server using `sop/infs` to open B-Trees as `[any, any]`.
- **Frontend**: Single HTML file with vanilla JS for API interaction.
- **Performance**: The browser is highly responsive because SOP includes a built-in caching layer, even in Standalone mode, ensuring fast access to frequently viewed nodes.
