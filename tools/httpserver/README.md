# SOP Data Management Suite (HTTP Server & UI)

A powerful, web-based management suite for SOP B-Tree repositories. This tool transforms SOP from a library into a full-fledged database server with a rich User Interface.

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
- **Mobile Optimized**: Fully responsive design with touch-friendly controls and a fullscreen AI assistant for managing data on the go.

## The Power of the Web UI: From Embedded to Enterprise

The SOP Web UI is not just a simple admin tool; it is a demonstration of the SOP architecture's flexibility. Because SOP is a library that turns your application into the database engine, this Web UI is effectively a **universal database server** that can adapt to any scale.

### 1. Embedded & Single-Node
For local development, IoT devices, or single-user desktop applications, the Web UI acts as a standalone database server.
- **Zero Setup**: Just point it to a folder.
- **Instant Access**: Serve one or many databases from a single lightweight process.
- **Use Case**: A developer inspecting their local data, or an embedded device exposing a management interface.

### 2. Enterprise Swarm (Clustered)
In a large-scale enterprise environment, the Web UI shines as a stateless management node within your storage swarm.
- **Scalable Hosting**: Deploy the Web UI on **Kubernetes**, **AWS EC2 Auto Scaling Groups**, or **Linux Bare Metal** farms.
- **Dynamic Scaling**: Spin up as many instances of the Web UI as needed to serve different teams or departments.
- **Cluster-Aware**: When configured with the same Redis endpoint as your production applications, the Web UI participates in the same distributed transaction protocols.
- **Safe Production Access**: You can view, edit, and manage live production data safely. The Web UI respects all distributed locks, ensuring that manual admin actions never corrupt data or violate ACID properties, even while your high-throughput microservices are hammering the same data files.

## Usage

### Prerequisites
- Go 1.20+ installed.
- An existing SOP database (folder containing SOP data).

### Running

You can run the server in two ways: using command-line flags for a single database, or using a configuration file for multiple databases.

#### Option 1: Single Database (Quick Start)

From the root of the `sop` repository:

```bash
go run tools/httpserver/main.go -database /path/to/your/sop/data
```

#### Option 2: Multiple Databases (Recommended)

Create a JSON configuration file (e.g., `config.json`) to define your environments:

```json
{
  "port": 8080,
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

Run the server with the config file:

```bash
go run tools/httpserver/main.go -config config.json
```

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
