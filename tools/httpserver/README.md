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
