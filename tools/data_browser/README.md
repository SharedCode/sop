# SOP Data Browser

A lightweight, web-based tool for inspecting and browsing SOP B-Tree repositories.

## Features
- **Store Listing**: View all B-Trees in a registry.
- **Data Grid**: Browse key/value pairs with pagination.
- **Navigation**: Seamlessly navigate between data pages (Next/Previous) to explore large datasets.
- **Search**: Find specific records using complex key inputs to jump directly to a location in the B-Tree.
- **JSON Inspection**: View complex value structures as formatted JSON.
- **Universal Access**: Works with any SOP store, regardless of the original data type (uses generic JSON serialization).

## Usage

### Prerequisites
- Go 1.20+ installed.
- An existing SOP registry (folder containing SOP data).

### Running

From the root of the `sop` repository:

```bash
go run tools/data_browser/main.go -registry /path/to/your/sop/data
```

By default, it runs on port 8080. You can change this with the `-port` flag:

```bash
go run tools/data_browser/main.go -registry ./data -port 9090
```

### Accessing
Open your browser and navigate to:
http://localhost:8080

## Architecture
- **Backend**: Go HTTP server using `sop/infs` to open B-Trees as `[any, any]`.
- **Frontend**: Single HTML file with vanilla JS for API interaction.
