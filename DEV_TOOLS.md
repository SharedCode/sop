# Developer Tools & Scripts Cheat Sheet

This document serves as a reference for the automation scripts and tools available in the SOP repository. These scripts streamline development, testing, and release processes.

## 🛠️ Essential Maintenance Scripts

### `update_version.sh`
**Usage:** `./update_version.sh <version_number>`
**Example:** `./update_version.sh 2.2.5`
Updates the version number across the entire ecosystem. It modifies:
- Go Server (`VERSION`)
- Python (`pyproject.toml`)
- Rust (`Cargo.toml`)
- Java (`pom.xml`)
- C# (`.csproj` files within `bindings/csharp`)
- C# (`VERSION` file)

### `build_release.sh`
**Usage:** `./build_release.sh`
Compiles and packages the project for release. It builds binaries for supported platforms and may generate distribution archives.

## 🧪 Test Automation Suites

These scripts allow you to run tests for specific languages or the entire platform.

### `run_all_tests.sh`
**Usage:** `./run_all_tests.sh`
The master text script. It executes the core Go tests and likely triggers the binding suites (check script content for full scope).

### Language-Specific Test Suites
*   **`run_go_suite.sh`**: Runs the core Go unit and integration tests.
*   **`run_python_suite.sh`**: Builds the Go bridge and runs Python binding tests.
*   **`run_java_suite.sh`**: Runs Java binding tests.
*   **`run_rust_suite.sh`**: Runs Rust binding tests.
*   **`run_dotnet_suite.sh`**: Runs C# (.NET) binding tests.

### Specialized Tests
*   **`run_ui_tests.sh`**: Executes tests for the Web UI.
*   **`run_incfs_integtests.sh`**: Runs integration tests specifically for the `incfs` (In-Cache File System) component (likely involving Cassandra connection tests).

## 🖥️ Applications & Tools

### Admin UI / Data Manager
**Location:** `tools/httpserver`
**Usage:** `go run ./tools/httpserver`
Launches the SOP HTTP Server, which includes a Web UI for managing data sections, visualizing stores, and monitoring the system.

### CLI Tools
(Check `tools/` directory for other utilities)

## 📦 Build & Release - Language Bindings

The `bindings` directory implements a "One Stop Hub" strategy. The Go core (`bindings/main`) is compiled into shared or static libraries, which are then consumed by each language's build system.

### 1. Compile Core Binaries
**Script:** `bindings/main/build.sh`
This is the **prerequisite step** for all bindings. It cross-compiles the Go code into:
- Shared Libraries (`.so`, `.dll`, `.dylib`) for Python, Java (JNA), and C#.
- Static Archives (`.a`) for Rust.

### 2. Python (Wheels)
**Script:** `bindings/python/build_wheels.sh`
**Output:** `.whl` and `.tar.gz` in `bindings/python/dist/`
Syncs the version from `pyproject.toml`, requires the shared libraries from Step 1, and builds platform-specific wheels.

### 3. C# / .NET (NuGet)
**Script:** `bindings/csharp/build.sh`
**Output:** `.nupkg` files in `bindings/csharp/dist/`
Packages the .NET assemblies along with the native binaries (in `runtimes/` folder) into NuGet packages for `Sop`, `Sop.HttpServer`, and `Sop.CLI`.

### 4. Java (Maven)
**Command:** `mvn package` (in `bindings/java/`)
**Output:** `.jar` in `bindings/java/target/`
The `pom.xml` relies on the shared libraries being present in `src/main/resources` (which Step 1 populates).

### 5. Rust (Cargo)
**Command:** `cargo build --release` (in `bindings/rust/`)
**Output:** Static binary in `bindings/rust/target/release/`
Reliant on `build.rs` finding the static `.a` archives generated in Step 1.
